// Package handler implements the archive-sweeper lambda.
//
// Daily EventBridge schedule invokes this lambda; it scans manifest_table for
// rows older than MaxAgeDays and whose Status != Archived, then fires an
// async Lambda.Invoke to the archive_lambda for each (with
// remove_from_db=true). The archive_lambda writes the manifest to CSV in S3
// and removes the manifest_files rows.
//
// Never directly mutates state itself — it only dispatches to the existing
// archive flow. One-shot invocation is supported for operator use:
//
//	aws lambda invoke --function-name ... \
//	  --payload '{"maxAgeDays": 90, "dryRun": true}' /tmp/out.json
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	log "github.com/sirupsen/logrus"
)

var (
	dyClient          *dynamodb.Client
	lambdaClient      *lambda.Client
	manifestTableName string
	archiveLambdaArn  string
)

const (
	defaultMaxAgeDays       = 90
	defaultMaxInvokesPerRun = 50
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	manifestTableName = os.Getenv("MANIFEST_TABLE")
	archiveLambdaArn = os.Getenv("ARCHIVE_LAMBDA_ARN")
}

// InitializeClients is called from main's package init so cold starts share
// one set of AWS clients.
func InitializeClients() {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v", err)
	}
	dyClient = dynamodb.NewFromConfig(cfg)
	lambdaClient = lambda.NewFromConfig(cfg)
}

type Payload struct {
	// MaxAgeDays is the minimum age (in days, based on manifest.DateCreated)
	// at which an un-archived manifest becomes eligible for auto-archival.
	// Defaults to 90 when unset or <= 0.
	MaxAgeDays int `json:"maxAgeDays,omitempty"`

	// MaxInvokesPerRun bounds the number of archive_lambda invocations
	// triggered per sweep run. Defaults to 500. Remaining manifests are left
	// for subsequent scheduled runs so a burst of abandoned manifests
	// doesn't hammer the archive pipeline (each archive writes every
	// manifest_files row to CSV + DeleteItem loops on the DynamoDB side).
	MaxInvokesPerRun int `json:"maxInvokesPerRun,omitempty"`

	// DryRun reports what would be archived without actually invoking
	// archive_lambda. Useful for operator dry-run before tuning MaxAgeDays.
	DryRun bool `json:"dryRun,omitempty"`
}

type Result struct {
	ManifestsScanned  int      `json:"manifestsScanned"`
	ManifestsEligible int      `json:"manifestsEligible"`
	InvokesAttempted  int      `json:"invokesAttempted"`
	InvokesFailed     int      `json:"invokesFailed"`
	DryRun            bool     `json:"dryRun"`
	MaxAgeDays        int      `json:"maxAgeDays"`
	Errors            []string `json:"errors,omitempty"`
}

func Handle(ctx context.Context, p Payload) (Result, error) {
	if p.MaxAgeDays <= 0 {
		p.MaxAgeDays = defaultMaxAgeDays
	}
	if p.MaxInvokesPerRun <= 0 {
		p.MaxInvokesPerRun = defaultMaxInvokesPerRun
	}

	if archiveLambdaArn == "" && !p.DryRun {
		return Result{}, fmt.Errorf("ARCHIVE_LAMBDA_ARN not configured")
	}

	s := &sweeper{
		dy:                dyClient,
		lambda:            lambdaClient,
		manifestTable:     manifestTableName,
		archiveLambdaArn:  archiveLambdaArn,
		maxAgeDays:        p.MaxAgeDays,
		maxInvokesPerRun:  p.MaxInvokesPerRun,
		dryRun:            p.DryRun,
	}

	res, err := s.run(ctx)
	emitMetrics(res)

	body, _ := json.Marshal(res)
	log.WithField("result", string(body)).Info("archive-sweeper run complete")
	return res, err
}

func emitMetrics(r Result) {
	emf := map[string]any{
		"_aws": map[string]any{
			"Timestamp": timestampMillis(),
			"CloudWatchMetrics": []map[string]any{{
				"Namespace":  "UploadService/ArchiveSweeper",
				"Dimensions": [][]string{{}},
				"Metrics": []map[string]string{
					{"Name": "ManifestsEligible", "Unit": "Count"},
					{"Name": "InvokesAttempted", "Unit": "Count"},
					{"Name": "InvokesFailed", "Unit": "Count"},
					{"Name": "SweeperErrors", "Unit": "Count"},
				},
			}},
		},
		"ManifestsEligible": r.ManifestsEligible,
		"InvokesAttempted":  r.InvokesAttempted,
		"InvokesFailed":     r.InvokesFailed,
		"SweeperErrors":     len(r.Errors),
	}
	line, _ := json.Marshal(emf)
	fmt.Println(string(line))
}
