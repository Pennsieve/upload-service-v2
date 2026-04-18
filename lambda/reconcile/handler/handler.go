// Package handler implements the reconcile-orphans lambda.
//
// Two invocation modes share the same code path:
//
//  1. One-shot manifest recovery (operator-triggered).
//     Payload: {"manifestNodeId": "<uuid>"}.
//     Scans manifest_files for that manifest, HEADs each expected storage
//     key, enqueues recoverable files to upload_trigger_queue.
//
//  2. Scheduled sweep (EventBridge, daily).
//     Payload: {"gracePeriodHours": 6}.
//     Scans the StatusIndex GSI for every manifest_files row in "Registered"
//     status whose manifest's DateCreated is older than gracePeriodHours,
//     applies the same HEAD-then-enqueue recovery.
//
// Never deletes anything. Recovery means "HEAD succeeded, synthesized S3
// event sent to upload_trigger_queue"; from that point the existing upload
// lambda consumer imports the file and transitions the DynamoDB row to
// Finalized. Files whose S3 object is absent (case B in the orphan taxonomy)
// are counted as "missing" and logged for operator follow-up.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
)

var (
	dyClient  *dynamodb.Client
	s3Client  *s3.Client
	sqsClient *sqs.Client

	manifestTableName     string
	manifestFileTableName string
	uploadTriggerQueueURL string
	defaultStorageBucket  string
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
	manifestFileTableName = os.Getenv("MANIFEST_FILE_TABLE")
	uploadTriggerQueueURL = os.Getenv("UPLOAD_TRIGGER_QUEUE_URL")
	defaultStorageBucket = os.Getenv("DEFAULT_STORAGE_BUCKET")
}

// InitializeClients constructs AWS SDK clients. Called from main.go's
// package init hook so cold starts share one set of clients.
func InitializeClients() {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v", err)
	}
	dyClient = dynamodb.NewFromConfig(cfg)
	s3Client = s3.NewFromConfig(cfg)
	sqsClient = sqs.NewFromConfig(cfg)
}

// Payload is the JSON body that invokes the lambda. Exactly one of
// ManifestNodeID or GracePeriodHours should be set. Concurrency caps the
// number of in-flight HEAD requests; default is 16, which suits a 512 MB
// Lambda (HEAD is network-bound, not CPU-bound).
type Payload struct {
	ManifestNodeID   string `json:"manifestNodeId,omitempty"`
	GracePeriodHours int    `json:"gracePeriodHours,omitempty"`
	DryRun           bool   `json:"dryRun,omitempty"`
	Concurrency      int    `json:"concurrency,omitempty"`
}

const (
	defaultConcurrency = 16
	maxConcurrency     = 64
)

// Result is the aggregate outcome of a reconciliation run.
type Result struct {
	ManifestsScanned int                      `json:"manifestsScanned"`
	FilesScanned     int                      `json:"filesScanned"`
	Recovered        int                      `json:"recovered"`
	Missing          int                      `json:"missing"`
	EnqueueFailed    int                      `json:"enqueueFailed"`
	Errors           []string                 `json:"errors,omitempty"`
	PerManifest      map[string]ManifestStats `json:"perManifest,omitempty"`
	DryRun           bool                     `json:"dryRun"`
}

type ManifestStats struct {
	FilesScanned int `json:"filesScanned"`
	Recovered    int `json:"recovered"`
	Missing      int `json:"missing"`
}

// Handle is the Lambda entrypoint. Loads a short-lived Postgres connection
// via RDS Proxy (required for storage-bucket resolution), dispatches on
// payload shape, and emits a summary record the scheduled alarm watches.
func Handle(ctx context.Context, p Payload) (Result, error) {
	if (p.ManifestNodeID == "") == (p.GracePeriodHours == 0) {
		return Result{}, errors.New("exactly one of manifestNodeId or gracePeriodHours must be set")
	}

	pgdb, err := pgQueries.ConnectRDS()
	if err != nil {
		return Result{}, fmt.Errorf("connect rds: %w", err)
	}
	defer pgdb.Close()
	pg := pgQueries.New(pgdb)

	store := &store{
		dy:                    dyClient,
		s3:                    s3Client,
		sqs:                   sqsClient,
		pg:                    pg,
		manifestTable:         manifestTableName,
		manifestFileTable:     manifestFileTableName,
		uploadTriggerQueueURL: uploadTriggerQueueURL,
		defaultStorageBucket:  defaultStorageBucket,
	}

	concurrency := p.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	if concurrency > maxConcurrency {
		concurrency = maxConcurrency
	}

	var result Result
	result.DryRun = p.DryRun
	result.PerManifest = make(map[string]ManifestStats)

	if p.ManifestNodeID != "" {
		if err := store.reconcileManifest(ctx, p.ManifestNodeID, p.DryRun, concurrency, &result); err != nil {
			result.Errors = append(result.Errors, err.Error())
		}
	} else {
		if err := store.reconcileByGracePeriod(ctx, p.GracePeriodHours, p.DryRun, concurrency, &result); err != nil {
			result.Errors = append(result.Errors, err.Error())
		}
	}

	emitMetrics(result)

	body, _ := json.Marshal(result)
	log.WithField("result", string(body)).Info("reconciliation complete")
	return result, nil
}

// emitMetrics writes a CloudWatch EMF record so `OrphansRecovered`,
// `OrphansMissing`, and `ReconciliationErrors` show up as metrics without a
// separate PutMetricData call. Alarms/dashboards consume these directly.
func emitMetrics(r Result) {
	emf := map[string]any{
		"_aws": map[string]any{
			"Timestamp": timestampMillis(),
			"CloudWatchMetrics": []map[string]any{{
				"Namespace":  "UploadService/Reconcile",
				"Dimensions": [][]string{{}},
				"Metrics": []map[string]string{
					{"Name": "OrphansRecovered", "Unit": "Count"},
					{"Name": "OrphansMissing", "Unit": "Count"},
					{"Name": "ReconciliationErrors", "Unit": "Count"},
					{"Name": "EnqueueFailed", "Unit": "Count"},
				},
			}},
		},
		"OrphansRecovered":     r.Recovered,
		"OrphansMissing":       r.Missing,
		"ReconciliationErrors": len(r.Errors),
		"EnqueueFailed":        r.EnqueueFailed,
	}
	line, _ := json.Marshal(emf)
	// stdout so Lambda picks it up as a log line and parses the EMF block.
	fmt.Println(string(line))
}
