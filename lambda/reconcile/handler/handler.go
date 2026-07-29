// Package handler implements the reconcile-orphans lambda.
//
// Three invocation modes share the same code path:
//
//  1. One-shot manifest recovery (operator-triggered).
//     Payload: {"manifestNodeId": "<uuid>"}.
//     Scans manifest_files for that manifest, HEADs each expected storage
//     key, enqueues recoverable files to upload_trigger_queue.
//
//  2. Scheduled sweep (EventBridge, hourly).
//     Payload: {"gracePeriodHours": 6}.
//     Scans the StatusIndex GSI for every manifest_files row in "Registered"
//     status whose manifest's DateCreated is older than gracePeriodHours,
//     applies the same HEAD-then-enqueue recovery.
//
//  3. Failed-orphan audit (operator-triggered, read-only).
//     Payload: {"reportOnly": true}.
//     Scans the StatusIndex GSI for "FailedOrphan" rows and reports how many
//     still have an object in the storage bucket, and how many bytes those
//     hold. Writes nothing at all — not even a status flip. Exists so the
//     question "is there anything worth cleaning up?" can be answered with a
//     number before anyone builds a deletion path.
//
// Never deletes anything, in any mode. Recovery means "HEAD succeeded,
// synthesized S3 event sent to upload_trigger_queue"; from that point the
// existing upload lambda consumer imports the file and transitions the DynamoDB
// row to Finalized. Files whose S3 object is absent (case B in the orphan
// taxonomy) are counted as "missing" and logged for operator follow-up.
//
// Note that FailedOrphan is not a tombstone: the service lambda's
// resetFailedOrphans flips those rows back to Registered when a user re-syncs a
// manifest containing them, so anything acting on this report has to assume the
// set can shrink underneath it.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

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
// ManifestNodeID, GracePeriodHours or ReportOnly should be set. Concurrency
// caps the number of in-flight HEAD requests; default is 16, which suits a
// 512 MB Lambda (HEAD is network-bound, not CPU-bound).
type Payload struct {
	ManifestNodeID   string `json:"manifestNodeId,omitempty"`
	GracePeriodHours int    `json:"gracePeriodHours,omitempty"`
	DryRun           bool   `json:"dryRun,omitempty"`
	Concurrency      int    `json:"concurrency,omitempty"`

	// ReportOnly runs a third, strictly read-only mode: audit every
	// FailedOrphan row and report which ones still have bytes sitting in the
	// storage bucket. Writes nothing, enqueues nothing, deletes nothing.
	//
	// This exists to size the population a future cleanup tool would target,
	// before anyone builds one. As of 2026-07-29 a sample of the top 30
	// manifests in prod (13,890 of 20,640 rows) found zero surviving objects —
	// FailedOrphan is currently reached almost entirely via the HEAD-404 path,
	// where the client never finished its PUT, so there were never any bytes.
	// Deleted-dataset orphans are the first case that can leave real bytes
	// behind, so watch this number after that change ships.
	ReportOnly bool `json:"reportOnly,omitempty"`
}

const (
	defaultConcurrency = 16
	maxConcurrency     = 64
)

// validatePayload enforces exactly one mode. Kept separate from Handle so it is
// testable without AWS clients or a Postgres connection — Handle's first real
// action is ConnectRDS, so an invalid payload must be rejected before that.
func validatePayload(p Payload) error {
	modes := 0
	if p.ManifestNodeID != "" {
		modes++
	}
	if p.GracePeriodHours != 0 {
		modes++
	}
	if p.ReportOnly {
		modes++
	}
	if modes != 1 {
		return errors.New("exactly one of manifestNodeId, gracePeriodHours or reportOnly must be set")
	}
	return nil
}

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

	// Report is populated only by reportOnly runs.
	Report *OrphanReport `json:"report,omitempty"`
}

type ManifestStats struct {
	FilesScanned int `json:"filesScanned"`
	Recovered    int `json:"recovered"`
	Missing      int `json:"missing"`
}

// OrphanReport is the outcome of a reportOnly run: how many FailedOrphan rows
// still have a corresponding object in the storage bucket, and how many bytes
// those objects hold.
//
// ObjectsPresent is the number that a cleanup tool could actually reclaim.
// ObjectsAbsent is the expected common case — the row is a tombstone for an
// upload whose bytes never landed, so there is nothing to reclaim and nothing
// to delete.
type OrphanReport struct {
	FilesScanned   int   `json:"filesScanned"`
	ObjectsPresent int   `json:"objectsPresent"`
	ObjectsAbsent  int   `json:"objectsAbsent"`
	BytesPresent   int64 `json:"bytesPresent"`
	// HeadErrors counts rows we could not classify (5xx, permission, region
	// lookup). They are neither present nor absent — a nonzero value means the
	// report undercounts and should be re-run before acting on it.
	HeadErrors  int                            `json:"headErrors"`
	PerManifest map[string]OrphanManifestStats `json:"perManifest,omitempty"`
}

type OrphanManifestStats struct {
	FilesScanned   int   `json:"filesScanned"`
	ObjectsPresent int   `json:"objectsPresent"`
	BytesPresent   int64 `json:"bytesPresent"`
	// Set when the manifest's dataset row is gone. These are the orphans most
	// likely to hold real bytes, since the upload succeeded and only the import
	// was impossible.
	DatasetGone bool `json:"datasetGone,omitempty"`
}

// Handle is the Lambda entrypoint. Loads a short-lived Postgres connection
// via RDS Proxy (required for storage-bucket resolution), dispatches on
// payload shape, and emits a summary record the scheduled alarm watches.
func Handle(ctx context.Context, p Payload) (Result, error) {
	if err := validatePayload(p); err != nil {
		return Result{}, err
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

	// Progress ticker: logs running totals every 10s. Without this, a 600s
	// timeout produces a final summary that never fires, and CloudWatch
	// shows only sporadic warnings. The per-file DynamoDB commits already
	// preserve partial progress across timeouts — this is just visibility.
	progressDone := make(chan struct{})
	go func() {
		start := time.Now()
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-tick.C:
				scanned, recovered, missing, enqFailed, errs := store.snapshot(&result)
				elapsed := time.Since(start).Seconds()
				rate := float64(scanned) / elapsed
				log.WithFields(log.Fields{
					"scanned":        scanned,
					"recovered":      recovered,
					"missing":        missing,
					"enqueue_failed": enqFailed,
					"errors":         errs,
					"elapsed_sec":    int(elapsed),
					"rate_per_sec":   int(rate),
				}).Info("reconcile progress")
			}
		}
	}()
	defer close(progressDone)

	switch {
	case p.ReportOnly:
		if err := store.reportFailedOrphans(ctx, concurrency, &result); err != nil {
			result.Errors = append(result.Errors, err.Error())
		}
	case p.ManifestNodeID != "":
		if err := store.reconcileManifest(ctx, p.ManifestNodeID, p.DryRun, concurrency, &result); err != nil {
			result.Errors = append(result.Errors, err.Error())
		}
	default:
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
//
// reportOnly runs additionally emit OrphanedObjectsPresent / OrphanedBytesPresent.
// Those are deliberately not on a schedule and have no alarm — reportOnly is
// operator-invoked, so the metrics only appear when someone asks the question.
func emitMetrics(r Result) {
	line, _ := json.Marshal(buildEMF(r))
	// stdout so Lambda picks it up as a log line and parses the EMF block.
	fmt.Println(string(line))
}

// buildEMF is split out from emitMetrics purely so tests can assert on which
// metrics a given Result publishes without capturing stdout.
func buildEMF(r Result) map[string]any {
	metrics := []map[string]string{
		{"Name": "OrphansRecovered", "Unit": "Count"},
		{"Name": "OrphansMissing", "Unit": "Count"},
		{"Name": "ReconciliationErrors", "Unit": "Count"},
		{"Name": "EnqueueFailed", "Unit": "Count"},
	}
	values := map[string]any{
		"OrphansRecovered":     r.Recovered,
		"OrphansMissing":       r.Missing,
		"ReconciliationErrors": len(r.Errors),
		"EnqueueFailed":        r.EnqueueFailed,
	}

	if r.Report != nil {
		metrics = append(metrics,
			map[string]string{"Name": "OrphanedObjectsPresent", "Unit": "Count"},
			map[string]string{"Name": "OrphanedBytesPresent", "Unit": "Bytes"},
		)
		values["OrphanedObjectsPresent"] = r.Report.ObjectsPresent
		values["OrphanedBytesPresent"] = r.Report.BytesPresent
	}

	emf := map[string]any{
		"_aws": map[string]any{
			"Timestamp": timestampMillis(),
			"CloudWatchMetrics": []map[string]any{{
				"Namespace":  "UploadService/Reconcile",
				"Dimensions": [][]string{{}},
				"Metrics":    metrics,
			}},
		},
	}
	for k, v := range values {
		emf[k] = v
	}
	return emf
}
