package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/gateway"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	dyQueriesNs "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	"github.com/pennsieve/pennsieve-upload-service-v2/service/pkg/storage"
	log "github.com/sirupsen/logrus"
)

// maxFinalizeBatch bounds a single finalize request. Sized so p99 handler
// latency stays well under API Gateway HTTP v2's default 30s integration
// timeout even under Postgres contention — the upload-lambda p99 per batch
// is observed around 17 ms/file on the 10k-file test, so 250 = ~4.5s of
// lambda time plus overhead. Keep in sync with the agent's dispatch batch
// size in pkg/server/upload.go (finalizeBatcher.maxBatch) and the
// maxItems constraint in terraform/upload-service.yml.
const maxFinalizeBatch = 250

// headConcurrency caps how many HeadObject calls run in parallel per invocation.
const headConcurrency = 50

type finalizeFileRequest struct {
	UploadID string `json:"uploadId"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256,omitempty"`
}

type finalizeRequest struct {
	ManifestNodeID string                `json:"manifestNodeId"`
	Files          []finalizeFileRequest `json:"files"`
}

type finalizeResult struct {
	UploadID string `json:"uploadId"`
	Status   string `json:"status"` // "finalized" | "failed"
	Error    string `json:"error,omitempty"`
}

type finalizeResponse struct {
	Results []finalizeResult `json:"results"`
}

// postFinalizeFilesRoute is the two-phase upload completion endpoint. The
// agent calls this after it has successfully PUT each file directly to the
// storage bucket; we verify, import into Postgres, and mark the manifest file
// Finalized. Idempotent per uploadId.
func postFinalizeFilesRoute(request events.APIGatewayV2HTTPRequest, claims *authorizer.Claims) (*events.APIGatewayV2HTTPResponse, error) {
	var req finalizeRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		log.WithError(err).Warn("finalize: invalid request body")
		return errResp(400, "invalid request body")
	}
	if !isValidUUID(req.ManifestNodeID) {
		return errResp(400, "manifestNodeId must be a UUID")
	}
	if len(req.Files) == 0 {
		return errResp(400, "files is required and must be non-empty")
	}
	if len(req.Files) > maxFinalizeBatch {
		return errResp(400, fmt.Sprintf("batch_too_large: max %d files per request", maxFinalizeBatch))
	}
	// Per-file input validation. Bad inputs fail the whole batch — we don't
	// want to quietly drop malformed entries because the client may think they
	// were accepted.
	for i, f := range req.Files {
		if !isValidUUID(f.UploadID) {
			return errResp(400, fmt.Sprintf("files[%d].uploadId must be a UUID", i))
		}
		if f.Size <= 0 {
			return errResp(400, fmt.Sprintf("files[%d].size must be > 0", i))
		}
		if f.SHA256 == "" {
			return errResp(400, fmt.Sprintf("files[%d].sha256 is required", i))
		}
	}

	ctx := context.Background()

	// Auth: manifest must belong to the caller's dataset.
	manifestRecord, err := store.dy.GetManifestById(ctx, store.tableName, req.ManifestNodeID)
	if err != nil {
		log.WithError(err).WithField("manifestNodeId", req.ManifestNodeID).Warn("manifest not found")
		return errResp(404, "Manifest not found")
	}
	if manifestRecord.DatasetNodeId != claims.DatasetClaim.NodeId {
		return errResp(403, "Manifest does not belong to this dataset")
	}

	defaultStorageBucket := os.Getenv("DEFAULT_STORAGE_BUCKET")
	if defaultStorageBucket == "" {
		log.Error("DEFAULT_STORAGE_BUCKET not configured")
		return errResp(500, "Storage not configured")
	}
	uploadTriggerQueueURL := os.Getenv("UPLOAD_TRIGGER_QUEUE_URL")
	if uploadTriggerQueueURL == "" {
		log.Error("UPLOAD_TRIGGER_QUEUE_URL not configured")
		return errResp(500, "Finalize dispatch not configured")
	}

	// Resolve destination bucket (same resolver the storage-credentials endpoint uses).
	pgdb, err := pgQueries.ConnectRDS()
	if err != nil {
		log.WithError(err).Error("failed to connect to RDS")
		return errResp(500, "Internal error")
	}
	defer pgdb.Close()

	resolution, err := storage.ResolveForManifest(
		ctx,
		req.ManifestNodeID,
		store.tableName,
		defaultStorageBucket,
		dyQueriesNs.New(store.dynamodb),
		pgQueries.New(pgdb),
	)
	if err != nil {
		log.WithError(err).WithField("manifestNodeId", req.ManifestNodeID).Error("failed to resolve storage bucket")
		return errResp(500, "Failed to resolve storage bucket")
	}
	keyPrefix := resolution.KeyPrefix(req.ManifestNodeID)

	// Look up all files' current status in one BatchGetItem. The result map
	// serves two purposes:
	//   1. idempotency — skip files already in Finalized status
	//   2. auth/ownership — uploadIds not present in the manifest's
	//      manifest_files rows are rejected outright (prevents a caller from
	//      submitting arbitrary uploadIds and triggering orphan-file deletion
	//      in the upload lambda downstream)
	manifestFileStatus, err := fetchManifestFileStatuses(ctx, store.dynamodb, store.fileTableName, req.ManifestNodeID, req.Files)
	if err != nil {
		log.WithError(err).WithField("manifestNodeId", req.ManifestNodeID).Error("finalize: failed to load manifest file statuses")
		return errResp(500, "internal error")
	}

	// Parallel HEAD verification on the storage bucket.
	resultsByUploadID := make(map[string]finalizeResult, len(req.Files))
	toImport := make([]finalizeFileRequest, 0, len(req.Files))
	var mu sync.Mutex
	sem := make(chan struct{}, headConcurrency)
	var wg sync.WaitGroup

	for _, f := range req.Files {
		status, inManifest := manifestFileStatus[f.UploadID]
		if !inManifest {
			resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "uploadId not found in manifest"}
			continue
		}
		if status == manifestFile.Finalized.String() {
			resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "finalized"}
			continue
		}

		wg.Add(1)
		f := f
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			key := fmt.Sprintf("%s/%s", keyPrefix, f.UploadID)
			// ChecksumMode: ENABLED is required for HeadObject to populate
			// ChecksumSHA256; without it the field is nil even when S3 has
			// stored the checksum, and the sha256 check below always fails.
			head, err := store.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:       aws.String(resolution.StorageBucket),
				Key:          aws.String(key),
				ChecksumMode: s3Types.ChecksumModeEnabled,
			})
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"manifest_id": req.ManifestNodeID,
					"upload_id":   f.UploadID,
				}).Warn("finalize: HEAD failed")
				mu.Lock()
				resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "object not found"}
				mu.Unlock()
				return
			}
			if head.ContentLength != nil && *head.ContentLength != f.Size {
				mu.Lock()
				resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "size mismatch"}
				mu.Unlock()
				return
			}
			// SHA256 is required (validated upstream). Enforce match against
			// what S3 computed & stored during the multipart upload.
			if head.ChecksumSHA256 == nil || *head.ChecksumSHA256 != f.SHA256 {
				mu.Lock()
				resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "sha256 mismatch"}
				mu.Unlock()
				return
			}
			mu.Lock()
			toImport = append(toImport, f)
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Enqueue synthesized S3 events to the upload_trigger_queue. The upload
	// lambda's existing SQS event-source mapping consumes them and runs the
	// same import flow as real S3 events. This path is async — we return
	// "finalized" on successful enqueue (SQS delivery guarantees + idempotent
	// upload-lambda import + DLQ-backed retries make "enqueued" equivalent to
	// "will be finalized"). Import failures that exhaust retries end up in
	// the dead-letter queue for operator review; the agent's
	// VerifyFinalizedStatus ticker is the belt-and-suspenders consistency
	// check.
	if len(toImport) > 0 {
		failed, err := enqueueToUploadQueue(ctx, store.sqsClient, uploadTriggerQueueURL, resolution.StorageBucket, keyPrefix, toImport)
		if err != nil {
			log.WithError(err).Error("failed to enqueue to upload queue")
			for _, f := range toImport {
				resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "enqueue failed"}
			}
		} else {
			for _, f := range toImport {
				if _, bad := failed[f.UploadID]; bad {
					resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "enqueue failed"}
				} else {
					resultsByUploadID[f.UploadID] = finalizeResult{UploadID: f.UploadID, Status: "finalized"}
				}
			}
		}
	}

	// Preserve input order in the response.
	results := make([]finalizeResult, 0, len(req.Files))
	for _, f := range req.Files {
		if r, ok := resultsByUploadID[f.UploadID]; ok {
			results = append(results, r)
		} else {
			results = append(results, finalizeResult{UploadID: f.UploadID, Status: "failed", Error: "unknown"})
		}
	}

	body, _ := json.Marshal(finalizeResponse{Results: results})
	return &events.APIGatewayV2HTTPResponse{StatusCode: 200, Body: string(body)}, nil
}

func errResp(code int, msg string) (*events.APIGatewayV2HTTPResponse, error) {
	return &events.APIGatewayV2HTTPResponse{
		StatusCode: code,
		Body:       gateway.CreateErrorMessage(msg, code),
	}, nil
}

// fetchManifestFileStatuses returns a map from uploadId to current Status for
// every requested uploadId that exists in the manifest's manifest_files rows.
// UploadIds not in the result map are not part of the manifest and must be
// rejected by the caller (prevents callers from submitting arbitrary
// uploadIds and triggering orphan-file deletion downstream).
func fetchManifestFileStatuses(
	ctx context.Context,
	dy *dynamodb.Client,
	fileTable string,
	manifestId string,
	files []finalizeFileRequest,
) (map[string]string, error) {
	if len(files) == 0 {
		return map[string]string{}, nil
	}

	// BatchGetItem caps at 100 items per request.
	statuses := make(map[string]string, len(files))
	for start := 0; start < len(files); start += 100 {
		end := start + 100
		if end > len(files) {
			end = len(files)
		}
		keys := make([]map[string]dyTypes.AttributeValue, 0, end-start)
		for _, f := range files[start:end] {
			keys = append(keys, map[string]dyTypes.AttributeValue{
				"ManifestId": &dyTypes.AttributeValueMemberS{Value: manifestId},
				"UploadId":   &dyTypes.AttributeValueMemberS{Value: f.UploadID},
			})
		}
		out, err := dy.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]dyTypes.KeysAndAttributes{
				fileTable: {
					Keys:                 keys,
					ProjectionExpression: aws.String("UploadId, #s"),
					ExpressionAttributeNames: map[string]string{
						"#s": "Status",
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Responses[fileTable] {
			uid, _ := item["UploadId"].(*dyTypes.AttributeValueMemberS)
			s, _ := item["Status"].(*dyTypes.AttributeValueMemberS)
			if uid == nil || s == nil {
				continue
			}
			statuses[uid.Value] = s.Value
		}
	}
	return statuses, nil
}

// enqueueToUploadQueue sends one synthesized S3 "Object Created" event per
// file to the upload_trigger_queue. The upload lambda's existing SQS event
// source drains the queue in small batches and runs the same ImportFiles
// flow as for real S3 notifications. Failed sends (per-message) are returned
// in the map so the caller can mark those files failed in the response; the
// vast majority either succeed or hit the dead-letter queue on the
// consumer side, giving SQS's at-least-once delivery guarantees.
//
// SQS SendMessageBatch limits: 10 messages, 256 KB per message, 256 KB total.
// Each synthesized S3 event is ~500 B so 10 per batch is well under the
// 256 KB cap. 250 files -> 25 batches, which with minimal round-trip latency
// totals well under 500 ms even sequentially; we fan out to keep it under
// ~100 ms.
func enqueueToUploadQueue(
	ctx context.Context,
	sqsClient *sqs.Client,
	queueURL string,
	bucket string,
	keyPrefix string,
	files []finalizeFileRequest,
) (map[string]struct{}, error) {
	const sqsBatchLimit = 10
	const sendConcurrency = 5

	failed := make(map[string]struct{})
	var mu sync.Mutex
	sem := make(chan struct{}, sendConcurrency)
	var wg sync.WaitGroup

	for start := 0; start < len(files); start += sqsBatchLimit {
		end := start + sqsBatchLimit
		if end > len(files) {
			end = len(files)
		}
		batch := files[start:end]

		wg.Add(1)
		sem <- struct{}{}
		go func(batch []finalizeFileRequest) {
			defer wg.Done()
			defer func() { <-sem }()

			entries := make([]sqsTypes.SendMessageBatchRequestEntry, 0, len(batch))
			idToUpload := make(map[string]string, len(batch))
			for i, f := range batch {
				key := fmt.Sprintf("%s/%s", keyPrefix, f.UploadID)
				s3Ev := events.S3Event{Records: []events.S3EventRecord{{
					EventSource: "aws:s3",
					EventName:   "ObjectCreated:Put",
					S3: events.S3Entity{
						Bucket: events.S3Bucket{Name: bucket},
						Object: events.S3Object{Key: key, Size: f.Size},
					},
				}}}
				body, _ := json.Marshal(s3Ev)
				// SQS batch entry Id must match ^[a-zA-Z0-9_-]{1,80}$; use
				// position so we can map failures back to uploadIds below.
				id := fmt.Sprintf("b%d", i)
				idToUpload[id] = f.UploadID
				entries = append(entries, sqsTypes.SendMessageBatchRequestEntry{
					Id:          aws.String(id),
					MessageBody: aws.String(string(body)),
				})
			}

			out, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				QueueUrl: aws.String(queueURL),
				Entries:  entries,
			})
			if err != nil {
				log.WithError(err).Errorf("SendMessageBatch failed for %d entries", len(entries))
				mu.Lock()
				for _, uid := range idToUpload {
					failed[uid] = struct{}{}
				}
				mu.Unlock()
				return
			}
			if len(out.Failed) > 0 {
				mu.Lock()
				for _, f := range out.Failed {
					if f.Id != nil {
						if uid, ok := idToUpload[*f.Id]; ok {
							failed[uid] = struct{}{}
						}
					}
					log.WithFields(log.Fields{
						"code":    aws.ToString(f.Code),
						"message": aws.ToString(f.Message),
					}).Warn("SQS message enqueue failed")
				}
				mu.Unlock()
			}
		}(batch)
	}

	wg.Wait()
	return failed, nil
}
