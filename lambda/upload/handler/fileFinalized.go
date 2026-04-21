package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	log "github.com/sirupsen/logrus"
)

// FileFinalizedEvent mirrors the payload consumed by scan-service (and
// future metadata / AI-readiness services). Keep the JSON tags in sync
// with scan-service/cmd/scanner/main.go — this topic is an external
// fan-out contract, not an internal orchestration channel.
type FileFinalizedEvent struct {
	EventType      string `json:"eventType"`
	EventVersion   int    `json:"eventVersion"`
	FileID         int64  `json:"fileId"`
	FileUUID       string `json:"fileUUID"`
	S3Bucket       string `json:"s3Bucket"`
	S3Key          string `json:"s3Key"`
	Size           int64  `json:"size"`
	FileType       string `json:"fileType"`
	Extension      string `json:"extension"`
	Checksum       string `json:"checksum,omitempty"`
	OrganizationID int    `json:"organizationId"`
	DatasetID      int    `json:"datasetId"`
	ManifestID     string `json:"manifestId"`
	ComplianceTier string `json:"complianceTier"`
	Timestamp      string `json:"timestamp"`
	TraceID        string `json:"traceId"`
}

// complianceTierCache is a process-wide map[orgId]tier. An org's
// compliance tier changes at most once per deploy (and on change the
// lambda sandboxes recycle naturally), so we never expire entries.
var (
	complianceTierCache   = map[int]string{}
	complianceTierCacheMu sync.RWMutex
)

// getComplianceTier returns the cached compliance_tier for an org,
// loading it from pennsieve.organizations on first miss. Unknown orgs
// default to "standard" so missing rows don't block uploads.
func (s *UploadHandlerStore) getComplianceTier(ctx context.Context, orgID int) string {
	complianceTierCacheMu.RLock()
	tier, ok := complianceTierCache[orgID]
	complianceTierCacheMu.RUnlock()
	if ok {
		return tier
	}

	var loaded string
	err := s.pgdb.QueryRowContext(ctx,
		"SELECT compliance_tier FROM pennsieve.organizations WHERE id = $1",
		orgID,
	).Scan(&loaded)
	if err != nil {
		log.WithError(err).Warnf("file_finalized: compliance_tier lookup failed for org %d; defaulting to standard", orgID)
		loaded = "standard"
	}

	complianceTierCacheMu.Lock()
	complianceTierCache[orgID] = loaded
	complianceTierCacheMu.Unlock()
	return loaded
}

// PublishFileFinalized emits one FileFinalized event per committed file
// to the FileFinalized SNS topic. Publishes are batched (PublishBatch,
// 10 entries/request) mirroring the existing PublishToSNS pattern.
//
// Errors are logged and returned but ImportFiles callers should not
// fail the request on a publish error — the file is already durable
// and the scanner can be re-driven via the reconcile lambda if needed.
func (s *UploadHandlerStore) PublishFileFinalized(ctx context.Context, files []pgdb.File, manifest *dydb.ManifestTable) error {
	if s.FileFinalizedTopic == "" {
		// Topic not configured (local/unit-test env). Skip.
		return nil
	}

	tier := s.getComplianceTier(ctx, int(manifest.OrganizationId))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	traceID := ""
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		traceID = lc.AwsRequestID
	}

	const batchSize = 10
	entries := make([]types.PublishBatchRequestEntry, 0, batchSize)

	for i := range files {
		f := files[i]

		fileID, err := strconv.ParseInt(f.Id, 10, 64)
		if err != nil {
			log.WithError(err).Warnf("file_finalized: unable to parse file.Id %q; skipping event", f.Id)
			continue
		}

		evt := FileFinalizedEvent{
			EventType:      "FileFinalized",
			EventVersion:   1,
			FileID:         fileID,
			FileUUID:       f.UUID.String(),
			S3Bucket:       f.S3Bucket,
			S3Key:          f.S3Key,
			Size:           f.Size,
			FileType:       f.FileType.String(),
			Extension:      extractExtension(f.Name),
			Checksum:       f.CheckSum,
			OrganizationID: int(manifest.OrganizationId),
			DatasetID:      int(manifest.DatasetId),
			ManifestID:     manifest.ManifestId,
			ComplianceTier: tier,
			Timestamp:      now,
			TraceID:        traceID,
		}

		body, err := json.Marshal(evt)
		if err != nil {
			log.WithError(err).Warnf("file_finalized: marshal failed for file %d; skipping", fileID)
			continue
		}

		entries = append(entries, types.PublishBatchRequestEntry{
			Id:      aws.String(f.UUID.String()),
			Message: aws.String(string(body)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"complianceTier": {
					DataType:    aws.String("String"),
					StringValue: aws.String(tier),
				},
				"eventType": {
					DataType:    aws.String("String"),
					StringValue: aws.String("FileFinalized"),
				},
			},
		})

		if len(entries) == batchSize {
			if err := s.publishFileFinalizedBatch(ctx, entries); err != nil {
				return err
			}
			entries = entries[:0]
		}
	}

	if len(entries) > 0 {
		if err := s.publishFileFinalizedBatch(ctx, entries); err != nil {
			return err
		}
	}
	return nil
}

func (s *UploadHandlerStore) publishFileFinalizedBatch(ctx context.Context, entries []types.PublishBatchRequestEntry) error {
	out, err := s.SNSClient.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(s.FileFinalizedTopic),
		PublishBatchRequestEntries: entries,
	})
	if err != nil {
		return fmt.Errorf("file_finalized: PublishBatch: %w", err)
	}
	if len(out.Failed) > 0 {
		for _, f := range out.Failed {
			log.Warnf("file_finalized: failed entry id=%s code=%s msg=%s",
				aws.ToString(f.Id), aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return nil
}

func extractExtension(name string) string {
	i := strings.LastIndex(name, ".")
	if i < 0 || i == len(name)-1 {
		return ""
	}
	return strings.ToLower(name[i+1:])
}
