package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
)

type store struct {
	dy  *dynamodb.Client
	s3  *s3.Client
	sqs *sqs.Client
	pg  *pgQueries.Queries

	manifestTable         string
	manifestFileTable     string
	uploadTriggerQueueURL string
	defaultStorageBucket  string
}

// resolvedManifest is what we need to reconstruct an S3 key and decide the
// destination bucket. Cached per-manifest within a run so we hit Postgres
// at most once per manifest.
type resolvedManifest struct {
	manifest      *dydb.ManifestTable
	storageBucket string
	keyPrefix     string // O{org}/D{ds}/{manifest}
}

func (s *store) resolveManifest(ctx context.Context, manifestID string) (*resolvedManifest, error) {
	dy := dyQueries.New(s.dy)
	m, err := dy.GetManifestById(ctx, s.manifestTable, manifestID)
	if err != nil {
		return nil, fmt.Errorf("get manifest %s: %w", manifestID, err)
	}
	org, err := s.pg.GetOrganization(ctx, m.OrganizationId)
	if err != nil {
		return nil, fmt.Errorf("get organization %d: %w", m.OrganizationId, err)
	}
	bucket := s.defaultStorageBucket
	if org.StorageBucket.Valid {
		bucket = org.StorageBucket.String
	}
	return &resolvedManifest{
		manifest:      m,
		storageBucket: bucket,
		keyPrefix:     fmt.Sprintf("O%d/D%d/%s", m.OrganizationId, m.DatasetId, manifestID),
	}, nil
}

// reconcileManifest recovers all stuck-Registered files for a single
// manifest (one-shot mode).
func (s *store) reconcileManifest(ctx context.Context, manifestID string, dryRun bool, result *Result) error {
	resolved, err := s.resolveManifest(ctx, manifestID)
	if err != nil {
		return err
	}
	stats := ManifestStats{}
	err = s.forEachRegisteredFile(ctx, manifestID, func(uploadID string, size int64) {
		stats.FilesScanned++
		result.FilesScanned++
		s.reconcileFile(ctx, resolved, uploadID, size, dryRun, &stats, result)
	})
	result.ManifestsScanned = 1
	result.PerManifest[manifestID] = stats
	return err
}

// reconcileByGracePeriod scans StatusIndex for Status=Registered across all
// manifests, filtering out manifests whose DateCreated is newer than the
// grace window (still legitimately in-progress). Caches manifest resolutions
// so each manifest only pays one Postgres lookup per run.
func (s *store) reconcileByGracePeriod(ctx context.Context, gracePeriodHours int, dryRun bool, result *Result) error {
	cutoff := time.Now().Add(-time.Duration(gracePeriodHours) * time.Hour).Unix()
	cache := make(map[string]*resolvedManifest)

	p := dynamodb.NewQueryPaginator(s.dy, &dynamodb.QueryInput{
		TableName:              aws.String(s.manifestFileTable),
		IndexName:              aws.String("StatusIndex"),
		KeyConditionExpression: aws.String("#status = :hashKey"),
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":hashKey": &dyTypes.AttributeValueMemberS{Value: "Registered"},
		},
		ExpressionAttributeNames: map[string]string{"#status": "Status"},
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("scan StatusIndex: %w", err)
		}
		for _, item := range page.Items {
			var manifestID, uploadID string
			_ = attributevalue.Unmarshal(item["ManifestId"], &manifestID)
			_ = attributevalue.Unmarshal(item["UploadId"], &uploadID)
			if manifestID == "" || uploadID == "" {
				continue
			}

			resolved, ok := cache[manifestID]
			if !ok {
				r, err := s.resolveManifest(ctx, manifestID)
				if err != nil {
					log.WithError(err).WithField("manifest_id", manifestID).Warn("resolve failed, skipping manifest")
					result.Errors = append(result.Errors, err.Error())
					cache[manifestID] = nil // skip subsequent files for this manifest
					continue
				}
				// Grace period check at the manifest level: skip if the
				// manifest is newer than cutoff.
				if r.manifest.DateCreated > cutoff {
					cache[manifestID] = nil
					continue
				}
				cache[manifestID] = r
				resolved = r
			}
			if resolved == nil {
				continue
			}

			stats := result.PerManifest[manifestID]
			stats.FilesScanned++
			result.FilesScanned++
			s.reconcileFile(ctx, resolved, uploadID, 0, dryRun, &stats, result)
			result.PerManifest[manifestID] = stats
		}
	}

	// Manifests that actually produced at least one file counted as scanned.
	scanned := 0
	for _, r := range cache {
		if r != nil {
			scanned++
		}
	}
	result.ManifestsScanned = scanned
	return nil
}

// forEachRegisteredFile paginates DynamoDB manifest_files for a single
// manifestId filtering by Status=Registered.
func (s *store) forEachRegisteredFile(ctx context.Context, manifestID string, fn func(uploadID string, size int64)) error {
	p := dynamodb.NewQueryPaginator(s.dy, &dynamodb.QueryInput{
		TableName:              aws.String(s.manifestFileTable),
		KeyConditionExpression: aws.String("ManifestId = :m"),
		FilterExpression:       aws.String("#status = :s"),
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":m": &dyTypes.AttributeValueMemberS{Value: manifestID},
			":s": &dyTypes.AttributeValueMemberS{Value: manifestFile.Registered.String()},
		},
		ExpressionAttributeNames: map[string]string{"#status": "Status"},
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("query manifest_files: %w", err)
		}
		for _, item := range page.Items {
			var uploadID string
			_ = attributevalue.Unmarshal(item["UploadId"], &uploadID)
			if uploadID == "" {
				continue
			}
			fn(uploadID, 0)
		}
	}
	return nil
}

// reconcileFile does the per-file recovery: HEAD the storage key, enqueue if
// present, otherwise count as missing.
func (s *store) reconcileFile(
	ctx context.Context,
	resolved *resolvedManifest,
	uploadID string,
	expectedSize int64,
	dryRun bool,
	stats *ManifestStats,
	result *Result,
) {
	key := fmt.Sprintf("%s/%s", resolved.keyPrefix, uploadID)
	head, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(resolved.storageBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Any error (NotFound, Forbidden, etc.) → file is unrecoverable via
		// this Lambda. Log the specifics but don't spam the metric with
		// non-missing errors — only NoSuchKey / 404 count as "missing".
		if isS3NotFound(err) {
			stats.Missing++
			result.Missing++
		} else {
			result.Errors = append(result.Errors, fmt.Sprintf("HEAD %s: %v", uploadID, err))
			log.WithError(err).WithFields(log.Fields{
				"manifest_id": resolved.manifest.ManifestId,
				"upload_id":   uploadID,
				"key":         key,
			}).Warn("HEAD failed")
		}
		return
	}

	if dryRun {
		stats.Recovered++
		result.Recovered++
		log.WithFields(log.Fields{
			"manifest_id": resolved.manifest.ManifestId,
			"upload_id":   uploadID,
			"key":         key,
			"size":        aws.ToInt64(head.ContentLength),
		}).Info("dry-run: would enqueue recovery")
		return
	}

	if err := s.enqueueRecovery(ctx, resolved.storageBucket, key, aws.ToInt64(head.ContentLength)); err != nil {
		result.EnqueueFailed++
		result.Errors = append(result.Errors, fmt.Sprintf("enqueue %s: %v", uploadID, err))
		log.WithError(err).WithFields(log.Fields{
			"manifest_id": resolved.manifest.ManifestId,
			"upload_id":   uploadID,
		}).Warn("enqueue failed")
		return
	}
	stats.Recovered++
	result.Recovered++
}

func (s *store) enqueueRecovery(ctx context.Context, bucket, key string, size int64) error {
	ev := events.S3Event{Records: []events.S3EventRecord{{
		EventSource: "aws:s3",
		EventName:   "ObjectCreated:Put",
		S3: events.S3Entity{
			Bucket: events.S3Bucket{Name: bucket},
			Object: events.S3Object{Key: key, Size: size},
		},
	}}}
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = s.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.uploadTriggerQueueURL),
		MessageBody: aws.String(string(body)),
	})
	return err
}

func isS3NotFound(err error) bool {
	var notFound *types404
	if errors.As(err, &notFound) {
		return true
	}
	// Fallback: the SDK exposes the code via the wrapped http response. Match
	// the common string forms; StatusCode: 404 is what S3 returns for HEAD
	// on a missing key.
	msg := err.Error()
	return strings.Contains(msg, "StatusCode: 404") ||
		strings.Contains(msg, "NotFound") ||
		strings.Contains(msg, "NoSuchKey")
}

// types404 is a placeholder for errors.As; aws-sdk-go-v2 v1.51.4 does expose
// a typed NotFound but depending on the operation the concrete type differs.
// The string-match fallback above catches the remaining cases.
type types404 struct{}

func (types404) Error() string { return "NotFound" }

func timestampMillis() int64 {
	return time.Now().UnixMilli()
}

// Compile-time check: keep the sqs batch entry type in scope if we later
// want to upgrade enqueueRecovery to SendMessageBatch (it's not needed for
// one-message-at-a-time recovery traffic).
var _ = sqsTypes.SendMessageBatchRequestEntry{}
