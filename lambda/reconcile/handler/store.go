package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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

	// mu serializes Result/PerManifest updates across the worker pool.
	// Lives on the store so the Handle goroutine can safely snapshot
	// counters for periodic progress logging.
	mu sync.Mutex
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
// manifest (one-shot mode). HEAD calls are fanned out across `concurrency`
// goroutines bounded by a semaphore — S3 HEAD is network-bound, so 16-way
// parallelism on a 512 MB Lambda collapses the first-run backlog without
// hitting vCPU limits.
func (s *store) reconcileManifest(ctx context.Context, manifestID string, dryRun bool, concurrency int, result *Result) error {
	resolved, err := s.resolveManifest(ctx, manifestID)
	if err != nil {
		return err
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	err = s.forEachRegisteredFile(ctx, manifestID, func(uploadID string, size int64) {
		sem <- struct{}{}
		wg.Add(1)
		go func(uid string, sz int64) {
			defer wg.Done()
			defer func() { <-sem }()
			s.reconcileFile(ctx, resolved, uid, sz, dryRun, result)
		}(uploadID, size)
	})
	wg.Wait()

	s.mu.Lock()
	result.ManifestsScanned = 1
	s.mu.Unlock()
	return err
}

// reconcileByGracePeriod scans StatusIndex for Status=Registered across all
// manifests, filtering out manifests whose DateCreated is newer than the
// grace window (still legitimately in-progress). Caches manifest resolutions
// so each manifest only pays one Postgres lookup per run. HEAD calls fan
// out across `concurrency` goroutines; manifest resolution stays on the
// paginator goroutine so the cache remains uncontended.
func (s *store) reconcileByGracePeriod(ctx context.Context, gracePeriodHours int, dryRun bool, concurrency int, result *Result) error {
	cutoff := time.Now().Add(-time.Duration(gracePeriodHours) * time.Hour).Unix()
	cache := make(map[string]*resolvedManifest)

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

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
			wg.Wait()
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
					s.mu.Lock()
					result.Errors = append(result.Errors, err.Error())
					s.mu.Unlock()
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

			sem <- struct{}{}
			wg.Add(1)
			go func(res *resolvedManifest, uid string) {
				defer wg.Done()
				defer func() { <-sem }()
				s.reconcileFile(ctx, res, uid, 0, dryRun, result)
			}(resolved, uploadID)
		}
	}
	wg.Wait()

	// Manifests that actually produced at least one file counted as scanned.
	scanned := 0
	for _, r := range cache {
		if r != nil {
			scanned++
		}
	}
	s.mu.Lock()
	result.ManifestsScanned = scanned
	s.mu.Unlock()
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
// present, otherwise count as missing. Safe to call concurrently — all
// Result/PerManifest updates are serialized through s.mu. I/O (HEAD,
// UpdateItem, SendMessage) runs outside the mutex.
func (s *store) reconcileFile(
	ctx context.Context,
	resolved *resolvedManifest,
	uploadID string,
	expectedSize int64,
	dryRun bool,
	result *Result,
) {
	manifestID := resolved.manifest.ManifestId
	s.bumpScanned(result, manifestID)

	key := fmt.Sprintf("%s/%s", resolved.keyPrefix, uploadID)
	head, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(resolved.storageBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// S3 is strongly consistent for read-after-write (since 2020), so a
		// NotFound / 404 on HEAD means the object was never written or has
		// been deleted. That's terminal: flip the DynamoDB row to
		// FailedOrphan so subsequent reconciliation runs skip it (Status ==
		// Registered query doesn't match). Other errors (5xx, throttle,
		// permission) are transient — leave Registered so the next
		// scheduled run retries.
		if isS3NotFound(err) {
			s.bumpMissing(result, manifestID)
			if !dryRun {
				if updErr := s.markFailedOrphan(ctx, manifestID, uploadID); updErr != nil {
					s.appendError(result, fmt.Sprintf("markFailedOrphan %s: %v", uploadID, updErr))
					log.WithError(updErr).WithFields(log.Fields{
						"manifest_id": manifestID,
						"upload_id":   uploadID,
					}).Warn("failed to mark FailedOrphan (row remains Registered)")
				}
			}
		} else {
			s.appendError(result, fmt.Sprintf("HEAD %s: %v", uploadID, err))
			log.WithError(err).WithFields(log.Fields{
				"manifest_id": manifestID,
				"upload_id":   uploadID,
				"key":         key,
			}).Warn("HEAD failed")
		}
		return
	}

	if dryRun {
		s.bumpRecovered(result, manifestID)
		log.WithFields(log.Fields{
			"manifest_id": manifestID,
			"upload_id":   uploadID,
			"key":         key,
			"size":        aws.ToInt64(head.ContentLength),
		}).Info("dry-run: would enqueue recovery")
		return
	}

	if err := s.enqueueRecovery(ctx, resolved.storageBucket, key, aws.ToInt64(head.ContentLength)); err != nil {
		s.mu.Lock()
		result.EnqueueFailed++
		result.Errors = append(result.Errors, fmt.Sprintf("enqueue %s: %v", uploadID, err))
		s.mu.Unlock()
		log.WithError(err).WithFields(log.Fields{
			"manifest_id": manifestID,
			"upload_id":   uploadID,
		}).Warn("enqueue failed")
		return
	}
	s.bumpRecovered(result, manifestID)
}

func (s *store) bumpScanned(r *Result, manifestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.FilesScanned++
	stats := r.PerManifest[manifestID]
	stats.FilesScanned++
	r.PerManifest[manifestID] = stats
}

func (s *store) bumpMissing(r *Result, manifestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.Missing++
	stats := r.PerManifest[manifestID]
	stats.Missing++
	r.PerManifest[manifestID] = stats
}

func (s *store) bumpRecovered(r *Result, manifestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.Recovered++
	stats := r.PerManifest[manifestID]
	stats.Recovered++
	r.PerManifest[manifestID] = stats
}

func (s *store) appendError(r *Result, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.Errors = append(r.Errors, msg)
}

// snapshot returns a copy of the counters under mu. Intended for the
// progress-logging goroutine; PerManifest/Errors are deliberately omitted to
// keep the snapshot small.
func (s *store) snapshot(r *Result) (scanned, recovered, missing, enqueueFailed, errs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return r.FilesScanned, r.Recovered, r.Missing, r.EnqueueFailed, len(r.Errors)
}

// markFailedOrphan flips a Registered row to FailedOrphan status. The
// conditional expression guards against races — if the row transitioned
// out of Registered between our HEAD and this update (e.g. agent completed
// late), the update is a no-op.
func (s *store) markFailedOrphan(ctx context.Context, manifestID, uploadID string) error {
	_, err := s.dy.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.manifestFileTable),
		Key: map[string]dyTypes.AttributeValue{
			"ManifestId": &dyTypes.AttributeValueMemberS{Value: manifestID},
			"UploadId":   &dyTypes.AttributeValueMemberS{Value: uploadID},
		},
		// Drop from the sparse InProgressIndex GSI since FailedOrphan is
		// terminal. Setting Status alone isn't enough — InProgressIndex is
		// queried by CheckUpdateManifestStatus to decide manifest completion.
		UpdateExpression: aws.String("SET #s = :new REMOVE InProgress"),
		ConditionExpression: aws.String("#s = :reg"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":new": &dyTypes.AttributeValueMemberS{Value: manifestFile.FailedOrphan.String()},
			":reg": &dyTypes.AttributeValueMemberS{Value: manifestFile.Registered.String()},
		},
	})
	if err != nil {
		// ConditionalCheckFailedException means the row transitioned out of
		// Registered between HEAD and this update — treat as a benign race
		// and return nil (nothing to do). Any other error propagates up.
		var ccf *dyTypes.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil
		}
		return err
	}
	return nil
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
