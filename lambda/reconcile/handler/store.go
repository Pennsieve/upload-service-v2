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
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/bucketregion"
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
	// storageBucket's region; resolved once per manifest, reused per HeadObject call
	storageRegion string
}

// resolveManifest locates a manifest's objects and confirms the manifest is
// actually importable. Used by the recovery paths, which must not enqueue work
// that can never succeed.
func (s *store) resolveManifest(ctx context.Context, manifestID string) (*resolvedManifest, error) {
	r, err := s.resolveManifestLocation(ctx, manifestID)
	if err != nil {
		return nil, err
	}
	if err := s.assertDatasetExists(ctx, r.manifest); err != nil {
		return nil, err
	}
	return r, nil
}

// resolveManifestLocation works out which bucket, region and key prefix a
// manifest's objects live under. Deliberately says nothing about whether the
// manifest can be imported — reportOnly needs to locate objects belonging to
// deleted datasets, which are exactly the ones resolveManifest rejects.
func (s *store) resolveManifestLocation(ctx context.Context, manifestID string) (*resolvedManifest, error) {
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
	region, err := bucketregion.Resolve(ctx, s.s3, bucket)
	if err != nil {
		return nil, fmt.Errorf("resolve region for %s: %w", bucket, err)
	}
	return &resolvedManifest{
		manifest:      m,
		storageBucket: bucket,
		keyPrefix:     fmt.Sprintf("O%d/D%d/%s", m.OrganizationId, m.DatasetId, manifestID),
		storageRegion: region,
	}, nil
}

// assertDatasetExists returns errDatasetGone when the manifest's dataset row is
// no longer in Postgres.
//
// Without this check a manifest whose dataset has been deleted looks perfectly
// healthy — the DynamoDB manifest row and the org row both survive dataset
// deletion — so every run HEADs the objects, re-enqueues them, and the upload
// lambda fails each one on packages_dataset_id_fkey. That loop is what pinned
// the prod upload_trigger DLQ at ~1470 messages (210 files/day x 7-day
// retention) and fired the DLQ alarm daily.
//
// datasets lives in the per-org schema and pgdb's getDataset issues an
// unqualified `FROM datasets`, so the search_path has to be set first. Safe
// here because this only runs on reconcileByGracePeriod's paginator goroutine
// (results are cached per manifest) — the concurrent HEAD workers never touch
// Postgres, so no other goroutine can observe a half-switched search_path.
func (s *store) assertDatasetExists(ctx context.Context, m *dydb.ManifestTable) error {
	orgPg, err := s.pg.WithOrg(int(m.OrganizationId))
	if err != nil {
		return fmt.Errorf("set search_path for org %d: %w", m.OrganizationId, err)
	}
	if _, err := orgPg.GetDatasetById(ctx, m.DatasetId); err != nil {
		// pgdb's scanDataset converts sql.ErrNoRows into a typed
		// DatasetNotFoundError, so matching on sql.ErrNoRows here would never
		// fire. Any other error is treated as possibly-transient by the caller.
		var notFound pgQueries.DatasetNotFoundError
		if errors.As(err, &notFound) {
			return fmt.Errorf("%w: dataset %d (org %d)", errDatasetGone, m.DatasetId, m.OrganizationId)
		}
		return fmt.Errorf("get dataset %d: %w", m.DatasetId, err)
	}
	return nil
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
	// Tracks manifest IDs that can never be imported — either the manifest_table
	// row or the Postgres dataset row is confirmed gone (not a transient error).
	// Files pointing at these have no recoverable state, so flip them to
	// FailedOrphan and they drop out of the StatusIndex=Registered scan on
	// future runs instead of being re-enqueued forever.
	unrecoverableManifests := make(map[string]bool)

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
					cache[manifestID] = nil
					// Only permanent failures (manifest row gone, dataset row
					// gone) become clean orphans. Any other resolve error
					// (Postgres 5xx, throttling, missing org row, region
					// lookup) might be transient, so we skip-without-flipping
					// and let a future run retry.
					if !isTerminalResolveError(err) {
						log.WithError(err).WithField("manifest_id", manifestID).Warn("resolve failed, skipping manifest")
						s.mu.Lock()
						result.Errors = append(result.Errors, err.Error())
						s.mu.Unlock()
						continue
					}
					unrecoverableManifests[manifestID] = true
					// Deliberately NOT appended to result.Errors. This is an
					// expected terminal state that we handle by flipping the rows
					// to FailedOrphan; counting it as a reconciliation error would
					// fire the errors alarm on every run until the flip lands,
					// which just trains everyone to ignore that alarm. The rows are
					// still counted toward Missing.
					log.WithError(err).WithField("manifest_id", manifestID).
						Info("manifest unrecoverable, flipping its rows to FailedOrphan")
					// fall through to orphan-mark branch below
				} else {
					// Grace period check at the manifest level: skip if the
					// manifest is newer than cutoff.
					if r.manifest.DateCreated > cutoff {
						cache[manifestID] = nil
						continue
					}
					cache[manifestID] = r
					resolved = r
				}
			}

			if resolved == nil {
				if !unrecoverableManifests[manifestID] {
					continue
				}
				// Manifest is unrecoverable (its row or its dataset is gone) —
				// just flip this row to FailedOrphan. No HEAD needed: for a
				// missing manifest there's no bucket to check, and for a deleted
				// dataset the object may well still be there but can never be
				// imported.
				if dryRun {
					s.bumpScanned(result, manifestID)
					s.bumpMissing(result, manifestID)
					continue
				}
				sem <- struct{}{}
				wg.Add(1)
				go func(mid, uid string) {
					defer wg.Done()
					defer func() { <-sem }()
					s.markOrphanedRow(ctx, mid, uid, result)
				}(manifestID, uploadID)
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

	// Count manifests actually visited: cache entries with resolved != nil,
	// plus confirmed-missing manifests (whose file rows we cleaned up).
	scanned := len(unrecoverableManifests)
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

// reportFailedOrphans audits every FailedOrphan row and reports which ones still
// have bytes in the storage bucket. Strictly read-only: no UpdateItem, no
// SendMessage, no DeleteObject. Its purpose is to size the population a cleanup
// tool would target, so that decision can be made against a number rather than
// an assumption.
//
// Most FailedOrphan rows are expected to have no object at all — the status is
// normally reached via the HEAD-404 path, meaning the client never finished its
// PUT. Rows whose object *does* survive are the interesting ones, and they are
// broken out per manifest with a datasetGone flag.
func (s *store) reportFailedOrphans(ctx context.Context, concurrency int, result *Result) error {
	report := &OrphanReport{PerManifest: make(map[string]OrphanManifestStats)}
	result.Report = report

	// Resolution is cached per manifest and, like the recovery path, happens on
	// this goroutine only — assertDatasetExists mutates the connection's
	// search_path, so it must not race the HEAD workers.
	type resolution struct {
		loc         *resolvedManifest
		datasetGone bool
		err         error
	}
	cache := make(map[string]*resolution)

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	p := dynamodb.NewQueryPaginator(s.dy, &dynamodb.QueryInput{
		TableName:              aws.String(s.manifestFileTable),
		IndexName:              aws.String("StatusIndex"),
		KeyConditionExpression: aws.String("#status = :hashKey"),
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":hashKey": &dyTypes.AttributeValueMemberS{Value: manifestFile.FailedOrphan.String()},
		},
		ExpressionAttributeNames: map[string]string{"#status": "Status"},
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			wg.Wait()
			return fmt.Errorf("scan StatusIndex for FailedOrphan: %w", err)
		}
		for _, item := range page.Items {
			var manifestID, uploadID string
			_ = attributevalue.Unmarshal(item["ManifestId"], &manifestID)
			_ = attributevalue.Unmarshal(item["UploadId"], &uploadID)
			if manifestID == "" || uploadID == "" {
				continue
			}

			res, ok := cache[manifestID]
			if !ok {
				res = &resolution{}
				loc, err := s.resolveManifestLocation(ctx, manifestID)
				if err != nil {
					// Can't locate the objects (manifest row gone, org gone,
					// region lookup failed). Nothing to report for these beyond
					// the row count.
					res.err = err
					log.WithError(err).WithField("manifest_id", manifestID).
						Debug("reportFailedOrphans: cannot locate manifest, skipping")
				} else {
					res.loc = loc
					res.datasetGone = errors.Is(s.assertDatasetExists(ctx, loc.manifest), errDatasetGone)
				}
				cache[manifestID] = res
			}

			s.bumpReportScanned(report, manifestID, res.datasetGone)
			if res.loc == nil {
				continue
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(loc *resolvedManifest, mid, uid string) {
				defer wg.Done()
				defer func() { <-sem }()

				head, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(loc.storageBucket),
					Key:    aws.String(fmt.Sprintf("%s/%s", loc.keyPrefix, uid)),
				}, func(o *s3.Options) {
					// Workspace buckets are not all in us-east-1 (there is an
					// af-south-1 one), and the default client would 301 on those.
					o.Region = loc.storageRegion
				})
				if err != nil {
					s.mu.Lock()
					if isS3NotFound(err) {
						report.ObjectsAbsent++
					} else {
						// Unclassifiable — the report undercounts, so say so
						// rather than silently treating it as absent.
						report.HeadErrors++
					}
					s.mu.Unlock()
					return
				}
				s.bumpReportPresent(report, mid, aws.ToInt64(head.ContentLength))
			}(res.loc, manifestID, uploadID)
		}
	}
	wg.Wait()

	log.WithFields(log.Fields{
		"files_scanned":   report.FilesScanned,
		"objects_present": report.ObjectsPresent,
		"objects_absent":  report.ObjectsAbsent,
		"bytes_present":   report.BytesPresent,
		"head_errors":     report.HeadErrors,
		"manifests":       len(report.PerManifest),
	}).Info("failed-orphan report complete")
	return nil
}

func (s *store) bumpReportScanned(r *OrphanReport, manifestID string, datasetGone bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.FilesScanned++
	st := r.PerManifest[manifestID]
	st.FilesScanned++
	if datasetGone {
		st.DatasetGone = true
	}
	r.PerManifest[manifestID] = st
}

func (s *store) bumpReportPresent(r *OrphanReport, manifestID string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.ObjectsPresent++
	r.BytesPresent += size
	st := r.PerManifest[manifestID]
	st.ObjectsPresent++
	st.BytesPresent += size
	r.PerManifest[manifestID] = st
}

// errDatasetGone marks a resolve failure caused by the manifest's dataset row
// no longer existing in Postgres. A sentinel rather than a string match because
// this one we raise ourselves.
var errDatasetGone = errors.New("dataset no longer exists")

// isTerminalResolveError reports whether a resolveManifest failure is permanent,
// meaning no future run can succeed and the manifest_files rows should be
// flipped to FailedOrphan rather than retried forever.
//
// Two cases qualify:
//   - the DynamoDB manifest row is gone
//   - the Postgres dataset row is gone (packages has an FK to datasets, so the
//     import can never succeed)
//
// Everything else — Postgres 5xx, DynamoDB throttling, a missing org row, an
// S3 region lookup failure — may be transient and must leave the rows in
// Registered so a later run can retry.
func isTerminalResolveError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errDatasetGone) {
		return true
	}
	// GetManifestById reports a missing manifest with a bare error string;
	// pennsieve-go-core exposes no sentinel or typed error to match on.
	return strings.Contains(err.Error(), "Manifest not found")
}

// markOrphanedRow flips a single manifest_files row whose manifest is
// unrecoverable — either the manifest row or its dataset is known-gone.
// Separate from reconcileFile because there's no bucket or keyPrefix to
// resolve (and nothing to HEAD) — all we have is manifestID + uploadID.
func (s *store) markOrphanedRow(ctx context.Context, manifestID, uploadID string, result *Result) {
	s.bumpScanned(result, manifestID)
	if err := s.markFailedOrphan(ctx, manifestID, uploadID); err != nil {
		s.appendError(result, fmt.Sprintf("markFailedOrphan %s: %v", uploadID, err))
		log.WithError(err).WithFields(log.Fields{
			"manifest_id": manifestID,
			"upload_id":   uploadID,
		}).Warn("failed to mark orphaned row FailedOrphan (manifest or dataset gone)")
		return
	}
	s.bumpMissing(result, manifestID)
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
	}, func(o *s3.Options) {
		o.Region = resolved.storageRegion
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
		UpdateExpression:    aws.String("SET #s = :new REMOVE InProgress"),
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
