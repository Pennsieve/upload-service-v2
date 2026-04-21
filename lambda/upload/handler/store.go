package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pennsieve/pennsieve-go-core/pkg/domain"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/conflictStrategy"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	ps "github.com/pennsieve/pennsieve-go-core/pkg/models/pusher"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFolder"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	"time"
)

// seenFileUUIDs allows this Lambda to avoid trying to create the same file in Postgres more than once.
// Can happen if AWS sends out the same S3 create object event more than once.
// This could be a local variable to ImportFiles(). Just here in case the Lambda gets used more than once
// we get a little more de-duplication.
var seenFileUUIDs = map[uuid.UUID]int{}

// UploadHandlerStore provides the Queries interface and a db instance.
type UploadHandlerStore struct {
	pg                 *UploadPgQueries
	dy                 *UploadDyQueries
	pgdb               *sql.DB
	dynamodb           *dynamodb.Client
	SNSClient          domain.SnsAPI
	S3Client           domain.S3API
	pusherClient       domain.PusherAPI
	changelogClient    Changelogger
	sqsClient          *sqs.Client
	jobsQueueURL       string
	SNSTopic           string
	FileFinalizedTopic string
	fileTableName      string
	tableName          string
}

type PackagesAndFiles struct {
	packages []pgdb.Package
	files    []pgdb.File
}

type storageUpdateParams struct {
	total    int64
	packages map[int64]int64
}

// NewUploadHandlerStore returns a UploadHandlerStore object which implements the Queries
func NewUploadHandlerStore(db *sql.DB, dy *dynamodb.Client, sns domain.SnsAPI,
	s3 domain.S3API, fileTableName string, tableName string, snsTopic string,
	fileFinalizedTopic string,
	pc domain.PusherAPI, changelogger Changelogger,
	sqsClient *sqs.Client, jobsQueueURL string) *UploadHandlerStore {
	return &UploadHandlerStore{
		pgdb:               db,
		dynamodb:           dy,
		pusherClient:       pc,
		SNSClient:          sns,
		SNSTopic:           snsTopic,
		FileFinalizedTopic: fileFinalizedTopic,
		S3Client:           s3,
		changelogClient:    changelogger,
		sqsClient:          sqsClient,
		jobsQueueURL:       jobsQueueURL,
		pg:                 NewUploadPgQueries(db),
		dy:                 NewUploadDyQueries(dy),
		fileTableName:      fileTableName,
		tableName:          tableName,
	}
}

// WithOrg sets the search path for the pg queries
func (s *UploadHandlerStore) WithOrg(orgId int) error {
	_, err := s.pg.WithOrg(orgId)
	return err

}

// execTx wraps a function in a transaction.
func (s *UploadHandlerStore) execTx(ctx context.Context, fn func(queries *UploadPgQueries) (interface{}, error)) (interface{}, error) {

	// NOTE: When you create a new transaction (as below), the s.pgdb is NOT part of the transaction.
	// This has the following impact
	// 1. If you have set the search-path for the pgdb, the search path is no longer applied to s.pgdb
	// 2. Any function that is wrapped in the execTx method should ONLY use the provided queries' struct that wraps the transaction.
	// 3. To enable custom Queries for a service, we wrap the pgdb.Queries in a service specific Queries struct.
	//	  This enables you to create custom queries within the service that leverage the transaction
	//    You can use the exposed Db property of the Queries' struct to create custom database interactions.

	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return err, nil
	}

	q := NewUploadPgQueries(tx)

	result, err := fn(q)
	if err != nil {
		log.WithFields(log.Fields{
			"service": "Upload-service",
		}).Warn("Rolling back transaction.")

		if rbErr := tx.Rollback(); rbErr != nil {
			log.WithFields(log.Fields{
				"service": "Upload-service",
			}).Error("Error while rolling back transaction.")

			return nil, fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return nil, err
	}

	return result, tx.Commit()
}

// ImportFiles creates rows for uploaded files in Packages and Files tables as a transaction
// All files belong to a single manifest, and therefor single dataset in a single organization.
//
// When directToStorage is true, the files already landed at final storage (the
// agent uploaded straight to the storage bucket), so SNS publish is skipped —
// there's nothing for the Fargate move task to do.
func (s *UploadHandlerStore) ImportFiles(ctx context.Context, datasetId int, orgId int, user pgdb.User,
	files []uploadFile.UploadFile, manifest *dydb.ManifestTable, directToStorage bool, onConflict string) error {

	contextLogger := log.WithFields(log.Fields{
		"service":     "Upload-service",
		"manifest_id": manifest.ManifestId,
		"dataset_id":  manifest.DatasetNodeId,
		"org_id":      manifest.OrganizationId,
		"user":        fmt.Sprintf("%s %s", user.FirstName, user.LastName),
	})

	// Verify assumptions
	for i, f := range files {
		// avoiding for-loop variable gotcha
		files[i].Path = trimSlashes(f.Path)
		if f.ManifestId != manifest.ManifestId {
			return errors.New("not all files belong to the same manifest (required for ImportFiles method)")
		}
	}

	var f uploadFile.UploadFile
	f.Sort(files)

	// 1. Iterate over files and return map of folders and sub-folders
	folderMap := getUploadFolderMap(files, "")
	if contextLogger.Logger.IsLevelEnabled(log.DebugLevel) {
		contextLogger.WithFields(log.Fields{"folderMap": folderMap}).Debug("calculated folder map")
	}

	// 2. Iterate over folders and create them if they do not exist in organization
	// This will lock rows in db for concurrent Lambda handlers so wrapping in its own TX to minimize time.
	res, err := s.execTx(ctx, func(qtx *UploadPgQueries) (interface{}, error) {
		folderPackageMap, err := qtx.GetCreateUploadFolders(datasetId, int(user.Id), folderMap)
		if err != nil {
			contextLogger.Error("Unable to create folders in ImportFiles function: ", err)
			return nil, err
		}
		return folderPackageMap, nil
	})
	if err != nil {
		contextLogger.Error("Unable to create folders. ", err)
		return err
	}

	folderPackageMap := res.(pgdb.PackageMap)
	if contextLogger.Logger.IsLevelEnabled(log.DebugLevel) {
		contextLogger.WithFields(log.Fields{"folderPackageMap": folderPackageMap}).Debug("calculated folder package map")
	}

	pkgParams, err := getPackageParams(datasetId, int(user.Id), files, folderPackageMap)
	if err != nil {
		contextLogger.Error("Unable to parse package parameters: ", err)
		return err
	}
	if contextLogger.Logger.IsLevelEnabled(log.DebugLevel) {
		contextLogger.WithFields(log.Fields{"pkgParams": pkgParams}).Debug("calculated package parameters")
	}

	// 3. Create packages and Files in Transaction
	strategy := conflictStrategyFromAttr(onConflict)
	res, err = s.execTx(ctx, func(qtx *UploadPgQueries) (interface{}, error) {
		packages, err := qtx.AddPackagesWithConflict(context.Background(), pkgParams, strategy)
		if err != nil {
			contextLogger.Error("Error creating packages: ", err)
			return nil, err
		}

		packageMap := map[string]pgdb.Package{}
		for _, p := range packages {
			packageMap[p.NodeId] = p
			contextLogger.Info(fmt.Sprintf("Package created: %s", p.NodeId))
		}

		var allFileParams []pgdb.FileParams
		for i, f := range files {
			packageNodeId := fmt.Sprintf("N:package:%s", f.UploadId)
			if len(files[i].MergePackageId) > 0 {
				contextLogger.Debug("USING MERGED PACKAGE")
				packageNodeId = fmt.Sprintf("N:package:%s", files[i].MergePackageId)
			}
			fileUUID := uuid.MustParse(files[i].UploadId)
			file := pgdb.FileParams{
				PackageId:  int(packageMap[packageNodeId].Id),
				Name:       files[i].Name,
				FileType:   files[i].FileType,
				S3Bucket:   files[i].S3Bucket,
				S3Key:      files[i].S3Key,
				ObjectType: objectType.Source,
				Size:       files[i].Size,
				CheckSum:   files[i].ETag,
				UUID:       fileUUID,
				Sha256:     files[i].Sha256,
			}
			// S3 may send a create object event
			// for a given file more than once. If more than one instance
			// ended up in this batch, here we ensure only one is sent
			// to AddFiles() below.
			if seen, ok := seenFileUUIDs[fileUUID]; !ok {
				seenFileUUIDs[fileUUID] = 1
				allFileParams = append(allFileParams, file)
			} else {
				seenCount := seen + 1
				seenFileUUIDs[fileUUID] = seenCount
				contextLogger.WithFields(log.Fields{"duplicate_file_uuid": fileUUID, "seen_count": seenCount}).Warn("duplicate uuid")
			}
		}

		returnedFiles, err := qtx.AddFiles(context.Background(), allFileParams)
		if err != nil {
			contextLogger.Error("Unable to add files to postgres.", err)
			return nil, err
		}

		response := PackagesAndFiles{
			packages: packages,
			files:    returnedFiles,
		}

		return response, nil
	})
	if err != nil {
		contextLogger.Error("Unable to create Packages and/or Files. ", err)
		return err
	}

	result := res.(PackagesAndFiles)
	for _, f := range result.files {
		contextLogger.Info(fmt.Sprintf("Package and File created: %s", f.UUID))
	}

	// 4. Update storage for Packages, Dataset and Organization.
	storageMap, err := s.createStorageUpdateMap(ctx, result)
	if err != nil {
		contextLogger.WithError(err).Error("Unable to compute storage update map.")
		return err
	}
	_, err = s.execTx(ctx, func(qtx *UploadPgQueries) (interface{}, error) {

		err = qtx.IncrementOrganizationStorage(ctx, int64(orgId), storageMap.total)
		if err != nil {
			return nil, err
		}

		err = qtx.IncrementDatasetStorage(ctx, int64(datasetId), storageMap.total)
		if err != nil {
			return nil, err
		}

		for p, value := range storageMap.packages {
			err = qtx.IncrementPackageStorage(ctx, p, value)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	if err != nil {
		log.Error("Unable to update storage for Packages, Dataset and Organization.")
	}

	log.Debug("Total storage added to dataset: ", storageMap.total)
	for p, v := range storageMap.packages {
		log.Debug(fmt.Sprintf("Package %d storage incremented by %d", p, v))
	}

	// 5. Notify SNS that files were imported — triggers the Fargate move task.
	// Skip for direct-to-storage: files are already at final location.
	if !directToStorage {
		err = s.PublishToSNS(result.files)
		if err != nil {
			contextLogger.Error("Error with notifying SNS that records are imported.", err)
		}
	}

	// 5b. Fan-out FileFinalized events to downstream consumers (scan-service, etc.).
	// Runs for both direct-to-storage and Fargate-moved files. Failure here is logged
	// but does not fail ImportFiles — the file is already committed in Postgres.
	if err := s.PublishFileFinalized(ctx, result.files, manifest); err != nil {
		contextLogger.Error("Error publishing FileFinalized events.", err)
	}

	// 6. Update activity Log
	var evnts []changelog.Event
	for _, pkg := range result.packages {
		event := changelog.Event{
			EventType: changelog.CreatePackage,
			EventDetail: changelog.PackageCreateEvent{
				Id:     pkg.Id,
				Name:   pkg.Name,
				NodeId: pkg.NodeId,
			},
			Timestamp: time.Now(),
		}
		evnts = append(evnts, event)
	}

	params := changelog.MessageParams{
		OrganizationId: int64(orgId),
		DatasetId:      int64(datasetId),
		UserId:         user.NodeId,
		Events:         evnts,
		TraceId:        manifest.ManifestId,
		Id:             uuid.NewString(),
	}

	mes := changelog.Message{
		DatasetChangelogEventJob: params,
	}

	err = s.changelogClient.EmitEvents(context.Background(), mes)
	if err != nil {
		contextLogger.Error("Error with notifying Changelog about imported records: ", err)
	}

	// 7. Publish a DeletePackageJob for every replaced predecessor so the
	// Scala jobs service can run the async S3 asset cleanup. The DB-level
	// soft-delete (state=DELETING, name prefix) + storage decrement is
	// already done by pennsieve-go-core's AddPackagesWithConflict inside the
	// import transaction; the queue publish is the side effect the library
	// leaves to us. Batched via SendMessageBatch — at 100k-file replace
	// scale this drops the SQS round-trip count ~10x vs per-message
	// SendMessage.
	var replacementJobs []DeletePackageJobParams
	for _, pkg := range result.packages {
		if !pkg.ReplacesPackageId.Valid {
			continue
		}
		replacementJobs = append(replacementJobs, DeletePackageJobParams{
			PackageId:      pkg.ReplacesPackageId.Int64,
			OrganizationId: orgId,
			UserNodeId:     user.NodeId,
			TraceId:        manifest.ManifestId,
		})
	}
	var replacementPublishFailures int
	if len(replacementJobs) > 0 {
		if err := PublishDeletePackageJobs(ctx, s.sqsClient, s.jobsQueueURL, replacementJobs); err != nil {
			replacementPublishFailures = 1
			contextLogger.WithError(err).WithField("replacement_count", len(replacementJobs)).
				Error("failed to enqueue one or more DeletePackageJob messages for replaced predecessors")
		}
		// Warn when a single ImportFiles batch generates a lot of replacements.
		// 100 is a soft threshold — no throttling, just signal so ops can watch
		// jobs_queue depth if this fires frequently. Pair with a CloudWatch
		// alarm on jobs_queue ApproximateAgeOfOldestMessage.
		if len(replacementJobs) > 100 {
			contextLogger.WithField("replacement_count", len(replacementJobs)).
				Warn("large replacement batch — monitor jobs_queue depth")
		}
		emitReplacementMetrics(len(replacementJobs), replacementPublishFailures)
	}

	// 8. Notify Pusher. Include replaced_package_id so the frontend can
	// remove the replaced row from the files table instead of rendering both
	// the old (now trashed) and new packages side by side.
	chName := strings.ReplaceAll(manifest.DatasetNodeId, "N:dataset:", "dataset-")
	var pusherData []uploadPusherItem
	for _, pkg := range result.packages {
		item := uploadPusherItem{
			UploadMessageItem: ps.UploadMessageItem{
				Name:     pkg.Name,
				NodeId:   pkg.NodeId,
				ParentId: pkg.ParentId,
				UploadId: pkg.ImportId,
			},
		}
		if pkg.ReplacesPackageId.Valid {
			item.ReplacedPackageId = &pkg.ReplacesPackageId.Int64
		}
		pusherData = append(pusherData, item)
	}
	err = s.pusherClient.Trigger(chName, "upload-event", pusherData)
	if err != nil {
		log.Warnf(err.Error())
	}

	return nil
}

// uploadPusherItem extends the shared UploadMessageItem with the
// replaced_package_id field that only the replace-on-conflict flow
// populates. Kept local to upload-service-v2 to avoid churning the
// shared pusher model for a field the rest of the platform does not
// read. Frontend treats the field as optional.
type uploadPusherItem struct {
	ps.UploadMessageItem
	ReplacedPackageId *int64 `json:"replaced_package_id,omitempty"`
}

// Handler is the primary entrypoint that handles importing files and is called by the Lambda
func (s *UploadHandlerStore) Handler(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	/*
		Messages can be from multiple upload sessions --> multiple organizations.
		We need to:
			1. Separate by manifest-session
			2. Create/get the folders from postgres for each upload session
			3. Create Packages
			4. Create Files in Packages
	*/

	// Response can include list of failed SQS messages

	var batchItemFailures []events.SQSBatchItemFailure
	response := events.SQSEventResponse{
		BatchItemFailures: batchItemFailures,
	}

	// Drop heartbeat + malformed records up front. Heartbeats are emitted
	// by the upload_lambda_heartbeat EventBridge rule (see terraform/
	// cloudwatch.tf) to keep SQS pollers and the lambda execution
	// environment warm during idle periods — they have no S3 Records
	// payload and must be ack'd without processing. The same guard also
	// hardens the Records[0] access below against any other non-S3
	// message that might reach this queue.
	liveRecords := sqsEvent.Records[:0]
	heartbeatCount := 0
	for _, m := range sqsEvent.Records {
		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(m.Body), &parsedS3Event); err != nil || len(parsedS3Event.Records) == 0 {
			heartbeatCount++
			continue
		}
		liveRecords = append(liveRecords, m)
	}
	if heartbeatCount > 0 {
		log.Debugf("Dropped %d heartbeat/non-S3 message(s) from batch", heartbeatCount)
	}
	if len(liveRecords) == 0 {
		return response, nil
	}
	sqsEvent.Records = liveRecords

	// Map SQS Events by s3Key - This is used in case of failed imports.
	s3KeySQSMessageMap := map[string]events.SQSMessage{}
	// s3KeyToOnConflict carries the OnConflict MessageAttribute (set by the
	// service lambda's finalize handler) from the SQS message to the per-
	// manifest ImportFiles call. Missing attribute = "keepBoth" for backward
	// compatibility with legacy S3-triggered uploads that predate this flag.
	s3KeyToOnConflict := map[string]string{}
	for _, m := range sqsEvent.Records {
		parsedS3Event := events.S3Event{}
		_ = json.Unmarshal([]byte(m.Body), &parsedS3Event)
		s3Key := parsedS3Event.Records[0].S3.Object.Key
		s3KeySQSMessageMap[s3Key] = m
		s3KeyToOnConflict[s3Key] = extractOnConflictAttr(m)
	}

	// 1. Parse UploadEntries
	uploadEntries, orphanEntries, err := s.GetUploadEntries(sqsEvent.Records)
	if orphanEntries != nil {
		log.Info("Files uploaded that do not follow the correct s3Key format and don't belong to manifest.")
		// These files are unexpected and do not follow the expected S3Key format for Pennsieve uploads
		err := s.deleteOrphanFiles(orphanEntries)
		if err != nil {
			log.Error("unable to delete orphan files")
		}
	}
	if err != nil {
		// This really should never happen --> Somehow the SQS queue received a non-S3 message.
		log.Error(err.Error())
		return response, err
	}

	// 2. Match against Manifest and create uploadFiles
	uploadFiles, orphanEntries, err := s.dy.GetUploadFiles(uploadEntries)
	if orphanEntries != nil {
		log.Warn("Files uploaded that don't belong to a manifest.")
		// These files somehow did parse correctly in the GetUploadEntries method.
		err := s.deleteOrphanFiles(orphanEntries)
		if err != nil {
			log.Error("Unable to delete orphan files")
		}
	}
	if err != nil {
		log.Error("Error with GetUploadFiles: ", err)
		return response, err
	}

	// 3. Map by uploadSessionID
	var fileByManifest = map[string][]uploadFile.UploadFile{}
	for _, f := range uploadFiles {
		fileByManifest[f.ManifestId] = append(fileByManifest[f.ManifestId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for manifestId, uploadFilesForManifest := range fileByManifest {

		// Get manifest from dynamodb
		manifest, err := s.dy.GetManifestById(ctx, s.tableName, manifestId)
		if err != nil {
			log.WithFields(log.Fields{
				"manifest_id": manifest.ManifestId,
				"dataset_id":  manifest.DatasetNodeId,
				"org_id":      manifest.OrganizationId,
			}).Error("GetManifestById: Unable to get manifest.", err)
			batchItemFailures = addToFailedFiles(uploadFilesForManifest, s3KeySQSMessageMap, batchItemFailures)
			continue
		}

		// Get User
		user, err := s.pg.GetUserById(ctx, manifest.UserId)
		if err != nil {
			log.WithFields(log.Fields{
				"manifest_id": manifest.ManifestId,
				"dataset_id":  manifest.DatasetNodeId,
				"org_id":      manifest.OrganizationId,
			}).Error("Unable to get user.", err)
			batchItemFailures = addToFailedFiles(uploadFilesForManifest, s3KeySQSMessageMap, batchItemFailures)
			continue
		}

		contextLogger := log.WithFields(log.Fields{
			"manifest_id": manifest.ManifestId,
			"dataset_id":  manifest.DatasetNodeId,
			"org_id":      manifest.OrganizationId,
			"user":        fmt.Sprintf("%s %s", user.FirstName, user.LastName),
		})

		err = s.WithOrg(int(manifest.OrganizationId))
		if err != nil {
			contextLogger.Error("Unable to set search path.", err)
			batchItemFailures = addToFailedFiles(uploadFilesForManifest, s3KeySQSMessageMap, batchItemFailures)
			continue
		}

		// Detect whether this manifest-batch came in via direct-to-storage
		// upload (key starts with "O...") or the legacy upload-bucket path.
		// All files in a single manifest-batch share the same origin.
		direct := len(uploadFilesForManifest) > 0 && strings.HasPrefix(uploadFilesForManifest[0].S3Key, "O")

		// Direct-to-storage goes straight to Finalized (no Fargate move).
		// Legacy goes to Imported so Fargate picks them up for the move.
		targetStatus := manifestFile.Imported
		if direct {
			targetStatus = manifestFile.Finalized
		}

		// Resolve the manifest-wide onConflict strategy from the SQS message
		// attributes. All messages for a single manifest in one batch should
		// share the same value (set at finalize time); we take the first and
		// log if anything in the group differs.
		onConflict := resolveOnConflictForManifest(uploadFilesForManifest, s3KeyToOnConflict, contextLogger)

		err = s.ImportFiles(ctx, int(manifest.DatasetId), int(manifest.OrganizationId), *user, uploadFilesForManifest, manifest, direct, onConflict)
		if err != nil {
			contextLogger.Error("Error in batch create packages: ", err)

			// Try to import each file individually and add item to batchItemFailures if it fails.
			// This will ensure that only the files that cause the failure will be returned to the sqs queue
			for _, f := range uploadFilesForManifest {
				singleFileArr := []uploadFile.UploadFile{f}
				err = s.ImportFiles(ctx, int(manifest.DatasetId), int(manifest.OrganizationId), *user, singleFileArr, manifest, direct, onConflict)
				if err != nil {
					batchItemFailures = addToFailedFiles(singleFileArr, s3KeySQSMessageMap, batchItemFailures)
					contextLogger.WithFields(
						log.Fields{
							"upload_id": f.UploadId,
						}).Error("Error when creating package: ", err)

					continue
				}

				// Update entries in manifest to target status for single file
				err = s.dy.updateManifestFileStatusTo(singleFileArr, manifestId, targetStatus)
				if err != nil {
					// Status is not correctly updated in Manifest but files are completely imported.
					// This should not return the failed files.
					contextLogger.Error("Unable to update manifest file", err)
				}

			}
			continue
		}

		// Update entries in manifest to target status for all files
		err = s.dy.updateManifestFileStatusTo(uploadFilesForManifest, manifestId, targetStatus)
		if err != nil {
			// Status is not correctly updated in Manifest but files are completely imported.
			// This should not return the failed files.
			contextLogger.Error(err)
			continue
		}

		// Update Dataset updated_at value.
		err = s.pg.SetUpdatedAt(ctx, manifest.DatasetId, time.Now())
		if err != nil {
			contextLogger.Error(err)
			continue
		}
	}

	response.BatchItemFailures = batchItemFailures
	return response, nil
}

// deleteOrphanFiles deletes files from upload bucket if no representation exists in manifest.
//
// Safety guard: a key starting with "/" is almost certainly a buggy caller
// (legitimate keys are {manifestId}/{uploadId}), so we refuse to delete it.
// Dropping bytes on the floor because of a client-side bug that emits a bad
// key is exactly the kind of data loss that's hardest to recover from — we'd
// rather leave the object alone, log loudly, and let reconciliation decide.
func (s *UploadHandlerStore) deleteOrphanFiles(files []OrphanS3File) error {

	ctx := context.Background()

	// Assert all buckets are the same
	s3Bucket := files[0].S3Bucket

	var keys []s3Types.ObjectIdentifier
	for _, f := range files {
		if strings.HasPrefix(f.S3Key, "/") {
			log.WithFields(log.Fields{
				"service":   "Upload-service",
				"s3_bucket": f.S3Bucket,
				"s3_key":    f.S3Key,
			}).Error("Refusing to delete suspicious orphan with leading-slash key; likely a client bug — leaving object in place for manual review")
			continue
		}
		log.WithFields(log.Fields{
			"service": "Upload-service",
		}).Warn(fmt.Sprintf("Deleting file %s/%s", f.S3Bucket, f.S3Key))

		if f.S3Bucket != s3Bucket {
			return errors.New("not all orphan files have the same bucket")
		}
		keys = append(keys, s3Types.ObjectIdentifier{
			Key:       aws.String(f.S3Key),
			VersionId: nil,
		})
	}

	if len(keys) == 0 {
		return nil
	}

	f := s3Types.Delete{
		Objects: keys,
		Quiet:   aws.Bool(false),
	}

	params := s3.DeleteObjectsInput{
		Bucket: aws.String(s3Bucket),
		Delete: &f,
	}

	result, err := s.S3Client.DeleteObjects(ctx, &params)
	if err != nil {
		return err
	}
	if len(result.Deleted) != len(files) {
		return errors.New("unable to delete all orphan files")
	}

	return nil

}

// createStorageUpdateMap returns object with information on how to update storage for packages, dataset, and org.
func (s *UploadHandlerStore) createStorageUpdateMap(ctx context.Context, pf PackagesAndFiles) (*storageUpdateParams, error) {

	storageMap := storageUpdateParams{
		total:    0,
		packages: map[int64]int64{},
	}

	for _, curFile := range pf.files {

		parentIds, err := s.pg.GetPackageAncestorIds(ctx, int64(curFile.PackageId))
		if err != nil {
			return nil, err
		}

		// Add to individual packages in map
		for _, p := range parentIds {
			storageMap.packages[p] += curFile.Size
		}

		// Add to total storage for Dataset and Organization
		storageMap.total += curFile.Size
	}

	return &storageMap, nil

}

func trimSlashes(path string) string {
	// Remove leading and trailing "/"
	leadingSlashes := regexp.MustCompile(`^/+`)
	trimmed := leadingSlashes.ReplaceAllString(path, "")

	trailingSlashes := regexp.MustCompile(`/+$`)
	trimmed = trailingSlashes.ReplaceAllString(trimmed, "")
	return trimmed
}

// getUploadFolderMap returns an object that maps path name to Folder object.
func getUploadFolderMap(sortedFiles []uploadFile.UploadFile, targetFolder string) uploadFolder.UploadFolderMap {

	// Mapping path from targetFolder to UploadFolder Object
	var folderNameMap = map[string]*uploadFolder.UploadFolder{}

	// Iterate over the files and create the UploadFolder objects.
	for _, f := range sortedFiles {
		p := f.Path

		if p == "" {
			continue
		}

		// Prepend the target-Folder if it exists
		if targetFolder != "" {
			p = strings.Join(
				[]string{
					targetFolder, p,
				}, "/")
		}

		// Iterate over path segments in a single file and identify folders.
		pathSegments := strings.Split(p, "/")
		absoluteSegment := "" // Current location in the path walker for current file.
		currentNodeId := ""
		currentFolderPath := ""
		for depth, segment := range pathSegments {

			parentNodeId := currentNodeId
			parentFolderPath := currentFolderPath

			// If depth > 0 ==> prepend the previous absoluteSegment to the current path name.
			if depth > 0 {
				absoluteSegment = strings.Join(
					[]string{

						absoluteSegment,
						segment,
					}, "/")
			} else {
				absoluteSegment = segment
			}

			// If folder already exists in map, add current folder as a child to the parent
			// folder (which will exist too at this point). If not, create new folder to the map and add to parent folder.

			folder, ok := folderNameMap[absoluteSegment]
			if ok {
				currentNodeId = folder.NodeId
				currentFolderPath = absoluteSegment

			} else {
				currentNodeId = fmt.Sprintf("N:collection:%s", uuid.New().String())
				currentFolderPath = absoluteSegment

				folder = &uploadFolder.UploadFolder{
					NodeId:       currentNodeId,
					Name:         segment,
					ParentNodeId: parentNodeId,
					ParentId:     -1,
					Depth:        depth,
				}
				folderNameMap[absoluteSegment] = folder
			}

			// Add current segment to parent if exist
			if parentFolderPath != "" {
				folderNameMap[parentFolderPath].Children = append(folderNameMap[parentFolderPath].Children, folder)
			}

		}
	}

	return folderNameMap
}

// getPackageParams returns an array of PackageParams to insert in the Packages Table.
func getPackageParams(datasetId int, ownerId int, uploadFiles []uploadFile.UploadFile, pathToFolderMap pgdb.PackageMap) ([]pgdb.PackageParams, error) {
	var pkgParams []pgdb.PackageParams

	// First create a map of params. As there can be upload-files that should be mapped to the same package,
	// we want to ensure we are not creating duplicate packages (as this will cause an error when inserting in db).
	// Then we turn map into array and return the array.
	pkgParamsMap := map[string]pgdb.PackageParams{}
	for _, file := range uploadFiles {

		// Create the packageID based on the uploadID or the mergePackageID if it exists
		packageId, packageName, err := parsePackageId(file)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		parentId := int64(-1)
		if file.Path != "" {
			parentId = pathToFolderMap[file.Path].Id
		}

		uploadId := sql.NullString{
			String: file.UploadId,
			Valid:  true,
		}

		// Set Default attributes for File ==> Subtype and Icon
		var attributes []packageInfo.PackageAttribute
		attributes = append(attributes, packageInfo.PackageAttribute{
			Key:      "subtype",
			Fixed:    false,
			Value:    file.SubType,
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		}, packageInfo.PackageAttribute{
			Key:      "icon",
			Fixed:    false,
			Value:    file.Icon.String(),
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		})

		// Select Package State
		// UPLOADED if there is a workflow associated with the package type
		// READY is there is no workflow associated with the package type
		setPackageState := packageState.Uploaded
		if !packageType.FileTypeToInfoDict[file.FileType].HasWorkflow {
			setPackageState = packageState.Ready
		}

		pkgParam := pgdb.PackageParams{
			Name:         packageName,
			PackageType:  file.Type,
			PackageState: setPackageState,
			NodeId:       packageId,
			ParentId:     parentId,
			DatasetId:    datasetId,
			OwnerId:      ownerId,
			Size:         0,
			ImportId:     uploadId,
			Attributes:   attributes,
		}

		pkgParamsMap[packageId] = pkgParam
		//// If entry already exists --> sum size, else assign value
		//if val, ok := pkgParamsMap[packageId]; ok {
		//	val.Size += pkgParam.Size
		//} else {
		//	pkgParamsMap[packageId] = pkgParam
		//}

	}

	// Turn map into array --> ensure no duplicate packages.
	for i := range pkgParamsMap {
		pkgParams = append(pkgParams, pkgParamsMap[i])
	}

	return pkgParams, nil

}

// parsePackageId returns a packageId and name based on upload-file
func parsePackageId(file uploadFile.UploadFile) (string, string, error) {
	packageId := fmt.Sprintf("N:package:%s", file.UploadId)
	packageName := file.Name
	if len(file.MergePackageId) > 0 {
		packageId = fmt.Sprintf("N:package:%s", file.MergePackageId)

		// Set packageName to name without extension
		r := regexp.MustCompile(`(?P<FileName>[^.]*)?\.?(?P<Extension>.*)`)
		pathParts := r.FindStringSubmatch(file.Name)
		if pathParts == nil {
			log.Error("Unable to parse filename:", file.Name)
			return "", "", errors.New(fmt.Sprintf("Unable to parse filename: %s", file.Name))
		}

		packageName = pathParts[r.SubexpIndex("FileName")]
	}

	return packageId, packageName, nil
}

// addToFailedFiles appends array of upload files to failed SQS messages
func addToFailedFiles(files []uploadFile.UploadFile, s3KeySQSMessageMap map[string]events.SQSMessage,
	failures []events.SQSBatchItemFailure) []events.SQSBatchItemFailure {
	for _, f := range files {
		failedMessage := events.SQSBatchItemFailure{
			ItemIdentifier: s3KeySQSMessageMap[f.S3Key].MessageId}

		failures = append(failures, failedMessage)

	}
	return failures
}

// extractOnConflictAttr returns the OnConflict MessageAttribute from an SQS
// message, or "keepBoth" when the attribute is missing or empty. Missing
// attribute is the backward-compatible default: legacy S3-triggered uploads
// and pre-v1.16 finalize messages never set this attribute.
func extractOnConflictAttr(m events.SQSMessage) string {
	attr, ok := m.MessageAttributes["OnConflict"]
	if !ok {
		return "keepBoth"
	}
	if attr.StringValue == nil || *attr.StringValue == "" {
		return "keepBoth"
	}
	return *attr.StringValue
}

// resolveOnConflictForManifest picks the onConflict value for a manifest's
// file group. Normally every message in the group carries the same value
// (set at finalize time); we take the first file's value and log a warning
// if a different value shows up within the group. That's defensive against
// a client that somehow issues two concurrent finalize calls for the same
// manifest with different conflict strategies — unlikely, but we don't want
// a silent divergence if it happens.
func resolveOnConflictForManifest(files []uploadFile.UploadFile, s3KeyToOnConflict map[string]string, logger *log.Entry) string {
	if len(files) == 0 {
		return "keepBoth"
	}
	chosen := s3KeyToOnConflict[files[0].S3Key]
	if chosen == "" {
		chosen = "keepBoth"
	}
	for _, f := range files[1:] {
		if v, ok := s3KeyToOnConflict[f.S3Key]; ok && v != "" && v != chosen {
			logger.WithFields(log.Fields{
				"chosen": chosen,
				"other":  v,
			}).Warn("mixed onConflict values within manifest batch; using first-observed value")
			break
		}
	}
	return chosen
}

// conflictStrategyFromAttr maps the SQS MessageAttribute string to the typed
// conflictStrategy.Strategy consumed by pennsieve-go-core. Unknown values
// fall back to KeepBoth (legacy behavior) to keep older clients working.
func conflictStrategyFromAttr(s string) conflictStrategy.Strategy {
	switch s {
	case "replace":
		return conflictStrategy.Replace
	default:
		return conflictStrategy.KeepBoth
	}
}
