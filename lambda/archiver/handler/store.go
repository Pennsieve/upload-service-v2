package handler

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	dydbModels "github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

// ArchiverStore provides the Queries interface.
type ArchiverStore struct {
	dy            *ServiceDyQueries
	dynamodb      *dynamodb.Client
	s3Client      *s3.Client
	fileTableName string
	tableName     string
}

// NewArchiverStore returns a ArchiverStore object which implements the Queires
func NewArchiverStore(dy *dynamodb.Client, s3Client *s3.Client, fileTableName string, tableName string) *ArchiverStore {
	return &ArchiverStore{
		dynamodb:      dy,
		s3Client:      s3Client,
		dy:            NewServiceDyQueries(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}

// writeCSVFile writes manifestFiles to a CSV file in the /tmp/ folder.
// Any error from GetFilesPaginated is returned to the caller — a throttle
// or other query failure must abort the archive, not produce a 0-byte CSV
// that then gets paired with a row-delete (silent data loss).
func (s *ArchiverStore) writeCSVFile(ctx context.Context, fileName string, manifestId string) (string, error) {
	file, err := os.Create(fmt.Sprintf("/tmp/%s", fileName))
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
			}).Error("unable to create archive CSV file")
		return "", err
	}
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	pageSize := int32(200)

	files, lastEntry, err := s.dy.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, pageSize, nil)
	if err != nil {
		return file.Name(), fmt.Errorf("GetFilesPaginated: %w", err)
	}

	if len(files) == 0 {
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
			}).Info("Archived manifest has no files.")
		return file.Name(), nil
	}

	// Write headers from the first file's schema.
	if err := w.Write(files[0].GetHeaders()); err != nil {
		return file.Name(), err
	}

	for _, f := range files {
		if err := w.Write(f.ToSlice()); err != nil {
			return file.Name(), err
		}
	}

	for len(lastEntry) != 0 {
		files, lastEntry, err = s.dy.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, pageSize, lastEntry)
		if err != nil {
			return file.Name(), fmt.Errorf("GetFilesPaginated page: %w", err)
		}
		for _, f := range files {
			if err := w.Write(f.ToSlice()); err != nil {
				return file.Name(), err
			}
		}
	}

	return file.Name(), nil
}

// writeManifestToS3 takes a CSV file and stores it in the manifest-archive bucket
func (s *ArchiverStore) writeManifestToS3(ctx context.Context, fileName string, organizationId int64, datasetId int64) (string, error) {

	archiverS3Key := fmt.Sprintf("O%d/D%d/%s", organizationId, datasetId, fileName)
	filePath := fmt.Sprintf("/tmp/%s", fileName)
	// open file for reading
	uploadFile, err := os.Open(filePath)
	if err != nil {
		return "", err
	}

	uploader := manager.NewUploader(store.s3Client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(archiverBucket),
		Key:         aws.String(archiverS3Key),
		ContentType: aws.String("text/csv"),
		Body:        uploadFile,
	})
	if err != nil {
		return "", err
	}

	// Remove the original CSV file in the lambda
	err = os.Remove(filePath)
	if err != nil {
		log.Warn(fmt.Sprintf("Could not remove the local CSV file: %s", filePath))
	}

	return archiverS3Key, nil
}

// removeManifestFiles removes all manifestFile entries in the manifestFileTable
// for a particular manifest. Returns an error if any page fails to delete after
// retries — the caller MUST propagate this so AWS Lambda's async-retry policy
// can re-attempt. Previously the inner error was logged-and-swallowed, which
// produced silent orphans (Status=Archived manifest with file rows still
// present) whenever DynamoDB throttling outlasted the SDK's transparent
// retries during bulk archival runs.
func (s *ArchiverStore) removeManifestFiles(ctx context.Context, manifestId string) error {
	var startKey map[string]types.AttributeValue

	for {
		files, next, err := s.dy.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, 25, startKey)
		if err != nil {
			return err
		}

		if len(files) > 0 {
			if err := removeFilesFromManifest(ctx, files, s.fileTableName, manifestId); err != nil {
				log.WithFields(log.Fields{"manifest_id": manifestId}).
					Error("Error removing files from manifest: ", err)
				return err
			}
		}

		if len(next) == 0 {
			return nil
		}
		startKey = next
	}
}

// removeFilesFromManifest removes file-rows from the dynamodb manifest-file-table.
// Retries up to nrRetries times on transient BatchWriteItem failures (e.g.
// ProvisionedThroughputExceededException after the SDK's own retries are
// exhausted) AND on UnprocessedItems, with exponential backoff. Returns an
// error if rows remain after all retries — the caller propagates this so
// AWS Lambda's async-retry can re-attempt instead of silently leaving the
// manifest's file rows behind a Status=Archived flag.
//
// Two prior bugs lived here:
//   - The initial BatchWriteItem call returned its error directly, bypassing
//     the inner UnprocessedItems retry buffer. A hard throttle on the first
//     call was therefore unrecoverable from within the lambda.
//   - The inner retry's error path used Fatalln (os.Exit(1)) which terminated
//     the lambda runtime instead of returning a recoverable error.
func removeFilesFromManifest(ctx context.Context, files []dydbModels.ManifestFileTable, fileTableName string, manifestId string) error {
	var writeRequests []types.WriteRequest
	for _, f := range files {
		data, err := attributevalue.MarshalMap(dydbModels.ManifestFilePrimaryKey{
			ManifestId: manifestId,
			UploadId:   f.UploadId,
		})
		if err != nil {
			return err
		}
		writeRequests = append(writeRequests, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{Key: data},
		})
	}

	const nrRetries = 5
	unProcessed := map[string][]types.WriteRequest{fileTableName: writeRequests}

	for retryIndex := 0; len(unProcessed[fileTableName]) > 0; retryIndex++ {
		if retryIndex == nrRetries {
			return fmt.Errorf("manifest %s: %d rows still unprocessed after %d retries",
				manifestId, len(unProcessed[fileTableName]), nrRetries)
		}
		if retryIndex > 0 {
			time.Sleep(time.Duration(200*(1+retryIndex)) * time.Millisecond)
		}

		data, err := store.dynamodb.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems:                unProcessed,
			ReturnConsumedCapacity:      "NONE",
			ReturnItemCollectionMetrics: "NONE",
		})
		if err != nil {
			log.WithFields(log.Fields{"manifest_id": manifestId, "retry": retryIndex}).
				Warn("BatchWriteItem failed, will retry: ", err)
			continue // keep unProcessed unchanged; retry the whole batch
		}
		if len(data.UnprocessedItems[fileTableName]) == 0 {
			return nil
		}
		unProcessed = data.UnprocessedItems
	}

	return nil
}
