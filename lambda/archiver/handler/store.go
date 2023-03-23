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
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

// ArchiverStore provides the Queries interface.
type ArchiverStore struct {
	*dydb.Queries
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
		Queries:       dydb.New(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}

// writeCSVFile writes manifestFiles to a CSV file in the /tmp/ folder
func (s *ArchiverStore) writeCSVFile(ctx context.Context, fileName string, manifestId string) (string, error) {
	file, err := os.Create(fmt.Sprintf("/tmp/%s", fileName))
	defer file.Close()
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
			}).Error("unable to create archive CSV file")
		return file.Name(), err

	}

	w := csv.NewWriter(file)
	defer w.Flush()

	var files []dydbModels.ManifestFileTable
	pageSize := int32(200)

	files, lastEntry, err := s.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, pageSize, nil)

	if len(files) > 0 {

		// Write Headers
		firstFile := files[0]
		headers := firstFile.GetHeaders()
		err = w.Write(headers)
		if err != nil {
			return file.Name(), err
		}

		// Write Files
		for _, f := range files {
			rowSlice := f.ToSlice()
			err := w.Write(rowSlice)
			if err != nil {
				return file.Name(), err
			}
		}

		// If there are more entries, get next page and write files.
		for len(lastEntry) != 0 {
			files, lastEntry, err = s.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, pageSize, lastEntry)

			for _, f := range files {
				rowSlice := f.ToSlice()
				err := w.Write(rowSlice)
				if err != nil {
					return file.Name(), err
				}
			}
		}
	} else {
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
			}).Info("Archived manifest has no files.")
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

// removeManifestFiles removes all manifestFile entries in the manifestFileTable for a particular manifest
func (s *ArchiverStore) removeManifestFiles(ctx context.Context, manifestId string) error {

	var err error
	var files []dydbModels.ManifestFileTable
	var startKey map[string]types.AttributeValue
	startKey = nil

	// Get First Page
	files, startKey, err = s.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, 25, startKey)
	if err != nil {
		return err
	}

	removeFilesFromManifest(ctx, files, s.fileTableName, manifestId)

	for len(startKey) != 0 {
		files, startKey, err = s.GetFilesPaginated(ctx, s.fileTableName, manifestId, sql.NullString{Valid: false}, 25, startKey)
		if err != nil {
			return err
		}

		removeFilesFromManifest(ctx, files, s.fileTableName, manifestId)
	}

	return nil

}

// removeFilesFromManifest removes file-rows from the dynamodb manifest-file-table
func removeFilesFromManifest(ctx context.Context, files []dydbModels.ManifestFileTable, fileTableName string, manifestId string) error {
	var writeRequests []types.WriteRequest

	// Iterate over file-rows and create delete request
	for _, f := range files {
		data, err := attributevalue.MarshalMap(dydbModels.ManifestFilePrimaryKey{
			ManifestId: manifestId,
			UploadId:   f.UploadId,
		})
		if err != nil {
			return err
		}

		request := types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: data,
			},
		}

		writeRequests = append(writeRequests, request)
	}

	// Create RequestItems for DynamoDB with all deleteRequests
	requestItems := map[string][]types.WriteRequest{
		fileTableName: writeRequests,
	}

	params := dynamodb.BatchWriteItemInput{
		RequestItems:                requestItems,
		ReturnConsumedCapacity:      "NONE",
		ReturnItemCollectionMetrics: "NONE",
	}

	// Write files to upload file dynamodb table
	data, err := store.dynamodb.BatchWriteItem(ctx, &params)
	if err != nil {
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
			},
		).Error("Unable to Batch Delete: ", err)
		return err
	}

	// Support retries in case delete does not delete all rows.
	nrRetries := 5
	retryIndex := 0
	unProcessedItems := data.UnprocessedItems
	for len(unProcessedItems) > 0 {
		params = dynamodb.BatchWriteItemInput{
			RequestItems:                unProcessedItems,
			ReturnConsumedCapacity:      "NONE",
			ReturnItemCollectionMetrics: "NONE",
		}

		data, err = store.dynamodb.BatchWriteItem(context.Background(), &params)
		if err != nil {
			log.WithFields(
				log.Fields{
					"manifest_id": manifestId,
				},
			).Fatalln("Unable to Batch Write: ", err)
		}

		unProcessedItems = data.UnprocessedItems

		retryIndex++
		if retryIndex == nrRetries {
			log.WithFields(
				log.Fields{
					"manifest_id": manifestId,
				},
			).Warn("Dynamodb did not delete all files associated with the manifest.")
			break
		}
		time.Sleep(time.Duration(200*(1+retryIndex)) * time.Millisecond)

	}

	return nil
}
