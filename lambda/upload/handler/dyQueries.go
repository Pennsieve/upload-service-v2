package handler

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamoTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	log "github.com/sirupsen/logrus"
	"regexp"
)

// UploadDyQueries is the UploadHandler Queries Struct embedding the shared Queries struct
type UploadDyQueries struct {
	*dyQueries.Queries
	db dyQueries.DB
}

// NewUploadDyQueries returns a new instance of an UploadPgQueries object
func NewUploadDyQueries(db dyQueries.DB) *UploadDyQueries {
	q := dyQueries.New(db)
	return &UploadDyQueries{
		q,
		db,
	}
}

// GetUploadFiles returns a set of UploadFiles from a set of UploadEntries by verifying against DynamoDB
func (q *UploadDyQueries) GetUploadFiles(entries []UploadEntry) ([]uploadFile.UploadFile, error) {

	// 1. Check all standard uploadEntities against dynamodb
	var getItems []map[string]dynamoTypes.AttributeValue
	entryMap := map[string]UploadEntry{}
	for _, item := range entries {
		entryMap[item.UploadId] = item

		data, err := attributevalue.MarshalMap(dydb.ManifestFilePrimaryKey{
			ManifestId: item.ManifestId,
			UploadId:   item.UploadId,
		})
		if err != nil {
			log.Fatalf("MarshalMap: %v\n", err)
		}
		getItems = append(getItems, data)
	}

	var verifiedFiles []UploadEntry
	if len(getItems) > 0 {

		keysAndAttributes := dynamoTypes.KeysAndAttributes{
			Keys:                     getItems,
			AttributesToGet:          nil,
			ConsistentRead:           nil,
			ExpressionAttributeNames: nil,
			ProjectionExpression:     nil,
		}

		getTableItems := map[string]dynamoTypes.KeysAndAttributes{
			ManifestFileTableName: keysAndAttributes,
		}

		batchItemInput := dynamodb.BatchGetItemInput{
			RequestItems:           getTableItems,
			ReturnConsumedCapacity: "",
		}

		dbResults, err := q.db.BatchGetItem(context.Background(), &batchItemInput)
		if err != nil {
			log.Fatalln("Unable to get dbItems.")
		}

		dbItems := dbResults.Responses[ManifestFileTableName]

		for _, dbItem := range dbItems {
			fileEntry := dydb.ManifestFileTable{}
			err := attributevalue.UnmarshalMap(dbItem, &fileEntry)
			if err != nil {
				log.Error("Unable to UnMarshall unprocessed items. ", err)
				return nil, err
			}

			// Match with original upload entry from SQS queue
			inputUploadEntry := entryMap[fileEntry.UploadId]

			r := regexp.MustCompile(`(?P<FileName>[^\.]*)?\.?(?P<Extension>.*)`)
			pathParts := r.FindStringSubmatch(fileEntry.FileName)
			if pathParts == nil {
				// File does not contain the required s3key components
				return nil, errors.New(fmt.Sprintf("File path does not contain the required s3key components: %s",
					fileEntry.FilePath))
			}

			verifiedFiles = append(verifiedFiles, UploadEntry{
				ManifestId:     fileEntry.ManifestId,
				UploadId:       fileEntry.UploadId,
				S3Bucket:       inputUploadEntry.S3Bucket,
				S3Key:          inputUploadEntry.S3Key,
				Size:           inputUploadEntry.Size,
				Path:           fileEntry.FilePath,
				Name:           fileEntry.FileName,
				Extension:      pathParts[r.SubexpIndex("Extension")],
				ETag:           inputUploadEntry.ETag,
				MergePackageId: fileEntry.MergePackageId,
				FileType:       fileEntry.FileType,
				Sha256:         inputUploadEntry.Sha256,
			})

			log.WithFields(
				log.Fields{
					"manifest_id": fileEntry.ManifestId,
					"upload_id":   fileEntry.UploadId,
				},
			).Debug("GetUploadFiles: uploadID ", fileEntry.UploadId)
		}
	}

	if len(verifiedFiles) != len(entries) {
		log.Error("MISMATCH BETWEEN UPLOADED ENTRIES AND RETURN FROM DYNAMO-BD.")
	}

	var uploadFiles []uploadFile.UploadFile
	for _, f := range verifiedFiles {

		// Get FileInfo from fileType string in verified file.
		fType, pInfo := getFileInfo(f.FileType)

		file := uploadFile.UploadFile{
			ManifestId:     f.ManifestId,
			UploadId:       f.UploadId,
			FileType:       fType,
			S3Bucket:       f.S3Bucket,
			S3Key:          f.S3Key,
			Path:           f.Path,
			Name:           f.Name,
			Extension:      f.Extension,
			Type:           pInfo.PackageType,
			SubType:        pInfo.PackageSubType,
			Icon:           pInfo.Icon,
			Size:           f.Size,
			ETag:           f.ETag,
			MergePackageId: f.MergePackageId,
			Sha256:         f.Sha256,
		}

		log.WithFields(
			log.Fields{
				"manifest_id": file.ManifestId,
				"upload_id":   file.UploadId,
			},
		).Debug("uploadFile: ", file.Name, " || merge: ", file.MergePackageId)

		uploadFiles = append(uploadFiles, file)
	}

	return uploadFiles, nil

	// TODO handle (delete) non-compliant uploads.
	// Currently non-compliant uploads are ignored and will remain in uploads folder.

}

// getFileInfo returns a FileType and PackageType.Info object based on filetype string.
func getFileInfo(fileTypeStr string) (fileType.Type, packageType.Info) {

	fType := fileType.Dict[fileTypeStr]

	pType, exists := packageType.FileTypeToInfoDict[fType]
	if !exists {
		log.Warn("Unmatched filetype. ?!?:", fType)
		pType = packageType.FileTypeToInfoDict[fileType.GenericData]
	}

	return fType, pType
}
