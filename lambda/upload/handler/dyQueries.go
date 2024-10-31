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
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"regexp"
	"time"
)

// UploadDyQueries is the UploadHandler Queries Struct embedding the shared Queries struct
type UploadDyQueries struct {
	*dyQueries.Queries
	db dyQueries.DB
}

type OrphanS3File struct {
	S3Bucket string
	S3Key    string
	ETag     string
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
func (q *UploadDyQueries) GetUploadFiles(entries []UploadEntry) ([]uploadFile.UploadFile, []OrphanS3File, error) {

	var unexpectedEntries []OrphanS3File

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
			log.Error(fmt.Sprintf("Unable to get dbItems: %v", err))
			return nil, nil, err
		}
		dbItems := dbResults.Responses[ManifestFileTableName]

		// Re-request missing items if for some reason dynamodb does not return all events
		retryCount := 1
		for len(dbResults.UnprocessedKeys) > 0 {
			exponentialWaitWithJitter(retryCount)
			dbResults, err = q.db.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: dbResults.UnprocessedKeys,
			})
			if err != nil {
				log.Error(fmt.Sprintf("Unable to get dbItems: %v", err))
				return nil, nil, err
			}
			dbItems = append(dbItems, dbResults.Responses[ManifestFileTableName]...)
			retryCount++
		}

		for _, dbItem := range dbItems {
			fileEntry := dydb.ManifestFileTable{}
			err := attributevalue.UnmarshalMap(dbItem, &fileEntry)
			if err != nil {
				log.Error("Unable to UnMarshall unprocessed items. ", err)
				return nil, nil, err
			}

			// Match with original upload entry from SQS queue
			inputUploadEntry := entryMap[fileEntry.UploadId]

			r := regexp.MustCompile(`(?P<FileName>[^.]*)?\.?(?P<Extension>.*)`)
			pathParts := r.FindStringSubmatch(fileEntry.FileName)
			if pathParts == nil {
				// File does not contain the required s3key components
				return nil, nil, errors.New(fmt.Sprintf("File path does not contain the required s3key components: %s",
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

		// create verified fileMap
		var verifiedFileIds []string
		for _, e := range verifiedFiles {
			verifiedFileIds = append(verifiedFileIds, e.UploadId)
		}

		// Add to unexpected entries if not returned
		for _, e := range entries {
			if !contains(verifiedFileIds, e.UploadId) {
				f := OrphanS3File{
					S3Bucket: e.S3Bucket,
					S3Key:    e.S3Key,
					ETag:     e.ETag,
				}

				unexpectedEntries = append(unexpectedEntries, f)
			}
		}

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

	return uploadFiles, unexpectedEntries, nil
}

//// checkUpdateManifestStatus checks current status of Manifest and updates if necessary.
//func (q *UploadDyQueries) checkUpdateManifestStatus(ctx context.Context, manifest *dydb.ManifestTable) (manifestModels.Status, error) {
//
//	// Check if there are any remaining items for manifest and
//	// set manifest status if not
//	reqStatus := sql.NullString{
//		String: "InProgress",
//		Valid:  true,
//	}
//
//	setStatus := manifestModels.Initiated
//
//	remaining, _, err := q.GetFilesPaginated(ctx, ManifestFileTableName,
//		manifest.ManifestId, reqStatus, 1, nil)
//	if err != nil {
//		return setStatus, err
//	}
//
//	if len(remaining) == 0 {
//		setStatus = manifestModels.Completed
//		err = q.UpdateManifestStatus(ctx, ManifestTableName, manifest.ManifestId, setStatus)
//		if err != nil {
//			return setStatus, err
//		}
//	} else if manifest.Status == "Completed" {
//		setStatus = manifestModels.Uploading
//		err = q.UpdateManifestStatus(ctx, ManifestTableName, manifest.ManifestId, setStatus)
//		if err != nil {
//			return setStatus, err
//		}
//	}
//
//	return setStatus, nil
//
//}

// updateManifest updates the manifestFiles to IMPORTED status and updates other fields.
func (q *UploadDyQueries) updateManifestFileStatus(uploadFilesForManifest []uploadFile.UploadFile, manifestId string) error {

	// Update status of files in dynamoDB
	var fileDTOs []manifestFile.FileDTO
	for _, u := range uploadFilesForManifest {
		f := manifestFile.FileDTO{
			UploadID:       u.UploadId,
			S3Key:          u.S3Key,
			TargetPath:     u.Path,
			TargetName:     u.Name,
			Status:         manifestFile.Imported,
			MergePackageId: u.MergePackageId,
			FileType:       u.FileType.String(),
		}
		fileDTOs = append(fileDTOs, f)
	}

	// We are replacing the entries instead of updating the status field as
	// this is the only way we can batch update, and we also update the name in
	// case that we need to append index (on name conflict).
	setStatus := manifestFile.Imported
	stats, err := q.SyncFiles(manifestId, fileDTOs, &setStatus, ManifestTableName, ManifestFileTableName)
	if err != nil {
		return err
	}

	if stats.NrFilesUpdated != len(uploadFilesForManifest) {
		return errors.New("could not update status for manifest files to IMPORTED")
	}

	return nil

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

// https://play.golang.org/p/Qg_uv_inCek
// contains checks if a string is present in a slice
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

var maxRetryWaitMs = time.Minute.Milliseconds()

func exponentialWaitWithJitter(retryCount int) {
	waitMs := int64(100 * (1 << retryCount))
	if waitMs > maxRetryWaitMs {
		waitMs = maxRetryWaitMs
	}
	waitDuration := time.Duration(rand.Int63n(waitMs)) * time.Millisecond
	time.Sleep(waitDuration)
}
