package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/fileType"
	manfestModels "github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	manifestPkg "github.com/pennsieve/pennsieve-go-core/pkg/upload"
	log "github.com/sirupsen/logrus"
	"os"
	"regexp"
)

var manifestSession manifestPkg.ManifestSession

// init runs on cold start of lambda and gets jwt keysets from Cognito user pools.
func init() {

	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	manifestSession = manifestPkg.ManifestSession{
		FileTableName: os.Getenv("MANIFEST_FILE_TABLE"),
		TableName:     os.Getenv("MANIFEST_TABLE"),
		Client:        dynamodb.NewFromConfig(cfg),
		SNSClient:     sns.NewFromConfig(cfg),
		SNSTopic:      os.Getenv("IMPORTED_SNS_TOPIC"),
		S3Client:      s3.NewFromConfig(cfg),
	}
}

// Handler implements the function that is called when new SQS Events arrive.
func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {

	/*
		Messages can be from multiple upload sessions --> multiple organizations.
		We need to:
			1. Separate by manifest-session
			2. Create/get the folders from postgres for each upload session
			3. Create Packages
			4. Create Files in Packages
	*/

	// 1. Parse UploadEntries
	uploadEntries, err := GetUploadEntries(sqsEvent.Records)
	if err != nil {
		// This really should never happen.
		log.Fatalf(err.Error())
	}

	// 2. Match against Manifest and create uploadFiles
	uploadFiles, err := GetUploadFiles(uploadEntries)
	if err != nil {
		log.Error("Error with GetUploadFiles: ", err)
	}

	// 3. Map by uploadSessionID
	var fileByManifest = map[string][]uploadFile.UploadFile{}
	for _, f := range uploadFiles {
		fileByManifest[f.ManifestId] = append(fileByManifest[f.ManifestId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for manifestId, uploadFilesForManifest := range fileByManifest {
		var s UploadSession

		// Get manifest from dynamodb
		var m *dbTable.ManifestTable
		var mf *dbTable.ManifestFileTable
		manifest, err := m.GetFromManifest(manifestSession.Client, manifestSession.TableName, manifestId)

		// Create upload session (with DB access) and import files
		session, err := s.CreateUploadSession(manifest)
		if err != nil {
			log.Error("Unable to create upload session.", err)
			continue
		}
		err = session.ImportFiles(uploadFilesForManifest, manifest)

		if err != nil {
			log.Error("Unable to create packages: ", err)
			continue
		}

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
		manifestSession.AddFiles(manifestId, fileDTOs, &setStatus)

		// Check if there are any remaining items for manifest and
		// set manifest status if not
		reqStatus := sql.NullString{
			String: "InProgress",
			Valid:  true,
		}
		remaining, _, err := mf.GetFilesPaginated(manifestSession.Client, manifestSession.TableName,
			manifestId, reqStatus, 1, nil)
		if len(remaining) == 0 {
			m.UpdateManifestStatus(manifestSession.Client, manifestSession.TableName, manifestId, manfestModels.Completed)
		} else if manifest.Status == "Completed" {
			m.UpdateManifestStatus(manifestSession.Client, manifestSession.TableName, manifestId, manfestModels.Uploading)
		}

	}

	return nil
}

// GetUploadEntries parses the events from SQS into meaningful objects
func GetUploadEntries(fileEvents []events.SQSMessage) ([]uploadEntry, error) {

	var entries []uploadEntry
	for _, message := range fileEvents {
		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(message.Body), &parsedS3Event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message, %v", err)
		}

		entry, err := uploadEntryFromS3Event(&parsedS3Event)
		if err != nil {
			log.Error("Unable to parse s3-key: ", err)
			continue
		}

		entries = append(entries, *entry)

	}

	return entries, nil
}

// GetUploadFiles returns a set of UploadFiles from a set of UploadEntries by verifying against
func GetUploadFiles(entries []uploadEntry) ([]uploadFile.UploadFile, error) {

	// 1. Check all standard uploadEntities against dynamodb
	var getItems []map[string]types.AttributeValue
	entryMap := map[string]uploadEntry{}
	for _, item := range entries {
		entryMap[item.uploadId] = item

		data, err := attributevalue.MarshalMap(dbTable.ManifestFilePrimaryKey{
			ManifestId: item.manifestId,
			UploadId:   item.uploadId,
		})
		if err != nil {
			log.Fatalf("MarshalMap: %v\n", err)
		}
		getItems = append(getItems, data)

	}

	var verifiedFiles []uploadEntry
	if len(getItems) > 0 {

		keysAndAttributes := types.KeysAndAttributes{
			Keys:                     getItems,
			AttributesToGet:          nil,
			ConsistentRead:           nil,
			ExpressionAttributeNames: nil,
			ProjectionExpression:     nil,
		}

		getTableItems := map[string]types.KeysAndAttributes{
			manifestSession.FileTableName: keysAndAttributes,
		}

		batchItemInput := dynamodb.BatchGetItemInput{
			RequestItems:           getTableItems,
			ReturnConsumedCapacity: "",
		}

		dbResults, err := manifestSession.Client.BatchGetItem(context.Background(), &batchItemInput)
		if err != nil {
			log.Fatalln("Unable to get dbItems.")
		}

		dbItems := dbResults.Responses[manifestSession.FileTableName]

		for _, dbItem := range dbItems {
			fileEntry := dbTable.ManifestFileTable{}
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

			verifiedFiles = append(verifiedFiles, uploadEntry{
				manifestId:     fileEntry.ManifestId,
				uploadId:       fileEntry.UploadId,
				s3Bucket:       inputUploadEntry.s3Bucket,
				s3Key:          inputUploadEntry.s3Key,
				size:           inputUploadEntry.size,
				path:           fileEntry.FilePath,
				name:           fileEntry.FileName,
				extension:      pathParts[r.SubexpIndex("Extension")],
				eTag:           inputUploadEntry.eTag,
				mergePackageId: fileEntry.MergePackageId,
				fileType:       fileEntry.FileType,
				sha256:         inputUploadEntry.sha256,
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
		log.Error("MISMATCH BETWEEN UPLOADED ENTRIES AND RETURN FROM DYNAMOBD.")
	}

	var uploadFiles []uploadFile.UploadFile
	for _, f := range verifiedFiles {

		// Get FileInfo from fileType string in verified file.
		fType, pInfo := getFileInfo(f.fileType)

		file := uploadFile.UploadFile{
			ManifestId:     f.manifestId,
			UploadId:       f.uploadId,
			FileType:       fType,
			S3Bucket:       f.s3Bucket,
			S3Key:          f.s3Key,
			Path:           f.path,
			Name:           f.name,
			Extension:      f.extension,
			Type:           pInfo.PackageType,
			SubType:        pInfo.PackageSubType,
			Icon:           pInfo.Icon,
			Size:           f.size,
			ETag:           f.eTag,
			MergePackageId: f.mergePackageId,
			Sha256:         f.sha256,
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

// uploadEntryFromS3Event returns an object representing an uploaded file from an S3 Event.
func uploadEntryFromS3Event(event *events.S3Event) (*uploadEntry, error) {
	r := regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<UploadId>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})`)
	res := r.FindStringSubmatch(event.Records[0].S3.Object.Key)

	if res == nil {
		return nil, errors.New(fmt.Sprintf("File does not contain the required S3-Key components: %s",
			event.Records[0].S3.Object.Key))
	}

	// Found standard upload manifest/key combination
	manifestId := res[r.SubexpIndex("Manifest")]
	uploadId := res[r.SubexpIndex("UploadId")]

	s3Bucket := event.Records[0].S3.Bucket.Name
	s3Key := event.Records[0].S3.Object.Key

	// Get File Size
	headObj := s3.HeadObjectInput{
		Bucket:       aws.String(s3Bucket),
		Key:          aws.String(s3Key),
		ChecksumMode: s3Types.ChecksumModeEnabled,
	}
	result, err := manifestSession.S3Client.HeadObject(context.Background(), &headObj)
	if err != nil {
		log.Println(err)
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
				"upload_id":   uploadId,
			},
		).Warn(fmt.Sprintf("Unable to get HEAD object %s / %s", s3Bucket, s3Key))
	}

	response := uploadEntry{
		s3Bucket:   s3Bucket,
		s3Key:      s3Key,
		manifestId: manifestId,
		uploadId:   uploadId,
		eTag:       event.Records[0].S3.Object.ETag,
		size:       event.Records[0].S3.Object.Size,
		sha256:     checkSumOrEmpty(result.ChecksumSHA256),
	}

	log.WithFields(
		log.Fields{
			"manifest_id": response.manifestId,
			"upload_id":   response.uploadId,
		},
	).Debugf("UploadEntry created in %s / %s", response.s3Bucket, response.s3Key)
	return &response, nil
}

func checkSumOrEmpty(checkSum *string) string {
	if checkSum != nil {
		return *checkSum
	}
	return ""
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
