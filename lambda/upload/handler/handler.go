package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/fileInfo/fileType"
	"github.com/pennsieve/pennsieve-go-api/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-api/models/uploadFile"
	manifestPkg "github.com/pennsieve/pennsieve-go-api/pkg/manifest"
	"log"
	"os"
	"regexp"
	"strings"
)

var manifestSession manifestPkg.ManifestSession

// init runs on cold start of lambda and gets jwt keysets from Cognito user pools.
func init() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	manifestSession = manifestPkg.ManifestSession{
		FileTableName: os.Getenv("MANIFEST_FILE_TABLE"),
		TableName:     os.Getenv("MANIFEST_TABLE"),
		Client:        dynamodb.NewFromConfig(cfg),
		SNSClient:     sns.NewFromConfig(cfg),
		SNSTopic:      os.Getenv("IMPORTED_SNS_TOPIC"),
	}
}

//

// Handler implements the function that is called when new SQS Events arrive.
func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {

	/*
		Messages can be from multiple upload sessions --> multiple organizations.
		We need to:
			1. Separate by upload-session
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
	uploadFiles, _ := GetUploadFiles(uploadEntries)

	// 3. Map by uploadSessionID
	var fileByManifest = map[string][]uploadFile.UploadFile{}
	for _, f := range uploadFiles {
		fileByManifest[f.ManifestId] = append(fileByManifest[f.ManifestId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for manifestId, uploadFilesForManifest := range fileByManifest {
		var s UploadSession

		// Get manifest from dynamodb
		manifest, err := dbTable.GetFromManifest(manifestSession.Client, manifestSession.TableName, manifestId)

		// Create upload session (with DB access) and import files
		session, err := s.CreateUploadSession(manifest)
		if err != nil {
			log.Println("Unable to create upload session.", err)
			continue
		}
		err = session.ImportFiles(uploadFilesForManifest, manifest)

		if err != nil {
			log.Println("Unable to create packages: ", err)
			continue
		}

		// Update status of files in dynamoDB
		var fileDTOs []manifestFile.FileDTO
		for _, u := range uploadFilesForManifest {
			f := manifestFile.FileDTO{
				UploadID:   u.UploadId,
				S3Key:      u.S3Key,
				TargetPath: u.Path,
				TargetName: u.Name,
				Status:     manifestFile.Imported,
			}
			fileDTOs = append(fileDTOs, f)
		}

		// We are replacing the entries instead of updating the status field as
		// this is the only way we can batch update, and we also update the name in
		// case that we need to append index (on name conflict).
		setStatus := manifestFile.Imported
		manifestSession.AddFiles(manifestId, fileDTOs, &setStatus)

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
			log.Println("Unable to parse s3-key: ", err)
			continue
		}

		fmt.Println("getUploadEntries: uploadID ", entry.uploadId)

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

		if item.isStandard {

			data, err := attributevalue.MarshalMap(dbTable.ManifestFilePrimaryKey{
				ManifestId: item.manifestId,
				UploadId:   item.uploadId,
			})
			if err != nil {
				log.Fatalf("MarshalMap: %v\n", err)
			}
			getItems = append(getItems, data)

		} else {
			continue
			// TODO: Remove non-compliant uploads
		}

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
				fmt.Println("Unable to UnMarshall unprocessed items. ", err)
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
				manifestId: fileEntry.ManifestId,
				uploadId:   fileEntry.UploadId,
				s3Bucket:   inputUploadEntry.s3Bucket,
				s3Key:      inputUploadEntry.s3Key,
				size:       inputUploadEntry.size,
				isStandard: true,
				path:       fileEntry.FilePath,
				name:       fileEntry.FileName,
				extension:  pathParts[r.SubexpIndex("Extension")],
			})

			fmt.Println("GetUploadFiles: uploadID ", fileEntry.UploadId)
		}
	}

	if len(verifiedFiles) != len(entries) {
		log.Println("MISMATCH BETWEEN UPLOADED ENTRIES AND RETURN FROM DYNAMOBD.")
	}

	var uploadFiles []uploadFile.UploadFile
	for _, f := range verifiedFiles {

		// Match uploadEntry
		fType, pInfo := getFileInfo(f.extension)
		file := uploadFile.UploadFile{
			ManifestId: f.manifestId,
			UploadId:   f.uploadId,
			FileType:   fType,
			S3Bucket:   f.s3Bucket,
			S3Key:      f.s3Key,
			Path:       f.path,
			Name:       f.name,
			Extension:  f.extension,
			Type:       pInfo.PackageType,
			SubType:    pInfo.PackageSubType,
			Icon:       pInfo.Icon,
			Size:       f.size,
			ETag:       f.eTag,
		}

		fmt.Println(file)

		uploadFiles = append(uploadFiles, file)
	}

	return uploadFiles, nil

	// 4. TODO handle (delete) non-compliant uploads.
	// Currently non-compliant uploads are ignored and will remain in uploads folder.

}

// uploadEntryFromS3Event returns an object representing an uploaded file from an S3 Event.
func uploadEntryFromS3Event(event *events.S3Event) (*uploadEntry, error) {
	r := regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<UploadId>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})`)
	res := r.FindStringSubmatch(event.Records[0].S3.Object.Key)

	if res != nil {
		// Found standard upload manifest/key combination
		manifestId := res[r.SubexpIndex("Manifest")]
		uploadId := res[r.SubexpIndex("UploadId")]
		response := uploadEntry{
			s3Bucket:   event.Records[0].S3.Bucket.Name,
			s3Key:      event.Records[0].S3.Object.Key,
			manifestId: manifestId,
			uploadId:   uploadId,
			isStandard: true,
			eTag:       event.Records[0].S3.Object.ETag,
			size:       event.Records[0].S3.Object.Size,
		}

		fmt.Println("uploadEntry:", response.s3Bucket, response.s3Key)
		return &response, nil
	}

	// Check if this entry is valid manifest ID and filename path.
	r = regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<Key>.*)`)
	res = r.FindStringSubmatch(event.Records[0].S3.Object.Key)
	if res == nil {
		// File does not contain the required s3key components
		return nil, errors.New(fmt.Sprintf("File does not contain the required s3key components: %s",
			event.Records[0].S3.Object.Key))
	}

	// 2. Split Path into name and path
	/*
		Match path as 0+ segments that end with a /
		Match Filename as set of characters up to the first .
		Match optional Extension as everything after the first . in Filename
	*/
	manifestId := res[r.SubexpIndex("Manifest")]
	path := res[r.SubexpIndex("Key")]
	r2 := regexp.MustCompile(`(?P<Path>([^\/]*\/)*)(?P<FileName>[^\.]*)?\.?(?P<Extension>.*)`)
	pathParts := r2.FindStringSubmatch(path)
	if pathParts == nil {
		// File does not contain the required s3key components
		return nil, errors.New(fmt.Sprintf("File path does not contain the required s3key components: %s",
			path))
	}

	fileExtension := pathParts[r2.SubexpIndex("Extension")]
	fileName := pathParts[r2.SubexpIndex("FileName")]
	if fileExtension != "" {
		str := []string{pathParts[r2.SubexpIndex("FileName")], fileExtension}
		fileName = strings.Join(str, ".")
	}

	response := uploadEntry{
		s3Bucket:   event.Records[0].S3.Bucket.Name,
		s3Key:      event.Records[0].S3.Object.Key,
		manifestId: manifestId,
		isStandard: false,
		path:       pathParts[r2.SubexpIndex("Path")],
		name:       fileName,
		extension:  fileExtension,
		eTag:       event.Records[0].S3.Object.ETag,
		size:       event.Records[0].S3.Object.Size,
	}

	return &response, nil

}

// getFileInfo returns a PackageTypeInfo for a particular extension.
func getFileInfo(extension string) (fileType.Type, packageType.Info) {

	// Check full extension
	fType, exists := fileType.ExtensionToTypeDict[extension]
	if !exists {
		fType = fileType.GenericData

		// Check last extension if unknown as extension can contain multiple '.'.
		r := regexp.MustCompile(`(?P<Extension>[^.]*)$`)
		pathParts := r.FindStringSubmatch(extension)

		fType, exists = fileType.ExtensionToTypeDict[pathParts[r.SubexpIndex("Extension")]]
		if !exists {
			fType = fileType.GenericData
		}

	}

	pType, exists := packageType.FileTypeToInfoDict[fType]
	if !exists {
		log.Println("Unmatched filetype. ?!?:", fType)
		pType = packageType.FileTypeToInfoDict[fileType.GenericData]
	}

	return fType, pType
}
