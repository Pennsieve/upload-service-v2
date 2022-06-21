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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/fileInfo"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-api/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-api/pkg"
	"log"
	"os"
	"regexp"
	"strings"
)

var manifestFileTableName string
var manifestTableName string
var client *dynamodb.Client

// init runs on cold start of lambda and gets jwt keysets from Cognito user pools.
func init() {
	manifestFileTableName = os.Getenv("MANIFEST_FILE_TABLE")
	manifestTableName = os.Getenv("MANIFEST_TABLE")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	client = dynamodb.NewFromConfig(cfg)
}

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
	uploadEntries, err := getUploadEntries(sqsEvent.Records)
	if err != nil {
		// This really should never happen.
		log.Fatalf(err.Error())
	}

	// 2. Match against Manifest and create uploadFiles
	uploadFiles, _ := getUploadFiles(uploadEntries)

	// 3. Map by uploadSessionID
	var fileByManifest = map[string][]uploadFile.UploadFile{}
	for _, f := range uploadFiles {
		fileByManifest[f.ManifestId] = append(fileByManifest[f.ManifestId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for manifestId, uploadFilesForManifest := range fileByManifest {
		var s pkg.UploadSession

		manifest, err := dbTable.GetFromManifest(client, manifestTableName, manifestId)

		session, err := s.CreateUploadSession(manifest)
		if err != nil {
			log.Println("Unable to create upload session.")
			continue
		}
		session.ImportFiles(uploadFilesForManifest)
		session.Close()
	}

	return nil

}

func getUploadEntries(fileEvents []events.SQSMessage) ([]uploadEntry, error) {

	var entries []uploadEntry
	for _, message := range fileEvents {
		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(aws.StringValue(&message.Body)), &parsedS3Event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message, %v", err)
		}

		entry, err := uploadEntryFromS3Event(&parsedS3Event)
		if err != nil {
			log.Println("Unable to parse s3-key: ", err)
			continue
		}

		entries = append(entries, *entry)

	}

	return entries, nil
}

func getUploadFiles(entries []uploadEntry) ([]uploadFile.UploadFile, error) {

	// 1. Check all standard uploadEntities against dynamodb

	var getItems []map[string]types.AttributeValue
	for _, item := range entries {
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
			// TODO: Implement manifestID checking
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
			manifestFileTableName: keysAndAttributes,
		}

		batchItemInput := dynamodb.BatchGetItemInput{
			RequestItems:           getTableItems,
			ReturnConsumedCapacity: "",
		}

		dbResults, err := client.BatchGetItem(context.Background(), &batchItemInput)
		if err != nil {
			log.Fatalln("Unable to get dbItems.")
		}

		fileEntry := dbTable.ManifestFileTable{}
		dbItems := dbResults.Responses[manifestFileTableName]

		for _, dbItem := range dbItems {
			err := attributevalue.UnmarshalMap(dbItem, &fileEntry)
			if err != nil {
				fmt.Println("Unable to UnMarshall unprocessed items. ", err)
				return nil, err
			}

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
				isStandard: true,
				path:       fileEntry.FilePath,
				name:       fileEntry.FileName,
				extension:  pathParts[r.SubexpIndex("Extension")],
			})
		}
	}

	// 2. Check non-normal against manifestId

	// 3. Create uploadFiles for valid uploadEntries

	if len(verifiedFiles) != len(entries) {
		log.Println("MISMATCH BETWEEN UPLOADED ENTRIES AND RETURN FROM DYNAMOBD.")
	}

	var uploadFiles []uploadFile.UploadFile
	for _, f := range verifiedFiles {

		// Match uploadEntry
		info := getFileInfo(f.extension)
		file := uploadFile.UploadFile{
			ManifestId: f.manifestId,
			Path:       f.path,
			Name:       f.name,
			Extension:  f.extension,
			Type:       info.PackageType,
			SubType:    info.PackageSubType,
			Icon:       info.Icon,
			Size:       f.size,
			ETag:       f.eTag,
		}

		uploadFiles = append(uploadFiles, file)
	}

	return uploadFiles, nil

	// 4. TODO handle (delete) non-compliant uploads.

}

type uploadEntry struct {
	manifestId string
	uploadId   string
	isStandard bool
	path       string
	name       string
	extension  string
	eTag       string
	size       int64
}

func uploadEntryFromS3Event(event *events.S3Event) (*uploadEntry, error) {
	log.Println("Full S3 Key:", event.Records[0].S3.Object.Key)
	r := regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<UploadId>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})`)
	res := r.FindStringSubmatch(event.Records[0].S3.Object.Key)

	if res != nil {
		// Found standard upload manifest/key combination
		manifestId := res[r.SubexpIndex("Manifest")]
		uploadId := res[r.SubexpIndex("UploadId")]
		response := uploadEntry{
			manifestId: manifestId,
			uploadId:   uploadId,
			isStandard: true,
			eTag:       event.Records[0].S3.Object.ETag,
			size:       event.Records[0].S3.Object.Size,
		}
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

// getFileInfo returns a FileTypeInfo for a particular extension.
func getFileInfo(extension string) packageInfo.FileTypeInfo {

	// Check full extension
	fileType, exists := fileInfo.FileExtensionDict[extension]
	if !exists {
		fileType = fileInfo.Unknown

		// Check last extension if unknown as extension can contain multiple '.'.
		r := regexp.MustCompile(`(?P<Extension>[^.]*)$`)
		pathParts := r.FindStringSubmatch(extension)

		fileType, exists = fileInfo.FileExtensionDict[pathParts[r.SubexpIndex("Extension")]]
		if !exists {
			fileType = fileInfo.Unknown
		}

	}

	packageType, exists := packageInfo.FileTypeDict[fileType]
	if !exists {
		log.Println("Unmatched filetype. ?!?")
		packageType = packageInfo.FileTypeDict[fileInfo.Unknown]
	}

	return packageType
}
