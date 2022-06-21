package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-api/pkg"
	"log"
	"os"
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
			log.Println("Unable to create upload session.", err)
			continue
		}
		err = session.ImportFiles(uploadFilesForManifest)
		session.Close()

		if err != nil {
			log.Println("Unable to create packages: ", err)
			continue
		}
	}

	return nil
}
