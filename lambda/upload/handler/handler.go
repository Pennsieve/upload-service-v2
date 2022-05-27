package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pennsieve/pennsieve-go-api/pkg"
	"strings"
)

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

	// 1. Retrieve info from Manifest Tables
	// TODO: Replace fake token by sessionID

	// 2. Parse uploadFiles from S3 Event
	uploadFiles, _ := getUploadFiles(sqsEvent.Records)

	// 3. Map by uploadSessionID
	var fileBySession = map[string][]pkg.UploadFile{}
	for _, f := range uploadFiles {
		fileBySession[f.SessionId] = append(fileBySession[f.SessionId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for _, uploadFilesForSession := range fileBySession {
		session := pkg.CreateUploadSession("fakeSessionToken")
		session.ImportFiles(uploadFilesForSession)
		session.Close()
	}

	return nil
}

// getUploadFiles parses the SQS Messages and constructs an array of UploadFiles.
func getUploadFiles(fileEvents []events.SQSMessage) ([]pkg.UploadFile, error) {

	var pkgs []pkg.UploadFile
	for _, message := range fileEvents {

		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(aws.StringValue(&message.Body)), &parsedS3Event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message, %v", err)
		}

		// Get UploadFile Representation from event
		var uf = pkg.UploadFile{}
		uploadFile, _ := uf.FromS3Event(&parsedS3Event)
		uploadFile.Path = strings.TrimSuffix(uploadFile.Path, "/")
		uploadFile.Path = strings.TrimPrefix(uploadFile.Path, "/")
		fmt.Printf("Upload File: %s\n", uploadFile.Path)

		pkgs = append(pkgs, uploadFile)

	}

	return pkgs, nil
}
