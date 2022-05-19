package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-api/config"
	"github.com/pennsieve/pennsieve-go-api/models"
	"github.com/pennsieve/pennsieve-go-api/models/db"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo"
	"log"
)

// UploadSession contains the information that is shared based on the upload session ID
type UploadSession struct {
	OrganizationId int
	DatasetId      int
	OwnerId        int
}

func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {
		log.Printf("The message %s for event source %s = %s \n\n", message.MessageId, message.EventSource, message.Body)
	}

	// 1. Connect to Database
	conn, _ := config.ConnectRDS()
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Println("Unable to close DB connection from Lambda function.")
			return
		}
		log.Println("Closing DB connection.")
	}()

	// 2. Retrieve info from Manifest Tables
	// TODO: Replace this with API calls
	session := UploadSession{
		OrganizationId: 19,   // Pennsieve Test
		DatasetId:      1682, // Test Upload
		OwnerId:        24,   // Joost
	}

	// Parse uploadFiles from S3 Event
	uploadFiles, _ := getUploadFiles(sqsEvent.Records)

	// Map by uploadSessionID
	var fileBySession = map[string][]models.UploadFile{}
	for _, f := range uploadFiles {
		fileBySession[f.SessionId] = append(fileBySession[f.SessionId], f)
	}

	for _, fileUploads := range fileBySession {
		importFiles(fileUploads, session)
	}

	return nil
}

func getUploadFiles(fileEvents []events.SQSMessage) ([]models.UploadFile, error) {

	var pkgs []models.UploadFile
	for _, message := range fileEvents {

		/*
			Messages can be from multiple upload sessions --> multiple organizations.
			We need to:
				1. Separate by upload-session
				2. Create/get the folders from postgres for each upload session
				3. Create Packages
				4. Create Files in Packages
		*/

		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(aws.StringValue(&message.Body)), &parsedS3Event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message, %v", err)
		}

		// Get UploadFile Representation from event
		var uf = models.UploadFile{}
		uploadFile, _ := uf.FromS3Event(&parsedS3Event)

		pkgs = append(pkgs, uploadFile)

	}

	return pkgs, nil
}

func getPackageParams(uploadFiles []models.UploadFile, session UploadSession) ([]db.PackageParams, error) {
	var pkgParams []db.PackageParams

	for _, file := range uploadFiles {
		packageID := fmt.Sprintf("N:package:%s", uuid.New().String())

		// TODO: Figure out how to handle Parent ID and Folders
		parentId := -1 // Setting to -1 identifies the root of the dataset

		//TODO: Replace by s3Key when mapped
		uploadId := uuid.New().String()

		// Set Default attributes for File ==> Subtype and Icon
		var attributes []models.FileAttribute
		attributes = append(attributes, models.FileAttribute{
			Key:      "subType",
			Fixed:    false,
			Value:    file.SubType,
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		}, models.FileAttribute{
			Key:      "icon",
			Fixed:    false,
			Value:    file.Icon.String(),
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		})

		pkgParam := db.PackageParams{
			Name:         file.Name,
			PackageType:  file.Type,
			PackageState: packageInfo.Uploaded,
			NodeId:       packageID,
			ParentId:     parentId,
			DatasetId:    session.DatasetId,
			OwnerId:      session.OwnerId,
			Size:         file.Size,
			ImportId:     uploadId,
			Attributes:   attributes,
		}

		pkgParams = append(pkgParams, pkgParam)
	}

	return pkgParams, nil

}

func importFiles(files []models.UploadFile, session UploadSession) {
	pkgParams, _ := getPackageParams(files, session)

	var pkg db.Package
	pkg.Add(session.OrganizationId, pkgParams)

}
