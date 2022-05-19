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
	"github.com/pennsieve/pennsieve-go-api/pkg"
	"github.com/pennsieve/pennsieve-go-api/pkg/packageInfo"
	"log"
)

// UploadSession contains the information that is shared based on the upload session ID
type UploadSession struct {
	OrganizationId int
	DatasetId      int
	OwnerId        int
}

// Handler implements the function that is called when new SQS Events arrive.
func Handler(ctx context.Context, sqsEvent events.SQSEvent) error {

	// 1. Retrieve info from Manifest Tables
	// TODO: Replace this with API calls
	session := UploadSession{
		OrganizationId: 19,   // Pennsieve Test
		DatasetId:      1682, // Test Upload
		OwnerId:        24,   // Joost
	}

	// 1. Connect to Database
	conn, _ := config.ConnectRDS(session.OrganizationId)
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Println("Unable to close DB connection from Lambda function.")
			return
		}
		log.Println("Closing DB connection.")
	}()

	// 2. Parse uploadFiles from S3 Event
	uploadFiles, _ := getUploadFiles(sqsEvent.Records)

	// 3. Map by uploadSessionID
	var fileBySession = map[string][]pkg.UploadFile{}
	for _, f := range uploadFiles {
		fileBySession[f.SessionId] = append(fileBySession[f.SessionId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for _, uploadFilesForSession := range fileBySession {
		importFiles(uploadFilesForSession, session)
	}

	return nil
}

// getUploadFiles parses the SQS Messages and constructs an array of UploadFiles.
func getUploadFiles(fileEvents []events.SQSMessage) ([]pkg.UploadFile, error) {

	var pkgs []pkg.UploadFile
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
		var uf = pkg.UploadFile{}
		uploadFile, _ := uf.FromS3Event(&parsedS3Event)

		pkgs = append(pkgs, uploadFile)

	}

	return pkgs, nil
}

// getPackageParams returns an array of PackageParams to insert in the Packages Table.
func getPackageParams(uploadFiles []pkg.UploadFile, session UploadSession) ([]models.PackageParams, error) {
	var pkgParams []models.PackageParams

	for _, file := range uploadFiles {
		packageID := fmt.Sprintf("N:package:%s", uuid.New().String())

		// TODO: Figure out how to handle Parent ID and Folders
		parentId := -1 // Setting to -1 identifies the root of the dataset

		//TODO: Replace by s3Key when mapped
		uploadId := uuid.New().String()

		// Set Default attributes for File ==> Subtype and Icon
		var attributes []pkg.FileAttribute
		attributes = append(attributes, pkg.FileAttribute{
			Key:      "subType",
			Fixed:    false,
			Value:    file.SubType,
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		}, pkg.FileAttribute{
			Key:      "icon",
			Fixed:    false,
			Value:    file.Icon.String(),
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		})

		pkgParam := models.PackageParams{
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

// importFiles is the wrapper function to import files from a single upload-session.
// A single upload session implies that all files belong to the same organization, dataset and owner.
func importFiles(files []pkg.UploadFile, session UploadSession) {

	// 2. Find list of folders and create folders if necessary
	//TODO: implement

	// 3. Create Package Params to add files to packages table.
	pkgParams, _ := getPackageParams(files, session)

	var packageTable models.Package
	packageTable.Add(session.OrganizationId, pkgParams)

}
