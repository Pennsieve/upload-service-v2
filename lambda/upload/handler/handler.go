package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-api/config"
	"github.com/pennsieve/pennsieve-go-api/models"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-api/pkg"
	"log"
	"sort"
	"strings"
)

// UploadSession contains the information that is shared based on the upload session ID
type UploadSession struct {
	OrganizationId  int    `json:"organization_id"`
	DatasetId       int    `json:"dataset_id"`
	OwnerId         int    `json:"owner_id"`
	TargetPackageId string `json:"target_package_id"`
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

// getPackageParams returns an array of PackageParams to insert in the Packages Table.
func getPackageParams(uploadFiles []pkg.UploadFile, session UploadSession, packageMap pkg.PackageMap) ([]models.PackageParams, error) {
	var pkgParams []models.PackageParams

	for _, file := range uploadFiles {
		packageID := fmt.Sprintf("N:package:%s", uuid.New().String())

		parentId := int64(-1)
		if file.Path != "" {
			parentId = packageMap[file.Path].Id
		}

		//TODO: Replace by s3Key when mapped
		uploadId := sql.NullString{
			String: uuid.New().String(),
			Valid:  true,
		}

		// Set Default attributes for File ==> Subtype and Icon
		var attributes []packageInfo.PackageAttribute
		attributes = append(attributes, packageInfo.PackageAttribute{
			Key:      "subType",
			Fixed:    false,
			Value:    file.SubType,
			Hidden:   true,
			Category: "Pennsieve",
			DataType: "string",
		}, packageInfo.PackageAttribute{
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

	fmt.Println("PKGPARAMS")
	for _, pkg := range pkgParams {
		fmt.Printf("Name: %s, ParentId: %d, NodeId: %s", pkg.Name, pkg.ParentId, pkg.NodeId)
	}

	return pkgParams, nil

}

// importFiles is the wrapper function to import files from a single upload-session.
// A single upload session implies that all files belong to the same organization, dataset and owner.
func importFiles(files []pkg.UploadFile, session UploadSession) {

	// Sort files by the length of their path
	// First element closest to root.
	sortUploadFiles(files)

	// Iterate over files and return map of folders and subfolders
	folderMap := getUploadFolderMap(files, "")

	// Iterate over folders and create them if they do not exist in database
	packageMap := pkg.GetCreateUploadFolders(session.OrganizationId, session.DatasetId, session.OwnerId, folderMap)

	// 3. Create Package Params to add files to packages table.
	pkgParams, _ := getPackageParams(files, session, packageMap)

	var packageTable models.Package
	packageTable.Add(session.OrganizationId, pkgParams)

}

// sortUploadFiles sorts []UploadFiles by the depth of the folder the file resides in.
func sortUploadFiles(files []pkg.UploadFile) {
	sort.Slice(files, func(i, j int) bool {
		//pathSlices1 := strings.Split(files[i].Path, "/")
		//pathSlices2 := strings.Split(files[j].Path, "/")
		return files[i].Path < files[j].Path
	})
}

// getUploadFolderMap returns an object that maps path name to Folder object.
func getUploadFolderMap(sortedFiles []pkg.UploadFile, targetFolder string) pkg.UploadFolderMap {

	// Mapping path from targetFolder to UploadFolder Object
	var folderNameMap = map[string]*pkg.UploadFolder{}

	// Iterate over the files and create the UploadFolder objects.
	for index, f := range sortedFiles {

		if f.Path == "" {
			continue
		}

		fmt.Printf("File index: %d, File Path: %s\n ", index, f.Path)

		// Prepend the target-Folder if it exists
		p := f.Path
		if targetFolder != "" {
			p = strings.Join(
				[]string{
					targetFolder, p,
				}, "/")
		}

		// Iterate over path segments in a single file and identify folders.
		pathSegments := strings.Split(p, "/")
		absoluteSegment := "" // Current location in the path walker for current file.
		currentNodeId := ""
		currentFolderPath := ""
		for depth, segment := range pathSegments {

			fmt.Printf("Depth: %d, segment: %s, abs_segment: %s\n", depth, segment, absoluteSegment)

			parentNodeId := currentNodeId
			parentFolderPath := currentFolderPath

			// If depth > 0 ==> prepend the previous absoluteSegment to the current path name.
			if depth > 0 {
				absoluteSegment = strings.Join(
					[]string{

						absoluteSegment,
						segment,
					}, "/")
			} else {
				absoluteSegment = segment
			}

			// If folder already exists in map, add current folder as a child to the parent
			// folder (which will exist too at this point). If not, create new folder to the map and add to parent folder.

			folder, ok := folderNameMap[absoluteSegment]
			if ok {
				currentNodeId = folder.NodeId
				currentFolderPath = absoluteSegment

			} else {
				currentNodeId = fmt.Sprintf("N:collection:%s", uuid.New().String())
				currentFolderPath = absoluteSegment

				folder = &pkg.UploadFolder{
					NodeId:       currentNodeId,
					Name:         segment,
					ParentNodeId: parentNodeId,
					ParentId:     -1,
					Depth:        depth,
				}
				folderNameMap[absoluteSegment] = folder

				fmt.Printf("Create Folder: %s with Name: %s\n", absoluteSegment, folder.Name)
			}

			// Add current segment to parent if exist
			if parentFolderPath != "" {
				folderNameMap[parentFolderPath].Children = append(folderNameMap[parentFolderPath].Children, folder)
			}

		}
	}

	return folderNameMap
}
