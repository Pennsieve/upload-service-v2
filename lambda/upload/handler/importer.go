package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-api/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-api/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-api/models/uploadFolder"
	"github.com/pennsieve/pennsieve-go-api/pkg/core"
	"log"
	"sort"
)

type uploadEntry struct {
	manifestId string
	uploadId   string
	s3Bucket   string
	s3Key      string
	isStandard bool
	path       string
	name       string
	extension  string
	eTag       string
	size       int64
}

// UploadSession contains the information that is shared based on the upload session ID
type UploadSession struct {
	organizationId  int
	datasetId       int
	datasetNodeId   string
	ownerId         int
	targetPackageId string
	db              *sql.DB
}

// Close closes the organization connection associated with the session.
func (s *UploadSession) Close() {
	err := s.db.Close()
	if err != nil {
		log.Println("Unable to close DB connection from Lambda function.")
		return
	}

}

// CreateUploadSession returns an authenticated object based on the uploadSession UUID
func (*UploadSession) CreateUploadSession(manifest *dbTable.ManifestTable) (*UploadSession, error) {

	s := UploadSession{
		organizationId: int(manifest.OrganizationId),
		datasetId:      int(manifest.DatasetId),
		datasetNodeId:  manifest.DatasetNodeId,
		ownerId:        int(manifest.UserId),
	}

	db, err := core.ConnectRDSWithOrg(s.organizationId)
	s.db = db
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// ImportFiles is the wrapper function to import files from a single upload-session.
// A single upload session implies that all files belong to the same organization, dataset and owner.
func (s *UploadSession) ImportFiles(files []uploadFile.UploadFile, manifest *dbTable.ManifestTable) error {

	// Sort files by the length of their path
	// First element closest to root.
	defer s.Close()

	var f uploadFile.UploadFile
	f.Sort(files)

	// 1. Iterate over files and return map of folders and subfolders
	folderMap := f.GetUploadFolderMap(files, "")

	// 2. Iterate over folders and create them if they do not exist in organization
	folderPackageMap := s.GetCreateUploadFolders(folderMap)

	// 3. Create Package Params to add files to packages table.
	pkgParams, _ := s.GetPackageParams(files, folderPackageMap)

	var packageTable dbTable.Package
	packages, err := packageTable.Add(s.db, pkgParams)
	if err != nil {
		return err
	}

	// 4. Associate Files with Packages
	fileMap := map[string]uploadFile.UploadFile{}
	for _, f := range files {
		fileMap[f.UploadId] = f
	}

	var allFileParams []dbTable.FileParams
	for i, _ := range packages {

		curFile := fileMap[packages[i].ImportId.String]

		file := dbTable.FileParams{
			PackageId:  int(packages[i].Id),
			Name:       packages[i].Name,
			FileType:   curFile.FileType,
			S3Bucket:   curFile.S3Bucket,
			S3Key:      curFile.S3Key,
			ObjectType: objectType.Source,
			Size:       curFile.Size,
			CheckSum:   curFile.ETag,
			UUID:       uuid.MustParse(curFile.UploadId),
		}

		allFileParams = append(allFileParams, file)
	}

	var ff dbTable.File
	returnedFiles, err := ff.Add(s.db, allFileParams)
	if err != nil {
		log.Println(err)
	}

	// Notify SNS that files were imported.
	s.PublishToSNS(returnedFiles)

	// Update storage for packages, datasets, and org
	s.UpdateStorage(packages, manifest)

	return nil
}

// PublishToSNS publishes messages to SNS after files are imported.
func (s *UploadSession) PublishToSNS(files []dbTable.File) error {
	// Send SNS Message
	var snsEntries []types.PublishBatchRequestEntry
	for _, f := range files {
		e := types.PublishBatchRequestEntry{
			Id:      aws.String(f.UUID.String()),
			Message: aws.String(fmt.Sprintf("%d", f.PackageId)),
		}
		snsEntries = append(snsEntries, e)
	}

	params := sns.PublishBatchInput{
		PublishBatchRequestEntries: snsEntries,
		TopicArn:                   nil,
	}
	manifestSession.SNSClient.PublishBatch(context.Background(), &params)

	return nil
}

// GetCreateUploadFolders creates new folders in the organization.
// It updates UploadFolders with real folder ID for folders that already exist.
// Assumes map keys are absolute paths in the dataset
func (s *UploadSession) GetCreateUploadFolders(folders uploadFolder.UploadFolderMap) dbTable.PackageMap {

	// Create map to map parentID to array of children

	// Get Root Folders
	p := dbTable.Package{}
	rootChildren, _ := p.Children(s.db, s.organizationId, &p, s.datasetId, true)

	// Map NodeId to Packages for folders that exist in DB
	existingFolders := dbTable.PackageMap{}
	for _, k := range rootChildren {
		existingFolders[k.Name] = k
	}

	// Sort the keys of the map so we can iterate over the sorted map
	pathKeys := make([]string, 0)
	for k := range folders {
		pathKeys = append(pathKeys, k)
	}
	sort.Strings(pathKeys)

	// Iterate over the sorted map
	for _, path := range pathKeys {

		if folder, ok := existingFolders[path]; ok {

			// Use existing folder
			folders[path].NodeId = folder.NodeId
			folders[path].Id = folder.Id

			// Iterate over map and update values that have identified current folder as parent.
			for _, childFolder := range folders[path].Children {
				childFolder.ParentId = folder.Id
				childFolder.ParentNodeId = folder.NodeId
			}

			// Add children of current folder to existing folders
			children, _ := p.Children(s.db, s.organizationId, &folder, s.datasetId, true)
			for _, k := range children {
				p := fmt.Sprintf("%s/%s", path, k.Name)
				existingFolders[p] = k
			}

		} else {
			// Create folder
			pkgParams := dbTable.PackageParams{
				Name:         folders[path].Name,
				PackageType:  packageType.Collection,
				PackageState: packageState.Ready,
				NodeId:       folders[path].NodeId,
				ParentId:     folders[path].ParentId,
				DatasetId:    s.datasetId,
				OwnerId:      s.ownerId,
				Size:         0,
				Attributes:   nil,
			}

			result, _ := p.Add(s.db, []dbTable.PackageParams{pkgParams})
			folders[path].Id = result[0].Id
			existingFolders[path] = result[0]

			for _, childFolder := range folders[path].Children {
				childFolder.ParentId = result[0].Id
				childFolder.ParentNodeId = result[0].NodeId
			}
		}
	}

	return existingFolders

}

// GetPackageParams returns an array of PackageParams to insert in the Packages Table.
func (s *UploadSession) GetPackageParams(uploadFiles []uploadFile.UploadFile, pathToFolderMap dbTable.PackageMap) ([]dbTable.PackageParams, error) {
	var pkgParams []dbTable.PackageParams

	for _, file := range uploadFiles {
		packageID := fmt.Sprintf("N:package:%s", uuid.New().String())

		parentId := int64(-1)
		if file.Path != "" {
			parentId = pathToFolderMap[file.Path].Id
		}

		uploadId := sql.NullString{
			String: file.UploadId,
			Valid:  true,
		}

		// Set Default attributes for File ==> Subtype and Icon
		var attributes []packageInfo.PackageAttribute
		attributes = append(attributes, packageInfo.PackageAttribute{
			Key:      "subtype",
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

		pkgParam := dbTable.PackageParams{
			Name:         file.Name,
			PackageType:  file.Type,
			PackageState: packageState.Uploaded,
			NodeId:       packageID,
			ParentId:     parentId,
			DatasetId:    s.datasetId,
			OwnerId:      s.ownerId,
			Size:         file.Size,
			ImportId:     uploadId,
			Attributes:   attributes,
		}

		pkgParams = append(pkgParams, pkgParam)
	}

	return pkgParams, nil

}

// UpdateStorage updates storage in packages, dataset and organization for uploaded package
func (s *UploadSession) UpdateStorage(packages []dbTable.Package, manifest *dbTable.ManifestTable) error {

	dbOrg, err := core.ConnectRDS()
	if err != nil {
		log.Println("Error connecting to database.")
		return err
	}
	defer dbOrg.Close()

	// Update all packageSize
	for _, pkg := range packages {

		var p dbTable.PackageStorage
		err := p.Increment(s.db, pkg.Id, pkg.Size.Int64)
		if err != nil {
			log.Println("Error incrementing package")
			return err
		}

		if pkg.ParentId.Valid {
			p.IncrementAncestors(s.db, pkg.ParentId.Int64, pkg.Size.Int64)
			if err != nil {
				log.Println("Error incrementing package ancestors")
				return err
			}
		}

		var d dbTable.DatasetStorage
		err = d.Increment(s.db, manifest.DatasetId, pkg.Size.Int64)
		if err != nil {
			log.Println("Error incrementing dataset.")
			return err
		}

		var o dbTable.OrganizationStorage
		err = o.Increment(dbOrg, manifest.OrganizationId, pkg.Size.Int64)
		if err != nil {
			log.Println("Error incrementing organization")
			return err
		}
	}

	return nil

}
