package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-api/pkg/core"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-api/pkg/models/uploadFolder"
	log "github.com/sirupsen/logrus"
	"regexp"
	"sort"
)

// uploadEntry representation of file from SQS queue on Upload Trigger
type uploadEntry struct {
	manifestId     string
	uploadId       string
	s3Bucket       string
	s3Key          string
	path           string
	name           string
	extension      string
	eTag           string
	size           int64
	mergePackageId string
	fileType       string
	sha256         string
}

// UploadSession contains the information that is shared based on the manifest ID
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
		log.Error("Unable to close DB connection from Lambda function.")
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

// ImportFiles is the wrapper function to import files from a single manifest.
// A single manifest implies that all files belong to the same organization, dataset and owner.
// This function is called in response to an upload handler event.
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

	log.Println(pkgParams)

	var packageTable dbTable.Package
	packages, err := packageTable.Add(s.db, pkgParams)
	if err != nil {
		log.Error("Error creating a package: ", err)
		// Some error in creating packages --> none of the packages are imported.

		// This should not really happen but we see this when adding packages causes a constraint violation.
		// such as importing an already imported package. (upload id)

		// TODO should we retry packages individually? or send SNS message for import lambda to handle?

		// TODO what do we do with failed uploads?
		return err
	}

	// 4. Associate Files with Packages
	packageMap := map[string]dbTable.Package{}
	for _, p := range packages {
		packageMap[p.NodeId] = p
	}

	var allFileParams []dbTable.FileParams
	for i, f := range files {
		packageNodeId := fmt.Sprintf("N:package:%s", f.UploadId)
		if len(files[i].MergePackageId) > 0 {
			log.Debug("USING MERGED PACKAGE")
			packageNodeId = fmt.Sprintf("N:package:%s", files[i].MergePackageId)
		}

		file := dbTable.FileParams{
			PackageId:  int(packageMap[packageNodeId].Id),
			Name:       files[i].Name,
			FileType:   files[i].FileType,
			S3Bucket:   files[i].S3Bucket,
			S3Key:      files[i].S3Key,
			ObjectType: objectType.Source,
			Size:       files[i].Size,
			CheckSum:   files[i].ETag,
			UUID:       uuid.MustParse(files[i].UploadId),
			Sha256:     files[i].Sha256,
		}

		allFileParams = append(allFileParams, file)
	}

	var ff dbTable.File
	returnedFiles, err := ff.Add(s.db, allFileParams)
	if err != nil {
		log.Error(err)
	}

	// Notify SNS that files were imported.
	err = s.PublishToSNS(returnedFiles)
	if err != nil {
		log.Error("Error with notifying SNS that records are imported.", err)
	}

	// Update storage for packages, datasets, and org
	s.UpdateStorage(allFileParams, packages, manifest)

	return nil
}

// PublishToSNS publishes messages to SNS after files are imported.
func (s *UploadSession) PublishToSNS(files []dbTable.File) error {

	const batchSize = 10
	var snsEntries []types.PublishBatchRequestEntry
	for _, f := range files {
		e := types.PublishBatchRequestEntry{
			Id:      aws.String(f.UUID.String()),
			Message: aws.String(fmt.Sprintf("%d", f.PackageId)),
		}
		snsEntries = append(snsEntries, e)

		// Send SNS messages in blocks of batchSize
		if len(snsEntries) == batchSize {
			err := sendSNSMessages(snsEntries)
			if err != nil {
				return err
			}
			snsEntries = nil
		}
	}

	// send remaining entries
	err := sendSNSMessages(snsEntries)

	return err
}

func sendSNSMessages(snsEntries []types.PublishBatchRequestEntry) error {
	log.Debug("Number of SNS messages: ", len(snsEntries))

	if len(snsEntries) > 0 {
		params := sns.PublishBatchInput{
			PublishBatchRequestEntries: snsEntries,
			TopicArn:                   aws.String(manifestSession.SNSTopic),
		}
		_, err := manifestSession.SNSClient.PublishBatch(context.Background(), &params)
		if err != nil {
			log.Error("Error publishing to SNS: ", err)
			return err
		}
	}

	return nil

}

// GetCreateUploadFolders creates new folders in the organization.
// It updates UploadFolders with real folder ID for folders that already exist.
// Assumes map keys are absolute paths in the dataset
func (s *UploadSession) GetCreateUploadFolders(folders uploadFolder.UploadFolderMap) dbTable.PackageMap {

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

	// First create a map of params. As there can be upload-files that should be mapped to the same package,
	// we want to ensure we are not creating duplicate packages (as this will cause an error when inserting in db).
	// Then we turn map into array and return the array.
	pkgParamsMap := map[string]dbTable.PackageParams{}
	for _, file := range uploadFiles {

		// Create the packageID based on the uploadID or the mergePackageID if it exists
		packageId, packageName, err := parsePackageId(file)
		if err != nil {
			log.Error(err.Error())
			continue
		}

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
			Name:         packageName,
			PackageType:  file.Type,
			PackageState: packageState.Uploaded,
			NodeId:       packageId,
			ParentId:     parentId,
			DatasetId:    s.datasetId,
			OwnerId:      s.ownerId,
			Size:         0,
			ImportId:     uploadId,
			Attributes:   attributes,
		}

		pkgParamsMap[packageId] = pkgParam
		//// If entry already exists --> sum size, else assign value
		//if val, ok := pkgParamsMap[packageId]; ok {
		//	val.Size += pkgParam.Size
		//} else {
		//	pkgParamsMap[packageId] = pkgParam
		//}

	}

	// Turn map into array --> ensure no duplicate packages.
	for i, _ := range pkgParamsMap {
		pkgParams = append(pkgParams, pkgParamsMap[i])
	}

	return pkgParams, nil

}

// UpdateStorage updates storage in packages, dataset and organization for uploaded package
func (s *UploadSession) UpdateStorage(files []dbTable.FileParams, packages []dbTable.Package, manifest *dbTable.ManifestTable) error {

	packageMap := map[int]dbTable.Package{}
	for _, p := range packages {
		packageMap[int(p.Id)] = p
	}

	dbOrg, err := core.ConnectRDS()
	if err != nil {
		log.Error("Error connecting to database.")
		return err
	}
	defer dbOrg.Close()

	// Update all packageSize
	for _, f := range files {

		var p dbTable.PackageStorage
		err := p.Increment(s.db, int64(f.PackageId), f.Size)
		if err != nil {
			log.Error("Error incrementing package")
			return err
		}

		pkg := packageMap[f.PackageId]
		if pkg.ParentId.Valid {
			p.IncrementAncestors(s.db, pkg.ParentId.Int64, f.Size)
			if err != nil {
				log.Error("Error incrementing package ancestors")
				return err
			}
		}

		var d dbTable.DatasetStorage
		err = d.Increment(s.db, manifest.DatasetId, f.Size)
		if err != nil {
			log.Error("Error incrementing dataset.")
			return err
		}

		var o dbTable.OrganizationStorage
		err = o.Increment(dbOrg, manifest.OrganizationId, f.Size)
		if err != nil {
			log.Error("Error incrementing organization")
			return err
		}
	}

	return nil

}

// parsePackageId returns a packageId and name based on upload-file
func parsePackageId(file uploadFile.UploadFile) (string, string, error) {
	packageId := fmt.Sprintf("N:package:%s", file.UploadId)
	packageName := file.Name
	if len(file.MergePackageId) > 0 {
		packageId = fmt.Sprintf("N:package:%s", file.MergePackageId)

		// Set packageName to name without extension
		r := regexp.MustCompile(`(?P<FileName>[^\.]*)?\.?(?P<Extension>.*)`)
		pathParts := r.FindStringSubmatch(file.Name)
		if pathParts == nil {
			log.Error("Unable to parse filename:", file.Name)
			return "", "", errors.New(fmt.Sprintf("Unable to parse filename: %s", file.Name))
		}

		packageName = pathParts[r.SubexpIndex("FileName")]
	}

	return packageId, packageName, nil
}
