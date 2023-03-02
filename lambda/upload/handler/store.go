package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/domain"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"
	manifestModels "github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest/manifestFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	log "github.com/sirupsen/logrus"
	"regexp"
)

// UploadHandlerStore provides the Queries interface and a db instance.
type UploadHandlerStore struct {
	pg            *UploadPgQueries
	dy            *UploadDyQueries
	pgdb          *sql.DB
	dynamodb      *dynamodb.Client
	SNSClient     domain.SnsAPI
	S3Client      domain.S3API
	SNSTopic      string
	fileTableName string
	tableName     string
}

// NewUploadHandlerStore returns a UploadHandlerStore object which implements the Queries
func NewUploadHandlerStore(db *sql.DB, dy *dynamodb.Client, sns domain.SnsAPI, s3 domain.S3API, fileTableName string, tableName string) *UploadHandlerStore {
	return &UploadHandlerStore{
		pgdb:          db,
		dynamodb:      dy,
		SNSClient:     sns,
		S3Client:      s3,
		pg:            NewUploadPgQueries(db),
		dy:            NewUploadDyQueries(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}

func (s *UploadHandlerStore) WithOrg(orgId int) error {
	_, err := s.pg.WithOrg(orgId)
	return err
}

func (s *UploadHandlerStore) execTx(ctx context.Context, fn func(queries *UploadPgQueries) error) error {

	// NOTE: When you create a new transaction (as below), the s.pgdb is NOT part of the transaction.
	// This has the following impact
	// 1. If you have set the search-path for the pgdb, the search path is no longer applied to s.pgdb
	// 2. Any function that is wrapped in the execTx method should ONLY use the provided queries' struct that wraps the transaction.
	// 3. To enable custom Queries for a service, we wrap the pgdb.Queries in a service specific Queries struct.
	//	  This enables you to create custom queries within the service that leverage the transaction
	//    You can use the exposed Db property of the Queries' struct to create custom database interactions.

	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := NewUploadPgQueries(tx)

	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

// ImportFiles creates rows for uploaded files in Packages and Files tables as a transaction
// All files belong to a single manifest, and therefor single dataset in a single organization.
func (s *UploadHandlerStore) ImportFiles(ctx context.Context, datasetId int, ownerId int, files []uploadFile.UploadFile, manifest *dydb.ManifestTable) error {

	err := s.execTx(ctx, func(qtx *UploadPgQueries) error {

		// Verify assumptions
		for _, f := range files {
			if f.ManifestId != manifest.ManifestId {
				return errors.New("not all files belong to the same manifest (required for ImportFiles method)")
			}
		}

		var f uploadFile.UploadFile
		f.Sort(files)

		// 1. Iterate over files and return map of folders and sub-folders
		folderMap := f.GetUploadFolderMap(files, "")

		// 2. Iterate over folders and create them if they do not exist in organization
		folderPackageMap := qtx.GetCreateUploadFolders(datasetId, ownerId, folderMap)

		// 3. Create Package Params to add files to "packages" table.
		pkgParams, _ := getPackageParams(datasetId, ownerId, files, folderPackageMap)

		packages, err := qtx.AddPackages(context.Background(), pkgParams)
		if err != nil {
			log.Error("Error creating a package: ", err)
			// Some error in creating packages --> none of the packages are imported.

			// This should not really happen, but we see this when adding packages causes a constraint violation.
			// such as importing an already imported package. (upload id)

			// TODO should we retry packages individually? or send SNS message for import lambda to handle?

			// TODO what do we do with failed uploads?
			return err
		}

		// 4. Associate Files with Packages
		packageMap := map[string]pgdb.Package{}
		for _, p := range packages {
			packageMap[p.NodeId] = p
		}

		var allFileParams []pgdb.FileParams
		for i, f := range files {
			packageNodeId := fmt.Sprintf("N:package:%s", f.UploadId)
			if len(files[i].MergePackageId) > 0 {
				log.Debug("USING MERGED PACKAGE")
				packageNodeId = fmt.Sprintf("N:package:%s", files[i].MergePackageId)
			}

			file := pgdb.FileParams{
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

		returnedFiles, err := qtx.AddFiles(context.Background(), allFileParams)
		if err != nil {
			return err
		}

		// Notify SNS that files were imported.
		err = s.PublishToSNS(returnedFiles)
		if err != nil {
			log.Error("Error with notifying SNS that records are imported.", err)
		}

		// Update storage for packages, datasets, and org
		err = qtx.UpdateStorage(allFileParams, packages, manifest)
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

func (s *UploadHandlerStore) Handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	/*
		Messages can be from multiple upload sessions --> multiple organizations.
		We need to:
			1. Separate by manifest-session
			2. Create/get the folders from postgres for each upload session
			3. Create Packages
			4. Create Files in Packages
	*/

	// 1. Parse UploadEntries
	uploadEntries, err := s.GetUploadEntries(sqsEvent.Records)
	if err != nil {
		// This really should never happen --> Somehow the SQS queue received a non-S3 message.
		log.Fatalf(err.Error())
	}

	// 2. Match against Manifest and create uploadFiles
	uploadFiles, err := s.dy.GetUploadFiles(uploadEntries)
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

		// Get manifest from dynamodb
		manifest, err := s.dy.GetFromManifest(ctx, s.tableName, manifestId)
		if err != nil {
			log.Error("GetFromManifest: Unable to get manifest.", err)
			continue
		}

		s.WithOrg(int(manifest.OrganizationId))
		if err != nil {
			log.Error("Unable to set search path.", err)
			continue
		}
		err = s.ImportFiles(ctx, int(manifest.DatasetId), int(manifest.OrganizationId), uploadFilesForManifest, manifest)
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
		s.dy.AddFiles(manifestId, fileDTOs, &setStatus, s.fileTableName)

		// Check if there are any remaining items for manifest and
		// set manifest status if not
		reqStatus := sql.NullString{
			String: "InProgress",
			Valid:  true,
		}
		remaining, _, err := s.dy.GetFilesPaginated(ctx, s.tableName,
			manifestId, reqStatus, 1, nil)
		if len(remaining) == 0 {
			err = s.dy.UpdateManifestStatus(ctx, s.tableName, manifestId, manifestModels.Completed)
			if err != nil {
				return err
			}
		} else if manifest.Status == "Completed" {
			err = s.dy.UpdateManifestStatus(ctx, s.tableName, manifestId, manifestModels.Uploading)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

// getPackageParams returns an array of PackageParams to insert in the Packages Table.
func getPackageParams(datasetId int, ownerId int, uploadFiles []uploadFile.UploadFile, pathToFolderMap pgdb.PackageMap) ([]pgdb.PackageParams, error) {
	var pkgParams []pgdb.PackageParams

	// First create a map of params. As there can be upload-files that should be mapped to the same package,
	// we want to ensure we are not creating duplicate packages (as this will cause an error when inserting in db).
	// Then we turn map into array and return the array.
	pkgParamsMap := map[string]pgdb.PackageParams{}
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

		pkgParam := pgdb.PackageParams{
			Name:         packageName,
			PackageType:  file.Type,
			PackageState: packageState.Uploaded,
			NodeId:       packageId,
			ParentId:     parentId,
			DatasetId:    datasetId,
			OwnerId:      ownerId,
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
