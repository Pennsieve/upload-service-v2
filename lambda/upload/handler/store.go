package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pennsieve/pennsieve-go-core/pkg/domain"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFolder"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
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
func NewUploadHandlerStore(db *sql.DB, dy *dynamodb.Client, sns domain.SnsAPI, s3 domain.S3API, fileTableName string, tableName string, snsTopic string) *UploadHandlerStore {
	return &UploadHandlerStore{
		pgdb:          db,
		dynamodb:      dy,
		SNSClient:     sns,
		SNSTopic:      snsTopic,
		S3Client:      s3,
		pg:            NewUploadPgQueries(db),
		dy:            NewUploadDyQueries(dy),
		fileTableName: fileTableName,
		tableName:     tableName,
	}
}

// WithOrg sets the search path for the pg queries
func (s *UploadHandlerStore) WithOrg(orgId int) error {
	_, err := s.pg.WithOrg(orgId)
	return err

}

// execTx wraps a function in a transaction.
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
func (s *UploadHandlerStore) ImportFiles(ctx context.Context, datasetId int, orgId int, ownerId int, files []uploadFile.UploadFile, manifest *dydb.ManifestTable) error {

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
		folderMap := getUploadFolderMap(files, "")

		// 2. Iterate over folders and create them if they do not exist in organization
		folderPackageMap, err := qtx.GetCreateUploadFolders(datasetId, ownerId, folderMap)
		if err != nil {
			log.Error("Unable to create folders in ImportFiles function: ", err)
			return err
		}

		// 3. Create Package Params to add files to "packages" table.
		pkgParams, err := getPackageParams(datasetId, ownerId, files, folderPackageMap)
		if err != nil {
			log.Error("Unable to parse package parameters: ", err)
			return err
		}

		packages, err := qtx.AddPackages(context.Background(), pkgParams)
		if err != nil {
			log.Error("Error creating a package: ", err)
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

		// Update storage for packages, datasets, and org
		err = qtx.UpdateStorage(allFileParams, packages, manifest.DatasetId, manifest.OrganizationId)
		if err != nil {
			return err
		}

		// Notify SNS that files were imported.
		err = s.PublishToSNS(returnedFiles)
		if err != nil {
			log.Error("Error with notifying SNS that records are imported.", err)
		}

		// Update activity Log
		var evnts []interface{}
		for _, pkg := range packages {
			event := changelog.PackageCreateEvent{
				Id:     pkg.Id,
				Name:   pkg.Name,
				NodeId: pkg.NodeId,
			}
			evnts = append(evnts, event)
		}

		params := changelog.MessageParams{
			OrganizationId: int64(orgId),
			DatasetId:      int64(datasetId),
			UserId:         int64(ownerId),
			Events:         evnts,
		}

		log.Info(params)

		err = ChangelogClient.EmitEvents(context.Background(), params)
		if err != nil {
			log.Error("Error with notifying Changelog about imported records: ", err)
		}

		return nil
	})

	return err
}

// Handler is the primary entrypoint that handles importing files and is called by the Lambda
func (s *UploadHandlerStore) Handler(ctx context.Context, sqsEvent events.SQSEvent) (events.SQSEventResponse, error) {
	/*
		Messages can be from multiple upload sessions --> multiple organizations.
		We need to:
			1. Separate by manifest-session
			2. Create/get the folders from postgres for each upload session
			3. Create Packages
			4. Create Files in Packages
	*/

	// Response can include list of failed SQS messages

	var batchItemFailures []events.SQSBatchItemFailure
	response := events.SQSEventResponse{
		BatchItemFailures: batchItemFailures,
	}

	// Map SQS Events by s3Key - This is used in case of failed imports.
	s3KeySQSMessageMap := map[string]events.SQSMessage{}
	for _, m := range sqsEvent.Records {
		parsedS3Event := events.S3Event{}
		_ = json.Unmarshal([]byte(m.Body), &parsedS3Event)
		s3Key := parsedS3Event.Records[0].S3.Object.Key
		s3KeySQSMessageMap[s3Key] = m
	}

	// 1. Parse UploadEntries
	uploadEntries, orphanEntries, err := s.GetUploadEntries(sqsEvent.Records)
	if orphanEntries != nil {
		log.Info("Files uploaded that do not follow the correct s3Key format and don't belong to manifest.")
		// These files are unexpected and do not follow the expected S3Key format for Pennsieve uploads
		err := s.deleteOrphanFiles(orphanEntries)
		if err != nil {
			log.Error("unable to delete orphan files")
		}
	}
	if err != nil {
		// This really should never happen --> Somehow the SQS queue received a non-S3 message.
		log.Error(err.Error())
		return response, err
	}

	// 2. Match against Manifest and create uploadFiles
	uploadFiles, orphanEntries, err := s.dy.GetUploadFiles(uploadEntries)
	if orphanEntries != nil {
		log.Info("Files uploaded that don't belong to a manifest.")
		// These files somehow did parse correctly in the GetUploadEntries method.
		err := s.deleteOrphanFiles(orphanEntries)
		if err != nil {
			log.Error("unable to delete orphan files")
		}
	}
	if err != nil {
		log.Error("Error with GetUploadFiles: ", err)
		return response, err
	}

	// 3. Map by uploadSessionID
	var fileByManifest = map[string][]uploadFile.UploadFile{}
	for _, f := range uploadFiles {
		fileByManifest[f.ManifestId] = append(fileByManifest[f.ManifestId], f)
	}

	// 4. Iterate over different import sessions and import files.
	for manifestId, uploadFilesForManifest := range fileByManifest {

		// Get manifest from dynamodb
		manifest, err := s.dy.GetManifestById(ctx, s.tableName, manifestId)
		if err != nil {
			log.Error("GetManifestById: Unable to get manifest.", err)
			batchItemFailures = addToFailedFiles(uploadFilesForManifest, s3KeySQSMessageMap, batchItemFailures)
			continue
		}

		err = s.WithOrg(int(manifest.OrganizationId))
		if err != nil {
			log.Error("Unable to set search path.", err)
			batchItemFailures = addToFailedFiles(uploadFilesForManifest, s3KeySQSMessageMap, batchItemFailures)
			continue
		}

		err = s.ImportFiles(ctx, int(manifest.DatasetId), int(manifest.OrganizationId), int(manifest.UserId), uploadFilesForManifest, manifest)
		if err != nil {
			log.Error("Unable to create packages: ", err)
			batchItemFailures = addToFailedFiles(uploadFilesForManifest, s3KeySQSMessageMap, batchItemFailures)
			continue
		}

		// Update entries in manifest to IMPORTED for all files
		err = s.dy.updateManifestFileStatus(uploadFilesForManifest, manifestId)
		if err != nil {
			// Status is not correctly updated in Manifest but files are completely imported.
			// This should not return the failed files.
			log.Error(err)
			continue
		}

	}

	response.BatchItemFailures = batchItemFailures
	return response, nil
}

// deleteOrphanFiles deletes files from upload bucket if no representation exists in manifest.
func (s *UploadHandlerStore) deleteOrphanFiles(files []OrphanS3File) error {

	ctx := context.Background()

	// Assert all buckets are the same
	s3Bucket := files[0].S3Bucket

	var keys []s3Types.ObjectIdentifier
	for _, f := range files {
		if f.S3Bucket != s3Bucket {
			return errors.New("not all orphan files have the same bucket")
		}
		keys = append(keys, s3Types.ObjectIdentifier{
			Key:       aws.String(f.S3Key),
			VersionId: nil,
		})
	}

	f := s3Types.Delete{
		Objects: keys,
		Quiet:   false,
	}

	params := s3.DeleteObjectsInput{
		Bucket: aws.String(s3Bucket),
		Delete: &f,
	}

	result, err := s.S3Client.DeleteObjects(ctx, &params)
	if err != nil {
		return err
	}
	if len(result.Deleted) != len(files) {
		return errors.New("unable to delete all orphan files")
	}

	return nil

}

// getUploadFolderMap returns an object that maps path name to Folder object.
func getUploadFolderMap(sortedFiles []uploadFile.UploadFile, targetFolder string) uploadFolder.UploadFolderMap {

	// Mapping path from targetFolder to UploadFolder Object
	var folderNameMap = map[string]*uploadFolder.UploadFolder{}

	// Iterate over the files and create the UploadFolder objects.
	for _, f := range sortedFiles {

		if f.Path == "" {
			continue
		}

		// Prepend the target-Folder if it exists
		p := f.Path
		if targetFolder != "" {
			p = strings.Join(
				[]string{
					targetFolder, p,
				}, "/")
		}

		// Remove leading and trailing "/"
		leadingSlashes := regexp.MustCompile(`^/+`)
		p = leadingSlashes.ReplaceAllString(p, "")

		trailingSlashes := regexp.MustCompile(`/+$`)
		p = trailingSlashes.ReplaceAllString(p, "")

		// Iterate over path segments in a single file and identify folders.
		pathSegments := strings.Split(p, "/")
		absoluteSegment := "" // Current location in the path walker for current file.
		currentNodeId := ""
		currentFolderPath := ""
		for depth, segment := range pathSegments {

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

				folder = &uploadFolder.UploadFolder{
					NodeId:       currentNodeId,
					Name:         segment,
					ParentNodeId: parentNodeId,
					ParentId:     -1,
					Depth:        depth,
				}
				folderNameMap[absoluteSegment] = folder
			}

			// Add current segment to parent if exist
			if parentFolderPath != "" {
				folderNameMap[parentFolderPath].Children = append(folderNameMap[parentFolderPath].Children, folder)
			}

		}
	}

	return folderNameMap
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
			return nil, err
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

		// Select Package State
		// UPLOADED if there is a workflow associated with the package type
		// READY is there is no workflow associated with the package type
		setPackageState := packageState.Uploaded
		if !packageType.FileTypeToInfoDict[file.FileType].HasWorkflow {
			setPackageState = packageState.Ready
		}

		pkgParam := pgdb.PackageParams{
			Name:         packageName,
			PackageType:  file.Type,
			PackageState: setPackageState,
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
	for i := range pkgParamsMap {
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
		r := regexp.MustCompile(`(?P<FileName>[^.]*)?\.?(?P<Extension>.*)`)
		pathParts := r.FindStringSubmatch(file.Name)
		if pathParts == nil {
			log.Error("Unable to parse filename:", file.Name)
			return "", "", errors.New(fmt.Sprintf("Unable to parse filename: %s", file.Name))
		}

		packageName = pathParts[r.SubexpIndex("FileName")]
	}

	return packageId, packageName, nil
}

// addToFailedFiles appends array of upload files to failed SQS messages
func addToFailedFiles(files []uploadFile.UploadFile, s3KeySQSMessageMap map[string]events.SQSMessage,
	failures []events.SQSBatchItemFailure) []events.SQSBatchItemFailure {
	for _, f := range files {
		failedMessage := events.SQSBatchItemFailure{
			ItemIdentifier: s3KeySQSMessageMap[f.S3Key].MessageId}

		failures = append(failures, failedMessage)

	}
	return failures
}
