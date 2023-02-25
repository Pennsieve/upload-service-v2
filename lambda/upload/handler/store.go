package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/domain"
	dynamoStore "github.com/pennsieve/pennsieve-go-core/pkg/dynamodb"
	//"github.com/pennsieve/pennsieve-go-core/pkg/dynamodb"
	dynamoModels "github.com/pennsieve/pennsieve-go-core/pkg/dynamodb/models"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/fileInfo/objectType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFolder"
	"github.com/pennsieve/pennsieve-go-core/pkg/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/pgdb/models"
	log "github.com/sirupsen/logrus"
	"sort"
)

// UploadHandlerStore provides the Queries interface and a db instance.
type UploadHandlerStore struct {
	pg        *pgdb.Queries
	dy        *dynamoStore.Queries
	pgdb      *sql.DB
	dynamodb  *dynamodb.Client
	SNSClient domain.SnsAPI
	SNSTopic  string
}

// NewUploadHandlerStore returns a UploadHandlerStore object which implements the Queires
func NewUploadHandlerStore(db *sql.DB, dy *dynamodb.Client, sns domain.SnsAPI) *UploadHandlerStore {
	return &UploadHandlerStore{
		pgdb:      db,
		dynamodb:  dy,
		SNSClient: sns,
		pg:        pgdb.New(db),
		dy:        dynamoStore.New(dy),
	}
}

func (s *UploadHandlerStore) execTx(ctx context.Context, fn func(*pgdb.Queries) error) error {
	tx, err := s.pgdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := pgdb.New(tx)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

// GetCreateUploadFolders creates new folders in the organization.
// It updates UploadFolders with real folder ID for folders that already exist.
// Assumes map keys are absolute paths in the dataset
func (s *UploadHandlerStore) GetCreateUploadFolders(datasetId int, ownerId int, folders uploadFolder.UploadFolderMap) models.PackageMap {

	// Get Root Folders
	p := models.Package{}
	rootChildren, _ := s.pg.GetPackageChildren(context.Background(), &p, datasetId, true)

	// Map NodeId to Packages for folders that exist in DB
	existingFolders := models.PackageMap{}
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
			children, _ := s.pg.GetPackageChildren(context.Background(), &folder, datasetId, true)
			for _, k := range children {
				p := fmt.Sprintf("%s/%s", path, k.Name)
				existingFolders[p] = k
			}

		} else {
			// Create folder
			pkgParams := models.PackageParams{
				Name:         folders[path].Name,
				PackageType:  packageType.Collection,
				PackageState: packageState.Ready,
				NodeId:       folders[path].NodeId,
				ParentId:     folders[path].ParentId,
				DatasetId:    datasetId,
				OwnerId:      ownerId,
				Size:         0,
				Attributes:   nil,
			}

			result, _ := s.pg.AddPackages(context.Background(), []models.PackageParams{pkgParams})
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

// PublishToSNS publishes messages to SNS after files are imported.
func (s *UploadHandlerStore) PublishToSNS(files []models.File) error {

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
			err := s.sendSNSMessages(snsEntries)
			if err != nil {
				return err
			}
			snsEntries = nil
		}
	}

	// send remaining entries
	err := s.sendSNSMessages(snsEntries)

	return err
}

func (s *UploadHandlerStore) sendSNSMessages(snsEntries []types.PublishBatchRequestEntry) error {
	log.Debug("Number of SNS messages: ", len(snsEntries))

	if len(snsEntries) > 0 {
		params := sns.PublishBatchInput{
			PublishBatchRequestEntries: snsEntries,
			TopicArn:                   aws.String(manifestSession.SNSTopic),
		}
		_, err := s.SNSClient.PublishBatch(context.Background(), &params)
		if err != nil {
			log.Error("Error publishing to SNS: ", err)
			return err
		}
	}

	return nil
}

// ImportFiles creates rows for uploaded files in Packages and Files tables as a transaction
func (s *UploadHandlerStore) ImportFiles(ctx context.Context, datasetId int, ownerId int, files []uploadFile.UploadFile, manifest *dynamoModels.ManifestTable) error {

	err := s.execTx(ctx, func(q *pgdb.Queries) error {
		// TODO: add packages
		var f uploadFile.UploadFile
		f.Sort(files)

		// 1. Iterate over files and return map of folders and subfolders
		folderMap := f.GetUploadFolderMap(files, "")

		// 2. Iterate over folders and create them if they do not exist in organization
		folderPackageMap := s.GetCreateUploadFolders(datasetId, ownerId, folderMap)

		// 3. Create Package Params to add files to packages table.
		pkgParams, _ := getPackageParams(datasetId, ownerId, files, folderPackageMap)

		packages, err := s.pg.AddPackages(context.Background(), pkgParams)
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
		packageMap := map[string]models.Package{}
		for _, p := range packages {
			packageMap[p.NodeId] = p
		}

		var allFileParams []models.FileParams
		for i, f := range files {
			packageNodeId := fmt.Sprintf("N:package:%s", f.UploadId)
			if len(files[i].MergePackageId) > 0 {
				log.Debug("USING MERGED PACKAGE")
				packageNodeId = fmt.Sprintf("N:package:%s", files[i].MergePackageId)
			}

			file := models.FileParams{
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

		returnedFiles, err := s.pg.AddFiles(context.Background(), allFileParams)
		if err != nil {
			return err
		}

		// Notify SNS that files were imported.
		err = s.PublishToSNS(returnedFiles)
		if err != nil {
			log.Error("Error with notifying SNS that records are imported.", err)
		}

		// Update storage for packages, datasets, and org
		s.UpdateStorage(allFileParams, packages, manifest)

		// TODO: add files
		return nil
	})

	return err
}

// UpdateStorage updates storage in packages, dataset and organization for uploaded package
func (s *UploadHandlerStore) UpdateStorage(files []models.FileParams, packages []models.Package, manifest *dynamoModels.ManifestTable) error {

	packageMap := map[int]models.Package{}
	for _, p := range packages {
		packageMap[int(p.Id)] = p
	}

	ctx := context.Background()

	// Create new store for Pennsieve (non-org schema)
	dbOrg, err := pgdb.ConnectRDS()
	if err != nil {
		log.Error("Error connecting to database.")
		return err
	}
	defer dbOrg.Close()
	PennsieveStore := NewUploadHandlerStore(dbOrg, s.dynamodb, s.SNSClient)

	// Update all packageSize
	for _, f := range files {

		err = s.pg.IncrementPackageStorage(ctx, int64(f.PackageId), f.Size)
		if err != nil {
			log.Error("Error incrementing package")
			return err
		}

		pkg := packageMap[f.PackageId]
		if pkg.ParentId.Valid {
			s.pg.IncrementPackageStorageAncestors(ctx, pkg.ParentId.Int64, f.Size)
			if err != nil {
				log.Error("Error incrementing package ancestors")
				return err
			}
		}

		err = s.pg.IncrementDatasetStorage(ctx, manifest.DatasetId, f.Size)
		if err != nil {
			log.Error("Error incrementing dataset.")
			return err
		}

		err = PennsieveStore.pg.IncrementOrganizationStorage(ctx, manifest.OrganizationId, f.Size)
		if err != nil {
			log.Error("Error incrementing organization")
			return err
		}
	}

	return nil
}
