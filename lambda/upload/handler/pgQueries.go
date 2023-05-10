package handler

import (
	"context"
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageState"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/packageInfo/packageType"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFolder"
	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	log "github.com/sirupsen/logrus"
	"sort"
)

// UploadPgQueries is the UploadHandler Queries Struct embedding the shared Queries struct
type UploadPgQueries struct {
	*pgQueries.Queries
	db pgQueries.DBTX
}

// NewUploadPgQueries returns a new instance of an UploadPgQueries object
func NewUploadPgQueries(db pgQueries.DBTX) *UploadPgQueries {
	q := pgQueries.New(db)
	return &UploadPgQueries{
		q,
		db,
	}
}

// GetCreateUploadFolders creates new folders in the organization.
// It updates UploadFolders with real folder ID for folders that already exist.
// Assumes map keys are absolute paths in the dataset
func (q *UploadPgQueries) GetCreateUploadFolders(datasetId int, ownerId int, folders uploadFolder.UploadFolderMap) (pgdb.PackageMap, error) {

	contextLogger := log.WithFields(log.Fields{
		"service": "Upload-service",
	})

	// Get Root Folders
	p := pgdb.Package{}
	rootChildren, err := q.GetPackageChildren(context.Background(), &p, datasetId, true)
	if err != nil {
		contextLogger.WithFields(
			log.Fields{
				"dataset_id": datasetId,
			}).Error("Error getting root folders:  ", err)
		return nil, err
	}

	// Map NodeId to Packages for folders that exist in DB
	existingFolders := pgdb.PackageMap{}
	for _, k := range rootChildren {
		existingFolders[k.Name] = k
	}

	// Sort the keys of the provided uploadFolderMap, so we can iterate over the sorted map
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
			children, _ := q.GetPackageChildren(context.Background(), &folder, datasetId, true)
			if err != nil {
				contextLogger.WithFields(
					log.Fields{
						"dataset_id":       datasetId,
						"folder":           folder.Name,
						"folder_parent_id": folder.ParentId,
					}).Error("Error getting children for folder:  ", err)
				return nil, err
			}
			for _, k := range children {
				p := fmt.Sprintf("%s/%s", path, k.Name)
				existingFolders[p] = k
			}

		} else {
			// Create folder
			pkgParams := pgdb.PackageParams{
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

			result, err := q.AddFolder(context.Background(), pkgParams)
			if err != nil {
				contextLogger.WithFields(
					log.Fields{
						"dataset_id": datasetId,
						"folder":     folders[path].Name,
					}).Error("Error adding folder to package:  ", err)
				return nil, err
			}

			folders[path].Id = result.Id
			existingFolders[path] = *result

			for _, childFolder := range folders[path].Children {
				childFolder.ParentId = result.Id
				childFolder.ParentNodeId = result.NodeId
			}
		}
	}

	return existingFolders, nil
}

// UpdateStorage updates storage in packages, dataset and organization for uploaded package
// 	* Typically needs to be wrapped in Transaction as this contains multiple insert queries.
func (q *UploadPgQueries) UpdateStorage(files []pgdb.FileParams, packages []pgdb.Package, datasetId int64, orgId int64) error {

	ctx := context.Background()
	contextLogger := log.WithFields(log.Fields{
		"service": "Upload-service",
	})

	packageMap := map[int]pgdb.Package{}
	for _, p := range packages {
		packageMap[int(p.Id)] = p
	}

	// Update all packageSize
	for _, f := range files {

		err := q.IncrementPackageStorage(ctx, int64(f.PackageId), f.Size)
		if err != nil {
			contextLogger.Error("Error incrementing package")
			return err
		}

		pkg := packageMap[f.PackageId]
		if pkg.ParentId.Valid {
			err = q.IncrementPackageStorageAncestors(ctx, pkg.ParentId.Int64, f.Size)
			if err != nil {
				contextLogger.Error("Error incrementing package ancestors")
				return err
			}
		}

		err = q.IncrementDatasetStorage(ctx, datasetId, f.Size)
		if err != nil {
			contextLogger.Error("Error incrementing dataset.")
			return err
		}

		err = q.IncrementOrganizationStorage(ctx, orgId, f.Size)
		if err != nil {
			contextLogger.Error("Error incrementing organization")
			return err
		}
	}

	return nil
}
