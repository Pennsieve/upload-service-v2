package handler

import (
	"context"
	"database/sql"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
)

// ServiceDyQueries is the Service Queries Struct embedding the shared Queries struct
type ServiceDyQueries struct {
	*dyQueries.Queries
	db dyQueries.DB
}

// NewServiceDyQueries returns a new instance of an ServiceDyQueries object
func NewServiceDyQueries(db dyQueries.DB) *ServiceDyQueries {
	q := dyQueries.New(db)
	return &ServiceDyQueries{
		q,
		db,
	}
}

// CheckUpdateManifestStatus checks current status of Manifest and updates if necessary.
func (q *ServiceDyQueries) CheckUpdateManifestStatus(ctx context.Context, manifestFileTableName string, manifestTableName string,
	manifestId string, currentStatus string) (manifest.Status, error) {

	// If manifest is archived, return current status.
	if currentStatus == manifest.Archived.String() {
		return manifest.Archived, nil
	}

	// Check if there are any remaining items for manifest and
	// set manifest status if not
	reqStatus := sql.NullString{
		String: "InProgress",
		Valid:  true,
	}

	setStatus := manifest.Initiated

	remaining, _, err := q.GetFilesPaginated(ctx, manifestFileTableName,
		manifestId, reqStatus, 1, nil)
	if err != nil {
		return setStatus, err
	}

	if len(remaining) == 0 {
		setStatus = manifest.Completed
		err = q.UpdateManifestStatus(ctx, manifestTableName, manifestId, setStatus)
		if err != nil {
			return setStatus, err
		}
	} else if currentStatus == "Completed" {
		setStatus = manifest.Uploading
		err = q.UpdateManifestStatus(ctx, manifestTableName, manifestId, setStatus)
		if err != nil {
			return setStatus, err
		}
	}

	return setStatus, nil

}
