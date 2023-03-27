package handler

import (
	"context"
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	dyQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
)

// ServiceDyQueries is the Service Queries Struct embedding the shared Queries struct
type ServiceDyQueries struct {
	*dyQueries.Queries
	db *dynamodb.Client
}

// NewServiceDyQueries returns a new instance of an ServiceDyQueries object
func NewServiceDyQueries(db *dynamodb.Client) *ServiceDyQueries {
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

// DeleteManifest deletes a manifest from the manifest table
func (q *ServiceDyQueries) DeleteManifest(ctx context.Context, manifestTableName string, manifestId string) error {

	// Check existing manifest exist and get status
	m, err := q.GetManifestById(ctx, manifestTableName, manifestId)
	if err != nil {
		return &ManifestNotExistError{
			id: manifestId,
		}
	}

	// Only allow deleting when manifest is archived.
	if m.Status != manifest.Archived.String() {
		return &ManifestNotArchivedError{
			id:     manifestId,
			status: m.Status,
		}
	}

	_, err = q.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"ManifestId": &types.AttributeValueMemberS{Value: manifestId},
		},
		TableName: aws.String(manifestTableName),
	})
	if err != nil {
		return err
	}

	return nil
}
