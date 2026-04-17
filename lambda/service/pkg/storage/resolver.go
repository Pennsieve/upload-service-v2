// Package storage resolves the destination storage bucket and prefix for a manifest.
//
// Today the bucket is scoped per-workspace (Organization.StorageBucket); in the
// near future it will be scoped per-dataset. Keeping the resolver keyed on
// manifestId means that switch is a single-site change.
//
// Duplicates fargate/upload-move/cache.go:DefaultStorageOrgItemQuery because
// the fargate and service-lambda Go modules pin different pennsieve-go-core
// versions. The two implementations are intentionally kept in lock-step; unify
// them when the per-dataset redesign lands.
package storage

import (
	"context"
	"fmt"

	"github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
)

// Resolution describes where files for a given manifest must be written.
type Resolution struct {
	OrganizationId int64
	DatasetId      int64
	StorageBucket  string
}

// KeyPrefix returns the full object-key prefix used for files in this manifest.
// Format: O{orgId}/D{datasetId}/{manifestId}.
func (r Resolution) KeyPrefix(manifestId string) string {
	return fmt.Sprintf("O%d/D%d/%s", r.OrganizationId, r.DatasetId, manifestId)
}

// ResolveForManifest looks up the storage bucket + org/dataset ids for a manifest.
// Falls back to defaultStorageBucket when the organization has no StorageBucket override.
func ResolveForManifest(
	ctx context.Context,
	manifestId string,
	manifestTableName string,
	defaultStorageBucket string,
	dy *dydb.Queries,
	pg *pgdb.Queries,
) (*Resolution, error) {
	manifest, err := dy.GetManifestById(ctx, manifestTableName, manifestId)
	if err != nil {
		return nil, fmt.Errorf("error getting manifest %s: %w", manifestId, err)
	}

	org, err := pg.GetOrganization(ctx, manifest.OrganizationId)
	if err != nil {
		return nil, fmt.Errorf("error getting organization %d referenced in manifest %s: %w",
			manifest.OrganizationId, manifestId, err)
	}

	bucket := defaultStorageBucket
	if org.StorageBucket.Valid {
		bucket = org.StorageBucket.String
	}

	return &Resolution{
		OrganizationId: manifest.OrganizationId,
		DatasetId:      manifest.DatasetId,
		StorageBucket:  bucket,
	}, nil
}
