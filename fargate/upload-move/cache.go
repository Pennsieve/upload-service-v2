package main

import (
	"context"
	"fmt"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"sync"
)

type StorageOrgItemQuery func(manifestId string) (*storageOrgItem, error)

type StorageOrgItemCache struct {
	m     sync.Map
	mutex sync.Mutex
	query StorageOrgItemQuery
}

func NewStorageItemCache(loader StorageOrgItemQuery) *StorageOrgItemCache {
	return &StorageOrgItemCache{
		query: loader,
	}
}

func (c *StorageOrgItemCache) GetOrLoad(manifestId string) (*storageOrgItem, error) {
	c.mutex.Lock()
	defer func() {
		c.mutex.Unlock()
	}()
	if item, found := c.m.Load(manifestId); found {
		return item.(*storageOrgItem), nil
	}

	item, err := c.query(manifestId)
	if err != nil {
		return nil, fmt.Errorf("error loading item for manifestId %s: %w", manifestId, err)
	}
	c.m.LoadOrStore(manifestId, item)
	return item, nil
}

func makeDefaultStorageOrgItemQuery(dy *dydb.Queries, pg *pgdb.Queries) StorageOrgItemQuery {
	return func(manifestId string) (*storageOrgItem, error) {
		// Get manifest from dynamodb based on id
		manifest, err := dy.GetManifestById(context.Background(), TableName, manifestId)
		if err != nil {
			err := fmt.Errorf("error getting manifest %s: %w", manifestId, err)
			return nil, err
		}

		//var o dbTable.Organization
		org, err := pg.GetOrganization(context.Background(), manifest.OrganizationId)
		if err != nil {
			err := fmt.Errorf("error getting organization %d referenced in manifest %s: %w",
				manifest.OrganizationId,
				manifestId,
				err)
			return nil, err
		}

		// Return storagebucket if defined, or default bucket.
		sbName := defaultStorageBucket
		if org.StorageBucket.Valid {
			sbName = org.StorageBucket.String
		}

		si := storageOrgItem{
			organizationId: manifest.OrganizationId,
			storageBucket:  sbName,
			datasetId:      manifest.DatasetId,
		}

		return &si, nil
	}
}
