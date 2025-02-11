package main

import (
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/dydb"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testItemMap = map[string]*storageOrgItem{
	uuid.NewString(): &storageOrgItem{
		organizationId: 15,
		storageBucket:  "org-15-bucket",
		datasetId:      57,
	},
	uuid.NewString(): &storageOrgItem{
		organizationId: 31,
		storageBucket:  "org-31-bucket",
		datasetId:      113,
	},
}

var actualQueryHits atomic.Int32

func mockStorageOrgItemQuery(manifestId string, _ *dydb.Queries, _ *pgdb.Queries) (*storageOrgItem, error) {
	actualQueryHits.Add(1)
	delay := time.Duration(rand.Int63n(500)+1) * time.Millisecond
	time.Sleep(delay)
	return testItemMap[manifestId], nil
}

func TestStorageOrgItemCache_ConcurrentAccess(t *testing.T) {
	cacheUnderTest := NewStorageItemCache(mockStorageOrgItemQuery)

	routines := 100
	var wg sync.WaitGroup

	for manifestId, expectedItem := range testItemMap {
		for i := 0; i < routines; i++ {
			wg.Add(1)
			go func(manifestId string, expectedItem *storageOrgItem) {
				defer func() {
					wg.Done()
				}()
				item, err := cacheUnderTest.GetOrLoad(manifestId, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, expectedItem, item)
			}(manifestId, expectedItem)
		}
	}
	wg.Wait()

	assert.Equal(t, int32(len(testItemMap)), actualQueryHits.Load())
}
