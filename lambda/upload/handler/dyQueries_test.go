package handler

import (
	"context"
	"database/sql"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestUploadService iterates over and runs tests.
func TestDyQueries(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadHandlerStore,
	){
		"correctly creating uploadFiles from upload entries": testGetUploadFiles,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getDynamoDBClient()

			pgdbClient, err := pgdb.ConnectENV()
			if err != nil {
				log.Fatal("cannot connect to db:", err)
			}

			mSNS := test.MockSNS{}
			mS3 := test.MockS3{}
			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, ManifestFileTableName, ManifestTableName, SNSTopic)

			fn(t, store)
		})
	}
}

func testGetUploadFiles(t *testing.T, store *UploadHandlerStore) {

	// Create Manifest and ManifestFiles and then get files
	err := populateManifest(store)
	assert.NoError(t, err)
	files, _, err := store.dy.GetFilesPaginated(context.Background(),
		ManifestFileTableName, "00000000-0000-0000-0000-000000000000", sql.NullString{Valid: false},
		100, nil)

	// Recreate upload entries using returned files
	var entries []UploadEntry
	for _, f := range files {
		e := UploadEntry{
			ManifestId: f.ManifestId,
			UploadId:   f.UploadId,
		}
		entries = append(entries, e)
	}

	results, orphanEntries, err := store.dy.GetUploadFiles(entries)
	assert.NoError(t, err)
	assert.Len(t, results, len(entries), "Expect the number of Upload Files to match the submitted files")
	assert.Nil(t, orphanEntries, "We should not see any missing entries in the dynamodb")

	// Add non-existing entry
	entries = append(entries, UploadEntry{ManifestId: "non-existing", UploadId: "fake", S3Key: "non-existing/fake"})
	results, orphanEntries, err = store.dy.GetUploadFiles(entries)
	assert.NoError(t, err)
	assert.Len(t, results, len(entries)-1, "Expect incorrect entry to be ignored, and the number of Upload Files to match the submitted files")
	assert.Len(t, orphanEntries, 1, "We should see a single orphan entry")
	assert.Equal(t, "non-existing/fake", orphanEntries[0].S3Key)

}
