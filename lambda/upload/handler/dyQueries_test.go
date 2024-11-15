package handler

import (
	"context"
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/dydb"
	manifestModels "github.com/pennsieve/pennsieve-go-core/pkg/models/manifest"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/uploadFile"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestUploadService iterates over and runs tests.
func TestDyQueries(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadHandlerStore,
	){
		"correctly creating uploadFiles from upload entries": testGetUploadFiles,
		"check manifest status workflow":                     testCheckUpdateManifest,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getDynamoDBClient()

			pgdbClient, err := pgdb.ConnectENV()
			if err != nil {
				log.Fatal("cannot connect to db:", err)
			}

			mSNS := test.MockSNS{}
			mS3 := test.MockS3{}
			mPusher := test.NewMockPusherClient()
			mChangelogger := &test.MockChangelogger{}
			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, ManifestFileTableName, ManifestTableName, SNSTopic, mPusher, mChangelogger)

			fn(t, store)
		})
	}
}

func testGetUploadFiles(t *testing.T, store *UploadHandlerStore) {

	// Create Manifest and ManifestFiles and then get files
	manifestId := "00000000-0000-0000-0000-000000000002"
	err := populateManifest(store, manifestId, 2)
	assert.NoError(t, err)
	files, _, err := store.dy.GetFilesPaginated(context.Background(),
		ManifestFileTableName, manifestId, sql.NullString{Valid: false},
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

func testCheckUpdateManifest(t *testing.T, store *UploadHandlerStore) {

	manifestId := "00000000-0000-0000-0000-000000000001"
	err := populateManifest(store, manifestId, 3)

	manifest, err := store.dy.GetManifestById(context.Background(), ManifestTableName, manifestId)
	assert.NoError(t, err)

	status, err := store.dy.CheckUpdateManifestStatus(context.Background(), ManifestFileTableName, ManifestTableName, manifest.ManifestId, manifest.Status)
	assert.NoError(t, err)
	assert.Equal(t, manifestModels.Initiated, status, "expected to be in INITIATED status")

	manifestFiles, _, err := store.dy.GetFilesPaginated(context.Background(), ManifestFileTableName, manifestId, sql.NullString{Valid: false}, 100, nil)
	assert.NoError(t, err)

	var uFiles []uploadFile.UploadFile
	for _, f := range manifestFiles {
		uFiles = append(uFiles, uploadFile.UploadFile{
			ManifestId: f.ManifestId,
			UploadId:   f.UploadId,
		})
	}

	// Update entries in manifest to IMPORTED for all files
	err = store.dy.updateManifestFileStatus(uFiles, manifestId)
	assert.NoError(t, err)

	status, err = store.dy.CheckUpdateManifestStatus(context.Background(), ManifestFileTableName, ManifestTableName, manifest.ManifestId, manifest.Status)
	if err != nil {
		log.Error(err)
	}
	assert.Equal(t, manifestModels.Completed, status, "expected to now be in COMPLETED status")

}

func TestGetUploadFilesUnprocessedKeys(t *testing.T) {

	manifestId := "00000000-0000-0000-0000-000000000007"
	// Create entries and expectations
	var entries []UploadEntry
	var expectedResponseValues []map[string]types.AttributeValue
	var expectedUnprocessedKeys []map[string]types.AttributeValue
	for i := 0; i < 10; i++ {
		e := UploadEntry{
			ManifestId: manifestId,
			UploadId:   uuid.NewString(),
		}
		entries = append(entries, e)

		// The expected response value corresponding to e for the mock response
		expectedResponseValue, err := attributevalue.MarshalMap(dydb.ManifestFileTable{ManifestId: e.ManifestId, UploadId: e.UploadId})
		require.NoError(t, err)
		expectedResponseValues = append(expectedResponseValues, expectedResponseValue)

		// The expected key corresponding to e for mock unprocessed key
		expectedUnprocessedKey, err := attributevalue.MarshalMap(dydb.ManifestFilePrimaryKey{
			ManifestId: e.ManifestId,
			UploadId:   e.UploadId,
		})
		require.NoError(t, err)
		expectedUnprocessedKeys = append(expectedUnprocessedKeys, expectedUnprocessedKey)
	}

	// Build expected outputs with unprocessed keys
	expectedOutputs := []*dynamodb.BatchGetItemOutput{
		{
			Responses: map[string][]map[string]types.AttributeValue{ManifestFileTableName: expectedResponseValues[0:5]},
			UnprocessedKeys: map[string]types.KeysAndAttributes{ManifestFileTableName: {
				Keys: expectedUnprocessedKeys[5:],
			}},
		},
		{
			Responses: map[string][]map[string]types.AttributeValue{ManifestFileTableName: expectedResponseValues[5:8]},
			UnprocessedKeys: map[string]types.KeysAndAttributes{ManifestFileTableName: {
				Keys: expectedUnprocessedKeys[8:],
			}},
		},
		{
			Responses:       map[string][]map[string]types.AttributeValue{ManifestFileTableName: expectedResponseValues[8:]},
			UnprocessedKeys: nil,
		},
	}

	// set up mock to return batches with unprocessed keys
	mockDy := test.MockDynamoDB{
		TestingT:            t,
		BatchGetItemOutputs: expectedOutputs,
	}
	queries := NewUploadDyQueries(&mockDy)

	results, orphanEntries, err := queries.GetUploadFiles(entries)
	assert.NoError(t, err)
	assert.Len(t, results, len(entries), "Expect the number of Upload Files to match the submitted files")
	for _, entry := range entries {
		var foundMatch bool
		for _, result := range results {
			if foundMatch = entry.ManifestId == result.ManifestId && entry.UploadId == result.UploadId; foundMatch {
				break
			}
		}
		assert.True(t, foundMatch, "no result found for entry with uploadId %s", entry.UploadId)
	}
	assert.Nil(t, orphanEntries, "We should not see any missing entries in the dynamodb")
	mockDy.AssertBatchGetItemCallCount(len(expectedOutputs))

}
