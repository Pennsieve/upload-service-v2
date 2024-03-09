package handler

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	"github.com/pusher/pusher-http-go/v5"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestUploadService iterates over and runs tests.
func TestS3(t *testing.T) {
	for scenario, fn := range map[string]func(
		tt *testing.T, store *UploadHandlerStore,
	){
		"test correctly formed SQS message and S3Key":   testCorrectSQSMessage,
		"test incorrectly formed SQS message and S3Key": testInCorrectSQSMessage,
		"test duplicate keys in SQS events":             testDuplicateKey,
	} {
		t.Run(scenario, func(t *testing.T) {
			client := getDynamoDBClient()

			pgdbClient, err := pgdb.ConnectENV()
			if err != nil {
				log.Fatal("cannot connect to db:", err)
			}

			mSNS := test.MockSNS{}
			mS3 := test.MockS3{}
			store := NewUploadHandlerStore(pgdbClient, client, mSNS, mS3, ManifestFileTableName, ManifestTableName, SNSTopic, &pusher.Client{})

			fn(t, store)
		})
	}
}

func testDuplicateKey(t *testing.T, store *UploadHandlerStore) {
	manifestId := "00000000-0000-0000-0000-000000000000"
	uploadId := "00000000-1111-1111-1111-000000000000"

	validFileEvent := events.S3Event{Records: []events.S3EventRecord{
		{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{
					Name: "testBucket",
				},
				Object: events.S3Object{
					Key: fmt.Sprintf("%s/%s", manifestId, uploadId),
				},
			},
		},
	}}

	validEventBody, _ := json.Marshal(validFileEvent)

	testSQSMessages := []events.SQSMessage{
		{
			Body: string(validEventBody),
		},
		{
			Body: string(validEventBody),
		},
	}

	entries, orphanEntries, err := store.GetUploadEntries(testSQSMessages)
	assert.NoError(t, err)
	assert.Nil(t, orphanEntries, "should not have orphan entries as all messages are correctly formed.")
	assert.Len(t, entries, 1, "should have single entry as both entry represent the same event")
	assert.Equal(t, entries[0].UploadId, uploadId)

}

func testCorrectSQSMessage(t *testing.T, store *UploadHandlerStore) {

	manifestId := "00000000-0000-0000-0000-000000000000"
	uploadId := "00000000-1111-1111-1111-000000000000"

	validFileEvent := events.S3Event{Records: []events.S3EventRecord{
		{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{
					Name: "testBucket",
				},
				Object: events.S3Object{
					Key: fmt.Sprintf("%s/%s", manifestId, uploadId),
				},
			},
		},
	}}

	validEventBody, _ := json.Marshal(validFileEvent)

	testSQSMessages := []events.SQSMessage{
		{
			Body: string(validEventBody),
		},
	}

	entries, orphanEntries, err := store.GetUploadEntries(testSQSMessages)
	assert.NoError(t, err)
	assert.Nil(t, orphanEntries, "should not have orphan entreis as all messages are correctly formed.")
	assert.Len(t, entries, 1)
	assert.Equal(t, entries[0].UploadId, uploadId)

}

func testInCorrectSQSMessage(t *testing.T, store *UploadHandlerStore) {

	manifestId := "00000000-0000-0000-0000-000000000000"
	uploadId := "incorectUploadId"

	validFileEvent := events.S3Event{Records: []events.S3EventRecord{
		{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{
					Name: "testBucket",
				},
				Object: events.S3Object{
					Key: fmt.Sprintf("%s/%s", manifestId, uploadId),
				},
			},
		},
	}}

	validEventBody, _ := json.Marshal(validFileEvent)

	testSQSMessages := []events.SQSMessage{
		{
			Body: string(validEventBody),
		},
	}

	entries, orphanEntries, err := store.GetUploadEntries(testSQSMessages)
	assert.NoError(t, err)
	assert.Nil(t, entries, "should not have any entries as S3 is malformed for import")
	assert.Len(t, orphanEntries, 1)
	assert.Equal(t, orphanEntries[0].S3Key, fmt.Sprintf("%s/%s", manifestId, uploadId))

}
