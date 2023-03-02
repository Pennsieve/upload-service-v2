package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
	"regexp"
)

// GetUploadEntries parses the events from SQS into meaningful objects
func (s *UploadHandlerStore) GetUploadEntries(fileEvents []events.SQSMessage) ([]UploadEntry, error) {

	var entries []UploadEntry
	for _, message := range fileEvents {
		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(message.Body), &parsedS3Event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message, %v", err)
		}

		entry, err := s.uploadEntryFromS3Event(&parsedS3Event)
		if err != nil {
			log.Error("Unable to parse s3-key: ", err)
			continue
		}

		entries = append(entries, *entry)

	}

	return entries, nil
}

// uploadEntryFromS3Event returns an object representing an uploaded file from an S3 Event.
func (s *UploadHandlerStore) uploadEntryFromS3Event(event *events.S3Event) (*UploadEntry, error) {
	r := regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<UploadId>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})`)
	res := r.FindStringSubmatch(event.Records[0].S3.Object.Key)

	if res == nil {
		return nil, errors.New(fmt.Sprintf("File does not contain the required S3-Key components: %s",
			event.Records[0].S3.Object.Key))
	}

	// Found standard upload manifest/key combination
	manifestId := res[r.SubexpIndex("Manifest")]
	uploadId := res[r.SubexpIndex("UploadId")]

	s3Bucket := event.Records[0].S3.Bucket.Name
	s3Key := event.Records[0].S3.Object.Key

	// Get File Size
	headObj := s3.HeadObjectInput{
		Bucket:       aws.String(s3Bucket),
		Key:          aws.String(s3Key),
		ChecksumMode: s3Types.ChecksumModeEnabled,
	}
	result, err := s.S3Client.HeadObject(context.Background(), &headObj)
	if err != nil {
		log.Println(err)
		log.WithFields(
			log.Fields{
				"manifest_id": manifestId,
				"upload_id":   uploadId,
			},
		).Warn(fmt.Sprintf("Unable to get HEAD object %s / %s", s3Bucket, s3Key))
	}

	response := UploadEntry{
		S3Bucket:   s3Bucket,
		S3Key:      s3Key,
		ManifestId: manifestId,
		UploadId:   uploadId,
		ETag:       event.Records[0].S3.Object.ETag,
		Size:       event.Records[0].S3.Object.Size,
		Sha256:     checkSumOrEmpty(result.ChecksumSHA256),
	}

	log.WithFields(
		log.Fields{
			"manifest_id": response.ManifestId,
			"upload_id":   response.UploadId,
		},
	).Debugf("UploadEntry created in %s / %s", response.S3Bucket, response.S3Key)
	return &response, nil
}

func checkSumOrEmpty(checkSum *string) string {
	if checkSum != nil {
		return *checkSum
	}
	return ""
}
