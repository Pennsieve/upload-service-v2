package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	log "github.com/sirupsen/logrus"
	"regexp"
)

// GetUploadEntries parses the events from SQS into meaningful objects
func (s *UploadHandlerStore) GetUploadEntries(fileEvents []events.SQSMessage) ([]UploadEntry, []OrphanS3File, error) {

	var entries []UploadEntry
	var orphanFiles []OrphanS3File

	var uploadIdMap = map[string]struct{}{}

	for _, message := range fileEvents {
		parsedS3Event := events.S3Event{}
		if err := json.Unmarshal([]byte(message.Body), &parsedS3Event); err != nil {
			log.Error("unexpected error, could not parse event send by S3")
			return nil, nil, fmt.Errorf("failed to unmarshal message, %v", err)
		}

		entry, err := s.uploadEntryFromS3Event(&parsedS3Event)
		if err != nil {
			switch err.(type) {
			case *S3FileNotExistError:
				parsedErr := err.(*S3FileNotExistError)
				log.WithFields(
					log.Fields{
						"manifest_id": parsedErr.ManifestId,
						"upload_id":   parsedErr.UploadId,
					},
				).Warn(fmt.Sprintf("Unable to get HEAD object %s / %s",
					parsedErr.S3Bucket, parsedErr.S3Key))

				// Ignore sqs event as the file does not exist (for some reason)
				continue
			case *S3FileMalFormedError:
				orphanFile := OrphanS3File{
					S3Bucket: parsedS3Event.Records[0].S3.Bucket.Name,
					S3Key:    parsedS3Event.Records[0].S3.Object.Key,
					ETag:     parsedS3Event.Records[0].S3.Object.ETag,
				}

				orphanFiles = append(orphanFiles, orphanFile)
				log.Warn(fmt.Sprintf("Unable to parse s3-key %s into expected format: ", orphanFile.S3Key), err)

				// Do not add sqs event to records as it is already added to the OrphanFiles list
				continue
			}

		}

		if _, found := uploadIdMap[entry.UploadId]; found {
			// Ignore upload entry and send warning log.
			log.Warn(fmt.Sprintf("Duplicate UploadID found in SQS events: %s/%s", entry.ManifestId, entry.UploadId))
		} else {
			entries = append(entries, *entry)
			uploadIdMap[entry.UploadId] = struct{}{}
		}

	}

	return entries, orphanFiles, nil
}

// uploadEntryFromS3Event returns an object representing an uploaded file from an S3 Event.
func (s *UploadHandlerStore) uploadEntryFromS3Event(event *events.S3Event) (*UploadEntry, error) {
	r := regexp.MustCompile(`(?P<Manifest>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})\/(?P<UploadId>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})`)
	res := r.FindStringSubmatch(event.Records[0].S3.Object.Key)

	if res == nil {
		return nil, &S3FileMalFormedError{
			S3Bucket: event.Records[0].S3.Bucket.Name,
			S3Key:    event.Records[0].S3.Object.Key,
		}
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

		return nil, &S3FileNotExistError{
			S3Bucket:   s3Bucket,
			S3Key:      s3Key,
			ManifestId: manifestId,
			UploadId:   uploadId,
		}
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
