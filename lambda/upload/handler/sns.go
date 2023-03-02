package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	log "github.com/sirupsen/logrus"
)

func (s *UploadHandlerStore) sendSNSMessages(snsEntries []types.PublishBatchRequestEntry) error {
	log.Debug("Number of SNS messages: ", len(snsEntries))

	if len(snsEntries) > 0 {
		params := sns.PublishBatchInput{
			PublishBatchRequestEntries: snsEntries,
			TopicArn:                   aws.String(s.SNSTopic),
		}
		_, err := s.SNSClient.PublishBatch(context.Background(), &params)
		if err != nil {
			log.Error("Error publishing to SNS: ", err)
			return err
		}
	}

	return nil
}

// PublishToSNS publishes messages to SNS after files are imported.
func (s *UploadHandlerStore) PublishToSNS(files []pgdb.File) error {

	const batchSize = 10
	var snsEntries []types.PublishBatchRequestEntry
	for _, f := range files {
		e := types.PublishBatchRequestEntry{
			Id:      aws.String(f.UUID.String()),
			Message: aws.String(fmt.Sprintf("%d", f.PackageId)),
		}
		snsEntries = append(snsEntries, e)

		// Send SNS messages in blocks of batchSize
		if len(snsEntries) == batchSize {
			err := s.sendSNSMessages(snsEntries)
			if err != nil {
				return err
			}
			snsEntries = nil
		}
	}

	// send remaining entries
	err := s.sendSNSMessages(snsEntries)

	return err
}
