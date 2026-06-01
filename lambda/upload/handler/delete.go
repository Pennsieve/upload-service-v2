package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// sqsQueueSender makes an SQS client that gets sent to pennsieve-go-core
type sqsQueueSender struct {
	client   *sqs.Client
	queueURL string
}

func (s *sqsQueueSender) SendToQueue(ctx context.Context, body []byte) error {
	_, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.queueURL),
		MessageBody: aws.String(string(body)),
	})
	return err
}

// emitReplacementMetrics writes a CloudWatch EMF record so ops can
// dashboard + alarm on replacement volume without a separate
// PutMetricData call. Pair with an alarm on the delete queue's
// ApproximateAgeOfOldestMessage to detect consumer-side lag.
//
// ReplacementCount = number of predecessors soft-deleted in this
//
//	ImportFiles invocation (batch).
//
// ReplacementPublishFailures = 1 if DeletePackages returned any error
//
//	for this batch, 0 otherwise. A Count dimension averaged over time
//	gives the failure rate.
func emitReplacementMetrics(replacementCount, publishFailures int) {
	emf := map[string]any{
		"_aws": map[string]any{
			"Timestamp": time.Now().UnixMilli(),
			"CloudWatchMetrics": []map[string]any{{
				"Namespace":  "UploadService/Replace",
				"Dimensions": [][]string{{}},
				"Metrics": []map[string]string{
					{"Name": "ReplacementCount", "Unit": "Count"},
					{"Name": "ReplacementPublishFailures", "Unit": "Count"},
				},
			}},
		},
		"ReplacementCount":           replacementCount,
		"ReplacementPublishFailures": publishFailures,
	}
	line, _ := json.Marshal(emf)
	// stdout so the Lambda runtime picks up the line and CloudWatch parses
	// the embedded _aws block as a metric record.
	fmt.Println(string(line))
}
