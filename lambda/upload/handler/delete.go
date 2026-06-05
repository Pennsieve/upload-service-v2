package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/pennsieve/pennsieve-go-core/pkg/packagedelete"
)

// sqs has a hard limit of 10 messages per SendMessageBatch call.
const (
	sqsBatchLimit   = 10
	sendConcurrency = 5
)

// sqsBatchAPI is the slice of the SQS client the sender needs, kept small
// so tests can fake it.
type sqsBatchAPI interface {
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

// buildDeleteRequests turns the packages created by an import into delete
// jobs for the predecessors they replaced
func buildDeleteRequests(packages []pgdb.Package, orgId int, userNodeId, traceId string) []packagedelete.DeleteRequest {
	var reqs []packagedelete.DeleteRequest
	for _, pkg := range packages {
		if !pkg.ReplacesPackageId.Valid {
			continue
		}
		reqs = append(reqs, packagedelete.DeleteRequest{
			PackageID:      pkg.ReplacesPackageId.Int64,
			OrganizationID: int64(orgId),
			UserNodeID:     userNodeId,
			TraceID:        traceId,
		})
	}
	return reqs
}

// sqsQueueSender implements pennsieve-go-core's packagedelete.QueueSender.
// go-core hands it all the delete messages at once; it chunks them into
// SQS batches and sends those in parallel.
type sqsQueueSender struct {
	client   sqsBatchAPI
	queueURL string
}

func (s *sqsQueueSender) SendToQueue(ctx context.Context, bodies [][]byte) error {
	if len(bodies) == 0 {
		return nil
	}

	// Collect failures from all batches rather than just the first, so a
	// partial outage shows every batch that didn't make it.
	var (
		mu   sync.Mutex
		errs []error
	)
	recordErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}

	// used tp limit new goroutines to sendConcurrency at a time
	countingSemaphore := make(chan struct{}, sendConcurrency)
	var wg sync.WaitGroup

	for start := 0; start < len(bodies); start += sqsBatchLimit {
		end := start + sqsBatchLimit
		if end > len(bodies) {
			end = len(bodies)
		}
		chunk := bodies[start:end]

		wg.Add(1)

		countingSemaphore <- struct{}{}
		go func(chunk [][]byte) {
			defer wg.Done()
			//free up space on channel
			defer func() { <-countingSemaphore }()

			entries := make([]sqsTypes.SendMessageBatchRequestEntry, len(chunk))
			for i, body := range chunk {
				// Entry Id just has to be unique within the batch.
				entries[i] = sqsTypes.SendMessageBatchRequestEntry{
					Id:          aws.String("m" + strconv.Itoa(i)),
					MessageBody: aws.String(string(body)),
				}
			}

			out, err := s.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
				QueueUrl: aws.String(s.queueURL),
				Entries:  entries,
			})
			if err != nil {
				recordErr(fmt.Errorf("SendMessageBatch: %w", err))
				return
			}
			if len(out.Failed) > 0 {
				recordErr(fmt.Errorf("SendMessageBatch: %d of %d messages failed (first: %s)",
					len(out.Failed), len(entries), aws.ToString(out.Failed[0].Message)))
			}
		}(chunk)
	}

	wg.Wait()
	// errors.Join returns nil when errs is empty, so a fully-successful
	// run returns nil.
	return errors.Join(errs...)
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
