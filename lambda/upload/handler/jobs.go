package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

// Scala's pennsieve-api `BackgroundJob` sealed trait serializes with Circe's
// wrapped-discriminator format — the class name is the outer JSON key and
// the fields live in a nested object. TraceId is a derived-unwrapped
// value class, so it is emitted as a bare string, not an object. See
// pennsieve-api/core/src/main/scala/com/pennsieve/messages/BackgroundJob.scala.
// Exported for the external test package that pins the wire format.
type DeletePackageJobInner struct {
	PackageId      int64  `json:"packageId"`
	OrganizationId int    `json:"organizationId"`
	UserId         string `json:"userId"`
	TraceId        string `json:"traceId"`
	Id             string `json:"id"`
}

type DeletePackageJobEnvelope struct {
	DeletePackageJob DeletePackageJobInner `json:"DeletePackageJob"`
}

// DeletePackageJobParams describes one replaced predecessor the caller
// wants cleaned up asynchronously by the Scala jobs service. Build a slice
// and pass it to PublishDeletePackageJobs to send the whole slice in as
// few SQS batches as possible.
type DeletePackageJobParams struct {
	PackageId      int64
	OrganizationId int
	UserNodeId     string
	TraceId        string
}

// sqsSendMessageBatchLimit is SQS's hard cap on messages per batch.
const sqsSendMessageBatchLimit = 10

// PublishDeletePackageJobs enqueues a batch of DeletePackageJobs on the
// platform jobs queue using SendMessageBatch. Matters at scale: a 100k-file
// replace goes through ~4000 upload-lambda invocations; per-message
// SendMessage would be 100k SQS round trips, batching collapses that to
// ~10k. The Scala jobs service consumes the queue and performs the async
// S3 asset cleanup; callers must have already soft-deleted each package
// in Postgres (pennsieve-go-core's AddPackagesWithConflict does this
// in-tx for the replace path).
//
// Partial-batch failures are logged and returned as a non-nil error after
// all batches are attempted. The caller can decide whether to surface or
// swallow; for upload-service-v2's import path we log and continue so a
// single SQS hiccup doesn't block the whole manifest from importing.
func PublishDeletePackageJobs(
	ctx context.Context,
	sqsClient *sqs.Client,
	queueURL string,
	jobs []DeletePackageJobParams,
) error {
	if len(jobs) == 0 {
		return nil
	}
	if queueURL == "" {
		return fmt.Errorf("PublishDeletePackageJobs: jobs queue URL is not configured")
	}

	var firstErr error
	for start := 0; start < len(jobs); start += sqsSendMessageBatchLimit {
		end := start + sqsSendMessageBatchLimit
		if end > len(jobs) {
			end = len(jobs)
		}
		entries := make([]sqsTypes.SendMessageBatchRequestEntry, 0, end-start)
		for i, j := range jobs[start:end] {
			msg := DeletePackageJobEnvelope{
				DeletePackageJob: DeletePackageJobInner{
					PackageId:      j.PackageId,
					OrganizationId: j.OrganizationId,
					UserId:         j.UserNodeId,
					TraceId:        j.TraceId,
					Id:             uuid.NewString(),
				},
			}
			body, err := json.Marshal(msg)
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("marshal job for package %d: %w", j.PackageId, err)
				}
				continue
			}
			entries = append(entries, sqsTypes.SendMessageBatchRequestEntry{
				// SQS batch entry Id must match ^[a-zA-Z0-9_-]{1,80}$. Position-
				// within-batch is unique inside the call.
				Id:          aws.String("j" + strconv.Itoa(i)),
				MessageBody: aws.String(string(body)),
			})
		}
		if len(entries) == 0 {
			continue
		}
		out, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: aws.String(queueURL),
			Entries:  entries,
		})
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("SendMessageBatch: %w", err)
			}
			continue
		}
		if len(out.Failed) > 0 && firstErr == nil {
			firstErr = fmt.Errorf("SendMessageBatch: %d of %d messages failed (first: %s/%s)",
				len(out.Failed), len(entries),
				aws.ToString(out.Failed[0].Code),
				aws.ToString(out.Failed[0].Message))
		}
	}
	return firstErr
}