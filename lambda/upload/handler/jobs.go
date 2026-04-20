package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
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

// PublishDeletePackageJob enqueues a DeletePackageJob on the platform jobs
// queue. The Scala jobs service consumes the queue and performs the async
// S3 asset cleanup + final state transition. Caller must have already
// soft-deleted the package in Postgres (state=DELETING, name prefixed with
// __DELETED__<nodeId>_). For our replace-on-conflict flow, pennsieve-go-core's
// AddPackagesWithConflict does that soft-delete in-tx; this function is
// the async side effect for each returned package with ReplacesPackageId set.
func PublishDeletePackageJob(
	ctx context.Context,
	sqsClient *sqs.Client,
	queueURL string,
	packageId int64,
	organizationId int,
	userNodeId string,
	traceId string,
) error {
	if queueURL == "" {
		return fmt.Errorf("PublishDeletePackageJob: jobs queue URL is not configured")
	}
	msg := DeletePackageJobEnvelope{
		DeletePackageJob: DeletePackageJobInner{
			PackageId:      packageId,
			OrganizationId: organizationId,
			UserId:         userNodeId,
			TraceId:        traceId,
			Id:             uuid.NewString(),
		},
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("PublishDeletePackageJob: marshal: %w", err)
	}
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return fmt.Errorf("PublishDeletePackageJob: SendMessage: %w", err)
	}
	return nil
}