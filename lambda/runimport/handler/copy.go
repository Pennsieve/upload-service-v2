// Package handler copies a workflow run's staged results into dataset
// storage, one queued Job per file (see pkg/runresults), and hands each copy
// to the upload lambda, which creates its package as for any
// direct-to-storage upload.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/runresults"
	log "github.com/sirupsen/logrus"
)

// maxAttempts is how often a copy is tried (the queue's maxReceiveCount):
// the last attempt to fail marks the file Failed, so the run's import ends
// instead of waiting on a message parked in the dead-letter queue.
const maxAttempts = 3

// onConflictAttr is the upload trigger queue's message attribute for the
// conflict strategy (service/handler.OnConflictAttrName).
const onConflictAttr = "OnConflict"

// Reader opens a staged result, through the compute account's role.
type Reader interface {
	Open(ctx context.Context, src runresults.Source) (io.ReadCloser, error)
}

// Writer stores a file in dataset storage, through the storage credentials
// role.
type Writer interface {
	Put(ctx context.Context, dest runresults.Dest, body io.Reader, size int64, tagging string) error
}

// FileTable is the subset of DynamoDB used for manifest file rows.
type FileTable interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Queue is the subset of SQS used for the upload trigger queue.
type Queue interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Copier copies jobs.
type Copier struct {
	Reader        Reader
	Writer        Writer
	Files         FileTable
	FileTableName string
	Queue         Queue
	// UploadTriggerQueueURL is the upload lambda's queue.
	UploadTriggerQueueURL string
}

// errPermanent is a failure a retry won't fix: the file is marked Failed at
// once.
type errPermanent struct{ msg string }

func (e errPermanent) Error() string { return e.msg }

// Handle copies each message's file; a message whose copy may succeed on a
// retry is returned as a batch item failure.
func (c *Copier) Handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, msg := range ev.Records {
		var job runresults.Job
		if err := json.Unmarshal([]byte(msg.Body), &job); err != nil || job.ManifestId == "" || job.UploadId == "" {
			log.WithField("messageId", msg.MessageId).Error("run import: unreadable job, dropped")
			continue
		}
		attempt, _ := strconv.Atoi(msg.Attributes["ApproximateReceiveCount"])
		if c.copyOne(ctx, job, attempt) {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: msg.MessageId})
		}
	}
	return resp, nil
}

// copyOne copies job's file, reporting whether to retry it.
func (c *Copier) copyOne(ctx context.Context, job runresults.Job, attempt int) (retry bool) {
	logger := log.WithFields(log.Fields{"manifestId": job.ManifestId, "uploadId": job.UploadId, "runId": job.RunId, "key": job.Source.Key})
	status, err := c.status(ctx, job)
	if err != nil {
		logger.WithError(err).Warn("run import: reading the file's status")
		return true
	}
	switch status {
	case "":
		logger.Warn("run import: the file isn't on its manifest, dropped")
		return false
	case "Finalized", "Verified", "Failed", "FailedOrphan", "Removed":
		// Done already (a redelivered message), or not to be copied.
		return false
	}

	err = c.copyFile(ctx, job)
	if err == nil {
		logger.Info("run import: copied")
		return false
	}
	var perm errPermanent
	if errors.As(err, &perm) || attempt >= maxAttempts {
		logger.WithError(err).Error("run import: copy failed")
		if markErr := c.markFailed(ctx, job, err.Error()); markErr != nil {
			logger.WithError(markErr).Error("run import: marking the file failed")
			return true
		}
		return false
	}
	logger.WithError(err).Warnf("run import: copy failed (attempt %d), retrying", attempt)
	return true
}

func (c *Copier) copyFile(ctx context.Context, job runresults.Job) error {
	if job.Size > runresults.MaxFileBytes {
		return errPermanent{fmt.Sprintf("%s (%d bytes; the limit is %d)", runresults.ErrTooLarge, job.Size, int64(runresults.MaxFileBytes))}
	}
	body, err := c.Reader.Open(ctx, job.Source)
	if err != nil {
		var noKey *s3Types.NoSuchKey
		if errors.As(err, &noKey) {
			return errPermanent{"the staged file is gone (results are kept 7 days)"}
		}
		return fmt.Errorf("reading the staged file: %w", err)
	}
	defer body.Close()
	tagging := url.Values{"OrgId": {strconv.FormatInt(job.OrgIntId, 10)}, "DatasetId": {strconv.FormatInt(job.DatasetIntId, 10)}}.Encode()
	if err := c.Writer.Put(ctx, job.Dest, body, job.Size, tagging); err != nil {
		return fmt.Errorf("storing the file: %w", err)
	}
	return c.handToUpload(ctx, job)
}

// handToUpload queues the stored file for the upload lambda, as finalize
// does for a client's upload: a synthesized S3 "object created" event.
func (c *Copier) handToUpload(ctx context.Context, job runresults.Job) error {
	ev := events.S3Event{Records: []events.S3EventRecord{{
		EventSource: "aws:s3",
		EventName:   "ObjectCreated:Put",
		S3: events.S3Entity{
			Bucket: events.S3Bucket{Name: job.Dest.Bucket},
			Object: events.S3Object{Key: job.Dest.Key, Size: job.Size},
		},
	}}}
	body, _ := json.Marshal(ev)
	onConflict := job.OnConflict
	if onConflict == "" {
		onConflict = "keepBoth"
	}
	_, err := c.Queue.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(c.UploadTriggerQueueURL),
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]sqsTypes.MessageAttributeValue{
			onConflictAttr: {DataType: aws.String("String"), StringValue: aws.String(onConflict)},
		},
	})
	if err != nil {
		return fmt.Errorf("handing the file to the upload lambda: %w", err)
	}
	return nil
}

func (c *Copier) key(job runresults.Job) map[string]dyTypes.AttributeValue {
	return map[string]dyTypes.AttributeValue{
		"ManifestId": &dyTypes.AttributeValueMemberS{Value: job.ManifestId},
		"UploadId":   &dyTypes.AttributeValueMemberS{Value: job.UploadId},
	}
}

// status is the file row's status, or "" when there's no row.
func (c *Copier) status(ctx context.Context, job runresults.Job) (string, error) {
	out, err := c.Files.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:                aws.String(c.FileTableName),
		Key:                      c.key(job),
		ProjectionExpression:     aws.String("#s"),
		ExpressionAttributeNames: map[string]string{"#s": "Status"},
		ConsistentRead:           aws.Bool(true),
	})
	if err != nil {
		return "", err
	}
	s, _ := out.Item["Status"].(*dyTypes.AttributeValueMemberS)
	if s == nil {
		return "", nil
	}
	return s.Value, nil
}

// markFailed ends the file's import with msg (as UpdateFileTableStatus does
// for a terminal status: no longer in progress).
func (c *Copier) markFailed(ctx context.Context, job runresults.Job, msg string) error {
	_, err := c.Files.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(c.FileTableName),
		Key:              c.key(job),
		UpdateExpression: aws.String("SET #s = :s, #m = :m REMOVE #p"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status", "#m": "Message", "#p": "InProgress",
		},
		ExpressionAttributeValues: map[string]dyTypes.AttributeValue{
			":s": &dyTypes.AttributeValueMemberS{Value: "Failed"},
			":m": &dyTypes.AttributeValueMemberS{Value: msg},
		},
	})
	return err
}

// s3Reader opens staged results with sessions of the compute account's
// role, limited to the target's prefix.
type s3Reader struct{ sessions *sessions }

func (r s3Reader) Open(ctx context.Context, src runresults.Source) (io.ReadCloser, error) {
	client := r.sessions.client(src.Region, src.RoleArn, src.ExternalId, runresults.ReadSessionPolicy(src.Bucket, src.ReadPrefix))
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(src.Bucket), Key: aws.String(src.Key)})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}
