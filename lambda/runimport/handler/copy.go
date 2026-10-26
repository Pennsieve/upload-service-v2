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

	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dyTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/runresults"
	"github.com/pennsieve/pennsieve-upload-service-v2/pkg/uploadtrigger"
	log "github.com/sirupsen/logrus"
)

// Reader opens a staged file, through the compute account's role, and
// says how large it is (as stored, not as the job says).
type Reader interface {
	Open(ctx context.Context, src runresults.Source) (body io.ReadCloser, size int64, err error)
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
	// MaxAttempts is how often a copy is tried (the import queue's
	// maxReceiveCount): the last attempt to fail marks the file Failed, so
	// the import ends instead of a message waiting in the dead-letter queue.
	MaxAttempts int
}

// copyMargin is how long before the lambda's deadline a copy gives up, so
// there's time left to mark the file Failed.
const copyMargin = time.Minute

// copyError is a failed copy: what the file's row says (Message, which
// dataset viewers see: no account ids, ARNs or request ids), and the
// detail, which is only logged. A permanent one won't be fixed by a retry.
type copyError struct {
	message   string
	permanent bool
	err       error
}

func (e copyError) Error() string {
	if e.err != nil {
		return e.message + ": " + e.err.Error()
	}
	return e.message
}

func (e copyError) Unwrap() error { return e.err }

func permanent(message string, err error) copyError { return copyError{message, true, err} }
func transient(message string, err error) copyError { return copyError{message, false, err} }

// Handle copies each message's file; a message whose copy may succeed on a
// retry is returned as a batch item failure.
func (c *Copier) Handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, msg := range ev.Records {
		var job runresults.Job
		if err := json.Unmarshal([]byte(msg.Body), &job); err != nil {
			log.WithField("messageId", msg.MessageId).Error("run import: unreadable job, dropped")
			continue
		}
		// Only jobs as the service lambda queues them: a message can't widen
		// what this lambda reads or writes.
		if err := job.Check(); err != nil {
			log.WithField("messageId", msg.MessageId).WithError(err).Error("run import: unexpected job, dropped")
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

	copyCtx, cancel := withCopyDeadline(ctx)
	defer cancel()
	err = c.copyFile(copyCtx, job)
	if err == nil {
		logger.Info("run import: copied")
		return false
	}
	var ce copyError
	if !errors.As(err, &ce) {
		ce = transient("the file could not be copied", err)
	}
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		// Out of the lambda's time: a retry would run out again.
		ce = permanent("the file could not be copied in time", err)
	}
	if ce.permanent || attempt >= c.MaxAttempts {
		logger.WithError(err).Error("run import: copy failed")
		if markErr := c.markFailed(ctx, job, ce.message); markErr != nil {
			logger.WithError(markErr).Error("run import: marking the file failed")
			return true
		}
		return false
	}
	logger.WithError(err).Warnf("run import: copy failed (attempt %d), retrying", attempt)
	return true
}

// withCopyDeadline gives a copy until copyMargin before the lambda's own
// deadline, when there is one.
func withCopyDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(ctx, deadline.Add(-copyMargin))
	}
	return context.WithCancel(ctx)
}

func (c *Copier) copyFile(ctx context.Context, job runresults.Job) error {
	body, size, err := c.Reader.Open(ctx, job.Source)
	if err != nil {
		var noKey *s3Types.NoSuchKey
		if errors.As(err, &noKey) {
			return permanent("the staged file is gone (staged files are kept 7 days)", err)
		}
		return transient("the staged file could not be read", err)
	}
	defer body.Close()
	if size > runresults.MaxFileBytes {
		return permanent(fmt.Sprintf("%s (%d GiB at most)", runresults.ErrTooLarge, int64(runresults.MaxFileBytes)>>30), nil)
	}
	tagging := url.Values{"OrgId": {strconv.FormatInt(job.OrgIntId, 10)}, "DatasetId": {strconv.FormatInt(job.DatasetIntId, 10)}}.Encode()
	if err := c.Writer.Put(ctx, job.Dest, body, size, tagging); err != nil {
		return transient("the file could not be stored", err)
	}
	if err := c.handToUpload(ctx, job, size); err != nil {
		return transient("the file could not be handed to the upload pipeline", err)
	}
	return nil
}

// handToUpload queues the stored file for the upload lambda, as finalize
// does for a client's upload.
func (c *Copier) handToUpload(ctx context.Context, job runresults.Job, size int64) error {
	onConflict := job.OnConflict
	if onConflict == "" {
		onConflict = "keepBoth"
	}
	_, err := c.Queue.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:          aws.String(c.UploadTriggerQueueURL),
		MessageBody:       aws.String(uploadtrigger.Body(job.Dest.Bucket, job.Dest.Key, size)),
		MessageAttributes: uploadtrigger.Attributes(onConflict),
	})
	return err
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

func (r s3Reader) Open(ctx context.Context, src runresults.Source) (io.ReadCloser, int64, error) {
	client := r.sessions.client(src.Region, src.RoleArn, src.ExternalId, runresults.ReadSessionPolicy(src.Bucket, src.ReadPrefix))
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(src.Bucket), Key: aws.String(src.Key)})
	if err != nil {
		return nil, 0, err
	}
	return out.Body, aws.ToInt64(out.ContentLength), nil
}
