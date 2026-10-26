// Package uploadtrigger builds the messages that hand a stored file to the
// upload lambda: a synthesized S3 "object created" event on the
// upload_trigger_queue, as a real S3 notification would send, with the
// conflict strategy as a message attribute. Finalize sends one per file a
// client uploaded; the run-import lambda one per file it copied.
package uploadtrigger

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// OnConflictAttrName is the message attribute carrying the conflict
// strategy (keepBoth or replace). The upload lambda treats a missing
// attribute as keepBoth, for messages from before it existed.
const OnConflictAttrName = "OnConflict"

// Body is the message body for a file stored at bucket/key.
func Body(bucket, key string, size int64) string {
	ev := events.S3Event{Records: []events.S3EventRecord{{
		EventSource: "aws:s3",
		EventName:   "ObjectCreated:Put",
		S3: events.S3Entity{
			Bucket: events.S3Bucket{Name: bucket},
			Object: events.S3Object{Key: key, Size: size},
		},
	}}}
	body, _ := json.Marshal(ev)
	return string(body)
}

// Attributes are the message attributes for onConflict.
func Attributes(onConflict string) map[string]sqsTypes.MessageAttributeValue {
	return map[string]sqsTypes.MessageAttributeValue{
		OnConflictAttrName: {DataType: aws.String("String"), StringValue: aws.String(onConflict)},
	}
}
