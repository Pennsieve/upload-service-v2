package test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/smithy-go/middleware"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pusher/pusher-http-go/v5"
)

type MockSNS struct{}

func (s MockSNS) PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	result := sns.PublishBatchOutput{
		Failed:         nil,
		Successful:     nil,
		ResultMetadata: middleware.Metadata{},
	}
	return &result, nil
}

type MockS3 struct{}

func (s MockS3) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	result := s3.HeadObjectOutput{
		ChecksumSHA256: aws.String("fakeSHA"),
	}

	return &result, nil
}

func (s MockS3) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {

	var deleted []types.DeletedObject
	toBeDeleted := params.Delete.Objects
	for _, f := range toBeDeleted {
		deleted = append(deleted, types.DeletedObject{
			Key: f.Key,
		})
	}

	result := s3.DeleteObjectsOutput{
		Deleted: deleted,
	}
	return &result, nil
}

type MockChangelogger struct {
	Messages []changelog.Message
}

func (m *MockChangelogger) EmitEvents(ctx context.Context, params changelog.Message) error {
	m.Messages = append(m.Messages, params)
	return nil
}

func (m *MockChangelogger) Clear() {
	m.Messages = nil
}

type MockPusherClient struct {
	// Triggered maps channel -> eventName -> []data
	Triggered map[string]map[string][]any
}

func NewMockPusherClient() *MockPusherClient {
	return &MockPusherClient{Triggered: map[string]map[string][]any{}}
}

func (m *MockPusherClient) captureEvent(channel string, eventName string, data any) {
	if eventMap, ok := m.Triggered[channel]; ok {
		eventMap[eventName] = append(eventMap[eventName], data)
		m.Triggered[channel] = eventMap
	} else {
		m.Triggered[channel] = map[string][]any{eventName: {data}}
	}
}

func (m *MockPusherClient) TriggerBatch(batch []pusher.Event) (*pusher.TriggerBatchChannelsList, error) {
	for _, event := range batch {
		m.captureEvent(event.Channel, event.Name, event.Data)
	}
	// TODO a more realistic return value
	return &pusher.TriggerBatchChannelsList{}, nil
}

func (m *MockPusherClient) Trigger(channel string, eventName string, data interface{}) error {
	m.captureEvent(channel, eventName, data)
	return nil
}
func (m *MockPusherClient) Clear() {
	m.Triggered = map[string]map[string][]any{}
}
