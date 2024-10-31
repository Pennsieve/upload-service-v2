package test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/smithy-go/middleware"
	"github.com/pennsieve/pennsieve-go-core/pkg/changelog"
	"github.com/pusher/pusher-http-go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type MockDynamoDB struct {
	TestingT              require.TestingT
	BatchGetItemOutputs   []*dynamodb.BatchGetItemOutput
	BatchGetItemCallCount int
}

func (m *MockDynamoDB) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	require.FailNow(m.TestingT, "mock method has not been implemented; implement test.MockDynamoDB.GetItem if this is an expected call")
	return nil, nil
}

func (m *MockDynamoDB) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	require.FailNow(m.TestingT, "mock method has not been implemented; implement test.MockDynamoDB.Query if this is an expected call")
	return nil, nil
}

func (m *MockDynamoDB) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	require.FailNow(m.TestingT, "mock method has not been implemented; implement test.MockDynamoDB.UpdateItem if this is an expected call")
	return nil, nil
}

func (m *MockDynamoDB) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	require.FailNow(m.TestingT, "mock method has not been implemented; implement test.MockDynamoDB.BatchWriteItem if this is an expected call")
	return nil, nil
}

func (m *MockDynamoDB) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	require.FailNow(m.TestingT, "mock method has not been implemented; implement test.MockDynamoDB.PutItem if this is an expected call")
	return nil, nil
}

func (m *MockDynamoDB) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if m.BatchGetItemCallCount < len(m.BatchGetItemOutputs) {
		output := m.BatchGetItemOutputs[m.BatchGetItemCallCount]
		m.BatchGetItemCallCount++
		return output, nil
	}
	require.FailNow(m.TestingT, "unexpected number of calls to BatchGetItem", "expected %d calls, got %d calls", len(m.BatchGetItemOutputs), m.BatchGetItemCallCount+1)
	return nil, nil
}

func (m *MockDynamoDB) BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error) {
	require.FailNow(m.TestingT, "mock method has not been implemented; implement test.MockDynamoDB.BatchExecuteStatement if this is an expected call")
	return nil, nil
}

func (m *MockDynamoDB) AssertBatchGetItemCallCount(expectedCallCount int) bool {
	return assert.Equal(m.TestingT, expectedCallCount, m.BatchGetItemCallCount)
}
