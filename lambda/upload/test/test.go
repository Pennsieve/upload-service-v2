package test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/smithy-go/middleware"
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