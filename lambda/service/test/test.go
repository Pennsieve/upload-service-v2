package test

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
)

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

type MockLambda struct{}

func (s MockLambda) Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	return &lambda.InvokeOutput{
		ExecutedVersion: nil,
		FunctionError:   nil,
		LogResult:       nil,
		Payload:         nil,
		StatusCode:      200,
		ResultMetadata:  middleware.Metadata{},
	}, nil
}
