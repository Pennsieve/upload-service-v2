package handler

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

type LambdaAPI interface {
	Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

//type S3API interface {
//	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
//	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
//	NewPresignClient(c *s3.Client, optFns ...func(*s3.PresignOptions)) *s3.PresignClient
//}
