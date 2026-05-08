// Package bucketregion resolves the AWS region of an S3 bucket and returns
// a region-correct S3 client for it. Required because workspace storage
// buckets can live in different regions per workspace, but a default-region
// S3 client signs/routes against the wrong endpoint.
package bucketregion

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// keep a cache of bucket names after cold start
var cache sync.Map

// Set seeds the cache. Intended for tests where HeadBucket against the test
// S3 backend (e.g. minio) doesn't reliably return x-amz-bucket-region.
func Set(bucket, region string) { cache.Store(bucket, region) }

// Resolve returns bucket's AWS region; the client can be in any region.
func Resolve(ctx context.Context, client *s3.Client, bucket string) (string, error) {
	if v, ok := cache.Load(bucket); ok {
		return v.(string), nil
	}
	out, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		if out.BucketRegion != nil && *out.BucketRegion != "" {
			cache.Store(bucket, *out.BucketRegion)
			return *out.BucketRegion, nil
		}
		return "", fmt.Errorf("HeadBucket %s: no BucketRegion in response", bucket)
	} else {
		// can still get the region even on a failure which is all we need
		var responseError *smithyhttp.ResponseError
		if errors.As(err, &responseError) {
			if r := responseError.Response.Header.Get("x-amz-bucket-region"); r != "" {
				cache.Store(bucket, r)
				return r, nil
			}
		}
		return "", fmt.Errorf("HeadBucket %s: %w", bucket, err)
	}
}

// ClientForBucket returns an S3 client pinned to bucket's region.
func ClientForBucket(ctx context.Context, client *s3.Client, bucket string) (*s3.Client, error) {
	region, err := Resolve(ctx, client, bucket)
	if err != nil {
		return nil, err
	}
	opts := client.Options()
	opts.Region = region
	return s3.New(opts), nil
}
