// Package buckettest provides test-only helpers for pre-seeding the
// bucketregion cache. Importing this package from non-test code is a smell
// and should be flagged in review.
package buckettest

import "github.com/pennsieve/pennsieve-upload-service-v2/pkg/bucketregion/internal/cache"

// Seed pre-populates the bucket->region cache. Tests call this to bypass
// HeadBucket against a backend (e.g. minio) that doesn't reliably return
// x-amz-bucket-region.
func Seed(bucket, region string) { cache.Store(bucket, region) }

// Reset clears the cache. Use in TestMain or test setup if previous tests
// may have polluted state.
func Reset() { cache.Reset() }
