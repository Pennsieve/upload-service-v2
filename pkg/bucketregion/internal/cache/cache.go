// Package cache holds the bucket->region cache. Lives under internal/ so
// only sibling packages under pkg/bucketregion (Resolve and the buckettest
// helper) can write to it; production lambdas can't reach in and corrupt
// the mapping.
package cache

import "sync"

var m sync.Map

func Load(bucket string) (string, bool) {
	v, ok := m.Load(bucket)
	if !ok {
		return "", false
	}
	return v.(string), true
}

func Store(bucket, region string) { m.Store(bucket, region) }

// Reset clears the cache. Tests use it to start from a clean slate.
func Reset() { m = sync.Map{} }
