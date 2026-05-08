package bucketregion

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// roundTripperFunc lets us script HTTP responses for the SDK client.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestClient(rt http.RoundTripper) *s3.Client {
	return s3.New(s3.Options{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  &http.Client{Transport: rt},
		// Disable retries so 5xx error tests don't sit in backoff.
		RetryMaxAttempts: 1,
	})
}

func resetCache() { cache = sync.Map{} }

func makeResp(status int, headers map[string]string) *http.Response {
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode: status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader("")),
	}
}

func TestResolve_CacheHit(t *testing.T) {
	resetCache()
	cache.Store("cached-bucket", "ap-south-1")

	// RoundTripper would fail the test if called — cache hit shouldn't
	// trigger a network request.
	client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("HeadBucket should not be called on cache hit")
		return nil, nil
	}))

	got, err := Resolve(context.Background(), client, "cached-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ap-south-1" {
		t.Errorf("region = %q; want ap-south-1", got)
	}
}

func TestResolve_SuccessReturnsBucketRegion(t *testing.T) {
	resetCache()
	client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		// Successful HeadBucket carries the region in this header; the
		// SDK deserializes it into HeadBucketOutput.BucketRegion.
		return makeResp(200, map[string]string{"x-amz-bucket-region": "eu-west-1"}), nil
	}))

	got, err := Resolve(context.Background(), client, "happy-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "eu-west-1" {
		t.Errorf("region = %q; want eu-west-1", got)
	}

	// Second call should be served from cache (no network) — verify by
	// swapping in a transport that would fail if invoked.
	cached, err := Resolve(context.Background(), newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("second Resolve should hit cache")
		return nil, nil
	})), "happy-bucket")
	if err != nil || cached != "eu-west-1" {
		t.Errorf("cache not populated: got %q, %v", cached, err)
	}
}

func TestResolve_CrossRegionRedirectHeader(t *testing.T) {
	resetCache()
	client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		// S3 returns 301 PermanentRedirect for cross-region requests, but
		// still sets x-amz-bucket-region. The smithy transport wraps the
		// response in a ResponseError that we unwrap to read the header.
		return makeResp(301, map[string]string{"x-amz-bucket-region": "ap-southeast-2"}), nil
	}))

	got, err := Resolve(context.Background(), client, "cross-region-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ap-southeast-2" {
		t.Errorf("region = %q; want ap-southeast-2", got)
	}
}

func TestResolve_ErrorWithoutRegionHeader(t *testing.T) {
	resetCache()
	client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		// 500 with no header — nothing recoverable; should propagate as error.
		return makeResp(500, nil), nil
	}))

	_, err := Resolve(context.Background(), client, "broken-bucket")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "broken-bucket") {
		t.Errorf("error should mention bucket name; got: %v", err)
	}
}

func TestResolve_SuccessButNoBucketRegionField(t *testing.T) {
	resetCache()
	client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		// 200 success but no x-amz-bucket-region header — SDK leaves the
		// field unset; we treat that as an error rather than silently
		// succeeding with an empty region.
		return makeResp(200, nil), nil
	}))

	_, err := Resolve(context.Background(), client, "no-region-bucket")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no BucketRegion") {
		t.Errorf("error should mention missing BucketRegion; got: %v", err)
	}
}

func TestClientForBucket_PinsRegion(t *testing.T) {
	resetCache()
	src := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return makeResp(301, map[string]string{"x-amz-bucket-region": "ca-central-1"}), nil
	}))

	got, err := ClientForBucket(context.Background(), src, "some-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r := got.Options().Region; r != "ca-central-1" {
		t.Errorf("client region = %q; want ca-central-1", r)
	}
	// Source client's region must remain untouched.
	if r := src.Options().Region; r != "us-east-1" {
		t.Errorf("source client region mutated to %q", r)
	}
}

// Pennsieve runs primarily in us-east-1 but supports per-workspace storage
// buckets in other regions (e.g. af-south-1). When a us-east-1 lambda HEADs
// an af-south-1 bucket, S3 responds with 301 PermanentRedirect plus
// x-amz-bucket-region: af-south-1. This test pins the production scenario:
// the helper must return af-south-1 (not us-east-1, not the bucket name)
// and the returned client must be region-pinned to af-south-1 for downstream
// signing/routing.
func TestProductionScenario_USEast1ClientResolvesAfSouth1Bucket(t *testing.T) {
	resetCache()

	// Lambda/server cold-starts in the platform's default region.
	usEast1Client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return makeResp(301, map[string]string{"x-amz-bucket-region": "af-south-1"}), nil
	}))
	if r := usEast1Client.Options().Region; r != "us-east-1" {
		t.Fatalf("test setup wrong: discovery client region = %q; want us-east-1", r)
	}

	const workspaceBucket = "pennsieve-workspace-storage-cape-town"

	region, err := Resolve(context.Background(), usEast1Client, workspaceBucket)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if region != "af-south-1" {
		t.Errorf("Resolve = %q; want af-south-1 (the bucket's actual region from HEAD)", region)
	}

	bucketClient, err := ClientForBucket(context.Background(), usEast1Client, workspaceBucket)
	if err != nil {
		t.Fatalf("ClientForBucket: %v", err)
	}
	if r := bucketClient.Options().Region; r != "af-south-1" {
		t.Errorf("returned client region = %q; want af-south-1", r)
	}
	if r := usEast1Client.Options().Region; r != "us-east-1" {
		t.Errorf("discovery client mutated to %q; should still be us-east-1", r)
	}
}

func TestClientForBucket_PropagatesError(t *testing.T) {
	resetCache()
	client := newTestClient(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return makeResp(500, nil), nil
	}))

	got, err := ClientForBucket(context.Background(), client, "broken-bucket")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got != nil {
		t.Errorf("expected nil client on error, got %+v", got)
	}
}
