package handler

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/pgdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSQSBatch records each SendMessageBatch call. It's concurrency-safe
// because the sender fires batches in parallel.
type fakeSQSBatch struct {
	mu          sync.Mutex
	batchSizes  []int
	totalMsgs   int
	returnErr   error // returned from every call
	failEntries int   // mark this many entries failed in each response
}

func (f *fakeSQSBatch) SendMessageBatch(_ context.Context, in *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.returnErr != nil {
		return nil, f.returnErr
	}
	f.batchSizes = append(f.batchSizes, len(in.Entries))
	f.totalMsgs += len(in.Entries)
	out := &sqs.SendMessageBatchOutput{}
	for i := 0; i < f.failEntries && i < len(in.Entries); i++ {
		out.Failed = append(out.Failed, sqsTypes.BatchResultErrorEntry{
			Id:      in.Entries[i].Id,
			Message: aws.String("boom"),
		})
	}
	return out, nil
}

// TestBuildDeleteRequests: only packages that replaced a predecessor turn
// into delete jobs, and each carries the predecessor's id (not the new
// package's) plus the shared org/user/trace fields.
func TestBuildDeleteRequests(t *testing.T) {
	packages := []pgdb.Package{
		{Id: 1, ReplacesPackageId: sql.NullInt64{Int64: 100, Valid: true}}, // replaced 100
		{Id: 2},                                                            // replaced nothing
		{Id: 3, ReplacesPackageId: sql.NullInt64{Int64: 300, Valid: true}}, // replaced 300
	}

	reqs := buildDeleteRequests(packages, 7, "N:user:abc", "manifest-xyz")

	require.Len(t, reqs, 2, "only the two packages that replaced something become requests")
	// PackageID must be the predecessor's id (ReplacesPackageId), not pkg.Id.
	assert.Equal(t, int64(100), reqs[0].PackageID)
	assert.Equal(t, int64(300), reqs[1].PackageID)
	for _, r := range reqs {
		assert.Equal(t, int64(7), r.OrganizationID)
		assert.Equal(t, "N:user:abc", r.UserNodeID)
		assert.Equal(t, "manifest-xyz", r.TraceID)
	}
}

// TestBuildDeleteRequests_NoReplacements: a normal upload (no replaces)
// produces no delete jobs.
func TestBuildDeleteRequests_NoReplacements(t *testing.T) {
	packages := []pgdb.Package{{Id: 1}, {Id: 2}}
	assert.Empty(t, buildDeleteRequests(packages, 7, "N:user:abc", "m"))
}

func deleteBodies(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(`{"DeletePackageJob":{}}`)
	}
	return out
}

// TestSqsQueueSender_ChunksIntoBatches: more than 10 messages get split
// into SQS-sized batches, and every message is sent.
func TestSqsQueueSender_ChunksIntoBatches(t *testing.T) {
	f := &fakeSQSBatch{}
	s := &sqsQueueSender{client: f, queueURL: "url"}

	require.NoError(t, s.SendToQueue(context.Background(), deleteBodies(25)))

	assert.Equal(t, 25, f.totalMsgs, "all messages should be sent")
	assert.Len(t, f.batchSizes, 3, "25 messages -> 3 batches")
	for _, n := range f.batchSizes {
		assert.LessOrEqual(t, n, sqsBatchLimit, "no batch exceeds the SQS limit")
	}
}

// TestSqsQueueSender_Empty: nothing to send means no calls.
func TestSqsQueueSender_Empty(t *testing.T) {
	f := &fakeSQSBatch{}
	s := &sqsQueueSender{client: f, queueURL: "url"}
	require.NoError(t, s.SendToQueue(context.Background(), nil))
	assert.Empty(t, f.batchSizes)
}

// TestSqsQueueSender_SendError: a failed SendMessageBatch call surfaces
// as an error.
func TestSqsQueueSender_SendError(t *testing.T) {
	f := &fakeSQSBatch{returnErr: errors.New("network down")}
	s := &sqsQueueSender{client: f, queueURL: "url"}
	assert.Error(t, s.SendToQueue(context.Background(), deleteBodies(5)))
}

// TestSqsQueueSender_PartialFailure: a failed entry inside an otherwise-OK
// batch response still surfaces as an error.
func TestSqsQueueSender_PartialFailure(t *testing.T) {
	f := &fakeSQSBatch{failEntries: 1}
	s := &sqsQueueSender{client: f, queueURL: "url"}
	assert.Error(t, s.SendToQueue(context.Background(), deleteBodies(3)))
}
