package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/pennsieve/pennsieve-go-core/pkg/realtime"
	"github.com/pennsieve/pennsieve-upload-service-v2/upload/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const notifyDatasetNodeId = "N:dataset:12345678-1234-1234-1234-123456789abc"

type fakePublisher struct {
	batches [][]realtime.Event
	err     error
}

func (f *fakePublisher) Publish(ctx context.Context, ch realtime.Channel, name string, data any) error {
	return f.PublishBatch(ctx, []realtime.Event{{Channel: ch, Name: name, Data: data}})
}

func (f *fakePublisher) PublishBatch(_ context.Context, events []realtime.Event) error {
	f.batches = append(f.batches, events)
	return f.err
}

func uploadRows(n int) []uploadPusherItem {
	rows := make([]uploadPusherItem, n)
	for i := range rows {
		rows[i].Name = "file.csv"
	}
	return rows
}

func TestNotifyUploadPublishesToTheDatasetChannel(t *testing.T) {
	pub := &fakePublisher{}
	pc := test.NewMockPusherClient()
	s := (&UploadHandlerStore{pusherClient: pc}).WithRealtime(pub)

	s.notifyUpload(context.Background(), notifyDatasetNodeId, uploadRows(3))

	require.Len(t, pub.batches, 1)
	require.Len(t, pub.batches[0], 1)
	e := pub.batches[0][0]
	assert.Equal(t, "/datasets/12345678-1234-1234-1234-123456789abc", e.Channel.Path())
	assert.Equal(t, "upload-event", e.Name)
	assert.Len(t, e.Data, 3)
	assert.Empty(t, pc.Triggered, "Pusher is not used when AppSync is configured")
}

func TestNotifyUploadSplitsLargeBatches(t *testing.T) {
	pub := &fakePublisher{}
	s := (&UploadHandlerStore{}).WithRealtime(pub)

	s.notifyUpload(context.Background(), notifyDatasetNodeId, uploadRows(2*maxRowsPerUploadEvent+1))

	require.Len(t, pub.batches, 1)
	events := pub.batches[0]
	require.Len(t, events, 3)
	assert.Len(t, events[0].Data, maxRowsPerUploadEvent)
	assert.Len(t, events[1].Data, maxRowsPerUploadEvent)
	assert.Len(t, events[2].Data, 1)
}

func TestNotifyUploadStillSignalsAnEmptyBatch(t *testing.T) {
	pub := &fakePublisher{}
	s := (&UploadHandlerStore{}).WithRealtime(pub)

	s.notifyUpload(context.Background(), notifyDatasetNodeId, nil)

	require.Len(t, pub.batches, 1)
	assert.Len(t, pub.batches[0], 1, "one event, like the single Pusher trigger")
}

func TestNotifyUploadPublishErrorsAreNotFatal(t *testing.T) {
	s := (&UploadHandlerStore{}).WithRealtime(&fakePublisher{err: errors.New("boom")})
	assert.NotPanics(t, func() { s.notifyUpload(context.Background(), notifyDatasetNodeId, uploadRows(1)) })
}

func TestNotifyUploadFallsBackToPusher(t *testing.T) {
	pc := test.NewMockPusherClient()
	s := &UploadHandlerStore{pusherClient: pc}

	s.notifyUpload(context.Background(), notifyDatasetNodeId, uploadRows(2))

	events := pc.Triggered["dataset-12345678-1234-1234-1234-123456789abc"]["upload-event"]
	require.Len(t, events, 1)
	assert.Len(t, events[0], 2)
}

func TestNotifyUploadWithNeitherIsANoop(t *testing.T) {
	assert.NotPanics(t, func() {
		(&UploadHandlerStore{}).notifyUpload(context.Background(), notifyDatasetNodeId, uploadRows(1))
	})
}
