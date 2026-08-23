package handler

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testS3Body = `{"Records":[{"s3":{"bucket":{"name":"some-bucket"},"object":{"key":"some/key","size":10}}}]}`

func TestFilterLiveRecords(t *testing.T) {
	for _, tt := range []struct {
		name        string
		bodies      []string
		wantLive    int
		wantDropped int
	}{
		{
			name:        "heartbeat only",
			bodies:      []string{`{"heartbeat":true}`},
			wantLive:    0,
			wantDropped: 1,
		},
		{
			name:        "s3 event only",
			bodies:      []string{testS3Body},
			wantLive:    1,
			wantDropped: 0,
		},
		{
			name:        "heartbeat mixed with s3 event",
			bodies:      []string{`{"heartbeat":true}`, testS3Body, `{"heartbeat":true}`},
			wantLive:    1,
			wantDropped: 2,
		},
		{
			name:        "malformed json is dropped",
			bodies:      []string{`not json at all`, testS3Body},
			wantLive:    1,
			wantDropped: 1,
		},
		{
			name:        "s3 envelope with no records is dropped",
			bodies:      []string{`{"Records":[]}`},
			wantLive:    0,
			wantDropped: 1,
		},
		{
			name:        "empty batch",
			bodies:      nil,
			wantLive:    0,
			wantDropped: 0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			records := make([]events.SQSMessage, 0, len(tt.bodies))
			for _, b := range tt.bodies {
				records = append(records, events.SQSMessage{Body: b})
			}

			live, dropped := filterLiveRecords(records)

			assert.Len(t, live, tt.wantLive)
			assert.Equal(t, tt.wantDropped, dropped)
			for _, m := range live {
				assert.NotEqual(t, `{"heartbeat":true}`, m.Body,
					"a heartbeat survived the filter")
			}
		})
	}
}

// TestHandlerSkipsDatabaseForHeartbeatOnlyBatch guards the ordering in
// Handler: heartbeats must be discarded before ConnectRDS is called. When
// that ordering regressed, every once-a-minute heartbeat opened a Postgres
// connection, and an RDS outage sent the whole heartbeat stream to the DLQ —
// paging on a warmth signal rather than on a real upload failure.
func TestHandlerSkipsDatabaseForHeartbeatOnlyBatch(t *testing.T) {
	resp, err := Handler(context.Background(), events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "hb-1", Body: `{"heartbeat":true}`},
			{MessageId: "hb-2", Body: `{"heartbeat":true}`},
		},
	})

	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures,
		"heartbeats must be acknowledged, never returned as batch item failures")
}
