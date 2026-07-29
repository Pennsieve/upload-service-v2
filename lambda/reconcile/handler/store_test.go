package handler

import (
	"errors"
	"fmt"
	"testing"

	pgQueries "github.com/pennsieve/pennsieve-go-core/pkg/queries/pgdb"
)

// TestIsTerminalResolveError guards the blast radius in both directions.
//
// A false negative means a permanently-broken manifest is retried forever: the
// reconciler re-enqueues its files every run, the upload lambda fails them, and
// they pile into the dead-letter queue (210 files/day pinned the prod DLQ at
// ~1470 for six weeks before anyone traced it).
//
// A false positive is worse — it flips recoverable rows to FailedOrphan during a
// transient Postgres or DynamoDB blip, which takes them out of the
// StatusIndex=Registered scan permanently. Those files stop being retried and
// only a manual one-shot reconcile brings them back. Anything not positively
// known to be terminal must classify as transient.
func TestIsTerminalResolveError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil is not terminal",
			err:  nil,
			want: false,
		},
		{
			name: "dataset gone sentinel",
			err:  fmt.Errorf("%w: dataset 4191 (org 367)", errDatasetGone),
			want: true,
		},
		{
			name: "dataset gone survives extra wrapping",
			err:  fmt.Errorf("resolve manifest abc: %w", fmt.Errorf("%w: dataset 1", errDatasetGone)),
			want: true,
		},
		{
			name: "missing manifest matched by message",
			err:  errors.New("get manifest abc: Manifest not found"),
			want: true,
		},
		{
			name: "postgres failure is transient",
			err:  errors.New("get organization 367: driver: bad connection"),
			want: false,
		},
		{
			name: "dynamodb throttle is transient",
			err:  errors.New("ProvisionedThroughputExceededException: slow down"),
			want: false,
		},
		{
			name: "region lookup failure is transient",
			err:  errors.New("resolve region for some-bucket: RequestTimeout"),
			want: false,
		},
		{
			name: "missing org row is transient, not terminal",
			err:  errors.New("get organization 999: sql: no rows in result set"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTerminalResolveError(tt.err); got != tt.want {
				t.Errorf("isTerminalResolveError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestDatasetNotFoundErrorIsMatchable pins the assumption resolveManifest depends
// on: pgdb converts sql.ErrNoRows into a typed DatasetNotFoundError rather than
// propagating ErrNoRows, and that type is reachable through errors.As after
// wrapping. If a pennsieve-go-core bump changes either, resolveManifest silently
// stops recognising deleted datasets and the re-enqueue loop returns — with no
// other test failing.
func TestDatasetNotFoundErrorIsMatchable(t *testing.T) {
	wrapped := fmt.Errorf("get dataset 4191: %w", pgQueries.DatasetNotFoundError{ErrorMessage: "No rows were returned!"})

	var notFound pgQueries.DatasetNotFoundError
	if !errors.As(wrapped, &notFound) {
		t.Fatal("errors.As failed to match a wrapped DatasetNotFoundError; resolveManifest's deleted-dataset detection is broken")
	}
}
