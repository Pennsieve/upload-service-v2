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

// TestValidatePayload covers mode selection. Handle's first real action is
// ConnectRDS, so an invalid payload has to be rejected before that — and a
// payload that accidentally satisfies two modes must not silently pick one,
// since "reportOnly plus gracePeriodHours" would otherwise run the mutating
// sweep when the operator asked for a read-only audit.
func TestValidatePayload(t *testing.T) {
	tests := []struct {
		name    string
		payload Payload
		wantErr bool
	}{
		{name: "grace period alone", payload: Payload{GracePeriodHours: 6}},
		{name: "manifest alone", payload: Payload{ManifestNodeID: "abc"}},
		{name: "reportOnly alone", payload: Payload{ReportOnly: true}},
		{name: "reportOnly with dryRun is fine", payload: Payload{ReportOnly: true, DryRun: true}},
		{name: "nothing set", payload: Payload{}, wantErr: true},
		{name: "empty except concurrency", payload: Payload{Concurrency: 8}, wantErr: true},
		{name: "reportOnly plus grace period", payload: Payload{ReportOnly: true, GracePeriodHours: 6}, wantErr: true},
		{name: "reportOnly plus manifest", payload: Payload{ReportOnly: true, ManifestNodeID: "abc"}, wantErr: true},
		{name: "manifest plus grace period", payload: Payload{ManifestNodeID: "abc", GracePeriodHours: 6}, wantErr: true},
		{name: "all three", payload: Payload{ReportOnly: true, ManifestNodeID: "abc", GracePeriodHours: 6}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePayload(tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("validatePayload(%+v) error = %v, wantErr %v", tt.payload, err, tt.wantErr)
			}
		})
	}
}

// TestEmitMetricsReportFields checks that reportOnly runs publish the orphan-byte
// metrics and ordinary runs do not — an always-present zero would make
// "nobody has measured this" indistinguishable from "measured, and it is zero".
func TestEmitMetricsReportFields(t *testing.T) {
	withReport := Result{Report: &OrphanReport{ObjectsPresent: 3, BytesPresent: 4096}}
	if got := metricNames(withReport); !contains(got, "OrphanedBytesPresent") || !contains(got, "OrphanedObjectsPresent") {
		t.Errorf("reportOnly run should publish orphan metrics, got %v", got)
	}
	if got := metricNames(Result{Recovered: 5}); contains(got, "OrphanedBytesPresent") {
		t.Errorf("non-report run should not publish orphan metrics, got %v", got)
	}
}

// metricNames pulls the published metric names out of the EMF payload.
func metricNames(r Result) []string {
	emf := buildEMF(r)
	aws := emf["_aws"].(map[string]any)
	defs := aws["CloudWatchMetrics"].([]map[string]any)
	var out []string
	for _, m := range defs[0]["Metrics"].([]map[string]string) {
		out = append(out, m["Name"])
	}
	return out
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
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
