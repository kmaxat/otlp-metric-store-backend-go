package ingest_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/seriescache"
)

// fakeStore is an ingest.Store that records calls in order and can be told to fail.
type fakeStore struct {
	calls         []string
	series        []ingest.SeriesRow
	datapoints    []ingest.Datapoint
	seriesErr     error
	datapointsErr error
}

func (f *fakeStore) InsertSeriesRows(_ context.Context, s []ingest.SeriesRow) error {
	f.calls = append(f.calls, "series")
	if f.seriesErr != nil {
		return f.seriesErr
	}
	f.series = append(f.series, s...)
	return nil
}

func (f *fakeStore) InsertDatapoints(_ context.Context, d []ingest.Datapoint) error {
	f.calls = append(f.calls, "datapoints")
	if f.datapointsErr != nil {
		return f.datapointsErr
	}
	f.datapoints = append(f.datapoints, d...)
	return nil
}

func testOptions() ingest.Options {
	return ingest.Options{
		Retention:      testRetention,
		MaxSeriesBytes: testMaxSeriesBytes,
		Now:            func() time.Time { return testNow },
	}
}

func newTestService(t *testing.T, store ingest.Store) *ingest.Service {
	t.Helper()
	s, err := ingest.NewService(store, noop.NewMeterProvider().Meter("test"), testOptions())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func gaugeRequest() *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: wrap(svc(), gaugeMetric("g", dpAt(testNow, 1))),
	}
}

func histogramMetric() *metricspb.Metric {
	return &metricspb.Metric{Name: "h", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		DataPoints: []*metricspb.HistogramDataPoint{{}}}}}
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v (err %v), want %v", got, err, want)
	}
}

func TestExportStoresSeriesThenDatapoints(t *testing.T) {
	store := &fakeStore{}
	resp, err := newTestService(t, store).Export(context.Background(), gaugeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"series", "datapoints"}; !slices.Equal(store.calls, want) {
		t.Errorf("calls = %v, want %v", store.calls, want)
	}
	if resp.GetPartialSuccess() != nil {
		t.Errorf("PartialSuccess = %v, want nil", resp.GetPartialSuccess())
	}
}

func TestExportPartialSuccess(t *testing.T) {
	store := &fakeStore{}
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: wrap(svc(), gaugeMetric("g", dpAt(testNow, 1)), histogramMetric()),
	}
	resp, err := newTestService(t, store).Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ps := resp.GetPartialSuccess()
	if ps.GetRejectedDataPoints() != 1 {
		t.Errorf("RejectedDataPoints = %d, want 1", ps.GetRejectedDataPoints())
	}
	if want := "1 datapoint rejected: 1 unsupported_metric_type"; ps.GetErrorMessage() != want {
		t.Errorf("ErrorMessage = %q, want %q", ps.GetErrorMessage(), want)
	}
	if len(store.datapoints) != 1 {
		t.Errorf("stored %d datapoints, want 1", len(store.datapoints))
	}
}

func TestExportAllRejectedStoresNothing(t *testing.T) {
	store := &fakeStore{}
	req := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: wrap(svc(), histogramMetric())}
	resp, err := newTestService(t, store).Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.calls) != 0 {
		t.Errorf("calls = %v, want none", store.calls)
	}
	if resp.GetPartialSuccess().GetRejectedDataPoints() != 1 {
		t.Errorf("RejectedDataPoints = %d, want 1", resp.GetPartialSuccess().GetRejectedDataPoints())
	}
}

func TestExportSeriesInsertFails(t *testing.T) {
	store := &fakeStore{seriesErr: errors.New("boom")}
	_, err := newTestService(t, store).Export(context.Background(), gaugeRequest())
	wantCode(t, err, codes.Unavailable)
	if want := []string{"series"}; !slices.Equal(store.calls, want) {
		t.Errorf("calls = %v, want %v", store.calls, want)
	}
}

// Proves the gRPC status sent to an unauthenticated sender never contains the
// raw ClickHouse error (internal addresses, table names); that stays in our logs.
func TestExportHidesStoreErrorDetails(t *testing.T) {
	for name, store := range map[string]*fakeStore{
		"series":     {seriesErr: errors.New("dial tcp 10.0.0.5:9000: connect: connection refused")},
		"datapoints": {datapointsErr: errors.New("code: 60, message: Table metricstore.otel_metrics_datapoints does not exist")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newTestService(t, store).Export(context.Background(), gaugeRequest())
			wantCode(t, err, codes.Unavailable)
			msg := status.Convert(err).Message()
			for _, secret := range []string{"10.0.0.5", "otel_metrics_datapoints", "code: 60"} {
				if strings.Contains(msg, secret) {
					t.Errorf("status message %q leaks %q", msg, secret)
				}
			}
		})
	}
}

// The Service passes Options.Stored to Map: with every series row stored,
// even a 1-byte series limit rejects nothing.
func TestExportStoredSeriesRowsSkipLimit(t *testing.T) {
	opts := testOptions()
	opts.MaxSeriesBytes = 1
	opts.Stored = func(ingest.SeriesRowKey) bool { return true }
	store := &fakeStore{}
	s, err := ingest.NewService(store, noop.NewMeterProvider().Meter("test"), opts)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.Export(context.Background(), gaugeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPartialSuccess() != nil || len(store.datapoints) != 1 {
		t.Errorf("PartialSuccess = %v, stored %d datapoints; want nil, 1", resp.GetPartialSuccess(), len(store.datapoints))
	}
}

// With the real series cache as Stored, as in main: series rows the first
// Export stored no longer count against the series limit in the next one.
// Export #2 holds m0..m2 (stored) and m3 (new); without the cache its four
// rows would not fit the 3.5 KB limit.
func TestExportSeriesLimitWithSeriesCache(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	cache, err := seriescache.Wrap(&fakeStore{}, 100, meter)
	if err != nil {
		t.Fatal(err)
	}
	opts := testOptions()
	opts.MaxSeriesBytes = 3500
	opts.Stored = cache.Stored
	s, err := ingest.NewService(cache, meter, opts)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := s.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: bigResourceMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	if want := "7 datapoints rejected: 7 series_limit_exceeded"; resp.GetPartialSuccess().GetErrorMessage() != want {
		t.Errorf("Export #1 ErrorMessage = %q, want %q", resp.GetPartialSuccess().GetErrorMessage(), want)
	}

	rms := bigResourceMetrics()
	sm := rms[0].ScopeMetrics[0]
	sm.Metrics = sm.Metrics[:4]
	resp, err = s.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rms})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPartialSuccess() != nil {
		t.Errorf("Export #2 PartialSuccess = %v, want nil", resp.GetPartialSuccess())
	}
}

func TestExportDatapointsInsertFails(t *testing.T) {
	store := &fakeStore{datapointsErr: errors.New("boom")}
	_, err := newTestService(t, store).Export(context.Background(), gaugeRequest())
	wantCode(t, err, codes.Unavailable)
	if want := []string{"series", "datapoints"}; !slices.Equal(store.calls, want) {
		t.Errorf("calls = %v, want %v", store.calls, want)
	}
}

// sumInt64 returns the summed value of the named counter datapoints whose
// attributes include attr (any datapoint when attr is zero).
func sumInt64(t *testing.T, rm metricdata.ResourceMetrics, name string, attr attribute.KeyValue) int64 {
	t.Helper()
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				got, ok := dp.Attributes.Value(attr.Key)
				if attr.Key == "" || ok && got == attr.Value {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func TestExportSelfMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	store := &fakeStore{}
	s, err := ingest.NewService(store, meter, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Request 1: one gauge datapoint and one histogram datapoint (rejected).
	mixed := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: wrap(svc(), gaugeMetric("g", dpAt(testNow, 1)), histogramMetric()),
	}
	if _, err := s.Export(ctx, mixed); err != nil {
		t.Fatal(err)
	}
	// Request 2: a datapoints insert that fails.
	store.datapointsErr = errors.New("boom")
	failing := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: wrap(svc(), gaugeMetric("g2", dpAt(testNow, 1))),
	}
	if _, err := s.Export(ctx, failing); err == nil {
		t.Fatal("want error")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	none := attribute.KeyValue{}
	checks := []struct {
		name string
		attr attribute.KeyValue
		want int64
	}{
		{"metricstore.datapoints.received", none, 3},
		{"metricstore.datapoints.rejected", attribute.String("reason", "unsupported_metric_type"), 1},
		{"metricstore.datapoints.rejected", none, 1},
		{"metricstore.datapoints.stored", none, 1},
	}
	for _, c := range checks {
		if got := sumInt64(t, rm, c.name, c.attr); got != c.want {
			t.Errorf("%s %v = %d, want %d", c.name, c.attr, got, c.want)
		}
	}
}

func TestExportCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &fakeStore{seriesErr: context.Canceled}
	_, err := newTestService(t, store).Export(ctx, gaugeRequest())
	wantCode(t, err, codes.Canceled)
}

func TestExportEmptyRequest(t *testing.T) {
	store := &fakeStore{}
	resp, err := newTestService(t, store).Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.calls) != 0 || resp.GetPartialSuccess() != nil {
		t.Errorf("calls = %v, resp = %v; want no calls and empty response", store.calls, resp)
	}
}
