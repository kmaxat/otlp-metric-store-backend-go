package ingest_test

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"dash0.com/otlp-metric-store/internal/ingest"
)

var (
	testNow       = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	testRetention = 30 * 24 * time.Hour
	// testMaxSeriesBytes is the default series limit, far above what other tests produce.
	testMaxSeriesBytes = 16 << 20
)

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func dpAt(t time.Time, v float64) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		TimeUnixNano: uint64(t.UnixNano()),
		Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: v},
	}
}

func gaugeMetric(name string, dps ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}}}
}

func sumMetric(name string, temp metricspb.AggregationTemporality, mono bool, dps ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		AggregationTemporality: temp, IsMonotonic: mono, DataPoints: dps}}}
}

func wrap(res []*commonpb.KeyValue, metrics ...*metricspb.Metric) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{{
		Resource:  &resourcepb.Resource{Attributes: res},
		SchemaUrl: "http://res",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{
				Name: "scope", Version: "1.0", Attributes: []*commonpb.KeyValue{strKV("s", "a")},
				DroppedAttributesCount: 3,
			},
			SchemaUrl: "http://scope",
			Metrics:   metrics,
		}},
	}}
}

// midnightUTC returns UTC midnight of the day containing t.
func midnightUTC(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func svc() []*commonpb.KeyValue { return []*commonpb.KeyValue{strKV("service.name", "checkout")} }

func mapIt(rms []*metricspb.ResourceMetrics) ingest.Mapped {
	return ingest.Map(rms, testMapOptions())
}

func testMapOptions() ingest.MapOptions {
	return ingest.MapOptions{Now: testNow, Retention: testRetention, MaxSeriesBytes: testMaxSeriesBytes}
}

func TestMapGauge(t *testing.T) {
	ts := time.Date(2026, 11, 5, 11, 0, 0, 5, time.UTC)
	dp := dpAt(ts, 1.5)
	dp.StartTimeUnixNano = uint64(ts.Add(-time.Minute).UnixNano())
	dp.Flags = 0
	dp.Attributes = []*commonpb.KeyValue{strKV("k", "v")}
	m := gaugeMetric("cpu", dp)
	m.Description, m.Unit = "desc", "1"
	got := mapIt(wrap(svc(), m))
	if len(got.SeriesRows) != 1 || len(got.Datapoints) != 1 || got.RejectedCount() != 0 {
		t.Fatalf("unexpected result: %+v", got)
	}
	s := got.SeriesRows[0]
	if s.MetricType != ingest.MetricTypeGauge || s.AggregationTemporality != 0 || s.IsMonotonic {
		t.Errorf("bad type fields: %+v", s)
	}
	if s.ServiceName != "checkout" || s.MetricName != "cpu" || s.MetricDescription != "desc" || s.MetricUnit != "1" ||
		s.ResourceSchemaUrl != "http://res" || s.ScopeName != "scope" || s.ScopeVersion != "1.0" ||
		s.ScopeDroppedAttrCount != 3 || s.ScopeSchemaUrl != "http://scope" ||
		s.ScopeAttributes["s"] != "a" || s.ResourceAttributes["service.name"] != "checkout" || s.Attributes["k"] != "v" {
		t.Errorf("bad series: %+v", s)
	}
	if !s.Day.Equal(time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("Day = %v", s.Day)
	}
	d := got.Datapoints[0]
	if d.SeriesId != s.SeriesId || d.Value != 1.5 || !d.TimeUnix.Equal(ts) || d.TimeUnix.Location() != time.UTC ||
		!d.StartTimeUnix.Equal(ts.Add(-time.Minute)) || d.Flags != 0 {
		t.Errorf("bad datapoint: %+v", d)
	}
}

func TestMapSum(t *testing.T) {
	m := sumMetric("reqs", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true, dpAt(testNow, 1))
	got := mapIt(wrap(svc(), m))
	if len(got.SeriesRows) != 1 {
		t.Fatalf("series = %d", len(got.SeriesRows))
	}
	s := got.SeriesRows[0]
	if s.MetricType != ingest.MetricTypeSum || s.AggregationTemporality != 2 || !s.IsMonotonic {
		t.Errorf("bad sum series: %+v", s)
	}
}

func TestMapIntValue(t *testing.T) {
	dp := &metricspb.NumberDataPoint{TimeUnixNano: uint64(testNow.UnixNano()), Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7}}
	got := mapIt(wrap(svc(), gaugeMetric("g", dp)))
	if len(got.Datapoints) != 1 || got.Datapoints[0].Value != 7.0 {
		t.Fatalf("got %+v", got.Datapoints)
	}
}

func TestManyPointsOneSeries(t *testing.T) {
	got := mapIt(wrap(svc(), gaugeMetric("g", dpAt(testNow, 1), dpAt(testNow.Add(time.Second), 2), dpAt(testNow.Add(2*time.Second), 3))))
	if len(got.SeriesRows) != 1 || len(got.Datapoints) != 3 {
		t.Fatalf("series=%d datapoints=%d", len(got.SeriesRows), len(got.Datapoints))
	}
}

func TestMidnightCrossingCreatesTwoSeriesRows(t *testing.T) {
	a := time.Date(2026, 11, 4, 23, 59, 59, 0, time.UTC)
	b := time.Date(2026, 11, 5, 0, 0, 1, 0, time.UTC)
	got := mapIt(wrap(svc(), gaugeMetric("g", dpAt(a, 1), dpAt(b, 2))))
	if len(got.SeriesRows) != 2 || len(got.Datapoints) != 2 {
		t.Fatalf("series=%d datapoints=%d", len(got.SeriesRows), len(got.Datapoints))
	}
	if got.SeriesRows[0].SeriesId != got.SeriesRows[1].SeriesId {
		t.Errorf("SeriesId differs across days")
	}
	if !got.SeriesRows[0].Day.Equal(midnightUTC(a)) || !got.SeriesRows[1].Day.Equal(midnightUTC(b)) {
		t.Errorf("days = %v, %v", got.SeriesRows[0].Day, got.SeriesRows[1].Day)
	}
}

func TestRejections(t *testing.T) {
	tests := []struct {
		name   string
		metric *metricspb.Metric
		reason ingest.RejectReason
		count  int64
	}{
		{"histogram", &metricspb.Metric{Name: "h", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints: []*metricspb.HistogramDataPoint{{}, {}}}}}, ingest.RejectUnsupportedMetricType, 2},
		{"summary", &metricspb.Metric{Name: "s", Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{
			DataPoints: []*metricspb.SummaryDataPoint{{}}}}}, ingest.RejectUnsupportedMetricType, 1},
		{"exp histogram", &metricspb.Metric{Name: "e", Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints: []*metricspb.ExponentialHistogramDataPoint{{}}}}}, ingest.RejectUnsupportedMetricType, 1},
		{"empty name", gaugeMetric("", dpAt(testNow, 1), dpAt(testNow, 2)), ingest.RejectMissingMetricName, 2},
		{"unspecified temporality", sumMetric("s", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED, true, dpAt(testNow, 1)), ingest.RejectInvalidTemporality, 1},
		{"out-of-enum temporality", sumMetric("s", metricspb.AggregationTemporality(7), true, dpAt(testNow, 1)), ingest.RejectInvalidTemporality, 1},
		{"time zero", gaugeMetric("g", &metricspb.NumberDataPoint{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}}), ingest.RejectInvalidTimestamp, 1},
		{"time > maxint64", gaugeMetric("g", &metricspb.NumberDataPoint{TimeUnixNano: math.MaxInt64 + 1, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}}), ingest.RejectInvalidTimestamp, 1},
		{"too old", gaugeMetric("g", dpAt(testNow.Add(-31*24*time.Hour), 1)), ingest.RejectInvalidTimestamp, 1},
		// Less than 30 days old, but its day's partition expired at midnight.
		{"in an expired day", gaugeMetric("g", dpAt(midnightUTC(testNow.Add(-testRetention)).Add(23*time.Hour), 1)), ingest.RejectInvalidTimestamp, 1},
		{"too far future", gaugeMetric("g", dpAt(testNow.Add(2*time.Hour), 1)), ingest.RejectInvalidTimestamp, 1},
		{"missing value", gaugeMetric("g", &metricspb.NumberDataPoint{TimeUnixNano: uint64(testNow.UnixNano())}), ingest.RejectMissingValue, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := gaugeMetric("ok", dpAt(testNow, 1))
			got := mapIt(wrap(svc(), tt.metric, valid))
			if got.Rejected[tt.reason] != tt.count || got.RejectedCount() != tt.count {
				t.Errorf("Rejected = %v, want %s:%d", got.Rejected, tt.reason, tt.count)
			}
			if len(got.Datapoints) != 1 || got.SeriesRows[0].MetricName != "ok" {
				t.Errorf("valid datapoint not mapped: %+v", got)
			}
		})
	}
}

// bigResourceMetrics returns ten one-series metrics under a resource with a
// 1 KB attribute, so each series row is ~1.1 KB; m0 has three datapoints, its
// last one after all other metrics.
func bigResourceMetrics() []*metricspb.ResourceMetrics {
	res := append(svc(), strKV("big", strings.Repeat("x", 1000)))
	metrics := []*metricspb.Metric{gaugeMetric("m0", dpAt(testNow, 1), dpAt(testNow.Add(-time.Minute), 2))}
	for i := 1; i < 10; i++ {
		metrics = append(metrics, gaugeMetric(fmt.Sprintf("m%d", i), dpAt(testNow, 1)))
	}
	metrics = append(metrics, gaugeMetric("m0", dpAt(testNow.Add(-2*time.Minute), 3)))
	return wrap(res, metrics...)
}

// A large shared resource attribute across many tiny metrics is held back by
// the series limit: a 3.5 KB limit fits three ~1.1 KB series rows. Later datapoints
// of an accepted series are still stored.
func TestSeriesLimit(t *testing.T) {
	opts := testMapOptions()
	opts.MaxSeriesBytes = 3500
	got := ingest.Map(bigResourceMetrics(), opts)
	if len(got.SeriesRows) != 3 {
		t.Fatalf("series rows = %d, want 3", len(got.SeriesRows))
	}
	if n := got.Rejected[ingest.RejectSeriesLimitExceeded]; n != 7 || got.RejectedCount() != 7 {
		t.Errorf("Rejected = %v, want series_limit_exceeded:7", got.Rejected)
	}
	// m0's three datapoints and one each for m1, m2.
	if len(got.Datapoints) != 5 {
		t.Errorf("datapoints = %d, want 5", len(got.Datapoints))
	}
}

// Series rows already stored create nothing, so they don't count against the
// series limit: with m1..m9 stored, m0 is the only new row and fits alone.
func TestSeriesLimitSkipsStoredRows(t *testing.T) {
	all := ingest.Map(bigResourceMetrics(), testMapOptions())
	stored := map[ingest.SeriesRowKey]bool{}
	for _, s := range all.SeriesRows[1:] {
		stored[s.Key()] = true
	}
	opts := testMapOptions()
	opts.MaxSeriesBytes = 1500
	opts.Stored = func(k ingest.SeriesRowKey) bool { return stored[k] }
	got := ingest.Map(bigResourceMetrics(), opts)
	if got.RejectedCount() != 0 || len(got.Datapoints) != 12 || len(got.SeriesRows) != 10 {
		t.Errorf("got %d series rows, %d datapoints, rejected %v; want 10, 12, none",
			len(got.SeriesRows), len(got.Datapoints), got.Rejected)
	}
}

// The first instant of the oldest day the TTL still keeps is accepted.
func TestOldestKeptDayAccepted(t *testing.T) {
	oldest := midnightUTC(testNow.Add(-testRetention)).AddDate(0, 0, 1)
	got := mapIt(wrap(svc(), gaugeMetric("g", dpAt(oldest, 1))))
	if len(got.Datapoints) != 1 || got.RejectedCount() != 0 {
		t.Errorf("datapoint at %v: got %+v, want it stored", oldest, got)
	}
}

func TestNoRecordedValueAccepted(t *testing.T) {
	dp := &metricspb.NumberDataPoint{TimeUnixNano: uint64(testNow.UnixNano()), Flags: 1}
	got := mapIt(wrap(svc(), gaugeMetric("g", dp)))
	if got.RejectedCount() != 0 || len(got.Datapoints) != 1 {
		t.Fatalf("got %+v", got)
	}
	if d := got.Datapoints[0]; !math.IsNaN(d.Value) || d.Flags != 1 {
		t.Errorf("bad datapoint: %+v", d)
	}
}

func TestNaNAndInfAccepted(t *testing.T) {
	got := mapIt(wrap(svc(), gaugeMetric("g", dpAt(testNow, math.NaN()), dpAt(testNow.Add(time.Second), math.Inf(1)), dpAt(testNow.Add(2*time.Second), math.Inf(-1)))))
	if got.RejectedCount() != 0 || len(got.Datapoints) != 3 {
		t.Fatalf("got %+v", got)
	}
	if !math.IsNaN(got.Datapoints[0].Value) || !math.IsInf(got.Datapoints[1].Value, 1) || !math.IsInf(got.Datapoints[2].Value, -1) {
		t.Errorf("values changed: %+v", got.Datapoints)
	}
}

func TestZeroStartTimeAccepted(t *testing.T) {
	got := mapIt(wrap(svc(), gaugeMetric("g", dpAt(testNow, 1))))
	if got.RejectedCount() != 0 || len(got.Datapoints) != 1 {
		t.Fatalf("got %+v", got)
	}
	if !got.Datapoints[0].StartTimeUnix.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("StartTimeUnix = %v", got.Datapoints[0].StartTimeUnix)
	}
}

func TestMissingServiceName(t *testing.T) {
	got := mapIt(wrap(nil, gaugeMetric("g", dpAt(testNow, 1))))
	if len(got.SeriesRows) != 1 || got.SeriesRows[0].ServiceName != "" {
		t.Fatalf("got %+v", got.SeriesRows)
	}
}

func TestNonStringServiceName(t *testing.T) {
	res := []*commonpb.KeyValue{{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 7}}}}
	got := mapIt(wrap(res, gaugeMetric("g", dpAt(testNow, 1))))
	if len(got.SeriesRows) != 1 || got.SeriesRows[0].ServiceName != "7" {
		t.Fatalf("got %+v", got.SeriesRows)
	}
}

// A repeated service.name must not leave ServiceName and
// ResourceAttributes["service.name"] disagreeing on the same row.
func TestDuplicateServiceNameConsistent(t *testing.T) {
	res := []*commonpb.KeyValue{strKV("service.name", "first"), strKV("service.name", "last")}
	got := mapIt(wrap(res, gaugeMetric("g", dpAt(testNow, 1))))
	if len(got.SeriesRows) != 1 {
		t.Fatalf("series = %d, want 1", len(got.SeriesRows))
	}
	s := got.SeriesRows[0]
	if s.ServiceName != s.ResourceAttributes["service.name"] {
		t.Errorf("ServiceName %q != ResourceAttributes[service.name] %q", s.ServiceName, s.ResourceAttributes["service.name"])
	}
}

func TestRejectionMessage(t *testing.T) {
	m := ingest.Mapped{Rejected: map[ingest.RejectReason]int64{ingest.RejectInvalidTimestamp: 1, ingest.RejectUnsupportedMetricType: 2}}
	want := "3 datapoints rejected: 1 invalid_timestamp, 2 unsupported_metric_type"
	if got := m.RejectionMessage(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := (ingest.Mapped{}).RejectionMessage(); got != "" {
		t.Errorf("empty = %q", got)
	}
}

func TestStartTimeOverflowTreatedAsZero(t *testing.T) {
	dp := dpAt(testNow, 1)
	dp.StartTimeUnixNano = math.MaxInt64 + 1
	got := mapIt(wrap(svc(), gaugeMetric("g", dp)))
	if got.RejectedCount() != 0 || len(got.Datapoints) != 1 {
		t.Fatalf("got %+v", got)
	}
	if !got.Datapoints[0].StartTimeUnix.Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("StartTimeUnix = %v, want Unix epoch", got.Datapoints[0].StartTimeUnix)
	}
}

// BenchmarkMapSharedResourceAttributes shows mapping cost stays linear when
// many datapoints share a large resource: compare R=1000 vs R=4000 at fixed P.
func BenchmarkMapSharedResourceAttributes(b *testing.B) {
	for _, r := range []int{1000, 4000} {
		b.Run(fmt.Sprintf("R=%d", r), func(b *testing.B) {
			res := svc()
			for i := range r {
				res = append(res, strKV(fmt.Sprintf("k%d", i), ""))
			}
			dps := make([]*metricspb.NumberDataPoint, 2000)
			for i := range dps {
				dps[i] = dpAt(testNow, float64(i))
			}
			rms := wrap(res, gaugeMetric("g", dps...))
			for b.Loop() {
				mapIt(rms)
			}
		})
	}
}
