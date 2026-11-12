package ingest_test

import (
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"dash0.com/otlp-metric-store/internal/ingest"
)

// FuzzMap feeds arbitrary Export Requests to Map. It must not panic, must
// account for every datapoint (stored or rejected), and every stored
// datapoint must have exactly one series row for its SeriesId and day.
// Run longer with: go test -fuzz FuzzMap ./internal/ingest/
func FuzzMap(f *testing.F) {
	for _, rms := range [][]*metricspb.ResourceMetrics{
		wrap(svc(), gaugeMetric("g", dpAt(testNow, 1), dpAt(testNow.Add(-time.Hour), 2))),
		wrap(svc(), sumMetric("s", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA, true, dpAt(testNow, 1))),
		wrap(svc(), gaugeMetric("", dpAt(testNow, 1))),
	} {
		b, err := proto.Marshal(&colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rms})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, b []byte) {
		var req colmetricspb.ExportMetricsServiceRequest
		if err := proto.Unmarshal(b, &req); err != nil {
			t.Skip()
		}
		got := ingest.Map(req.GetResourceMetrics(), testMapOptions())

		if want := countDatapoints(req.GetResourceMetrics()); got.Received() != want {
			t.Errorf("Received() = %d, want %d datapoints in the request", got.Received(), want)
		}
		rows := make(map[ingest.SeriesRowKey]bool, len(got.SeriesRows))
		for _, s := range got.SeriesRows {
			if rows[s.Key()] {
				t.Errorf("duplicate series row %+v", s.Key())
			}
			rows[s.Key()] = true
		}
		for _, d := range got.Datapoints {
			key := ingest.SeriesRow{Day: midnightUTC(d.TimeUnix), SeriesId: d.SeriesId}.Key()
			if !rows[key] {
				t.Errorf("datapoint %+v has no series row", d)
			}
		}
	})
}

// countDatapoints returns the number of datapoints of every metric type in rms.
func countDatapoints(rms []*metricspb.ResourceMetrics) int64 {
	var n int
	for _, rm := range rms {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				n += len(m.GetGauge().GetDataPoints()) + len(m.GetSum().GetDataPoints()) +
					len(m.GetHistogram().GetDataPoints()) + len(m.GetExponentialHistogram().GetDataPoints()) +
					len(m.GetSummary().GetDataPoints())
			}
		}
	}
	return int64(n)
}
