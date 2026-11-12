package ingest_test

import (
	"fmt"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// identityRequest is one Sum datapoint with every identity field set.
func identityRequest() []*metricspb.ResourceMetrics {
	dp := dpAt(testNow, 1)
	dp.Attributes = []*commonpb.KeyValue{strKV("method", "GET")}
	m := sumMetric("http.requests", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true, dp)
	m.Description, m.Unit = "requests", "1"
	return []*metricspb.ResourceMetrics{{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "svc"), strKV("host", "a")}},
		SchemaUrl: "r",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{
				Name: "scope", Version: "1.0", Attributes: []*commonpb.KeyValue{strKV("s", "1")},
			},
			SchemaUrl: "s",
			Metrics:   []*metricspb.Metric{m},
		}},
	}}
}

// levels points into an identityRequest, one field per OTLP level.
type levels struct {
	rm *metricspb.ResourceMetrics
	sm *metricspb.ScopeMetrics
	m  *metricspb.Metric
	dp *metricspb.NumberDataPoint
}

// levelsOf returns the levels of an identityRequest.
func levelsOf(rms []*metricspb.ResourceMetrics) levels {
	rm := rms[0]
	sm := rm.ScopeMetrics[0]
	m := sm.Metrics[0]
	return levels{rm: rm, sm: sm, m: m, dp: m.GetSum().GetDataPoints()[0]}
}

// seriesIdOf maps rms and returns the SeriesId of its single series.
func seriesIdOf(t *testing.T, rms []*metricspb.ResourceMetrics) uint64 {
	t.Helper()
	got := mapIt(rms)
	if len(got.SeriesRows) != 1 {
		t.Fatalf("series = %d (rejected %v), want 1", len(got.SeriesRows), got.Rejected)
	}
	return got.SeriesRows[0].SeriesId
}

// withChange returns the SeriesId of an identityRequest after change.
func withChange(t *testing.T, change func(l levels)) uint64 {
	t.Helper()
	rms := identityRequest()
	change(levelsOf(rms))
	return seriesIdOf(t, rms)
}

func TestSeriesIdStableAcrossAttributeOrder(t *testing.T) {
	forward, reverse := identityRequest(), identityRequest()
	a, b := levelsOf(forward).dp, levelsOf(reverse).dp
	a.Attributes, b.Attributes = nil, nil
	for i := 0; i < 20; i++ {
		a.Attributes = append(a.Attributes, strKV(fmt.Sprintf("k%d", i), fmt.Sprint(i)))
	}
	for i := 19; i >= 0; i-- {
		b.Attributes = append(b.Attributes, strKV(fmt.Sprintf("k%d", i), fmt.Sprint(i)))
	}
	if seriesIdOf(t, forward) != seriesIdOf(t, reverse) {
		t.Fatal("ids differ for the same attributes in a different order")
	}
}

func TestSeriesIdChangesWithEachIdentityField(t *testing.T) {
	tests := map[string]func(l levels){
		"MetricType": func(l levels) {
			l.m.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{l.dp}}}
		},
		"MetricName": func(l levels) {
			l.m.Name = "other"
		},
		"MetricUnit": func(l levels) {
			l.m.Unit = "ms"
		},
		"AggregationTemporality": func(l levels) {
			l.m.GetSum().AggregationTemporality = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
		},
		"IsMonotonic": func(l levels) {
			l.m.GetSum().IsMonotonic = false
		},
		"ServiceName": func(l levels) {
			l.rm.Resource.Attributes[0] = strKV("service.name", "other")
		},
		"ResourceAttributeValue": func(l levels) {
			l.rm.Resource.Attributes[1] = strKV("host", "b")
		},
		"ResourceAttributeKey": func(l levels) {
			l.rm.Resource.Attributes = append(l.rm.Resource.Attributes, strKV("extra", "x"))
		},
		"ResourceSchemaUrl": func(l levels) {
			l.rm.SchemaUrl = "r2"
		},
		"ScopeName": func(l levels) {
			l.sm.Scope.Name = "other"
		},
		"ScopeVersion": func(l levels) {
			l.sm.Scope.Version = "2.0"
		},
		"ScopeAttributes": func(l levels) {
			l.sm.Scope.Attributes = []*commonpb.KeyValue{strKV("s", "2")}
		},
		"ScopeSchemaUrl": func(l levels) {
			l.sm.SchemaUrl = "s2"
		},
		"Attributes": func(l levels) {
			l.dp.Attributes = []*commonpb.KeyValue{strKV("method", "POST")}
		},
	}
	base := seriesIdOf(t, identityRequest())
	for name, c := range tests {
		t.Run(name, func(t *testing.T) {
			if withChange(t, c) == base {
				t.Fatalf("id unchanged after changing %s", name)
			}
		})
	}
}

func TestSeriesIdIgnoresNonIdentityFields(t *testing.T) {
	base := seriesIdOf(t, identityRequest())
	got := withChange(t, func(l levels) {
		l.m.Description = "different"
		l.sm.Scope.DroppedAttributesCount = 7
		l.dp.TimeUnixNano -= uint64(3 * 24 * 60 * 60 * 1e9) // another Day
		l.dp.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: 99}
	})
	if got != base {
		t.Fatal("id changed after changing non-identity fields")
	}
}

func TestSeriesIdNoConcatenationAmbiguity(t *testing.T) {
	a := withChange(t, func(l levels) {
		l.dp.Attributes = []*commonpb.KeyValue{strKV("ab", "c")}
	})
	b := withChange(t, func(l levels) {
		l.dp.Attributes = []*commonpb.KeyValue{strKV("a", "bc")}
	})
	if a == b {
		t.Fatal("attribute key/value boundary is ambiguous")
	}
	a = withChange(t, func(l levels) {
		l.m.Name, l.m.Unit = "ab", "c"
	})
	b = withChange(t, func(l levels) {
		l.m.Name, l.m.Unit = "a", "bc"
	})
	if a == b {
		t.Fatal("name/unit boundary is ambiguous")
	}
}

// Pins the encoding: if this changes, every stored SeriesId
// changes too, which must be a deliberate fingerprint version bump.
func TestSeriesIdGolden(t *testing.T) {
	const want uint64 = 14200474360059273409
	if got := seriesIdOf(t, identityRequest()); got != want {
		t.Errorf("SeriesId of identityRequest() = %d, want %d", got, want)
	}
}

// The hasher reuses each OTLP level's hash state for the levels below it, so
// a series' SeriesId must not depend on the resources, scopes, metrics or
// datapoints mapped before it in the same request.
func TestSeriesIdIndependentOfSiblings(t *testing.T) {
	want := seriesIdOf(t, identityRequest())
	rms := identityRequest()
	l := levelsOf(rms)
	other := func() *metricspb.Metric {
		dp := dpAt(testNow, 2)
		dp.Attributes = []*commonpb.KeyValue{strKV("method", "PUT")}
		return sumMetric("http.requests", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA, false, dp)
	}
	siblingRes := &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "other"), strKV("host", "z")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{Name: "other"}, Metrics: []*metricspb.Metric{other()},
		}},
	}
	siblingScope := &metricspb.ScopeMetrics{
		Scope: &commonpb.InstrumentationScope{Name: "other"}, Metrics: []*metricspb.Metric{other()},
	}
	siblingDp := dpAt(testNow, 3)
	siblingDp.Attributes = []*commonpb.KeyValue{strKV("method", "POST")}
	l.m.GetSum().DataPoints = append([]*metricspb.NumberDataPoint{siblingDp}, l.dp)
	l.sm.Metrics = append([]*metricspb.Metric{other()}, l.m)
	l.rm.ScopeMetrics = append([]*metricspb.ScopeMetrics{siblingScope}, l.sm)
	rms = append([]*metricspb.ResourceMetrics{siblingRes}, rms...)

	got := mapIt(rms)
	found := false
	for _, s := range got.SeriesRows {
		if s.ResourceAttributes["host"] == "a" && s.ScopeName == "scope" &&
			s.MetricName == "http.requests" && s.Attributes["method"] == "GET" {
			found = true
			if s.SeriesId != want {
				t.Errorf("SeriesId after siblings = %d, want %d", s.SeriesId, want)
			}
		}
	}
	if !found {
		t.Fatalf("target series not mapped (rejected %v)", got.Rejected)
	}
}
