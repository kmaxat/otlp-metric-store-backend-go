package ingest_test

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func strVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func intVal(i int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: i}}
}

func arrayVal(vs ...*commonpb.AnyValue) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vs}}}
}

func kvlistVal(kvs ...*commonpb.KeyValue) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: kvs}}}
}

func kv(k string, v *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: v}
}

// attrOf maps one gauge datapoint carrying attribute "k" = v and returns the
// stored string for "k".
func attrOf(t *testing.T, v *commonpb.AnyValue) string {
	t.Helper()
	dp := dpAt(testNow, 1)
	dp.Attributes = []*commonpb.KeyValue{kv("k", v)}
	got := mapIt(wrap(svc(), gaugeMetric("g", dp)))
	if len(got.SeriesRows) != 1 {
		t.Fatalf("series = %d, want 1", len(got.SeriesRows))
	}
	return got.SeriesRows[0].Attributes["k"]
}

func TestAttributeValuesStoredAsStrings(t *testing.T) {
	tests := []struct {
		name string
		in   *commonpb.AnyValue
		want string
	}{
		{"nil", nil, ""},
		{"string", strVal("x"), "x"},
		{"int", intVal(42), "42"},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.5}}, "1.5"},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"bytes", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{1, 2}}}, "AQI="},
		{"array", arrayVal(strVal("a"), intVal(1)), `["a","1"]`},
		{"kvlist", kvlistVal(kv("b", strVal("2")), kv("a", strVal("1"))), `{"a":"1","b":"2"}`},
		{"kvlist with nested array", kvlistVal(kv("k", arrayVal(strVal("a"), intVal(1)))), `{"k":["a","1"]}`},
		{"unset", &commonpb.AnyValue{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := attrOf(t, tt.in); got != tt.want {
				t.Errorf("stored attribute = %q, want %q", got, tt.want)
			}
		})
	}
}

// A kvlist's string form must not depend on Go map iteration order, or the
// same series would get a different SeriesId from one request to the next.
func TestKvlistAttributeIsStable(t *testing.T) {
	v := kvlistVal(kv("z", strVal("1")), kv("m", strVal("2")), kv("a", strVal("3")), kv("b", strVal("4")))
	want := attrOf(t, v)
	for i := 0; i < 100; i++ {
		if got := attrOf(t, v); got != want {
			t.Fatalf("iteration %d: got %q, want %q", i, got, want)
		}
	}
}
