package ingest

import (
	"encoding/base64"
	"encoding/json"
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map. When a key
// repeats, the last value wins.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to a deterministic string.
// Scalars use their natural form, bytes are base64-encoded, and arrays and
// kvlists are rendered as JSON with sorted map keys so equal values always
// produce equal strings.
func anyValueToString(v *commonpb.AnyValue) string {
	switch x := anyValueToJSONable(v).(type) {
	case nil:
		return ""
	case string:
		return x
	default:
		// Can't fail: x holds only strings, nils, []any and map[string]any.
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// anyValueToJSONable maps an AnyValue to a JSON-marshalable Go value: a string
// for scalars, []any for arrays, map[string]any for kvlists, nil if unset.
func anyValueToJSONable(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		values := x.ArrayValue.GetValues()
		out := make([]any, 0, len(values))
		for _, e := range values {
			out = append(out, anyValueToJSONable(e))
		}
		return out
	case *commonpb.AnyValue_KvlistValue:
		values := x.KvlistValue.GetValues()
		out := make(map[string]any, len(values))
		for _, kv := range values {
			out[kv.GetKey()] = anyValueToJSONable(kv.GetValue())
		}
		return out
	default:
		return nil
	}
}
