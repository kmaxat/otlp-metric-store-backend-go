package ingest

import (
	"encoding/binary"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// fingerprintVersion is written first into every fingerprint so the encoding
// can change without silently colliding with ids from an older encoding.
const fingerprintVersion byte = 1

// seriesHasher computes SeriesId, the fingerprint of a series' identity.
//
// Identity fields, hashed in this fixed order, outermost OTLP level first:
// resource (ResourceAttributes, ResourceSchemaUrl), scope
// (ScopeName, ScopeVersion, ScopeAttributes, ScopeSchemaUrl), metric
// (MetricType, MetricName, MetricUnit, AggregationTemporality, IsMonotonic),
// datapoint (Attributes).
//
// Excluded: Day and SeriesId (not identity), ServiceName (a copy of
// ResourceAttributes["service.name"], already hashed there), MetricDescription
// and ScopeDroppedAttrCount (cosmetic text/counters must not split a series'
// history).
//
// It is an xxhash state that has already consumed a prefix of the
// identity encoding. It is a plain value: each with* method returns a copy
// extended by one OTLP level, so the mapper hashes a resource's attributes
// once and reuses that state for every scope, metric and datapoint below it.
// This keeps hashing cost linear in the request size; re-hashing the shared
// resource attributes for every datapoint would be quadratic.
type seriesHasher struct {
	d xxhash.Digest
}

// newSeriesHasher returns a hasher that has consumed only the version byte.
func newSeriesHasher() seriesHasher {
	var h seriesHasher
	h.d.Reset()
	h.d.Write([]byte{fingerprintVersion})
	return h
}

// withResource returns h extended with the resource-level identity fields.
func (h seriesHasher) withResource(attrs map[string]string, schemaURL string) seriesHasher {
	b := appendMap(nil, attrs)
	b = appendString(b, schemaURL)
	h.d.Write(b)
	return h
}

// withScope returns h extended with the scope-level identity fields.
func (h seriesHasher) withScope(name, version string, attrs map[string]string, schemaURL string) seriesHasher {
	b := appendString(nil, name)
	b = appendString(b, version)
	b = appendMap(b, attrs)
	b = appendString(b, schemaURL)
	h.d.Write(b)
	return h
}

// withMetric returns h extended with the metric-level identity fields.
func (h seriesHasher) withMetric(t MetricType, name, unit string, temporality int32, monotonic bool) seriesHasher {
	b := appendString(nil, string(t))
	b = appendString(b, name)
	b = appendString(b, unit)
	b = binary.LittleEndian.AppendUint64(b, uint64(int64(temporality)))
	if monotonic {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	h.d.Write(b)
	return h
}

// sum returns the SeriesId for a datapoint with attrs under h's prefix.
func (h seriesHasher) sum(attrs map[string]string) uint64 {
	h.d.Write(appendMap(nil, attrs))
	return h.d.Sum64()
}

// appendString appends a uvarint length prefix followed by the bytes of v.
func appendString(b []byte, v string) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

// appendMap appends the entry count followed by key-sorted (key, value) pairs.
func appendMap(b []byte, m map[string]string) []byte {
	b = binary.AppendUvarint(b, uint64(len(m)))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b = appendString(b, k)
		b = appendString(b, m[k])
	}
	return b
}
