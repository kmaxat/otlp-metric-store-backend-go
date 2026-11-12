// Package ingest turns OTLP Export Requests into series rows and datapoints
// and stores them through a Store. Terms follow GLOSSARY.md.
//
// Field names such as SeriesId and ResourceSchemaUrl mirror the ClickHouse
// column names (and the OpenTelemetry Collector's ClickHouse schema) rather
// than Go's ID/URL style, so a field and its column always read the same.
package ingest

import "time"

// MetricType is the metric type (gauge or sum) of the metric a series belongs to.
type MetricType string

const (
	MetricTypeGauge MetricType = "gauge"
	MetricTypeSum   MetricType = "sum"
)

// SeriesRow is one row of otel_metrics_series.
type SeriesRow struct {
	Day                    time.Time // UTC midnight
	SeriesId               uint64
	MetricType             MetricType
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	AggregationTemporality int32
	IsMonotonic            bool
	ServiceName            string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	ScopeSchemaUrl         string
	Attributes             map[string]string
}

// Datapoint is one row of otel_metrics_datapoints.
type Datapoint struct {
	SeriesId      uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// SeriesRowKey identifies one series row: a series on a given day. Day is Unix
// seconds rather than a time.Time, whose == also compares the *time.Location.
type SeriesRowKey struct {
	Day      int64
	SeriesId uint64
}

// Key returns the key of the series row s is stored as.
func (s SeriesRow) Key() SeriesRowKey {
	return SeriesRowKey{Day: s.Day.Unix(), SeriesId: s.SeriesId}
}
