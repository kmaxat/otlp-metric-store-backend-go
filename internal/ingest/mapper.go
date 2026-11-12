package ingest

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// RejectReason says why a datapoint was rejected instead of stored.
type RejectReason string

const (
	RejectUnsupportedMetricType RejectReason = "unsupported_metric_type"
	RejectMissingMetricName     RejectReason = "missing_metric_name"
	RejectInvalidTemporality    RejectReason = "invalid_temporality"
	RejectInvalidTimestamp      RejectReason = "invalid_timestamp"
	RejectMissingValue          RejectReason = "missing_value"
	RejectSeriesLimitExceeded   RejectReason = "series_limit_exceeded"
)

// maxFutureSkew is how far past now a datapoint timestamp may be.
const maxFutureSkew = time.Hour

// MapOptions bound what Map accepts from one Export Request.
type MapOptions struct {
	// Now and Retention bound valid timestamps, retention counted in whole
	// UTC days like the tables' TTL.
	Now       time.Time
	Retention time.Duration
	// MaxSeriesBytes is the series limit: the most bytes of new series rows the
	// request may create (see seriesBudget).
	MaxSeriesBytes int
	// Stored reports whether a series row is already stored; nil means no
	// row is known to be stored.
	Stored func(SeriesRowKey) bool
}

// seriesRowOverhead approximates the fixed-size columns of one series row
// (Day, SeriesId, numbers), so the series limit also bounds the row count.
const seriesRowOverhead = 64

// mapBytes returns the total key and value length of m.
func mapBytes(m map[string]string) int {
	n := 0
	for k, v := range m {
		n += len(k) + len(v)
	}
	return n
}

// seriesBudget is what is left of the series limit (MaxSeriesBytes) for the
// series rows one request creates. Every series row repeats its resource and
// scope attributes, so without a limit one small, compressible request (a
// large shared attribute, many tiny metrics) could create gigabytes of series
// rows.
type seriesBudget struct {
	left   int
	stored func(SeriesRowKey) bool
}

// admit reports whether the series row key, of rowBytes, may be created, and
// if so takes its bytes from what is left of the series limit. A row already
// stored creates nothing, so it is admitted without counting against the
// limit: a sender whose series rows are all stored is never rejected, as long
// as the series cache still holds them.
func (b *seriesBudget) admit(key SeriesRowKey, rowBytes int) bool {
	if b.stored != nil && b.stored(key) {
		return true
	}
	if rowBytes > b.left {
		return false
	}
	b.left -= rowBytes
	return true
}

// Mapped is the storage-ready result of mapping one Export Request.
type Mapped struct {
	SeriesRows []SeriesRow // unique by SeriesRowKey, in first-seen order
	Datapoints []Datapoint
	Rejected   map[RejectReason]int64 // nil when nothing rejected
}

// RejectedCount returns the total number of rejected datapoints.
func (m Mapped) RejectedCount() int64 {
	var n int64
	for _, c := range m.Rejected {
		n += c
	}
	return n
}

// Received returns the number of datapoints in the request: stored plus rejected.
func (m Mapped) Received() int64 {
	return int64(len(m.Datapoints)) + m.RejectedCount()
}

// RejectionMessage returns "" when nothing was rejected, otherwise e.g.
// "3 datapoints rejected: 1 invalid_timestamp, 2 unsupported_metric_type".
func (m Mapped) RejectionMessage() string {
	total := m.RejectedCount()
	if total == 0 {
		return ""
	}
	parts := make([]string, 0, len(m.Rejected))
	for _, r := range slices.Sorted(maps.Keys(m.Rejected)) {
		parts = append(parts, fmt.Sprintf("%d %s", m.Rejected[r], r))
	}
	noun := "datapoints"
	if total == 1 {
		noun = "datapoint"
	}
	return fmt.Sprintf("%d %s rejected: %s", total, noun, strings.Join(parts, ", "))
}

// utcDay returns UTC midnight of the day containing t.
func utcDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// noRecordedValueFlag is the OTLP DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK bit.
const noRecordedValueFlag = uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK)

// Map validates and converts datapoints within the bounds of opts. A
// datapoint that needs a new series row beyond opts.MaxSeriesBytes is
// rejected as series_limit_exceeded; datapoints of rows already accepted in
// the request are kept.
func Map(rms []*metricspb.ResourceMetrics, opts MapOptions) Mapped {
	var out Mapped
	seen := make(map[SeriesRowKey]struct{})
	budget := seriesBudget{left: opts.MaxSeriesBytes, stored: opts.Stored}
	// The TTL drops a whole day's partition once that day + retention has
	// passed, so the oldest datapoint still kept is at the start of the day
	// after now - retention. Anything older would be stored and then dropped.
	oldest := utcDay(opts.Now.Add(-opts.Retention)).AddDate(0, 0, 1)
	newest := opts.Now.Add(maxFutureSkew)

	reject := func(r RejectReason, n int) {
		if n == 0 {
			return
		}
		if out.Rejected == nil {
			out.Rejected = make(map[RejectReason]int64)
		}
		out.Rejected[r] += int64(n)
	}

	for _, rm := range rms {
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		// From the map, so ServiceName always equals ResourceAttributes["service.name"].
		svcName := resAttrs["service.name"]
		// Hash each OTLP level once and reuse it below (see seriesHasher).
		resHasher := newSeriesHasher().withResource(resAttrs, rm.GetSchemaUrl())
		// Sizes are computed once per level, like the hashes.
		resBytes := len(svcName) + mapBytes(resAttrs) + len(rm.GetSchemaUrl())

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())
			scopeHasher := resHasher.withScope(scope.GetName(), scope.GetVersion(), scopeAttrs, sm.GetSchemaUrl())
			scopeBytes := len(scope.GetName()) + len(scope.GetVersion()) + mapBytes(scopeAttrs) + len(sm.GetSchemaUrl())

			for _, metric := range sm.GetMetrics() {
				tmpl := SeriesRow{
					MetricName:            metric.GetName(),
					MetricDescription:     metric.GetDescription(),
					MetricUnit:            metric.GetUnit(),
					ServiceName:           svcName,
					ResourceAttributes:    resAttrs,
					ResourceSchemaUrl:     rm.GetSchemaUrl(),
					ScopeName:             scope.GetName(),
					ScopeVersion:          scope.GetVersion(),
					ScopeAttributes:       scopeAttrs,
					ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
					ScopeSchemaUrl:        sm.GetSchemaUrl(),
				}

				var datapoints []*metricspb.NumberDataPoint
				switch d := metric.GetData().(type) {
				case *metricspb.Metric_Gauge:
					tmpl.MetricType = MetricTypeGauge
					datapoints = d.Gauge.GetDataPoints()
				case *metricspb.Metric_Sum:
					tmpl.MetricType = MetricTypeSum
					tmpl.AggregationTemporality = int32(d.Sum.GetAggregationTemporality())
					tmpl.IsMonotonic = d.Sum.GetIsMonotonic()
					datapoints = d.Sum.GetDataPoints()
				case *metricspb.Metric_Histogram:
					reject(RejectUnsupportedMetricType, len(d.Histogram.GetDataPoints()))
					continue
				case *metricspb.Metric_ExponentialHistogram:
					reject(RejectUnsupportedMetricType, len(d.ExponentialHistogram.GetDataPoints()))
					continue
				case *metricspb.Metric_Summary:
					reject(RejectUnsupportedMetricType, len(d.Summary.GetDataPoints()))
					continue
				default:
					continue
				}

				if tmpl.MetricName == "" {
					reject(RejectMissingMetricName, len(datapoints))
					continue
				}
				// Only delta and cumulative are valid; unspecified and values
				// outside the enum would otherwise become part of the SeriesId.
				if tmpl.MetricType == MetricTypeSum &&
					tmpl.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA) &&
					tmpl.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
					reject(RejectInvalidTemporality, len(datapoints))
					continue
				}
				metricHasher := scopeHasher.withMetric(tmpl.MetricType, tmpl.MetricName, tmpl.MetricUnit,
					tmpl.AggregationTemporality, tmpl.IsMonotonic)
				metricBytes := resBytes + scopeBytes + len(tmpl.MetricName) + len(tmpl.MetricDescription) +
					len(tmpl.MetricUnit) + len(tmpl.MetricType) + seriesRowOverhead

				for _, dp := range datapoints {
					n := dp.GetTimeUnixNano()
					if n == 0 || n > math.MaxInt64 {
						reject(RejectInvalidTimestamp, 1)
						continue
					}
					t := time.Unix(0, int64(n)).UTC()
					if t.Before(oldest) || t.After(newest) {
						reject(RejectInvalidTimestamp, 1)
						continue
					}

					flags := dp.GetFlags()
					var value float64
					switch v := dp.GetValue().(type) {
					case *metricspb.NumberDataPoint_AsDouble:
						value = v.AsDouble
					case *metricspb.NumberDataPoint_AsInt:
						value = float64(v.AsInt)
					default:
						if flags&noRecordedValueFlag == 0 {
							reject(RejectMissingValue, 1)
							continue
						}
						value = math.NaN()
					}

					s := tmpl
					s.Attributes = kvToMap(dp.GetAttributes())
					s.SeriesId = metricHasher.sum(s.Attributes)
					s.Day = utcDay(t)
					if _, ok := seen[s.Key()]; !ok {
						if !budget.admit(s.Key(), metricBytes+mapBytes(s.Attributes)) {
							reject(RejectSeriesLimitExceeded, 1)
							continue
						}
						seen[s.Key()] = struct{}{}
						out.SeriesRows = append(out.SeriesRows, s)
					}
					// A start time that doesn't fit int64 nanoseconds is treated as unknown (0).
					startNano := dp.GetStartTimeUnixNano()
					if startNano > math.MaxInt64 {
						startNano = 0
					}
					out.Datapoints = append(out.Datapoints, Datapoint{
						SeriesId:      s.SeriesId,
						StartTimeUnix: time.Unix(0, int64(startNano)).UTC(),
						TimeUnix:      t,
						Value:         value,
						Flags:         flags,
					})
				}
			}
		}
	}
	return out
}
