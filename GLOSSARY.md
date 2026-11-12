# OTLP Metric Store

Receives OpenTelemetry metrics and stores them as a lookup table of series rows plus thin datapoints that reference it, so data can always be read by time range without full scans.

## Incoming data (OTLP)

**Export Request**:
One OTLP call from a sender, carrying resources, their scopes, their metrics, and the metrics' datapoints.
_Avoid_: Message, payload, batch

**Resource**:
The entity that produced telemetry (a service, a host, a pod), described by resource attributes.
_Avoid_: Source, origin

**Scope**:
The instrumentation scope that recorded a metric, with its name, version and attributes.
_Avoid_: Instrumentation library, library, meter

**Metric**:
A named, typed measurement (Gauge or Sum) with a unit, under one scope.
_Avoid_: Instrument, signal

**Metric Type**:
Whether a metric is a Gauge or a Sum. Histogram, Exponential Histogram and Summary are recognised but not supported.
_Avoid_: Kind, data type

**Datapoint**:
One measured value of a metric at a point in time, with its own attributes.
_Avoid_: Data point, data-point, sample, point, record

**No-Value Marker**:
A datapoint that the sender flags as having no recorded value, such as a series going stale. It is stored, not rejected as a missing value.
_Avoid_: Stale marker, NaN point, empty datapoint

**Sender**:
Any OTLP client that calls the store (an SDK, a Collector, telemetrygen).
_Avoid_: Client, exporter, producer

## Series

**Series**:
One continuous time series: every datapoint sharing a series identity, across all days.
_Avoid_: Stream, timeseries, metric (a metric has many series)

**Series Row**:
The Lookup Table entry describing one series on one UTC day. A series reporting across midnight has two series rows and one history.
_Avoid_: Series (for the row), metadata row

**Series Identity**:
The fields that decide whether two datapoints belong to the same time series: resource, scope, metric name, type, unit, temporality, monotonicity, and datapoint attributes. Description and dropped-attribute counts are not part of it.
_Avoid_: Metadata, labels, key

**SeriesId**:
The fingerprint of a series identity. Equal identities always give the same SeriesId, on any instance, with no coordination.
_Avoid_: Hash, series key, fingerprint (as a noun for the ID)

**Lookup Table**:
The store of series rows that datapoints reference by SeriesId.
_Avoid_: Metadata table, dimension table

**Series Cache**:
The in-memory record of series rows this instance has already stored, so it does not write them again. Bounded; per instance, not shared.
_Avoid_: Known-series cache, seen set, dedup cache

## Ingestion

**Rejection**:
A datapoint that the store refuses to keep, always counted under exactly one reject reason and reported back to the sender as partial success.
_Avoid_: Drop, invalid point, error

**Reject Reason**:
Why a datapoint was rejected: unsupported metric type, missing metric name, invalid temporality, invalid timestamp, missing value, or series limit exceeded.
_Avoid_: Error code, failure reason

**Series Limit**:
The most series row bytes one Export Request may add that are new to this instance, meaning not in its series cache. A row another instance already stored, or one stored before a restart, still counts as new.
_Avoid_: Series budget, series cap, metadata limit

**Store Failure**:
An Export Request the store could not store at all, which the sender retries in full. It is never partial and never a rejection.
_Avoid_: Error, rejection, permanent failure

**Partial Success**:
The OTLP reply meaning "some datapoints were rejected, everything else was stored".
_Avoid_: Partial failure

**Retention**:
How long stored data is kept, in whole UTC days. Datapoints in a day that retention has already dropped are rejected on arrival rather than stored and immediately expired.
_Avoid_: TTL (that is the storage mechanism), lifetime
