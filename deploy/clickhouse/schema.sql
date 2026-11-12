-- Tables for the OTLP metric store. ClickHouse runs this file on first start
-- (mounted into /docker-entrypoint-initdb.d); the metric store only inserts.
-- Retention is the TTL below (30 days); keep METRICSTORE_RETENTION_DAYS equal
-- to it so the service rejects datapoints the TTL would drop anyway.

-- One row per series per day: the identity (name, attributes) of a series,
-- joined to datapoints by SeriesId.
CREATE TABLE IF NOT EXISTS otel_metrics_series (
    Day                    Date,
    SeriesId               UInt64,
    MetricType             LowCardinality(String),      -- 'gauge' | 'sum'
    MetricName             LowCardinality(String),
    MetricDescription      String CODEC(ZSTD(1)),
    MetricUnit             LowCardinality(String),
    AggregationTemporality Int32,                       -- 0 = not applicable (gauge)
    IsMonotonic            Bool,                        -- false for gauge
    ServiceName            LowCardinality(String),
    ResourceAttributes     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl      String CODEC(ZSTD(1)),
    ScopeName              String CODEC(ZSTD(1)),
    ScopeVersion           String CODEC(ZSTD(1)),
    ScopeAttributes        Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount  UInt32,
    ScopeSchemaUrl         String CODEC(ZSTD(1)),
    Attributes             Map(LowCardinality(String), String) CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
PARTITION BY Day
ORDER BY (MetricName, SeriesId)
TTL Day + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1;

-- One row per datapoint.
--
-- The last sort-key column is toUnixTimestamp64Nano(TimeUnix), not TimeUnix:
-- the order is identical, but if TimeUnix itself were a key column,
-- ClickHouse (verified on 26.2) would match a "TimeUnix >= x" condition to
-- that last column only and never derive the toStartOfHour(TimeUnix) bound,
-- so time-only queries would read every granule of the day.
CREATE TABLE IF NOT EXISTS otel_metrics_datapoints (
    SeriesId      UInt64,
    StartTimeUnix DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    Value         Float64              CODEC(Gorilla, ZSTD(1)),
    Flags         UInt32               CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(TimeUnix, 'UTC')
ORDER BY (toStartOfHour(TimeUnix), SeriesId, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix, 'UTC') + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1;
