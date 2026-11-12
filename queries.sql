-- Example queries against the two tables. The metric "gen" and the
-- host.name "telemetrygen-host" are what the docker-compose telemetrygen
-- service sends.
--
-- Rules every query follows:
--   1. The datapoints side is always bounded by TimeUnix (partition + hour pruning).
--   2. The series side is always restricted by Day (partition pruning) and,
--      when the metric is known, by MetricName (primary key). The Day range
--      must cover the TimeUnix range, which has no upper bound: datapoints may
--      be stamped up to an hour in the future, hence now() + INTERVAL 1 HOUR.
--   3. The series side is deduplicated (GROUP BY SeriesId / any(), or used only
--      inside IN): a series has one row per UTC day, and ReplacingMergeTree
--      duplicates may not be merged yet. Never join the raw series table.
--
-- Run all of them:
--   docker compose exec -T clickhouse clickhouse-client --password metricstore --multiquery --format PrettyCompact < queries.sql
--
-- telemetrygen sends for 5 minutes after `docker compose up`. If that was more
-- than an hour ago, queries 2 and 4 come back empty (1 after two hours, 5
-- after midnight UTC); send more first with
--   docker compose up -d telemetrygen


-- 1. Average of a metric per host over the last 2 hours.
--    Works across midnight (two Day partitions) and before merges.
WITH series AS (
    SELECT SeriesId, any(ResourceAttributes['host.name']) AS host
    FROM otel_metrics_series
    WHERE MetricName = 'gen'
      AND Day BETWEEN toDate(now() - INTERVAL 2 HOUR, 'UTC') AND toDate(now() + INTERVAL 1 HOUR, 'UTC')
    GROUP BY SeriesId
)
SELECT host, count() AS datapoints, avg(Value) AS avg_value
FROM otel_metrics_datapoints
INNER JOIN series USING SeriesId
WHERE TimeUnix >= now() - INTERVAL 2 HOUR
  -- The IN as well as the JOIN: SeriesId is the datapoints' second sort-key
  -- column, so the IN lets the primary index skip granules of other series.
  AND SeriesId IN (SELECT SeriesId FROM series)
GROUP BY host
ORDER BY host;


-- 2. Time-only query: datapoints per minute over the last hour,
--    across all series. No series filter at all.
SELECT toStartOfMinute(TimeUnix) AS minute, count() AS datapoints, sum(Value) AS total
FROM otel_metrics_datapoints
WHERE TimeUnix >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute;


-- 3. Proof of pruning for the time-only query: the MinMax/Partition sections
--    show other days' parts skipped, the PrimaryKey section shows the
--    toStartOfHour(TimeUnix) condition selecting only the matching granules.
--    On a fresh stack all data is within the hour, so every count reads N/N;
--    the skipping shows once there is older data (TestTimeOnlyQueryPrunes
--    selects 1-2 of 9 granules).
EXPLAIN indexes = 1
SELECT sum(Value)
FROM otel_metrics_datapoints
WHERE TimeUnix >= now() - INTERVAL 1 HOUR;


-- 4. Raw datapoints of the series of one host over the last hour.
SELECT SeriesId, TimeUnix, Value
FROM otel_metrics_datapoints
WHERE TimeUnix >= now() - INTERVAL 1 HOUR
  AND SeriesId IN (
      SELECT SeriesId
      FROM otel_metrics_series
      WHERE MetricName = 'gen'
        AND Day BETWEEN toDate(now() - INTERVAL 1 HOUR, 'UTC') AND toDate(now() + INTERVAL 1 HOUR, 'UTC')
        AND ResourceAttributes['host.name'] = 'telemetrygen-host'
  )
ORDER BY SeriesId, TimeUnix DESC
LIMIT 10;


-- 5. Series active today (UTC), one row per series.
SELECT
    MetricName,
    SeriesId,
    any(MetricType)         AS type,
    any(MetricUnit)         AS unit,
    any(ServiceName)        AS service,
    any(ResourceAttributes) AS resource_attributes,
    any(Attributes)         AS attributes
FROM otel_metrics_series
WHERE Day = toDate(now(), 'UTC')
GROUP BY MetricName, SeriesId
ORDER BY MetricName, SeriesId;
