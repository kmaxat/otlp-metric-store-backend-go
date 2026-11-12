// Package clickhouse stores series and datapoints in ClickHouse; *Store
// implements ingest.Store. The tables are created outside the service, by
// deploy/clickhouse/schema.sql.
package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"dash0.com/otlp-metric-store/internal/ingest"
)

var _ ingest.Store = (*Store)(nil)

// Tables the store inserts into, defined in deploy/clickhouse/schema.sql.
const (
	SeriesTable     = "otel_metrics_series"
	DatapointsTable = "otel_metrics_datapoints"
)

// Column lists of the inserts, in the order of the batch.Append arguments
// below. Naming them keeps the inserts independent of the column order in
// schema.sql.
const (
	seriesColumns = "Day, SeriesId, MetricType, MetricName, MetricDescription, MetricUnit, " +
		"AggregationTemporality, IsMonotonic, ServiceName, ResourceAttributes, ResourceSchemaUrl, " +
		"ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, Attributes"
	datapointsColumns = "SeriesId, StartTimeUnix, TimeUnix, Value, Flags"
)

// Config holds the connection settings for the ClickHouse store.
type Config struct {
	Addr     string
	Database string
	Username string
	Password string
	// MaxOpenConns caps the pool. With wait_for_async_insert every insert
	// holds its connection until ClickHouse flushes (up to ~200 ms), so the
	// pool size bounds throughput: about MaxOpenConns / 0.2 s inserts per second.
	MaxOpenConns int
}

// Store writes series and datapoints to ClickHouse.
type Store struct {
	conn           driver.Conn
	insertDuration metric.Float64Histogram
	insertErrors   metric.Int64Counter
}

// New opens and pings a ClickHouse connection configured for async inserts,
// recording insert durations and errors on meter.
func New(ctx context.Context, cfg Config, meter metric.Meter) (*Store, error) {
	insertDuration, err := meter.Float64Histogram("metricstore.insert.duration",
		metric.WithUnit("s"), metric.WithDescription("ClickHouse insert duration, by table"),
		// Second-scale boundaries (the SDK default is for ms). Inserts wait
		// for the async insert flush (50-200 ms).
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1, 2.5, 5, 10))
	if err != nil {
		return nil, fmt.Errorf("creating histogram metricstore.insert.duration: %w", err)
	}
	insertErrors, err := meter.Int64Counter("metricstore.insert.errors",
		metric.WithUnit("{error}"), metric.WithDescription("Failed ClickHouse inserts, by table"))
	if err != nil {
		return nil, fmt.Errorf("creating counter metricstore.insert.errors: %w", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"async_insert":          1,
			"wait_for_async_insert": 1,
			// Server-side cap per insert, in seconds, so a stuck insert fails instead of hanging.
			"max_execution_time": 60,
		},
		// Compresses the blocks sent to ClickHouse; attribute maps compress well.
		Compression:  &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		MaxOpenConns: cfg.MaxOpenConns,
		// Keep every open connection when idle; the driver default (5) would
		// close the rest after each burst and dial them again on the next.
		MaxIdleConns: cfg.MaxOpenConns,
		// Bounds dialing and the wait for a free pool connection: the startup
		// ping fails fast, and a health check never hangs on a dead server.
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &Store{
		conn:           conn,
		insertDuration: insertDuration,
		insertErrors:   insertErrors,
	}, nil
}

// InsertSeriesRows batch-inserts series rows into otel_metrics_series.
func (s *Store) InsertSeriesRows(ctx context.Context, series []ingest.SeriesRow) error {
	if len(series) == 0 {
		return nil
	}
	return s.insert(ctx, SeriesTable, seriesColumns, func(batch driver.Batch) error {
		for _, r := range series {
			if err := batch.Append(
				r.Day.UTC(), // a Date column takes the date in the time's own Location
				r.SeriesId,
				string(r.MetricType),
				r.MetricName,
				r.MetricDescription,
				r.MetricUnit,
				r.AggregationTemporality,
				r.IsMonotonic,
				r.ServiceName,
				r.ResourceAttributes,
				r.ResourceSchemaUrl,
				r.ScopeName,
				r.ScopeVersion,
				r.ScopeAttributes,
				r.ScopeDroppedAttrCount,
				r.ScopeSchemaUrl,
				r.Attributes,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

// InsertDatapoints batch-inserts datapoint rows into otel_metrics_datapoints.
func (s *Store) InsertDatapoints(ctx context.Context, datapoints []ingest.Datapoint) error {
	if len(datapoints) == 0 {
		return nil
	}
	return s.insert(ctx, DatapointsTable, datapointsColumns, func(batch driver.Batch) error {
		for _, d := range datapoints {
			if err := batch.Append(d.SeriesId, d.StartTimeUnix, d.TimeUnix, d.Value, d.Flags); err != nil {
				return err
			}
		}
		return nil
	})
}

// insert sends one batch into the columns of table, filled by appendRows, recording its
// duration in seconds and counting a failure, both attributed to table.
func (s *Store) insert(ctx context.Context, table, columns string, appendRows func(driver.Batch) error) error {
	attrs := metric.WithAttributes(attribute.String("table", table))
	start := time.Now()
	err := s.sendBatch(ctx, table, columns, appendRows)
	s.insertDuration.Record(ctx, time.Since(start).Seconds(), attrs)
	if err != nil {
		s.insertErrors.Add(ctx, 1, attrs)
		return fmt.Errorf("inserting into %s: %w", table, err)
	}
	return nil
}

// sendBatch prepares an INSERT into the columns of table, fills it with
// appendRows and sends it, aborting the batch when any step fails.
func (s *Store) sendBatch(ctx context.Context, table, columns string, appendRows func(driver.Batch) error) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+table+" ("+columns+")")
	if err != nil {
		return err
	}
	defer batch.Abort()
	if err := appendRows(batch); err != nil {
		return err
	}
	return batch.Send()
}

// Ping checks that ClickHouse is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}

// Close closes the underlying ClickHouse connection.
func (s *Store) Close() error {
	return s.conn.Close()
}
