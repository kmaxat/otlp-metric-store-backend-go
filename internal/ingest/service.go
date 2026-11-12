package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Store persists series rows and datapoints.
type Store interface {
	// InsertSeriesRows stores series rows. The Service passes every series row of
	// a request, including ones stored before, so storing a row again must be
	// harmless (ReplacingMergeTree collapses duplicates); an adapter may skip
	// rows it knows are stored. Within one call the rows are unique by
	// SeriesRowKey, so an adapter need not deduplicate them.
	InsertSeriesRows(ctx context.Context, series []SeriesRow) error
	// InsertDatapoints stores datapoints. The Service calls it only after
	// InsertSeriesRows succeeded, so every SeriesId already has a series row.
	InsertDatapoints(ctx context.Context, datapoints []Datapoint) error
}

// Options are the Service's tunables.
type Options struct {
	// Retention is how long datapoints are kept, in whole UTC days (see Map).
	Retention time.Duration
	// MaxSeriesBytes is the series limit: the most bytes of new series rows one
	// Export Request may create (see MapOptions).
	MaxSeriesBytes int
	// Stored reports whether a series row is already stored, so it does not
	// count against MaxSeriesBytes; nil means no row is known to be stored.
	Stored func(SeriesRowKey) bool
	// Now returns the current time; nil means time.Now.
	Now func() time.Time
}

// Service implements the OTLP metrics service: it maps each request into
// series rows and datapoints and stores them through a Store.
type Service struct {
	colmetricspb.UnimplementedMetricsServiceServer

	store Store
	opts  Options

	datapointsReceived, datapointsStored, datapointsRejected metric.Int64Counter
}

// NewService creates a Service that stores into store and records its own
// metrics on meter.
func NewService(store Store, meter metric.Meter, opts Options) (*Service, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Service{store: store, opts: opts}
	var err error
	counter := func(dst *metric.Int64Counter, name, unit, desc string) {
		if err != nil {
			return
		}
		*dst, err = meter.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
		if err != nil {
			err = fmt.Errorf("creating counter %s: %w", name, err)
		}
	}
	counter(&s.datapointsReceived, "metricstore.datapoints.received", "{datapoint}", "Datapoints received in export requests")
	counter(&s.datapointsStored, "metricstore.datapoints.stored", "{datapoint}", "Datapoints stored")
	counter(&s.datapointsRejected, "metricstore.datapoints.rejected", "{datapoint}", "Datapoints rejected, by reason")
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Export maps the request, stores its series rows and then the datapoints, and
// reports rejected datapoints as OTLP partial success. Store failures are
// returned as codes.Unavailable so senders retry; context errors keep their
// own status code.
func (s *Service) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	mapped := Map(request.GetResourceMetrics(), MapOptions{
		Now:            s.opts.Now(),
		Retention:      s.opts.Retention,
		MaxSeriesBytes: s.opts.MaxSeriesBytes,
		Stored:         s.opts.Stored,
	})

	rejected := mapped.RejectedCount()
	s.datapointsReceived.Add(ctx, mapped.Received())
	for reason, n := range mapped.Rejected {
		s.datapointsRejected.Add(ctx, n, metric.WithAttributes(attribute.String("reason", string(reason))))
	}

	if len(mapped.SeriesRows) > 0 {
		if err := s.store.InsertSeriesRows(ctx, mapped.SeriesRows); err != nil {
			return nil, storeError(ctx, "storing series rows", mapped, err)
		}
	}

	if len(mapped.Datapoints) > 0 {
		if err := s.store.InsertDatapoints(ctx, mapped.Datapoints); err != nil {
			return nil, storeError(ctx, "storing datapoints", mapped, err)
		}
		s.datapointsStored.Add(ctx, int64(len(mapped.Datapoints)))
	}

	slog.DebugContext(ctx, "exported metrics",
		slog.Int64("received", mapped.Received()),
		slog.Int("datapoints", len(mapped.Datapoints)),
		slog.Int64("rejected", rejected),
		slog.Int("seriesRows", len(mapped.SeriesRows)))

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       mapped.RejectionMessage(),
		}
	}
	return resp, nil
}

// storeError logs a failed store call and maps err to a gRPC status.
// The full error is logged but never returned to the sender: senders are
// unauthenticated, and driver errors contain internal addresses and table names.
// A canceled or expired request is logged as a warning, not an error: the
// sender gave up or the Export timeout fired, and ClickHouse may be fine.
func storeError(ctx context.Context, action string, mapped Mapped, err error) error {
	level := slog.LevelError
	if ctx.Err() != nil {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "storing metrics failed",
		slog.String("action", action),
		slog.Int("datapoints", len(mapped.Datapoints)),
		slog.Int64("rejected", mapped.RejectedCount()),
		slog.Int("seriesRows", len(mapped.SeriesRows)),
		slog.Any("error", err))
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	return status.Errorf(codes.Unavailable, "%s failed, retry later", action)
}
