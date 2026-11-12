package main

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// Environment variables that select each signal's exporter (via autoexport):
// e.g. "otlp" to push to an OTLP backend, "prometheus" (metrics only) to
// serve :9464/metrics, "console" to print. Unset means the signal is not
// exported; logs still go to stderr (see run).
const (
	tracesExporterEnv  = "OTEL_TRACES_EXPORTER"
	metricsExporterEnv = "OTEL_METRICS_EXPORTER"
	logsExporterEnv    = "OTEL_LOGS_EXPORTER"
)

// rpcServerDurationView gives otelgrpc's rpc.server.call.duration, recorded
// in seconds, second-scale bucket boundaries instead of the SDK default, which
// is meant for milliseconds and puts every call in the first bucket. Export
// calls wait on the ClickHouse insert, so most take 50-200 ms.
var rpcServerDurationView = metric.NewView(
	metric.Instrument{Name: "rpc.server.call.duration"},
	metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{
		Boundaries: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1, 2.5, 5, 10},
	}},
)

// setupOTelSDK installs global trace, meter and logger providers and returns a
// shutdown func that flushes and closes all of them. A provider whose exporter
// variable is unset in getenv has no exporter, so its telemetry is dropped.
// The only spans are otelgrpc's server spans, one per RPC.
func setupOTelSDK(ctx context.Context, getenv func(string) string) (func(context.Context) error, error) {
	var shutdownFuncs []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}
	// On a setup error, shut down the providers created so far.
	fail := func(err error) (func(context.Context) error, error) {
		return nil, errors.Join(err, shutdown(ctx))
	}

	// The SDK defaults (telemetry.sdk.*), with service.name defaulting to the
	// binary name; OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override it.
	// Schemaless, so it never conflicts with the SDK's semconv schema URL.
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(semconv.ServiceNameKey.String(name)))
	if err == nil {
		res, err = resource.Merge(res, resource.Environment())
	}
	if err != nil {
		return nil, fmt.Errorf("building resource: %w", err)
	}

	// otelgrpc extracts incoming trace context with the global propagator.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	traceOpts := []trace.TracerProviderOption{trace.WithResource(res)}
	if getenv(tracesExporterEnv) != "" {
		exporter, err := autoexport.NewSpanExporter(ctx)
		if err != nil {
			return fail(fmt.Errorf("creating span exporter: %w", err))
		}
		traceOpts = append(traceOpts, trace.WithBatcher(exporter))
	}
	tracerProvider := trace.NewTracerProvider(traceOpts...)
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	meterOpts := []metric.Option{metric.WithResource(res), metric.WithView(rpcServerDurationView)}
	if getenv(metricsExporterEnv) != "" {
		reader, err := autoexport.NewMetricReader(ctx)
		if err != nil {
			return fail(fmt.Errorf("creating metric reader: %w", err))
		}
		meterOpts = append(meterOpts, metric.WithReader(reader))
	}
	meterProvider := metric.NewMeterProvider(meterOpts...)
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	logOpts := []log.LoggerProviderOption{log.WithResource(res)}
	if getenv(logsExporterEnv) != "" {
		exporter, err := autoexport.NewLogExporter(ctx)
		if err != nil {
			return fail(fmt.Errorf("creating log exporter: %w", err))
		}
		logOpts = append(logOpts, log.WithProcessor(log.NewBatchProcessor(exporter)))
	}
	loggerProvider := log.NewLoggerProvider(logOpts...)
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return shutdown, nil
}
