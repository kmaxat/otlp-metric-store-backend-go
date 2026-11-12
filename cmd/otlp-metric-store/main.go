package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"

	"dash0.com/otlp-metric-store/internal/clickhouse"
	"dash0.com/otlp-metric-store/internal/config"
	"dash0.com/otlp-metric-store/internal/grpcserver"
	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/seriescache"
)

// healthCheckInterval is how often health re-checks ClickHouse.
const healthCheckInterval = 5 * time.Second

// name is the default service.name and the instrumentation scope of the
// service's own meter and logger.
const name = "otlp-metric-store"

var meter = otel.Meter(name)

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		// Plain stderr: the error may come from before logging was set up
		// (e.g. invalid config).
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run loads config, connects to ClickHouse, and serves OTLP metrics over
// gRPC until SIGINT/SIGTERM or a serve error. A second signal during the
// shutdown kills the process. After the server has shut down
// (see grpcserver.Server.Serve) it closes the store, then flushes the OTel SDK.
func run(args []string, getenv func(string) string) (err error) {
	cfg, err := config.Load(args, getenv)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	otelShutdown, err := setupOTelSDK(context.Background(), getenv)
	if err != nil {
		return fmt.Errorf("setting up OpenTelemetry SDK: %w", err)
	}
	// Flush telemetry last so every log line above is exported.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	// Logs always go to stderr as JSON lines. With an OTel logs exporter
	// configured they are also sent through the SDK; the otelslog bridge
	// passes every level on, so levelHandler filters for it.
	var logHandler slog.Handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})
	if getenv(logsExporterEnv) != "" {
		logHandler = slog.NewMultiHandler(logHandler,
			levelHandler{Handler: otelslog.NewHandler(name), level: cfg.LogLevel})
	}
	slog.SetDefault(slog.New(logHandler))
	slog.Info("starting application",
		slog.String("logLevel", cfg.LogLevel.String()),
		slog.String("listenAddr", cfg.ListenAddr),
		slog.String("clickhouseAddr", cfg.ClickHouse.Addr),
		slog.String("clickhouseDatabase", cfg.ClickHouse.Database),
		slog.Int("retentionDays", cfg.RetentionDays),
		slog.Int("seriesCacheSize", cfg.SeriesCacheSize),
		slog.Int("maxSeriesBytesPerRequest", cfg.MaxSeriesBytesPerRequest),
		slog.Int("maxConcurrentExports", cfg.MaxConcurrentExports))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Once the first signal has started the shutdown, restore the default
	// handling, so a second signal kills the process instead of being ignored.
	context.AfterFunc(ctx, stop)

	store, err := clickhouse.New(ctx, cfg.ClickHouse, meter)
	if err != nil {
		slog.Error("connecting to ClickHouse",
			slog.String("clickhouseAddr", cfg.ClickHouse.Addr), slog.Any("error", err))
		return fmt.Errorf("connecting to ClickHouse: %w", err)
	}
	// Runs after server.Serve has returned and before the telemetry flush.
	defer func() {
		if cerr := store.Close(); cerr != nil {
			slog.Error("closing ClickHouse store", slog.Any("error", cerr))
			err = errors.Join(err, fmt.Errorf("closing ClickHouse store: %w", cerr))
		}
	}()

	cachedStore, err := seriescache.Wrap(store, cfg.SeriesCacheSize, meter)
	if err != nil {
		return fmt.Errorf("creating series cache: %w", err)
	}
	svc, err := ingest.NewService(cachedStore, meter, ingest.Options{
		Retention:      time.Duration(cfg.RetentionDays) * 24 * time.Hour,
		MaxSeriesBytes: cfg.MaxSeriesBytesPerRequest,
		Stored:         cachedStore.Stored,
	})
	if err != nil {
		return fmt.Errorf("creating metrics service: %w", err)
	}

	server, err := grpcserver.New(grpcserver.Config{
		MaxReceiveMessageSize: cfg.MaxReceiveMessageSize,
		MaxConcurrentExports:  cfg.MaxConcurrentExports,
		ExportTimeout:         cfg.ExportTimeout,
		ShutdownDelay:         cfg.ShutdownDelay,
		ShutdownTimeout:       cfg.ShutdownTimeout,
		HealthCheckInterval:   healthCheckInterval,
	}, svc, store.Ping, meter)
	if err != nil {
		return fmt.Errorf("creating gRPC server: %w", err)
	}

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.ListenAddr, err)
	}
	// Serve returns once RPCs have drained and health stopped pinging, so the
	// deferred store close runs after both.
	return server.Serve(ctx, listener)
}
