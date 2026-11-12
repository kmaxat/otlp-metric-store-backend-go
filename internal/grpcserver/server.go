// Package grpcserver serves OTLP metrics and health checks over gRPC, with
// health following a dependency check and a graceful shutdown.
package grpcserver

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	// Registers the gzip codec: OTLP senders (e.g. the Collector) compress
	// with gzip by default, and the server can only decode registered codecs.
	_ "google.golang.org/grpc/encoding/gzip"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Config holds the Server's tunables.
type Config struct {
	// MaxReceiveMessageSize is the largest request accepted, in bytes.
	MaxReceiveMessageSize int
	// MaxConcurrentExports is how many Export calls run at once; an extra
	// call fails immediately with codes.Unavailable.
	MaxConcurrentExports int
	// ExportTimeout is the longest one Export call may run.
	ExportTimeout time.Duration
	// ShutdownDelay is how long health reports NOT_SERVING while RPCs are
	// still served, before draining starts, so readiness probes can take
	// the instance out of the load balancer first.
	ShutdownDelay time.Duration
	// ShutdownTimeout is how long in-flight RPCs may drain before they are
	// cut off.
	ShutdownTimeout time.Duration
	// HealthCheckInterval is how often the dependency check runs, and also
	// the timeout of each check.
	HealthCheckInterval time.Duration
}

// Server serves an OTLP MetricsService and the gRPC health service. Health
// is reported for the overall server ("") and for the MetricsService.
type Server struct {
	cfg        Config
	grpc       *grpc.Server
	health     *health.Server
	endWatches context.CancelFunc // ends every open health Watch stream
	check      func(context.Context) error
	services   []string
}

// New builds a Server for metrics whose health follows check: SERVING while
// check succeeds, NOT_SERVING while it fails. Export calls turned away by the
// MaxConcurrentExports limit are counted on meter.
func New(cfg Config, metrics colmetricspb.MetricsServiceServer, check func(context.Context) error, meter metric.Meter) (*Server, error) {
	throttled, err := meter.Int64Counter("metricstore.exports.throttled",
		metric.WithUnit("{request}"), metric.WithDescription("Export calls refused because MaxConcurrentExports were running"))
	if err != nil {
		return nil, fmt.Errorf("creating counter metricstore.exports.throttled: %w", err)
	}
	g := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(cfg.MaxReceiveMessageSize),
		grpc.UnaryInterceptor(guardExports(cfg.MaxConcurrentExports, cfg.ExportTimeout, throttled)),
	)
	hs := health.NewServer()
	ending, endWatches := context.WithCancel(context.Background())
	colmetricspb.RegisterMetricsServiceServer(g, metrics)
	healthpb.RegisterHealthServer(g, healthServer{Server: hs, ending: ending})
	return &Server{
		cfg:        cfg,
		grpc:       g,
		health:     hs,
		endWatches: endWatches,
		check:      check,
		services:   []string{"", colmetricspb.MetricsService_ServiceDesc.ServiceName},
	}, nil
}

// healthServer is a health.Server whose Watch streams also end when ending
// is done. GracefulStop waits for every RPC, and a Watch stream otherwise
// runs until the client cancels it, so an open one would hold the drain
// until ShutdownTimeout.
type healthServer struct {
	*health.Server
	ending context.Context
}

func (h healthServer) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	defer context.AfterFunc(h.ending, cancel)()
	err := h.Server.Watch(req, &watchStream{Health_WatchServer: stream, ctx: ctx})
	if h.ending.Err() != nil {
		return status.Error(codes.Unavailable, "server shutting down")
	}
	return err
}

// watchStream is a Watch stream with its context replaced.
type watchStream struct {
	healthpb.Health_WatchServer
	ctx context.Context
}

func (w *watchStream) Context() context.Context { return w.ctx }

// exportMethod is the full gRPC method name of the OTLP metrics Export call.
var exportMethod = "/" + colmetricspb.MetricsService_ServiceDesc.ServiceName + "/Export"

// guardExports returns an interceptor for Export calls. It runs at most limit
// of them at once: an extra call fails immediately with codes.Unavailable,
// which OTLP senders retry with backoff, instead of queueing while it holds
// its decoded request in memory. It ends each call after timeout, whatever
// deadline the sender set, and turns a panic into codes.Internal instead of
// a crash (grpc-go does not recover panics). Health checks pass through.
func guardExports(limit int, timeout time.Duration, throttled metric.Int64Counter) grpc.UnaryServerInterceptor {
	slots := make(chan struct{}, limit) // one token per running Export
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if info.FullMethod != exportMethod {
			return handler(ctx, req)
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			throttled.Add(ctx, 1)
			return nil, status.Error(codes.Unavailable, "server busy, retry later")
		}
		defer func() {
			if p := recover(); p != nil {
				slog.ErrorContext(ctx, "export panicked",
					slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return handler(ctx, req)
	}
}

// Serve serves on lis until ctx is done or serving fails, then shuts down:
// it stops the health checks, reports NOT_SERVING, keeps serving for
// ShutdownDelay (skipped after a serve error), ends open health Watch
// streams, and drains in-flight RPCs, cutting them off after
// ShutdownTimeout. When Serve returns, no RPC is running and check is no
// longer called, so its dependencies can be closed.
//
// It returns nil when ctx ended serving, and the serve error otherwise.
// Serve may be called only once.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	for _, svc := range s.services {
		s.health.SetServingStatus(svc, healthpb.HealthCheckResponse_SERVING)
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	var watching sync.WaitGroup
	watching.Add(1)
	go func() {
		defer watching.Done()
		s.watchHealth(watchCtx)
	}()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.grpc.Serve(lis)
	}()
	slog.Info("gRPC server started", slog.String("listenAddr", lis.Addr().String()))

	var err error
	reason := "context done"
	select {
	case <-ctx.Done():
	case serr := <-serveErr:
		slog.Error("gRPC server stopped unexpectedly", slog.Any("error", serr))
		err = fmt.Errorf("serving gRPC: %w", serr)
		reason = "serve error"
	}

	slog.Info("shutting down", slog.String("reason", reason))
	stopWatch()
	watching.Wait()
	s.health.Shutdown() // sets every service NOT_SERVING
	if err == nil && s.cfg.ShutdownDelay > 0 {
		// Keep serving so probes see NOT_SERVING before new connections are refused.
		time.Sleep(s.cfg.ShutdownDelay)
	}
	s.endWatches() // a Watch stream would otherwise hold the drain open
	s.gracefulStop()
	return err
}

// gracefulStop drains in-flight RPCs with GracefulStop, falling back to a hard
// Stop when draining takes longer than ShutdownTimeout.
func (s *Server) gracefulStop() {
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(s.cfg.ShutdownTimeout):
		slog.Warn("graceful stop timed out, forcing stop", slog.Duration("timeout", s.cfg.ShutdownTimeout))
		s.grpc.Stop()
		<-done
	}
}

// watchHealth runs check every HealthCheckInterval until ctx is done,
// reporting every service as SERVING while it succeeds and NOT_SERVING while
// it fails. It logs only status changes.
func (s *Server) watchHealth(ctx context.Context) {
	serving := true // Serve starts every service as SERVING
	ticker := time.NewTicker(s.cfg.HealthCheckInterval)
	defer ticker.Stop()
	for {
		checkCtx, cancel := context.WithTimeout(ctx, s.cfg.HealthCheckInterval)
		err := s.check(checkCtx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if ok := err == nil; ok != serving {
			serving = ok
			status := healthpb.HealthCheckResponse_SERVING
			if ok {
				slog.Info("dependency check recovered, health SERVING")
			} else {
				status = healthpb.HealthCheckResponse_NOT_SERVING
				slog.Warn("dependency check failed, health NOT_SERVING", slog.Any("error", err))
			}
			for _, svc := range s.services {
				s.health.SetServingStatus(svc, status)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
