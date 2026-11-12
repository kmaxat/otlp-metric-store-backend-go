package grpcserver_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"dash0.com/otlp-metric-store/internal/grpcserver"
)

const maxMessageSize = 1024

var metricsService = colmetricspb.MetricsService_ServiceDesc.ServiceName

// fakeMetrics is a MetricsServiceServer whose Export returns an empty
// response. With release set, Export signals started and then blocks until
// release is closed or the RPC is cut off.
type fakeMetrics struct {
	colmetricspb.UnimplementedMetricsServiceServer
	started chan struct{}
	release chan struct{}
}

func (f fakeMetrics) Export(ctx context.Context, _ *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if f.release != nil {
		f.started <- struct{}{}
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func blockingMetrics(t *testing.T) fakeMetrics {
	t.Helper()
	f := fakeMetrics{started: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-f.release:
		default:
			close(f.release)
		}
	})
	return f
}

func testConfig() grpcserver.Config {
	return grpcserver.Config{
		MaxReceiveMessageSize: maxMessageSize,
		MaxConcurrentExports:  4,
		ExportTimeout:         10 * time.Second,
		ShutdownTimeout:       5 * time.Second,
		HealthCheckInterval:   10 * time.Millisecond,
	}
}

func healthy(context.Context) error { return nil }

// newServer is grpcserver.New with a no-op meter, failing the test on error.
func newServer(t *testing.T, cfg grpcserver.Config, metrics colmetricspb.MetricsServiceServer, check func(context.Context) error) *grpcserver.Server {
	t.Helper()
	return newServerWithMeter(t, cfg, metrics, check, noop.NewMeterProvider().Meter("test"))
}

// newServerWithMeter is grpcserver.New, failing the test on error.
func newServerWithMeter(t *testing.T, cfg grpcserver.Config, metrics colmetricspb.MetricsServiceServer, check func(context.Context) error, meter metric.Meter) *grpcserver.Server {
	t.Helper()
	s, err := grpcserver.New(cfg, metrics, check, meter)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// served is a Server running Serve on an in-memory listener.
type served struct {
	conn *grpc.ClientConn
	stop context.CancelFunc // cancels Serve's context
	done chan struct{}      // closed when Serve has returned
	err  *error             // Serve's result, set before done is closed
}

// wait returns Serve's result once it has returned.
func (s served) wait() error {
	<-s.done
	return *s.err
}

// serve runs newServer(t, cfg, metrics, check).Serve on an in-memory
// listener and returns a client connection to it. Cleanup stops Serve and
// waits for it to return.
func serve(t *testing.T, cfg grpcserver.Config, metrics colmetricspb.MetricsServiceServer, check func(context.Context) error) served {
	t.Helper()
	return serveServer(t, newServer(t, cfg, metrics, check))
}

// serveServer is serve for an already built Server.
func serveServer(t *testing.T, s *grpcserver.Server) served {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = s.Serve(ctx, lis)
		close(done)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Serve did not return")
		}
	})
	return served{conn: conn, stop: cancel, done: done, err: &serveErr}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestServesExportAndHealth(t *testing.T) {
	srv := serve(t, testConfig(), fakeMetrics{}, healthy)
	ctx := testContext(t)
	if _, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(ctx, &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("export: %v", err)
	}
	for _, svc := range []string{"", metricsService} {
		hc, err := healthpb.NewHealthClient(srv.conn).Check(ctx, &healthpb.HealthCheckRequest{Service: svc})
		if err != nil || hc.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Errorf("health check %q = %v, %v; want SERVING", svc, hc.GetStatus(), err)
		}
	}
}

func TestAcceptsGzipCompressedExport(t *testing.T) {
	srv := serve(t, testConfig(), fakeMetrics{}, healthy)
	_, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(testContext(t),
		&colmetricspb.ExportMetricsServiceRequest{}, grpc.UseCompressor(gzip.Name))
	if err != nil {
		t.Fatalf("gzip export: %v", err)
	}
}

func TestRejectsOversizedMessage(t *testing.T) {
	srv := serve(t, testConfig(), fakeMetrics{}, healthy)
	big := &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{
		{SchemaUrl: strings.Repeat("x", 2*maxMessageSize)},
	}}
	_, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(testContext(t), big)
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("code = %v (err %v), want %v", got, err, codes.ResourceExhausted)
	}
}

func TestHealthFollowsCheck(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	srv := serve(t, testConfig(), fakeMetrics{}, func(context.Context) error {
		if failing.Load() {
			return errors.New("clickhouse down")
		}
		return nil
	})
	hc := healthpb.NewHealthClient(srv.conn)

	waitForStatus(t, hc, metricsService, healthpb.HealthCheckResponse_NOT_SERVING)
	waitForStatus(t, hc, "", healthpb.HealthCheckResponse_NOT_SERVING)
	failing.Store(false)
	waitForStatus(t, hc, metricsService, healthpb.HealthCheckResponse_SERVING)
	waitForStatus(t, hc, "", healthpb.HealthCheckResponse_SERVING)
}

// An open Watch stream sees NOT_SERVING when shutdown starts, and is ended
// after ShutdownDelay so it does not hold the drain open until
// ShutdownTimeout.
func TestShutdownReportsNotServing(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownDelay = 100 * time.Millisecond
	srv := serve(t, cfg, fakeMetrics{}, healthy)
	watch, err := healthpb.NewHealthClient(srv.conn).Watch(testContext(t), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := watch.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first status = %v, %v; want SERVING", resp.GetStatus(), err)
	}

	start := time.Now()
	srv.stop()
	if resp, err := watch.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("status after stop = %v, %v; want NOT_SERVING", resp.GetStatus(), err)
	}
	waitForStatus(t, healthpb.NewHealthClient(srv.conn), "", healthpb.HealthCheckResponse_NOT_SERVING)
	if _, err := watch.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("watch after ShutdownDelay = %v, want %v", err, codes.Unavailable)
	}
	if err := srv.wait(); err != nil {
		t.Errorf("Serve = %v, want nil", err)
	}
	if took := time.Since(start); took >= cfg.ShutdownTimeout/2 {
		t.Errorf("shutdown took %v with a Watch open, want well under ShutdownTimeout %v", took, cfg.ShutdownTimeout)
	}
}

// During ShutdownDelay a readiness probe sees NOT_SERVING while new Exports
// are still served, so a load balancer can take the instance out first.
func TestShutdownServesDuringDelay(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownDelay = 2 * time.Second
	srv := serve(t, cfg, fakeMetrics{}, healthy)
	srv.stop()

	waitForStatus(t, healthpb.NewHealthClient(srv.conn), "", healthpb.HealthCheckResponse_NOT_SERVING)
	if _, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("export during shutdown delay: %v", err)
	}
	select {
	case <-srv.done:
		t.Fatal("Serve returned before ShutdownDelay elapsed")
	default:
	}
}

// panickingMetrics is a MetricsServiceServer whose Export panics.
type panickingMetrics struct {
	colmetricspb.UnimplementedMetricsServiceServer
}

func (panickingMetrics) Export(context.Context, *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	panic("boom")
}

func TestRefusesExportsOverLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConcurrentExports = 1
	metrics := blockingMetrics(t)
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	srv := serveServer(t, newServerWithMeter(t, cfg, metrics, healthy, meter))
	client := colmetricspb.NewMetricsServiceClient(srv.conn)
	go func() { _, _ = client.Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{}) }()
	<-metrics.started // the only slot is taken

	_, err := client.Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{})
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("second export code = %v (err %v), want %v", got, err, codes.Unavailable)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if got := throttledCount(rm); got != 1 {
		t.Errorf("metricstore.exports.throttled = %d, want 1", got)
	}
	// Health checks are not limited.
	if _, err := healthpb.NewHealthClient(srv.conn).Check(testContext(t), &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("health check while exports are full: %v", err)
	}
}

// throttledCount returns the metricstore.exports.throttled total in rm.
func throttledCount(rm metricdata.ResourceMetrics) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "metricstore.exports.throttled" {
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

func TestExportTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.ExportTimeout = 50 * time.Millisecond
	metrics := blockingMetrics(t)
	srv := serve(t, cfg, metrics, healthy)
	_, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{})
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("code = %v (err %v), want %v", got, err, codes.DeadlineExceeded)
	}
}

func TestExportPanicBecomesInternal(t *testing.T) {
	srv := serve(t, testConfig(), panickingMetrics{}, healthy)
	client := colmetricspb.NewMetricsServiceClient(srv.conn)
	for range 2 { // the second call proves the server survived and freed the slot
		_, err := client.Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{})
		if got := status.Code(err); got != codes.Internal {
			t.Errorf("code = %v (err %v), want %v", got, err, codes.Internal)
		}
	}
}

func TestShutdownDrainsInFlightExport(t *testing.T) {
	metrics := blockingMetrics(t)
	srv := serve(t, testConfig(), metrics, healthy)
	exportErr := make(chan error, 1)
	go func() {
		_, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{})
		exportErr <- err
	}()
	<-metrics.started

	srv.stop()
	select {
	case <-srv.done:
		t.Fatalf("Serve returned %v with an export still running", srv.wait())
	case <-time.After(50 * time.Millisecond):
	}

	close(metrics.release)
	if err := <-exportErr; err != nil {
		t.Errorf("in-flight export: %v, want success", err)
	}
	if err := srv.wait(); err != nil {
		t.Errorf("Serve = %v, want nil", err)
	}
}

func TestShutdownCutsOffAfterTimeout(t *testing.T) {
	metrics := blockingMetrics(t)
	cfg := testConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond
	srv := serve(t, cfg, metrics, healthy)
	exportErr := make(chan error, 1)
	go func() {
		_, err := colmetricspb.NewMetricsServiceClient(srv.conn).Export(testContext(t), &colmetricspb.ExportMetricsServiceRequest{})
		exportErr <- err
	}()
	<-metrics.started

	srv.stop()
	select {
	case <-srv.done:
		if err := srv.wait(); err != nil {
			t.Errorf("Serve = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after ShutdownTimeout")
	}
	if err := <-exportErr; err == nil {
		t.Error("export succeeded, want it cut off")
	}
}

func TestCheckNotCalledAfterServeReturns(t *testing.T) {
	var returned, calledAfter atomic.Bool
	srv := serve(t, testConfig(), fakeMetrics{}, func(context.Context) error {
		if returned.Load() {
			calledAfter.Store(true)
		}
		return nil
	})
	time.Sleep(30 * time.Millisecond) // let a few checks run

	srv.stop()
	if err := srv.wait(); err != nil {
		t.Fatalf("Serve = %v, want nil", err)
	}
	returned.Store(true)
	time.Sleep(50 * time.Millisecond) // several check intervals
	if calledAfter.Load() {
		t.Error("check called after Serve returned")
	}
}

func TestServeReturnsServeError(t *testing.T) {
	s := newServer(t, testConfig(), fakeMetrics{}, healthy)
	lis := bufconn.Listen(1 << 20)
	_ = lis.Close()
	done := make(chan error, 1)
	go func() { done <- s.Serve(context.Background(), lis) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Serve = nil, want the serve error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the listener failed")
	}
}

// waitForStatus fails the test unless service reaches want within a second.
func waitForStatus(t *testing.T, hc healthpb.HealthClient, service string, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		resp, err := hc.Check(testContext(t), &healthpb.HealthCheckRequest{Service: service})
		if err == nil && resp.GetStatus() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("service %q status = %v (err %v), want %v", service, resp.GetStatus(), err, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
