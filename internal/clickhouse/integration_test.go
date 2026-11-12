//go:build integration

package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"dash0.com/otlp-metric-store/internal/clickhouse"
	"dash0.com/otlp-metric-store/internal/grpcserver"
	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/seriescache"
)

// The whole integration suite shares one ClickHouse container (started on
// first use, terminated in TestMain). Tests run sequentially, and
// setupClickHouse empties both tables before each one, so no test reads rows
// an earlier test (or an earlier -count run) wrote. Tests rely on that
// sequential order: state they change on the shared server
// (SYSTEM STOP MERGES) is restored via defer before the next test starts.
var (
	sharedOnce      sync.Once
	sharedContainer testcontainers.Container
	sharedStore     *clickhouse.Store
	sharedConfig    clickhouse.Config
	// sharedConn is the tests' own connection, for reading back what the store wrote.
	sharedConn driver.Conn
	sharedErr  error
)

// TestMain runs the tests, then closes the shared store and terminates the
// shared ClickHouse container if a test started it.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedStore != nil {
		sharedStore.Close()
	}
	if sharedConn != nil {
		sharedConn.Close()
	}
	if sharedContainer != nil {
		if err := sharedContainer.Terminate(context.Background()); err != nil {
			log.Printf("terminating clickhouse container: %v", err)
		}
	}
	os.Exit(code)
}

// schemaFile is the DDL that docker compose also mounts into ClickHouse, so the
// tests run against exactly the deployed tables.
const schemaFile = "../../deploy/clickhouse/schema.sql"

// startClickHouse starts the ClickHouse container with the tables from
// schemaFile and returns a store and its config.
func startClickHouse(ctx context.Context) (*clickhouse.Store, clickhouse.Config, error) {
	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      schemaFile,
			ContainerFilePath: "/docker-entrypoint-initdb.d/schema.sql",
			FileMode:          0o644,
		}),
		// The init script runs on a temporary server first, so wait until
		// the tables exist, not only for the port.
		testcontainers.WithWaitStrategy(wait.ForAll(
			wait.ForListeningPort("9000/tcp"),
			wait.ForExec([]string{"clickhouse-client", "--password", "test", "--query",
				"SELECT 1 FROM " + clickhouse.DatapointsTable + " LIMIT 1"}),
		).WithDeadline(60*time.Second)),
	)
	if ctr != nil {
		sharedContainer = ctr
	}
	if err != nil {
		return nil, clickhouse.Config{}, fmt.Errorf("starting clickhouse container: %w", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return nil, clickhouse.Config{}, fmt.Errorf("getting container host: %w", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return nil, clickhouse.Config{}, fmt.Errorf("getting mapped port: %w", err)
	}
	cfg := clickhouse.Config{
		Addr:         fmt.Sprintf("%s:%s", host, mappedPort.Port()),
		Database:     "default",
		Username:     "default",
		Password:     "test",
		MaxOpenConns: 4,
	}
	// The wait strategy can still pass against the temporary init server, and
	// a host-side connection made while it restarts is reset. Retry until the
	// real server answers.
	var store *clickhouse.Store
	for deadline := time.Now().Add(30 * time.Second); ; {
		store, err = connect(ctx, cfg)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		return nil, clickhouse.Config{}, err
	}
	return store, cfg, nil
}

// connect opens the store and the tests' own connection (sharedConn), pinging
// both. On error it closes whatever it opened.
func connect(ctx context.Context, cfg clickhouse.Config) (*clickhouse.Store, error) {
	store, err := clickhouse.New(ctx, cfg, noop.NewMeterProvider().Meter("test"))
	if err != nil {
		return nil, fmt.Errorf("creating clickhouse metrics store: %w", err)
	}
	conn, err := chgo.Open(&chgo.Options{
		Addr: []string{cfg.Addr},
		Auth: chgo.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
	})
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("opening test connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		store.Close()
		return nil, fmt.Errorf("pinging test connection: %w", err)
	}
	sharedConn = conn
	return store, nil
}

// setupClickHouse returns the shared store and its config, starting
// the container on first use, and empties both tables so the test starts
// from no rows.
func setupClickHouse(t *testing.T) (*clickhouse.Store, clickhouse.Config) {
	t.Helper()
	sharedOnce.Do(func() {
		sharedStore, sharedConfig, sharedErr = startClickHouse(context.Background())
	})
	if sharedErr != nil {
		t.Fatalf("setting up clickhouse: %v", sharedErr)
	}
	for _, table := range []string{clickhouse.SeriesTable, clickhouse.DatapointsTable} {
		if err := sharedConn.Exec(context.Background(), "TRUNCATE TABLE "+table); err != nil {
			t.Fatalf("truncating %s: %v", table, err)
		}
	}
	return sharedStore, sharedConfig
}

// startServer serves the real metrics service (noop meter, a fresh series
// cache) on the production gRPC server over an in-memory bufconn listener,
// and returns a client for it. Each call is an independent "instance" of the
// service.
func startServer(t *testing.T, store ingest.Store) colmetricspb.MetricsServiceClient {
	t.Helper()
	meter := noop.NewMeterProvider().Meter("test")
	cached, err := seriescache.Wrap(store, 1000, meter)
	if err != nil {
		t.Fatalf("creating series cache: %v", err)
	}
	svc, err := ingest.NewService(cached, meter, ingest.Options{
		Retention:      30 * 24 * time.Hour,
		MaxSeriesBytes: 16 << 20,
		Stored:         cached.Stored,
	})
	if err != nil {
		t.Fatalf("creating metrics service: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	server, err := grpcserver.New(grpcserver.Config{
		MaxReceiveMessageSize: 16 << 20,
		MaxConcurrentExports:  8,
		ExportTimeout:         10 * time.Second,
		ShutdownTimeout:       5 * time.Second,
		HealthCheckInterval:   time.Second,
	}, svc, func(context.Context) error { return nil }, meter)
	if err != nil {
		t.Fatalf("creating grpc server: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := server.Serve(ctx, lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-served
	})

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return colmetricspb.NewMetricsServiceClient(conn)
}

// export sends one resource (service.name=checkout, host.name=host) with metrics and fails on any error or rejection.
func export(t *testing.T, client colmetricspb.MetricsServiceClient, host string, metrics ...*metricspb.Metric) {
	t.Helper()
	res := checkoutResource()
	res = append(res, strKV("host.name", host))
	resp, err := client.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: wrap(res, metrics...),
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}
	if ps := resp.GetPartialSuccess(); ps != nil {
		t.Fatalf("unexpected partial success: %d rejected: %s", ps.GetRejectedDataPoints(), ps.GetErrorMessage())
	}
}

// joinedDatapoint is one datapoint joined with its series row.
type joinedDatapoint struct {
	SeriesId uint64
	Attr     string // the requested resource attribute of the series
	TimeUnix time.Time
	Value    float64
}

// seriesQuery runs the README query shape for metric over [from, to]: the
// series side is restricted by Day range and deduplicated with GROUP BY
// SeriesId + any() — deliberately WITHOUT FINAL — and the datapoints side is
// filtered by TimeUnix. Rows are ordered by TimeUnix, then SeriesId.
func seriesQuery(t *testing.T, metric, attrKey string, from, to time.Time) []joinedDatapoint {
	t.Helper()
	const q = `
WITH series AS (
    SELECT SeriesId, any(ResourceAttributes[$2]) AS attr
    FROM otel_metrics_series
    WHERE MetricName = $1
      AND Day BETWEEN toDate(fromUnixTimestamp64Nano(toInt64($3), 'UTC'), 'UTC')
                  AND toDate(fromUnixTimestamp64Nano(toInt64($4), 'UTC'), 'UTC')
    GROUP BY SeriesId
)
SELECT SeriesId, attr, TimeUnix, Value
FROM otel_metrics_datapoints
INNER JOIN series USING SeriesId
WHERE TimeUnix BETWEEN fromUnixTimestamp64Nano(toInt64($3), 'UTC') AND fromUnixTimestamp64Nano(toInt64($4), 'UTC')
  AND SeriesId IN (SELECT SeriesId FROM series)
ORDER BY TimeUnix, SeriesId`
	rows, err := sharedConn.Query(context.Background(), q, metric, attrKey, from.UnixNano(), to.UnixNano())
	if err != nil {
		t.Fatalf("running series query for %s: %v", metric, err)
	}
	defer rows.Close()
	var out []joinedDatapoint
	for rows.Next() {
		var r joinedDatapoint
		if err := rows.Scan(&r.SeriesId, &r.Attr, &r.TimeUnix, &r.Value); err != nil {
			t.Fatalf("scanning series query row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating series query rows: %v", err)
	}
	return out
}

// queryUint64 runs a single-value query returning a UInt64 (e.g. count()).
func queryUint64(t *testing.T, q string, args ...any) uint64 {
	t.Helper()
	var n uint64
	if err := sharedConn.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("running %q: %v", q, err)
	}
	return n
}

// tableCount returns how many tables named table exist in the default database.
func tableCount(t *testing.T, table string) uint64 {
	t.Helper()
	return queryUint64(t,
		"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table)
}

// Proves schema.sql creates the two tables the store inserts into.
func TestSchemaCreatesTables(t *testing.T) {
	setupClickHouse(t)
	for _, table := range []string{clickhouse.SeriesTable, clickhouse.DatapointsTable} {
		if got := tableCount(t, table); got != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, got)
		}
	}
}

// Proves the health check's dependency check (Store.Ping) succeeds against a
// running ClickHouse.
func TestPing(t *testing.T) {
	store, _ := setupClickHouse(t)
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// Proves the store writes rows that read back with the right columns and a
// UTC Day.
func TestInsertSeriesAndDatapoints(t *testing.T) {
	store, _ := setupClickHouse(t)
	ctx := context.Background()

	// Yesterday 23:59:59 UTC: the last second of a Day, relative to now so it
	// stays inside the TTL.
	now := time.Now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	ts := day.Add(24*time.Hour - time.Second)
	wantDay := day.Format(time.DateOnly)
	s := ingest.SeriesRow{
		Day:                    day,
		SeriesId:               12345,
		MetricType:             ingest.MetricTypeSum,
		MetricName:             "http.requests",
		AggregationTemporality: 2,
		IsMonotonic:            true,
		ServiceName:            "checkout",
		ResourceAttributes:     map[string]string{"service.name": "checkout"},
		ScopeName:              "scope",
		ScopeAttributes:        map[string]string{},
		Attributes:             map[string]string{"k": "v"},
	}
	dps := []ingest.Datapoint{
		{SeriesId: 12345, StartTimeUnix: time.Unix(0, 0).UTC(), TimeUnix: ts.Add(-time.Second), Value: 1},
		{SeriesId: 12345, StartTimeUnix: time.Unix(0, 0).UTC(), TimeUnix: ts, Value: 2},
	}

	if err := store.InsertSeriesRows(ctx, []ingest.SeriesRow{s}); err != nil {
		t.Fatalf("inserting series: %v", err)
	}
	if err := store.InsertDatapoints(ctx, dps); err != nil {
		t.Fatalf("inserting datapoints: %v", err)
	}
	if err := store.InsertSeriesRows(ctx, nil); err != nil {
		t.Fatalf("inserting empty series: %v", err)
	}
	if err := store.InsertDatapoints(ctx, nil); err != nil {
		t.Fatalf("inserting empty datapoints: %v", err)
	}

	if count := queryUint64(t,
		"SELECT count() FROM otel_metrics_datapoints WHERE SeriesId = $1", uint64(12345)); count != 2 {
		t.Errorf("datapoint count = %d, want 2", count)
	}

	var (
		metricName string
		attrK      string
		gotDay     time.Time
		dayString  string
	)
	if err := sharedConn.QueryRow(ctx,
		"SELECT MetricName, Attributes['k'], Day, toString(Day) FROM otel_metrics_series WHERE SeriesId = $1", uint64(12345),
	).Scan(&metricName, &attrK, &gotDay, &dayString); err != nil {
		t.Fatalf("querying series: %v", err)
	}
	if metricName != "http.requests" {
		t.Errorf("MetricName = %q", metricName)
	}
	if attrK != "v" {
		t.Errorf("Attributes['k'] = %q", attrK)
	}
	if dayString != wantDay {
		t.Errorf("stored Day = %q, want %s", dayString, wantDay)
	}
	if gotDay.Format(time.DateOnly) != wantDay {
		t.Errorf("scanned Day = %v, want %s", gotDay, wantDay)
	}
}

// Proves the full path gRPC Export -> mapper -> ClickHouse: seriesQuery
// joins each datapoint to its series row (host, value), and the series
// row records the metric type and temporality of a gauge and of a sum.
func TestGRPCToClickHouse(t *testing.T) {
	store, _ := setupClickHouse(t)
	client := startServer(t, store)

	now := time.Now().UTC()
	ts := now.Add(-time.Minute)
	export(t, client, "host-e2e",
		gaugeMetric("e2e.gauge", dpAt(ts, 99.9)),
		sumMetric("e2e.sum", metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, true, dpAt(ts, 42)),
	)

	for _, tc := range []struct {
		metric string
		value  float64
	}{{"e2e.gauge", 99.9}, {"e2e.sum", 42}} {
		rows := seriesQuery(t, tc.metric, "host.name", now.Add(-time.Hour), now)
		if len(rows) != 1 {
			t.Fatalf("%s: rows = %+v, want exactly 1", tc.metric, rows)
		}
		r := rows[0]
		if r.Attr != "host-e2e" || r.Value != tc.value || !r.TimeUnix.Equal(ts) {
			t.Errorf("%s: row = %+v, want host-e2e / %v / %v", tc.metric, r, tc.value, ts)
		}
	}

	for _, tc := range []struct {
		metric      string
		metricType  string
		temporality int32
		monotonic   bool
	}{
		{"e2e.gauge", "gauge", 0, false},
		{"e2e.sum", "sum", int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE), true},
	} {
		var (
			metricType  string
			temporality int32
			monotonic   bool
			service     string
		)
		if err := sharedConn.QueryRow(context.Background(),
			"SELECT MetricType, AggregationTemporality, IsMonotonic, ServiceName FROM otel_metrics_series WHERE MetricName = $1",
			tc.metric,
		).Scan(&metricType, &temporality, &monotonic, &service); err != nil {
			t.Fatalf("querying series of %s: %v", tc.metric, err)
		}
		if metricType != tc.metricType || temporality != tc.temporality || monotonic != tc.monotonic || service != "checkout" {
			t.Errorf("%s: series = (%q, %d, %v, %q), want (%q, %d, %v, \"checkout\")",
				tc.metric, metricType, temporality, monotonic, service, tc.metricType, tc.temporality, tc.monotonic)
		}
	}
}

// Proves the lookup table is correct before ReplacingMergeTree merges: two
// service instances (separate caches) both write the same series row, so the
// table holds 2 raw rows (FINAL collapses them to 1). A naive join would
// double-count every datapoint (4), but seriesQuery (GROUP BY SeriesId)
// returns exactly the 2 datapoints that were sent.
func TestDuplicateSeriesDoesNotDoubleCount(t *testing.T) {
	store, _ := setupClickHouse(t)
	ctx := context.Background()

	// Stop background merges so the duplicate rows deterministically stay
	// unmerged — the state the query must cope with in production.
	if err := sharedConn.Exec(ctx, "SYSTEM STOP MERGES otel_metrics_series"); err != nil {
		t.Fatalf("stopping merges: %v", err)
	}
	defer func() {
		if err := sharedConn.Exec(ctx, "SYSTEM START MERGES otel_metrics_series"); err != nil {
			t.Errorf("starting merges: %v", err)
		}
	}()

	instanceA := startServer(t, store)
	instanceB := startServer(t, store)
	now := time.Now().UTC()
	export(t, instanceA, "host-dup", gaugeMetric("dup.gauge", dpAt(now.Add(-2*time.Minute), 1)))
	export(t, instanceB, "host-dup", gaugeMetric("dup.gauge", dpAt(now.Add(-time.Minute), 2)))

	if raw := queryUint64(t,
		"SELECT count() FROM otel_metrics_series WHERE MetricName = 'dup.gauge'"); raw != 2 {
		t.Errorf("raw series rows = %d, want 2 (one per instance)", raw)
	}
	if final := queryUint64(t,
		"SELECT count() FROM otel_metrics_series FINAL WHERE MetricName = 'dup.gauge'"); final != 1 {
		t.Errorf("series rows with FINAL = %d, want 1", final)
	}
	naive := queryUint64(t, `
SELECT count()
FROM otel_metrics_datapoints AS d
INNER JOIN otel_metrics_series AS s ON d.SeriesId = s.SeriesId
WHERE s.MetricName = 'dup.gauge'`)
	if naive != 4 {
		t.Errorf("naive join count = %d, want 4 (each datapoint matched twice)", naive)
	}

	rows := seriesQuery(t, "dup.gauge", "host.name", now.Add(-time.Hour), now)
	if len(rows) != 2 {
		t.Fatalf("seriesQuery rows = %+v, want exactly 2", rows)
	}
	if rows[0].Value != 1 || rows[1].Value != 2 || rows[0].SeriesId != rows[1].SeriesId {
		t.Errorf("seriesQuery rows = %+v, want values 1 then 2 of one series", rows)
	}
}

// Proves a resource change (host.name a -> b) starts a new series: two
// distinct SeriesIds, each datapoint joined to its own host, and the old
// series row is still in the lookup table.
func TestResourceChangeCreatesNewSeries(t *testing.T) {
	store, _ := setupClickHouse(t)
	client := startServer(t, store)

	now := time.Now().UTC()
	export(t, client, "host-a", gaugeMetric("move.gauge", dpAt(now.Add(-2*time.Minute), 1)))
	export(t, client, "host-b", gaugeMetric("move.gauge", dpAt(now.Add(-time.Minute), 2)))

	rows := seriesQuery(t, "move.gauge", "host.name", now.Add(-time.Hour), now)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want exactly 2", rows)
	}
	if rows[0].Attr != "host-a" || rows[0].Value != 1 || rows[1].Attr != "host-b" || rows[1].Value != 2 {
		t.Errorf("rows = %+v, want host-a/1 then host-b/2", rows)
	}
	if rows[0].SeriesId == rows[1].SeriesId {
		t.Errorf("both hosts share SeriesId %d, want distinct series", rows[0].SeriesId)
	}
	if n := queryUint64(t,
		"SELECT uniqExact(SeriesId) FROM otel_metrics_series WHERE MetricName = 'move.gauge'"); n != 2 {
		t.Errorf("distinct SeriesIds in series table = %d, want 2", n)
	}
}

// Proves per-day series rows make midnight-crossing queries work: one request
// with datapoints just before and just after UTC midnight writes a series row
// for each Day, and seriesQuery over both days finds each datapoint's series
// row.
func TestMidnightCrossingJoins(t *testing.T) {
	store, _ := setupClickHouse(t)
	client := startServer(t, store)

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if now.Sub(today) < 5*time.Second {
		t.Skip("too close to UTC midnight: today's datapoint would be in the future")
	}
	before := today.Add(-500 * time.Millisecond) // yesterday 23:59:59.5
	after := today.Add(500 * time.Millisecond)   // today 00:00:00.5
	export(t, client, "host-midnight", gaugeMetric("midnight.gauge", dpAt(before, 1), dpAt(after, 2)))

	if n := queryUint64(t,
		"SELECT uniqExact(Day) FROM otel_metrics_series WHERE MetricName = 'midnight.gauge'"); n != 2 {
		t.Errorf("series rows span %d days, want 2", n)
	}
	rows := seriesQuery(t, "midnight.gauge", "host.name", before.Add(-time.Minute), now)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want exactly 2", rows)
	}
	for i, want := range []struct {
		ts    time.Time
		value float64
	}{{before, 1}, {after, 2}} {
		r := rows[i]
		if r.Attr != "host-midnight" || r.Value != want.value || !r.TimeUnix.Equal(want.ts) {
			t.Errorf("row %d = %+v, want host-midnight / %v / %v", i, r, want.value, want.ts)
		}
	}
}

// explainSection is one index section ("MinMax", "Partition", "PrimaryKey")
// of EXPLAIN indexes = 1 output.
type explainSection struct {
	Keys                  []string
	Condition             string
	PartsSel, PartsTotal  int
	GranSel, GranTotal    int
	sawParts, sawGranules bool
}

var fractionRe = regexp.MustCompile(`^(Parts|Granules): (\d+)/(\d+)$`)

// parseExplainIndexes extracts the index sections of EXPLAIN indexes = 1 output.
func parseExplainIndexes(t *testing.T, lines []string) map[string]*explainSection {
	t.Helper()
	sections := map[string]*explainSection{}
	var cur *explainSection
	inKeys := false
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case line == "MinMax" || line == "Partition" || line == "PrimaryKey" || line == "Skip":
			cur = &explainSection{}
			sections[line] = cur
			inKeys = false
		case cur == nil:
		case line == "Keys:":
			inKeys = true
		case strings.HasPrefix(line, "Condition:"):
			inKeys = false
			cur.Condition = strings.TrimSpace(strings.TrimPrefix(line, "Condition:"))
		case fractionRe.MatchString(line):
			inKeys = false
			m := fractionRe.FindStringSubmatch(line)
			sel, _ := strconv.Atoi(m[2])
			total, _ := strconv.Atoi(m[3])
			if m[1] == "Parts" {
				cur.PartsSel, cur.PartsTotal, cur.sawParts = sel, total, true
			} else {
				cur.GranSel, cur.GranTotal, cur.sawGranules = sel, total, true
			}
		case inKeys:
			cur.Keys = append(cur.Keys, line)
		}
	}
	return sections
}

// Proves the datapoints schema serves a time-bounded query without scanning
// everything: for a 10-minute window, partition pruning keeps only the parts
// of that window's day, and the time-first primary key
// (toStartOfHour(TimeUnix), SeriesId, ...) keeps only the granules of
// that window inside the day's part. Fixed past days and stopped merges make
// the part and granule counts deterministic.
func TestTimeOnlyQueryPrunes(t *testing.T) {
	store, _ := setupClickHouse(t)
	ctx := context.Background()

	// Stop background merges so the part counts read from system.parts are
	// still the ones EXPLAIN sees.
	if err := sharedConn.Exec(ctx, "SYSTEM STOP MERGES otel_metrics_datapoints"); err != nil {
		t.Fatalf("stopping merges: %v", err)
	}
	defer func() {
		if err := sharedConn.Exec(ctx, "SYSTEM START MERGES otel_metrics_datapoints"); err != nil {
			t.Errorf("starting merges: %v", err)
		}
	}()

	// Two whole past days (well inside the 30-day TTL) that no other test
	// writes to. Rows go straight to the store.
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day := today.AddDate(0, 0, -3)
	prevDay := today.AddDate(0, 0, -4)

	// prevDay: 1,000 datapoints around noon — a second partition to prune.
	var dps []ingest.Datapoint
	for i := range 1000 {
		dps = append(dps, ingest.Datapoint{SeriesId: 777, TimeUnix: prevDay.Add(12*time.Hour + time.Duration(i)*time.Second), Value: float64(i)})
	}
	if err := store.InsertDatapoints(ctx, dps); err != nil {
		t.Fatalf("inserting datapoints of %s: %v", prevDay.Format(time.DateOnly), err)
	}
	// day: 70,000 datapoints evenly across all 24 hours — one part of
	// ceil(70000/8192) = 9 granules.
	const n = 70000
	step := 24 * time.Hour / n
	dps = dps[:0]
	for i := range n {
		dps = append(dps, ingest.Datapoint{SeriesId: 777, TimeUnix: day.Add(time.Duration(i) * step), Value: float64(i)})
	}
	if err := store.InsertDatapoints(ctx, dps); err != nil {
		t.Fatalf("inserting datapoints of %s: %v", day.Format(time.DateOnly), err)
	}

	partsQuery := "SELECT count() FROM system.parts WHERE database = 'default' AND table = 'otel_metrics_datapoints' AND active"
	activeParts := queryUint64(t, partsQuery)
	dayParts := queryUint64(t, partsQuery+" AND partition = $1", day.Format(time.DateOnly))
	if dayParts < 1 || dayParts >= activeParts {
		t.Fatalf("parts: %d of day %s out of %d active, want at least 1 and fewer than all", dayParts, day.Format(time.DateOnly), activeParts)
	}

	// A bounded 10-minute window at noon of day. sum(Value), not count(), so
	// ClickHouse can't answer from its implicit count projection.
	from := day.Add(12 * time.Hour).Format(time.DateTime)
	explain := fmt.Sprintf(`EXPLAIN indexes = 1
SELECT sum(Value) FROM otel_metrics_datapoints
WHERE TimeUnix >= toDateTime64('%[1]s', 9, 'UTC')
  AND TimeUnix < toDateTime64('%[1]s', 9, 'UTC') + INTERVAL 10 MINUTE`, from)
	rows, err := sharedConn.Query(ctx, explain)
	if err != nil {
		t.Fatalf("running EXPLAIN: %v", err)
	}
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scanning EXPLAIN line: %v", err)
		}
		lines = append(lines, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("reading EXPLAIN output: %v", err)
	}
	t.Logf("active parts: %d, of which %d in partition %s\n%s\n%s",
		activeParts, dayParts, day.Format(time.DateOnly), explain, strings.Join(lines, "\n"))

	// ClickHouse 26.2 prints three index sections for this query:
	//   MinMax     — per-part min/max of TimeUnix (the partition-key column): drops whole parts
	//   Partition  — the partition key toDate(TimeUnix): checks the parts MinMax kept
	//   PrimaryKey — the sort key: drops granules inside the remaining parts
	sections := parseExplainIndexes(t, lines)
	minmax, partition, primary := sections["MinMax"], sections["Partition"], sections["PrimaryKey"]
	if minmax == nil || partition == nil || primary == nil {
		t.Fatalf("EXPLAIN is missing an index section, got %v", sections)
	}

	// Partition pruning: of all active parts, exactly the parts of day's
	// partition are kept; every other day (prevDay, today, ...) is never read.
	if !minmax.sawParts || minmax.PartsTotal != int(activeParts) || minmax.PartsSel != int(dayParts) {
		t.Errorf("MinMax parts = %d/%d, want %d/%d", minmax.PartsSel, minmax.PartsTotal, dayParts, activeParts)
	}
	if !partition.sawParts || partition.PartsSel != int(dayParts) || partition.PartsTotal != int(dayParts) {
		t.Errorf("Partition parts = %d/%d, want %d/%d", partition.PartsSel, partition.PartsTotal, dayParts, dayParts)
	}

	// Primary key: ClickHouse derives a toStartOfHour(TimeUnix) range from
	// the plain TimeUnix condition and uses the sort key's first column.
	if !slices.Contains(primary.Keys, "toStartOfHour(TimeUnix)") || !strings.Contains(primary.Condition, "toStartOfHour(TimeUnix)") {
		t.Errorf("PrimaryKey keys = %v, condition = %q, want a toStartOfHour(TimeUnix) condition", primary.Keys, primary.Condition)
	}
	// Granule pruning: the 10-minute window (~486 of 70,000 rows) lies in at
	// most 2 of the day part's 9 granules.
	if !minmax.sawGranules || minmax.GranSel != 9 {
		t.Errorf("granules of the kept part = %d, want 9", minmax.GranSel)
	}
	if !primary.sawGranules || primary.GranTotal != minmax.GranSel || primary.GranSel < 1 || primary.GranSel > 2 {
		t.Errorf("PrimaryKey granules = %d/%d, want 1..2 of %d", primary.GranSel, primary.GranTotal, minmax.GranSel)
	}
}

// Proves Export writes through ClickHouse async inserts: the INSERT into
// otel_metrics_datapoints was counted as an async insert query and its row
// was buffered and flushed by the server's async insert queue.
func TestInsertsAreAsync(t *testing.T) {
	store, _ := setupClickHouse(t)
	client := startServer(t, store)

	ctx := context.Background()
	// Mark the start on the server's own clock, so the log filter below
	// can't be skewed by the host clock.
	var start time.Time
	if err := sharedConn.QueryRow(ctx, "SELECT now64(6, 'UTC')").Scan(&start); err != nil {
		t.Fatalf("reading server time: %v", err)
	}
	export(t, client, "host-async", gaugeMetric("async.gauge", dpAt(time.Now().UTC().Add(-time.Minute), 1)))

	if err := sharedConn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flushing logs: %v", err)
	}

	// Exactly one async insert into the datapoints table since the test
	// started: the one this Export made.
	logRows, err := sharedConn.Query(ctx, `
SELECT query_id, flush_query_id, toString(status), rows
FROM system.asynchronous_insert_log
WHERE database = 'default' AND table = 'otel_metrics_datapoints'
  AND event_time_microseconds >= fromUnixTimestamp64Micro(toInt64($1), 'UTC')`, start.UnixMicro())
	if err != nil {
		t.Fatalf("reading asynchronous_insert_log: %v", err)
	}
	type asyncInsert struct {
		queryID, flushQueryID, status string
		rows                          uint64
	}
	var inserts []asyncInsert
	for logRows.Next() {
		var a asyncInsert
		if err := logRows.Scan(&a.queryID, &a.flushQueryID, &a.status, &a.rows); err != nil {
			t.Fatalf("scanning asynchronous_insert_log: %v", err)
		}
		inserts = append(inserts, a)
	}
	logRows.Close()
	if err := logRows.Err(); err != nil {
		t.Fatalf("iterating asynchronous_insert_log: %v", err)
	}
	if len(inserts) != 1 {
		t.Fatalf("async inserts into otel_metrics_datapoints since start = %+v, want exactly 1", inserts)
	}
	queryID := inserts[0].queryID
	if a := inserts[0]; a.status != "Ok" || a.rows != 1 || a.flushQueryID == "" {
		t.Errorf("async insert = (status %q, rows %d, flush_query_id %q), want (Ok, 1, non-empty)", a.status, a.rows, a.flushQueryID)
	}

	// The client's INSERT query itself took the async insert path. (We can't
	// check Settings['async_insert'] here: query_log only records settings
	// that differ from the server default, and ClickHouse 26.2 already
	// defaults async_insert to 1 — the store still sets it explicitly so the
	// behaviour doesn't depend on the server version.)
	var asyncInsertQuery uint64
	var kind string
	if err := sharedConn.QueryRow(ctx, `
SELECT ProfileEvents['AsyncInsertQuery'], toString(query_kind)
FROM system.query_log
WHERE query_id = $1 AND type = 'QueryFinish'`, queryID).Scan(&asyncInsertQuery, &kind); err != nil {
		t.Fatalf("reading query_log for %s: %v", queryID, err)
	}
	if asyncInsertQuery != 1 || kind != "Insert" {
		t.Errorf("query_log = (AsyncInsertQuery %d, kind %q), want (1, Insert)", asyncInsertQuery, kind)
	}
}

// Proves the store records each insert's duration and counts failed inserts,
// both attributed to the table.
func TestInsertSelfMetrics(t *testing.T) {
	_, cfg := setupClickHouse(t)
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	store, err := clickhouse.New(ctx, cfg, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC()
	if err := store.InsertDatapoints(ctx, []ingest.Datapoint{{SeriesId: 1, TimeUnix: now, StartTimeUnix: now}}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.InsertSeriesRows(canceled, []ingest.SeriesRow{{Day: now, MetricName: "selfmetrics"}}); err == nil {
		t.Fatal("insert with canceled context: want error")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	durations := map[string]uint64{}
	errorsByTable := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "metricstore.insert.duration":
				for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					v, _ := dp.Attributes.Value(attribute.Key("table"))
					durations[v.AsString()] += dp.Count
				}
			case "metricstore.insert.errors":
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					v, _ := dp.Attributes.Value(attribute.Key("table"))
					errorsByTable[v.AsString()] += dp.Value
				}
			}
		}
	}
	if durations[clickhouse.DatapointsTable] != 1 || durations[clickhouse.SeriesTable] != 1 {
		t.Errorf("insert.duration counts by table = %v, want 1 each", durations)
	}
	if errorsByTable[clickhouse.SeriesTable] != 1 || errorsByTable[clickhouse.DatapointsTable] != 0 {
		t.Errorf("insert.errors by table = %v, want 1 for %s only", errorsByTable, clickhouse.SeriesTable)
	}
}

// Proves why every store failure is retryable (README "Write path"): ClickHouse
// merges concurrent async inserts into one flush, and an error at flush time
// fails every insert in it. Here a CHECK constraint violated by one insert
// also fails a valid insert flushed with it, and neither is stored. Were such
// an error permanent, the valid request's sender would drop good data.
func TestAsyncInsertErrorIsShared(t *testing.T) {
	_, cfg := setupClickHouse(t)
	ctx := context.Background()
	const table = "shared_flush"
	if err := sharedConn.Exec(ctx, "CREATE TABLE "+table+
		" (Value Float64, CONSTRAINT small CHECK Value < 100) ENGINE = MergeTree ORDER BY tuple()"); err != nil {
		t.Fatal(err)
	}
	defer sharedConn.Exec(ctx, "DROP TABLE "+table)

	// A fixed 1 s flush window, so both inserts land in the same flush.
	conn, err := chgo.Open(&chgo.Options{
		Addr: []string{cfg.Addr},
		Auth: chgo.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		Settings: chgo.Settings{
			"async_insert":                           1,
			"wait_for_async_insert":                  1,
			"async_insert_use_adaptive_busy_timeout": 0,
			"async_insert_busy_timeout_ms":           1000,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	insert := func(v float64) error {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table+" (Value)")
		if err != nil {
			return err
		}
		if err := batch.Append(v); err != nil {
			return err
		}
		return batch.Send()
	}

	values := []float64{1, 1000} // valid, violating
	errs := make([]error, len(values))
	var wg sync.WaitGroup
	for i, v := range values {
		wg.Go(func() { errs[i] = insert(v) })
	}
	wg.Wait()

	for i, v := range values {
		var ex *chgo.Exception
		if !errors.As(errs[i], &ex) || ex.Code != 469 { // VIOLATED_CONSTRAINT
			t.Errorf("insert of %v: err = %v, want the flush's VIOLATED_CONSTRAINT", v, errs[i])
		}
	}
	if n := queryUint64(t, "SELECT count() FROM "+table); n != 0 {
		t.Errorf("stored %d rows, want 0", n)
	}
}

// OTLP request builders.

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func checkoutResource() []*commonpb.KeyValue {
	return []*commonpb.KeyValue{strKV("service.name", "checkout")}
}

func dpAt(t time.Time, v float64) *metricspb.NumberDataPoint {
	return &metricspb.NumberDataPoint{
		TimeUnixNano: uint64(t.UnixNano()),
		Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: v},
	}
}

func gaugeMetric(name string, dps ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: dps}}}
}

func sumMetric(name string, temp metricspb.AggregationTemporality, mono bool, dps ...*metricspb.NumberDataPoint) *metricspb.Metric {
	return &metricspb.Metric{Name: name, Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
		AggregationTemporality: temp, IsMonotonic: mono, DataPoints: dps}}}
}

func wrap(res []*commonpb.KeyValue, metrics ...*metricspb.Metric) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{{
		Resource:  &resourcepb.Resource{Attributes: res},
		SchemaUrl: "http://res",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{
				Name: "scope", Version: "1.0", Attributes: []*commonpb.KeyValue{strKV("s", "a")},
				DroppedAttributesCount: 3,
			},
			SchemaUrl: "http://scope",
			Metrics:   metrics,
		}},
	}}
}
