package seriescache_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"dash0.com/otlp-metric-store/internal/ingest"
	"dash0.com/otlp-metric-store/internal/seriescache"
)

// fakeStore is an ingest.Store that records the SeriesIds of each InsertSeriesRows
// call and can be told to fail it.
type fakeStore struct {
	seriesCalls [][]uint64
	datapoints  []ingest.Datapoint
	seriesErr   error
}

func (f *fakeStore) InsertSeriesRows(_ context.Context, series []ingest.SeriesRow) error {
	ids := make([]uint64, 0, len(series))
	for _, s := range series {
		ids = append(ids, s.SeriesId)
	}
	f.seriesCalls = append(f.seriesCalls, ids)
	return f.seriesErr
}

func (f *fakeStore) InsertDatapoints(_ context.Context, d []ingest.Datapoint) error {
	f.datapoints = append(f.datapoints, d...)
	return nil
}

func wrapWithMeter(t *testing.T, size int, meter metric.Meter) (*seriescache.Store, *fakeStore) {
	t.Helper()
	next := &fakeStore{}
	s, err := seriescache.Wrap(next, size, meter)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	return s, next
}

func wrap(t *testing.T, size int) (*seriescache.Store, *fakeStore) {
	t.Helper()
	return wrapWithMeter(t, size, noop.NewMeterProvider().Meter("test"))
}

func testDay() time.Time { return time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC) }

func series(ids ...uint64) []ingest.SeriesRow {
	out := make([]ingest.SeriesRow, 0, len(ids))
	for _, id := range ids {
		out = append(out, ingest.SeriesRow{Day: testDay(), SeriesId: id})
	}
	return out
}

// insert calls InsertSeriesRows and returns the SeriesIds it forwarded to next.
func insert(t *testing.T, s *seriescache.Store, next *fakeStore, series []ingest.SeriesRow) []uint64 {
	t.Helper()
	before := len(next.seriesCalls)
	if err := s.InsertSeriesRows(context.Background(), series); err != nil {
		t.Fatalf("InsertSeriesRows: %v", err)
	}
	if len(next.seriesCalls) == before {
		return nil
	}
	return next.seriesCalls[len(next.seriesCalls)-1]
}

func TestForwardsOnlyUnstoredSeries(t *testing.T) {
	s, next := wrap(t, 10)
	if got := insert(t, s, next, series(1, 2)); !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("first insert forwarded %v, want [1 2]", got)
	}
	if got := insert(t, s, next, series(1, 2, 3)); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("second insert forwarded %v, want [3]", got)
	}
	if got := insert(t, s, next, series(1, 2, 3)); got != nil {
		t.Fatalf("third insert forwarded %v, want no call", got)
	}
}

func TestFailedInsertIsNotRemembered(t *testing.T) {
	s, next := wrap(t, 10)
	next.seriesErr = errors.New("boom")
	if err := s.InsertSeriesRows(context.Background(), series(1)); !errors.Is(err, next.seriesErr) {
		t.Fatalf("err = %v, want the wrapped store's error", err)
	}
	next.seriesErr = nil
	if got := insert(t, s, next, series(1)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("retry forwarded %v, want [1]", got)
	}
}

func TestForwardsDatapoints(t *testing.T) {
	s, next := wrap(t, 10)
	dps := []ingest.Datapoint{{SeriesId: 1}, {SeriesId: 2}}
	if err := s.InsertDatapoints(context.Background(), dps); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(next.datapoints, dps) {
		t.Fatalf("forwarded %v, want %v", next.datapoints, dps)
	}
}

func TestKeyIncludesDay(t *testing.T) {
	s, next := wrap(t, 10)
	insert(t, s, next, []ingest.SeriesRow{{Day: testDay(), SeriesId: 1}})
	nextDay := []ingest.SeriesRow{{Day: testDay().AddDate(0, 0, 1), SeriesId: 1}}
	if got := insert(t, s, next, nextDay); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("forwarded %v, want [1] (different Day)", got)
	}
}

func TestDayLocationIndependent(t *testing.T) {
	s, next := wrap(t, 10)
	a := ingest.SeriesRow{Day: testDay(), SeriesId: 1}
	b := ingest.SeriesRow{Day: testDay().In(time.FixedZone("x", 3600)), SeriesId: 1}
	if a.Day == b.Day {
		t.Fatal("test setup: raw Day values must differ to exercise normalization")
	}
	insert(t, s, next, []ingest.SeriesRow{a})
	if got := insert(t, s, next, []ingest.SeriesRow{b}); got != nil {
		t.Fatalf("forwarded %v, want no call (same Day built differently)", got)
	}
}

func TestBounded(t *testing.T) {
	s, next := wrap(t, 2)
	insert(t, s, next, series(1, 2, 3))
	if got := insert(t, s, next, series(1)); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("oldest series should have been evicted, forwarded %v", got)
	}
}

func TestHitPromotesRecency(t *testing.T) {
	s, next := wrap(t, 2)
	insert(t, s, next, series(1, 2))
	insert(t, s, next, series(1)) // hit: 1 becomes most recent
	insert(t, s, next, series(3)) // evicts 2
	if got := insert(t, s, next, series(1)); got != nil {
		t.Fatalf("1 should still be cached, forwarded %v", got)
	}
	if got := insert(t, s, next, series(2)); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("2 should have been evicted, forwarded %v", got)
	}
}

// Stored is true only once a row's insert succeeded, and checking does not
// promote it: 1 is still evicted first.
func TestStored(t *testing.T) {
	s, next := wrap(t, 2)
	key := series(1)[0].Key()
	if s.Stored(key) {
		t.Fatal("Stored before insert")
	}
	next.seriesErr = errors.New("boom")
	_ = s.InsertSeriesRows(context.Background(), series(1))
	if s.Stored(key) {
		t.Fatal("Stored after a failed insert")
	}
	next.seriesErr = nil
	insert(t, s, next, series(1, 2))
	if !s.Stored(key) {
		t.Fatal("not Stored after insert")
	}
	insert(t, s, next, series(3)) // evicts 1, the least recent despite the check
	if s.Stored(key) {
		t.Fatal("Stored promoted recency: 1 was not evicted")
	}
}

func TestRejectsInvalidSize(t *testing.T) {
	if _, err := seriescache.Wrap(&fakeStore{}, 0, noop.NewMeterProvider().Meter("test")); err == nil {
		t.Fatal("expected error for size 0")
	}
}

func TestSelfMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	s, next := wrapWithMeter(t, 1, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	insert(t, s, next, series(1)) // 1 miss, 1 written
	insert(t, s, next, series(1)) // 1 hit
	next.seriesErr = errors.New("boom")
	_ = s.InsertSeriesRows(context.Background(), series(2)) // 1 miss, not written
	next.seriesErr = nil
	insert(t, s, next, series(2)) // 1 miss, 1 written, evicts 1

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				got[m.Name] += dp.Value
			}
		}
	}
	for name, want := range map[string]int64{
		"metricstore.series.cache.hits":      1,
		"metricstore.series.cache.misses":    3,
		"metricstore.series.cache.evictions": 1,
		"metricstore.series.written":         2,
	} {
		if got[name] != want {
			t.Errorf("%s = %d, want %d", name, got[name], want)
		}
	}
}

// countingStore is an ingest.Store, safe for concurrent use, that counts the
// series rows forwarded to it.
type countingStore struct{ rows atomic.Int64 }

func (c *countingStore) InsertSeriesRows(_ context.Context, series []ingest.SeriesRow) error {
	c.rows.Add(int64(len(series)))
	return nil
}

func (c *countingStore) InsertDatapoints(context.Context, []ingest.Datapoint) error { return nil }

// Many concurrent requests share one cache (run with -race). Requests racing
// on a new series may each forward it; once remembered, it is never forwarded
// again.
func TestConcurrentUse(t *testing.T) {
	const workers, requests = 8, 100
	next := &countingStore{}
	s, err := seriescache.Wrap(next, 1000, noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range requests {
				if err := s.InsertSeriesRows(context.Background(), series(1, 2, 3, 4, 5)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	forwarded := next.rows.Load()
	if forwarded < 5 || forwarded > workers*5 {
		t.Errorf("forwarded %d series rows, want between 5 and %d", forwarded, workers*5)
	}
	if err := s.InsertSeriesRows(context.Background(), series(1, 2, 3, 4, 5)); err != nil {
		t.Fatal(err)
	}
	if next.rows.Load() != forwarded {
		t.Errorf("a remembered series was forwarded again")
	}
}
