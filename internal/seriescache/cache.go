// Package seriescache provides an ingest.Store that skips series rows it has
// already stored, remembering them in a bounded LRU.
package seriescache

import (
	"context"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/otel/metric"

	"dash0.com/otlp-metric-store/internal/ingest"
)

var _ ingest.Store = (*Store)(nil)

// Store wraps another ingest.Store and forwards only the series rows it has
// not stored before; datapoints are always forwarded. A row counts as stored
// only once the wrapped InsertSeriesRows succeeded, so a failed insert is retried
// on the next request.
//
// It is safe for concurrent use; two concurrent requests may both forward the
// same series row, which is harmless because ReplacingMergeTree collapses
// duplicates.
type Store struct {
	next                  ingest.Store
	lru                   *lru.Cache[ingest.SeriesRowKey, struct{}]
	hits, misses, written metric.Int64Counter
}

// Wrap returns a Store that remembers at most size series rows in front of next,
// recording cache hits, misses, evictions and series rows written on meter.
func Wrap(next ingest.Store, size int, meter metric.Meter) (*Store, error) {
	s := &Store{next: next}
	var evictions metric.Int64Counter
	var err error
	counter := func(dst *metric.Int64Counter, name, desc string) {
		if err != nil {
			return
		}
		*dst, err = meter.Int64Counter(name, metric.WithUnit("{series_row}"), metric.WithDescription(desc))
		if err != nil {
			err = fmt.Errorf("creating counter %s: %w", name, err)
		}
	}
	counter(&s.hits, "metricstore.series.cache.hits", "Series rows found in the series cache")
	counter(&s.misses, "metricstore.series.cache.misses", "Series rows not found in the series cache")
	counter(&evictions, "metricstore.series.cache.evictions", "Series rows evicted from the series cache")
	counter(&s.written, "metricstore.series.written", "Series rows written")
	if err != nil {
		return nil, err
	}
	s.lru, err = lru.NewWithEvict(size, func(ingest.SeriesRowKey, struct{}) {
		evictions.Add(context.Background(), 1)
	})
	if err != nil {
		return nil, fmt.Errorf("creating series cache: %w", err)
	}
	return s, nil
}

// InsertSeriesRows forwards the series rows not stored before and remembers them once
// the wrapped Store has stored them. It relies on ingest.Store's guarantee that
// the keys in series are unique and does not deduplicate.
func (s *Store) InsertSeriesRows(ctx context.Context, series []ingest.SeriesRow) error {
	unseen := s.unseen(ctx, series)
	if len(unseen) == 0 {
		return nil
	}
	if err := s.next.InsertSeriesRows(ctx, unseen); err != nil {
		return err
	}
	for _, r := range unseen {
		s.lru.Add(r.Key(), struct{}{})
	}
	s.written.Add(ctx, int64(len(unseen)))
	return nil
}

// Stored reports whether the series row with key is in the cache, without
// counting a hit or changing its recency.
func (s *Store) Stored(key ingest.SeriesRowKey) bool {
	return s.lru.Contains(key)
}

// InsertDatapoints forwards datapoints to the wrapped Store.
func (s *Store) InsertDatapoints(ctx context.Context, datapoints []ingest.Datapoint) error {
	return s.next.InsertDatapoints(ctx, datapoints)
}

// unseen returns the series rows not in the cache, recording hits and misses.
func (s *Store) unseen(ctx context.Context, series []ingest.SeriesRow) []ingest.SeriesRow {
	var out []ingest.SeriesRow
	var hits int64
	for _, r := range series {
		if _, ok := s.lru.Get(r.Key()); ok { // Get promotes recency
			hits++
			continue
		}
		out = append(out, r)
	}
	s.hits.Add(ctx, hits)
	s.misses.Add(ctx, int64(len(out)))
	return out
}
