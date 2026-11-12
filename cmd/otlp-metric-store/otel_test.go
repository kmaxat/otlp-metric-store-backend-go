package main

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRPCServerDurationViewUsesSecondBuckets(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader), metric.WithView(rpcServerDurationView))
	hist, err := provider.Meter("test").Float64Histogram("rpc.server.call.duration")
	if err != nil {
		t.Fatal(err)
	}
	hist.Record(context.Background(), 0.15)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	data := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
	dp := data.DataPoints[0]
	want := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1, 2.5, 5, 10}
	if !slices.Equal(dp.Bounds, want) {
		t.Errorf("bounds = %v, want %v", dp.Bounds, want)
	}
	// 0.15 s falls in (0.1, 0.2], the sixth bucket.
	if dp.BucketCounts[5] != 1 {
		t.Errorf("bucket counts = %v, want the 0.15 s call in (0.1, 0.2]", dp.BucketCounts)
	}
}
