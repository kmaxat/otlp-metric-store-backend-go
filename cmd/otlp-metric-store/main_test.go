package main

import (
	"strings"
	"testing"
	"time"
)

// Startup fails fast, with no retry loop, when ClickHouse is unreachable.
func TestRunFailsFastWithoutClickHouse(t *testing.T) {
	start := time.Now()
	err := run([]string{"-clickhouseAddr", "127.0.0.1:1"}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "connecting to ClickHouse") {
		t.Fatalf("run = %v, want a connecting to ClickHouse error", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("run took %s to fail, want it to fail fast", took)
	}
}
