package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLevelHandlerDropsBelowLevel(t *testing.T) {
	var buf bytes.Buffer
	// The inner handler accepts every level, like the otelslog bridge.
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(levelHandler{Handler: inner, level: slog.LevelInfo})

	logger.Debug("dropped")
	logger.With("k", "v").WithGroup("g").Debug("dropped derived")
	logger.Info("kept")

	out := buf.String()
	if strings.Contains(out, "dropped") {
		t.Errorf("debug record passed the info filter:\n%s", out)
	}
	if !strings.Contains(out, "kept") {
		t.Errorf("info record was dropped:\n%s", out)
	}
}
