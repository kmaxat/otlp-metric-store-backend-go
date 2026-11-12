package main

import (
	"context"
	"log/slog"
)

// levelHandler drops records below level before they reach Handler.
type levelHandler struct {
	slog.Handler
	level slog.Leveler
}

// Enabled reports whether l is at least the minimum level and Handler
// accepts it.
func (h levelHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level.Level() && h.Handler.Enabled(ctx, l)
}

// WithAttrs keeps the level filter on the derived handler.
func (h levelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return levelHandler{Handler: h.Handler.WithAttrs(attrs), level: h.level}
}

// WithGroup keeps the level filter on the derived handler.
func (h levelHandler) WithGroup(name string) slog.Handler {
	return levelHandler{Handler: h.Handler.WithGroup(name), level: h.level}
}
