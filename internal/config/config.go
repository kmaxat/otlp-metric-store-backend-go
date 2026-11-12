// Package config builds the metric store's Config from flags and environment
// variables.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	"dash0.com/otlp-metric-store/internal/clickhouse"
)

// Config is the runtime configuration of the metric store.
// Precedence: built-in default < environment variable < command-line flag.
type Config struct {
	ListenAddr               string
	MaxReceiveMessageSize    int
	MaxSeriesBytesPerRequest int
	ClickHouse               clickhouse.Config
	MaxConcurrentExports     int
	ExportTimeout            time.Duration
	RetentionDays            int
	SeriesCacheSize          int
	ShutdownTimeout          time.Duration
	ShutdownDelay            time.Duration
	LogLevel                 slog.Level
}

// Load builds a Config from args (without the program name) and getenv.
// Each flag's default is its environment variable when set, otherwise the
// built-in default, so flags override env which overrides defaults.
// The ClickHouse password is read only from CLICKHOUSE_PASSWORD (never a flag)
// to keep it out of the process list.
func Load(args []string, getenv func(string) string) (Config, error) {
	var cfg Config
	fs := flag.NewFlagSet("otlp-metric-store", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var envErrs []error
	envString := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	envInt := func(key string, def int) int {
		v := getenv(key)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			envErrs = append(envErrs, fmt.Errorf("parsing env %s=%q: %w", key, v, err))
			return def
		}
		return n
	}
	envDuration := func(key string, def time.Duration) time.Duration {
		v := getenv(key)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			envErrs = append(envErrs, fmt.Errorf("parsing env %s=%q: %w", key, v, err))
			return def
		}
		return d
	}
	envLevel := func(key string, def slog.Level) slog.Level {
		v := getenv(key)
		if v == "" {
			return def
		}
		var l slog.Level
		if err := l.UnmarshalText([]byte(v)); err != nil {
			envErrs = append(envErrs, fmt.Errorf("parsing env %s=%q: %w", key, v, err))
			return def
		}
		return l
	}

	fs.StringVar(&cfg.ListenAddr, "listenAddr",
		envString("METRICSTORE_LISTEN_ADDR", "localhost:4317"),
		"The listen address (env METRICSTORE_LISTEN_ADDR)")
	fs.IntVar(&cfg.MaxReceiveMessageSize, "maxReceiveMessageSize",
		envInt("METRICSTORE_MAX_RECEIVE_MESSAGE_SIZE", 16<<20),
		"The max message size in bytes the server can receive (env METRICSTORE_MAX_RECEIVE_MESSAGE_SIZE)")
	fs.IntVar(&cfg.MaxSeriesBytesPerRequest, "maxSeriesBytesPerRequest",
		envInt("METRICSTORE_MAX_SERIES_BYTES_PER_REQUEST", 16<<20),
		"Max bytes of new series rows one Export Request may create; datapoints needing more are rejected (env METRICSTORE_MAX_SERIES_BYTES_PER_REQUEST)")
	fs.StringVar(&cfg.ClickHouse.Addr, "clickhouseAddr",
		envString("CLICKHOUSE_ADDR", "localhost:9000"),
		"ClickHouse native protocol address (env CLICKHOUSE_ADDR)")
	fs.StringVar(&cfg.ClickHouse.Database, "clickhouseDatabase",
		envString("CLICKHOUSE_DATABASE", "default"),
		"ClickHouse database (env CLICKHOUSE_DATABASE)")
	fs.StringVar(&cfg.ClickHouse.Username, "clickhouseUsername",
		envString("CLICKHOUSE_USERNAME", "default"),
		"ClickHouse username (env CLICKHOUSE_USERNAME); the password is env CLICKHOUSE_PASSWORD only")
	fs.IntVar(&cfg.MaxConcurrentExports, "maxConcurrentExports",
		envInt("METRICSTORE_MAX_CONCURRENT_EXPORTS", 64),
		"Max Export calls served at once; more fail fast with Unavailable. Also sizes the ClickHouse connection pool (env METRICSTORE_MAX_CONCURRENT_EXPORTS)")
	fs.DurationVar(&cfg.ExportTimeout, "exportTimeout",
		envDuration("METRICSTORE_EXPORT_TIMEOUT", 10*time.Second),
		"Max duration of one Export call, whatever deadline the sender sets (env METRICSTORE_EXPORT_TIMEOUT)")
	fs.IntVar(&cfg.RetentionDays, "retentionDays",
		envInt("METRICSTORE_RETENTION_DAYS", 30),
		"Reject datapoints older than this many days; keep equal to the table TTL (env METRICSTORE_RETENTION_DAYS)")
	fs.IntVar(&cfg.SeriesCacheSize, "seriesCacheSize",
		envInt("METRICSTORE_SERIES_CACHE_SIZE", 100000),
		"Max entries in the series cache (env METRICSTORE_SERIES_CACHE_SIZE)")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdownTimeout",
		envDuration("METRICSTORE_SHUTDOWN_TIMEOUT", 30*time.Second),
		"Graceful shutdown timeout before forcing stop (env METRICSTORE_SHUTDOWN_TIMEOUT)")
	fs.DurationVar(&cfg.ShutdownDelay, "shutdownDelay",
		envDuration("METRICSTORE_SHUTDOWN_DELAY", 5*time.Second),
		"How long health reports NOT_SERVING before draining starts, so load balancers stop routing here (env METRICSTORE_SHUTDOWN_DELAY)")
	fs.TextVar(&cfg.LogLevel, "logLevel",
		envLevel("METRICSTORE_LOG_LEVEL", slog.LevelInfo),
		"Minimum log level: debug, info, warn or error (env METRICSTORE_LOG_LEVEL)")
	cfg.ClickHouse.Password = getenv("CLICKHOUSE_PASSWORD")

	if err := errors.Join(envErrs...); err != nil {
		return Config{}, err
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stderr)
			fs.PrintDefaults()
		}
		return Config{}, fmt.Errorf("parsing flags: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	// An Export holds at most one connection at a time (series, then
	// datapoints), so one more than the Export limit means an insert never
	// waits for a connection and the health check always finds a free one.
	cfg.ClickHouse.MaxOpenConns = cfg.MaxConcurrentExports + 1
	return cfg, nil
}

// validate checks value ranges that flag parsing cannot express.
func (c Config) validate() error {
	var errs []error
	if c.RetentionDays < 1 {
		errs = append(errs, fmt.Errorf("retention days must be >= 1, got %d", c.RetentionDays))
	}
	if c.SeriesCacheSize < 1 {
		errs = append(errs, fmt.Errorf("series cache size must be >= 1, got %d", c.SeriesCacheSize))
	}
	if c.MaxConcurrentExports < 1 {
		errs = append(errs, fmt.Errorf("max concurrent exports must be >= 1, got %d", c.MaxConcurrentExports))
	}
	if c.ExportTimeout <= 0 {
		errs = append(errs, fmt.Errorf("export timeout must be > 0, got %s", c.ExportTimeout))
	}
	if c.MaxReceiveMessageSize < 1 {
		errs = append(errs, fmt.Errorf("max receive message size must be >= 1, got %d", c.MaxReceiveMessageSize))
	}
	if c.MaxSeriesBytesPerRequest < 1 {
		errs = append(errs, fmt.Errorf("max series bytes per request must be >= 1, got %d", c.MaxSeriesBytesPerRequest))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("shutdown timeout must be > 0, got %s", c.ShutdownTimeout))
	}
	if c.ShutdownDelay < 0 {
		errs = append(errs, fmt.Errorf("shutdown delay must be >= 0, got %s", c.ShutdownDelay))
	}
	return errors.Join(errs...)
}
