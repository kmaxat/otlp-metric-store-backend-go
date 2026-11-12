package config_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"dash0.com/otlp-metric-store/internal/clickhouse"
	"dash0.com/otlp-metric-store/internal/config"
)

// envMap returns a getenv function backed by m.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := config.Load(nil, envMap(nil))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := config.Config{
		ListenAddr:               "localhost:4317",
		MaxReceiveMessageSize:    16777216,
		MaxSeriesBytesPerRequest: 16777216,
		ClickHouse: clickhouse.Config{
			Addr:         "localhost:9000",
			Database:     "default",
			Username:     "default",
			Password:     "",
			MaxOpenConns: 65,
		},
		MaxConcurrentExports: 64,
		ExportTimeout:        10 * time.Second,
		RetentionDays:        30,
		SeriesCacheSize:      100000,
		ShutdownTimeout:      30 * time.Second,
		ShutdownDelay:        5 * time.Second,
		LogLevel:             slog.LevelInfo,
	}
	if cfg != want {
		t.Errorf("defaults:\nwant %+v\ngot  %+v", want, cfg)
	}
}

func TestLoadConfigEnvOverridesDefault(t *testing.T) {
	cfg, err := config.Load(nil, envMap(map[string]string{
		"METRICSTORE_LISTEN_ADDR":                  ":5317",
		"METRICSTORE_MAX_RECEIVE_MESSAGE_SIZE":     "1024",
		"METRICSTORE_MAX_SERIES_BYTES_PER_REQUEST": "4096",
		"CLICKHOUSE_ADDR":                          "ch:9000",
		"CLICKHOUSE_DATABASE":                      "metrics",
		"CLICKHOUSE_USERNAME":                      "writer",
		"CLICKHOUSE_PASSWORD":                      "secret",
		"METRICSTORE_MAX_CONCURRENT_EXPORTS":       "4",
		"METRICSTORE_EXPORT_TIMEOUT":               "3s",
		"METRICSTORE_RETENTION_DAYS":               "7",
		"METRICSTORE_SERIES_CACHE_SIZE":            "500",
		"METRICSTORE_SHUTDOWN_TIMEOUT":             "5s",
		"METRICSTORE_SHUTDOWN_DELAY":               "2s",
		"METRICSTORE_LOG_LEVEL":                    "debug",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := config.Config{
		ListenAddr:               ":5317",
		MaxReceiveMessageSize:    1024,
		MaxSeriesBytesPerRequest: 4096,
		ClickHouse: clickhouse.Config{
			Addr:         "ch:9000",
			Database:     "metrics",
			Username:     "writer",
			Password:     "secret",
			MaxOpenConns: 5,
		},
		MaxConcurrentExports: 4,
		ExportTimeout:        3 * time.Second,
		RetentionDays:        7,
		SeriesCacheSize:      500,
		ShutdownTimeout:      5 * time.Second,
		ShutdownDelay:        2 * time.Second,
		LogLevel:             slog.LevelDebug,
	}
	if cfg != want {
		t.Errorf("env:\nwant %+v\ngot  %+v", want, cfg)
	}
}

func TestLoadConfigFlagOverridesEnv(t *testing.T) {
	cfg, err := config.Load([]string{
		"-listenAddr", ":6317",
		"-maxReceiveMessageSize", "2048",
		"-maxSeriesBytesPerRequest", "8192",
		"-clickhouseAddr", "flag-ch:9000",
		"-clickhouseDatabase", "flagdb",
		"-clickhouseUsername", "flaguser",
		"-maxConcurrentExports", "8",
		"-exportTimeout", "1s",
		"-retentionDays", "90",
		"-seriesCacheSize", "42",
		"-shutdownTimeout", "1m",
		"-shutdownDelay", "0s",
		"-logLevel", "warn",
	}, envMap(map[string]string{
		"METRICSTORE_LISTEN_ADDR":                  ":5317",
		"METRICSTORE_MAX_RECEIVE_MESSAGE_SIZE":     "1024",
		"METRICSTORE_MAX_SERIES_BYTES_PER_REQUEST": "4096",
		"CLICKHOUSE_ADDR":                          "ch:9000",
		"CLICKHOUSE_DATABASE":                      "metrics",
		"CLICKHOUSE_USERNAME":                      "writer",
		"CLICKHOUSE_PASSWORD":                      "secret",
		"METRICSTORE_MAX_CONCURRENT_EXPORTS":       "4",
		"METRICSTORE_EXPORT_TIMEOUT":               "3s",
		"METRICSTORE_RETENTION_DAYS":               "7",
		"METRICSTORE_SERIES_CACHE_SIZE":            "500",
		"METRICSTORE_SHUTDOWN_TIMEOUT":             "5s",
		"METRICSTORE_SHUTDOWN_DELAY":               "2s",
		"METRICSTORE_LOG_LEVEL":                    "debug",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := config.Config{
		ListenAddr:               ":6317",
		MaxReceiveMessageSize:    2048,
		MaxSeriesBytesPerRequest: 8192,
		ClickHouse: clickhouse.Config{
			Addr:         "flag-ch:9000",
			Database:     "flagdb",
			Username:     "flaguser",
			Password:     "secret", // env only, never a flag
			MaxOpenConns: 9,
		},
		MaxConcurrentExports: 8,
		ExportTimeout:        time.Second,
		RetentionDays:        90,
		SeriesCacheSize:      42,
		ShutdownTimeout:      time.Minute,
		ShutdownDelay:        0,
		LogLevel:             slog.LevelWarn,
	}
	if cfg != want {
		t.Errorf("flags:\nwant %+v\ngot  %+v", want, cfg)
	}
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	tests := map[string]struct {
		args    []string
		env     map[string]string
		wantErr string
	}{
		"retention 0":             {args: []string{"-retentionDays", "0"}, wantErr: "retention"},
		"cache size 0":            {args: []string{"-seriesCacheSize", "0"}, wantErr: "cache"},
		"concurrent exports 0":    {args: []string{"-maxConcurrentExports", "0"}, wantErr: "concurrent"},
		"export timeout 0":        {args: []string{"-exportTimeout", "0s"}, wantErr: "export timeout"},
		"max message size 0":      {args: []string{"-maxReceiveMessageSize", "0"}, wantErr: "message"},
		"series bytes 0":          {args: []string{"-maxSeriesBytesPerRequest", "0"}, wantErr: "series bytes"},
		"shutdown timeout 0":      {args: []string{"-shutdownTimeout", "0s"}, wantErr: "shutdown"},
		"retention env 0":         {env: map[string]string{"METRICSTORE_RETENTION_DAYS": "0"}, wantErr: "retention"},
		"bad int env":             {env: map[string]string{"METRICSTORE_RETENTION_DAYS": "abc"}, wantErr: "METRICSTORE_RETENTION_DAYS"},
		"bad duration env":        {env: map[string]string{"METRICSTORE_SHUTDOWN_TIMEOUT": "soon"}, wantErr: "METRICSTORE_SHUTDOWN_TIMEOUT"},
		"negative shutdown delay": {args: []string{"-shutdownDelay", "-1s"}, wantErr: "delay"},
		"bad log level flag":      {args: []string{"-logLevel", "loud"}, wantErr: "logLevel"},
		"bad log level env":       {env: map[string]string{"METRICSTORE_LOG_LEVEL": "loud"}, wantErr: "METRICSTORE_LOG_LEVEL"},
		"unknown flag":            {args: []string{"-nope"}, wantErr: "nope"},
		"password not a flag":     {args: []string{"-clickhousePassword", "x"}, wantErr: "clickhousePassword"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := config.Load(tt.args, envMap(tt.env))
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
				t.Errorf("error %q does not mention %q", err, tt.wantErr)
			}
		})
	}
}
