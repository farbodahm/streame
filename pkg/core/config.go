package core

import "log/slog"

// Option implements the Functional Option pattern for StreamDataFrame
type Option func(*Config)

// Config is the configuration options for StreamDataFrame
type Config struct {
	LogLevel slog.Level
}

// WithLogLevel sets the log level for StreamDataFrame
func WithLogLevel(level slog.Level) Option {
	return func(c *Config) {
		c.LogLevel = level
	}
}
