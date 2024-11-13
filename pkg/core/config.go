package core

import (
	"log/slog"

	"github.com/farbodahm/streame/pkg/functions/join"
	"github.com/farbodahm/streame/pkg/state_store"
)

// Option implements the Functional Option pattern for StreamDataFrame
type Option func(*Config)

// Config is the configuration options for StreamDataFrame
type Config struct {
	LogLevel   slog.Level
	StateStore state_store.StateStore
}

// WithLogLevel sets the log level for StreamDataFrame
func WithLogLevel(level slog.Level) Option {
	return func(c *Config) {
		c.LogLevel = level
	}
}

// WithStateStore sets the state store for StreamDataFrame.
// If not set, the default in-memory state store will be used
// which is not recommended for production use
func WithStateStore(ss state_store.StateStore) Option {
	return func(c *Config) {
		c.StateStore = ss
	}
}

// RuntimeConfig is used internally by the SDF to store configurations
// that are generated on run time.
type RuntimeConfig struct {
	StreamCorrelationMap map[string]string          // map each stream name to its correlated stream in join
	StreamType           map[string]join.RecordType // map each stream name to its type
}
