package utils

import (
	"log/slog"
	"os"
	"sync"
)

var (
	once   sync.Once
	Logger *slog.Logger
)

// InitLogger initializes the logger instance with specified configs (EX log level)
func InitLogger(level slog.Level) {
	once.Do(func() {
		lvl := new(slog.LevelVar)
		lvl.Set(level)

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: lvl,
		}))
		Logger = logger
	})
}
