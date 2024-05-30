package core_test

import (
	"log/slog"
	"testing"

	. "github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestStreamDataFrame_ConfigDefaultValues_DefaultValuesAssignedCorrectly(t *testing.T) {
	sdf := NewStreamDataFrame(nil, nil, nil, types.Schema{})

	default_config := Config{
		LogLevel: slog.LevelInfo,
	}

	assert.Equal(t, default_config, *sdf.Configs)
}

func TestStreamDataFrame_ConfigWithLogLevel_LogLevelAssignedCorrectly(t *testing.T) {
	sdf := NewStreamDataFrame(nil, nil, nil, types.Schema{},
		WithLogLevel(slog.LevelError),
	)

	assert.Equal(t, slog.LevelError, sdf.Configs.LogLevel)
}
