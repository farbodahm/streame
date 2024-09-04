package core_test

import (
	"log/slog"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	. "github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestStreamDataFrame_ConfigDefaultValues_DefaultValuesAssignedCorrectly(t *testing.T) {
	sdf := NewStreamDataFrame(nil, nil, nil, types.Schema{}, "test-stream")

	assert.Equal(t, sdf.Configs.LogLevel, slog.LevelInfo)
	_, in_memory_ss := sdf.Configs.StateStore.(*state_store.InMemorySS)
	assert.True(t, in_memory_ss)
}

func TestStreamDataFrame_ConfigWithLogLevel_LogLevelAssignedCorrectly(t *testing.T) {
	sdf := NewStreamDataFrame(nil, nil, nil, types.Schema{}, "test-stream",
		WithLogLevel(slog.LevelError),
	)

	assert.Equal(t, slog.LevelError, sdf.Configs.LogLevel)
}

func TestStreamDataFrame_WithStateStore_StateStoreAssignedCorrectly(t *testing.T) {
	warehouse_path := "./test-path"
	defer os.RemoveAll(warehouse_path)
	ss, _ := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	sdf := NewStreamDataFrame(nil, nil, nil, types.Schema{}, "test-stream",
		WithStateStore(ss),
	)

	assert.Equal(t, ss, sdf.Configs.StateStore)
}
