package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestStage_NoopExecutor_WriteInputToOutputWithSameOrder(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
	errors := make(chan error)
	noop_executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		return []types.Record{data}, nil
	}

	stage := core.Stage{
		Id:       "test_stage",
		Input:    input,
		Output:   output,
		Error:    errors,
		Executor: noop_executor,
	}

	// Sample Data
	record1 := types.Record{
		Key:   "key1",
		Value: map[string]string{"first_name": "random_name1", "last_name": "random_lastname1"},
	}
	record2 := types.Record{
		Key:   "key2",
		Value: map[string]string{"first_name": "random_name2", "last_name": "random_lastname2"},
	}
	record3 := types.Record{
		Key:   "key3",
		Value: map[string]string{"first_name": "random_name3", "last_name": "random_lastname3"},
	}
	go func() {
		input <- record1
		input <- record2
		input <- record3
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go stage.Run(ctx)

	// Assertions
	result := <-output
	assert.Equal(t, result, record1)
	result = <-output
	assert.Equal(t, result, record2)
	result = <-output
	assert.Equal(t, result, record3)
	cancel()
	assert.Equal(t, len(output), 0)
}

func TestStage_ExecutorWithError_StageWritesErrorToErrChannel(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
	errors_channel := make(chan error)
	executor_with_error := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		return nil, errors.New("executor with error")
	}

	stage := core.Stage{
		Id:       "test_stage",
		Input:    input,
		Output:   output,
		Error:    errors_channel,
		Executor: executor_with_error,
	}

	go func() {
		input <- types.Record{
			Key:   "key1",
			Value: map[string]string{"first_name": "random_name1", "last_name": "random_lastname1"},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go stage.Run(ctx)

	// Assertions
	assert.Equal(t, len(output), 0)
	err := <-errors_channel
	assert.Equal(t, err.Error(), "executor with error")
	cancel()
	assert.Equal(t, len(errors_channel), 0)
}
