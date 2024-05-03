package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestStage_NoopExecutor_WriteInputToOutputWithSameOrder(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)
	noop_executor := func(ctx context.Context, data Record) ([]Record, error) {
		return []Record{data}, nil
	}

	stage := core.Stage{
		Id:       "test_stage",
		Input:    input,
		Output:   output,
		Error:    errors,
		Executor: noop_executor,
	}

	// Sample Data
	Record1 := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "random_name1"},
			"last_name":  String{Val: "random_lastname1"},
		},
	}
	Record2 := Record{
		Key: "key2",
		Data: ValueMap{
			"first_name": String{Val: "random_name2"},
			"last_name":  String{Val: "random_lastname2"},
		},
	}

	Record3 := Record{
		Key: "key3",
		Data: ValueMap{
			"first_name": String{Val: "random_name3"},
			"last_name":  String{Val: "random_lastname3"},
		},
	}
	go func() {
		input <- Record1
		input <- Record2
		input <- Record3
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go stage.Run(ctx)

	// Assertions
	result := <-output
	assert.Equal(t, result, Record1)
	result = <-output
	assert.Equal(t, result, Record2)
	result = <-output
	assert.Equal(t, result, Record3)
	cancel()
	assert.Equal(t, len(output), 0)
}

func TestStage_ExecutorWithError_StageWritesErrorToErrChannel(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors_channel := make(chan error)
	executor_with_error := func(ctx context.Context, data Record) ([]Record, error) {
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
		input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "random_name1"},
				"last_name":  String{Val: "random_lastname1"},
			},
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
