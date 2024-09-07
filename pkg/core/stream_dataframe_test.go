package core

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/functions/join"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestStreamDataFrame_AddStage_FirstStage(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{},
	}
	sdf := NewStreamDataFrame(input, output, errors, schema, "test-stream")
	executor := func(ctx context.Context, data Record) ([]Record, error) {
		return nil, nil
	}

	sdf.addToStages(executor)

	// Assertions
	// First stage is schema validator, so the first added stage by user will be second stage
	assert.Equal(t, 2, len(sdf.Stages))
	assert.Equal(t, input, sdf.Stages[0].Input)
	assert.Equal(t, sdf.Stages[1].Output, sdf.OutputStream)
	assert.Equal(t, sdf.Stages[0].Error, sdf.ErrorStream)
}

func TestStreamDataFrame_AddStage_ChainStages(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{},
	}
	sdf := NewStreamDataFrame(input, output, errors, schema, "test-stream")
	executor := func(ctx context.Context, data Record) ([]Record, error) {
		return nil, nil
	}

	sdf.addToStages(executor)
	sdf.addToStages(executor)

	// Assertions
	// First stage is schema validator
	assert.Equal(t, 3, len(sdf.Stages))
	assert.Equal(t, input, sdf.Stages[0].Input)
	assert.Equal(t, sdf.Stages[0].Output, sdf.Stages[1].Input)
	assert.Equal(t, sdf.Stages[2].Output, sdf.OutputStream)
	assert.Equal(t, sdf.Stages[0].Error, sdf.ErrorStream)
	assert.Equal(t, sdf.Stages[1].Error, sdf.ErrorStream)
}

func TestStreamDataFrame_Execute_ErrorIfNoStagesDefined(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	sdf := StreamDataFrame{
		SourceStream: input,
		OutputStream: output,
		ErrorStream:  errors,
		Stages:       []Stage{},
	}

	err := sdf.Execute(context.Background())

	assert.Error(t, err)
}

func TestStreamDataFrame_Execute_CancellingContextStopsExecution(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{},
	}
	sdf := NewStreamDataFrame(input, output, errors, schema, "test-stream")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	execution_result_chan := make(chan error)
	go func() {
		res := sdf.Execute(ctx)
		execution_result_chan <- res
	}()

	cancel()

	err := <-execution_result_chan
	assert.Nil(t, err)
}

func TestStreamDataFrame_Join_ShouldAddPreviousStagesAsPreviousExecutors(t *testing.T) {
	// User Data
	user_input := make(chan Record)
	user_output := make(chan Record)
	user_errors := make(chan error)
	user_schema := Schema{
		Columns: Fields{
			"email":      StringType,
			"first_name": StringType,
			"last_name":  StringType,
		},
	}
	user_sdf := NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream")

	// Order Data
	order_input := make(chan Record)
	orders_output := make(chan Record)
	orders_errors := make(chan error)
	orders_schema := Schema{
		Columns: Fields{
			"user_email": StringType,
			"amount":     IntType,
		},
	}
	orders_sdf := NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream")

	// Logic to test
	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*StreamDataFrame)

	assert.Equal(t, 2, len(joined_sdf.previousExecutors))
}
