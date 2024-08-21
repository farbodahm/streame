package core

import (
	"context"
	"testing"

	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestSreamDataFrame_AddStage_FirstStage(t *testing.T) {
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

func TestSreamDataFrame_AddStage_ChainStages(t *testing.T) {
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

func TestSreamDataFrame_Execute_ErrorIfNoStagesDefined(t *testing.T) {
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

func TestSreamDataFrame_Execute_CancellingContextStopsExecution(t *testing.T) {
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
