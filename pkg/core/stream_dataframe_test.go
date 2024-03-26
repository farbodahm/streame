package core

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestSreamDataFrame_AddStage_FirstStage(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
	errors := make(chan error)

	sdf := StreamDataFrame{
		SourceStream: input,
		OutputStream: output,
		ErrorStream:  errors,
		Stages:       []Stage{},
	}
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		return nil, nil
	}

	sdf.addToStages(executor)

	// Assertions
	assert.Equal(t, 1, len(sdf.Stages))
	assert.Equal(t, input, sdf.Stages[0].Input)
	assert.Equal(t, sdf.Stages[0].Output, sdf.OutputStream)
	assert.Equal(t, sdf.Stages[0].Error, sdf.ErrorStream)
}

func TestSreamDataFrame_AddStage_ChainStages(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
	errors := make(chan error)

	sdf := StreamDataFrame{
		SourceStream: input,
		OutputStream: output,
		ErrorStream:  errors,
		Stages:       []Stage{},
	}
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		return nil, nil
	}

	sdf.addToStages(executor)
	sdf.addToStages(executor)

	// Assertions
	assert.Equal(t, 2, len(sdf.Stages))
	assert.Equal(t, input, sdf.Stages[0].Input)
	assert.Equal(t, sdf.Stages[0].Output, sdf.Stages[1].Input)
	assert.Equal(t, sdf.Stages[1].Output, sdf.OutputStream)
	assert.Equal(t, sdf.Stages[0].Error, sdf.ErrorStream)
	assert.Equal(t, sdf.Stages[1].Error, sdf.ErrorStream)
}

func TestSreamDataFrame_Execute_ErrorIfNoStagesDefined(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
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
