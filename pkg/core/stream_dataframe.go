package core

import (
	"context"
	"errors"

	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/google/uuid"
)

// StreamDataFrame represents an un-bounded DataFrame
type StreamDataFrame struct {
	SourceStream chan (types.Record)
	OutputStream chan (types.Record)
	ErrorStream  chan (error)
	Stages       []Stage
	Schema       types.Schema
}

func (sdf *StreamDataFrame) Filter(filter functions.Filter) DataFrame {
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		// TODO: Decide on passing by reference here
		result := functions.ApplyFilter(filter, &data)
		if result == nil {
			return []types.Record{}, nil
		}
		return []types.Record{*result}, nil
	}
	sdf.addToStages(executor)
	return sdf
}

func (sdf *StreamDataFrame) addToStages(executor StageExecutor) {
	var input_stream chan types.Record

	if len(sdf.Stages) == 0 {
		// Input of the first stage is the input stream of the DataFrame
		input_stream = sdf.SourceStream
	} else {
		// If any stages already exists, create a new channel between
		// the output of the previous stage and input of this stage.
		previous_output := make(chan types.Record)
		sdf.Stages[len(sdf.Stages)-1].Output = previous_output
		input_stream = previous_output
	}

	// Output of the last stage is the output of the DataFrame
	output_stream := sdf.OutputStream

	stage := Stage{
		Id:       uuid.New().String(),
		Input:    input_stream,
		Output:   output_stream,
		Error:    sdf.ErrorStream,
		Executor: executor,
	}
	sdf.Stages = append(sdf.Stages, stage)
}

func (sdf *StreamDataFrame) Execute(ctx context.Context) error {
	if len(sdf.Stages) == 0 {
		return errors.New("no stages are created")
	}

	for _, stage := range sdf.Stages {
		go stage.Run(ctx)
	}

	return nil
}
