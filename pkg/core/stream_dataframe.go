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
	// Wire output of last stage to input of the new stage
	if len(sdf.Stages) > 0 {
		input_stream = sdf.Stages[len(sdf.Stages)-1].Output
	}
	output_stream := make(chan types.Record)

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

	sdf.Stages[0].Input = sdf.SourceStream
	sdf.Stages[len(sdf.Stages)-1].Output = sdf.OutputStream

	for _, stage := range sdf.Stages {
		go stage.Run(ctx)
	}

	return nil
}
