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

// NewStreamDataFrame creates a new StreamDataFrame with the given options
func NewStreamDataFrame(
	sourceStream chan (types.Record),
	outputStream chan (types.Record),
	errorStream chan error,
	schema types.Schema) StreamDataFrame {
	sdf := StreamDataFrame{
		SourceStream: sourceStream,
		OutputStream: outputStream,
		ErrorStream:  errorStream,
		Stages:       []Stage{},
		Schema:       schema,
	}

	sdf.validateSchema()
	return sdf
}

// Filter applies filter function to each record of the DataFrame
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

// validateSchema validates that each individual record follows the schema of the DataFrame
func (sdf *StreamDataFrame) validateSchema() DataFrame {
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		err := functions.ValidateSchema(sdf.Schema, data)

		if err != nil {
			sdf.ErrorStream <- err
		}

		return []types.Record{data}, nil
	}

	sdf.addToStages(executor)
	return sdf
}

// addToStages adds a new stage to the DataFrame.
// it wires the output of the previous stage to the input of this stage
// and the output of this stage to the output of the DataFrame.
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

// Execute starts the data processing.
// It simply runs all of the stages.
// It's a blocking call and returns when the context is cancelled or panics when an error occurs.
func (sdf *StreamDataFrame) Execute(ctx context.Context) error {
	if len(sdf.Stages) == 0 {
		return errors.New("no stages are created")
	}

	for _, stage := range sdf.Stages {
		go stage.Run(ctx)
	}

	for {
		select {
		case err := <-sdf.ErrorStream:
			panic(err)
		case <-ctx.Done():
			return nil // Exit the loop if the context is cancelled
		}
	}
}
