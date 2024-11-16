package core

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/functions/join"
	"github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
	"github.com/google/uuid"
)

// Make sure StreamDataFrame implements DataFrame
var _ DataFrame = &StreamDataFrame{}

// StreamDataFrame represents an un-bounded DataFrame
type StreamDataFrame struct {
	SourceStream chan (types.Record)
	OutputStream chan (types.Record)
	ErrorStream  chan (error)
	Name         string
	Stages       []Stage
	Schema       types.Schema
	Configs      *Config

	stateStore state_store.StateStore
	rc         RuntimeConfig
	// previousExecutors holds all of the SDFs which current SDF is relying on.
	// Currently only `Join` operation requires this structure so that it can first run
	// all of the previous SDFs before running itself.
	previousExecutors []*StreamDataFrame
}

// NewStreamDataFrame creates a new StreamDataFrame with the given options
func NewStreamDataFrame(
	sourceStream chan (types.Record),
	outputStream chan (types.Record),
	errorStream chan error,
	schema types.Schema,
	streamName string,
	options ...Option,
) StreamDataFrame {
	// Create config with default values
	config := Config{
		LogLevel:   slog.LevelInfo,
		StateStore: state_store.NewInMemorySS(),
	}
	// Functional Option pattern
	for _, option := range options {
		option(&config)
	}

	utils.InitLogger(config.LogLevel)

	if _, ok := config.StateStore.(*state_store.InMemorySS); ok {
		utils.Logger.Warn("Using in-memory state store. This is not suitable for production use.")
	}

	rc := RuntimeConfig{
		StreamCorrelationMap: map[string]string{},
		StreamType:           map[string]join.RecordType{},
	}

	sdf := StreamDataFrame{
		SourceStream: sourceStream,
		OutputStream: outputStream,
		ErrorStream:  errorStream,
		Name:         streamName,
		Stages:       []Stage{},
		Schema:       schema,
		Configs:      &config,

		rc:                rc,
		stateStore:        config.StateStore,
		previousExecutors: []*StreamDataFrame{},
	}

	// Only source streams need to have schema validation. When a SDF
	// is created by joining 2 other streams, it doesn't need any schema validation stage.
	if !strings.HasSuffix(streamName, join.JoinedStreamSuffix) {
		sdf.validateSchema()
	}

	return sdf
}

// Select only selects the given columns from the DataFrame
func (sdf *StreamDataFrame) Select(columns ...string) DataFrame {
	utils.Logger.Info("Adding", "stage", "select", "columns", columns)

	new_schema, err := functions.ReduceSchema(sdf.Schema, columns...)
	if err != nil {
		panic(err)
	}

	new_sdf := StreamDataFrame{
		SourceStream: sdf.SourceStream,
		OutputStream: sdf.OutputStream,
		ErrorStream:  sdf.ErrorStream,
		Stages:       sdf.Stages,
		Schema:       new_schema,
	}
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		result := functions.ApplySelect(data, columns...)
		return []types.Record{result}, nil
	}

	new_sdf.addToStages(executor)
	return &new_sdf
}

// Join joins the DataFrame with another DataFrame based on the given join type and condition
func (sdf *StreamDataFrame) Join(other *StreamDataFrame, how join.JoinType, on join.JoinCondition, mode join.JoinMode) DataFrame {
	// Validate join condition
	err := join.ValidateJoinCondition(sdf.Schema, other.Schema, on)
	if err != nil {
		panic(err)
	}

	// Merge schemas
	new_schema, err := join.MergeSchema(sdf.Schema, other.GetSchema())
	if err != nil {
		panic(err)
	}

	// Fan-In pattern to join 2 streams into 1 stream
	merged_sources := utils.MergeChannels(sdf.OutputStream, other.OutputStream)
	merged_errors := utils.MergeChannels(sdf.ErrorStream, other.ErrorStream)

	out := make(chan (types.Record))
	new_sdf := NewStreamDataFrame(
		merged_sources,
		out,
		merged_errors,
		new_schema,
		sdf.Name+"-"+other.Name+join.JoinedStreamSuffix,
	)
	// TODO: Decide on how to pass configs
	new_sdf.Configs = sdf.Configs
	new_sdf.rc = sdf.rc

	new_sdf.rc.StreamType[sdf.Name] = join.Stream
	new_sdf.rc.StreamType[other.Name] = join.Table
	new_sdf.rc.StreamCorrelationMap[sdf.Name] = other.Name

	executor := func(ctx context.Context, record types.Record) ([]types.Record, error) {
		record_type := new_sdf.rc.StreamType[record.Metadata.Stream]
		return join.InnerJoinStreamTable(new_sdf.stateStore, record_type, record, on, sdf.Name), nil
	}

	new_sdf.previousExecutors = append(new_sdf.previousExecutors, sdf, other)
	new_sdf.addToStages(executor)
	return &new_sdf
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

// AddStaticColumn adds a static column to the DataFrame
func (sdf *StreamDataFrame) AddStaticColumn(name string, value types.ColumnValue) DataFrame {
	utils.Logger.Info("Adding", "stage", "static-column", "name", name)

	new_schema, err := functions.AddColumnToSchema(sdf.Schema, name, value.Type())
	if err != nil {
		panic(err)
	}

	new_sdf := StreamDataFrame{
		SourceStream: sdf.SourceStream,
		OutputStream: sdf.OutputStream,
		ErrorStream:  sdf.ErrorStream,
		Stages:       sdf.Stages,
		Schema:       new_schema,
	}
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		result := functions.AddStaticColumnToRecord(data, name, value)
		return []types.Record{result}, nil
	}

	new_sdf.addToStages(executor)
	return &new_sdf
}

// Rename renames a column in the DataFrame
func (sdf *StreamDataFrame) Rename(old_name string, new_name string) DataFrame {
	utils.Logger.Info("Adding", "stage", "rename", "name", old_name, "new_name", new_name)

	new_schema, err := functions.RenameColumnInSchema(sdf.Schema, old_name, new_name)
	if err != nil {
		panic(err)
	}

	new_sdf := StreamDataFrame{
		SourceStream: sdf.SourceStream,
		OutputStream: sdf.OutputStream,
		ErrorStream:  sdf.ErrorStream,
		Stages:       sdf.Stages,
		Schema:       new_schema,
	}
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		result := functions.RenameColumnInRecord(data, old_name, new_name)
		return []types.Record{result}, nil
	}

	new_sdf.addToStages(executor)
	return &new_sdf
}

// validateSchema validates that each individual record follows the schema of the DataFrame
func (sdf *StreamDataFrame) validateSchema() DataFrame {
	executor := func(ctx context.Context, data types.Record) ([]types.Record, error) {
		err := functions.ValidateSchema(sdf.Schema, data)

		if err != nil {
			sdf.ErrorStream <- err
		}

		// If schema was valid, add name of the stream record belongs to
		data.Metadata.Stream = sdf.Name

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
	utils.Logger.Info("Executing processor", "name", sdf.Name, "len(stages)", len(sdf.Stages))
	if len(sdf.Stages) == 0 {
		return errors.New("no stages are created")
	}

	// Execute previous SDFs which current SDF depends on first (if there are any)
	for _, previous_sdf := range sdf.previousExecutors {
		utils.Logger.Info("Executing previous SDF", "name", previous_sdf.Name)
		go previous_sdf.Execute(ctx)
	}

	for _, stage := range sdf.Stages {
		go stage.Run(ctx)
	}

	for {
		select {
		case err := <-sdf.ErrorStream:
			panic(err)
		case <-ctx.Done():
			utils.Logger.Info("Processor execution completed", "name", sdf.Name)
			return nil // Exit the loop if the context is cancelled
		}
	}
}

// GetSchema returns the schema of the DataFrame
func (sdf *StreamDataFrame) GetSchema() types.Schema {
	return sdf.Schema
}
