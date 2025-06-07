package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/functions/join"
	"github.com/farbodahm/streame/pkg/messaging"
	"github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
	"github.com/google/uuid"
	"google.golang.org/grpc"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

// GetNodeId generates a unique node ID for the current process.
// TODO: Make sure this is unique across all nodes in the cluster
func GetNodeId(nodeIP string) string {
	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", nodeIP, pid)
}

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
	NodeId       string

	stateStore state_store.StateStore
	rc         RuntimeConfig
	// previousExecutors holds all of the SDFs which current SDF is relying on.
	// Currently only `Join` operation requires this structure so that it can first run
	// all of the previous SDFs before running itself.
	previousExecutors []*StreamDataFrame
	etcd              *etcdv3.Client
}

// NewStreamDataFrame creates a new StreamDataFrame with the given options
func NewStreamDataFrame(
	sourceStream chan (types.Record),
	outputStream chan (types.Record),
	errorStream chan error,
	schema types.Schema,
	streamName string,
	etcd *etcdv3.Client,
	options ...Option,
) StreamDataFrame {
	// Create config with default values
	config := Config{
		LogLevel:                       slog.LevelInfo,
		StateStore:                     state_store.NewInMemorySS(),
		LeaderHeartbeatIntervalSeconds: 5,
		LeaderFetchTimeoutSeconds:      2,
		LeaderGRPCPort:                 50085,
	}
	// Functional Option pattern
	for _, option := range options {
		option(&config)
	}

	utils.InitLogger(config.LogLevel)

	if etcd != nil && config.NodeIP == "" {
		panic("Node IP is not set. This is required for distributed mode.")
	}
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
		NodeId:       GetNodeId(config.NodeIP),

		rc:                rc,
		stateStore:        config.StateStore,
		previousExecutors: []*StreamDataFrame{},
		etcd:              etcd,
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
		sdf.etcd,
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

// runStandalone runs the Streame in standalone mode.
// It runs all the stages in parallel and waits for them to finish.
func (sdf *StreamDataFrame) runStandalone(ctx context.Context) error {
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

func (sdf *StreamDataFrame) onStartedLeading(ctx context.Context) {
	utils.Logger.Info("Starting GRPC server for leader", "port", sdf.Configs.LeaderGRPCPort)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", sdf.Configs.LeaderGRPCPort))
	if err != nil {
		utils.Logger.Error("Failed to listen GRPC for leader", "error", err)
		sdf.ErrorStream <- fmt.Errorf("failed to listen GRPC for leader: %w", err)
		return
	}
	s := grpc.NewServer()

	server := messaging.RecordStreamService{
		InputChan: sdf.SourceStream,
	}
	messaging.RegisterRecordStreamServer(s, &server)

	if err := s.Serve(lis); err != nil {
		utils.Logger.Error("Failed to serve GRPC for leader", "error", err)
		sdf.ErrorStream <- fmt.Errorf("failed to serve GRPC for leader: %w", err)
		return
	}
}

func (sdf *StreamDataFrame) onStoppedLeading() {
	utils.Logger.Info("Stopped leading", "node", sdf.NodeId)
	// TODO: Implement logic to handle when this node stops being the leader
}

func (sdf *StreamDataFrame) onNewLeader(newLeaderId string, ctx context.Context) {
	utils.Logger.Info("New leader detected", "node", sdf.NodeId, "newLeader", newLeaderId)
	leaderIP := strings.Split(newLeaderId, "-")[0]
	address := fmt.Sprintf("%s:%d", leaderIP, sdf.Configs.LeaderGRPCPort)
	recChan, errChan := messaging.StreamRecordsFromLeader(ctx, address, sdf.NodeId)

	for {
		select {
		case <-ctx.Done():
			utils.Logger.Info("Context cancelled, stopping record streaming")
			return
		case err := <-errChan:
			if err != nil {
				utils.Logger.Error("Error receiving records from leader", "error", err)
				sdf.ErrorStream <- fmt.Errorf("error receiving records from leader: %w", err)
				return
			}

		case record, ok := <-recChan:
			if !ok {
				utils.Logger.Info("Receiving records from leader channel closed, stopping callback")
				return
			}
			time.Sleep(time.Millisecond * 100) // Simulate processing delay
			utils.Logger.Info("Received record from leader", "record", record)
		}

	}

}

// runDistributed runs the Streame in distributed mode.
func (sdf *StreamDataFrame) runDistributed(ctx context.Context) error {
	leaderElector, err := NewLeaderElector(
		sdf.NodeId,
		sdf.etcd,
		sdf.onStartedLeading,
		sdf.onStoppedLeading,
		sdf.onNewLeader,
		sdf.ErrorStream,
	)
	if err != nil {
		return err
	}

	return leaderElector.Start(ctx)
}

// Execute starts the data processing.
// It's a blocking call and returns when the context is cancelled or panics when an error occurs.
func (sdf *StreamDataFrame) Execute(ctx context.Context) error {
	utils.Logger.Info("Executing processor", "name", sdf.Name, "len(stages)", len(sdf.Stages))
	if len(sdf.Stages) == 0 {
		return errors.New("no stages are created")
	}

	if sdf.etcd == nil {
		utils.Logger.Info("Running in standalone mode")
		return sdf.runStandalone(ctx)
	}

	utils.Logger.Info("Running in distributed mode", "nodeId", sdf.NodeId)
	return sdf.runDistributed(ctx)
}

// GetSchema returns the schema of the DataFrame
func (sdf *StreamDataFrame) GetSchema() types.Schema {
	return sdf.Schema
}
