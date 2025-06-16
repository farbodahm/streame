//go:build integration

package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/functions/join"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStreamDataFrame_AddStage_FirstStage(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{},
	}
	sdf := NewStreamDataFrame(input, output, errors, schema, "test-stream", nil)
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
	sdf := NewStreamDataFrame(input, output, errors, schema, "test-stream", nil)
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
	sdf := NewStreamDataFrame(input, output, errors, schema, "test-stream", nil)

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
	user_sdf := NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

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
	orders_sdf := NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Logic to test
	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*StreamDataFrame)

	assert.Equal(t, 2, len(joined_sdf.previousExecutors))
}

func TestStreamDataFrame_Join_AddingCorrectRuntimeConfig(t *testing.T) {
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
	user_sdf := NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

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
	orders_sdf := NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*StreamDataFrame)

	assert.Equal(t, join.Stream, joined_sdf.rc.StreamType[orders_sdf.Name])
	assert.Equal(t, join.Table, joined_sdf.rc.StreamType[user_sdf.Name])
	assert.Equal(t, user_sdf.Name, joined_sdf.rc.StreamCorrelationMap[orders_sdf.Name])
}
func TestStreamDataFrame_Distributed_StreamingRecordsFromLeaderToWorkers(t *testing.T) {
	const grpcPort = 50505
	schema := Schema{Columns: Fields{"value": IntType}}

	leaderIn := make(chan Record)
	leaderOut := make(chan Record)
	leaderErr := make(chan error)
	leaderSdf := NewStreamDataFrame(leaderIn, leaderOut, leaderErr, schema, "leader", nil,
		WithNodeIP("127.0.0.1"), WithLeaderGRPCPort(grpcPort),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leaderSdf.onStartedLeading(ctx)
	// give leader gRPC server a moment to start
	time.Sleep(100 * time.Millisecond)
	// log any leader errors
	go func() {
		for err := range leaderErr {
			t.Logf("leader error: %v", err)
		}
	}()

	workerOut := make(chan Record)
	workerErr := make(chan error)
	workerSdf := NewStreamDataFrame(nil, workerOut, workerErr, schema, "worker", nil,
		WithNodeIP("127.0.0.1"), WithLeaderGRPCPort(grpcPort),
	)

	// log any worker errors
	go func() {
		for err := range workerErr {
			t.Logf("worker error: %v", err)
		}
	}()
	go workerSdf.onNewLeader(leaderSdf.NodeId, ctx)

	// send records on leader side
	recs := []Record{
		{Key: "foo", Data: ValueMap{"value": Integer{Val: 1}}},
		{Key: "bar", Data: ValueMap{"value": Integer{Val: 2}}},
	}
	for _, r := range recs {
		leaderIn <- r
	}

	// verify worker sees them via gRPC
	for _, expected := range recs {
		select {
		case got := <-workerOut:
			assert.Equal(t, expected.Key, got.Key)
			assert.Equal(t, expected.Data, got.Data)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for worker record %v", expected)
		}
	}
}

func TestStreamDataFrame_DistributedFilter_E2EWithOneWorker(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	defer cli.Close()

	schema := Schema{Columns: Fields{"value": IntType}}
	const count = 4

	// Leader node: user sets up DF + a no-op filter stage (identity)
	leaderIn := make(chan Record)
	leaderOut := make(chan Record)
	leaderErr := make(chan error)
	go func() {
		for err := range leaderErr {
			t.Errorf("leader error: %v", err)
		}
	}()
	leaderSdf := NewStreamDataFrame(leaderIn, leaderOut, leaderErr, schema, "stream", cli,
		WithNodeIP("localhost"),
	)
	leaderDf := (&leaderSdf).Filter(functions.Filter{ColumnName: "value", Operator: functions.NOT_EQUAL, Value: "1"})

	// One worker node: same pipeline, will join leader as non-leader
	wOut := make(chan Record)
	wErr := make(chan error)
	go func() {
		for err := range wErr {
			t.Errorf("worker error: %v", err)
		}
	}()
	wSdf := NewStreamDataFrame(nil, wOut, wErr, schema, "stream", cli,
		WithNodeIP("127.0.0.1"),
	)
	wDf := (&wSdf).Filter(functions.Filter{ColumnName: "value", Operator: functions.NOT_EQUAL, Value: "1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leaderDf.Execute(ctx)
	time.Sleep(2 * time.Second)
	go wDf.Execute(ctx)
	time.Sleep(2 * time.Second)

	// give time for leader election and gRPC subscription
	time.Sleep(2 * time.Second)

	go func() {
		for i := range count {
			leaderIn <- Record{Key: fmt.Sprintf("k%d", i), Data: ValueMap{"value": Integer{Val: i}}}
		}
	}()

	// collect results for the worker
	var result []Record

	for i := range count - 1 {
		select {
		case rec := <-wOut:
			result = append(result, rec)
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for worker record %d", i)
		}
	}

	assert.Len(t, result, count-1, "worker should filter out one record")
	expected := []Record{
		{Key: "k0", Data: ValueMap{"value": Integer{Val: 0}}},
		{Key: "k2", Data: ValueMap{"value": Integer{Val: 2}}},
		{Key: "k3", Data: ValueMap{"value": Integer{Val: 3}}},
	}
	for i, rec := range result {
		assert.Equal(t, expected[i].Key, rec.Key, "record key mismatch at index %d", i)
		assert.Equal(t, expected[i].Data, rec.Data, "record data mismatch at index %d", i)
	}
}

func TestStreamDataFrame_DistributedFilter_E2EWithTwoWorkers(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	defer cli.Close()

	schema := Schema{Columns: Fields{"value": IntType}}
	const count = 6

	// Leader node: user sets up DF + a filter stage to exclude value == 1
	leaderIn := make(chan Record)
	leaderOut := make(chan Record)
	leaderErr := make(chan error)
	go func() {
		for err := range leaderErr {
			t.Errorf("leader error: %v", err)
		}
	}()
	leaderSdf := NewStreamDataFrame(leaderIn, leaderOut, leaderErr, schema, "stream", cli,
		WithNodeIP("127.0.0.1"),
	)
	leaderDf := (&leaderSdf).Filter(functions.Filter{ColumnName: "value", Operator: functions.NOT_EQUAL, Value: "1"})

	// First worker node: same pipeline, will join leader as non-leader
	wOut := make(chan Record)
	wErr := make(chan error)
	go func() {
		for err := range wErr {
			t.Errorf("worker1 error: %v", err)
		}
	}()
	wSdf := NewStreamDataFrame(nil, wOut, wErr, schema, "stream", cli,
		WithNodeIP("127.0.0.2"),
	)
	wDf := (&wSdf).Filter(functions.Filter{ColumnName: "value", Operator: functions.NOT_EQUAL, Value: "1"})

	// Second worker node: same pipeline, will join leader as non-leader
	w2Out := make(chan Record)
	w2Err := make(chan error)
	go func() {
		for err := range w2Err {
			t.Errorf("worker2 error: %v", err)
		}
	}()
	w2Sdf := NewStreamDataFrame(nil, w2Out, w2Err, schema, "stream", cli,
		WithNodeIP("127.0.0.3"),
	)
	w2Df := (&w2Sdf).Filter(functions.Filter{ColumnName: "value", Operator: functions.NOT_EQUAL, Value: "1"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leaderDf.Execute(ctx)
	time.Sleep(2 * time.Second)
	go wDf.Execute(ctx)
	time.Sleep(2 * time.Second)
	go w2Df.Execute(ctx)
	time.Sleep(2 * time.Second)

	// give time for leader election and gRPC subscription
	time.Sleep(2 * time.Second)

	go func() {
		for i := range count {
			leaderIn <- Record{Key: fmt.Sprintf("k%d", i), Data: ValueMap{"value": Integer{Val: i}}}
		}
	}()

	var result1 []Record
	var result2 []Record
	for i := range count - 1 {
		select {
		case rec := <-wOut:
			result1 = append(result1, rec)
		case rec := <-w2Out:
			result2 = append(result2, rec)
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for worker1 record %d", i)
		}
	}

	assert.Equal(t, count-1, len(result1)+len(result2), "both workers should have same number of records")
	assert.NotEmpty(t, result1, "worker1 should have received records")
	assert.NotEmpty(t, result2, "worker2 should have received records")

	resultMerged := append(result1, result2...)
	expected := []Record{
		{Key: "k0", Data: ValueMap{"value": Integer{Val: 0}}},
		{Key: "k2", Data: ValueMap{"value": Integer{Val: 2}}},
		{Key: "k3", Data: ValueMap{"value": Integer{Val: 3}}},
		{Key: "k4", Data: ValueMap{"value": Integer{Val: 4}}},
		{Key: "k5", Data: ValueMap{"value": Integer{Val: 5}}},
	}

	for i, exp := range expected {
		found := false
		for _, rec := range resultMerged {
			if exp.Key == rec.Key {
				assert.Equal(t, exp.Data, rec.Data, "record data mismatch for key %s at expected index %d", exp.Key, i)
				found = true
				break
			}
		}
		assert.True(t, found, "Expected record with key %s not found in result at expected index %d", exp.Key, i)
	}
}
