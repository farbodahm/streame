package join_test

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/functions/join"
	"github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/types"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestInnerJoinStreamTable_TableRecord_StoresInStateStore(t *testing.T) {
	ss := state_store.NewInMemorySS()
	condition := join.JoinCondition{
		LeftKey:  "user_email",
		RightKey: "email",
	}

	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
			"email":      String{Val: "test@test.com"},
		},
	}

	// Test storing the Table record
	result := join.InnerJoinStreamTable(ss, join.Table, record, condition, "not_applicable")
	assert.Empty(t, result)

	// Verify that the record was stored in the state store
	storedRecord, err := ss.Get("test@test.com")
	assert.Nil(t, err)
	assert.Equal(t, record, storedRecord)

	err = ss.Close()
	assert.Nil(t, err)
}

func TestInnerJoinStreamTable_StreamRecordWithNoMatch_ReturnsEmpty(t *testing.T) {
	ss := state_store.NewInMemorySS()
	condition := join.JoinCondition{
		LeftKey:  "user_email",
		RightKey: "email",
	}

	orderInput := Record{
		Key: "key2",
		Data: ValueMap{
			"user_email": String{Val: "nonexistent@test.com"},
			"amount":     Integer{Val: 100},
		},
	}

	joinedRecords := join.InnerJoinStreamTable(ss, join.Stream, orderInput, condition, "not_applicable")
	assert.Empty(t, joinedRecords)

	err := ss.Close()
	assert.Nil(t, err)
}

func TestInnerJoinStreamTable_InvalidRecordType_Panics(t *testing.T) {
	ss := state_store.NewInMemorySS()
	condition := join.JoinCondition{
		LeftKey:  "user_email",
		RightKey: "email",
	}

	userInput := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
			"email":      String{Val: "test@test.com"},
		},
	}

	assert.PanicsWithValue(t, "Invalid record type", func() {
		join.InnerJoinStreamTable(ss, 3, userInput, condition, "not_applicable") // Invalid record type
	})

	err := ss.Close()
	assert.Nil(t, err)
}

func TestInnerJoinStreamTable_WithStreamRecord_JoinSuccessfully(t *testing.T) {
	ss := state_store.NewInMemorySS()
	condition := join.JoinCondition{
		LeftKey:  "user_email",
		RightKey: "email",
	}

	// First store the user record in the state store as a Table record
	userInput := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
			"email":      String{Val: "test@test.com"},
		},
		Metadata: Metadata{Stream: "user_test"},
	}

	err := ss.Set("test@test.com", userInput)
	assert.Nil(t, err)

	// Now perform a join with a Stream record
	orderInput := Record{
		Key: "key2",
		Data: ValueMap{
			"user_email": String{Val: "test@test.com"},
			"amount":     Integer{Val: 100},
		},
		Metadata: Metadata{Stream: "order_test"},
	}

	joinedRecords := join.InnerJoinStreamTable(ss, join.Stream, orderInput, condition, "order_test")
	assert.Len(t, joinedRecords, 1)

	expected_record := Record{
		Key: "key2-key1",
		Data: ValueMap{
			"user_email": String{Val: "test@test.com"},
			"email":      String{Val: "test@test.com"},
			"amount":     Integer{Val: 100},
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
		},
		Metadata: Metadata{Stream: "order_test-user_test-J"},
	}

	assert.Equal(t, expected_record, joinedRecords[0])

	err = ss.Close()
	assert.Nil(t, err)
}

// Integration tests with SDF
func TestJoin_SimpleStreamTableJoin_ShouldJoinStreamRecordToTableRecord(t *testing.T) {
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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Logic to test
	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*core.StreamDataFrame)

	go func() {
		user_input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "foo"},
				"last_name":  String{Val: "bar"},
				"email":      String{Val: "test@test.com"},
			},
		}
		order_input <- Record{
			Key: "key2",
			Data: ValueMap{
				"user_email": String{Val: "test@test.com"},
				"amount":     Integer{Val: 100},
			},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go joined_sdf.Execute(ctx)

	result := <-joined_sdf.OutputStream
	cancel()
	// Assertions
	expected_record := Record{
		Key: "key2-key1",
		Data: ValueMap{
			"user_email": String{Val: "test@test.com"},
			"amount":     Integer{Val: 100},
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
			"email":      String{Val: "test@test.com"},
		},
		Metadata: Metadata{
			Stream: orders_sdf.Name + "-" + user_sdf.Name + join.JoinedStreamSuffix,
		},
	}
	assert.Equal(t, expected_record, result)
	assert.Equal(t, 0, len(user_errors))
	assert.Equal(t, 0, len(orders_errors))
	assert.Equal(t, 0, len(joined_sdf.ErrorStream))
	assert.Equal(t, 0, len(joined_sdf.OutputStream))
}

func TestJoin_InvalidJoinCondition_ShouldPanic(t *testing.T) {
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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

	// Order Data with a schema that does not match the join condition
	order_input := make(chan Record)
	orders_output := make(chan Record)
	orders_errors := make(chan error)
	orders_schema := Schema{
		Columns: Fields{
			"order_id": StringType,
			"amount":   IntType,
		},
	}
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Invalid join condition that references a column not present in the order schema
	invalid_condition := join.JoinCondition{LeftKey: "email", RightKey: "non_existent_column"}

	assert.Panicsf(t,
		func() {
			user_sdf.Join(&orders_sdf, join.Inner, invalid_condition, join.StreamTable)
		},
		functions.ErrColumnNotFound,
		"non_existent_column",
	)
}

func TestJoin_MergeSchemaWithDuplicateColumns_ShouldPanic(t *testing.T) {
	// User Data with a column that will conflict with the order data
	user_input := make(chan Record)
	user_output := make(chan Record)
	user_errors := make(chan error)
	user_schema := Schema{
		Columns: Fields{
			"email":        StringType,
			"first_name":   StringType,
			"last_name":    StringType,
			"common_field": StringType, // Column that will cause a conflict
		},
	}
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

	// Order Data with a conflicting column name
	order_input := make(chan Record)
	orders_output := make(chan Record)
	orders_errors := make(chan error)
	orders_schema := Schema{
		Columns: Fields{
			"order_id":     StringType,
			"amount":       IntType,
			"common_field": StringType, // Conflicting column name
		},
	}
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Valid join condition, but schemas have conflicting column names
	valid_condition := join.JoinCondition{LeftKey: "email", RightKey: "order_id"}

	assert.Panicsf(t,
		func() {
			user_sdf.Join(&orders_sdf, join.Inner, valid_condition, join.StreamTable)
		},
		join.ErrDuplicateColumn,
		"common_field",
	)
}

func TestJoin_StreamRecordWithoutMatch_InnerJoinShouldNotProduceResult(t *testing.T) {
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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Logic to test
	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*core.StreamDataFrame)

	go func() {
		order_input <- Record{
			Key: "key2",
			Data: ValueMap{
				"user_email": String{Val: "test@test.com"},
				"amount":     Integer{Val: 100},
			},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go joined_sdf.Execute(ctx)

	assert.Equal(t, 0, len(user_errors))
	assert.Equal(t, 0, len(orders_errors))
	assert.Equal(t, 0, len(joined_sdf.ErrorStream))
	assert.Equal(t, 0, len(joined_sdf.OutputStream))
	cancel()
}

func TestJoin_TableRecordWithoutMatch_InnerJoinShouldNotProduceResult(t *testing.T) {
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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Logic to test
	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*core.StreamDataFrame)

	go func() {
		user_input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "foo"},
				"last_name":  String{Val: "bar"},
				"email":      String{Val: "test@test.com"},
			},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go joined_sdf.Execute(ctx)

	assert.Equal(t, 0, len(user_errors))
	assert.Equal(t, 0, len(orders_errors))
	assert.Equal(t, 0, len(joined_sdf.ErrorStream))
	assert.Equal(t, 0, len(joined_sdf.OutputStream))
	cancel()
}

func TestJoin_UnorderedDelayedStream_InnerJoinShouldWorkIfWeFirstGetStreamRecordThenTableRecord(t *testing.T) {
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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream", nil)

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream", nil)

	// Logic to test
	joined_sdf := orders_sdf.Join(&user_sdf, join.Inner, join.JoinCondition{LeftKey: "user_email", RightKey: "email"}, join.StreamTable).(*core.StreamDataFrame)

	go func() {
		// First publish the stream record and then the table record
		order_input <- Record{
			Key: "key2",
			Data: ValueMap{
				"user_email": String{Val: "test@test.com"},
				"amount":     Integer{Val: 100},
			},
		}
		user_input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "foo"},
				"last_name":  String{Val: "bar"},
				"email":      String{Val: "test@test.com"},
			},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go joined_sdf.Execute(ctx)

	result := <-joined_sdf.OutputStream
	cancel()
	// Assertions
	expected_record := Record{
		Key: "key2-key1",
		Data: ValueMap{
			"user_email": String{Val: "test@test.com"},
			"amount":     Integer{Val: 100},
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
			"email":      String{Val: "test@test.com"},
		},
		Metadata: Metadata{
			Stream: orders_sdf.Name + "-" + user_sdf.Name + join.JoinedStreamSuffix,
		},
	}
	assert.Equal(t, expected_record, result)
	assert.Equal(t, 0, len(user_errors))
	assert.Equal(t, 0, len(orders_errors))
	assert.Equal(t, 0, len(joined_sdf.ErrorStream))
	assert.Equal(t, 0, len(joined_sdf.OutputStream))
}

func TestStoreForRetry_FirstRecord_StoresFirstDelayedEventSuccessfully(t *testing.T) {
	ss := state_store.NewInMemorySS()
	event := Record{
		Key: "order1",
		Data: ValueMap{
			"value":       Integer{Val: 999},
			"invoice_id":  String{Val: "invoice100"},
			"description": String{Val: "some description"},
		},
		Metadata: Metadata{
			Stream: "stream1",
		},
	}
	condition := join.JoinCondition{
		LeftKey:  "invoice_id",
		RightKey: "id",
	}

	err := join.StoreForRetry(ss, event, condition)
	assert.NoError(t, err)

	record, err := ss.Get("stream1#order1")
	assert.NoError(t, err)
	assert.Equal(t, event, record)

	all_delayed_events, err := ss.Get("stream1#D#invoice100")
	assert.NoError(t, err)
	assert.Equal(t, Record{
		Key: "stream1#D#invoice100",
		Data: ValueMap{
			"ids": Array{Val: []ColumnValue{String{Val: "order1"}}},
		},
		Metadata: Metadata{Stream: record.Metadata.Stream + "#D"},
	}, all_delayed_events)
	ss.Close()
}

func TestStoreForRetry_SecondRecord_AppendToOtherDelayedEventsSuccessfully(t *testing.T) {
	ss := state_store.NewInMemorySS()

	ss.Set("stream1#D#invoice100", Record{
		Key: "stream1#D#invoice100",
		Data: ValueMap{
			"ids": Array{Val: []ColumnValue{String{Val: "order1"}}},
		},
		Metadata: Metadata{Stream: "stream1#D"},
	})

	event := Record{
		Key: "order2",
		Data: ValueMap{
			"value":       Integer{Val: 888},
			"invoice_id":  String{Val: "invoice100"},
			"description": String{Val: "some description 2"},
		},
		Metadata: Metadata{
			Stream: "stream1",
		},
	}
	condition := join.JoinCondition{
		LeftKey:  "invoice_id",
		RightKey: "id",
	}

	err := join.StoreForRetry(ss, event, condition)
	assert.NoError(t, err)

	record, err := ss.Get("stream1#order2")
	assert.NoError(t, err)
	assert.Equal(t, event, record)

	all_delayed_events, err := ss.Get("stream1#D#invoice100")
	assert.NoError(t, err)
	assert.Equal(t, Record{
		Key: "stream1#D#invoice100",
		Data: ValueMap{
			"ids": Array{Val: []ColumnValue{
				String{Val: "order1"},
				String{Val: "order2"}}},
		},
		Metadata: Metadata{Stream: record.Metadata.Stream + "#D"},
	}, all_delayed_events)
}

func TestRetryDelayedEvents_DelayedPrimaryStreamMultipleSecondaryEvents_JoinsAllOrdersSuccessfully(t *testing.T) {
	ss := state_store.NewInMemorySS()

	// Define the main user record that multiple orders reference
	userRecord := Record{
		Key: "user123",
		Data: ValueMap{
			"user_id":   String{Val: "user123"},
			"user_name": String{Val: "John Doe"},
			"email":     String{Val: "john.doe@example.com"},
		},
		Metadata: Metadata{
			Stream: "user_stream",
		},
	}

	// Define multiple orders, all linked to the same user
	order1 := Record{
		Key: "order1",
		Data: ValueMap{
			"order_id":    String{Val: "order1"},
			"value":       Integer{Val: 999},
			"user_id":     String{Val: "user123"},
			"description": String{Val: "First order"},
		},
		Metadata: Metadata{
			Stream: "order_stream",
		},
	}
	order2 := Record{
		Key: "order2",
		Data: ValueMap{
			"order_id":    String{Val: "order2"},
			"value":       Integer{Val: 888},
			"user_id":     String{Val: "user123"},
			"description": String{Val: "Second order"},
		},
		Metadata: Metadata{
			Stream: "order_stream",
		},
	}
	order3 := Record{
		Key: "order3",
		Data: ValueMap{
			"order_id":    String{Val: "order3"},
			"value":       Integer{Val: 777},
			"user_id":     String{Val: "user123"},
			"description": String{Val: "Third order"},
		},
		Metadata: Metadata{
			Stream: "order_stream",
		},
	}

	// Set up records in the state store to simulate unordered events (orders)
	ss.Set("order_stream#order1", order1)
	ss.Set("order_stream#order2", order2)
	ss.Set("order_stream#order3", order3)

	// Define keys of unordered order events that need to be joined with the user
	delayedEventsKeys := []types.ColumnValue{
		String{Val: "order1"},
		String{Val: "order2"},
		String{Val: "order3"},
	}

	result := join.RetryDelayedEvents(ss, userRecord, "order_stream", delayedEventsKeys)

	// Assertions to ensure that all delayed events (orders) were retrieved and merged correctly
	assert.Len(t, result, 3)

	// Each expected merged record should contain data from the user and the individual order
	expectedRecord1 := join.MergeRecords(order1, userRecord)
	expectedRecord2 := join.MergeRecords(order2, userRecord)
	expectedRecord3 := join.MergeRecords(order3, userRecord)

	assert.Equal(t, expectedRecord1, result[0])
	assert.Equal(t, expectedRecord2, result[1])
	assert.Equal(t, expectedRecord3, result[2])
}

func TestRetryDelayedEvents_RecordRetrievalError_Panics(t *testing.T) {
	ss := state_store.NewInMemorySS()

	record := Record{
		Key: "order1",
		Data: ValueMap{
			"value":       Integer{Val: 999},
			"invoice_id":  String{Val: "invoice100"},
			"description": String{Val: "initial description"},
		},
		Metadata: Metadata{
			Stream: "stream1",
		},
	}

	// Call the function under test with a non-existent delayed event key
	delayedEventsKeys := []types.ColumnValue{String{Val: "non_existent_key"}}

	assert.Panics(t, func() {
		join.RetryDelayedEvents(ss, record, "not_applicable", delayedEventsKeys)
	})
}

func TestRetryDelayedEvents_NoDelayedEvents_ReturnsEmpty(t *testing.T) {
	ss := state_store.NewInMemorySS()

	record := Record{
		Key: "order1",
		Data: ValueMap{
			"value":       Integer{Val: 999},
			"invoice_id":  String{Val: "invoice100"},
			"description": String{Val: "initial description"},
		},
		Metadata: Metadata{
			Stream: "stream1",
		},
	}

	// Call the function under test with no delayed events
	delayedEventsKeys := []types.ColumnValue{}
	result := join.RetryDelayedEvents(ss, record, "not_applicable", delayedEventsKeys)

	assert.Len(t, result, 0)
}
