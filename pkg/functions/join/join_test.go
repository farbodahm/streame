package join_test

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/functions/join"
	"github.com/farbodahm/streame/pkg/state_store"
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
	result := join.InnerJoinStreamTable(ss, join.Table, record, condition)
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

	joinedRecords := join.InnerJoinStreamTable(ss, join.Stream, orderInput, condition)
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
		join.InnerJoinStreamTable(ss, 3, userInput, condition) // Invalid record type
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

	joinedRecords := join.InnerJoinStreamTable(ss, join.Stream, orderInput, condition)
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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream")

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream")

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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream")

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream")

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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream")

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream")

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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream")

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream")

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
	user_sdf := core.NewStreamDataFrame(user_input, user_output, user_errors, user_schema, "user-stream")

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
	orders_sdf := core.NewStreamDataFrame(order_input, orders_output, orders_errors, orders_schema, "orders-stream")

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
	t.Skip("Currently Streame doesn't support Delayed Primary Stream. It's in the road map soon...")
}
