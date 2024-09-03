package join_test

import (
	"testing"

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
