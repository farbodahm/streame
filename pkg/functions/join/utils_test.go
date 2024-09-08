package join_test

import (
	"fmt"
	"testing"

	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/functions/join"
	"github.com/farbodahm/streame/pkg/types"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestMergeSchema_NoDuplicates_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}
	right := Schema{
		Columns: Fields{
			"age":    IntType,
			"salary": IntType,
		},
	}
	expected := Schema{
		Columns: Fields{
			"id":     IntType,
			"name":   StringType,
			"age":    IntType,
			"salary": IntType,
		},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeSchema_WithDuplicates_ReturnDuplicateColumnError(t *testing.T) {
	left := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}
	right := Schema{
		Columns: Fields{
			"id":     IntType,
			"salary": IntType,
		},
	}

	_, err := join.MergeSchema(left, right)
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(join.ErrDuplicateColumn, "id"), err.Error())
}

func TestMergeSchema_EmptyLeftSchema_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{},
	}
	right := Schema{
		Columns: Fields{
			"age":    IntType,
			"salary": IntType,
		},
	}
	expected := Schema{
		Columns: Fields{
			"age":    IntType,
			"salary": IntType,
		},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeSchema_EmptyRightSchema_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}
	right := Schema{
		Columns: Fields{},
	}
	expected := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeSchema_BothEmptySchemas_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{},
	}
	right := Schema{
		Columns: Fields{},
	}
	expected := Schema{
		Columns: Fields{},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeRecords_BothRecordsHaveUniqueKeys_MergesCorrectly(t *testing.T) {
	left := Record{
		Key: "left",
		Data: ValueMap{
			"first_name": String{Val: "John"},
			"last_name":  String{Val: "Doe"},
		},
		Metadata: Metadata{
			Stream: "stream1",
		},
	}

	right := Record{
		Key: "right",
		Data: ValueMap{
			"age":    Integer{Val: 30},
			"gender": String{Val: "Male"},
		},
		Metadata: Metadata{
			Stream: "stream2",
		},
	}

	expected := Record{
		Key: "left-right",
		Data: ValueMap{
			"first_name": String{Val: "John"},
			"last_name":  String{Val: "Doe"},
			"age":        Integer{Val: 30},
			"gender":     String{Val: "Male"},
		},
		Metadata: Metadata{
			Stream: "stream1-stream2" + join.JoinedStreamSuffix,
		},
	}

	result := join.MergeRecords(left, right)

	assert.Equal(t, expected, result)
}

func TestMergeRecords_EmptyLeftRecord_MergesCorrectly(t *testing.T) {
	left := types.Record{
		Key:  "left",
		Data: types.ValueMap{},
		Metadata: types.Metadata{
			Stream: "stream1",
		},
	}

	right := types.Record{
		Key: "right",
		Data: types.ValueMap{
			"name": types.String{Val: "Alice"},
			"age":  types.Integer{Val: 28},
		},
		Metadata: types.Metadata{
			Stream: "stream2",
		},
	}

	expected := types.Record{
		Key: "left-right",
		Data: types.ValueMap{
			"name": types.String{Val: "Alice"},
			"age":  types.Integer{Val: 28},
		},
		Metadata: types.Metadata{
			Stream: "stream1-stream2" + join.JoinedStreamSuffix,
		},
	}

	result := join.MergeRecords(left, right)

	assert.Equal(t, expected, result)
}

func TestMergeRecords_EmptyRightRecord_MergesCorrectly(t *testing.T) {
	left := types.Record{
		Key: "left",
		Data: types.ValueMap{
			"name": types.String{Val: "Alice"},
			"age":  types.Integer{Val: 28},
		},
		Metadata: types.Metadata{
			Stream: "stream1",
		},
	}

	right := types.Record{
		Key:  "right",
		Data: types.ValueMap{},
		Metadata: types.Metadata{
			Stream: "stream2",
		},
	}

	expected := types.Record{
		Key: "left-right",
		Data: types.ValueMap{
			"name": types.String{Val: "Alice"},
			"age":  types.Integer{Val: 28},
		},
		Metadata: types.Metadata{
			Stream: "stream1-stream2" + join.JoinedStreamSuffix,
		},
	}

	result := join.MergeRecords(left, right)

	assert.Equal(t, expected, result)
}

func TestMergeRecords_BothRecordsEmpty_ReturnsEmptyRecord(t *testing.T) {
	left := types.Record{
		Key:      "left",
		Data:     types.ValueMap{},
		Metadata: types.Metadata{Stream: "stream1"},
	}

	right := types.Record{
		Key:      "right",
		Data:     types.ValueMap{},
		Metadata: types.Metadata{Stream: "stream2"},
	}

	expected := types.Record{
		Key:      "left-right",
		Data:     types.ValueMap{},
		Metadata: types.Metadata{Stream: "stream1-stream2" + join.JoinedStreamSuffix},
	}

	result := join.MergeRecords(left, right)

	assert.Equal(t, expected, result)
}

func TestValidateJoinCondition_ValidCondition_NoError(t *testing.T) {
	leftSchema := types.Schema{
		Columns: types.Fields{
			"id": types.IntType,
		},
	}
	rightSchema := types.Schema{
		Columns: types.Fields{
			"user_id": types.IntType,
		},
	}
	condition := join.JoinCondition{
		LeftKey:  "id",
		RightKey: "user_id",
	}

	err := join.ValidateJoinCondition(leftSchema, rightSchema, condition)

	assert.Nil(t, err)
}

func TestValidateJoinCondition_MissingLeftKey_ReturnsError(t *testing.T) {
	leftSchema := types.Schema{
		Columns: types.Fields{
			"name": types.StringType,
		},
	}
	rightSchema := types.Schema{
		Columns: types.Fields{
			"user_id": types.IntType,
		},
	}
	condition := join.JoinCondition{
		LeftKey:  "id",
		RightKey: "user_id",
	}

	err := join.ValidateJoinCondition(leftSchema, rightSchema, condition)

	assert.NotNil(t, err)
	assert.EqualError(t, err, fmt.Sprintf(functions.ErrColumnNotFound, "id"))
}

func TestValidateJoinCondition_MissingRightKey_ReturnsError(t *testing.T) {
	leftSchema := types.Schema{
		Columns: types.Fields{
			"id": types.IntType,
		},
	}
	rightSchema := types.Schema{
		Columns: types.Fields{
			"email": types.StringType,
		},
	}
	condition := join.JoinCondition{
		LeftKey:  "id",
		RightKey: "user_id",
	}

	err := join.ValidateJoinCondition(leftSchema, rightSchema, condition)

	assert.NotNil(t, err)
	assert.EqualError(t, err, fmt.Sprintf(functions.ErrColumnNotFound, "user_id"))
}
