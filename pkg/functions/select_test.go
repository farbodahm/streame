package functions_test

import (
	"testing"

	"github.com/farbodahm/streame/pkg/functions"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestApplySelect_ValidColumnNames_Accepted(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 42},
		},
	}

	actual_result := functions.ApplySelect(record, "first_name", "age")
	expected_result := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"age":        Integer{Val: 42},
		},
	}

	assert.Equal(t, expected_result, actual_result)
	// Assert that input record is not modified
	assert.Equal(t, "key1", record.Key)
	assert.Equal(t, String{Val: "foobar"}, record.Data["first_name"])
	assert.Equal(t, String{Val: "random_lastname"}, record.Data["last_name"])
	assert.Equal(t, Integer{Val: 42}, record.Data["age"])
}

func TestReduceSchema_ValidColumnNames_Accepted(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
			"email":      StringType,
		},
	}
	columns := []string{"first_name", "age"}
	expected_schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"age":        IntType,
		},
	}

	actual_schema, err := functions.ReduceSchema(schema, columns...)

	assert.Nil(t, err)
	assert.Equal(t, expected_schema, actual_schema)
}

func TestReduceSchema_RemoveColumnsIncorrectly_ReturnColumnNotFoundError(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
			"email":      StringType,
		},
	}
	columns := []string{"first_name", "age", "unknown_column"}
	expected_error := "column 'unknown_column' not found"

	actual_schema, err := functions.ReduceSchema(schema, columns...)

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, Schema{}, actual_schema)
}
