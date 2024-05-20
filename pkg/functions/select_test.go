package functions_test

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
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

// Integration tests inside DataFrame
func TestSelect_WithDataFrame_SelectOnlyExpectedFields(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	sdf := core.NewStreamDataFrame(input, output, errors, schema)

	// Logic to test
	result_df := sdf.Select("first_name", "age")

	// Generate sample data
	go func() {
		records := []Record{
			{
				Key: "key1",
				Data: ValueMap{
					"first_name": String{Val: "random_name"},
					"last_name":  String{Val: "random_lastname"},
					"age":        Integer{Val: 10},
				},
			},
			{
				Key: "key2",
				Data: ValueMap{
					"first_name": String{Val: "foobar"},
					"last_name":  String{Val: "random_lastname"},
					"age":        Integer{Val: 20},
				},
			},
			{
				Key: "key3",
				Data: ValueMap{
					"first_name": String{Val: "random_name2"},
					"last_name":  String{Val: "random_lastname2"},
					"age":        Integer{Val: 30},
				},
			},
		}
		for _, record := range records {
			input <- record
		}
	}()

	ctx := context.Background()
	go result_df.Execute(ctx)

	// Assertions
	expected_records := []Record{
		{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "random_name"},
				"age":        Integer{Val: 10},
			},
		},
		{
			Key: "key2",
			Data: ValueMap{
				"first_name": String{Val: "foobar"},
				"age":        Integer{Val: 20},
			},
		},
		{
			Key: "key3",
			Data: ValueMap{
				"first_name": String{Val: "random_name2"},
				"age":        Integer{Val: 30},
			},
		},
	}

	for _, expected_record := range expected_records {
		result := <-output
		assert.Equal(t, expected_record, result)
	}
	ctx.Done()
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(sdf.ErrorStream))
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, Schema{
		Columns: Fields{
			"first_name": StringType,
			"age":        IntType,
		},
	}, result_df.GetSchema())
	// Assert that select did not modify the original schema
	assert.Equal(t, schema, sdf.GetSchema())
}
