package functions_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestAddColumnToSchema_ValidColumnName_ColumnIsAddedToSchema(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	expected_schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
			"email":      StringType,
		},
	}

	actual_schema, err := functions.AddColumnToSchema(schema, "email", StringType)

	assert.Nil(t, err)
	assert.Equal(t, expected_schema, actual_schema)
	// Assert that adding static column did not modify the original schema
	assert.Equal(t, Fields{
		"first_name": StringType,
		"last_name":  StringType,
		"age":        IntType,
	}, schema.Columns)
}

func TestAddColumnToSchema_ColumnAlreadyExists_ReturnColumnAlreadyExistsError(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	actual_schema, err := functions.AddColumnToSchema(schema, "age", IntType)
	expected_error := fmt.Sprintf(functions.ErrColumnAlreadyExists, "age")

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, Schema{}, actual_schema)
}

func TestAddStaticColumnToRecord_AddNewIntColumn_RecordAdded(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
		},
	}
	expected_result := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 10},
		},
	}

	actual_result := functions.AddStaticColumnToRecord(record, "age", Integer{Val: 10})

	assert.Equal(t, expected_result, actual_result)
	// Assert that input record is not modified
	assert.Equal(t, "key1", record.Key)
	assert.Equal(t, ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
	}, record.Data)
}

// Integration tests inside DataFrame
func TestAddStaticColumn_AddIntegerField_AddColumnToSchemaAndRecords(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
		},
	}
	sdf := core.NewStreamDataFrame(input, output, errors, schema, "test-stream", nil)

	// Logic to test
	result_df := sdf.AddStaticColumn("age", Integer{Val: 10})

	// Generate sample data
	go func() {
		records := []Record{
			{
				Key: "key1",
				Data: ValueMap{
					"first_name": String{Val: "random_name"},
					"last_name":  String{Val: "random_lastname"},
				},
			},
			{
				Key: "key2",
				Data: ValueMap{
					"first_name": String{Val: "foobar"},
					"last_name":  String{Val: "random_lastname"},
				},
			},
			{
				Key: "key3",
				Data: ValueMap{
					"first_name": String{Val: "random_name2"},
					"last_name":  String{Val: "random_lastname2"},
				},
			},
		}
		for _, record := range records {
			input <- record
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go result_df.Execute(ctx)

	// Assertions
	expected_records := []Record{
		{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "random_name"},
				"last_name":  String{Val: "random_lastname"},
				"age":        Integer{Val: 10},
			},
			Metadata: Metadata{
				Stream: "test-stream",
			},
		},
		{
			Key: "key2",
			Data: ValueMap{
				"first_name": String{Val: "foobar"},
				"last_name":  String{Val: "random_lastname"},
				"age":        Integer{Val: 10},
			},
			Metadata: Metadata{
				Stream: "test-stream",
			},
		},
		{
			Key: "key3",
			Data: ValueMap{
				"first_name": String{Val: "random_name2"},
				"last_name":  String{Val: "random_lastname2"},
				"age":        Integer{Val: 10},
			},
			Metadata: Metadata{
				Stream: "test-stream",
			},
		},
	}

	for _, expected_record := range expected_records {
		result := <-output
		assert.Equal(t, expected_record, result)
	}
	cancel()
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(sdf.ErrorStream))
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}, result_df.GetSchema())
	// Assert that adding static column did not modify the original schema
	assert.Equal(t, schema, sdf.GetSchema())
}

func TestAddStaticColumn_AddAlreadyExistingColumn_PanicsWithAlreadyExistsColumn(t *testing.T) {
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
	sdf := core.NewStreamDataFrame(input, output, errors, schema, "test-stream", nil)

	assert.Panicsf(t,
		func() {
			sdf.AddStaticColumn("age", Integer{Val: 10})
		},
		functions.ErrColumnAlreadyExists,
		"age",
	)
}
