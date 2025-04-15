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

func TestRenameColumnInSchema_ValidColumnName_ColumnIsRenamedInSchema(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	expected_schema := Schema{
		Columns: Fields{
			"first_name":  StringType,
			"family_name": StringType,
			"age":         IntType,
		},
	}

	actual_schema, err := functions.RenameColumnInSchema(schema, "last_name", "family_name")

	assert.Nil(t, err)
	assert.Equal(t, expected_schema, actual_schema)
	// Assert renaming column did not modify the original schema
	assert.Equal(t, Fields{
		"first_name": StringType,
		"last_name":  StringType,
		"age":        IntType,
	}, schema.Columns)
}

func TestRenameColumnInSchema_NewColumnNameExists_ReturnColumnAlreadyExistsError(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}

	actual_schema, err := functions.RenameColumnInSchema(schema, "last_name", "first_name")

	expected_error := fmt.Sprintf(functions.ErrColumnAlreadyExists, "first_name")

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, Schema{}, actual_schema)
}

func TestRenameColumnInSchema_OldColumnDoesntExists_ReturnColumnNotFoundError(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}

	actual_schema, err := functions.RenameColumnInSchema(schema, "random_name", "some_name")

	expected_error := fmt.Sprintf(functions.ErrColumnNotFound, "random_name")

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, Schema{}, actual_schema)
}

func TestRenameColumnInRecord_ValidName_ColumnIsRenamed(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 10},
		},
	}
	expected_result := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name":  String{Val: "foobar"},
			"family_name": String{Val: "random_lastname"},
			"age":         Integer{Val: 10},
		},
	}

	actual_result := functions.RenameColumnInRecord(record, "last_name", "family_name")

	assert.Equal(t, expected_result, actual_result)
	// Assert that input record is not modified
	assert.Equal(t, "key1", record.Key)
	assert.Equal(t, ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
		"age":        Integer{Val: 10},
	}, record.Data)
}

// Integration tests inside DataFrame
func TestRename_ValidNames_ColumnIsRenamedInSchemaAndRecords(t *testing.T) {
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

	// Logic to test
	result_df := sdf.Rename("last_name", "family_name")

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

	ctx, cancel := context.WithCancel(context.Background())
	go result_df.Execute(ctx)

	// Assertions
	expected_records := []Record{
		{
			Key: "key1",
			Data: ValueMap{
				"first_name":  String{Val: "random_name"},
				"family_name": String{Val: "random_lastname"},
				"age":         Integer{Val: 10},
			},
			Metadata: Metadata{
				Stream: "test-stream",
			},
		},
		{
			Key: "key2",
			Data: ValueMap{
				"first_name":  String{Val: "foobar"},
				"family_name": String{Val: "random_lastname"},
				"age":         Integer{Val: 20},
			},
			Metadata: Metadata{
				Stream: "test-stream",
			},
		},
		{
			Key: "key3",
			Data: ValueMap{
				"first_name":  String{Val: "random_name2"},
				"family_name": String{Val: "random_lastname2"},
				"age":         Integer{Val: 30},
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
			"first_name":  StringType,
			"family_name": StringType,
			"age":         IntType,
		},
	}, result_df.GetSchema())
	// Assert that renaming column did not modify the original schema
	assert.Equal(t, schema, sdf.GetSchema())
}

func TestRename_AddAlreadyExistingColumn_PanicsWithAlreadyExistsColumn(t *testing.T) {
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
			sdf.Rename("last_name", "first_name")
		},
		functions.ErrColumnAlreadyExists,
		"first_name",
	)
}

func TestRename_AddColumnNameNotExists_PanicsWithColumnNotFound(t *testing.T) {
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
			sdf.Rename("random_name", "first_name")
		},
		functions.ErrColumnNotFound,
		"random_name",
	)
}
