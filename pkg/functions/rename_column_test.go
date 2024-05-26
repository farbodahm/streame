package functions_test

import (
	"fmt"
	"testing"

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
