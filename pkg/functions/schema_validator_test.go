package functions_test

import (
	"fmt"
	"testing"

	"github.com/farbodahm/streame/pkg/functions"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestSchemaValidator_CorrectSchema_AcceptRecord(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
	}

	err := functions.ValidateSchema(schema, record)

	assert.Nil(t, err)
}

func TestSchemaValidator_SchemaHasMoreColumns_RejectRecord(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
		},
	}

	actual_err := functions.ValidateSchema(schema, record)
	expected_err := fmt.Sprintf(functions.ErrSchemaLengthMismatch, len(schema.Columns), len(record.Data))

	assert.EqualError(t, actual_err, expected_err)
}

func TestSchemaValidator_RecordHasLessColumns_RejectRecord(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
		},
	}
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
	}

	actual_err := functions.ValidateSchema(schema, record)
	expected_err := fmt.Sprintf(functions.ErrSchemaLengthMismatch, len(schema.Columns), len(record.Data))

	assert.EqualError(t, actual_err, expected_err)
}

func TestSchemaValidator_ColumnNotExistInRecord_RejectRecord(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
		},
	}
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"age":        Integer{Val: 23},
		},
	}

	actual_err := functions.ValidateSchema(schema, record)
	expected_err := fmt.Sprintf(functions.ErrColumnNotFound, "last_name")

	assert.EqualError(t, actual_err, expected_err)
}

func TestSchemaValidator_ColumnTypeMisMatch_RejectRecord(t *testing.T) {
	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"age":        IntType,
		},
	}
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        String{Val: "23"},
		},
	}

	actual_err := functions.ValidateSchema(schema, record)
	expected_err := fmt.Sprintf(functions.ErrColumnTypeMismatch, "age", IntType, StringType)

	assert.EqualError(t, actual_err, expected_err)
}
