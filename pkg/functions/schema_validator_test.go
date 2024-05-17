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

func TestSreamDataFrame_SchemaValidation_AcceptRecordFollowSchema(t *testing.T) {
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

	// Generate sample data
	record_1 := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "random_name"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 10},
		},
	}
	record_2 := Record{
		Key: "key2",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 20},
		},
	}
	go func() {
		input <- record_1
		input <- record_2
	}()

	ctx := context.Background()
	go sdf.Execute(ctx)

	ctx.Done()
	result_1 := <-output
	result_2 := <-output

	// Assertions
	assert.Equal(t, result_1.Key, "key1")
	assert.Equal(t, result_2.Key, "key2")
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(input))
}

func TestSreamDataFrame_SchemaValidation_PanicIfRecordDoesntFollowSchema(t *testing.T) {
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

	// Generate sample data
	faulty_record := Record{
		Key: "key2",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
		},
	}
	go func() {
		input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "random_name"},
				"last_name":  String{Val: "random_lastname"},
				"age":        Integer{Val: 10},
			},
		}
		input <- faulty_record

	}()
	go func() {
		<-output
		<-output
	}()

	assert.Panics(t, func() {
		sdf.Execute(context.Background())
	})
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(input))
}
