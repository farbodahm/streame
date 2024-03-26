package functions_test

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestFilter_EqualOperator_AcceptRecord(t *testing.T) {
	filter := functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.EQUAL,
	}
	record := types.Record{
		Key:   "key1",
		Value: map[string]string{"first_name": "foobar", "last_name": "random_lastname"},
	}

	res := functions.ApplyFilter(filter, &record)
	assert.NotNil(t, res)
	assert.Equal(t, *res, record)
}

func TestFilter_EqualOperator_RejectRecord(t *testing.T) {
	filter := functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.EQUAL,
	}
	record := types.Record{
		Key:   "key1",
		Value: map[string]string{"first_name": "random_name", "last_name": "random_lastname"},
	}

	res := functions.ApplyFilter(filter, &record)
	assert.Nil(t, res)
}

func TestFilter_NotEqualOperator_AcceptRecord(t *testing.T) {
	filter := functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.NOT_EQUAL,
	}
	record := types.Record{
		Key:   "key1",
		Value: map[string]string{"first_name": "random_name", "last_name": "random_lastname"},
	}

	res := functions.ApplyFilter(filter, &record)
	assert.NotNil(t, res)
	assert.Equal(t, *res, record)
}

func TestFilter_NotEqualOperator_RejectRecord(t *testing.T) {
	filter := functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.NOT_EQUAL,
	}
	record := types.Record{
		Key:   "key1",
		Value: map[string]string{"first_name": "foobar", "last_name": "random_lastname"},
	}

	res := functions.ApplyFilter(filter, &record)
	assert.Nil(t, res)
}

// Integration tests inside DataFrame
func TestFilter_WithDataFrame_AcceptRelatedRecord(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
	errors := make(chan error)

	sdf := core.StreamDataFrame{
		SourceStream: input,
		OutputStream: output,
		ErrorStream:  errors,
		Stages:       []core.Stage{},
	}

	// Logic to test
	result_df := sdf.Filter(functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.EQUAL,
	})

	// Generate sample data
	accepted_record := types.Record{
		Key:   "key2",
		Value: map[string]string{"first_name": "foobar", "last_name": "random_lastname"},
	}
	go func() {
		input <- types.Record{
			Key:   "key1",
			Value: map[string]string{"first_name": "random_name", "last_name": "random_lastname"},
		}
		input <- accepted_record
		input <- types.Record{
			Key:   "key3",
			Value: map[string]string{"first_name": "random_name2", "last_name": "random_lastname2"},
		}
	}()

	result_df.Execute(context.Background())

	// Assertions
	result := <-output
	assert.Equal(t, result, accepted_record)
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(sdf.ErrorStream))
}

func TestFilter_WithChainedDataFrame_AcceptRelatedRecord(t *testing.T) {
	input := make(chan types.Record)
	output := make(chan types.Record)
	errors := make(chan error)

	sdf := core.StreamDataFrame{
		SourceStream: input,
		OutputStream: output,
		ErrorStream:  errors,
		Stages:       []core.Stage{},
	}

	// Logic to test
	result_df := sdf.Filter(functions.Filter{
		ColumnName: "first_name",
		Value:      "foo",
		Operator:   functions.EQUAL,
	}).Filter(functions.Filter{
		ColumnName: "last_name",
		Value:      "bar",
		Operator:   functions.EQUAL,
	}).Filter(functions.Filter{
		ColumnName: "email",
		Value:      "baz",
		Operator:   functions.NOT_EQUAL,
	})

	// Generate sample data
	accepted_record := types.Record{
		Key:   "key2",
		Value: map[string]string{"first_name": "foo", "last_name": "bar", "email": "random_email"},
	}
	go func() {
		input <- types.Record{
			Key:   "key1",
			Value: map[string]string{"first_name": "foo", "last_name": "bar", "email": "baz"},
		}
		input <- accepted_record
		input <- types.Record{
			Key:   "key3",
			Value: map[string]string{"first_name": "foo", "last_name": "random_name", "email": "random_email"},
		}
	}()

	result_df.Execute(context.Background())

	// Assertions
	result := <-output
	assert.Equal(t, result, accepted_record)
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(sdf.ErrorStream))
}
