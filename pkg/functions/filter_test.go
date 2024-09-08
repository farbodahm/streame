package functions_test

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestFilter_EqualOperator_AcceptRecord(t *testing.T) {
	filter := functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.EQUAL,
	}
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
		},
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
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "random_name"},
			"last_name":  String{Val: "random_lastname"},
		},
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
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "random_name"},
			"last_name":  String{Val: "random_lastname"},
		},
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
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
		},
	}

	res := functions.ApplyFilter(filter, &record)
	assert.Nil(t, res)
}

// Integration tests inside DataFrame
func TestFilter_WithDataFrame_AcceptRelatedRecord(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
		},
	}
	sdf := core.NewStreamDataFrame(input, output, errors, schema, "test-stream")

	// Logic to test
	result_df := sdf.Filter(functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.EQUAL,
	})

	// Generate sample data
	accepted_record := Record{
		Key: "key2",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
		},
		Metadata: Metadata{
			Stream: "test-stream",
		},
	}
	go func() {
		input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "random_name"},
				"last_name":  String{Val: "random_lastname"},
			},
		}
		input <- accepted_record
		input <- Record{
			Key: "key3",
			Data: ValueMap{
				"first_name": String{Val: "random_name2"},
				"last_name":  String{Val: "random_lastname2"},
			},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go result_df.Execute(ctx)

	// Assertions
	result := <-output
	cancel()
	assert.Equal(t, result, accepted_record)
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(sdf.ErrorStream))
}

func TestFilter_WithChainedDataFrame_AcceptRelatedRecord(t *testing.T) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	schema := Schema{
		Columns: Fields{
			"first_name": StringType,
			"last_name":  StringType,
			"email":      StringType,
		},
	}
	sdf := core.NewStreamDataFrame(input, output, errors, schema, "test-stream")

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
	accepted_record := Record{
		Key: "key2",
		Data: ValueMap{
			"first_name": String{Val: "foo"},
			"last_name":  String{Val: "bar"},
			"email":      String{Val: "random_email"},
		},
		Metadata: Metadata{
			Stream: "test-stream",
		},
	}
	go func() {
		input <- Record{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "foo"},
				"last_name":  String{Val: "bar"},
				"email":      String{Val: "baz"},
			},
		}
		input <- accepted_record
		input <- Record{
			Key: "key3",
			Data: ValueMap{
				"first_name": String{Val: "foo"},
				"last_name":  String{Val: "random_name"},
				"email":      String{Val: "random_email"},
			},
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go result_df.Execute(ctx)

	// Assertions
	result := <-output
	cancel()
	assert.Equal(t, result, accepted_record)
	assert.Equal(t, 0, len(output))
	assert.Equal(t, 0, len(sdf.ErrorStream))
}
