package functions_test

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
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
	result_df, err := sdf.Filter(functions.Filter{
		ColumnName: "first_name",
		Value:      "foobar",
		Operator:   functions.EQUAL,
	})

	// Generate sample data
	go func() {
		input <- types.Record{
			Key:   "key1",
			Value: map[string]string{"first_name": "random_name", "last_name": "random_lastname"},
		}
		input <- types.Record{
			Key:   "key2",
			Value: map[string]string{"first_name": "foobar", "last_name": "random_lastname"},
		}
	}()

	result_df.Execute(context.Background())

	// Assertions
	assert.Nil(t, err)
	result := <-output
	assert.Equal(t, "key2", result.Key)
	assert.Equal(t, "foobar", result.Value.(map[string]string)["first_name"])
	assert.Equal(t, 0, len(output))
}
