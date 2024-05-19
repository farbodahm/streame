package functions_test

import (
	"testing"

	"github.com/farbodahm/streame/pkg/functions"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestSelect_RemoveColumns_Accepted(t *testing.T) {
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
