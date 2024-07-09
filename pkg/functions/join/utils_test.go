package join_test

import (
	"fmt"
	"testing"

	"github.com/farbodahm/streame/pkg/functions/join"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestMergeSchema_NoDuplicates_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}
	right := Schema{
		Columns: Fields{
			"age":    IntType,
			"salary": IntType,
		},
	}
	expected := Schema{
		Columns: Fields{
			"id":     IntType,
			"name":   StringType,
			"age":    IntType,
			"salary": IntType,
		},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeSchema_WithDuplicates_ReturnDuplicateColumnError(t *testing.T) {
	left := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}
	right := Schema{
		Columns: Fields{
			"id":     IntType,
			"salary": IntType,
		},
	}

	_, err := join.MergeSchema(left, right)
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(join.ErrDuplicateColumn, "id"), err.Error())
}

func TestMergeSchema_EmptyLeftSchema_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{},
	}
	right := Schema{
		Columns: Fields{
			"age":    IntType,
			"salary": IntType,
		},
	}
	expected := Schema{
		Columns: Fields{
			"age":    IntType,
			"salary": IntType,
		},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeSchema_EmptyRightSchema_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}
	right := Schema{
		Columns: Fields{},
	}
	expected := Schema{
		Columns: Fields{
			"id":   IntType,
			"name": StringType,
		},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestMergeSchema_BothEmptySchemas_SuccessfullyMerges(t *testing.T) {
	left := Schema{
		Columns: Fields{},
	}
	right := Schema{
		Columns: Fields{},
	}
	expected := Schema{
		Columns: Fields{},
	}

	result, err := join.MergeSchema(left, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}
