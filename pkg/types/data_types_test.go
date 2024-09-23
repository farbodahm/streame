package types_test

import (
	"fmt"
	"testing"

	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestInteger_Value_ReturnsInitializedValue(t *testing.T) {
	intVal := Integer{Val: 42}
	assert.Equal(t, 42, intVal.Value())
}

func TestInteger_ToInt_ReturnsInt(t *testing.T) {
	intVal := Integer{Val: 42}
	assert.Equal(t, 42, intVal.ToInt())
}

func TestInteger_ToString_ReturnsString(t *testing.T) {
	intVal := Integer{Val: 42}
	assert.Equal(t, "42", intVal.ToString())
}

func TestInteger_ToArray_PanicsWithError(t *testing.T) {
	intVal := Integer{Val: 42}
	expectedError := fmt.Sprintf(ErrNoneCastable, IntType, "Array")

	assert.PanicsWithError(t, expectedError, func() {
		_ = intVal.ToArray()
	}, "Expected panic with error: %s", expectedError)
}

func TestString_Value_ReturnsInitializedValue(t *testing.T) {
	strVal := String{Val: "test"}
	assert.Equal(t, "test", strVal.Value())
}

func TestString_ToInt_PanicsWithError(t *testing.T) {
	strVal := String{Val: "not_a_number"}
	expectedError := fmt.Sprintf(ErrNoneCastable, StringType, "Int")

	assert.PanicsWithError(t, expectedError, func() {
		_ = strVal.ToInt()
	}, "Expected panic with error: %s", expectedError)
}

func TestString_ToString_ReturnsString(t *testing.T) {
	strVal := String{Val: "test"}
	assert.Equal(t, "test", strVal.ToString())
}

func TestString_ToArray_PanicsWithError(t *testing.T) {
	strVal := String{Val: "test"}
	expectedError := fmt.Sprintf(ErrNoneCastable, StringType, "Array")

	assert.PanicsWithError(t, expectedError, func() {
		_ = strVal.ToArray()
	}, "Expected panic with error: %s", expectedError)
}

func TestArray_Value_ReturnsInitializedValue(t *testing.T) {
	arrVal := Array{Val: []ColumnValue{Integer{Val: 1}, String{Val: "two"}}}
	assert.Equal(t, arrVal.Val, arrVal.Value())
}

func TestArray_ToInt_PanicsWithError(t *testing.T) {
	arrVal := Array{Val: []ColumnValue{Integer{Val: 1}, String{Val: "two"}}}
	expectedError := fmt.Sprintf(ErrNoneCastable, ArrayType, "Int")

	assert.PanicsWithError(t, expectedError, func() {
		_ = arrVal.ToInt()
	}, "Expected panic with error: %s", expectedError)
}

func TestArray_ToString_PanicsWithError(t *testing.T) {
	arrVal := Array{Val: []ColumnValue{Integer{Val: 1}, String{Val: "two"}}}
	expectedError := fmt.Sprintf(ErrNoneCastable, ArrayType, "String")

	assert.PanicsWithError(t, expectedError, func() {
		_ = arrVal.ToString()
	}, "Expected panic with error: %s", expectedError)
}

func TestArray_ToArray_ReturnsArray(t *testing.T) {
	arrVal := Array{Val: []ColumnValue{Integer{Val: 1}, String{Val: "two"}}}
	assert.Equal(t, arrVal.Val, arrVal.ToArray())
}
