package types

import (
	"fmt"
	"strconv"
)

var ErrNoneCastable = "cannot cast '%s' to '%s' error"

// ColumnType represents available types for a column in the schema of a dataframe
type ColumnType int

const (
	IntType ColumnType = iota
	StringType
	ArrayType
)

// ColumnValue is an interface for data types that can be stored in a Record
type ColumnValue interface {
	Value() any
	Type() ColumnType
	ToInt() int
	ToString() string
	ToArray() []ColumnValue
}

// Integer represents a ColumnValue of type int
type Integer struct {
	Val int
}

func (v Integer) Value() any {
	return v.Val
}

func (v Integer) ToInt() int {
	return v.Val
}

func (v Integer) ToString() string {
	return fmt.Sprint(v.Val)
}

func (v Integer) ToArray() []ColumnValue {
	panic(fmt.Errorf(ErrNoneCastable, v.Type(), "Array"))
}

func (v Integer) Type() ColumnType {
	return IntType
}

// String represents a ColumnValue of type string
type String struct {
	Val string
}

func (v String) Value() any {
	return v.Val
}

func (v String) ToInt() int {
	i, err := strconv.Atoi(v.Val)
	if err != nil {
		panic(fmt.Errorf(ErrNoneCastable, v.Type(), "Int"))
	}
	return i
}

func (v String) ToString() string {
	return v.Val
}

func (v String) ToArray() []ColumnValue {
	panic(fmt.Errorf(ErrNoneCastable, v.Type(), "Array"))
}

func (v String) Type() ColumnType {
	return StringType
}

// Array represents a ColumnValue of type array
type Array struct {
	Val []ColumnValue
}

func (v Array) Value() any {
	return v.Val
}

func (v Array) ToInt() int {
	panic(fmt.Errorf(ErrNoneCastable, v.Type(), "Int"))
}

func (v Array) ToString() string {
	panic(fmt.Errorf(ErrNoneCastable, v.Type(), "String"))
}

func (v Array) ToArray() []ColumnValue {
	return v.Val
}

func (v Array) Type() ColumnType {
	return ArrayType
}
