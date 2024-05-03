package types

import (
	"errors"
	"fmt"
	"strconv"
)

var ErrNoneCastable = errors.New("cannot cast error")

// ColumnType represents available types for a column in schema of a dataframe
type ColumnType int

const (
	IntType ColumnType = iota
	StringType
)

// ColumnValue is an interface for data types that can be stored in a Record
type ColumnValue interface {
	Value() any
	Type() ColumnType
	ToInt() int
	ToString() string
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
		panic(errors.Join(ErrNoneCastable, err))
	}
	return i
}

func (v String) ToString() string {
	return v.Val
}

func (sv String) Type() ColumnType {
	return StringType
}
