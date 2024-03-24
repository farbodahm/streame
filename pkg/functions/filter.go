package functions

import (
	"github.com/farbodahm/streame/pkg/types"
)

type FilterOperator int

const (
	EQUAL FilterOperator = iota
	NOT_EQUAL
)

// Filter is used to filter data.
// ColumnName is the column name to filter.
// Value is the value to filter.
// Operator is the operator to use.
// Example:
//
//	var filter = Filter{
//	  ColumnName: "first_name",
//	  Value: "FooBar",
//	  Operator: EQUAL,
//	}
type Filter struct {
	ColumnName string
	Value      string
	Operator   FilterOperator
}

// ApplyFilter applies the filter to the record.
// If the filter is not satisfied, it returns nil.
func ApplyFilter(filter Filter, record *types.Record) *types.Record {
	switch filter.Operator {
	case EQUAL:
		// TODO: casting the value to correct type should happen inside the related logic
		if record.Value.(map[string]string)[filter.ColumnName] == filter.Value {
			return record
		}
	case NOT_EQUAL:
		if record.Value.(map[string]string)[filter.ColumnName] != filter.Value {
			return record
		}
	default:
		panic("invalid filter operator")
	}

	return nil
}
