package functions

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
)

// ReduceSchema reducts the schema to only include the given columns.
func ReduceSchema(old_schema types.Schema, columns ...string) (types.Schema, error) {
	new_schema := types.Schema{Columns: types.Fields{}}

	for _, column_str := range columns {
		column, ok := old_schema.Columns[column_str]
		if !ok {
			return types.Schema{}, fmt.Errorf(ErrColumnNotFound, column_str)
		}

		new_schema.Columns[column_str] = column
	}

	return new_schema, nil
}

// ApplySelect selects the given columns from the record
// and returns a new record with only those columns.
// NOTE: As schema validation is forced on first stage,
// this function doesn't check the schema of the record.
func ApplySelect(record types.Record, columns ...string) types.Record {
	new_record := types.Record{
		Key:      record.Key,
		Metadata: record.Metadata,
		Data:     types.ValueMap{},
	}

	for _, column := range columns {
		new_record.Data[column] = record.Data[column]
	}

	return new_record
}
