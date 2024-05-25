package functions

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
)

var ErrColumnAlreadyExists = "column '%s' already exists"

// AddColumnToSchema adds a new column to a schema
func AddColumnToSchema(schema types.Schema, column_name string, column_type types.ColumnType) (types.Schema, error) {
	if _, exists := schema.Columns[column_name]; exists {
		return types.Schema{}, fmt.Errorf(ErrColumnAlreadyExists, column_name)
	}

	new_schema := types.Schema{Columns: types.Fields{}}
	// Deep copy previous schema to new schema
	// This is to ensure that the original schema is not modified
	for key, value := range schema.Columns {
		new_schema.Columns[key] = value
	}

	// Add new column to new schema
	new_schema.Columns[column_name] = column_type

	return new_schema, nil
}

// AddStaticColumnToRecord adds a new static column to a record.
// NOTE: This function does not add the column to the schema.
// Caller needs to handle adding the column to the schema if needed.
func AddStaticColumnToRecord(record types.Record, column_name string, value types.ColumnValue) types.Record {
	new_record := types.Record{
		Key:      record.Key,
		Metadata: record.Metadata,
		Data:     types.ValueMap{},
	}

	// Deep copy previous record to new record
	for key, value := range record.Data {
		new_record.Data[key] = value
	}
	new_record.Data[column_name] = value
	return new_record
}
