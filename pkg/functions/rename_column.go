package functions

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
)

// RenameColumnInSchema renames the column in the schema.
// It returns a new schema with the column renamed.
func RenameColumnInSchema(schema types.Schema, old_column_name string, new_column_name string) (types.Schema, error) {
	if _, exists := schema.Columns[old_column_name]; !exists {
		return types.Schema{}, fmt.Errorf(ErrColumnNotFound, old_column_name)
	}

	if _, exists := schema.Columns[new_column_name]; exists {
		return types.Schema{}, fmt.Errorf(ErrColumnAlreadyExists, new_column_name)
	}

	new_schema := types.Schema{Columns: types.Fields{}}
	// Deep copy previous schema to new schema
	// This is to ensure that the original schema is not modified
	for key, value := range schema.Columns {
		// Skip the column to be renamed
		if key == old_column_name {
			continue
		}
		new_schema.Columns[key] = value
	}

	// Rename column in new schema
	new_schema.Columns[new_column_name] = schema.Columns[old_column_name]

	return new_schema, nil
}

// RenameColumnInRecord renames the column in the record.
// NOTE: As schema validation is forced on first stage,
// this function doesn't check the schema of the record.
// NOTE: This function does not add the column to the schema.
// Caller needs to handle adding the column to the schema if needed.
func RenameColumnInRecord(record types.Record, old_column_name string, new_column_name string) types.Record {
	new_record := types.Record{
		Key:      record.Key,
		Metadata: record.Metadata,
		Data:     types.ValueMap{},
	}

	// Deep copy previous record to new record
	for key, value := range record.Data {
		// Skip the column to be renamed
		if key == old_column_name {
			continue
		}
		new_record.Data[key] = value
	}
	new_record.Data[new_column_name] = record.Data[old_column_name]
	return new_record
}
