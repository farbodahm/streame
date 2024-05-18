package functions

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
)

var ErrSchemaLengthMismatch = "dataframe schema and record schema length mismatch: expected %d, got %d"
var ErrColumnNotFound = "column '%s' not found"
var ErrColumnTypeMismatch = "column type mismatch for column '%s': expected %s, got %s"

// ValidateSchema validates the record against the schema
func ValidateSchema(schema types.Schema, record types.Record) error {
	if len(schema.Columns) != len(record.Data) {
		return fmt.Errorf(ErrSchemaLengthMismatch, len(schema.Columns), len(record.Data))
	}

	for k, v := range schema.Columns {
		if _, ok := record.Data[k]; !ok {
			return fmt.Errorf(ErrColumnNotFound, k)
		}
		if v != record.Data[k].Type() {
			return fmt.Errorf(ErrColumnTypeMismatch, k, v, record.Data[k].Type())
		}
	}

	return nil
}
