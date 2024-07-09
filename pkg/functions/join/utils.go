package join

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
)

var ErrDuplicateColumn = "column '%s' already exists"

// MergeSchema merges two schemas.
// If duplicate columns are found, it returns an error.
func MergeSchema(left, right types.Schema) (types.Schema, error) {
	new_schema := types.Schema{Columns: types.Fields{}}

	// Deep copy first schema to new schema
	// This is to ensure that the original schema is not modified
	for key, value := range left.Columns {
		new_schema.Columns[key] = value
	}

	for key, value := range right.Columns {
		if _, ok := new_schema.Columns[key]; ok {
			return types.Schema{}, fmt.Errorf(ErrDuplicateColumn, key)
		}
		new_schema.Columns[key] = value
	}

	return new_schema, nil
}
