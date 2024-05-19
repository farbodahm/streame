package functions

import (
	"github.com/farbodahm/streame/pkg/types"
)

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
