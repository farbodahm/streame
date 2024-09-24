package join

import (
	"github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/types"
)

// JoinType specifies how to join 2 data frames
type JoinType int8

const (
	Inner JoinType = iota
	Left
)

// JoinMode specifies the way to see input streams
type JoinMode int8

const (
	StreamTable JoinMode = iota
	StreamStream
)

// RecordType specifies if a record should be considered as a Stream record or Table record
type RecordType int8

const (
	Stream RecordType = iota
	Table
)

// JoinCondition specifies conditions that should be met in order for a join to happen
type JoinCondition struct {
	LeftKey  string
	RightKey string
}

// JoinedStreamSuffix gets appended to the stream name of the SDF for the joined stream
// to distinguish it from the source streams
const JoinedStreamSuffix = "-J"

// InnerJoinStreamTable performs an inner join operation between a streaming record and records stored in a state store.
// Depending on the record type (Stream or Table), the function either updates the state store or fetches and joins records.
//
// Behavior:
// - If the record type is Table:
//   - The function retrieves the join key from the record using on.RightKey.
//   - It stores the record in the state store with the key.
//
// - If the record type is Stream:
//   - The function retrieves the join key from the record using on.LeftKey.
//   - It fetches the corresponding record from the state store.
//   - If the record exists in the state store, it merges the current record with the fetched record and returns the joined result.
//   - If the record does not exist in the state store, it logs a warning and returns an empty slice.
//
// - If the record type is invalid, it panics with an error message.
func InnerJoinStreamTable(ss state_store.StateStore, record_type RecordType, record types.Record, on JoinCondition) []types.Record {

	switch record_type {
	case Table:
		key := record.Data[on.RightKey]
		ss.Set(key.ToString(), record)
	case Stream:
		key := record.Data[on.LeftKey]

		relative_record, exists := ss.Get(key.ToString())
		if exists != nil {
			return []types.Record{}
		}

		joined_record := MergeRecords(record, relative_record)
		return []types.Record{joined_record}
	default:
		panic("Invalid record type")
	}

	return []types.Record{}
}

func storeForRetry(ss state_store.StateStore, record types.Record, on JoinCondition) {
	key := record.Data[on.LeftKey]
	ss.Set(key.ToString(), record)
}
