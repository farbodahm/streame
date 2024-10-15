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

// StoreForRetry stores a record in the state store for later retry when a join condition is met.
// It first retrieves or initializes a collection of delayed events for a given join value.
// The join value is derived from the record's data using the specified join condition (on.LeftKey).
//
//   - It builds a unique key for all delayed events using the record's stream and the join value (e.g., "stream#D#join_value").
//   - It attempts to retrieve any existing delayed events for this join value from the state store.
//   - If no previous events exist for the join value,
//     a new record is created with an empty list of event IDs ("ids") to track delayed events.
//
// - The record's key is then appended to the list of delayed event IDs for the corresponding join value.
// - After updating the list, it stores the modified collection of delayed events back into the state store.
//
//   - Finally, the function stores the actual record itself in the state store using a unique key
//     derived from the record's stream and key (e.g., "stream#record_key"), so it can be retrieved later
//     when the respective join condition is met.
func StoreForRetry(ss state_store.StateStore, record types.Record, on JoinCondition) error {
	join_value := record.Data[on.LeftKey].ToString()

	all_delayed_events_key := record.Metadata.Stream + "#D#" + join_value
	var all_delayed_events types.Record
	all_delayed_events, err := ss.Get(all_delayed_events_key)

	// If there are no delayed events for this join value, create a new list
	if err != nil {
		all_delayed_events = types.Record{
			Key: all_delayed_events_key,
			Data: types.ValueMap{
				"ids": types.Array{Val: []types.ColumnValue{}},
			},
			Metadata: types.Metadata{Stream: record.Metadata.Stream + "#D"},
		}
	}

	all_delayed_events.Data = types.ValueMap{
		"ids": types.Array{Val: append(all_delayed_events.Data["ids"].ToArray(), types.String{Val: record.Key})},
	}

	if err := ss.Set(all_delayed_events_key, all_delayed_events); err != nil {
		return err
	}

	// Store the record itself to get when respective join is ready
	record_key := record.Metadata.Stream + "#" + record.Key

	if err := ss.Set(record_key, record); err != nil {
		return err
	}

	return nil
}
