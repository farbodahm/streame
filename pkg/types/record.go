package types

// Fields represents columns for a schema
type Fields map[string]ColumnType

// ValueMap represents a row of data for a dataframe
type ValueMap map[string]ColumnValue

// Schema represents a schema for a dataframe
type Schema struct {
	Columns Fields
}

// Record represents a single data record in the stream.
type Record struct {
	Key      string
	Data     ValueMap
	Metadata Metadata
}

// Metadata contains metadata for a data record, such as timestamps.
type Metadata struct {
	Timestamp int64 // Unix timestamp
}
