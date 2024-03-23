package types

// DataRecord represents a single data record in the stream.
type DataRecord struct {
	Key      string
	Value    interface{}
	Metadata Metadata
}

// Metadata contains metadata for a data record, such as timestamps.
type Metadata struct {
	Timestamp int64 // Unix timestamp
}
