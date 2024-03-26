package types

// Record represents a single data record in the stream.
type Record struct {
	Key      string
	Value    interface{}
	Metadata Metadata
}

// Metadata contains metadata for a data record, such as timestamps.
type Metadata struct {
	Timestamp int64 // Unix timestamp
}
