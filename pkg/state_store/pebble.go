package state_store

import (
	"github.com/cockroachdb/pebble"
	"github.com/farbodahm/streame/pkg/messaging"
	"github.com/farbodahm/streame/pkg/types"
)

// Make sure PebbleStateStore implements StateStore
var _ StateStore = PebbleStateStore{}

// PebbleStateStore implements Pebble as a state store.
// Pebble is a RocksDB inspired key-value store.
type PebbleStateStore struct {
	db *pebble.DB
}

// NewPebbleStateStore creates a new instance of PebbleStateStore
func NewPebbleStateStore(path string, options *pebble.Options) (*PebbleStateStore, error) {
	db, err := pebble.Open(path, options)
	if err != nil {
		return &PebbleStateStore{}, err
	}

	return &PebbleStateStore{db: db}, nil
}

// Get returns an object from state store
func (p PebbleStateStore) Get(key string) (types.Record, error) {
	value, closer, err := p.db.Get([]byte(key))
	if err != nil {
		return types.Record{}, err
	}
	defer closer.Close()

	// Use ProtocolBuffersToRecord to deserialize the value
	record, err := messaging.ProtocolBuffersToRecord(value)
	if err != nil {
		return types.Record{}, err
	}

	return record, nil
}

// Set inserts an object to state store
// NOTE: Set overwrites any previous value for that key; it doesn't do
// an upsert.
// Data of the record is stored as ProtobufMessage
func (p PebbleStateStore) Set(key string, value types.Record) error {
	data, err := messaging.RecordToProtocolBuffers(value)
	if err != nil {
		return err
	}

	if err := p.db.Set([]byte(key), data, pebble.Sync); err != nil {
		return err
	}

	return nil
}

// Close closes the connection to state store
func (p PebbleStateStore) Close() error {
	if err := p.db.Close(); err != nil {
		return err
	}

	return nil
}
