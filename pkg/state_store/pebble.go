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

	data, err := messaging.ProtocolBuffersToValueMap(value)
	if err != nil {
		return types.Record{}, err
	}

	if err := closer.Close(); err != nil {
		return types.Record{}, err
	}

	return types.Record{
		Key:  key,
		Data: data,
	}, nil
}

// Set inserts an object to state store
func (p PebbleStateStore) Set(key string, value types.Record) error {
	// TODO: Decide about metadata in record
	data, err := messaging.ValueMapToProtocolBuffers(value.Data)
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
