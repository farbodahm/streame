package state_store

import "github.com/farbodahm/streame/pkg/types"

// Make sure PebbleStateStore implements StateStore
var _ StateStore = PebbleStateStore{}

type PebbleStateStore struct{}

func (PebbleStateStore) Get(key string) (types.Record, error) {
	return types.Record{}, nil
}

func (PebbleStateStore) Set(key string, value types.Record) error {
	return nil
}

func (PebbleStateStore) Close() error {
	return nil
}
