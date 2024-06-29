package state_store

import "github.com/farbodahm/streame/pkg/types"

// StateStore provides functionalities for interacting with the state store
// using repository pattern.
type StateStore interface {
	// TODO: Add batch methods
	Get(key string) (types.Record, error)
	Set(key string, value types.Record) error
	Close() error
}
