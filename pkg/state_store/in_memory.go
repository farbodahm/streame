package state_store

import (
	"errors"
	"sync"

	"github.com/farbodahm/streame/pkg/types"
)

// Make sure InMemorySS implements StateStore
var _ StateStore = &InMemorySS{}

// InMemorySS is an in-memory implementation of the StateStore interface.
// NOTE: This should NOT be used on Production environments as it doesn't persist the data.
type InMemorySS struct {
	store map[string]types.Record
	mu    sync.RWMutex
}

// NewInMemorySS creates a new instance of InMemorySS
func NewInMemorySS() *InMemorySS {
	return &InMemorySS{
		store: make(map[string]types.Record),
	}
}

// Get retrieves a record from the state store by key.
// It returns an error if the key does not exist.
func (s *InMemorySS) Get(key string) (types.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.store[key]
	if !exists {
		return types.Record{}, errors.New("key not found")
	}

	return value, nil
}

// Set stores a record in the state store by key.
// It overwrites any existing value associated with the key.
func (s *InMemorySS) Set(key string, value types.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[key] = value
	return nil
}

// Close empties the state store by clearing the map.
func (s *InMemorySS) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.store)
	return nil
}
