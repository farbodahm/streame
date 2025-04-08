//go:build integration

package state_store_test

import (
	"testing"
	"time"

	. "github.com/farbodahm/streame/pkg/state_store"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func setupTestEtcdStore(t *testing.T) *EtcdStateStore {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.Nil(t, err)

	ss, err := NewEtcdStateStore(cli)
	assert.Nil(t, err)

	t.Cleanup(func() {
		err := ss.Close()
		assert.Nil(t, err)
	})

	return ss
}

func TestEtcdStateStore_ValidRecord_WriteAndReadToEtcdSuccessfully(t *testing.T) {
	ss := setupTestEtcdStore(t)

	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 10},
		},
		Metadata: Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
		},
	}

	err := ss.Set(record.Key, record)
	assert.Nil(t, err)

	actual, err := ss.Get(record.Key)
	assert.Nil(t, err)
	assert.Equal(t, record.Key, actual.Key)
	assert.Equal(t, record.Data, actual.Data)
	assert.Equal(t, record.Metadata.Stream, actual.Metadata.Stream)
}

func TestEtcdStateStore_GetNonExistentKey_ReturnsError(t *testing.T) {
	ss := setupTestEtcdStore(t)

	record, err := ss.Get("nonexistent-key")
	assert.EqualError(t, err, ErrEtcdKeyNotFound.Error())
	assert.Equal(t, Record{}, record)
}

func TestEtcdStateStore_SetEmptyKey_ReturnsError(t *testing.T) {
	ss := setupTestEtcdStore(t)
	err := ss.Set("", Record{})
	assert.EqualError(t, err, ErrEtcdKeyEmpty.Error())
}

func TestEtcdStateStore_SetWithExistingKey_OverwritesPreviousRecord(t *testing.T) {
	ss := setupTestEtcdStore(t)
	key := "user123"

	// Initial record
	record1 := Record{
		Key: key,
		Data: ValueMap{
			"first_name": String{Val: "Alice"},
			"age":        Integer{Val: 30},
		},
		Metadata: Metadata{
			Stream:    "initial_stream",
			Timestamp: time.Now(),
		},
	}

	// Overwriting record
	record2 := Record{
		Key: key,
		Data: ValueMap{
			"first_name": String{Val: "Bob"},
			"age":        Integer{Val: 45},
		},
		Metadata: Metadata{
			Stream:    "updated_stream",
			Timestamp: time.Now().Add(1 * time.Minute),
		},
	}

	// Set initial record
	err := ss.Set(key, record1)
	assert.Nil(t, err)

	// Overwrite it
	err = ss.Set(key, record2)
	assert.Nil(t, err)

	// Retrieve and validate it's the updated record
	actual, err := ss.Get(key)
	assert.Nil(t, err)

	assert.Equal(t, record2.Key, actual.Key)
	assert.Equal(t, record2.Data, actual.Data)
	assert.Equal(t, record2.Metadata.Stream, actual.Metadata.Stream)
	// TODO: logic for timestamps should be revisited in the future
	// assert.WithinDuration(t, record2.Metadata.Timestamp, actual.Metadata.Timestamp, time.Second)
}
