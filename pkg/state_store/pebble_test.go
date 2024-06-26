package state_store_test

import (
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/farbodahm/streame/pkg/state_store"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

var warehouse_path = "./test"

func TestPebbleStateStore_ValidRecordWithoutMetadata_WriteAndReadToPebbleSuccessfully(t *testing.T) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	assert.Nil(t, err)

	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 10},
		},
	}

	err = ss.Set(record.Key, record)
	assert.Nil(t, err)

	actual, err := ss.Get(record.Key)
	assert.Nil(t, err)
	assert.Equal(t, record, actual)

	err = ss.Close()
	assert.Nil(t, err)
}

func TestPebbleStateStore_GetNonExistentKey_ReturnsError(t *testing.T) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	assert.Nil(t, err)

	_, err = ss.Get("non_existent_key")
	assert.EqualError(t, err, pebble.ErrNotFound.Error())

	err = ss.Close()
	assert.Nil(t, err)
}

func TestPebbleStateStore_SetAndGetMultipleRecords_Successfully(t *testing.T) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	assert.Nil(t, err)

	records := []Record{
		{
			Key: "key1",
			Data: ValueMap{
				"first_name": String{Val: "John"},
				"last_name":  String{Val: "Doe"},
				"age":        Integer{Val: 30},
			},
		},
		{
			Key: "key2",
			Data: ValueMap{
				"first_name": String{Val: "Jane"},
				"last_name":  String{Val: "Doe"},
				"age":        Integer{Val: 25},
			},
		},
	}

	for _, record := range records {
		err = ss.Set(record.Key, record)
		assert.Nil(t, err)
	}

	for _, expected := range records {
		actual, err := ss.Get(expected.Key)
		assert.Nil(t, err)
		assert.Equal(t, expected, actual)
	}

	err = ss.Close()
	assert.Nil(t, err)
}

func TestPebbleStateStore_GetAfterClose_Panics(t *testing.T) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	assert.Nil(t, err)

	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "John"},
			"last_name":  String{Val: "Doe"},
			"age":        Integer{Val: 30},
		},
	}

	err = ss.Set(record.Key, record)
	assert.Nil(t, err)

	err = ss.Close()
	assert.Nil(t, err)

	assert.Panics(t, func() { ss.Get(record.Key) })
}

func TestPebbleStateStore_UpdateExistingRecord_Successfully(t *testing.T) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	assert.Nil(t, err)

	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "John"},
			"last_name":  String{Val: "Doe"},
			"age":        Integer{Val: 30},
		},
	}

	err = ss.Set(record.Key, record)
	assert.Nil(t, err)

	updatedRecord := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "Johnny"},
			"last_name":  String{Val: "Doe"},
			"age":        Integer{Val: 31},
		},
	}

	err = ss.Set(updatedRecord.Key, updatedRecord)
	assert.Nil(t, err)

	actual, err := ss.Get(updatedRecord.Key)
	assert.Nil(t, err)
	assert.Equal(t, updatedRecord, actual)

	err = ss.Close()
	assert.Nil(t, err)
}

func TestPebbleStateStore_ConcurrentAccess_SucceedsWithoutUndefinedBehaviour(t *testing.T) {
	defer os.RemoveAll(warehouse_path)
	ss, err := state_store.NewPebbleStateStore(warehouse_path, &pebble.Options{})
	assert.Nil(t, err)

	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "John"},
			"last_name":  String{Val: "Doe"},
			"age":        Integer{Val: 30},
		},
	}

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := ss.Set(record.Key, record)
			assert.Nil(t, err)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, err := ss.Get(record.Key)
			if err != nil {
				assert.Equal(t, pebble.ErrNotFound, err)
			}
		}
	}()

	wg.Wait()
	err = ss.Close()
	assert.Nil(t, err)
}
