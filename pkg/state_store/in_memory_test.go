package state_store_test

import (
	"sync"
	"testing"
	"time"

	. "github.com/farbodahm/streame/pkg/state_store"
	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestInMemorySS_ValidRecord_WriteAndReadSuccessfully(t *testing.T) {
	ss := NewInMemorySS()

	record := types.Record{
		Key: "key1",
		Data: types.ValueMap{
			"first_name": types.String{Val: "foobar"},
			"last_name":  types.String{Val: "random_lastname"},
			"age":        types.Integer{Val: 10},
		},
		Metadata: types.Metadata{
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
	assert.Equal(t, record.Metadata.Timestamp.Unix(), actual.Metadata.Timestamp.Unix())

	err = ss.Close()
	assert.Nil(t, err)
}

func TestInMemorySS_GetNonExistentKey_ReturnsError(t *testing.T) {
	ss := NewInMemorySS()

	_, err := ss.Get("non_existent_key")
	assert.EqualError(t, err, "key not found")

	err = ss.Close()
	assert.Nil(t, err)
}

func TestInMemorySS_SetAndGetMultipleRecords_Successfully(t *testing.T) {
	ss := NewInMemorySS()

	records := []types.Record{
		{
			Key: "key1",
			Data: types.ValueMap{
				"first_name": types.String{Val: "John"},
				"last_name":  types.String{Val: "Doe"},
				"age":        types.Integer{Val: 30},
			},
			Metadata: types.Metadata{
				Stream:    "test_stream1",
				Timestamp: time.Now(),
			},
		},
		{
			Key: "key2",
			Data: types.ValueMap{
				"first_name": types.String{Val: "Jane"},
				"last_name":  types.String{Val: "Doe"},
				"age":        types.Integer{Val: 25},
			},
			Metadata: types.Metadata{
				Stream:    "test_stream2",
				Timestamp: time.Now(),
			},
		},
	}

	for _, record := range records {
		err := ss.Set(record.Key, record)
		assert.Nil(t, err)
	}

	for _, expected := range records {
		actual, err := ss.Get(expected.Key)
		assert.Nil(t, err)
		assert.Equal(t, expected.Key, actual.Key)
		assert.Equal(t, expected.Data, actual.Data)
		assert.Equal(t, expected.Metadata.Stream, actual.Metadata.Stream)
		assert.Equal(t, expected.Metadata.Timestamp.Unix(), actual.Metadata.Timestamp.Unix())
	}

	err := ss.Close()
	assert.Nil(t, err)
}

func TestInMemorySS_CloseStateStore_CloseEmptiesStore(t *testing.T) {
	ss := NewInMemorySS()

	record := types.Record{
		Key: "key1",
		Data: types.ValueMap{
			"first_name": types.String{Val: "John"},
			"last_name":  types.String{Val: "Doe"},
			"age":        types.Integer{Val: 30},
		},
		Metadata: types.Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
		},
	}

	err := ss.Set(record.Key, record)
	assert.Nil(t, err)

	err = ss.Close()
	assert.Nil(t, err)

	_, err = ss.Get(record.Key)
	assert.EqualError(t, err, "key not found")
}

func TestInMemorySS_UpdateExistingRecord_Successfully(t *testing.T) {
	ss := NewInMemorySS()

	record := types.Record{
		Key: "key1",
		Data: types.ValueMap{
			"first_name": types.String{Val: "John"},
			"last_name":  types.String{Val: "Doe"},
			"age":        types.Integer{Val: 30},
		},
		Metadata: types.Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
		},
	}

	err := ss.Set(record.Key, record)
	assert.Nil(t, err)

	updatedRecord := types.Record{
		Key: "key1",
		Data: types.ValueMap{
			"first_name": types.String{Val: "Johnny"},
			"last_name":  types.String{Val: "Doe"},
			"age":        types.Integer{Val: 31},
		},
		Metadata: types.Metadata{
			Stream:    "updated_stream",
			Timestamp: time.Now(),
		},
	}

	err = ss.Set(updatedRecord.Key, updatedRecord)
	assert.Nil(t, err)

	actual, err := ss.Get(updatedRecord.Key)
	assert.Nil(t, err)
	assert.Equal(t, updatedRecord.Key, actual.Key)
	assert.Equal(t, updatedRecord.Data, actual.Data)
	assert.Equal(t, updatedRecord.Metadata.Stream, actual.Metadata.Stream)
	assert.Equal(t, updatedRecord.Metadata.Timestamp.Unix(), actual.Metadata.Timestamp.Unix())

	err = ss.Close()
	assert.Nil(t, err)
}

func TestInMemorySS_ConcurrentAccess_SucceedsWithoutUndefinedBehavior(t *testing.T) {
	ss := NewInMemorySS()

	record := types.Record{
		Key: "key1",
		Data: types.ValueMap{
			"first_name": types.String{Val: "John"},
			"last_name":  types.String{Val: "Doe"},
			"age":        types.Integer{Val: 30},
		},
		Metadata: types.Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
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
				assert.EqualError(t, err, "key not found")
			}
		}
	}()

	wg.Wait()
	err := ss.Close()
	assert.Nil(t, err)
}
