package state_store_test

import (
	"os"
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
