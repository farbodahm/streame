//go:build integration
// +build integration

package state_store

import (
	"log"
	"testing"
	"time"

	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdStateStore_ValidRecord_WriteAndReadToEtcdSuccessfully(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.Nil(t, err)
	defer cli.Close()

	ss, err := NewEtcdStateStore(cli)
	assert.Nil(t, err)

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

	err = ss.Set(record.Key, record)
	assert.Nil(t, err)

	actual, err := ss.Get(record.Key)
	log.Println(actual)
	assert.Nil(t, err)
	assert.Equal(t, record.Key, actual.Key)
	assert.Equal(t, record.Data, actual.Data)
	assert.Equal(t, record.Metadata.Stream, actual.Metadata.Stream)
}
