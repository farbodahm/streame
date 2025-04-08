package state_store

import (
	"context"
	"fmt"

	"github.com/farbodahm/streame/pkg/messaging"
	"github.com/farbodahm/streame/pkg/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrEtcdKeyNotFound = fmt.Errorf("key not found in etcd")
var ErrEtcdKeyEmpty = fmt.Errorf("key is empty")

// Make sure EtcdStateStore implements StateStore
var _ StateStore = EtcdStateStore{}

// EtcdStateStore implements ETCD as a state store.
type EtcdStateStore struct {
	etcdClient *clientv3.Client
}

func NewEtcdStateStore(etcdClient *clientv3.Client) (*EtcdStateStore, error) {
	return &EtcdStateStore{etcdClient: etcdClient}, nil
}

// Get returns an object from state store
func (ess EtcdStateStore) Get(key string) (types.Record, error) {
	valiueResponse, err := ess.etcdClient.Get(context.Background(), key)
	if err != nil {
		return types.Record{}, err
	}
	if len(valiueResponse.Kvs) == 0 {
		return types.Record{}, ErrEtcdKeyNotFound
	}
	if len(valiueResponse.Kvs) > 1 {
		return types.Record{}, fmt.Errorf("multiple values found for key %s", key)
	}

	value := valiueResponse.Kvs[0].Value

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
func (ess EtcdStateStore) Set(key string, value types.Record) error {
	if key == "" {
		return ErrEtcdKeyEmpty
	}

	data, err := messaging.RecordToProtocolBuffers(value)
	if err != nil {
		return err
	}

	// TODO: accept ctx as a parameter
	_, err = ess.etcdClient.Put(context.Background(), key, string(data))

	return err
}

// Close closes the connection to state store
func (ess EtcdStateStore) Close() error {
	if err := ess.etcdClient.Close(); err != nil {
		return err
	}
	return nil
}
