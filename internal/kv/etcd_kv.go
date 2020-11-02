package kv

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"go.etcd.io/etcd/clientv3"
	"log"
	"path"
	"time"
)

const (
	requestTimeout = 10 * time.Second
)

type EtcdKV struct {
	client   *clientv3.Client
	rootPath string
}

// NewEtcdKV creates a new etcd kv.
func NewEtcdKV(client *clientv3.Client, rootPath string) *EtcdKV {
	return &EtcdKV{
		client:   client,
		rootPath: rootPath,
	}
}

func (kv *EtcdKV) Close() {
	kv.client.Close()
}

func (kv *EtcdKV) LoadWithPrefix(key string) ([]string, []string, error) {
	key = path.Join(kv.rootPath, key)
	log.Printf("LoadWithPrefix %s", key)
	ctx, _ := context.WithTimeout(context.TODO(), requestTimeout)
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}
	return keys, values, nil
}

func (kv *EtcdKV) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)
	ctx, _ := context.WithTimeout(context.TODO(), requestTimeout)
	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if resp.Count <= 0 {
		return "", errors.Errorf("there is no value on key = %s", key)
	}

	return string(resp.Kvs[0].Value), nil
}

func (kv *EtcdKV) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)
	ctx, _ := context.WithTimeout(context.TODO(), requestTimeout)
	_, err := kv.client.Put(ctx, key, value)
	return err
}

func (kv *EtcdKV) Remove(key string) error {
	key = path.Join(kv.rootPath, key)
	ctx, _ := context.WithTimeout(context.TODO(), requestTimeout)
	_, err := kv.client.Delete(ctx, key)
	return err
}

func (kv *EtcdKV) Watch(key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key)
	return rch
}

func (kv *EtcdKV) WatchWithPrefix(key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithPrefix())
	return rch
}
