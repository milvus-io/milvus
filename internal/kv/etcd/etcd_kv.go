package etcdkv

import (
	"context"
	"log"
	"path"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"go.etcd.io/etcd/clientv3"
)

const (
	RequestTimeout = 10 * time.Second
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

func (kv *EtcdKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

func (kv *EtcdKV) LoadWithPrefix(key string) ([]string, []string, error) {
	key = path.Join(kv.rootPath, key)
	log.Printf("LoadWithPrefix %s", key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
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
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if resp.Count <= 0 {
		return "", errors.Errorf("there is no value on key = %s", key)
	}

	return string(resp.Kvs[0].Value), nil
}

func (kv *EtcdKV) GetCount(key string) (int64, error) {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return -1, err
	}

	return resp.Count, nil
}

func (kv *EtcdKV) MultiLoad(keys []string) ([]string, error) {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, keyLoad := range keys {
		ops = append(ops, clientv3.OpGet(path.Join(kv.rootPath, keyLoad)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		return []string{}, err
	}

	result := make([]string, 0, len(keys))
	invalid := make([]string, 0, len(keys))
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil {
			invalid = append(invalid, keys[index])
			result = append(result, "")
		}
		for _, ev := range rp.GetResponseRange().Kvs {
			log.Printf("MultiLoad: %s -> %s\n", string(ev.Key), string(ev.Value))
			result = append(result, string(ev.Value))
		}
	}
	if len(invalid) != 0 {
		log.Printf("MultiLoad: there are invalid keys: %s", invalid)
		err = errors.Errorf("there are invalid keys: %s", invalid)
		return result, err
	}
	return result, nil
}

func (kv *EtcdKV) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value)
	return err
}

func (kv *EtcdKV) MultiSave(kvs map[string]string) error {
	ops := make([]clientv3.Op, 0, len(kvs))
	for key, value := range kvs {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

func (kv *EtcdKV) RemoveWithPrefix(prefix string) error {
	key := path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Delete(ctx, key, clientv3.WithPrefix())
	return err
}

func (kv *EtcdKV) Remove(key string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Delete(ctx, key)
	return err
}

func (kv *EtcdKV) MultiRemove(keys []string) error {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, key)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

func (kv *EtcdKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	ops := make([]clientv3.Op, 0, len(saves))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete)))
	}

	log.Printf("MultiSaveAndRemove")
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
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
