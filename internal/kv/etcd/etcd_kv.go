// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package etcdkv

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

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
// func NewEtcdKV(etcdEndpints []string, rootPath string) (*EtcdKV, error)
func NewEtcdKV(etcdEndpoints []string, rootPath string) (*EtcdKV, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	kv := &EtcdKV{
		client:   client,
		rootPath: rootPath,
	}

	return kv, nil
}

func (kv *EtcdKV) Close() {
	kv.client.Close()
}

func (kv *EtcdKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

func (kv *EtcdKV) LoadWithPrefix(key string) ([]string, []string, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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

func (kv *EtcdKV) LoadWithPrefix2(key string) ([]string, []string, []int64, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, nil, nil, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	versions := make([]int64, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
		versions = append(versions, kv.Version)
	}
	return keys, values, versions, nil
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
		return "", fmt.Errorf("there is no value on key = %s", key)
	}

	return string(resp.Kvs[0].Value), nil
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
			log.Debug("MultiLoad", zap.ByteString("key", ev.Key),
				zap.ByteString("value", ev.Value))
			result = append(result, string(ev.Value))
		}
	}
	if len(invalid) != 0 {
		log.Debug("MultiLoad: there are invalid keys",
			zap.Strings("keys", invalid))
		err = fmt.Errorf("there are invalid keys: %s", invalid)
		return result, err
	}
	return result, nil
}

func (kv *EtcdKV) LoadWithRevision(key string) ([]string, []string, int64, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, nil, 0, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}
	return keys, values, resp.Header.Revision, nil
}

func (kv *EtcdKV) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value)
	return err
}

// SaveWithLease is a function to put value in etcd with etcd lease options.
func (kv *EtcdKV) SaveWithLease(key, value string, id clientv3.LeaseID) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value, clientv3.WithLease(id))
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
	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete)))
	}

	log.Debug("MultiSaveAndRemove")
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

func (kv *EtcdKV) Watch(key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithCreatedNotify())
	return rch
}

func (kv *EtcdKV) WatchWithPrefix(key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	return rch
}

func (kv *EtcdKV) WatchWithRevision(key string, revision int64) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	return rch
}

func (kv *EtcdKV) MultiRemoveWithPrefix(keys []string) error {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, k := range keys {
		op := clientv3.OpDelete(path.Join(kv.rootPath, k), clientv3.WithPrefix())
		ops = append(ops, op)
	}
	log.Debug("MultiRemoveWithPrefix")
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

func (kv *EtcdKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	ops := make([]clientv3.Op, 0, len(saves))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete), clientv3.WithPrefix()))
	}

	log.Debug("MultiSaveAndRemove")
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

// Grant creates a new lease implemented in etcd grant interface.
func (kv *EtcdKV) Grant(ttl int64) (id clientv3.LeaseID, err error) {
	resp, err := kv.client.Grant(context.Background(), ttl)
	return resp.ID, err
}

// KeepAlive keeps the lease alive forever with leaseID.
// Implemented in etcd interface.
func (kv *EtcdKV) KeepAlive(id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch, err := kv.client.KeepAlive(context.Background(), id)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// CompareValueAndSwap compares the existing value with compare, and if they are
// equal, the target is stored in etcd.
func (kv *EtcdKV) CompareValueAndSwap(key, value, target string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If(
		clientv3.Compare(
			clientv3.Value(path.Join(kv.rootPath, key)),
			"=",
			value)).
		Then(clientv3.OpPut(path.Join(kv.rootPath, key), target, opts...)).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", key)
	}

	return nil
}

// CompareVersionAndSwap compares the existing key-value's version with version, and if
// they are equal, the target is stored in etcd.
func (kv *EtcdKV) CompareVersionAndSwap(key string, version int64, target string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(kv.rootPath, key)),
			"=",
			version)).
		Then(clientv3.OpPut(path.Join(kv.rootPath, key), target, opts...)).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", key)
	}

	return nil
}
