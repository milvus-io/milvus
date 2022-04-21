// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdkv

import (
	"context"
	"fmt"
	"path"
	"time"

	kvi "github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	clientv3 "go.etcd.io/etcd/client/v3"

	"go.uber.org/zap"
)

const (
	//TODO make etcd configurable
	// RequestTimeout is default timeout for etcd request.
	RequestTimeout = 10 * time.Second
	// retry of etcd
	DefaultRetry = 20
)

// EtcdKV implements TxnKV interface, it supports to process multiple kvs in a transaction.
type EtcdKV struct {
	client   *clientv3.Client
	rootPath string
}

// NewEtcdKV creates a new etcd kv.
func NewEtcdKV(client *clientv3.Client, rootPath string) *EtcdKV {
	kv := &EtcdKV{
		client:   client,
		rootPath: rootPath,
	}
	return kv
}

// Close closes the connection to etcd.
func (kv *EtcdKV) Close() {
	log.Debug("etcd kv closed", zap.String("path", kv.rootPath))
}

// GetPath returns the path of the key.
func (kv *EtcdKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

// LoadWithPrefix returns all the keys and values with the given key prefix.
func (kv *EtcdKV) LoadWithPrefix(key string) ([]string, []string, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}
	CheckElapseAndWarn(start, "Slow etcd operation load with prefix")
	return keys, values, nil
}

// LoadBytesWithPrefix returns all the keys and values with the given key prefix.
func (kv *EtcdKV) LoadBytesWithPrefix(key string) ([]string, [][]byte, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([][]byte, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, kv.Value)
	}
	CheckElapseAndWarn(start, "Slow etcd operation load with prefix")
	return keys, values, nil
}

// LoadWithPrefix2 returns all the the keys,values and key versions with the given key prefix.
func (kv *EtcdKV) LoadWithPrefix2(key string) ([]string, []string, []int64, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
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
	CheckElapseAndWarn(start, "Slow etcd operation load with prefix2")
	return keys, values, versions, nil
}

// LoadBytesWithPrefix2 returns all the the keys,values and key versions with the given key prefix.
func (kv *EtcdKV) LoadBytesWithPrefix2(key string) ([]string, [][]byte, []int64, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return nil, nil, nil, err
	}

	keys := make([]string, 0, resp.Count)
	values := make([][]byte, 0, resp.Count)
	versions := make([]int64, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, kv.Value)
		versions = append(versions, kv.Version)
	}
	CheckElapseAndWarn(start, "Slow etcd operation load with prefix2")
	return keys, values, versions, nil
}

// Load returns value of the key.
func (kv *EtcdKV) Load(key string) (string, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key)
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return "", err
	}

	if resp.Count <= 0 {
		return "", fmt.Errorf("there is no value on key = %s", key)
	}
	CheckElapseAndWarn(start, "Slow etcd operation load")
	return string(resp.Kvs[0].Value), nil
}

// LoadBytes returns value of the key.
func (kv *EtcdKV) LoadBytes(key string) ([]byte, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key)
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return []byte{}, err
	}

	if resp.Count <= 0 {
		return []byte{}, fmt.Errorf("there is no value on key = %s", key)
	}
	CheckElapseAndWarn(start, "Slow etcd operation load")
	return resp.Kvs[0].Value, nil
}

// MultiLoad gets the values of the keys in a transaction.
func (kv *EtcdKV) MultiLoad(keys []string) ([]string, error) {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(keys))
	for _, keyLoad := range keys {
		ops = append(ops, clientv3.OpGet(path.Join(kv.rootPath, keyLoad)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.TxnResponse
	txnFunc := func() error {
		var errTxn error
		resp, errTxn = kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return []string{}, err
	}

	result := make([]string, 0, len(keys))
	invalid := make([]string, 0, len(keys))
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil || len(rp.GetResponseRange().Kvs) == 0 {
			invalid = append(invalid, keys[index])
			result = append(result, "")
		}
		for _, ev := range rp.GetResponseRange().Kvs {
			result = append(result, string(ev.Value))
		}
	}
	if len(invalid) != 0 {
		log.Warn("MultiLoad: there are invalid keys", zap.Strings("keys", invalid))
		err = fmt.Errorf("there are invalid keys: %s", invalid)
		return result, err
	}
	CheckElapseAndWarn(start, "Slow etcd operation multi load")
	return result, nil
}

// MultiLoadBytes gets the values of the keys in a transaction.
func (kv *EtcdKV) MultiLoadBytes(keys []string) ([][]byte, error) {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(keys))
	for _, keyLoad := range keys {
		ops = append(ops, clientv3.OpGet(path.Join(kv.rootPath, keyLoad)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	var resp *clientv3.TxnResponse
	txnFunc := func() error {
		var errTxn error
		resp, errTxn = kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return [][]byte{}, err
	}
	result := make([][]byte, 0, len(keys))
	invalid := make([]string, 0, len(keys))
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil || len(rp.GetResponseRange().Kvs) == 0 {
			invalid = append(invalid, keys[index])
			result = append(result, []byte{})
		}
		for _, ev := range rp.GetResponseRange().Kvs {
			result = append(result, ev.Value)
		}
	}
	if len(invalid) != 0 {
		log.Warn("MultiLoad: there are invalid keys", zap.Strings("keys", invalid))
		err = fmt.Errorf("there are invalid keys: %s", invalid)
		return result, err
	}
	CheckElapseAndWarn(start, "Slow etcd operation multi load")
	return result, nil
}

// LoadWithRevision returns keys, values and revision with given key prefix.
func (kv *EtcdKV) LoadWithRevision(key string) ([]string, []string, int64, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return nil, nil, 0, err
	}

	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}
	CheckElapseAndWarn(start, "Slow etcd operation load with revision")
	return keys, values, resp.Header.Revision, nil
}

// LoadBytesWithRevision returns keys, values and revision with given key prefix.
func (kv *EtcdKV) LoadBytesWithRevision(key string) ([]string, [][]byte, int64, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	var resp *clientv3.GetResponse
	getFunc := func() error {
		var errGet error
		resp, errGet = kv.client.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if errGet != nil {
			return errGet
		}
		return nil
	}

	err := retry.Do(ctx, getFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return nil, nil, 0, err
	}

	keys := make([]string, 0, resp.Count)
	values := make([][]byte, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, kv.Value)
	}
	CheckElapseAndWarn(start, "Slow etcd operation load with revision")
	return keys, values, resp.Header.Revision, nil
}

// Save saves the key-value pair.
func (kv *EtcdKV) Save(key, value string) error {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	putFunc := func() error {
		_, errPut := kv.client.Put(ctx, key, value)
		if errPut != nil {
			return errPut
		}
		return nil
	}

	err := retry.Do(ctx, putFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation save")
	return err
}

// SaveBytes saves the key-value pair.
func (kv *EtcdKV) SaveBytes(key string, value []byte) error {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	putFunc := func() error {
		_, errPut := kv.client.Put(ctx, key, string(value))
		if errPut != nil {
			return errPut
		}
		return nil
	}

	err := retry.Do(ctx, putFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}
	CheckElapseAndWarn(start, "Slow etcd operation save")
	return err
}

// SaveWithLease is a function to put value in etcd with etcd lease options.
func (kv *EtcdKV) SaveWithLease(key, value string, id clientv3.LeaseID) error {
	log.Debug("Etcd saving with lease", zap.String("etcd_key", key))
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	putFunc := func() error {
		_, errPut := kv.client.Put(ctx, key, value, clientv3.WithLease(id))
		if errPut != nil {
			return errPut
		}
		return nil
	}

	err := retry.Do(ctx, putFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}
	CheckElapseAndWarn(start, "Slow etcd operation save with lease")
	return err
}

// SaveWithIgnoreLease updates the key without changing its current lease. Must be used when key already exists.
func (kv *EtcdKV) SaveWithIgnoreLease(key, value string) error {
	log.Debug("Etcd saving with ignore lease", zap.String("etcd_key", key))
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value, clientv3.WithIgnoreLease())
	CheckElapseAndWarn(start, "Slow etcd operation save with lease")
	return err
}

// SaveBytesWithLease is a function to put value in etcd with etcd lease options.
func (kv *EtcdKV) SaveBytesWithLease(key string, value []byte, id clientv3.LeaseID) error {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	putFunc := func() error {
		_, errPut := kv.client.Put(ctx, key, string(value), clientv3.WithLease(id))
		if errPut != nil {
			return errPut
		}
		return nil
	}

	err := retry.Do(ctx, putFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation save with lease")
	return err
}

// MultiSave saves the key-value pairs in a transaction.
func (kv *EtcdKV) MultiSave(kvs map[string]string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(kvs))
	for key, value := range kvs {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errPut := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errPut != nil {
			return errPut
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi save")
	return err
}

// MultiSaveBytes saves the key-value pairs in a transaction.
func (kv *EtcdKV) MultiSaveBytes(kvs map[string][]byte) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(kvs))
	for key, value := range kvs {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), string(value)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errPut := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errPut != nil {
			return errPut
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi save")
	return err
}

// RemoveWithPrefix removes the keys with given prefix.
func (kv *EtcdKV) RemoveWithPrefix(prefix string) error {
	start := time.Now()
	key := path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	delFunc := func() error {
		_, errDel := kv.client.Delete(ctx, key, clientv3.WithPrefix())
		if errDel != nil {
			return errDel
		}
		return nil
	}

	err := retry.Do(ctx, delFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation remove with prefix")
	return err
}

// Remove removes the key.
func (kv *EtcdKV) Remove(key string) error {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	delFunc := func() error {
		_, errDel := kv.client.Delete(ctx, key, clientv3.WithPrefix())
		if errDel != nil {
			return errDel
		}
		return nil
	}

	err := retry.Do(ctx, delFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}
	CheckElapseAndWarn(start, "Slow etcd operation remove")
	return err
}

// MultiRemove removes the keys in a transaction.
func (kv *EtcdKV) MultiRemove(keys []string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, key)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errDel := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errDel != nil {
			return errDel
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi remove")
	return err
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *EtcdKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errTxn := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi save and remove")
	return err
}

// MultiSaveBytesAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *EtcdKV) MultiSaveBytesAndRemove(saves map[string][]byte, removals []string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), string(value)))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete)))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errTxn := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}
	CheckElapseAndWarn(start, "Slow etcd operation multi save and remove")
	return err
}

// Watch starts watching a key, returns a watch channel.
func (kv *EtcdKV) Watch(key string) clientv3.WatchChan {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithCreatedNotify())
	CheckElapseAndWarn(start, "Slow etcd operation watch")
	return rch
}

// WatchWithPrefix starts watching a key with prefix, returns a watch channel.
func (kv *EtcdKV) WatchWithPrefix(key string) clientv3.WatchChan {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	CheckElapseAndWarn(start, "Slow etcd operation watch with prefix")
	return rch
}

// WatchWithRevision starts watching a key with revision, returns a watch channel.
func (kv *EtcdKV) WatchWithRevision(key string, revision int64) clientv3.WatchChan {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(context.Background(), key, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	CheckElapseAndWarn(start, "Slow etcd operation watch with revision")
	return rch
}

// MultiRemoveWithPrefix removes the keys with given prefix.
func (kv *EtcdKV) MultiRemoveWithPrefix(keys []string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(keys))
	for _, k := range keys {
		op := clientv3.OpDelete(path.Join(kv.rootPath, k), clientv3.WithPrefix())
		ops = append(ops, op)
	}
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errTxn := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi remove with prefix")
	return err
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *EtcdKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(saves))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete), clientv3.WithPrefix()))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errTxn := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi save and move with prefix")
	return err
}

// MultiSaveBytesAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *EtcdKV) MultiSaveBytesAndRemoveWithPrefix(saves map[string][]byte, removals []string) error {
	start := time.Now()
	ops := make([]clientv3.Op, 0, len(saves))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), string(value)))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete), clientv3.WithPrefix()))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txnFunc := func() error {
		_, errTxn := kv.client.Txn(ctx).If().Then(ops...).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	CheckElapseAndWarn(start, "Slow etcd operation multi save and move with prefix")
	return err
}

// Grant creates a new lease implemented in etcd grant interface.
func (kv *EtcdKV) Grant(ttl int64) (clientv3.LeaseID, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.LeaseGrantResponse
	grantFunc := func() error {
		var errGrant error
		resp, errGrant = kv.client.Grant(context.Background(), ttl)
		if errGrant != nil {
			return errGrant
		}
		return nil
	}

	err := retry.Do(ctx, grantFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return -1, err
	}

	CheckElapseAndWarn(start, "Slow etcd operation grant")
	return resp.ID, err
}

// KeepAlive keeps the lease alive forever with leaseID.
// Implemented in etcd interface.
func (kv *EtcdKV) KeepAlive(id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var ch <-chan *clientv3.LeaseKeepAliveResponse
	grantFunc := func() error {
		var errKeepAlvie error
		ch, errKeepAlvie = kv.client.KeepAlive(context.Background(), id)
		if errKeepAlvie != nil {
			return errKeepAlvie
		}
		return nil
	}

	err := retry.Do(ctx, grantFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return nil, err
	}

	CheckElapseAndWarn(start, "Slow etcd operation keepAlive")
	return ch, nil
}

// CompareValueAndSwap compares the existing value with compare, and if they are
// equal, the target is stored in etcd.
func (kv *EtcdKV) CompareValueAndSwap(key, value, target string, opts ...clientv3.OpOption) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.TxnResponse
	txnFunc := func() error {
		var errTxn error
		resp, errTxn = kv.client.Txn(ctx).If(
			clientv3.Compare(
				clientv3.Value(path.Join(kv.rootPath, key)),
				"=",
				value)).
			Then(clientv3.OpPut(path.Join(kv.rootPath, key), target, opts...)).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return kvi.NewCompareFailedError(fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", key))
	}
	CheckElapseAndWarn(start, "Slow etcd operation compare value and swap")
	return nil
}

// CompareValueAndSwapBytes compares the existing value with compare, and if they are
// equal, the target is stored in etcd.
func (kv *EtcdKV) CompareValueAndSwapBytes(key string, value, target []byte, opts ...clientv3.OpOption) error {
	return kv.CompareValueAndSwap(key, string(value), string(target), opts...)
}

// CompareVersionAndSwap compares the existing key-value's version with version, and if
// they are equal, the target is stored in etcd.
func (kv *EtcdKV) CompareVersionAndSwap(key string, source int64, target string, opts ...clientv3.OpOption) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	var resp *clientv3.TxnResponse
	txnFunc := func() error {
		var errTxn error
		resp, errTxn = kv.client.Txn(ctx).If(
			clientv3.Compare(
				clientv3.Version(path.Join(kv.rootPath, key)),
				"=",
				source)).
			Then(clientv3.OpPut(path.Join(kv.rootPath, key), target, opts...)).Commit()
		if errTxn != nil {
			return errTxn
		}
		return nil
	}

	err := retry.Do(ctx, txnFunc, retry.Attempts(DefaultRetry))
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return kvi.NewCompareFailedError(fmt.Errorf("function CompareAndSwap error for compare is false for key: %s,"+
			" source version: %d, target version: %s", key, source, target))
	}
	CheckElapseAndWarn(start, "Slow etcd operation compare version and swap")
	return nil
}

// CompareVersionAndSwapBytes compares the existing key-value's version with version, and if
// they are equal, the target is stored in etcd.
func (kv *EtcdKV) CompareVersionAndSwapBytes(key string, source int64, target []byte, opts ...clientv3.OpOption) error {
	return kv.CompareVersionAndSwap(key, source, string(target), opts...)
}

// CheckElapseAndWarn checks the elapsed time and warns if it is too long.
func CheckElapseAndWarn(start time.Time, message string) bool {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > 2000 {
		log.Warn(message, zap.String("time spent", elapsed.String()))
		return true
	}
	return false
}
