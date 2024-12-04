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
// limitations under the License.
// See the License for the specific language governing permissions and

package etcdkv

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/kv/predicates"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// implementation assertion
var _ kv.MetaKv = (*EmbedEtcdKV)(nil)

const (
	defaultRetryCount    = 3
	defaultRetryInterval = 1 * time.Second
)

// EmbedEtcdKV use embedded Etcd instance as a KV storage
type EmbedEtcdKV struct {
	client    *clientv3.Client
	rootPath  string
	etcd      *embed.Etcd
	closeOnce sync.Once

	requestTimeout time.Duration
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	for i := 0; ; i++ {
		err := fn()
		if err == nil || i >= (attempts-1) {
			return err
		}
		time.Sleep(sleep)
	}
}

// NewEmbededEtcdKV creates a new etcd kv.
func NewEmbededEtcdKV(cfg *embed.Config, rootPath string, options ...Option) (*EmbedEtcdKV, error) {
	var e *embed.Etcd
	var err error

	err = retry(defaultRetryCount, defaultRetryInterval, func() error {
		e, err = embed.StartEtcd(cfg)
		return err
	})

	if err != nil {
		return nil, err
	}

	client := v3client.New(e.Server)

	opt := defaultOption()
	for _, option := range options {
		option(opt)
	}

	kv := &EmbedEtcdKV{
		client:   client,
		rootPath: rootPath,
		etcd:     e,

		requestTimeout: opt.requestTimeout,
	}
	// wait until embed etcd is ready with retry mechanism
	err = retry(defaultRetryCount, defaultRetryInterval, func() error {
		select {
		case <-e.Server.ReadyNotify():
			log.Info("Embedded etcd is ready!")
			return nil
		case <-time.After(60 * time.Second):
			e.Server.Stop() // trigger a shutdown
			return errors.New("Embedded etcd took too long to start")
		}
	})

	if err != nil {
		return nil, err
	}

	return kv, nil
}

// Close closes the embedded etcd
func (kv *EmbedEtcdKV) Close() {
	kv.closeOnce.Do(func() {
		kv.client.Close()
		kv.etcd.Close()
	})
}

// GetPath returns the full path by given key
func (kv *EmbedEtcdKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

func (kv *EmbedEtcdKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	batch := int64(paginationSize)
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(batch),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
	}

	key := prefix
	for {
		resp, err := kv.client.Get(ctx, key, opts...)
		if err != nil {
			return err
		}

		for _, kv := range resp.Kvs {
			if err = fn(kv.Key, kv.Value); err != nil {
				return err
			}
		}

		if !resp.More {
			break
		}
		// move to next key
		key = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}
	return nil
}

// LoadWithPrefix returns all the keys and values with the given key prefix
func (kv *EmbedEtcdKV) LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
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

func (kv *EmbedEtcdKV) Has(ctx context.Context, key string) (bool, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("Has", zap.String("key", key))
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithCountOnly())
	if err != nil {
		return false, err
	}
	return resp.Count != 0, nil
}

func (kv *EmbedEtcdKV) HasPrefix(ctx context.Context, prefix string) (bool, error) {
	prefix = path.Join(kv.rootPath, prefix)
	log.Debug("HasPrefix", zap.String("prefix", prefix))

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	resp, err := kv.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithLimit(1))
	if err != nil {
		return false, err
	}

	return resp.Count != 0, nil
}

// LoadBytesWithPrefix returns all the keys and values with the given key prefix
func (kv *EmbedEtcdKV) LoadBytesWithPrefix(ctx context.Context, key string) ([]string, [][]byte, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadBytesWithPrefix ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([][]byte, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, kv.Value)
	}
	return keys, values, nil
}

// LoadBytesWithPrefix2 returns all the keys and values with versions by the given key prefix
func (kv *EmbedEtcdKV) LoadBytesWithPrefix2(ctx context.Context, key string) ([]string, [][]byte, []int64, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadBytesWithPrefix2 ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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
	return keys, values, versions, nil
}

// Load returns value of the given key
func (kv *EmbedEtcdKV) Load(ctx context.Context, key string) (string, error) {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if resp.Count <= 0 {
		return "", merr.WrapErrIoKeyNotFound(key)
	}

	return string(resp.Kvs[0].Value), nil
}

// LoadBytes returns value of the given key
func (kv *EmbedEtcdKV) LoadBytes(ctx context.Context, key string) ([]byte, error) {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if resp.Count <= 0 {
		return nil, merr.WrapErrIoKeyNotFound(key)
	}

	return resp.Kvs[0].Value, nil
}

// MultiLoad returns values of a set of keys
func (kv *EmbedEtcdKV) MultiLoad(ctx context.Context, keys []string) ([]string, error) {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, keyLoad := range keys {
		ops = append(ops, clientv3.OpGet(path.Join(kv.rootPath, keyLoad)))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(keys))
	invalid := make([]string, 0, len(keys))
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil || len(rp.GetResponseRange().Kvs) == 0 {
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

// MultiLoadBytes returns values of a set of keys
func (kv *EmbedEtcdKV) MultiLoadBytes(ctx context.Context, keys []string) ([][]byte, error) {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, keyLoad := range keys {
		ops = append(ops, clientv3.OpGet(path.Join(kv.rootPath, keyLoad)))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0, len(keys))
	invalid := make([]string, 0, len(keys))
	for index, rp := range resp.Responses {
		if rp.GetResponseRange().Kvs == nil || len(rp.GetResponseRange().Kvs) == 0 {
			invalid = append(invalid, keys[index])
			result = append(result, []byte{})
		}
		for _, ev := range rp.GetResponseRange().Kvs {
			log.Debug("MultiLoadBytes", zap.ByteString("key", ev.Key),
				zap.ByteString("value", ev.Value))
			result = append(result, ev.Value)
		}
	}
	if len(invalid) != 0 {
		log.Debug("MultiLoadBytes: there are invalid keys",
			zap.Strings("keys", invalid))
		err = fmt.Errorf("there are invalid keys: %s", invalid)
		return result, err
	}
	return result, nil
}

// LoadBytesWithRevision returns keys, values and revision with given key prefix.
func (kv *EmbedEtcdKV) LoadBytesWithRevision(ctx context.Context, key string) ([]string, [][]byte, int64, error) {
	key = path.Join(kv.rootPath, key)
	log.Debug("LoadBytesWithRevision ", zap.String("prefix", key))
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Get(ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, nil, 0, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([][]byte, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, kv.Value)
	}
	return keys, values, resp.Header.Revision, nil
}

// Save saves the key-value pair.
func (kv *EmbedEtcdKV) Save(ctx context.Context, key, value string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, value)
	return err
}

// SaveBytes saves the key-value pair.
func (kv *EmbedEtcdKV) SaveBytes(ctx context.Context, key string, value []byte) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, string(value))
	return err
}

// SaveBytesWithLease is a function to put value in etcd with etcd lease options.
func (kv *EmbedEtcdKV) SaveBytesWithLease(ctx context.Context, key string, value []byte, id clientv3.LeaseID) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	_, err := kv.client.Put(ctx, key, string(value), clientv3.WithLease(id))
	return err
}

// MultiSave saves the key-value pairs in a transaction.
func (kv *EmbedEtcdKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	ops := make([]clientv3.Op, 0, len(kvs))
	for key, value := range kvs {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

// MultiSaveBytes saves the key-value pairs in a transaction.
func (kv *EmbedEtcdKV) MultiSaveBytes(ctx context.Context, kvs map[string][]byte) error {
	ops := make([]clientv3.Op, 0, len(kvs))
	for key, value := range kvs {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), string(value)))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

// RemoveWithPrefix removes the keys with given prefix.
func (kv *EmbedEtcdKV) RemoveWithPrefix(ctx context.Context, prefix string) error {
	key := path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Delete(ctx, key, clientv3.WithPrefix())
	return err
}

// Remove removes the key.
func (kv *EmbedEtcdKV) Remove(ctx context.Context, key string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Delete(ctx, key)
	return err
}

// MultiRemove removes the keys in a transaction.
func (kv *EmbedEtcdKV) MultiRemove(ctx context.Context, keys []string) error {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, key := range keys {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, key)))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *EmbedEtcdKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	cmps, err := parsePredicates(kv.rootPath, preds...)
	if err != nil {
		return err
	}

	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete)))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	resp, err := kv.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return merr.WrapErrIoFailedReason("failed to execute transaction")
	}
	return nil
}

// MultiSaveBytesAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *EmbedEtcdKV) MultiSaveBytesAndRemove(ctx context.Context, saves map[string][]byte, removals []string) error {
	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), string(value)))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete)))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

func (kv *EmbedEtcdKV) Watch(ctx context.Context, key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(ctx, key, clientv3.WithCreatedNotify())
	return rch
}

func (kv *EmbedEtcdKV) WatchWithPrefix(ctx context.Context, key string) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	return rch
}

func (kv *EmbedEtcdKV) WatchWithRevision(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	key = path.Join(kv.rootPath, key)
	rch := kv.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	return rch
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *EmbedEtcdKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	cmps, err := parsePredicates(kv.rootPath, preds...)
	if err != nil {
		return err
	}

	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), value))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete), clientv3.WithPrefix()))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	resp, err := kv.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return merr.WrapErrIoFailedReason("failed to execute transaction")
	}
	return nil
}

// MultiSaveBytesAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *EmbedEtcdKV) MultiSaveBytesAndRemoveWithPrefix(ctx context.Context, saves map[string][]byte, removals []string) error {
	ops := make([]clientv3.Op, 0, len(saves)+len(removals))
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(kv.rootPath, key), string(value)))
	}

	for _, keyDelete := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(kv.rootPath, keyDelete), clientv3.WithPrefix()))
	}

	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

// CompareVersionAndSwap compares the existing key-value's version with version, and if
// they are equal, the target is stored in etcd.
func (kv *EmbedEtcdKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(kv.rootPath, key)),
			"=",
			version)).
		Then(clientv3.OpPut(path.Join(kv.rootPath, key), target)).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

// CompareVersionAndSwapBytes compares the existing key-value's version with version, and if
// they are equal, the target is stored in etcd.
func (kv *EmbedEtcdKV) CompareVersionAndSwapBytes(ctx context.Context, key string, version int64, target []byte, opts ...clientv3.OpOption) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()
	resp, err := kv.client.Txn(ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(kv.rootPath, key)),
			"=",
			version)).
		Then(clientv3.OpPut(path.Join(kv.rootPath, key), string(target), opts...)).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

func (kv *EmbedEtcdKV) GetConfig() embed.Config {
	return kv.etcd.Config()
}
