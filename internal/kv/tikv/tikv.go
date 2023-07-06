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

package tikv

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"

	//"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

const (
	// RequestTimeout is default timeout for tikv request.
	RequestTimeout = 10 * time.Second
)

// implementation assertion
// var _ kv.MetaKv = (*tiKV)(nil)

// tiKV implements TxnKV interface, it supports to process multiple kvs in a transaction.
type tiKV struct {
	txn      *txnkv.Client
	rootPath string
}

// NewTiKV creates a new tikv kv.
func NewTiKV(txn *txnkv.Client, rootPath string) *tiKV {
	kv := &tiKV{
		txn:      txn,
		rootPath: rootPath,
	}
	return kv
}

// Close closes the connection to TiKV.
func (kv *tiKV) Close() {
	log.Debug("tiKV closed", zap.String("path", kv.rootPath))
}

// GetPath returns the path of the key.
func (kv *tiKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

// Load returns value of the key.
func (kv *tiKV) Load(key string) (string, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	val, err := kv.getTiKVMeta(ctx, key)
	if len(val) == 0 {
		return "", common.NewKeyNotExistError(key)
	}
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to read key %s", key))
	}
	CheckElapseAndWarn(start, "Slow tikv operation load", zap.String("key", key))
	return val, nil
}

// MultiLoad gets the values of the keys in a transaction.
func (kv *tiKV) MultiLoad(keys []string) ([]string, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Failed to build transaction for MultiLoad"))
	}

	var values []string
	invalid := make([]string, 0, len(keys))
	for _, key := range keys {
		key = path.Join(kv.rootPath, key)
		value, err := txn.Get(ctx, []byte(key))
		if err != nil {
			invalid = append(invalid, key)
		}
		values = append(values, string(value))
	}
	err = txn.Commit(context.Background())

	if len(invalid) != 0 {
		log.Warn("MultiLoad: there are invalid keys", zap.Strings("keys", invalid))
		err = fmt.Errorf("there are invalid keys: %s", invalid)
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi load", zap.Any("keys", keys))
	return values, err
}

// LoadWithPrefix returns all the keys and values with the given key prefix.
func (kv *tiKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	var keys []string
	var values []string

	// Iterate over the key-value pairs
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
		err = iter.Next()
		if err != nil {
			return nil, nil, err
		}
	}
	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv LoadWithPrefix error", zap.String("prefix", prefix), zap.Error(err))
	}
	CheckElapseAndWarn(start, "Slow load with prefix", zap.String("prefix", prefix))
	return keys, values, err
}

// Save saves the key-value pair.
func (kv *tiKV) Save(key, value string) error {
	start := time.Now()
	rs := timerecord.NewTimeRecorder("putTiKVMeta")
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()
	CheckValueSizeAndWarn(key, value)

	elapsed := rs.ElapseSpan()
	err := kv.putTiKVMeta(ctx, key, value)

	metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.MetaKvSize.WithLabelValues(metrics.MetaPutLabel).Observe(float64(len(value)))
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaPutLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.FailLabel).Inc()
	}
	CheckElapseAndWarn(start, "Slow tikv operation save", zap.String("key", key))

	return err
}

// MultiSave saves the key-value pairs in a transaction.
func (kv *tiKV) MultiSave(kvs map[string]string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	if err != nil {
		return err
	}

	for key, value := range kvs {
		key = path.Join(kv.rootPath, key)
		fmt.Println("debug 1", key, value)
		err = txn.Set([]byte(key), []byte(value))
		if err != nil {
			return err
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv MultiSave error", zap.Any("kvs", kvs), zap.Int("len", len(kvs)), zap.Error(err))
	}

	CheckElapseAndWarn(start, "Slow tikv operation multi save", zap.Any("kvs", kvs))
	return err
}

// Remove removes the key.
func (kv *tiKV) Remove(key string) error {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	rs := timerecord.NewTimeRecorder("removeTiKVMeta")
	err := kv.removeTiKVMeta(ctx, key)
	elapsed := rs.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.TotalLabel).Inc()

	if err == nil {
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaRemoveLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.FailLabel).Inc()
	}

	CheckElapseAndWarn(start, "Slow tikv operation remove", zap.String("key", key))
	return err
}

// MultiRemove removes the keys in a transaction.
func (kv *tiKV) MultiRemove(keys []string) error {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	if err != nil {
		return err
	}

	for _, key := range keys {
		key = path.Join(kv.rootPath, key)
		err := txn.Delete([]byte(key))
		if err != nil {
			txn.Rollback()
			return err
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv MultiRemove error", zap.Strings("keys", keys), zap.Int("len", len(keys)), zap.Error(err))
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi remove", zap.Strings("keys", keys))
	return err
}

// RemoveWithPrefix removes the keys with given prefix.
func (kv *tiKV) RemoveWithPrefix(prefix string) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey(startKey)
	_, err := kv.txn.DeleteRange(ctx, startKey, endKey, 1)
	CheckElapseAndWarn(start, "Slow tikv operation remove with prefix", zap.String("prefix", prefix))
	return err
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *tiKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	if err != nil {
		return err
	}

	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		if err := txn.Set([]byte(key), []byte(value)); err != nil {
			txn.Rollback()
			return err
		}
	}

	for _, key := range removals {
		key = path.Join(kv.rootPath, key)
		if err := txn.Delete([]byte(key)); err != nil {
			txn.Rollback()
			return err
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv MultiSaveAndRemove error",
			zap.Any("saves", saves),
			zap.Strings("removes", removals),
			zap.Int("saveLength", len(saves)),
			zap.Int("removeLength", len(removals)),
			zap.Error(err))
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi save and remove", zap.Any("saves", saves), zap.Strings("removals", removals))
	return err
}

// MultiRemoveWithPrefix removes the keys with given prefix.
func (kv *tiKV) MultiRemoveWithPrefix(prefixes []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	if err != nil {
		return err
	}

	for _, prefix := range prefixes {
		prefix = path.Join(kv.rootPath, prefix)
		// Get the start and end keys for the prefix range
		startKey := []byte(prefix)
		endKey := tikv.PrefixNextKey([]byte(prefix))

		// Use Scan to iterate over keys in the prefix range
		iter, err := txn.Iter(startKey, endKey)
		if err != nil {
			return err
		}

		// Iterate over keys and delete them
		for iter.Valid() {
			key := iter.Key()
			err := txn.Delete(key)
			if err != nil {
				return err
			}

			// Move the iterator to the next key
			err = iter.Next()
			if err != nil {
				return err
			}
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv MultiRemoveWithPrefix error", zap.Strings("keys", prefixes), zap.Int("len", len(prefixes)), zap.Error(err))
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi remove with prefix", zap.Strings("keys", prefixes))
	return err
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *tiKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	if err != nil {
		return err
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer func() {
		if err != nil {
			txn.Rollback()
		}
	}()

	// Save key-value pairs
	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		err = txn.Set([]byte(key), []byte(value))
		if err != nil {
			return err
		}
	}

	// Remove keys with prefix
	for _, prefix := range removals {
		prefix = path.Join(kv.rootPath, prefix)
		// Get the start and end keys for the prefix range
		startKey := []byte(prefix)
		endKey := tikv.PrefixNextKey([]byte(prefix))

		// Use Scan to iterate over keys in the prefix range
		iter, err := txn.Iter(startKey, endKey)
		if err != nil {
			return err
		}

		// Iterate over keys and delete them
		for iter.Valid() {
			key := iter.Key()
			err := txn.Delete(key)
			if err != nil {
				return err
			}

			// Move the iterator to the next key
			err = iter.Next()
			if err != nil {
				return err
			}
		}
	}

	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv MultiSaveAndRemoveWithPrefix error",
			zap.Any("saves", saves),
			zap.Strings("removes", removals),
			zap.Int("saveLength", len(saves)),
			zap.Int("removeLength", len(removals)),
			zap.Error(err))
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi save and move with prefix", zap.Any("saves", saves), zap.Strings("removals", removals))
	return err
}

func (kv *tiKV) WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.TODO(), RequestTimeout)
	defer cancel()

	txn, err := kv.txn.Begin()
	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		return err
	}
	defer iter.Close()

	// Iterate over the key-value pairs
	for iter.Valid() {
		err = fn(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		err = iter.Next()
		if err != nil {
			return err
		}
	}
	err = txn.Commit(ctx)
	if err != nil {
		log.Warn("TiKv WalkWithPagination error", zap.String("prefix", prefix), zap.Error(err))
	}

	CheckElapseAndWarn(start, "Slow tikv operation(WalkWithPagination)", zap.String("prefix", prefix))
	return nil
}

// CheckElapseAndWarn checks the elapsed time and warns if it is too long.
func CheckElapseAndWarn(start time.Time, message string, fields ...zap.Field) bool {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > 2000 {
		log.Warn(message, append([]zap.Field{zap.String("time spent", elapsed.String())}, fields...)...)
		return true
	}
	return false
}

func CheckValueSizeAndWarn(key string, value interface{}) bool {
	size := binary.Size(value)
	if size > 102400 {
		log.Warn("value size large than 100kb", zap.String("key", key), zap.Int("value_size(kb)", size/1024))
		return true
	}
	return false
}

func CheckTnxBytesValueSizeAndWarn(kvs map[string][]byte) bool {
	var hasWarn bool
	for key, value := range kvs {
		if CheckValueSizeAndWarn(key, value) {
			hasWarn = true
		}
	}
	return hasWarn
}

func CheckTnxStringValueSizeAndWarn(kvs map[string]string) bool {
	newKvs := make(map[string][]byte, len(kvs))
	for key, value := range kvs {
		newKvs[key] = []byte(value)
	}

	return CheckTnxBytesValueSizeAndWarn(newKvs)
}

/*
func (kv *tiKV) executeTxn(tx tikv.Pipeliner, ctx context.Context) error {
	start := timerecord.NewTimeRecorder("executeTxn")

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.TotalLabel).Inc()
	_, err := tx.Exec(ctx)
	if err == nil {
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaTxnLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.FailLabel).Inc()
	}

	return err
}
*/

func (kv *tiKV) getTiKVMeta(ctx context.Context, key string) (string, error) {
	ctx1, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("getTiKVMeta")

	txn, err := kv.txn.Begin()
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to build transaction for getTiKVMeta"))
	}
	val, err := txn.Get(ctx1, []byte(key))
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in getTiKVMeta", key))
	}
	err = txn.Commit(ctx)

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.TotalLabel).Inc()

	// cal meta kv size
	if err == nil {
		totalSize := binary.Size(val)
		metrics.MetaKvSize.WithLabelValues(metrics.MetaGetLabel).Observe(float64(totalSize))
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaGetLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.FailLabel).Inc()
	}
	return string(val), err
}

func (kv *tiKV) putTiKVMeta(ctx context.Context, key, val string) error {
	ctx1, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("putTiKVMeta")

	txn, err := kv.txn.Begin()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to build transaction for putTiKVMeta"))
	}
	err = txn.Set([]byte(key), []byte(val))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in putTiKVMeta", key))
	}
	err = txn.Commit(ctx1)

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.MetaKvSize.WithLabelValues(metrics.MetaPutLabel).Observe(float64(len(val)))
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaPutLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.FailLabel).Inc()
	}

	return err
}

func (kv *tiKV) removeTiKVMeta(ctx context.Context, key string) error {
	ctx1, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("removeTiKVMeta")

	txn, err := kv.txn.Begin()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to build transaction for putTiKVMeta"))
	}
	err = txn.Delete([]byte(key))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in putTiKVMeta", key))
	}
	err = txn.Commit(ctx1)

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.TotalLabel).Inc()

	if err == nil {
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaRemoveLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.FailLabel).Inc()
	}

	return err
}
