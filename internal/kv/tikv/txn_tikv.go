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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

const (
	// RequestTimeout is the default timeout for tikv request.
	RequestTimeout = 10 * time.Second
)

func tiTxnBegin(txn *txnkv.Client) (*transaction.KVTxn, error) {
	return txn.Begin()
}

func tiTxnCommit(txn *transaction.KVTxn, ctx context.Context) error {
	return txn.Commit(ctx)
}

var beginTxn = tiTxnBegin
var commitTxn = tiTxnCommit

// implementation assertion
var _ kv.MetaKv = (*txnTiKV)(nil)

// txnTiKV implements MetaKv and TxnKV interface. It supports processing multiple kvs within one transaction.
type txnTiKV struct {
	txn      *txnkv.Client
	rootPath string
}

// NewTiKV creates a new txnTiKV client.
func NewTiKV(txn *txnkv.Client, rootPath string) *txnTiKV {
	kv := &txnTiKV{
		txn:      txn,
		rootPath: rootPath,
	}
	return kv
}

// Close closes the connection to TiKV.
func (kv *txnTiKV) Close() {
	log.Debug("txnTiKV closed", zap.String("path", kv.rootPath))
}

// GetPath returns the path of the key/prefix.
func (kv *txnTiKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

func logWarnOnFailure(err error, msg string, fields ...zap.Field) {
	if err != nil {
		log.Warn(msg, fields...)
	}
}

// Has returns if a key exists.
func (kv *txnTiKV) Has(key string) (bool, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()
	_, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		if strings.HasPrefix(err.Error(), "Failed to get value for key") {
			return false, nil
		} else {
			return false, errors.Wrap(err, fmt.Sprintf("Failed to create txn for Has of key %s", key))
		}
	}
	CheckElapseAndWarn(start, "Slow tikv operation Has()", zap.String("key", key))
	return true, nil
}

func rollbackOnFailure(err error, txn *transaction.KVTxn) {
	if err != nil {
		txn.Rollback()
	}
}

// HasPrefix returns if a key prefix exists.
func (kv *txnTiKV) HasPrefix(prefix string) (bool, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV HasPrefix error", zap.String("prefix", prefix), zap.Error(err))

	var txn *transaction.KVTxn
	txn, err = beginTxn(kv.txn)
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("Failed to create txn for HasPrefix of prefix %s", prefix))
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("Failed to iterate for HasPrefix of prefix %s", prefix))
	}
	defer iter.Close()

	r := false
	// Iterater only needs to check the first key-value pair
	if iter.Valid() {
		r = true
	}
	if err = kv.executeTxn(txn, ctx); err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("Failed to commit txn for HasPrefix of prefix %s", prefix))
	}
	CheckElapseAndWarn(start, "Slow load with HasPrefix", zap.String("prefix", prefix))
	return r, nil
}

// Load returns value of the key.
func (kv *txnTiKV) Load(key string) (string, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV Load error", zap.String("key", key), zap.Error(err))

	var val string
	val, err = kv.getTiKVMeta(ctx, key)
	if len(val) == 0 {
		return "", common.NewKeyNotExistError(key)
	}
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to read key %s", key))
	}
	CheckElapseAndWarn(start, "Slow tikv operation load", zap.String("key", key))
	return val, nil
}

// MultiLoad gets the values of input keys in a transaction.
func (kv *txnTiKV) MultiLoad(keys []string) ([]string, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiLoad error", zap.Strings("keys", keys), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to build transaction for MultiLoad")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

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
	err = kv.executeTxn(txn, ctx)

	if len(invalid) != 0 {
		err = fmt.Errorf("there are invalid keys: %s", invalid)
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi load", zap.Any("keys", keys))
	return values, err
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *txnTiKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV LoadWithPrefix error", zap.String("prefix", prefix), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to create txn for LoadWithPrefix %s", prefix))
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to create iterater for LoadWithPrefix %s", prefix))
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
			return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to iterate for LoadWithPrefix %s", prefix))
		}
	}
	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to commit txn for LoadWithPrefix %s", prefix))
	}
	CheckElapseAndWarn(start, "Slow LoadWithPrefix", zap.String("prefix", prefix))
	return keys, values, nil
}

// Save saves the input key-value pair.
func (kv *txnTiKV) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV Save error", zap.String("key", key), zap.String("value", value), zap.Error(err))

	err = kv.putTiKVMeta(ctx, key, value)
	return err
}

// MultiSave saves the input key-value pairs in transaction manner.
func (kv *txnTiKV) MultiSave(kvs map[string]string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiSave error", zap.Any("kvs", kvs), zap.Int("len", len(kvs)), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to create txn for MultiSave")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	for key, value := range kvs {
		key = path.Join(kv.rootPath, key)
		err = txn.Set([]byte(key), []byte(value))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSave", key, value))
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for MultiSave")
	}

	CheckElapseAndWarn(start, "Slow tikv operation MultiSave", zap.Any("kvs", kvs))
	return nil
}

// Remove removes the input key.
func (kv *txnTiKV) Remove(key string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV Remove error", zap.String("key", key), zap.Error(err))

	err = kv.removeTiKVMeta(ctx, key)
	return err
}

// MultiRemove removes the input keys in transaction manner.
func (kv *txnTiKV) MultiRemove(keys []string) error {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiRemove error", zap.Strings("keys", keys), zap.Int("len", len(keys)), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to create txn for MultiRemove")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	for _, key := range keys {
		key = path.Join(kv.rootPath, key)
		err = txn.Delete([]byte(key))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiRemove", key))
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for MultiRemove")
	}
	CheckElapseAndWarn(start, "Slow tikv operation MultiRemove", zap.Strings("keys", keys))
	return nil
}

// RemoveWithPrefix removes the keys for the given prefix.
func (kv *txnTiKV) RemoveWithPrefix(prefix string) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV RemoveWithPrefix error", zap.String("prefix", prefix), zap.Error(err))

	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey(startKey)
	_, err = kv.txn.DeleteRange(ctx, startKey, endKey, 1)
	if err != nil {
		return errors.Wrap(err, "Failed to DeleteRange for RemoveWithPrefix")
	}
	CheckElapseAndWarn(start, "Slow tikv operation remove with prefix", zap.String("prefix", prefix))
	return nil
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *txnTiKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiSaveAndRemove error", zap.Any("saves", saves), zap.Strings("removes", removals), zap.Int("saveLength", len(saves)), zap.Int("removeLength", len(removals)), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to create txn for MultiSaveAndRemove")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		if err = txn.Set([]byte(key), []byte(value)); err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemove", key, value))
		}
	}

	for _, key := range removals {
		key = path.Join(kv.rootPath, key)
		if err = txn.Delete([]byte(key)); err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiSaveAndRemove", key))
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for MultiSaveAndRemove")
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi save and remove", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// MultiRemoveWithPrefix removes the keys with given prefix.
func (kv *txnTiKV) MultiRemoveWithPrefix(prefixes []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiRemoveWithPrefix error", zap.Strings("keys", prefixes), zap.Int("len", len(prefixes)), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to create txn for MultiRemoveWithPrefix")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	for _, prefix := range prefixes {
		prefix = path.Join(kv.rootPath, prefix)
		// Get the start and end keys for the prefix range
		startKey := []byte(prefix)
		endKey := tikv.PrefixNextKey([]byte(prefix))

		// Use Scan to iterate over keys in the prefix range
		iter, err := txn.Iter(startKey, endKey)
		if err != nil {
			return errors.Wrap(err, "Failed to create iterater for MultiRemoveWithPrefix")
		}

		// Iterate over keys and delete them
		for iter.Valid() {
			key := iter.Key()
			err = txn.Delete(key)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiRemoveWithPrefix", string(key)))
			}

			// Move the iterator to the next key
			err = iter.Next()
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for MultiRemoveWithPrefix", string(key)))
			}
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for MultiRemoveWithPrefix")
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi remove with prefix", zap.Strings("keys", prefixes))
	return nil
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *txnTiKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiSaveAndRemoveWithPrefix error", zap.Any("saves", saves), zap.Strings("removes", removals), zap.Int("saveLength", len(saves)), zap.Int("removeLength", len(removals)), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to create txn for MultiSaveAndRemoveWithPrefix")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	// Save key-value pairs
	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		err = txn.Set([]byte(key), []byte(value))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemoveWithPrefix", key, value))
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
			return errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during MultiSaveAndRemoveWithPrefix", prefix))
		}

		// Iterate over keys and delete them
		for iter.Valid() {
			key := iter.Key()
			err = txn.Delete(key)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiSaveAndRemoveWithPrefix", string(key)))
			}

			// Move the iterator to the next key
			err = iter.Next()
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for MultiSaveAndRemoveWithPrefix", string(key)))
			}
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for MultiSaveAndRemoveWithPrefix")
	}
	CheckElapseAndWarn(start, "Slow tikv operation multi save and move with prefix", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// WalkWithPrefix visits each kv with input prefix and apply given fn to it.
func (kv *txnTiKV) WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV WalkWithPagination error", zap.String("prefix", prefix), zap.Error(err))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to create txn for WalkWithPrefix")
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during WalkWithPrefix", prefix))
	}
	defer iter.Close()

	// Iterate over the key-value pairs
	for iter.Valid() {
		err = fn(iter.Key(), iter.Value())
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s;%s)", string(iter.Key()), string(iter.Value())))
		}
		err = iter.Next()
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for WalkWithPrefix", string(iter.Key())))
		}
	}
	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for WalkWithPagination")
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

func (kv *txnTiKV) executeTxn(txn *transaction.KVTxn, ctx context.Context) error {
	start := timerecord.NewTimeRecorder("executeTxn")

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.TotalLabel).Inc()
	err := commitTxn(txn, ctx)
	if err == nil {
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaTxnLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.FailLabel).Inc()
	}

	return err
}

func (kv *txnTiKV) getTiKVMeta(ctx context.Context, key string) (string, error) {
	ctx1, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("getTiKVMeta")

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return "", errors.Wrap(err, "Failed to build transaction for getTiKVMeta")
	}
	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	val, err := txn.Get(ctx1, []byte(key))
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in getTiKVMeta", key))
	}
	err = commitTxn(txn, ctx1)

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

func (kv *txnTiKV) putTiKVMeta(ctx context.Context, key, val string) error {
	ctx1, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("putTiKVMeta")

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to build transaction for putTiKVMeta")
	}
	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	err = txn.Set([]byte(key), []byte(val))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in putTiKVMeta", key))
	}
	err = commitTxn(txn, ctx1)

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

func (kv *txnTiKV) removeTiKVMeta(ctx context.Context, key string) error {
	ctx1, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("removeTiKVMeta")

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to build transaction for putTiKVMeta")
	}
	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	err = txn.Delete([]byte(key))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in putTiKVMeta", key))
	}
	err = commitTxn(txn, ctx1)

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

func (kv *txnTiKV) CompareVersionAndSwap(key string, version int64, target string) (bool, error) {
	return false, fmt.Errorf("Unimplemented! CompareVersionAndSwap is under deprecation")
}
