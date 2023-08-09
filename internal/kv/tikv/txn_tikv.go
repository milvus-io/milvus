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
	"fmt"
	"math"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
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
	// Grab the latest keys
	MaxSnapshotTS = uint64(math.MaxUint64)
	// How many keys to scan per batch when using prefix
	SnapshotScanSize = 256
	EnableRollback   = false
)

// Empty value default
var EmptyValue = []byte{0}

func tiTxnBegin(txn *txnkv.Client) (*transaction.KVTxn, error) {
	return txn.Begin()
}

func tiTxnCommit(txn *transaction.KVTxn, ctx context.Context) error {
	return txn.Commit(ctx)
}

func tiTxnSnapshot(txn *txnkv.Client) *txnsnapshot.KVSnapshot {
	ss := txn.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(SnapshotScanSize)
	return ss

}

var beginTxn = tiTxnBegin
var commitTxn = tiTxnCommit
var getSnapshot = tiTxnSnapshot

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
		// Dont error out if not present unless failed call to tikv
		if common.IsKeyNotExistError(err) {
			return false, nil
		} else {
			return false, errors.Wrap(err, fmt.Sprintf("Failed to read key: %s", key))
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV Has() operation", zap.String("key", key))
	return true, nil
}

func rollbackOnFailure(err error, txn *transaction.KVTxn) {
	if err != nil && EnableRollback == true {
		txn.Rollback()
	}
}

// HasPrefix returns if a key prefix exists.
func (kv *txnTiKV) HasPrefix(prefix string) (bool, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var err error
	defer logWarnOnFailure(err, "txnTiKV HasPrefix error", zap.String("prefix", prefix), zap.Error(err))

	ss := getSnapshot(kv.txn)

	// Retrieve bounding keys for prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))

	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("Failed to create iterator for prefix: %s", prefix))
	}
	defer iter.Close()

	r := false
	// Iterater only needs to check the first key-value pair
	if iter.Valid() {
		r = true
	}
	CheckElapseAndWarn(start, "Slow txnTiKV HasPrefix() operation", zap.String("prefix", prefix))
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

	if err != nil {
		if common.IsKeyNotExistError(err) {
			return "", err
		} else {
			return "", errors.Wrap(err, fmt.Sprintf("Failed to read key %s", key))
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV Load() operation", zap.String("key", key))
	return val, nil
}

func batchConvertFromString(prefix string, keys []string) [][]byte {
	output := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = path.Join(prefix, keys[i])
		output[i] = []byte(keys[i])
	}
	return output
}

// MultiLoad gets the values of input keys in a transaction.
func (kv *txnTiKV) MultiLoad(keys []string) ([]string, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var err error
	defer logWarnOnFailure(err, "txnTiKV MultiLoad error", zap.Strings("keys", keys), zap.Error(err))

	// Convert from []string to [][]byte
	byte_keys := batchConvertFromString(kv.rootPath, keys)

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn)

	key_map, err := ss.BatchGet(ctx, byte_keys)
	if err != nil {
		return nil, errors.Wrap(err, "Failed ss.BatchGet() for MultiLoad")
	}

	missing_values := []string{}
	valid_values := []string{}

	// Loop through keys and build valid/invalid slices
	for _, k := range keys {
		v, ok := key_map[k]
		if !ok {
			missing_values = append(missing_values, k)
		}
		// Check if empty value placeholder
		var str_val string
		if isEmpty(v) {
			str_val = ""
		} else {
			str_val = string(v)
		}
		valid_values = append(valid_values, str_val)

	}

	err = nil
	if len(missing_values) != 0 {
		err = fmt.Errorf("there are invalid keys: %s", missing_values)
	}

	CheckElapseAndWarn(start, "Slow txnTiKV MultiLoad() operation", zap.Any("keys", keys))
	return valid_values, err
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *txnTiKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var err error
	defer logWarnOnFailure(err, "txnTiKV LoadWithPrefix error", zap.String("prefix", prefix), zap.Error(err))

	ss := getSnapshot(kv.txn)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to create iterater for LoadWithPrefix %s", prefix))
	}
	defer iter.Close()

	var keys []string
	var values []string
	var str_val string

	// Iterate over the key-value pairs
	for iter.Valid() {
		val := iter.Value()
		// Check if empty value placeholder
		if isEmpty(val) {
			str_val = ""
		} else {
			str_val = string(val)
		}
		keys = append(keys, string(iter.Key()))
		values = append(values, str_val)
		err = iter.Next()
		if err != nil {
			return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to iterate for LoadWithPrefix %s", prefix))
		}
	}

	CheckElapseAndWarn(start, "Slow txnTiKV LoadWithPrefix() operation", zap.String("prefix", prefix))
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
		// Check if empty val and replace with placeholder
		if len(value) == 0 {
			err = txn.Set([]byte(key), EmptyValue)
		} else {
			err = txn.Set([]byte(key), []byte(value))
		}
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSave", key, value))
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to commit for MultiSave")
	}

	CheckElapseAndWarn(start, "Slow txnTiKV MultiSave() operation", zap.Any("kvs", kvs))
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
	CheckElapseAndWarn(start, "Slow txnTiKV MultiRemove() operation", zap.Strings("keys", keys))
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
	CheckElapseAndWarn(start, "Slow txnTiKV RemoveWithPrefix() operation", zap.String("prefix", prefix))
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
		// Check if empty val and replace with placeholder
		if len(value) == 0 {
			err = txn.Set([]byte(key), EmptyValue)
		} else {
			err = txn.Set([]byte(key), []byte(value))
		}
		if err != nil {
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
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemove() operation", zap.Any("saves", saves), zap.Strings("removals", removals))
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
	CheckElapseAndWarn(start, "Slow txnTiKV MultiRemoveWithPrefix() operation", zap.Strings("keys", prefixes))
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
		// Check if empty val and replace with placeholder
		if len(value) == 0 {
			err = txn.Set([]byte(key), EmptyValue)
		} else {
			err = txn.Set([]byte(key), []byte(value))
		}
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
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemoveWithPrefix() operation", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// WalkWithPrefix visits each kv with input prefix and apply given fn to it.
func (kv *txnTiKV) WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var err error
	defer logWarnOnFailure(err, "txnTiKV WalkWithPagination error", zap.String("prefix", prefix), zap.Error(err))

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during WalkWithPrefix", prefix))
	}
	defer iter.Close()

	// Iterate over the key-value pairs
	for iter.Valid() {
		// Grab value for empty check
		byte_val := iter.Value()
		// Check if empty val and replace with placeholder
		if isEmpty(byte_val) {
			byte_val = []byte{}
		}
		err = fn(iter.Key(), byte_val)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s;%s)", string(iter.Key()), string(byte_val)))
		}
		err = iter.Next()
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for WalkWithPrefix", string(iter.Key())))
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV WalkWithPagination() operation", zap.String("prefix", prefix))
	return nil
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

	ss := getSnapshot(kv.txn)

	val, err := ss.Get(ctx1, []byte(key))
	if err != nil {
		// Log key read fail
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.FailLabel).Inc()
		if err == tikverr.ErrNotExist {
			// If key is missing
			return "", common.NewKeyNotExistError(key)

		} else {
			// If call to tikv fails
			return "", errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in getTiKVMeta", key))
		}
	}

	// Check if value is the empty placeholder
	var str_val string
	if isEmpty(val) {
		str_val = ""
	} else {
		str_val = string(val)
	}

	elapsed := start.ElapseSpan()

	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.TotalLabel).Inc()
	metrics.MetaKvSize.WithLabelValues(metrics.MetaGetLabel).Observe(float64(len(val)))
	metrics.MetaRequestLatency.WithLabelValues(metrics.MetaGetLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.SuccessLabel).Inc()

	return str_val, nil
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

	// Check if the value being written needs to be empty
	var written_val []byte
	if len(val) == 0 {
		written_val = EmptyValue
	} else {
		written_val = []byte(val)
	}
	err = txn.Set([]byte(key), written_val)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in putTiKVMeta", key))
	}
	err = commitTxn(txn, ctx1)

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.MetaKvSize.WithLabelValues(metrics.MetaPutLabel).Observe(float64(len(written_val)))
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
		return errors.Wrap(err, "Failed to build transaction for removeTiKVMeta")
	}
	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(err, txn)

	err = txn.Delete([]byte(key))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to remove key %s in removeTiKVMeta", key))
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

// CheckElapseAndWarn checks the elapsed time and warns if it is too long.
func CheckElapseAndWarn(start time.Time, message string, fields ...zap.Field) bool {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > 2000 {
		log.Warn(message, append([]zap.Field{zap.String("time spent", elapsed.String())}, fields...)...)
		return true
	}
	return false
}

// Since tikv cannot store empty keys, we reserve EmptyValue as the empty string
func isEmpty(value []byte) bool {
	if len(value) == len(EmptyValue) {
		for i, v := range value {
			if v != EmptyValue[i] {
				// If we see mismatch, it isnt equal to placeholder
				return false
			}
		}
		// If we see equal length and no mismatch, it is empty
		return true
	}
	// If different length, it isnt equal to placeholder
	return false
}
