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
	"bytes"
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
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// A quick note is that we are using logging_error at our outermost scope in order to perform logging

const (
	// We are using a Snapshot instead of transaction when doing read only operations due to the
	// lower overhead (50% less overhead in small tests). In order to guarantee the latest values are
	// grabbed at call, we can set the TS to be the max uint64.
	MaxSnapshotTS = uint64(math.MaxUint64)
	// Whether to enable transaction rollback if a transaction fails. Due to TiKV transactions being
	// optimistic by default, a rollback does not need to be done and the transaction can just be
	// discarded. Discarding saw a small bump in performance on small scale tests.
	EnableRollback = false
	// This empty value is what we are reserving within TiKv to represent an empty string value.
	// TiKV does not allow storing empty values for keys which is something we do in Milvus, so
	// to get over this we are using the reserved keyword as placeholder.
	EmptyValueString = "__milvus_reserved_empty_tikv_value_DO_NOT_USE"
)

var Params *paramtable.ComponentParam = paramtable.Get()

// For reads by prefix we can customize the scan size to increase/decrease rpc calls.
var SnapshotScanSize int

// RequestTimeout is the default timeout for tikv request.
var RequestTimeout time.Duration

var EmptyValueByte = []byte(EmptyValueString)

func tiTxnBegin(txn *txnkv.Client) (*transaction.KVTxn, error) {
	return txn.Begin()
}

func tiTxnCommit(txn *transaction.KVTxn, ctx context.Context) error {
	return txn.Commit(ctx)
}

func tiTxnSnapshot(txn *txnkv.Client, paginationSize int) *txnsnapshot.KVSnapshot {
	ss := txn.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(paginationSize)
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
	SnapshotScanSize = Params.TiKVCfg.SnapshotScanSize.GetAsInt()
	RequestTimeout = Params.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)
	kv := &txnTiKV{
		txn:      txn,
		rootPath: rootPath,
	}
	return kv
}

// Close closes the connection to TiKV.
func (kv *txnTiKV) Close() {
	log.Info("txnTiKV closed", zap.String("path", kv.rootPath))
}

// GetPath returns the path of the key/prefix.
func (kv *txnTiKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

// Log if error is not nil. We use error pointer as in most cases this function
// is Deferred. Deferred functions evaluate their args immediately.
func logWarnOnFailure(err *error, msg string, fields ...zap.Field) {
	if *err != nil {
		fields = append(fields, zap.Error(*err))
		log.Warn(msg, fields...)
	}
}

// Has returns if a key exists.
func (kv *txnTiKV) Has(key string) (bool, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV Has() error", zap.String("key", key))

	_, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		// Dont error out if not present unless failed call to tikv
		if common.IsKeyNotExistError(err) {
			return false, nil
		} else {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to read key: %s", key))
			return false, logging_error
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV Has() operation", zap.String("key", key))
	return true, nil
}

func rollbackOnFailure(err *error, txn *transaction.KVTxn) {
	if *err != nil && EnableRollback == true {
		txn.Rollback()
	}
}

// HasPrefix returns if a key prefix exists.
func (kv *txnTiKV) HasPrefix(prefix string) (bool, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV HasPrefix() error", zap.String("prefix", prefix))

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	// Retrieve bounding keys for prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))

	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		logging_error = errors.Wrap(err, fmt.Sprintf("Failed to create iterator for prefix: %s", prefix))
		return false, logging_error
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

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV Load() error", zap.String("key", key))

	val, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		if common.IsKeyNotExistError(err) {
			logging_error = err
		} else {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to read key %s", key))
		}
		return "", logging_error
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

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV MultiLoad() error", zap.Strings("keys", keys))

	// Convert from []string to [][]byte
	byte_keys := batchConvertFromString(kv.rootPath, keys)

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn, SnapshotScanSize)

	key_map, err := ss.BatchGet(ctx, byte_keys)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed ss.BatchGet() for MultiLoad")
		return nil, logging_error
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
		str_val := convertEmptyByteToString(v)
		valid_values = append(valid_values, str_val)
	}
	if len(missing_values) != 0 {
		logging_error = fmt.Errorf("There are invalid keys: %s", missing_values)
	}

	CheckElapseAndWarn(start, "Slow txnTiKV MultiLoad() operation", zap.Any("keys", keys))
	return valid_values, logging_error
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *txnTiKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV LoadWithPrefix() error", zap.String("prefix", prefix))

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		logging_error = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for LoadWithPrefix() for prefix: %s", prefix))
		return nil, nil, logging_error
	}
	defer iter.Close()

	var keys []string
	var values []string

	// Iterate over the key-value pairs
	for iter.Valid() {
		val := iter.Value()
		// Check if empty value placeholder
		str_val := convertEmptyByteToString(val)
		keys = append(keys, string(iter.Key()))
		values = append(values, str_val)
		err = iter.Next()
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to iterate for LoadWithPrefix() for prefix: %s", prefix))
			return nil, nil, logging_error
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

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV Save() error", zap.String("key", key), zap.String("value", value))

	logging_error = kv.putTiKVMeta(ctx, key, value)
	return logging_error
}

// MultiSave saves the input key-value pairs in transaction manner.
func (kv *txnTiKV) MultiSave(kvs map[string]string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV MultiSave() error", zap.Any("kvs", kvs), zap.Int("len", len(kvs)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to create txn for MultiSave")
		return logging_error
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&logging_error, txn)

	for key, value := range kvs {
		key = path.Join(kv.rootPath, key)
		// Check if value is empty or taking reserved EmptyValue
		byte_value, err := convertEmptyStringToByte(value)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSave()", key, value))
			return logging_error
		}
		// Save the value within a transaction
		err = txn.Set([]byte(key), byte_value)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSave()", key, value))
			return logging_error
		}
	}
	err = kv.executeTxn(txn, ctx)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to commit for MultiSave()")
		return logging_error
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSave() operation", zap.Any("kvs", kvs))
	return nil
}

// Remove removes the input key.
func (kv *txnTiKV) Remove(key string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV Remove() error", zap.String("key", key))

	logging_error = kv.removeTiKVMeta(ctx, key)
	return logging_error
}

// MultiRemove removes the input keys in transaction manner.
func (kv *txnTiKV) MultiRemove(keys []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV MultiRemove() error", zap.Strings("keys", keys), zap.Int("len", len(keys)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to create txn for MultiRemove")
		return logging_error
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&logging_error, txn)

	for _, key := range keys {
		key = path.Join(kv.rootPath, key)
		logging_error = txn.Delete([]byte(key))
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiRemove", key))
			return logging_error
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to commit for MultiRemove()")
		return logging_error
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

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV RemoveWithPrefix() error", zap.String("prefix", prefix))

	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey(startKey)
	_, err := kv.txn.DeleteRange(ctx, startKey, endKey, 1)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to DeleteRange for RemoveWithPrefix")
		return logging_error
	}
	CheckElapseAndWarn(start, "Slow txnTiKV RemoveWithPrefix() operation", zap.String("prefix", prefix))
	return nil
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *txnTiKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV MultiSaveAndRemove error", zap.Any("saves", saves), zap.Strings("removes", removals), zap.Int("saveLength", len(saves)), zap.Int("removeLength", len(removals)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to create txn for MultiSaveAndRemove")
		return logging_error
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&logging_error, txn)

	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		// Check if value is empty or taking reserved EmptyValue
		byte_value, err := convertEmptyStringToByte(value)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSaveAndRemove", key, value))
			return logging_error
		}
		err = txn.Set([]byte(key), byte_value)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemove", key, value))
			return logging_error
		}
	}

	for _, key := range removals {
		key = path.Join(kv.rootPath, key)
		if err = txn.Delete([]byte(key)); err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiSaveAndRemove", key))
			return logging_error
		}
	}

	err = kv.executeTxn(txn, ctx)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to commit for MultiSaveAndRemove")
		return logging_error
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemove() operation", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *txnTiKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV MultiSaveAndRemoveWithPrefix() error", zap.Any("saves", saves), zap.Strings("removes", removals), zap.Int("saveLength", len(saves)), zap.Int("removeLength", len(removals)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to create txn for MultiSaveAndRemoveWithPrefix")
		return logging_error
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&logging_error, txn)

	// Save key-value pairs
	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		// Check if value is empty or taking reserved EmptyValue
		byte_value, err := convertEmptyStringToByte(value)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSaveAndRemoveWithPrefix()", key, value))
			return logging_error
		}
		err = txn.Set([]byte(key), byte_value)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemoveWithPrefix()", key, value))
			return logging_error
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
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during MultiSaveAndRemoveWithPrefix()", prefix))
			return logging_error
		}

		// Iterate over keys and delete them
		for iter.Valid() {
			key := iter.Key()
			err = txn.Delete(key)
			if logging_error != nil {
				logging_error = errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiSaveAndRemoveWithPrefix", string(key)))
				return logging_error
			}

			// Move the iterator to the next key
			err = iter.Next()
			if err != nil {
				logging_error = errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for MultiSaveAndRemoveWithPrefix", string(key)))
				return logging_error
			}
		}
	}
	err = kv.executeTxn(txn, ctx)
	if err != nil {
		logging_error = errors.Wrap(err, "Failed to commit for MultiSaveAndRemoveWithPrefix")
		return logging_error
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemoveWithPrefix() operation", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// WalkWithPrefix visits each kv with input prefix and apply given fn to it.
func (kv *txnTiKV) WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var logging_error error
	defer logWarnOnFailure(&logging_error, "txnTiKV WalkWithPagination error", zap.String("prefix", prefix))

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn, paginationSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		logging_error = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during WalkWithPrefix", prefix))
		return logging_error
	}
	defer iter.Close()

	// Iterate over the key-value pairs
	for iter.Valid() {
		// Grab value for empty check
		byte_val := iter.Value()
		// Check if empty val and replace with placeholder
		if isEmptyByte(byte_val) {
			byte_val = []byte{}
		}
		err = fn(iter.Key(), byte_val)
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s;%s)", string(iter.Key()), string(byte_val)))
			return logging_error
		}
		err = iter.Next()
		if err != nil {
			logging_error = errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for WalkWithPrefix", string(iter.Key())))
			return logging_error
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

	ss := getSnapshot(kv.txn, SnapshotScanSize)

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
	str_val := convertEmptyByteToString(val)

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
	defer rollbackOnFailure(&err, txn)

	// Check if the value being written needs to be empty plaeholder
	byte_value, err := convertEmptyStringToByte(val)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for putTiKVMeta", key, val))
	}
	err = txn.Set([]byte(key), byte_value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to set value for key %s in putTiKVMeta", key))
	}
	err = commitTxn(txn, ctx1)

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.MetaKvSize.WithLabelValues(metrics.MetaPutLabel).Observe(float64(len(byte_value)))
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
	defer rollbackOnFailure(&err, txn)

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
	err := fmt.Errorf("Unimplemented! CompareVersionAndSwap is under deprecation")
	logWarnOnFailure(&err, "Unimplemented")
	return false, err
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

// Since TiKV cannot store empty key values, we assign them a placeholder held by EmptyValue.
// Upon loading, we need to check if the returned value is the placeholder.
func isEmptyByte(value []byte) bool {
	return bytes.Equal(value, EmptyValueByte) || len(value) == 0
}

// Return an empty string if the value is the Empty placeholder, else return actual string value.
func convertEmptyByteToString(value []byte) string {
	if isEmptyByte(value) {
		return ""
	} else {
		return string(value)
	}
}

// Convert string into EmptyValue if empty else cast to []byte. Will throw error if value is equal
// to the EmptyValueString.
func convertEmptyStringToByte(value string) ([]byte, error) {
	if len(value) == 0 {
		return EmptyValueByte, nil
	} else if value == EmptyValueString {
		return nil, fmt.Errorf("Value for key is reserved by EmptyValue: %s", EmptyValueString)
	} else {
		return []byte(value), nil
	}
}
