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

	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

// A quick note is that we are using loggingErr at our outermost scope in order to perform logging

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

// defaultRequestTimeout is the default timeout for tikv request.
const (
	defaultRequestTimeout = 10 * time.Second
)

var EmptyValueByte = []byte(EmptyValueString)

func tiTxnBegin(txn *txnkv.Client) (*transaction.KVTxn, error) {
	return txn.Begin()
}

func tiTxnCommit(ctx context.Context, txn *transaction.KVTxn) error {
	return txn.Commit(ctx)
}

func tiTxnSnapshot(txn *txnkv.Client, paginationSize int) *txnsnapshot.KVSnapshot {
	ss := txn.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(paginationSize)
	return ss
}

var (
	beginTxn    = tiTxnBegin
	commitTxn   = tiTxnCommit
	getSnapshot = tiTxnSnapshot
)

// implementation assertion
var _ kv.MetaKv = (*txnTiKV)(nil)

// txnTiKV implements MetaKv and TxnKV interface. It supports processing multiple kvs within one transaction.
type txnTiKV struct {
	txn      *txnkv.Client
	rootPath string

	requestTimeout time.Duration
}

// NewTiKV creates a new txnTiKV client.
func NewTiKV(txn *txnkv.Client, rootPath string, options ...Option) *txnTiKV {
	SnapshotScanSize = Params.TiKVCfg.SnapshotScanSize.GetAsInt()

	opt := defaultOption()
	for _, option := range options {
		option(opt)
	}

	kv := &txnTiKV{
		txn:            txn,
		rootPath:       rootPath,
		requestTimeout: opt.requestTimeout,
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
func (kv *txnTiKV) Has(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Has() error", zap.String("key", key))

	_, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		// Dont error out if not present unless failed call to tikv
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return false, nil
		}
		loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to read key: %s", key))
		return false, loggingErr
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
func (kv *txnTiKV) HasPrefix(ctx context.Context, prefix string) (bool, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV HasPrefix() error", zap.String("prefix", prefix))

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	// Retrieve bounding keys for prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))

	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to create iterator for prefix: %s", prefix))
		return false, loggingErr
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
func (kv *txnTiKV) Load(ctx context.Context, key string) (string, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Load() error", zap.String("key", key))

	val, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			loggingErr = err
		} else {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to read key %s", key))
		}
		return "", loggingErr
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
func (kv *txnTiKV) MultiLoad(ctx context.Context, keys []string) ([]string, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiLoad() error", zap.Strings("keys", keys))

	// Convert from []string to [][]byte
	byteKeys := batchConvertFromString(kv.rootPath, keys)

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn, SnapshotScanSize)

	keyMap, err := ss.BatchGet(ctx, byteKeys)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed ss.BatchGet() for MultiLoad")
		return nil, loggingErr
	}

	missingValues := []string{}
	validValues := []string{}

	// Loop through keys and build valid/invalid slices
	for _, k := range keys {
		v, ok := keyMap[k]
		if !ok {
			missingValues = append(missingValues, k)
		}
		// Check if empty value placeholder
		strVal := convertEmptyByteToString(v)
		validValues = append(validValues, strVal)
	}
	if len(missingValues) != 0 {
		loggingErr = fmt.Errorf("There are invalid keys: %s", missingValues)
	}

	CheckElapseAndWarn(start, "Slow txnTiKV MultiLoad() operation", zap.Any("keys", keys))
	return validValues, loggingErr
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *txnTiKV) LoadWithPrefix(ctx context.Context, prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV LoadWithPrefix() error", zap.String("prefix", prefix))

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for LoadWithPrefix() for prefix: %s", prefix))
		return nil, nil, loggingErr
	}
	defer iter.Close()

	var keys []string
	var values []string

	// Iterate over the key-value pairs
	for iter.Valid() {
		val := iter.Value()
		// Check if empty value placeholder
		strVal := convertEmptyByteToString(val)
		keys = append(keys, string(iter.Key()))
		values = append(values, strVal)
		err = iter.Next()
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to iterate for LoadWithPrefix() for prefix: %s", prefix))
			return nil, nil, loggingErr
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV LoadWithPrefix() operation", zap.String("prefix", prefix))
	return keys, values, nil
}

// Save saves the input key-value pair.
func (kv *txnTiKV) Save(ctx context.Context, key, value string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Save() error", zap.String("key", key), zap.String("value", value))

	loggingErr = kv.putTiKVMeta(ctx, key, value)
	return loggingErr
}

// MultiSave saves the input key-value pairs in transaction manner.
func (kv *txnTiKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiSave() error", zap.Any("kvs", kvs), zap.Int("len", len(kvs)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to create txn for MultiSave")
		return loggingErr
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&loggingErr, txn)

	for key, value := range kvs {
		key = path.Join(kv.rootPath, key)
		// Check if value is empty or taking reserved EmptyValue
		byteValue, err := convertEmptyStringToByte(value)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSave()", key, value))
			return loggingErr
		}
		// Save the value within a transaction
		err = txn.Set([]byte(key), byteValue)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSave()", key, value))
			return loggingErr
		}
	}
	err = kv.executeTxn(ctx, txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to commit for MultiSave()")
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSave() operation", zap.Any("kvs", kvs))
	return nil
}

// Remove removes the input key.
func (kv *txnTiKV) Remove(ctx context.Context, key string) error {
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Remove() error", zap.String("key", key))

	loggingErr = kv.removeTiKVMeta(ctx, key)
	return loggingErr
}

// MultiRemove removes the input keys in transaction manner.
func (kv *txnTiKV) MultiRemove(ctx context.Context, keys []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiRemove() error", zap.Strings("keys", keys), zap.Int("len", len(keys)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to create txn for MultiRemove")
		return loggingErr
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&loggingErr, txn)

	for _, key := range keys {
		key = path.Join(kv.rootPath, key)
		err = txn.Delete([]byte(key))
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiRemove", key))
			return loggingErr
		}
	}

	err = kv.executeTxn(ctx, txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to commit for MultiRemove()")
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiRemove() operation", zap.Strings("keys", keys))
	return nil
}

// RemoveWithPrefix removes the keys for the given prefix.
func (kv *txnTiKV) RemoveWithPrefix(ctx context.Context, prefix string) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV RemoveWithPrefix() error", zap.String("prefix", prefix))

	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey(startKey)
	_, err := kv.txn.DeleteRange(ctx, startKey, endKey, 1)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to DeleteRange for RemoveWithPrefix")
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV RemoveWithPrefix() operation", zap.String("prefix", prefix))
	return nil
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *txnTiKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiSaveAndRemove error", zap.Any("saves", saves), zap.Strings("removes", removals), zap.Int("saveLength", len(saves)), zap.Int("removeLength", len(removals)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to create txn for MultiSaveAndRemove")
		return loggingErr
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&loggingErr, txn)

	for _, pred := range preds {
		key := path.Join(kv.rootPath, pred.Key())
		val, err := txn.Get(ctx, []byte(key))
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemove", pred.Key(), pred.TargetValue()))
			return loggingErr
		}
		if !pred.IsTrue(val) {
			loggingErr = merr.WrapErrIoFailedReason("failed to meet predicate", fmt.Sprintf("key=%s, value=%v", pred.Key(), pred.TargetValue()))
			return loggingErr
		}
	}

	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		// Check if value is empty or taking reserved EmptyValue
		byteValue, err := convertEmptyStringToByte(value)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSaveAndRemove", key, value))
			return loggingErr
		}
		err = txn.Set([]byte(key), byteValue)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemove", key, value))
			return loggingErr
		}
	}

	for _, key := range removals {
		key = path.Join(kv.rootPath, key)
		if err = txn.Delete([]byte(key)); err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiSaveAndRemove", key))
			return loggingErr
		}
	}

	err = kv.executeTxn(ctx, txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to commit for MultiSaveAndRemove")
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemove() operation", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *txnTiKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiSaveAndRemoveWithPrefix() error", zap.Any("saves", saves), zap.Strings("removes", removals), zap.Int("saveLength", len(saves)), zap.Int("removeLength", len(removals)))

	txn, err := beginTxn(kv.txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to create txn for MultiSaveAndRemoveWithPrefix")
		return loggingErr
	}

	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&loggingErr, txn)

	for _, pred := range preds {
		key := path.Join(kv.rootPath, pred.Key())
		val, err := txn.Get(ctx, []byte(key))
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemove", pred.Key(), pred.TargetValue()))
			return loggingErr
		}
		if !pred.IsTrue(val) {
			loggingErr = merr.WrapErrIoFailedReason("failed to meet predicate", fmt.Sprintf("key=%s, value=%v", pred.Key(), pred.TargetValue()))
			return loggingErr
		}
	}

	// Save key-value pairs
	for key, value := range saves {
		key = path.Join(kv.rootPath, key)
		// Check if value is empty or taking reserved EmptyValue
		byteValue, err := convertEmptyStringToByte(value)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSaveAndRemoveWithPrefix()", key, value))
			return loggingErr
		}
		err = txn.Set([]byte(key), byteValue)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemoveWithPrefix()", key, value))
			return loggingErr
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
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during MultiSaveAndRemoveWithPrefix()", prefix))
			return loggingErr
		}

		// Iterate over keys and delete them
		for iter.Valid() {
			key := iter.Key()
			err = txn.Delete(key)
			if loggingErr != nil {
				loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to delete %s for MultiSaveAndRemoveWithPrefix", string(key)))
				return loggingErr
			}

			// Move the iterator to the next key
			err = iter.Next()
			if err != nil {
				loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for MultiSaveAndRemoveWithPrefix", string(key)))
				return loggingErr
			}
		}
	}
	err = kv.executeTxn(ctx, txn)
	if err != nil {
		loggingErr = errors.Wrap(err, "Failed to commit for MultiSaveAndRemoveWithPrefix")
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemoveWithPrefix() operation", zap.Any("saves", saves), zap.Strings("removals", removals))
	return nil
}

// WalkWithPrefix visits each kv with input prefix and apply given fn to it.
func (kv *txnTiKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV WalkWithPagination error", zap.String("prefix", prefix))

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn, paginationSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during WalkWithPrefix", prefix))
		return loggingErr
	}
	defer iter.Close()

	// Iterate over the key-value pairs
	for iter.Valid() {
		// Grab value for empty check
		byteVal := iter.Value()
		// Check if empty val and replace with placeholder
		if isEmptyByte(byteVal) {
			byteVal = []byte{}
		}
		err = fn(iter.Key(), byteVal)
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s;%s)", string(iter.Key()), string(byteVal)))
			return loggingErr
		}
		err = iter.Next()
		if err != nil {
			loggingErr = errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for WalkWithPrefix", string(iter.Key())))
			return loggingErr
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV WalkWithPagination() operation", zap.String("prefix", prefix))
	return nil
}

func (kv *txnTiKV) executeTxn(ctx context.Context, txn *transaction.KVTxn) error {
	start := timerecord.NewTimeRecorder("executeTxn")

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.TotalLabel).Inc()
	err := commitTxn(ctx, txn)
	if err == nil {
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaTxnLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaTxnLabel, metrics.FailLabel).Inc()
	}

	return err
}

func (kv *txnTiKV) getTiKVMeta(ctx context.Context, key string) (string, error) {
	ctx1, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("getTiKVMeta")

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	val, err := ss.Get(ctx1, []byte(key))
	if err != nil {
		// Log key read fail
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.FailLabel).Inc()
		if err == tikverr.ErrNotExist {
			// If key is missing
			return "", merr.WrapErrIoKeyNotFound(key)
		}
		// If call to tikv fails
		return "", errors.Wrap(err, fmt.Sprintf("Failed to get value for key %s in getTiKVMeta", key))
	}

	// Check if value is the empty placeholder
	strVal := convertEmptyByteToString(val)

	elapsed := start.ElapseSpan()

	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.TotalLabel).Inc()
	metrics.MetaKvSize.WithLabelValues(metrics.MetaGetLabel).Observe(float64(len(val)))
	metrics.MetaRequestLatency.WithLabelValues(metrics.MetaGetLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.SuccessLabel).Inc()

	return strVal, nil
}

func (kv *txnTiKV) putTiKVMeta(ctx context.Context, key, val string) error {
	ctx1, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("putTiKVMeta")

	txn, err := beginTxn(kv.txn)
	if err != nil {
		return errors.Wrap(err, "Failed to build transaction for putTiKVMeta")
	}
	// Defer a rollback only if the transaction hasn't been committed
	defer rollbackOnFailure(&err, txn)

	// Check if the value being written needs to be empty placeholder
	byteValue, err := convertEmptyStringToByte(val)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for putTiKVMeta", key, val))
	}
	err = txn.Set([]byte(key), byteValue)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to set value for key %s in putTiKVMeta", key))
	}
	err = commitTxn(ctx1, txn)

	elapsed := start.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.MetaKvSize.WithLabelValues(metrics.MetaPutLabel).Observe(float64(len(byteValue)))
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaPutLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.FailLabel).Inc()
	}

	return err
}

func (kv *txnTiKV) removeTiKVMeta(ctx context.Context, key string) error {
	ctx1, cancel := context.WithTimeout(ctx, kv.requestTimeout)
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
	err = commitTxn(ctx1, txn)

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

func (kv *txnTiKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
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
	}
	return string(value)
}

// Convert string into EmptyValue if empty else cast to []byte. Will throw error if value is equal
// to the EmptyValueString.
func convertEmptyStringToByte(value string) ([]byte, error) {
	if len(value) == 0 {
		return EmptyValueByte, nil
	} else if value == EmptyValueString {
		return nil, fmt.Errorf("value for key is reserved by EmptyValue: %s", EmptyValueString)
	}
	return []byte(value), nil
}
