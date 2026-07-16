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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	tikverr "github.com/tikv/client-go/v2/error"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

var (
	// errPredicateNotMet marks deterministic predicate mismatches inside write transaction builds.
	errPredicateNotMet = errors.New("predicate not met")
	errBuildAbort      = errors.New("deterministic build error")
)

func wrapWriteBuildErr(reason string, err error) error {
	wrapped := merr.WrapErrIoFailedReason(reason, err.Error())
	if isLocalMutationValidationErr(err) {
		return markBuildAbort(wrapped)
	}
	return wrapped
}

func markPredicateNotMet(err error) error {
	return markBuildAbort(errors.Mark(err, errPredicateNotMet))
}

func markBuildAbort(err error) error {
	return errors.Mark(err, errBuildAbort)
}

func isLocalMutationValidationErr(err error) bool {
	var keyTooLarge *tikverr.ErrKeyTooLarge
	if errors.As(err, &keyTooLarge) {
		return true
	}
	var entryTooLarge *tikverr.ErrEntryTooLarge
	if errors.As(err, &entryTooLarge) {
		return true
	}
	return errors.Is(err, tikverr.ErrCannotSetNilValue)
}

// For reads by prefix we can customize the scan size to increase/decrease rpc calls.
var SnapshotScanSize int

// defaultRequestTimeout is the default timeout for tikv request.
const (
	defaultRequestTimeout = 10 * time.Second
)

// Write transactions are rebuilt and retried on transient TiKV failures.
// Region errors (e.g. epoch_not_match after a region split, or a briefly
// unreachable store) escape txn.Commit() as plain errors once client-go's
// internal backoff budget is exhausted, and a failed KVTxn cannot be reused,
// so the retry must re-run the whole transaction on a fresh region cache.
// Bounded by the per-call requestTimeout context; a persistently failing
// call spends up to ~6s in backoff sleep before surfacing the error.
// Declared as vars so tests injecting persistent failures can lower them.
var (
	writeTxnRetryAttempts = uint(5)
	writeTxnRetrySleep    = 200 * time.Millisecond
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
	mlog.Info(context.TODO(), "txnTiKV closed", mlog.String("path", kv.rootPath))
}

// GetPath returns the path of the key/prefix.
func (kv *txnTiKV) GetPath(key string) string {
	return util.GetPath(kv.rootPath, key)
}

// Log if error is not nil. We use error pointer as in most cases this function
// is Deferred. Deferred functions evaluate their args immediately.
func logWarnOnFailure(err *error, msg string, fields ...mlog.Field) {
	if *err != nil {
		fields = append(fields, mlog.Err(*err))
		mlog.Warn(context.TODO(),
			msg, fields...)
	}
}

// Has returns if a key exists.
func (kv *txnTiKV) Has(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	key = kv.GetPath(key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Has() error", mlog.String("key", key))

	_, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		// Dont error out if not present unless failed call to tikv
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return false, nil
		}
		loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to read key: %s", key), err.Error())
		return false, loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV Has() operation", mlog.String("key", key))
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
	prefix = kv.GetPath(prefix)

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV HasPrefix() error", mlog.String("prefix", prefix))

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	// Retrieve bounding keys for prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))

	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to create iterator for prefix: %s", prefix), err.Error())
		return false, loggingErr
	}
	defer iter.Close()

	r := iter.Valid()
	// Iterater only needs to check the first key-value pair

	CheckElapseAndWarn(start, "Slow txnTiKV HasPrefix() operation", mlog.String("prefix", prefix))
	return r, nil
}

// Load returns value of the key.
func (kv *txnTiKV) Load(ctx context.Context, key string) (string, error) {
	start := time.Now()
	key = kv.GetPath(key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Load() error", mlog.String("key", key))

	val, err := kv.getTiKVMeta(ctx, key)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			loggingErr = err
		} else {
			loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to read key %s", key), err.Error())
		}
		return "", loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV Load() operation", mlog.String("key", key))
	return val, nil
}

func batchConvertFromString(prefix string, keys []string) [][]byte {
	output := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		keys[i] = util.GetPath(prefix, keys[i])
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
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiLoad() error", mlog.Strings("keys", keys))

	// Convert from []string to [][]byte
	byteKeys := batchConvertFromString(kv.rootPath, keys)

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn, SnapshotScanSize)

	keyMap, err := ss.BatchGet(ctx, byteKeys)
	if err != nil {
		loggingErr = merr.WrapErrIoFailedReason("Failed ss.BatchGet() for MultiLoad", err.Error())
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
		strVal := convertEmptyByteToString(v.Value)
		validValues = append(validValues, strVal)
	}
	if len(missingValues) != 0 {
		loggingErr = merr.WrapErrIoKeyNotFound(fmt.Sprintf("%v", missingValues))
	}

	CheckElapseAndWarn(start, "Slow txnTiKV MultiLoad() operation", mlog.Any("keys", keys))
	return validValues, loggingErr
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *txnTiKV) LoadWithPrefix(ctx context.Context, prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = kv.GetPath(prefix)

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV LoadWithPrefix() error", mlog.String("prefix", prefix))

	ss := getSnapshot(kv.txn, SnapshotScanSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to create iterater for LoadWithPrefix() for prefix: %s", prefix), err.Error())
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
			loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to iterate for LoadWithPrefix() for prefix: %s", prefix), err.Error())
			return nil, nil, loggingErr
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV LoadWithPrefix() operation", mlog.String("prefix", prefix))
	return keys, values, nil
}

// Save saves the input key-value pair.
func (kv *txnTiKV) Save(ctx context.Context, key, value string) error {
	key = kv.GetPath(key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Save() error", mlog.String("key", key), mlog.String("value", value))

	loggingErr = kv.putTiKVMeta(ctx, key, value)
	return loggingErr
}

// MultiSave saves the input key-value pairs in transaction manner.
func (kv *txnTiKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiSave() error", mlog.Any("kvs", kvs), mlog.Int("len", len(kvs)))

	loggingErr = kv.runWriteTxnWithRetry(ctx, "MultiSave()", func(txn *transaction.KVTxn) error {
		for key, value := range kvs {
			key = kv.GetPath(key)
			// Check if value is empty or taking reserved EmptyValue
			byteValue, err := convertEmptyStringToByte(value)
			if err != nil {
				return merr.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSave()", key, value))
			}
			// Save the value within a transaction
			if err = txn.Set([]byte(key), byteValue); err != nil {
				return wrapWriteBuildErr(fmt.Sprintf("Failed to set (%s:%s) for MultiSave()", key, value), err)
			}
		}
		return nil
	}, kv.executeTxn)
	if loggingErr != nil {
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSave() operation", mlog.Any("kvs", kvs))
	return nil
}

// Remove removes the input key.
func (kv *txnTiKV) Remove(ctx context.Context, key string) error {
	key = kv.GetPath(key)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV Remove() error", mlog.String("key", key))

	loggingErr = kv.removeTiKVMeta(ctx, key)
	return loggingErr
}

// MultiRemove removes the input keys in transaction manner.
func (kv *txnTiKV) MultiRemove(ctx context.Context, keys []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiRemove() error", mlog.Strings("keys", keys), mlog.Int("len", len(keys)))

	loggingErr = kv.runWriteTxnWithRetry(ctx, "MultiRemove()", func(txn *transaction.KVTxn) error {
		for _, key := range keys {
			key = kv.GetPath(key)
			if err := txn.Delete([]byte(key)); err != nil {
				return wrapWriteBuildErr(fmt.Sprintf("Failed to delete %s for MultiRemove", key), err)
			}
		}
		return nil
	}, kv.executeTxn)
	if loggingErr != nil {
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiRemove() operation", mlog.Strings("keys", keys))
	return nil
}

// RemoveWithPrefix removes the keys for the given prefix.
func (kv *txnTiKV) RemoveWithPrefix(ctx context.Context, prefix string) error {
	start := time.Now()
	prefix = kv.GetPath(prefix)
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV RemoveWithPrefix() error", mlog.String("prefix", prefix))

	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey(startKey)
	_, err := kv.txn.DeleteRange(ctx, startKey, endKey, 1)
	if err != nil {
		loggingErr = merr.WrapErrIoFailedReason("Failed to DeleteRange for RemoveWithPrefix", err.Error())
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV RemoveWithPrefix() operation", mlog.String("prefix", prefix))
	return nil
}

// MultiSaveAndRemove saves the key-value pairs and removes the keys in a transaction.
func (kv *txnTiKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiSaveAndRemove error", mlog.Any("saves", saves), mlog.Strings("removes", removals), mlog.Int("saveLength", len(saves)), mlog.Int("removeLength", len(removals)))

	// use complement to remove keys that are not in saves
	saveKeys := typeutil.NewSet(lo.Keys(saves)...)
	removeKeys := typeutil.NewSet(removals...)
	removals = removeKeys.Complement(saveKeys).Collect()

	loggingErr = kv.runWriteTxnWithRetry(ctx, "MultiSaveAndRemove", func(txn *transaction.KVTxn) error {
		for _, pred := range preds {
			key := kv.GetPath(pred.Key())
			val, err := txn.Get(ctx, []byte(key))
			if err != nil {
				if errors.Is(err, tikverr.ErrNotExist) {
					return markPredicateNotMet(merr.WrapErrIoFailedReason(fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemove", pred.Key(), pred.TargetValue()), err.Error()))
				}
				return merr.WrapErrIoFailedReason(fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemove", pred.Key(), pred.TargetValue()), err.Error())
			}
			if !pred.IsTrue(val.Value) {
				return markPredicateNotMet(merr.WrapErrIoFailedReason("failed to meet predicate", fmt.Sprintf("key=%s, value=%v", pred.Key(), pred.TargetValue())))
			}
		}

		for _, key := range removals {
			key = kv.GetPath(key)
			if err := txn.Delete([]byte(key)); err != nil {
				return wrapWriteBuildErr(fmt.Sprintf("Failed to delete %s for MultiSaveAndRemove", key), err)
			}
		}

		for key, value := range saves {
			key = kv.GetPath(key)
			// Check if value is empty or taking reserved EmptyValue
			byteValue, err := convertEmptyStringToByte(value)
			if err != nil {
				return merr.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSaveAndRemove", key, value))
			}
			if err = txn.Set([]byte(key), byteValue); err != nil {
				return wrapWriteBuildErr(fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemove", key, value), err)
			}
		}
		return nil
	}, kv.executeTxn)
	if loggingErr != nil {
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemove() operation", mlog.Any("saves", saves), mlog.Strings("removals", removals))
	return nil
}

// MultiSaveAndRemoveWithPrefix saves kv in @saves and removes the keys with given prefix in @removals.
func (kv *txnTiKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV MultiSaveAndRemoveWithPrefix() error", mlog.Any("saves", saves), mlog.Strings("removes", removals), mlog.Int("saveLength", len(saves)), mlog.Int("removeLength", len(removals)))

	loggingErr = kv.runWriteTxnWithRetry(ctx, "MultiSaveAndRemoveWithPrefix", func(txn *transaction.KVTxn) error {
		for _, pred := range preds {
			key := kv.GetPath(pred.Key())
			val, err := txn.Get(ctx, []byte(key))
			if err != nil {
				if errors.Is(err, tikverr.ErrNotExist) {
					return markPredicateNotMet(merr.WrapErrIoFailedReason(fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemove", pred.Key(), pred.TargetValue()), err.Error()))
				}
				return merr.WrapErrIoFailedReason(fmt.Sprintf("failed to read predicate target (%s:%v) for MultiSaveAndRemove", pred.Key(), pred.TargetValue()), err.Error())
			}
			if !pred.IsTrue(val.Value) {
				return markPredicateNotMet(merr.WrapErrIoFailedReason("failed to meet predicate", fmt.Sprintf("key=%s, value=%v", pred.Key(), pred.TargetValue())))
			}
		}

		// Remove keys with prefix
		for _, prefix := range removals {
			prefix = kv.GetPath(prefix)
			if err := func(prefix string) error {
				// Get the start and end keys for the prefix range
				startKey := []byte(prefix)
				endKey := tikv.PrefixNextKey([]byte(prefix))

				// Use Scan to iterate over keys in the prefix range
				iter, err := txn.Iter(startKey, endKey)
				if err != nil {
					return merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to create iterater for %s during MultiSaveAndRemoveWithPrefix()", prefix), err.Error())
				}
				defer iter.Close()

				// Iterate over keys and delete them
				for iter.Valid() {
					key := iter.Key()
					if err = txn.Delete(key); err != nil {
						return wrapWriteBuildErr(fmt.Sprintf("Failed to delete %s for MultiSaveAndRemoveWithPrefix", string(key)), err)
					}

					// Move the iterator to the next key
					if err = iter.Next(); err != nil {
						return merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to move Iterator after key %s for MultiSaveAndRemoveWithPrefix", string(key)), err.Error())
					}
				}
				return nil
			}(prefix); err != nil {
				return err
			}
		}

		// Save key-value pairs
		for key, value := range saves {
			key = kv.GetPath(key)
			// Check if value is empty or taking reserved EmptyValue
			byteValue, err := convertEmptyStringToByte(value)
			if err != nil {
				return merr.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for MultiSaveAndRemoveWithPrefix()", key, value))
			}
			if err = txn.Set([]byte(key), byteValue); err != nil {
				return wrapWriteBuildErr(fmt.Sprintf("Failed to set (%s:%s) for MultiSaveAndRemoveWithPrefix()", key, value), err)
			}
		}
		return nil
	}, kv.executeTxn)
	if loggingErr != nil {
		return loggingErr
	}
	CheckElapseAndWarn(start, "Slow txnTiKV MultiSaveAndRemoveWithPrefix() operation", mlog.Any("saves", saves), mlog.Strings("removals", removals))
	return nil
}

// WalkWithPrefix visits each kv with input prefix and apply given fn to it.
func (kv *txnTiKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = kv.GetPath(prefix)

	var loggingErr error
	defer logWarnOnFailure(&loggingErr, "txnTiKV WalkWithPagination error", mlog.String("prefix", prefix))

	// Since only reading, use Snapshot for less overhead
	ss := getSnapshot(kv.txn, paginationSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to create iterater for %s during WalkWithPrefix", prefix), err.Error())
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
			loggingErr = merr.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s;%s)", string(iter.Key()), string(byteVal)))
			return loggingErr
		}
		err = iter.Next()
		if err != nil {
			loggingErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to move Iterator after key %s for WalkWithPrefix", string(iter.Key())), err.Error())
			return loggingErr
		}
	}
	CheckElapseAndWarn(start, "Slow txnTiKV WalkWithPagination() operation", mlog.String("prefix", prefix))
	return nil
}

// runWriteTxnWithRetry begins a transaction, applies build to it and commits,
// retrying the whole cycle on transient failures (see writeTxnRetryAttempts).
// All mutations in this file are idempotent, so re-committing after an
// undetermined commit result is safe. Deterministic build failures (bad input,
// unmet predicates, missing predicate targets, local TiKV mutation validation)
// abort immediately; transient IO errors from build reads are retried like
// begin/commit failures. Build errors are returned to the caller unwrapped. The
// commit function is passed in so that callers keep their existing metrics
// accounting (executeTxn vs raw commitTxn); note the asymmetry under retry:
// Multi* methods commit via executeTxn, whose MetaTxnLabel counters fire once
// per attempt, while Save/Remove aggregate their MetaPut/MetaRemove counters
// once per logical call after all retries.
func (kv *txnTiKV) runWriteTxnWithRetry(ctx context.Context, op string, build func(txn *transaction.KVTxn) error, commit func(ctx context.Context, txn *transaction.KVTxn) error) error {
	var buildErr error
	err := retry.Do(ctx, func() error {
		var attemptErr error
		buildErr = nil
		txn, err := beginTxn(kv.txn)
		if err != nil {
			return merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to create txn for %s", op), err.Error())
		}
		// Defer a rollback only if the transaction hasn't been committed
		defer rollbackOnFailure(&attemptErr, txn)

		if buildErr = build(txn); buildErr != nil {
			attemptErr = buildErr
			if errors.Is(buildErr, errBuildAbort) {
				return retry.Unrecoverable(buildErr)
			}
			return buildErr
		}
		if err := commit(ctx, txn); err != nil {
			attemptErr = merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to commit for %s", op), err.Error())
			return attemptErr
		}
		return nil
	}, retry.Attempts(writeTxnRetryAttempts), retry.Sleep(writeTxnRetrySleep))
	if buildErr != nil {
		return buildErr
	}
	return err
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
		return "", merr.WrapErrIoFailedReason(fmt.Sprintf("Failed to get value for key %s in getTiKVMeta", key), err.Error())
	}

	// Check if value is the empty placeholder
	strVal := convertEmptyByteToString(val.Value)

	elapsed := start.ElapseSpan()

	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.TotalLabel).Inc()
	metrics.MetaKvSize.WithLabelValues(metrics.MetaGetLabel).Observe(float64(len(val.Value)))
	metrics.MetaRequestLatency.WithLabelValues(metrics.MetaGetLabel).Observe(float64(elapsed.Milliseconds()))
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaGetLabel, metrics.SuccessLabel).Inc()

	return strVal, nil
}

func (kv *txnTiKV) putTiKVMeta(ctx context.Context, key, val string) error {
	ctx1, cancel := context.WithTimeout(ctx, kv.requestTimeout)
	defer cancel()

	start := timerecord.NewTimeRecorder("putTiKVMeta")

	// Check if the value being written needs to be empty placeholder
	byteValue, err := convertEmptyStringToByte(val)
	if err != nil {
		return merr.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for putTiKVMeta", key, val))
	}

	err = kv.runWriteTxnWithRetry(ctx1, "putTiKVMeta", func(txn *transaction.KVTxn) error {
		if err := txn.Set([]byte(key), byteValue); err != nil {
			return wrapWriteBuildErr(fmt.Sprintf("Failed to set value for key %s in putTiKVMeta", key), err)
		}
		return nil
	}, commitTxn)

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

	err := kv.runWriteTxnWithRetry(ctx1, "removeTiKVMeta", func(txn *transaction.KVTxn) error {
		if err := txn.Delete([]byte(key)); err != nil {
			return wrapWriteBuildErr(fmt.Sprintf("Failed to remove key %s in removeTiKVMeta", key), err)
		}
		return nil
	}, commitTxn)

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
	err := errors.Wrap(merr.ErrServiceUnimplemented, "CompareVersionAndSwap is under deprecation")
	logWarnOnFailure(&err, "Unimplemented")
	return false, err
}

// CheckElapseAndWarn checks the elapsed time and warns if it is too long.
func CheckElapseAndWarn(start time.Time, message string, fields ...mlog.Field) bool {
	elapsed := time.Since(start)
	if elapsed.Milliseconds() > 2000 {
		mlog.Warn(context.TODO(),
			message, append([]mlog.Field{mlog.String("time spent", elapsed.String())}, fields...)...)
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
		return nil, merr.WrapErrParameterInvalidMsg("value for key is reserved by EmptyValue: %s", EmptyValueString)
	}
	return []byte(value), nil
}
