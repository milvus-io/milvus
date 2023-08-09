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
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

const (
	MaxScanLimit = 10240
)

// implementation assertion
var _ kv.MetaKv = (*rawTiKV)(nil)

// rawTiKV implements MetaKv (non-TxnKV part) interface. It supports processing multiple kvs in a batch(not transaction) manner.
type rawTiKV struct {
	client   *rawkv.Client
	rootPath string
}

// NewRawTiKV creates a new rawTiKV client.
func NewRawTiKV(rawkv *rawkv.Client, rootPath string) *rawTiKV {
	kv := &rawTiKV{
		client:   rawkv,
		rootPath: rootPath,
	}
	return kv
}

// Close closes the connection to TiKV.
func (kv *rawTiKV) Close() {
	log.Debug("rawTiKV closed", zap.String("path", kv.rootPath))
}

// GetPath returns the path of the key/prefix.
func (kv *rawTiKV) GetPath(key string) string {
	return path.Join(kv.rootPath, key)
}

// Has returns if a key exists.
func (kv *rawTiKV) Has(key string) (bool, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	val, err := kv.client.Get(ctx, []byte(key))
	if err != nil {
		if strings.HasPrefix(err.Error(), "Failed to get value for key") {
			return false, nil
		} else {
			return false, errors.Wrap(err, fmt.Sprintf("Failed to check Has for key %s", key))
		}
	}
	if len(val) == 0 {
		return false, nil
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation Has()", zap.String("key", key))
	return true, nil
}

// HasPrefix returns if a key prefix exists.
func (kv *rawTiKV) HasPrefix(prefix string) (bool, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	keys, _, err := kv.client.Scan(ctx, startKey, endKey, 1, rawkv.ScanKeyOnly())
	if err != nil {
		log.Warn("rawTiKV HasPrefix error", zap.String("prefix", prefix), zap.Error(err))
		err = errors.Wrap(err, fmt.Sprintf("Failed to check HasPrefix for prefix %s", prefix))
	}
	r := len(keys) > 0
	CheckElapseAndWarn(start, "Slow load with HasPrefix", zap.String("prefix", prefix))
	return r, err
}

// Load returns value of the key.
func (kv *rawTiKV) Load(key string) (string, error) {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()
	v, err := kv.client.Get(ctx, []byte(key))
	value := string(v)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to load key %s", key))
	}
	if len(value) == 0 {
		return "", common.NewKeyNotExistError(key)
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation load", zap.String("key", key))
	return value, nil
}

// Helper function to convert a slice of strings to a slice of byte slices.
func toByteKeys(keys []string, rootPath string) [][]byte {
	bytesSlice := make([][]byte, len(keys))
	for i, key := range keys {
		bytesSlice[i] = []byte(path.Join(rootPath, key))
	}
	return bytesSlice
}

// Helper function to convert a slice of byte slices to a slice of strings.
func toStringSlice(bytes [][]byte) []string {
	r := make([]string, len(bytes))
	for i, b := range bytes {
		if b == nil {
			r[i] = ""
		} else {
			r[i] = string(b)
		}
	}
	return r
}

// MultiLoad gets the values of input keys in a transaction.
func (kv *rawTiKV) MultiLoad(keys []string) ([]string, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()
	bytes, err := kv.client.BatchGet(ctx, toByteKeys(keys, kv.rootPath))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to MultiLoad keys")
	}
	r := make([]string, len(bytes))
	invalid := make([]string, 0, len(keys))
	for i, b := range bytes {
		if b == nil {
			invalid = append(invalid, keys[i])
			r[i] = ""
		} else {
			r[i] = string(b)
		}
	}
	if len(invalid) != 0 {
		log.Warn("MultiLoad: there are invalid keys", zap.Strings("keys", invalid))
		return r, fmt.Errorf("there are invalid keys: %s", invalid)
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation multi load", zap.Any("keys", keys))
	return r, nil
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *rawTiKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	keys, values, err := kv.client.Scan(ctx, startKey, endKey, MaxScanLimit)
	if err != nil {
		log.Warn("rawTiKV LoadWithPrefix error", zap.String("prefix", prefix), zap.Error(err))
		return nil, nil, errors.Wrap(err, fmt.Sprintf("Failed to LoadWithPrefix for prefix %s", prefix))
	}
	strKeys := toStringSlice(keys)
	CheckElapseAndWarn(start, "Slow rawTiKV operation load with prefix", zap.Strings("keys", strKeys))
	return strKeys, toStringSlice(values), nil
}

// Save saves the input key-value pair.
func (kv *rawTiKV) Save(key, value string) error {
	start := time.Now()
	rs := timerecord.NewTimeRecorder("putTiKVMeta")
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	elapsed := rs.ElapseSpan()
	err := kv.client.Put(ctx, []byte(key), []byte(value))

	metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.TotalLabel).Inc()
	if err == nil {
		metrics.MetaKvSize.WithLabelValues(metrics.MetaPutLabel).Observe(float64(len(value)))
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaPutLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaPutLabel, metrics.FailLabel).Inc()
		err = errors.Wrap(err, fmt.Sprintf("Failed to save key %s", key))
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation save", zap.String("key", key))
	return err
}

// MultiSave saves the input key-value pairs in transaction manner.
func (kv *rawTiKV) MultiSave(kvs map[string]string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	keys := make([][]byte, len(kvs))
	values := make([][]byte, len(kvs))
	for key, value := range kvs {
		key = path.Join(kv.rootPath, key)
		keys = append(keys, []byte(key))
		values = append(values, []byte(value))
	}
	err := kv.client.BatchPut(ctx, keys, values)
	if err != nil {
		log.Warn("rawTiKV MultiSave error", zap.Any("kvs", kvs), zap.Int("len", len(kvs)), zap.Error(err))
		err = errors.Wrap(err, "Failed to MultiSave")
	}

	CheckElapseAndWarn(start, "Slow rawTiKV operation multi save", zap.Any("kvs", kvs))
	return err
}

// Remove removes the input key.
func (kv *rawTiKV) Remove(key string) error {
	start := time.Now()
	key = path.Join(kv.rootPath, key)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	rs := timerecord.NewTimeRecorder("removeTiKVMeta")
	err := kv.client.Delete(ctx, []byte(key))
	elapsed := rs.ElapseSpan()
	metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.TotalLabel).Inc()

	if err == nil {
		metrics.MetaRequestLatency.WithLabelValues(metrics.MetaRemoveLabel).Observe(float64(elapsed.Milliseconds()))
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.SuccessLabel).Inc()
	} else {
		metrics.MetaOpCounter.WithLabelValues(metrics.MetaRemoveLabel, metrics.FailLabel).Inc()
		err = errors.Wrap(err, fmt.Sprintf("Failed to Remove key %s", key))
	}

	CheckElapseAndWarn(start, "Slow rawTiKV operation remove", zap.String("key", key))
	return err
}

// MultiRemove removes the input keys in transaction manner.
func (kv *rawTiKV) MultiRemove(keys []string) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	err := kv.client.BatchDelete(ctx, toByteKeys(keys, kv.rootPath))
	if err != nil {
		log.Warn("rawTiKV MultiRemove error", zap.Strings("keys", keys), zap.Int("len", len(keys)), zap.Error(err))
		return errors.Wrap(err, "Failed to MultiRemove keys")
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation multi remove", zap.Strings("keys", keys))
	return nil
}

// RemoveWithPrefix removes the keys for the given prefix.
func (kv *rawTiKV) RemoveWithPrefix(prefix string) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	keys, _, err := kv.client.Scan(ctx, startKey, endKey, MaxScanLimit, rawkv.ScanKeyOnly())
	if err != nil {
		log.Warn("rawTiKV RemoveWithPrefix error", zap.String("prefix", prefix), zap.Error(err))
		return errors.Wrap(err, fmt.Sprintf("Failed to RemoveWithPrefix %s", prefix))
	}
	err = kv.client.BatchDelete(ctx, keys)
	if err != nil {
		log.Warn("rawTiKV RemoveWithPrefix error", zap.String("prefix", prefix), zap.Error(err))
		return errors.Wrap(err, fmt.Sprintf("Failed to BatchDelete for RemoveWithPrefix %s", prefix))
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation remove with prefix", zap.String("prefix", prefix))
	return nil
}

// WalkWithPrefix visits each kv with input prefix and apply given fn to it.
func (kv *rawTiKV) WalkWithPrefix(prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	start := time.Now()
	prefix = path.Join(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	keys, values, err := kv.client.Scan(ctx, startKey, endKey, MaxScanLimit)
	if err != nil {
		return errors.Wrap(err, "Failed to scan for WalkWithPagination")
	}
	for i, key := range keys {
		if err = fn(key, values[i]); err != nil {
			log.Warn("rawTiKV WalkWithPagination error", zap.String("prefix", prefix), zap.Error(err))
			return errors.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s, %s) for RemoveWithPrefix", string(key), string(values[i])))
		}
	}
	CheckElapseAndWarn(start, "Slow rawTiKV operation(WalkWithPagination)", zap.String("prefix", prefix))
	return nil
}

func (kv *rawTiKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	return fmt.Errorf("Unimplemented! MultiSaveAndRemove is not supported by rawTiKV.")
}

func (kv *rawTiKV) MultiRemoveWithPrefix(prefixes []string) error {
	return fmt.Errorf("Unimplemented! MultiRemoveWithPrefix is not supported by rawTiKV.")
}

func (kv *rawTiKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	return fmt.Errorf("Unimplemented! MultiSaveAndRemoveWithPrefix is not supported by rawTiKV.")
}

func (kv *rawTiKV) CompareVersionAndSwap(key string, version int64, target string) (bool, error) {
	return false, fmt.Errorf("Unimplemented! CompareVersionAndSwap is under deprecation")
}
