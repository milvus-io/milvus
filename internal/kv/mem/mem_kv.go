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

package memkv

import (
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/google/btree"
	"github.com/milvus-io/milvus/pkg/common"
)

// MemoryKV implements BaseKv interface and relies on underling btree.BTree.
// As its name implies, all data is stored in memory.
type MemoryKV struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewMemoryKV returns an in-memory kvBase for testing.
func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		tree: btree.New(2),
	}
}

type Value interface {
	String() string
	ByteSlice() []byte
}

type StringValue string

func (v StringValue) String() string {
	return string(v)
}

func (v StringValue) ByteSlice() []byte {
	return []byte(v)
}

type ByteSliceValue []byte

func (v ByteSliceValue) String() string {
	return string(v)
}

func (v ByteSliceValue) ByteSlice() []byte {
	return v
}

type memoryKVItem struct {
	key   string
	value Value
}

var _ btree.Item = (*memoryKVItem)(nil)

// Less returns true if the item is less than the given one.
func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

// Load loads an object with @key.
func (kv *MemoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return "", common.NewKeyNotExistError(key)
	}
	return item.(memoryKVItem).value.String(), nil
}

// LoadBytes loads an object with @key.
func (kv *MemoryKV) LoadBytes(key string) ([]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return []byte{}, common.NewKeyNotExistError(key)
	}
	return item.(memoryKVItem).value.ByteSlice(), nil
}

// Get return value if key exists, or return empty string
func (kv *MemoryKV) Get(key string) string {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return ""
	}
	return item.(memoryKVItem).value.String()
}

// LoadBytesWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (kv *MemoryKV) LoadBytesWithDefault(key string, defaultValue []byte) []byte {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key: key})
	if item == nil {
		return defaultValue
	}
	return item.(memoryKVItem).value.ByteSlice()
}

// LoadBytesRange loads objects with range @startKey to @endKey with @limit number of objects.
func (kv *MemoryKV) LoadBytesRange(key, endKey string, limit int) ([]string, [][]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	keys := make([]string, 0, limit)
	values := make([][]byte, 0, limit)
	kv.tree.AscendRange(memoryKVItem{key: key}, memoryKVItem{key: endKey}, func(item btree.Item) bool {
		keys = append(keys, item.(memoryKVItem).key)
		values = append(values, item.(memoryKVItem).value.ByteSlice())
		if limit > 0 {
			return len(keys) < limit
		}
		return true
	})
	return keys, values, nil
}

// Save object with @key to btree. Object value is @value.
func (kv *MemoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, StringValue(value)})
	return nil
}

// SaveBytes object with @key to btree. Object value is @value.
func (kv *MemoryKV) SaveBytes(key string, value []byte) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, ByteSliceValue(value)})
	return nil
}

// Remove deletes an object with @key.
func (kv *MemoryKV) Remove(key string) error {
	kv.Lock()
	defer kv.Unlock()

	kv.tree.Delete(memoryKVItem{key: key})
	return nil
}

// MultiLoad loads objects with multi @keys.
func (kv *MemoryKV) MultiLoad(keys []string) ([]string, error) {
	kv.RLock()
	defer kv.RUnlock()
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		item := kv.tree.Get(memoryKVItem{key: key})
		result = append(result, item.(memoryKVItem).value.String())
	}
	return result, nil
}

// MultiLoadBytes loads objects with multi @keys.
func (kv *MemoryKV) MultiLoadBytes(keys []string) ([][]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	result := make([][]byte, 0, len(keys))
	for _, key := range keys {
		item := kv.tree.Get(memoryKVItem{key: key})
		result = append(result, item.(memoryKVItem).value.ByteSlice())
	}
	return result, nil
}

// MultiSave saves given key-value pairs in MemoryKV atomicly.
func (kv *MemoryKV) MultiSave(kvs map[string]string) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range kvs {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, StringValue(value)})
	}
	return nil
}

// MultiSaveBytes saves given key-value pairs in MemoryKV atomicly.
func (kv *MemoryKV) MultiSaveBytes(kvs map[string][]byte) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range kvs {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, ByteSliceValue(value)})
	}
	return nil
}

// MultiRemove removes given @keys in MemoryKV atomicly.
func (kv *MemoryKV) MultiRemove(keys []string) error {
	kv.Lock()
	defer kv.Unlock()
	for _, key := range keys {
		kv.tree.Delete(memoryKVItem{key: key})
	}
	return nil
}

// MultiSaveAndRemove saves and removes given key-value pairs in MemoryKV atomicly.
func (kv *MemoryKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range saves {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, StringValue(value)})
	}
	for _, key := range removals {
		kv.tree.Delete(memoryKVItem{key: key})
	}
	return nil
}

// MultiSaveBytesAndRemove saves and removes given key-value pairs in MemoryKV atomicly.
func (kv *MemoryKV) MultiSaveBytesAndRemove(saves map[string][]byte, removals []string) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range saves {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, ByteSliceValue(value)})
	}
	for _, key := range removals {
		kv.tree.Delete(memoryKVItem{key: key})
	}
	return nil
}

// LoadWithPrefix returns all keys & values with given prefix.
func (kv *MemoryKV) LoadWithPrefix(key string) ([]string, []string, error) {
	kv.Lock()
	defer kv.Unlock()

	var keys []string
	var values []string

	kv.tree.Ascend(func(i btree.Item) bool {
		if strings.HasPrefix(i.(memoryKVItem).key, key) {
			keys = append(keys, i.(memoryKVItem).key)
			values = append(values, i.(memoryKVItem).value.String())
		}
		return true
	})
	return keys, values, nil
}

// LoadBytesWithPrefix returns all keys & values with given prefix.
func (kv *MemoryKV) LoadBytesWithPrefix(key string) ([]string, [][]byte, error) {
	kv.Lock()
	defer kv.Unlock()

	var keys []string
	var values [][]byte

	kv.tree.Ascend(func(i btree.Item) bool {
		if strings.HasPrefix(i.(memoryKVItem).key, key) {
			keys = append(keys, i.(memoryKVItem).key)
			values = append(values, i.(memoryKVItem).value.ByteSlice())
		}
		return true
	})
	return keys, values, nil
}

// Close dummy close
func (kv *MemoryKV) Close() {
}

// MultiRemoveWithPrefix not implemented
func (kv *MemoryKV) MultiRemoveWithPrefix(keys []string) error {
	return errors.New("not implement")
}

// MultiSaveAndRemoveWithPrefix saves key-value pairs in @saves, & remove key with prefix in @removals in MemoryKV atomically.
func (kv *MemoryKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	kv.Lock()
	defer kv.Unlock()

	var keys []memoryKVItem
	for _, key := range removals {
		kv.tree.Ascend(func(i btree.Item) bool {
			if strings.HasPrefix(i.(memoryKVItem).key, key) {
				keys = append(keys, i.(memoryKVItem))
			}
			return true
		})
	}
	for _, item := range keys {
		kv.tree.Delete(item)
	}

	for key, value := range saves {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, StringValue(value)})
	}
	return nil
}

// MultiSaveBytesAndRemoveWithPrefix saves key-value pairs in @saves, & remove key with prefix in @removals in MemoryKV atomically.
func (kv *MemoryKV) MultiSaveBytesAndRemoveWithPrefix(saves map[string][]byte, removals []string) error {
	kv.Lock()
	defer kv.Unlock()

	var keys []memoryKVItem
	for _, key := range removals {
		kv.tree.Ascend(func(i btree.Item) bool {
			if strings.HasPrefix(i.(memoryKVItem).key, key) {
				keys = append(keys, i.(memoryKVItem))
			}
			return true
		})
	}
	for _, item := range keys {
		kv.tree.Delete(item)
	}

	for key, value := range saves {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, ByteSliceValue(value)})
	}
	return nil
}

// RemoveWithPrefix remove key of given prefix in MemoryKV atomicly.
func (kv *MemoryKV) RemoveWithPrefix(key string) error {
	kv.Lock()
	defer kv.Unlock()

	var keys []btree.Item

	kv.tree.Ascend(func(i btree.Item) bool {
		if strings.HasPrefix(i.(memoryKVItem).key, key) {
			keys = append(keys, i.(memoryKVItem))
		}
		return true
	})
	for _, item := range keys {
		kv.tree.Delete(item)
	}
	return nil
}

func (kv *MemoryKV) Has(key string) (bool, error) {
	kv.Lock()
	defer kv.Unlock()
	return kv.tree.Has(memoryKVItem{key: key}), nil
}
