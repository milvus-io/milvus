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
	"fmt"
	"strings"
	"sync"

	"github.com/google/btree"
)

// MemoryKV implements DataKV interface and relies on underling btree.BTree.
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

type memoryKVItem struct {
	key, value string
}

// Less returns true if the item is less than the given one.
func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

// Load loads an object with @key.
func (kv *MemoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key, ""})
	// TODO，load unexisted key behavior is weird
	if item == nil {
		return "", nil
	}
	return item.(memoryKVItem).value, nil
}

// LoadWithDefault loads an object with @key. If the object does not exist, @defaultValue will be returned.
func (kv *MemoryKV) LoadWithDefault(key, defaultValue string) string {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key, ""})

	if item == nil {
		return defaultValue
	}
	return item.(memoryKVItem).value
}

// LoadRange loads objects with range @startKey to @endKey with @limit number of objects.
func (kv *MemoryKV) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	kv.RLock()
	defer kv.RUnlock()
	keys := make([]string, 0, limit)
	values := make([]string, 0, limit)
	kv.tree.AscendRange(memoryKVItem{key, ""}, memoryKVItem{endKey, ""}, func(item btree.Item) bool {
		keys = append(keys, item.(memoryKVItem).key)
		values = append(values, item.(memoryKVItem).value)
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
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

// Remove deletes an object with @key.
func (kv *MemoryKV) Remove(key string) error {
	kv.Lock()
	defer kv.Unlock()

	kv.tree.Delete(memoryKVItem{key, ""})
	return nil
}

// MultiLoad loads objects with multi @keys.
func (kv *MemoryKV) MultiLoad(keys []string) ([]string, error) {
	kv.RLock()
	defer kv.RUnlock()
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		item := kv.tree.Get(memoryKVItem{key, ""})
		result = append(result, item.(memoryKVItem).value)
	}
	return result, nil
}

// MultiSave saves given key-value pairs in MemoryKV atomicly.
func (kv *MemoryKV) MultiSave(kvs map[string]string) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range kvs {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	}
	return nil
}

// MultiRemove removes given @keys in MemoryKV atomicly.
func (kv *MemoryKV) MultiRemove(keys []string) error {
	kv.Lock()
	defer kv.Unlock()
	for _, key := range keys {
		kv.tree.Delete(memoryKVItem{key, ""})
	}
	return nil
}

// MultiSaveAndRemove saves and removes given key-value pairs in MemoryKV atomicly.
func (kv *MemoryKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range saves {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	}
	for _, key := range removals {
		kv.tree.Delete(memoryKVItem{key, ""})
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
			values = append(values, i.(memoryKVItem).value)
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
	panic("not implement")
}

// MultiSaveAndRemoveWithPrefix saves key-value pairs in @saves, & remove key with prefix in @removals in MemoryKV atomicly.
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
		kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
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

// LoadPartial item already in memory, just slice the value.
func (kv *MemoryKV) LoadPartial(key string, start, end int64) ([]byte, error) {
	value, err := kv.Load(key)
	if err != nil {
		return nil, err
	}
	switch {
	case 0 <= start && start < end && end <= int64(len(value)):
		return []byte(value[start:end]), nil
	default:
		return nil, fmt.Errorf("invalid range specified: start=%d end=%d",
			start, end)
	}
}

func (kv *MemoryKV) GetSize(key string) (int64, error) {
	value, err := kv.Load(key)
	if err != nil {
		return 0, err
	}

	return int64(len(value)), nil
}
