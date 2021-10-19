// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package memkv

import (
	"fmt"
	"strings"
	"sync"

	"github.com/google/btree"
)

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

func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

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

func (kv *MemoryKV) LoadWithDefault(key string, defaultValue string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key, ""})

	if item == nil {
		return defaultValue, nil
	}
	return item.(memoryKVItem).value, nil
}

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

func (kv *MemoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

func (kv *MemoryKV) Remove(key string) error {
	kv.Lock()
	defer kv.Unlock()

	kv.tree.Delete(memoryKVItem{key, ""})
	return nil
}

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

func (kv *MemoryKV) MultiSave(kvs map[string]string) error {
	kv.Lock()
	defer kv.Unlock()
	for key, value := range kvs {
		kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	}
	return nil
}

func (kv *MemoryKV) MultiRemove(keys []string) error {
	kv.Lock()
	defer kv.Unlock()
	for _, key := range keys {
		kv.tree.Delete(memoryKVItem{key, ""})
	}
	return nil
}

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

// todo
func (kv *MemoryKV) LoadWithPrefix(key string) ([]string, []string, error) {
	kv.Lock()
	defer kv.Unlock()

	keys := make([]string, 0)
	values := make([]string, 0)

	kv.tree.Ascend(func(i btree.Item) bool {
		if strings.HasPrefix(i.(memoryKVItem).key, key) {
			keys = append(keys, i.(memoryKVItem).key)
			values = append(values, i.(memoryKVItem).value)
		}
		return true
	})
	return keys, values, nil
}

func (kv *MemoryKV) Close() {
}

func (kv *MemoryKV) MultiRemoveWithPrefix(keys []string) error {
	panic("not implement")
}
func (kv *MemoryKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	kv.Lock()
	defer kv.Unlock()

	keys := make([]memoryKVItem, 0)
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

func (kv *MemoryKV) RemoveWithPrefix(key string) error {
	kv.Lock()
	defer kv.Unlock()

	keys := make([]btree.Item, 0)

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

// item already in memory, just slice the value.
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
