// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"sync"

	"github.com/google/btree"
)

type memoryKV struct {
	sync.RWMutex
	tree *btree.BTree
}

// NewMemoryKV returns an in-memory kvBase for testing.
func NewMemoryKV() Base {
	return &memoryKV{
		tree: btree.New(2),
	}
}

type memoryKVItem struct {
	key, value string
}

func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

func (kv *memoryKV) Load(key string) (string, error) {
	kv.RLock()
	defer kv.RUnlock()
	item := kv.tree.Get(memoryKVItem{key, ""})
	if item == nil {
		return "", nil
	}
	return item.(memoryKVItem).value, nil
}

func (kv *memoryKV) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
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

func (kv *memoryKV) Save(key, value string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

func (kv *memoryKV) Remove(key string) error {
	kv.Lock()
	defer kv.Unlock()

	kv.tree.Delete(memoryKVItem{key, ""})
	return nil
}
