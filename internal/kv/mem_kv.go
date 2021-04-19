package kv

import (
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
	if item == nil {
		return "", nil
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
	panic("implement me")
}

func (kv *MemoryKV) Close() {
}
