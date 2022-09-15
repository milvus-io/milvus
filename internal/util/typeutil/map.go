package typeutil

import "sync"

// MergeMap merge one map to another
func MergeMap(src map[string]string, dst map[string]string) map[string]string {
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// GetMapKeys return keys of a map
func GetMapKeys(src map[string]string) []string {
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	return keys
}

type ConcurrentMap[K comparable, V any] struct {
	inner sync.Map
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{}
}

func (m *ConcurrentMap[K, V]) Insert(key K, value V) {
	m.inner.Store(key, value)
}

func (m *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	var zeroValue V
	value, ok := m.inner.Load(key)
	if !ok {
		return zeroValue, ok
	}
	return value.(V), true
}

func (m *ConcurrentMap[K, V]) get(key K) (V, bool) {
	var zeroValue V
	value, ok := m.inner.Load(key)
	if !ok {
		return zeroValue, ok
	}
	return value.(V), true
}

func (m *ConcurrentMap[K, V]) GetOrInsert(key K, value V) (V, bool) {
	var zeroValue V
	loaded, exist := m.inner.LoadOrStore(key, value)
	if !exist {
		return zeroValue, exist
	}
	return loaded.(V), true
}

func (m *ConcurrentMap[K, V]) GetAndRemove(key K) (V, bool) {
	var zeroValue V
	value, ok := m.inner.LoadAndDelete(key)
	if !ok {
		return zeroValue, ok
	}
	return value.(V), true
}

func (m *ConcurrentMap[K, V]) Remove(key K) {
	m.inner.Delete(key)
}
