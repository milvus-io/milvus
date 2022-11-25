package typeutil

import (
	"sync"

	"go.uber.org/atomic"
)

// MapEqual returns true if the two map contain the same keys and values
func MapEqual(left, right map[int64]int64) bool {
	if len(left) != len(right) {
		return false
	}

	for k, v := range left {
		if v2, ok := right[k]; !ok || v != v2 {
			return false
		}
	}
	return true
}

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
	// Self-managed Len(), see: https://github.com/golang/go/issues/20680.
	len atomic.Uint64
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{}
}

func (m *ConcurrentMap[K, V]) Len() uint64 {
	return m.len.Load()
}

// InsertIfNotPresent inserts the key-value pair to the concurrent map if the key does not exist. It is otherwise a no-op.
func (m *ConcurrentMap[K, V]) InsertIfNotPresent(key K, value V) {
	if _, loaded := m.inner.LoadOrStore(key, value); !loaded {
		m.len.Inc()
	}
}

func (m *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	var zeroValue V
	value, ok := m.inner.Load(key)
	if !ok {
		return zeroValue, ok
	}
	return value.(V), true
}

// GetOrInsert returns the `value` and `loaded` on the given `key`, `value` set.
// If the key already exists, return the value and set `loaded` to true.
// If the key does not exist, insert the given `key` and `value` to map, return the value and set `loaded` to false.
func (m *ConcurrentMap[K, V]) GetOrInsert(key K, value V) (V, bool) {
	stored, loaded := m.inner.LoadOrStore(key, value)
	if !loaded {
		m.len.Inc()
		return stored.(V), false
	}
	return stored.(V), true
}

func (m *ConcurrentMap[K, V]) GetAndRemove(key K) (V, bool) {
	var zeroValue V
	value, loaded := m.inner.LoadAndDelete(key)
	if !loaded {
		return zeroValue, false
	}
	m.len.Dec()
	return value.(V), true
}
