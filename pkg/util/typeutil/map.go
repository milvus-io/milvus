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

func (m *ConcurrentMap[K, V]) Range(f func(key K, value V) bool) {
	m.inner.Range(func(key, value any) bool {
		trueKey := key.(K)
		trueValue := value.(V)
		return f(trueKey, trueValue)
	})
}

// Insert inserts the key-value pair to the concurrent map
func (m *ConcurrentMap[K, V]) Insert(key K, value V) {
	_, loaded := m.inner.LoadOrStore(key, value)
	if !loaded {
		m.len.Inc()
	} else {
		m.inner.Store(key, value)
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

func (m *ConcurrentMap[K, V]) Contain(key K) bool {
	_, ok := m.Get(key)
	return ok
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

// Remove removes the `key`, `value` set if `key` is in the map,
// does nothing if `key` not in the map.
func (m *ConcurrentMap[K, V]) Remove(key K) {
	if _, loaded := m.inner.LoadAndDelete(key); loaded {
		m.len.Dec()
	}
}

func (m *ConcurrentMap[K, V]) Len() int {
	return int(m.len.Load())
}

func (m *ConcurrentMap[K, V]) Values() []V {
	ret := make([]V, 0, m.Len())
	m.inner.Range(func(key, value any) bool {
		ret = append(ret, value.(V))
		return true
	})
	return ret
}

func (m *ConcurrentMap[K, V]) Keys() []K {
	ret := make([]K, 0, m.Len())
	m.inner.Range(func(key, value any) bool {
		ret = append(ret, key.(K))
		return true
	})
	return ret
}
