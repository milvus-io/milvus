package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPinnableLruCache(t *testing.T) {
	evictor := NewLRUEvictor[int]()
	measure := &defaultMeasure[int, int]{size: 0}
	cache := NewCacheImpl(100, evictor, measure)
	assert.Equal(t, uint64(100), cache.Capacity())
	cache.Put(1, 1)
	cache.Unpin(1)
	assert.Equal(t, uint64(1), cache.Size())
	v, ok := cache.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 1, v)
	_, _, err := cache.Evict()
	assert.EqualError(t, err, "no eviction possible")
	ok = cache.Remove(1)
	assert.False(t, ok)
	cache.Unpin(1)
	k, v, err := cache.Evict()
	assert.NoError(t, err)
	assert.Equal(t, 1, k)
	assert.Equal(t, 1, v)
	assert.Equal(t, uint64(0), cache.Size())
	ok = cache.Remove(2)
	assert.True(t, ok)
}
