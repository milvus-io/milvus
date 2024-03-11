package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type CacheSuite struct {
	suite.Suite
}

func (s *CacheSuite) TestLRUCache() {
	size := 10
	cache := NewLRUCache[int, int](int32(size), func(key int) (int, bool) {
		return key, true
	}, nil)

	for i := 1; i <= size; i++ {
		item, ok := cache.GetAndPin(i)
		s.True(ok)
		s.Equal(i, item.Value())
		item.Unpin()
	}

	s.False(cache.Contain(size + 1))
	for i := 1; i <= size; i++ {
		item, ok := cache.GetAndPin(size + i)
		s.True(ok)
		s.Equal(size+i, item.Value())
		s.False(cache.Contain(i))
		item.Unpin()
	}
}

func (s *CacheSuite) TestTryPin() {
	size := 3
	cache := NewLRUCache[int, int](int32(size), func(key int) (int, bool) {
		return key, true
	}, nil)

	_, ok := cache.TryPin([]int{1, 2, 3, 4}...)
	s.False(ok)

	_, ok = cache.TryPin([]int{1, 2, 3}...)
	s.True(ok)
	items, ok := cache.TryPin([]int{1, 2, 3}...)
	s.True(ok)
	for _, item := range items {
		s.EqualValues(2, item.pinCount.Load())
	}

	ok = cache.Get(1)
	s.True(ok)
	ok = cache.Get(4)
	s.False(ok)

	for _, item := range items {
		item.Unpin()
		item.Unpin()
	}

	_, ok = cache.TryPin([]int{1, 2, 4}...)
	s.True(ok)
	ok = cache.Get(4)
	s.True(ok)
	ok = cache.Get(3)
	s.False(ok)
}

func TestCacheSuite(t *testing.T) {
	suite.Run(t, new(CacheSuite))
}
