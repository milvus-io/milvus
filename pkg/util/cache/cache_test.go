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
		item, missing, ok := cache.GetAndPin(i)
		s.True(ok)
		s.True(missing)
		s.Equal(i, item.Value())
		item.Unpin()
	}

	for i := 1; i <= size; i++ {
		item, missing, ok := cache.GetAndPin(i)
		s.True(ok)
		s.False(missing)
		s.Equal(i, item.Value())
		item.Unpin()
	}

	s.False(cache.Contain(size + 1))
	for i := 1; i <= size; i++ {
		item, missing, ok := cache.GetAndPin(size + i)
		s.True(ok)
		s.True(missing)
		s.Equal(size+i, item.Value())
		s.False(cache.Contain(i))
		item.Unpin()
	}
}

func TestCacheSuite(t *testing.T) {
	suite.Run(t, new(CacheSuite))
}
