package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CacheSuite struct {
	suite.Suite
}

func (s *CacheSuite) TestLRUCache() {
	size := 100
	cache := NewLRUCache[int](int64(size))

	for i := 1; i <= size; i++ {
		cache.Set(fmt.Sprint(i), i)
		value, ok := cache.Get(fmt.Sprint(i))
		s.True(ok)
		s.Equal(i, value)
	}

	_, ok := cache.Get(fmt.Sprint(size + 1))
	s.False(ok)

	for i := 1; i <= size; i++ {
		cache.Set(fmt.Sprint(size+i), size+i)
		_, ok = cache.Get(fmt.Sprint(size + i))
		s.True(ok)
	}

	for i := 1; i <= size; i++ {
		_, ok = cache.Get(fmt.Sprint(i))
		s.False(ok)
	}
}

func TestCacheSuite(t *testing.T) {
	suite.Run(t, new(CacheSuite))
}
