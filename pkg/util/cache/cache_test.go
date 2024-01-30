package cache

import "github.com/stretchr/testify/suite"

type CacheSuite struct {
	suite.Suite
}

func (s *CacheSuite) TestLRUCache() {
	cache := NewLRUCache[int](2)
	cache.Set("1", 1)
	cache.Set("2", 2)

	value, ok := cache.Get("1")
	s.True(ok)
	s.Equal(1, value)

	value, ok = cache.Get("2")
	s.True(ok)
	s.Equal(2, value)

	cache.Set("3", 3)
	value, ok = cache.Get("3")
	s.True(ok)
	s.Equal(3, value)

	value, ok = cache.Get("1")
	s.False(ok)
}
