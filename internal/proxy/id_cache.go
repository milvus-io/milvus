package proxy

import (
	"strconv"
	"time"

	"github.com/patrickmn/go-cache"
)

type idCache struct {
	cache *cache.Cache
}

func newIDCache(defaultExpiration, cleanupInterval time.Duration) *idCache {
	c := cache.New(defaultExpiration, cleanupInterval)
	return &idCache{
		cache: c,
	}
}

func (r *idCache) Set(id UniqueID, value bool) {
	r.cache.Set(strconv.FormatInt(id, 36), value, 0)
}

func (r *idCache) Get(id UniqueID) (value bool, exists bool) {
	valueRaw, exists := r.cache.Get(strconv.FormatInt(id, 36))
	if valueRaw == nil {
		return false, exists
	}
	return valueRaw.(bool), exists
}
