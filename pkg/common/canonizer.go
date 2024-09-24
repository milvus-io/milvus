package common

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

type Canonizer[K comparable, V any] struct {
	cache  *expirable.LRU[K, V]
	loader func(K) V
}

func (c *Canonizer[K, V]) Canonize(key K) (canonized V, ok bool) {
	canonized, ok = c.cache.Get(key)
	if !ok {
		canonized := c.loader(key)
		c.cache.Add(key, canonized)
	}
	return
}

func NewCanonizer[K comparable, V any](capacity int, expiration time.Duration, loader func(K) V) *Canonizer[K, V] {
	return &Canonizer[K, V]{
		cache:  expirable.NewLRU[K, V](capacity, nil, expiration),
		loader: loader,
	}
}
