// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planparserv2

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

const (
	// exprCacheMaxBytes bounds the heap the parse cache is accounted to retain,
	// on top of its entry count and TTL. Short filters keep the full 1,024
	// entries under it (about 10 KiB each); only unusually large expressions
	// compete for it.
	exprCacheMaxBytes int64 = 64 << 20
	// exprCacheMaxEntryBytes is the largest single entry the cache admits. A
	// parse that would cost more is returned uncached rather than evicting an
	// eighth of the cache on its own. It sits at roughly a 24K-token
	// expression — an `in [...]` list of about 11K integers — so only filters
	// that already take several milliseconds to parse are parsed again.
	exprCacheMaxEntryBytes int64 = 8 << 20
)

// Accounting policy for what a cached parse retains (#53754). Measured on this
// parser with retained heap after GC over distinct expressions of each shape,
// both on the pooled parser and with each tree keeping a parser of its own: a
// parse tree keeps its input as 4-byte runes, the key, and 218–305 bytes per
// token (a typical 100-term filter 305, dense `+1+1…` arithmetic 305, nested
// parentheses 226, an `in [...]` list 218), plus a small fixed part — a
// 4-token tree retains about 2.5 KiB in all. Tokens, not bytes, are what grow a
// tree: per input byte the same measurements range from 5× (one long string
// literal) to 312× (dense arithmetic), so a byte-based bound is either far too
// loose or far too tight. With the constants below every measured shape retains
// at most 0.87 of what it is accounted. The fixed part is deliberately generous:
// short filters reach the 1,024-entry limit long before the byte budget.
const (
	treeCostFixed    int64 = 8 << 10
	treeCostPerRune  int64 = 4
	treeCostPerToken int64 = 350
	// A cached error keeps the expression as its key and its own message, which
	// quotes the input AFTER normalization — a Han character as a six-byte
	// escape, a tab as two bytes — so the message, not the key, is what to
	// measure it by.
	errorCostFixed int64 = 2 << 10
)

// treeCost is the accounted retained size of a cached parse tree. normalized is
// the string the lexer read, after convertHanToASCII; tokens counts them.
func treeCost(key, normalized string, tokens int) int64 {
	return treeCostFixed + int64(len(key)) + treeCostPerRune*int64(len(normalized)) + treeCostPerToken*int64(tokens)
}

// errorCost is the accounted retained size of a cached syntax error.
func errorCost(key string, err error) int64 {
	return errorCostFixed + int64(len(key)) + int64(len(err.Error()))
}

// exprCacheEntry is what the parse cache stores: a parse tree or a syntax
// error, with the bytes it is accounted to retain.
type exprCacheEntry struct {
	value any
	cost  int64
}

// boundedExprCache is the expression parse cache: the count-and-TTL LRU it has
// always been, plus a budget on the heap its entries retain. The count and TTL
// alone bound nothing in bytes — one cached 20 KB filter keeps about 1.3 MB of
// tree — so 1,024 large distinct expressions could hold gigabytes.
type boundedExprCache struct {
	lru *expirable.LRU[string, exprCacheEntry]
	// mu serializes writers. Reads go straight to the LRU, which locks itself.
	mu       sync.Mutex
	retained atomic.Int64
	maxBytes int64
	maxEntry int64
}

func newBoundedExprCache(size int, ttl time.Duration, maxBytes, maxEntry int64) *boundedExprCache {
	if maxEntry > maxBytes {
		maxEntry = maxBytes
	}
	c := &boundedExprCache{maxBytes: maxBytes, maxEntry: maxEntry}
	// The LRU calls this for every entry that leaves — evicted for space,
	// expired, removed or purged — under its own lock, so it must not take mu.
	c.lru = expirable.NewLRU[string, exprCacheEntry](size, func(_ string, e exprCacheEntry) {
		c.retained.Add(-e.cost)
	}, ttl)
	return c
}

// Get returns the cached tree or error for key.
func (c *boundedExprCache) Get(key string) (any, bool) {
	e, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	return e.value, true
}

// Add caches value at the given cost, unless that cost alone exceeds the
// per-entry limit, then evicts the oldest entries until the total is within
// budget again.
func (c *boundedExprCache) Add(key string, value any, cost int64) {
	if cost > c.maxEntry {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Two requests that miss on the same expression both add it. The LRU
	// replaces an existing key WITHOUT calling the eviction callback, which
	// would leave the first entry's cost counted after it is gone; Remove does
	// call it, and holding mu means nothing else can re-add in between.
	c.lru.Remove(key)
	c.retained.Add(cost)
	c.lru.Add(key, exprCacheEntry{value: value, cost: cost})
	for c.retained.Load() > c.maxBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			break
		}
	}
}

// Purge drops every entry.
func (c *boundedExprCache) Purge() {
	c.lru.Purge()
}

// Len is the number of cached entries.
func (c *boundedExprCache) Len() int {
	return c.lru.Len()
}

// Retained is the accounted size of every cached entry.
func (c *boundedExprCache) Retained() int64 {
	return c.retained.Load()
}
