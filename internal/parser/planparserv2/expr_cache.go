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

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	defaultExprCacheMaxBytes      int64 = 64 << 20 // 64 MiB
	defaultExprCacheMaxEntryBytes int64 = 8 << 20  // 8 MiB
	exprCacheEntryBaseBytes       int64 = 128
	exprCacheBytesPerToken        int64 = 256
)

// Capture static configuration on first use, after paramtable initialization.
// No configuration lookup or tree-size estimation is needed on subsequent parses.
var exprCache = sync.OnceValue(func() *expressionCache {
	return newExpressionCache(paramtable.Get())
})

type cachedExprEntry struct {
	value any
	cost  int64
}

type expressionCache struct {
	mu            sync.Mutex
	cache         *expirable.LRU[string, cachedExprEntry]
	maxBytes      int64
	maxEntryBytes int64
	// Updated inside expirable.LRU's onEvict callback (including background TTL expiry),
	// so it must remain lock-free to avoid lock-order inversion with mu.
	currentBytes atomic.Int64
}

func newExpressionCache(params *paramtable.ComponentParam) *expressionCache {
	cfg := &params.ProxyCfg
	c := &expressionCache{}
	if !cfg.ExprParseCacheEnabled.GetAsBool() {
		return c
	}

	maxBytes := cfg.ExprParseCacheMaxBytes.GetAsInt64()
	if maxBytes <= 0 {
		maxBytes = defaultExprCacheMaxBytes
	}
	maxEntryBytes := cfg.ExprParseCacheMaxEntryBytes.GetAsInt64()
	if maxEntryBytes <= 0 {
		maxEntryBytes = defaultExprCacheMaxEntryBytes
	}
	if maxEntryBytes > maxBytes {
		maxEntryBytes = maxBytes
	}

	c.maxBytes = maxBytes
	c.maxEntryBytes = maxEntryBytes
	c.cache = expirable.NewLRU[string, cachedExprEntry](
		cfg.ExprParseCacheMaxEntries.GetAsInt(),
		func(_ string, entry cachedExprEntry) {
			c.currentBytes.Add(-entry.cost)
		},
		10*time.Minute,
	)
	return c
}

// estimateASTCacheBytes conservatively accounts for the cache key, the UTF-32
// rune buffer retained by antlr.InputStream, and the buffered tokens + parse tree nodes.
func estimateASTCacheBytes(exprStr, exprNormal string, tokenCount int) int64 {
	if tokenCount < 0 {
		tokenCount = 0
	}
	return int64(len(exprStr)) + 4*int64(len(exprNormal)) + exprCacheBytesPerToken*int64(tokenCount) + exprCacheEntryBaseBytes
}

func estimateErrorCacheBytes(exprStr string, err error) int64 {
	errLen := 0
	if err != nil {
		errLen = len(err.Error())
	}
	return int64(len(exprStr)) + int64(errLen) + exprCacheEntryBaseBytes
}

func (c *expressionCache) enabled() bool {
	return c.cache != nil
}

func (c *expressionCache) get(key string) (any, bool) {
	if c.cache == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}
	return entry.value, true
}

func (c *expressionCache) add(key string, value any, cost int64) {
	if c.cache == nil || cost <= 0 || cost > c.maxEntryBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Remove any existing (including expired-but-unswept) entry so onEvict
	// subtracts its old cost before we account for the replacement.
	c.cache.Remove(key)
	c.currentBytes.Add(cost)
	c.cache.Add(key, cachedExprEntry{value: value, cost: cost})
	for c.currentBytes.Load() > c.maxBytes {
		if _, _, ok := c.cache.RemoveOldest(); !ok {
			break
		}
	}
}

func (c *expressionCache) purge() {
	if c.cache == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Purge()
}
