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
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func purgeExprCache() {
	exprCache().purge()
}

// Each case models a fresh process with its own static configuration snapshot.
// These tests must not run in parallel because the parser cache is process-wide.
func configureExprCache(t *testing.T, enabled, maxEntries string) {
	t.Helper()
	configureExprCacheWithBytes(t, enabled, maxEntries, "67108864", "8388608")
}

func configureExprCacheWithBytes(t *testing.T, enabled, maxEntries, maxBytes, maxEntryBytes string) {
	t.Helper()
	params := paramtable.Get()
	for item, value := range map[*paramtable.ParamItem]string{
		&params.ProxyCfg.ExprParseCacheEnabled:       enabled,
		&params.ProxyCfg.ExprParseCacheMaxEntries:    maxEntries,
		&params.ProxyCfg.ExprParseCacheMaxBytes:      maxBytes,
		&params.ProxyCfg.ExprParseCacheMaxEntryBytes: maxEntryBytes,
	} {
		old := item.GetValue()
		require.NoError(t, params.Save(item.Key, value))
		t.Cleanup(func() { require.NoError(t, params.Save(item.Key, old)) })
	}
	original := exprCache
	exprCache = sync.OnceValue(func() *expressionCache { return newExpressionCache(params) })
	t.Cleanup(func() {
		purgeExprCache()
		exprCache = original
	})
}

func TestExprCacheReuse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expr    string
		invalid bool
	}{
		{name: "valid", expr: "id == 1"},
		{name: "unicode", expr: `s=="中文"`},
		{name: "syntax error", expr: "id ==", invalid: true},
		{name: "trailing token", expr: "id id", invalid: true},
		{name: "large tree", expr: `id not in ["a"]` + strings.Repeat(" ", 1<<20)},
		{name: "large error", expr: "id ==" + strings.Repeat(" ", 1<<20), invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configureExprCache(t, "true", "2")
			first, firstErr := handleInternal(tc.expr)
			second, secondErr := handleInternal(tc.expr)
			if tc.invalid {
				require.Error(t, firstErr)
				require.EqualError(t, secondErr, firstErr.Error())
				require.Same(t, firstErr, secondErr)
			} else {
				require.NoError(t, firstErr)
				require.NoError(t, secondErr)
				require.Same(t, first, second)
			}
			require.True(t, exprCache().cache.Contains(tc.expr))
		})
	}
}

func TestExprCacheDisabled(t *testing.T) {
	configureExprCache(t, "false", "1024")
	first, err := handleInternal("id == 1")
	require.NoError(t, err)
	second, err := handleInternal("id == 1")
	require.NoError(t, err)
	require.NotSame(t, first, second)
	_, firstErr := handleInternal("id ==")
	_, secondErr := handleInternal("id ==")
	require.Error(t, firstErr)
	require.EqualError(t, secondErr, firstErr.Error())
	require.NotSame(t, firstErr, secondErr)
	require.Nil(t, exprCache().cache, "disabled cache must not allocate an LRU or expiry goroutine")
}

func TestExprCacheConfiguredCapacity(t *testing.T) {
	configureExprCache(t, "true", "2")
	first, err := handleInternal("id == 1")
	require.NoError(t, err)
	_, err = handleInternal("id ==") // Errors share the capacity with trees.
	require.Error(t, err)
	hit, err := handleInternal("id == 1") // Refresh the first entry's LRU position.
	require.NoError(t, err)
	require.Same(t, first, hit)
	_, err = handleInternal("id == 3")
	require.NoError(t, err)
	require.Equal(t, 2, exprCache().cache.Len())
	require.False(t, exprCache().cache.Contains("id =="))
	hit, err = handleInternal("id == 1")
	require.NoError(t, err)
	require.Same(t, first, hit)
}

func TestExprCacheStaticConfiguration(t *testing.T) {
	configureExprCache(t, "true", "2")
	first, err := handleInternal("id == 1")
	require.NoError(t, err)
	params := paramtable.Get()
	require.NoError(t, params.Save("proxy.exprParseCache.enabled", "false"))
	require.NoError(t, params.Save("proxy.exprParseCache.maxEntries", "1"))
	_, err = handleInternal("id == 2")
	require.NoError(t, err)
	hit, err := handleInternal("id == 1")
	require.NoError(t, err)
	require.Same(t, first, hit, "both settings must remain fixed until restart")
	require.Equal(t, 2, exprCache().cache.Len())
}

func TestExprCacheConcurrentFirstUse(t *testing.T) {
	configureExprCache(t, "true", "2")
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := handleInternal("id == 1")
			if err != nil {
				t.Errorf("parse failed: %v", err)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 1, exprCache().cache.Len())
}

func TestExprCacheByteBudgetEviction(t *testing.T) {
	// Keep maxEntries large so eviction is driven strictly by maxBytes.
	configureExprCacheWithBytes(t, "true", "100", "3000", "2000")

	c := exprCache()
	_, err := handleInternal("id == 1")
	require.NoError(t, err)
	_, err = handleInternal("id == 2")
	require.NoError(t, err)
	require.Equal(t, 2, c.cache.Len())
	require.LessOrEqual(t, c.currentBytes.Load(), c.maxBytes)

	// Adding more entries must evict the oldest ones to stay within maxBytes.
	for i := 3; i <= 10; i++ {
		_, err = handleInternal("id == " + strings.Repeat("1", i))
		require.NoError(t, err)
		require.LessOrEqual(t, c.currentBytes.Load(), c.maxBytes)
		require.Greater(t, c.currentBytes.Load(), int64(0))
	}
	require.False(t, c.cache.Contains("id == 1"))
	require.False(t, c.cache.Contains("id == 2"))

	c.purge()
	require.Equal(t, int64(0), c.currentBytes.Load())
}

func TestExprCacheEntryAdmissionLimit(t *testing.T) {
	configureExprCacheWithBytes(t, "true", "100", "4096", "16384")
	c := exprCache()
	// maxEntryBytes must be clamped to maxBytes.
	require.Equal(t, int64(4096), c.maxEntryBytes)

	largeValid := `id in [` + strings.Repeat("1,", 500) + `1]`
	first, err := handleInternal(largeValid)
	require.NoError(t, err)
	second, err := handleInternal(largeValid)
	require.NoError(t, err)
	require.NotSame(t, first, second, "oversized valid expression must parse normally without being cached")
	require.False(t, c.cache.Contains(largeValid))
	require.Equal(t, int64(0), c.currentBytes.Load())

	largeErr := "id ==" + strings.Repeat(" ", 5000)
	_, firstErr := handleInternal(largeErr)
	_, secondErr := handleInternal(largeErr)
	require.Error(t, firstErr)
	require.EqualError(t, secondErr, firstErr.Error())
	require.NotSame(t, firstErr, secondErr, "oversized error must not be cached")
	require.False(t, c.cache.Contains(largeErr))
	require.Equal(t, int64(0), c.currentBytes.Load())
}

func TestExprCacheAccountingOnReplaceAndError(t *testing.T) {
	configureExprCacheWithBytes(t, "true", "16", "1048576", "65536")
	c := exprCache()

	_, err := handleInternal("id == 1")
	require.NoError(t, err)
	cost1 := c.currentBytes.Load()
	require.Greater(t, cost1, int64(0))

	// Directly replacing an existing key must subtract the previous cost cleanly.
	c.add("id == 1", "replacement", 500)
	require.Equal(t, int64(500), c.currentBytes.Load())

	_, err = handleInternal("id ==")
	require.Error(t, err)
	require.Greater(t, c.currentBytes.Load(), int64(500))

	c.purge()
	require.Equal(t, int64(0), c.currentBytes.Load())
}

func TestExprCacheConcurrentByteBudget(t *testing.T) {
	configureExprCacheWithBytes(t, "true", "64", "8192", "2048")
	c := exprCache()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 25 {
				expr := "id == " + strings.Repeat("1", (id+i)%10+1)
				if i%5 == 0 {
					expr = "id ==" + strings.Repeat(" ", (id+i)%5)
				}
				_, _ = handleInternal(expr)
			}
		}(g)
	}
	wg.Wait()
	require.LessOrEqual(t, c.currentBytes.Load(), c.maxBytes)
	c.purge()
	require.Equal(t, int64(0), c.currentBytes.Load())
}
