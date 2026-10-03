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
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// liveCosts sums the accounted cost of the entries the LRU still holds; the
// cache's running total must always equal it.
func liveCosts(c *boundedExprCache) int64 {
	var sum int64
	for _, e := range c.lru.Values() {
		sum += e.cost
	}
	return sum
}

// withExprCache swaps the process-wide parse cache for the test's duration.
func withExprCache(t *testing.T, c *boundedExprCache) {
	t.Helper()
	orig := exprCache
	exprCache = c
	t.Cleanup(func() { exprCache = orig })
}

func TestBoundedExprCacheReplacementReleasesTheOldCost(t *testing.T) {
	// Two requests that miss on one expression both add it; the second must
	// not leave the first entry's cost counted.
	c := newBoundedExprCache(16, time.Hour, 1<<20, 1<<20)
	c.Add("a > 1", "first", 100)
	c.Add("a > 1", "second", 300)

	v, ok := c.Get("a > 1")
	require.True(t, ok)
	assert.Equal(t, "second", v)
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, int64(300), c.Retained())
	assert.Equal(t, liveCosts(c), c.Retained())
}

func TestBoundedExprCacheEvictsOldestToStayWithinBudget(t *testing.T) {
	c := newBoundedExprCache(16, time.Hour, 1000, 1000)
	c.Add("k1", 1, 400)
	c.Add("k2", 2, 400)
	c.Add("k3", 3, 400)

	_, ok := c.Get("k1")
	assert.False(t, ok, "the oldest entry makes room")
	for _, k := range []string{"k2", "k3"} {
		_, ok := c.Get(k)
		assert.True(t, ok, k)
	}
	assert.Equal(t, int64(800), c.Retained())
	assert.Equal(t, liveCosts(c), c.Retained())
}

func TestBoundedExprCacheEvictsAsManyEntriesAsItTakes(t *testing.T) {
	c := newBoundedExprCache(16, time.Hour, 1000, 1000)
	for i := 0; i < 5; i++ {
		c.Add(fmt.Sprintf("k%d", i), i, 200)
	}
	c.Add("big", 5, 900)

	assert.Equal(t, 1, c.Len())
	assert.Equal(t, int64(900), c.Retained())
}

func TestBoundedExprCacheBoundsAreInclusive(t *testing.T) {
	// An entry exactly at the per-entry limit is admitted, and a total exactly
	// at the budget evicts nothing.
	c := newBoundedExprCache(16, time.Hour, 1000, 500)
	c.Add("k1", 1, 500)
	c.Add("k2", 2, 500)

	assert.Equal(t, 2, c.Len())
	assert.Equal(t, int64(1000), c.Retained())
}

func TestExprCacheCostPolicy(t *testing.T) {
	assert.Equal(t, int64(8192+3+4*3+350*2), treeCost("a>1", "a>1", 2))
	assert.Equal(t, int64(2048+3+4), errorCost("a>(", errors.New("boom")))
	// The process-wide cache is bounded, at the documented sizes.
	assert.Equal(t, int64(64<<20), exprCache.maxBytes)
	assert.Equal(t, int64(8<<20), exprCache.maxEntry)
}

func TestBoundedExprCacheSkipsAnEntryOverThePerEntryLimit(t *testing.T) {
	c := newBoundedExprCache(16, time.Hour, 1000, 300)
	c.Add("small", 1, 200)
	c.Add("huge", 2, 301)

	_, ok := c.Get("huge")
	assert.False(t, ok)
	_, ok = c.Get("small")
	assert.True(t, ok, "an oversized entry must not evict what is already cached")
	assert.Equal(t, int64(200), c.Retained())
}

func TestBoundedExprCacheNeverAdmitsAnEntryLargerThanTheBudget(t *testing.T) {
	// A per-entry limit above the budget is clamped to it. Otherwise such an
	// entry would be admitted and the budget loop would flush every other
	// entry, oldest first, before finally evicting the newcomer itself.
	c := newBoundedExprCache(16, time.Hour, 100, 1000)
	c.Add("k1", 1, 30)
	c.Add("k2", 2, 30)
	c.Add("big", 3, 150)

	_, ok := c.Get("big")
	assert.False(t, ok)
	assert.Equal(t, 2, c.Len(), "the entries already cached survive")
	assert.Equal(t, int64(60), c.Retained())
}

func TestBoundedExprCacheCountLimitStillAppliesAndReleasesCost(t *testing.T) {
	c := newBoundedExprCache(2, time.Hour, 1<<20, 1<<20)
	c.Add("k1", 1, 10)
	c.Add("k2", 2, 20)
	c.Add("k3", 3, 30)

	assert.Equal(t, 2, c.Len())
	assert.Equal(t, int64(50), c.Retained())
	assert.Equal(t, liveCosts(c), c.Retained())
}

func TestBoundedExprCacheExpiryReleasesCost(t *testing.T) {
	// One second, not less: expirable.LRU v2.0.7 has no Close, so its cleanup
	// goroutine outlives the test and wakes every TTL/100 for the rest of the run.
	c := newBoundedExprCache(16, time.Second, 1<<20, 1<<20)
	c.Add("k1", 1, 100)
	c.Add("k2", 2, 200)

	assert.Eventually(t, func() bool { return c.Len() == 0 && c.Retained() == 0 },
		10*time.Second, 20*time.Millisecond)
}

func TestBoundedExprCachePurgeResetsTheTotal(t *testing.T) {
	c := newBoundedExprCache(16, time.Hour, 1<<20, 1<<20)
	c.Add("k1", 1, 100)
	c.Add("k2", 2, 200)
	c.Purge()

	assert.Equal(t, 0, c.Len())
	assert.Equal(t, int64(0), c.Retained())
}

func TestBoundedExprCacheAccountingHoldsUnderConcurrency(t *testing.T) {
	c := newBoundedExprCache(64, time.Hour, 5000, 1000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				// Overlapping keys exercise replacement; the costs force
				// budget evictions throughout.
				key := fmt.Sprintf("k%d", (g*7+i)%97)
				c.Add(key, i, int64(50+(i%9)*40))
				c.Get(fmt.Sprintf("k%d", i%97))
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, liveCosts(c), c.Retained())
	assert.LessOrEqual(t, c.Retained(), int64(5000))
}

func TestHandleInternalCachesTreesAndErrorsAtTheirCost(t *testing.T) {
	c := newBoundedExprCache(16, time.Hour, 1<<30, 1<<30)
	withExprCache(t, c)

	first, err := handleInternal("a > 1 and b < 2")
	require.NoError(t, err)
	second, err := handleInternal("a > 1 and b < 2")
	require.NoError(t, err)
	assert.Same(t, first, second, "a repeated expression is served from the cache")
	treeOnly := c.Retained()
	// 8 KiB fixed + 15-byte key + 4 × 15 runes + 350 × 8 tokens ("a", ">",
	// "1", "and", "b", "<", "2", EOF).
	assert.Equal(t, int64(8192+15+60+2800), treeOnly)

	_, err = handleInternal("a > (((")
	require.Error(t, err)
	_, ok := c.Get("a > (((")
	assert.True(t, ok, "syntax errors are cached too")
	// 2 KiB fixed + the key + the message the error actually carries.
	assert.Equal(t, treeOnly+int64(2048+len("a > (((")+len(err.Error())), c.Retained())
}

func TestHandleInternalAccountsTheNormalizedInput(t *testing.T) {
	// The lexer reads the string after convertHanToASCII, which spells each
	// Han character as an escape, so that — not the key — is the input the
	// tree retains as runes.
	c := newBoundedExprCache(16, time.Hour, 1<<30, 1<<30)
	withExprCache(t, c)

	expr := `a == "中文"`
	normalized := convertHanToASCII(expr)
	require.Greater(t, len(normalized), len(expr))
	_, err := handleInternal(expr)
	require.NoError(t, err)
	// 8 KiB + 13-byte key + 4 × 19 runes of `a == "\u4e2d\u6587"` + 350 × 4
	// tokens ("a", "==", the string literal, EOF).
	require.Equal(t, 13, len(expr))
	require.Equal(t, 19, len(normalized))
	assert.Equal(t, int64(8192+13+76+1400), c.Retained())
}

func TestHandleInternalParsesButDoesNotCacheAnOversizedTree(t *testing.T) {
	// A per-entry limit just above a one-term tree: the one-term expression is
	// admitted, a 200-term one is parsed correctly and not cached.
	c := newBoundedExprCache(16, time.Hour, 1<<30, treeCostFixed+2000)
	withExprCache(t, c)

	_, err := handleInternal("a > 1")
	require.NoError(t, err)
	assert.Equal(t, 1, c.Len())

	big := "a > 0" + strings.Repeat(" and a > 1", 200)
	ast, err := handleInternal(big)
	require.NoError(t, err)
	require.NotNil(t, ast)
	_, ok := c.Get(big)
	assert.False(t, ok)
	assert.Equal(t, 1, c.Len())
}

func heapInUse() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

func integerList(n int) string {
	values := make([]string, n)
	for i := range values {
		values[i] = strconv.Itoa(1_000_000 + i)
	}
	return strings.Join(values, ",")
}

// A cached tree keeps a pointer to the parser that built it. Before #53754 the
// parser also kept the last parse it ran, so each small cached tree pinned the
// large parse its parser handled next and the cache held about 30× what it
// accounted. The bound here is loose on purpose — the gap it guards is an
// order of magnitude, not a percentage.
func TestCachedTreesDoNotPinTheFailedParseTheirParserRanNext(t *testing.T) {
	c := newBoundedExprCache(1024, time.Hour, exprCacheMaxBytes, exprCacheMaxEntryBytes)
	withExprCache(t, c)
	list := integerList(7000)
	keys := make([]string, 0, 20)

	before := heapInUse()
	for i := 0; i < 10; i++ {
		small := fmt.Sprintf("s%d > 1", i)
		// A valid prefix and a stray ")": the error path, which did not use to
		// return its lexer and parser to the pool.
		bad := fmt.Sprintf("e%d in [%s] )", i, list)
		keys = append(keys, small, bad)
		_, err := handleInternal(small)
		require.NoError(t, err)
		_, err = handleInternal(bad)
		require.Error(t, err)
	}
	retained := heapInUse() - before

	assert.Less(t, retained, 3*c.Retained()+4<<20,
		"retained %d bytes for %d accounted", retained, c.Retained())
	runtime.KeepAlive(keys)
}

func TestCachedTreesDoNotPinTheUncachedParseTheirParserRanNext(t *testing.T) {
	// A 1 MiB per-entry limit keeps the uncached expression small enough to
	// parse quickly, even under -race.
	c := newBoundedExprCache(1024, time.Hour, exprCacheMaxBytes, 1<<20)
	withExprCache(t, c)
	// Over that limit, so these parses are never cached.
	list := integerList(5000)
	keys := make([]string, 0, 10)

	before := heapInUse()
	for i := 0; i < 10; i++ {
		// sync.Pool drops idle parsers after two collections, so each small
		// tree gets a parser of its own — which then parses the large one.
		runtime.GC()
		runtime.GC()
		small := fmt.Sprintf("s%d > 1", i)
		keys = append(keys, small)
		_, err := handleInternal(small)
		require.NoError(t, err)
		_, err = handleInternal(fmt.Sprintf("b%d in [%s]", i, list))
		require.NoError(t, err)
	}
	retained := heapInUse() - before

	require.Equal(t, 10, c.Len(), "only the small trees are cached")
	assert.Less(t, retained, 3*c.Retained()+4<<20,
		"retained %d bytes for %d accounted", retained, c.Retained())
	runtime.KeepAlive(keys)
}

// BenchmarkExprCacheRetainedVsAccounted reports, per cached entry, the heap
// the cache actually retains after GC next to the cost it is accounted, for
// the expression shapes the cost constants were fitted to. It is the
// measurement behind the accounting policy, kept runnable:
//
//	go test -run '^$' -bench ExprCacheRetained ./internal/parser/planparserv2/
func BenchmarkExprCacheRetainedVsAccounted(b *testing.B) {
	shapes := map[string]func(i int) string{
		"typical-100-terms": func(i int) string {
			parts := make([]string, 100)
			for k := range parts {
				parts[k] = fmt.Sprintf("field_%d > %d", k, k)
			}
			return fmt.Sprintf("uniq_%d == %d and ", i, i) + strings.Join(parts, " and ")
		},
		"dense-arithmetic": func(i int) string { return fmt.Sprintf("a%d > 0%s", i, strings.Repeat("+1", 800)) },
		"nested-parens": func(i int) string {
			return fmt.Sprintf("a%d > %s1%s", i, strings.Repeat("(", 200), strings.Repeat(")", 200))
		},
		"long-string-literal": func(i int) string { return fmt.Sprintf("a%d == \"%s\"", i, strings.Repeat("x", 8000)) },
	}
	// "shared" parses every tree on the pooled parser; "own" empties the pools
	// first, so each cached tree keeps a parser of its own — what happens once
	// sync.Pool has dropped idle parsers, and the costlier of the two.
	for name, mk := range shapes {
		for _, mode := range []string{"shared", "own"} {
			b.Run(name+"/"+mode+"-parser", func(b *testing.B) {
				const n = 100
				c := newBoundedExprCache(1024, time.Hour, 1<<40, 1<<40)
				orig := exprCache
				exprCache = c
				defer func() { exprCache = orig }()
				for iter := 0; iter < b.N; iter++ {
					c.Purge()
					keys := make([]string, n)
					before := heapInUse()
					for i := range keys {
						if mode == "own" {
							resetLexerPool()
							resetParserPool()
						}
						keys[i] = mk(i)
						_, _ = handleInternal(keys[i])
					}
					after := heapInUse()
					b.ReportMetric(float64(after-before)/n, "retained-B/entry")
					b.ReportMetric(float64(c.Retained())/n, "accounted-B/entry")
					runtime.KeepAlive(keys)
				}
			})
		}
	}
}
