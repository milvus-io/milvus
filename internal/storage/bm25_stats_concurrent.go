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

package storage

import (
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	// concurrentBM25StatsShardBits sets 32 shards; each token belongs to exactly one shard.
	concurrentBM25StatsShardBits = 5
	concurrentBM25StatsShards    = 1 << concurrentBM25StatsShardBits

	// below this many tokens a merge locks the shard per token instead of splitting the stats first
	concurrentBM25StatsSplitThreshold = 1024
)

// ConcurrentBM25Stats holds the same data as BM25Stats with the token table split into shards,
// each guarded by its own lock, so concurrent merges of different stats proceed in parallel.
// Every token lives in exactly one shard, so it takes the same memory as a single BM25Stats.
// A reader running concurrently with a merge may observe that merge partially applied.
//
// The shard locks cost an atomic update per token, which matters on hot paths that are already
// serialized by the caller. The Unlocked methods skip them: MergeUnlocked and MinusUnlocked need
// the caller to hold c exclusively, and BuildIDFUnlocked needs that no write runs concurrently.
type ConcurrentBM25Stats struct {
	shards    [concurrentBM25StatsShards]bm25StatsShard
	numRow    atomic.Int64
	numToken  atomic.Int64
	nextShard atomic.Uint32 // rotates the first shard a large merge writes to, to spread contention
}

type bm25StatsShard struct {
	sync.RWMutex
	rowsWithToken map[uint32]int32
}

type tokenCount struct {
	token uint32
	count int32
}

var splitBufferPool = sync.Pool{New: func() any {
	split := make([][]tokenCount, concurrentBM25StatsShards)
	return &split
}}

func NewConcurrentBM25Stats() *ConcurrentBM25Stats {
	c := &ConcurrentBM25Stats{}
	for i := range c.shards {
		c.shards[i].rowsWithToken = map[uint32]int32{}
	}
	return c
}

// shardOf spreads tokens with Fibonacci hashing, so shards stay balanced even if token low bits are not uniform.
func shardOf(token uint32) uint32 {
	return (token * 2654435769) >> (32 - concurrentBM25StatsShardBits)
}

// Merge adds stats into c. Safe for concurrent use.
func (c *ConcurrentBM25Stats) Merge(stats *BM25Stats) {
	c.apply(stats.rowsWithToken, 1)
	c.numRow.Add(stats.numRow)
	c.numToken.Add(stats.numToken)
}

// Minus subtracts stats from c. Safe for concurrent use.
func (c *ConcurrentBM25Stats) Minus(stats *BM25Stats) {
	c.apply(stats.rowsWithToken, -1)
	c.numRow.Add(-stats.numRow)
	c.numToken.Add(-stats.numToken)
}

// MergeUnlocked adds stats into c. The caller must hold c exclusively.
func (c *ConcurrentBM25Stats) MergeUnlocked(stats *BM25Stats) {
	c.applyUnlocked(stats.rowsWithToken, 1)
	c.numRow.Add(stats.numRow)
	c.numToken.Add(stats.numToken)
}

// MinusUnlocked subtracts stats from c. The caller must hold c exclusively.
func (c *ConcurrentBM25Stats) MinusUnlocked(stats *BM25Stats) {
	c.applyUnlocked(stats.rowsWithToken, -1)
	c.numRow.Add(-stats.numRow)
	c.numToken.Add(-stats.numToken)
}

func (c *ConcurrentBM25Stats) applyUnlocked(rows map[uint32]int32, sign int32) {
	for token, count := range rows {
		c.shards[shardOf(token)].rowsWithToken[token] += sign * count
	}
}

func (c *ConcurrentBM25Stats) apply(rows map[uint32]int32, sign int32) {
	if len(rows) < concurrentBM25StatsSplitThreshold {
		for token, count := range rows {
			shard := &c.shards[shardOf(token)]
			shard.Lock()
			shard.rowsWithToken[token] += sign * count
			shard.Unlock()
		}
		return
	}

	// the split buffers are reused across merges: a recovering delegator merges thousands of
	// segments with hundreds of thousands of tokens each, and fresh buffers per merge pile up as garbage
	bufp := splitBufferPool.Get().(*[][]tokenCount)
	split := *bufp
	for i := range split {
		split[i] = split[i][:0]
	}
	for token, count := range rows {
		s := shardOf(token)
		split[s] = append(split[s], tokenCount{token, count})
	}
	defer func() {
		*bufp = split
		splitBufferPool.Put(bufp)
	}()

	start := c.nextShard.Add(1)
	for i := uint32(0); i < concurrentBM25StatsShards; i++ {
		s := (start + i) % concurrentBM25StatsShards
		shard := &c.shards[s]
		shard.Lock()
		for _, tc := range split[s] {
			shard.rowsWithToken[tc.token] += sign * tc.count
		}
		shard.Unlock()
	}
}

// MergeFrom adds other into c, merging the shards in parallel. Safe for concurrent use,
// but other must not be modified concurrently.
func (c *ConcurrentBM25Stats) MergeFrom(other *ConcurrentBM25Stats) {
	var wg sync.WaitGroup
	for i := range c.shards {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			src := &other.shards[i]
			dst := &c.shards[i]
			src.RLock()
			dst.Lock()
			for token, count := range src.rowsWithToken {
				dst.rowsWithToken[token] += count
			}
			dst.Unlock()
			src.RUnlock()
		}(i)
	}
	wg.Wait()
	c.numRow.Add(other.numRow.Load())
	c.numToken.Add(other.numToken.Load())
}

func (c *ConcurrentBM25Stats) NumRow() int64 {
	return c.numRow.Load()
}

func (c *ConcurrentBM25Stats) NumToken() int64 {
	return c.numToken.Load()
}

// BuildIDF has the same result as BM25Stats.BuildIDF on the same data. Safe for concurrent use.
func (c *ConcurrentBM25Stats) BuildIDF(tf []byte) (idf []byte) {
	numRow := c.numRow.Load()
	numElements := typeutil.SparseFloatRowElementCount(tf)
	idf = make([]byte, len(tf))
	for idx := 0; idx < numElements; idx++ {
		key := typeutil.SparseFloatRowIndexAt(tf, idx)
		value := typeutil.SparseFloatRowValueAt(tf, idx)
		shard := &c.shards[shardOf(key)]
		shard.RLock()
		nq := shard.rowsWithToken[key]
		shard.RUnlock()
		typeutil.SparseFloatRowSetAt(idf, idx, key, bm25TermIDF(value, numRow, nq))
	}
	return
}

// BuildIDFUnlocked is BuildIDF for callers that guarantee no write runs concurrently.
// It is a separate loop on purpose: a lock switch inside the loop made it about twice as slow.
func (c *ConcurrentBM25Stats) BuildIDFUnlocked(tf []byte) (idf []byte) {
	numRow := c.numRow.Load()
	numElements := typeutil.SparseFloatRowElementCount(tf)
	idf = make([]byte, len(tf))
	for idx := 0; idx < numElements; idx++ {
		key := typeutil.SparseFloatRowIndexAt(tf, idx)
		value := typeutil.SparseFloatRowValueAt(tf, idx)
		nq := c.shards[shardOf(key)].rowsWithToken[key]
		typeutil.SparseFloatRowSetAt(idf, idx, key, bm25TermIDF(value, numRow, nq))
	}
	return
}

func (c *ConcurrentBM25Stats) GetAvgdl() float64 {
	numRow, numToken := c.numRow.Load(), c.numToken.Load()
	if numRow == 0 || numToken == 0 {
		return 0
	}
	return float64(numToken) / float64(numRow)
}

// MemSize estimates the in-memory size with the same per-entry cost as BM25Stats.MemSize.
func (c *ConcurrentBM25Stats) MemSize() int64 {
	entries := 0
	for i := range c.shards {
		shard := &c.shards[i]
		shard.RLock()
		entries += len(shard.rowsWithToken)
		shard.RUnlock()
	}
	return 120 + int64(entries)*paramtable.Get().QueryNodeCfg.BM25StatsBytesPerEntry.GetAsInt64()
}

// Snapshot copies the stats into a BM25Stats.
func (c *ConcurrentBM25Stats) Snapshot() *BM25Stats {
	stats := NewBM25Stats()
	for i := range c.shards {
		shard := &c.shards[i]
		shard.RLock()
		for token, count := range shard.rowsWithToken {
			stats.rowsWithToken[token] = count
		}
		shard.RUnlock()
	}
	stats.numRow = c.numRow.Load()
	stats.numToken = c.numToken.Load()
	return stats
}
