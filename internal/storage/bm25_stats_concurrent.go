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
	"maps"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ConcurrentBM25Stats holds the same data as BM25Stats with the token table split into shards,
// each guarded by its own lock, so concurrent merges of different stats proceed in parallel.
// The token maps use the same shard layout as BM25Stats; only the synchronization differs.
// A reader running concurrently with a merge may observe that merge partially applied.
//
// A concurrent BuildIDF still never sees more rows containing a token than rows in total, which would
// make the IDF negative: Merge and MergeFrom add the row count before the token counts, and BuildIDF reads
// the token counts before the row count. Minus does not keep this, so it must not run concurrently with BuildIDF.
//
// The Unlocked methods skip shard locks on paths already serialized by the caller:
// MergeUnlocked and MinusUnlocked need
// the caller to hold c exclusively, and BuildIDFUnlocked needs that no write runs concurrently.
type ConcurrentBM25Stats struct {
	rowsWithToken bm25TokenCounts
	shardLocks    [bm25StatsShards]sync.RWMutex
	numRow        atomic.Int64
	numToken      atomic.Int64
}

func NewConcurrentBM25Stats() *ConcurrentBM25Stats {
	return &ConcurrentBM25Stats{}
}

// Merge adds stats into c. Safe for concurrent use.
func (c *ConcurrentBM25Stats) Merge(stats *BM25Stats) {
	// the row count goes first, see the type comment
	c.numRow.Add(stats.numRow)
	c.numToken.Add(stats.numToken)
	c.apply(&stats.rowsWithToken, 1, true)
}

// Minus subtracts stats from c. Safe for concurrent use with other writes, but not with BuildIDF.
func (c *ConcurrentBM25Stats) Minus(stats *BM25Stats) {
	c.apply(&stats.rowsWithToken, -1, true)
	c.numRow.Add(-stats.numRow)
	c.numToken.Add(-stats.numToken)
}

// MergeUnlocked adds stats into c. The caller must hold c exclusively.
func (c *ConcurrentBM25Stats) MergeUnlocked(stats *BM25Stats) {
	c.apply(&stats.rowsWithToken, 1, false)
	c.numRow.Add(stats.numRow)
	c.numToken.Add(stats.numToken)
}

// MinusUnlocked subtracts stats from c. The caller must hold c exclusively.
func (c *ConcurrentBM25Stats) MinusUnlocked(stats *BM25Stats) {
	c.apply(&stats.rowsWithToken, -1, false)
	c.numRow.Add(-stats.numRow)
	c.numToken.Add(-stats.numToken)
}

func (c *ConcurrentBM25Stats) apply(source *bm25TokenCounts, sign int32, lockShards bool) {
	var locks *[bm25StatsShards]sync.RWMutex
	if lockShards {
		locks = &c.shardLocks
	}
	c.rowsWithToken.apply(source, sign, locks, true)
}

// MergeFrom adds other into c, merging the shards in parallel. Safe for concurrent use,
// but other must not be modified concurrently.
func (c *ConcurrentBM25Stats) MergeFrom(other *ConcurrentBM25Stats) {
	// the row count goes first, see the type comment
	c.numRow.Add(other.numRow.Load())
	c.numToken.Add(other.numToken.Load())
	c.apply(&other.rowsWithToken, 1, true)
}

func (c *ConcurrentBM25Stats) NumRow() int64 {
	return c.numRow.Load()
}

func (c *ConcurrentBM25Stats) NumToken() int64 {
	return c.numToken.Load()
}

// BuildIDF has the same result as BM25Stats.BuildIDF on the same data. Safe for concurrent use with
// Merge and MergeFrom.
func (c *ConcurrentBM25Stats) BuildIDF(tf []byte) (idf []byte) {
	numElements := typeutil.SparseFloatRowElementCount(tf)
	nqs := make([]int32, numElements)
	for idx := 0; idx < numElements; idx++ {
		key := typeutil.SparseFloatRowIndexAt(tf, idx)
		shard := shardOf(key)
		c.shardLocks[shard].RLock()
		nqs[idx] = c.rowsWithToken[shard][key]
		c.shardLocks[shard].RUnlock()
	}
	// read after the token counts, so it covers every merge they include, see the type comment
	numRow := c.numRow.Load()
	idf = make([]byte, len(tf))
	for idx := 0; idx < numElements; idx++ {
		key := typeutil.SparseFloatRowIndexAt(tf, idx)
		value := typeutil.SparseFloatRowValueAt(tf, idx)
		typeutil.SparseFloatRowSetAt(idf, idx, key, bm25TermIDF(value, numRow, nqs[idx]))
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
		nq := c.rowsWithToken.get(key)
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

// MemSize estimates the object, live shard-map headers and token entries.
func (c *ConcurrentBM25Stats) MemSize() int64 {
	size := int64(unsafe.Sizeof(*c))
	bytesPerEntry := paramtable.Get().QueryNodeCfg.BM25StatsBytesPerEntry.GetAsInt64()
	for i := range c.rowsWithToken {
		c.shardLocks[i].RLock()
		entries := len(c.rowsWithToken[i])
		c.shardLocks[i].RUnlock()
		if entries != 0 {
			size += bm25StatsMapOverhead + int64(entries)*bytesPerEntry
		}
	}
	return size
}

// Snapshot copies the stats into a BM25Stats with the same shard layout.
func (c *ConcurrentBM25Stats) Snapshot() *BM25Stats {
	stats := NewBM25Stats()
	for i := range c.rowsWithToken {
		c.shardLocks[i].RLock()
		stats.rowsWithToken[i] = maps.Clone(c.rowsWithToken[i])
		c.shardLocks[i].RUnlock()
	}
	stats.numRow = c.numRow.Load()
	stats.numToken = c.numToken.Load()
	return stats
}
