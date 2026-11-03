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
	"runtime"
	"sync"
)

const (
	bm25StatsShardBits         = 5
	bm25StatsShards            = 1 << bm25StatsShardBits
	bm25StatsParallelThreshold = 64 * 1024
	bm25StatsMapOverhead       = 100
)

// Segment statistics and concurrent aggregates use the same token-to-DF layout.
type bm25TokenCounts [bm25StatsShards]map[uint32]int32

// Fibonacci hashing spreads sequential and nonuniform token IDs across shards.
func shardOf(token uint32) uint32 {
	return (token * 2654435769) >> (32 - bm25StatsShardBits)
}

func (counts *bm25TokenCounts) len() int {
	n := 0
	for _, shard := range counts {
		n += len(shard)
	}
	return n
}

func (counts *bm25TokenCounts) get(token uint32) int32 {
	return counts[shardOf(token)][token]
}

func (counts *bm25TokenCounts) add(token uint32, count int32) {
	i := shardOf(token)
	if counts[i] == nil {
		counts[i] = make(map[uint32]int32)
	}
	counts[i][token] += count
}

func (counts *bm25TokenCounts) clone() bm25TokenCounts {
	var result bm25TokenCounts
	applyBM25StatsShards(counts.len(), func(i int) {
		result[i] = maps.Clone(counts[i])
	})
	return result
}

func (counts *bm25TokenCounts) apply(other *bm25TokenCounts, sign int32, locks *[bm25StatsShards]sync.RWMutex, pruneZeros bool) {
	tokens := other.len()
	if tokens < bm25StatsParallelThreshold || runtime.GOMAXPROCS(0) == 1 {
		// Keep small insert batches free of the parallel callback's heap allocation.
		for i := 0; i < bm25StatsShards; i++ {
			counts.applyShard(other[i], i, sign, locks, pruneZeros)
		}
		return
	}
	applyBM25StatsShards(tokens, func(i int) {
		counts.applyShard(other[i], i, sign, locks, pruneZeros)
	})
}

func (counts *bm25TokenCounts) applyShard(source map[uint32]int32, i int, sign int32, locks *[bm25StatsShards]sync.RWMutex, pruneZeros bool) {
	if len(source) == 0 {
		return
	}
	if locks != nil {
		locks[i].Lock()
	}
	if counts[i] == nil {
		counts[i] = make(map[uint32]int32, len(source))
	}
	if pruneZeros {
		for token, count := range source {
			addTokenCount(counts[i], token, sign*count)
		}
	} else {
		// Plain Merge preserves zero-count records in the existing wire format.
		for token, count := range source {
			counts[i][token] += sign * count
		}
	}
	if locks != nil {
		locks[i].Unlock()
	}
}

func addTokenCount(rows map[uint32]int32, token uint32, delta int32) {
	if count := rows[token] + delta; count != 0 {
		rows[token] = count
	} else {
		delete(rows, token)
	}
}

func applyBM25StatsShards(tokens int, apply func(int)) {
	if tokens < bm25StatsParallelThreshold || runtime.GOMAXPROCS(0) == 1 {
		for i := 0; i < bm25StatsShards; i++ {
			apply(i)
		}
		return
	}
	var wg sync.WaitGroup
	wg.Add(bm25StatsShards)
	for i := 0; i < bm25StatsShards; i++ {
		go func() {
			defer wg.Done()
			apply(i)
		}()
	}
	wg.Wait()
}
