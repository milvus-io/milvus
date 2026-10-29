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
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The benchmarks compare BM25Stats (master) and ConcurrentBM25Stats on the paths that run outside
// recovery, the way idfOracle calls them: BuildIDF under the oracle read lock (every BM25 search)
// and a small Merge under the oracle write lock (UpdateGrowing, every insert batch).

const benchVocab = 2_000_000

type benchTables struct {
	tokens     []uint32
	plain      *BM25Stats
	concurrent *ConcurrentBM25Stats
}

var (
	benchOnce sync.Once
	benchData benchTables
)

func loadBenchTables() benchTables {
	benchOnce.Do(func() {
		r := rand.New(rand.NewSource(1))
		tokens := make([]uint32, benchVocab)
		plain := NewBM25Stats()
		for i := range tokens {
			tokens[i] = r.Uint32()
			plain.rowsWithToken[tokens[i]] += 1 + int32(r.Intn(100))
		}
		plain.numRow, plain.numToken = 12_000_000, 600_000_000
		concurrent := NewConcurrentBM25Stats()
		concurrent.Merge(plain)
		benchData = benchTables{tokens: tokens, plain: plain, concurrent: concurrent}
	})
	return benchData
}

func benchQuery(r *rand.Rand, tokens []uint32, n int) []byte {
	row := make(map[uint32]float32, n)
	for len(row) < n {
		row[tokens[r.Intn(len(tokens))]] = 1
	}
	return typeutil.CreateAndSortSparseFloatRow(row)
}

func BenchmarkBuildIDF(b *testing.B) {
	data := loadBenchTables()
	for _, queryTokens := range []int{8, 128, 512} {
		queries := make([][]byte, 64)
		r := rand.New(rand.NewSource(int64(queryTokens)))
		for i := range queries {
			queries[i] = benchQuery(r, data.tokens, queryTokens)
		}
		for _, perCPU := range []int{1, 4} {
			run := func(b *testing.B, buildIDF func(tf []byte) []byte) {
				var oracle sync.RWMutex
				b.SetParallelism(perCPU)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						oracle.RLock()
						buildIDF(queries[i%len(queries)])
						oracle.RUnlock()
						i++
					}
				})
			}
			name := fmt.Sprintf("tokens=%d/goroutinesPerCPU=%d", queryTokens, perCPU)
			b.Run(name+"/plain", func(b *testing.B) { run(b, data.plain.BuildIDF) })
			b.Run(name+"/concurrent", func(b *testing.B) { run(b, data.concurrent.BuildIDF) })
			b.Run(name+"/concurrent-unlocked", func(b *testing.B) { run(b, data.concurrent.BuildIDFUnlocked) })
		}
	}
}

func BenchmarkUpdateGrowing(b *testing.B) {
	data := loadBenchTables()
	for _, rows := range []int{1, 100} {
		r := rand.New(rand.NewSource(int64(rows)))
		batches := make([]*BM25Stats, 64)
		for i := range batches {
			batches[i] = NewBM25Stats()
			for j := 0; j < rows; j++ {
				row := map[uint32]float32{}
				for k := 0; k < 50; k++ {
					row[data.tokens[r.Intn(len(data.tokens))]] += 1
				}
				batches[i].Append(row)
			}
		}
		run := func(b *testing.B, merge func(s *BM25Stats)) {
			var oracle sync.RWMutex
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				oracle.Lock()
				merge(batches[i%len(batches)])
				oracle.Unlock()
			}
		}
		// both tables keep growing their counts, which does not change their size
		plain := data.plain.Clone()
		concurrent := NewConcurrentBM25Stats()
		concurrent.Merge(plain)
		b.Run(fmt.Sprintf("rows=%d/plain", rows), func(b *testing.B) { run(b, plain.Merge) })
		b.Run(fmt.Sprintf("rows=%d/concurrent", rows), func(b *testing.B) { run(b, concurrent.Merge) })
		b.Run(fmt.Sprintf("rows=%d/concurrent-unlocked", rows), func(b *testing.B) { run(b, concurrent.MergeUnlocked) })
	}
}
