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
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func randomBM25Stats(r *rand.Rand, rows int, vocab int32) *BM25Stats {
	stats := NewBM25Stats()
	for i := 0; i < rows; i++ {
		row := map[uint32]float32{}
		for j := 0; j < 8; j++ {
			row[uint32(r.Int31n(vocab))] += 1
		}
		stats.Append(row)
	}
	return stats
}

func allTokensTF(vocab int32) []byte {
	all := map[uint32]float32{}
	for i := int32(0); i < vocab; i++ {
		all[uint32(i)] = 1
	}
	return typeutil.CreateAndSortSparseFloatRow(all)
}

func TestConcurrentBM25StatsMatchesSerial(t *testing.T) {
	// both the per-token path (small stats) and the split path (large stats)
	for _, tc := range []struct {
		name  string
		rows  int
		vocab int32
	}{
		{"small", 20, 200},
		{"large", 2000, 20000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(11))
			adds := make([]*BM25Stats, 64)
			for i := range adds {
				adds[i] = randomBM25Stats(r, tc.rows, tc.vocab)
			}
			minus := adds[:16]

			expected := NewBM25Stats()
			for _, s := range adds {
				expected.Merge(s)
			}
			for _, s := range minus {
				expected.Minus(s)
			}

			c := NewConcurrentBM25Stats()
			var wg sync.WaitGroup
			for _, s := range adds {
				wg.Add(1)
				go func(s *BM25Stats) {
					defer wg.Done()
					c.Merge(s)
				}(s)
			}
			wg.Wait()
			for _, s := range minus {
				wg.Add(1)
				go func(s *BM25Stats) {
					defer wg.Done()
					c.Minus(s)
				}(s)
			}
			wg.Wait()

			got := c.Snapshot()
			assert.Equal(t, expected.NumRow(), c.NumRow())
			assert.Equal(t, expected.NumToken(), c.NumToken())
			assert.Equal(t, expected.rowsWithToken, got.rowsWithToken)
			assert.Equal(t, expected.GetAvgdl(), c.GetAvgdl())
			tf := allTokensTF(tc.vocab)
			assert.Equal(t, expected.BuildIDF(tf), c.BuildIDF(tf))
		})
	}
}

func TestConcurrentBM25StatsMergeFrom(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	expected := NewBM25Stats()
	a, b := NewConcurrentBM25Stats(), NewConcurrentBM25Stats()
	for i := 0; i < 10; i++ {
		s := randomBM25Stats(r, 500, 5000)
		expected.Merge(s)
		if i%2 == 0 {
			a.Merge(s)
		} else {
			b.Minus(s) // MergeFrom must carry negative counts too
			expected.Minus(s)
			expected.Minus(s)
		}
	}
	a.MergeFrom(b)
	got := a.Snapshot()
	assert.Equal(t, expected.NumRow(), a.NumRow())
	assert.Equal(t, expected.NumToken(), a.NumToken())
	assert.Equal(t, expected.rowsWithToken, got.rowsWithToken)
}

func TestConcurrentBM25StatsEmptyAndMemSize(t *testing.T) {
	c := NewConcurrentBM25Stats()
	assert.Equal(t, float64(0), c.GetAvgdl())
	assert.Equal(t, int64(0), c.NumRow())
	empty := c.MemSize()

	s := randomBM25Stats(rand.New(rand.NewSource(1)), 100, 1000)
	c.Merge(s)
	assert.Equal(t, s.MemSize(), c.MemSize())
	assert.Greater(t, c.MemSize(), empty)
}

func TestConcurrentBM25StatsShardBalance(t *testing.T) {
	// sequential token IDs must still spread evenly over the shards
	counts := make([]int, concurrentBM25StatsShards)
	const n = 1 << 16
	for i := uint32(0); i < n; i++ {
		counts[shardOf(i)]++
	}
	for _, c := range counts {
		require.InDelta(t, n/concurrentBM25StatsShards, c, n/concurrentBM25StatsShards/4)
	}
}
