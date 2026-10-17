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

// nonZeroCounts drops the zero counts BM25Stats keeps when a merge brings a token back to zero;
// ConcurrentBM25Stats drops a token whenever its count reaches zero.
func nonZeroCounts(rows map[uint32]int32) map[uint32]int32 {
	out := make(map[uint32]int32, len(rows))
	for token, count := range rows {
		if count != 0 {
			out[token] = count
		}
	}
	return out
}

// Taking back everything that was merged must leave no tokens, on every write path.
func TestConcurrentBM25StatsDropsZeroCounts(t *testing.T) {
	r := rand.New(rand.NewSource(21))
	// below and above the split threshold of the locked path
	for _, rows := range []int{20, 2000} {
		s := randomBM25Stats(r, rows, 20000)

		c := NewConcurrentBM25Stats()
		c.Merge(s)
		c.Minus(s)
		assert.Empty(t, c.Snapshot().rowsWithToken, "Minus, %d rows", rows)
		assert.Zero(t, c.MemSize()-NewConcurrentBM25Stats().MemSize(), "Minus, %d rows", rows)

		c.MergeUnlocked(s)
		c.MinusUnlocked(s)
		assert.Empty(t, c.Snapshot().rowsWithToken, "MinusUnlocked, %d rows", rows)

		negative := NewConcurrentBM25Stats()
		negative.Minus(s)
		c.Merge(s)
		c.MergeFrom(negative)
		assert.Empty(t, c.Snapshot().rowsWithToken, "MergeFrom, %d rows", rows)
		assert.Zero(t, c.NumRow())
	}
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
			assert.Equal(t, nonZeroCounts(expected.rowsWithToken), got.rowsWithToken)
			assert.Equal(t, expected.GetAvgdl(), c.GetAvgdl())
			tf := allTokensTF(tc.vocab)
			assert.Equal(t, expected.BuildIDF(tf), c.BuildIDF(tf))
		})
	}
}

func TestConcurrentBM25StatsUnlockedMatchesSerial(t *testing.T) {
	// below and above the split threshold of the locked path
	for _, rows := range []int{20, 2000} {
		r := rand.New(rand.NewSource(int64(rows)))
		expected := NewBM25Stats()
		c := NewConcurrentBM25Stats()
		for i := 0; i < 16; i++ {
			s := randomBM25Stats(r, rows, 20000)
			expected.Merge(s)
			c.MergeUnlocked(s)
			if i%4 == 0 {
				expected.Minus(s)
				c.MinusUnlocked(s)
			}
		}
		assert.Equal(t, expected.NumRow(), c.NumRow())
		assert.Equal(t, expected.NumToken(), c.NumToken())
		assert.Equal(t, nonZeroCounts(expected.rowsWithToken), c.Snapshot().rowsWithToken)
		tf := allTokensTF(20000)
		assert.Equal(t, expected.BuildIDF(tf), c.BuildIDFUnlocked(tf))
		assert.Equal(t, c.BuildIDF(tf), c.BuildIDFUnlocked(tf))
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
	assert.Equal(t, nonZeroCounts(expected.rowsWithToken), got.rowsWithToken)
}

// A search during preload reads current while segments merge into it. It must never see a token in more
// rows than the row count, which gives a negative IDF that the worker rejects as an invalid sparse vector.
func TestConcurrentBM25StatsBuildIDFNeverNegativeDuringMerge(t *testing.T) {
	const vocab = 2048 // above the split threshold, like a real segment
	// every row holds every token, so after any set of whole merges each token is in exactly numRow rows
	row := map[uint32]float32{}
	for i := uint32(0); i < vocab; i++ {
		row[i] = 1
	}
	segment := NewBM25Stats()
	for i := 0; i < 4; i++ {
		segment.Append(row)
	}
	tf := allTokensTF(vocab)

	for _, merge := range []struct {
		name string
		fn   func(c *ConcurrentBM25Stats)
	}{
		{"Merge", func(c *ConcurrentBM25Stats) { c.Merge(segment) }},
		{"MergeFrom", func(c *ConcurrentBM25Stats) {
			other := NewConcurrentBM25Stats()
			other.MergeUnlocked(segment)
			c.MergeFrom(other)
		}},
	} {
		t.Run(merge.name, func(t *testing.T) {
			c := NewConcurrentBM25Stats()
			done := make(chan struct{})
			go func() {
				defer close(done)
				var wg sync.WaitGroup
				for w := 0; w < 4; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 200; i++ {
							merge.fn(c)
						}
					}()
				}
				wg.Wait()
			}()

			for searches := 0; ; searches++ {
				select {
				case <-done:
					t.Logf("%d searches ran during the merges", searches)
					return
				default:
				}
				idf := c.BuildIDF(tf)
				for idx := 0; idx < typeutil.SparseFloatRowElementCount(idf); idx++ {
					if v := typeutil.SparseFloatRowValueAt(idf, idx); v < 0 {
						require.FailNowf(t, "negative IDF", "token %d: %v", typeutil.SparseFloatRowIndexAt(idf, idx), v)
					}
				}
			}
		})
	}
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
