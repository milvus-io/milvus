/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"container/heap"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestMergeHeapScoreOrder(t *testing.T) {
	low := float32(0.042794593)
	high := math.Nextafter32(low, float32(math.Inf(1)))
	tests := []struct {
		name   string
		ids    [][]int64
		scores [][]float32
		want   []int64
	}{
		{"close cosine scores", [][]int64{{5989}, {7175}}, [][]float32{{low}, {high}}, []int64{7175, 5989}},
		{"close negative scores", [][]int64{{5989}, {7175}}, [][]float32{{-high}, {-low}}, []int64{7175, 5989}},
		// Adjacent gaps are below the old epsilon, but the total gap is above it.
		{"close score chain", [][]int64{{1}, {2}, {3}}, [][]float32{{0.0999999568}, {0.0999999642}, {0.0999999717}}, []int64{3, 2, 1}},
		{"advance into close scores", [][]int64{{1, 5989}, {2, 7175}}, [][]float32{{0.9, low}, {0.8, high}}, []int64{1, 2, 7175, 5989}},
		{"equal scores", [][]int64{{3}, {1}, {2}}, [][]float32{{0.5}, {0.5}, {0.5}}, []int64{1, 2, 3}},
		{"signed zero", [][]int64{{2}, {1}}, [][]float32{{float32(math.Copysign(0, -1))}, {0}}, []int64{1, 2}},
		{"infinite scores", [][]int64{{3}, {2}, {4}, {1}}, [][]float32{{float32(math.Inf(1))}, {float32(math.Inf(1))}, {0}, {float32(math.Inf(-1))}}, []int64{2, 3, 4, 1}},
	}
	for _, tt := range tests {
		for _, stringPK := range []bool{false, true} {
			for _, reverseInputs := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stringPK=%v/reverse=%v", tt.name, stringPK, reverseInputs), func(t *testing.T) {
					pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
					t.Cleanup(func() { pool.AssertSize(t, 0) })
					entries := make([]*mergeEntry, len(tt.ids))
					for i, ids := range tt.ids {
						entry := &mergeEntry{}
						scores := array.NewFloat32Builder(pool)
						scores.AppendValues(tt.scores[i], nil)
						entry.scoreArr = scores.NewFloat32Array()
						scores.Release()
						t.Cleanup(entry.scoreArr.Release)
						if stringPK {
							builder := array.NewStringBuilder(pool)
							for _, id := range ids {
								builder.Append(fmt.Sprintf("%05d", id))
							}
							entry.idString = builder.NewStringArray()
							builder.Release()
							t.Cleanup(entry.idString.Release)
						} else {
							builder := array.NewInt64Builder(pool)
							builder.AppendValues(ids, nil)
							entry.idInt64 = builder.NewInt64Array()
							builder.Release()
							t.Cleanup(entry.idInt64.Release)
						}
						index := i
						if reverseInputs {
							index = len(entries) - 1 - i
						}
						entries[index] = entry
					}
					var h heap.Interface = (*mergeHeapInt64Pk)(&entries)
					if stringPK {
						h = (*mergeHeapStringPk)(&entries)
					}
					heap.Init(h)
					var got []string
					previous := float32(math.Inf(1))
					for h.Len() > 0 {
						entry := entries[0]
						require.GreaterOrEqual(t, previous, entry.scoreVal())
						previous = entry.scoreVal()
						if stringPK {
							got = append(got, entry.idStringVal())
							h.(*mergeHeapStringPk).advanceRoot()
						} else {
							got = append(got, fmt.Sprintf("%05d", entry.idInt64Val()))
							h.(*mergeHeapInt64Pk).advanceRoot()
						}
					}
					want := make([]string, len(tt.want))
					for i, id := range tt.want {
						want[i] = fmt.Sprintf("%05d", id)
					}
					require.Equal(t, want, got)
				})
			}
		}
	}
}
