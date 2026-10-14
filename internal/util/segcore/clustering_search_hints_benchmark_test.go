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

package segcore

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

var benchmarkFlatHintsSink flatClusteringSearchHints

func BenchmarkFlattenClusteringSearchHints(b *testing.B) {
	for _, nq := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprint(nq), func(b *testing.B) {
			h := &querypb.SegmentClusterSearchHints{SnapshotVersion: 1, NumRows: 10000, Queries: make([]*querypb.QueryClusterSearchHints, nq)}
			for q := range h.Queries {
				h.Queries[q] = &querypb.QueryClusterSearchHints{Hints: &querypb.ClusterSearchHintList{}}
				for r := 0; r < 64; r++ {
					h.Queries[q].Hints.Ranges = append(h.Queries[q].Hints.Ranges, &querypb.ClusterSearchHint{RowOffset: int64(r * 8), RowCount: 8, CentroidDistance: float32(r)})
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkFlatHintsSink = flattenClusteringSearchHints(h, int64(nq))
			}
		})
	}
}
