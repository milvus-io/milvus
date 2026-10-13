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
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type flatClusteringSearchHints struct {
	rows                        int64
	presence                    []byte
	boundaries, offsets, counts []int64
	distances                   []float32
}

// Optional internal metadata must fail open as a whole, never partially narrow
// an otherwise valid search. C++ repeats the row-domain check on the pinned segment.
func flattenClusteringSearchHints(h *querypb.SegmentClusterSearchHints, nq int64) flatClusteringSearchHints {
	empty := flatClusteringSearchHints{}
	if h == nil || h.GetSnapshotVersion() <= 0 || h.GetNumRows() <= 0 || nq <= 0 || int64(len(h.GetQueries())) != nq {
		return empty
	}
	out := flatClusteringSearchHints{rows: h.GetNumRows(), presence: make([]byte, nq), boundaries: make([]int64, nq+1)}
	for q, query := range h.GetQueries() {
		if hints := query.GetHints(); hints != nil {
			out.presence[q] = 1
			for _, hint := range hints.GetRanges() {
				offset, count, distance := hint.GetRowOffset(), hint.GetRowCount(), hint.GetCentroidDistance()
				if offset < 0 || count <= 0 || offset > out.rows || count > out.rows-offset || math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 0) {
					return empty
				}
				out.offsets = append(out.offsets, offset)
				out.counts = append(out.counts, count)
				out.distances = append(out.distances, distance)
			}
		}
		out.boundaries[q+1] = int64(len(out.offsets))
	}
	return out
}
