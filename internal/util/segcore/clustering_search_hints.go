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

// ValidateClusteringSearchHints checks optional metadata without allocating. Merge
// uses the same validation as the CGo bridge before combining request scopes.
func ValidateClusteringSearchHints(h *querypb.SegmentClusterSearchHints, nq int64) bool {
	_, valid := clusteringHintCount(h, nq)
	return valid
}

func clusteringHintCount(h *querypb.SegmentClusterSearchHints, nq int64) (int, bool) {
	if h == nil || h.GetSnapshotVersion() <= 0 || h.GetNumRows() <= 0 || nq <= 0 || nq >= math.MaxInt || int64(len(h.GetQueries())) != nq {
		return 0, false
	}
	total, rows := 0, h.GetNumRows()
	for _, query := range h.GetQueries() {
		for _, hint := range query.GetHints().GetRanges() {
			offset, count, distance := hint.GetRowOffset(), hint.GetRowCount(), hint.GetCentroidDistance()
			if offset < 0 || count <= 0 || offset > rows || count > rows-offset || math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 0) {
				return 0, false
			}
			total++
			if total > (math.MaxInt-int(nq)-1)/2 {
				return 0, false
			}
		}
	}
	return total, true
}

// Optional metadata fails open as a whole. One validation/counting pass lets the
// bridge allocate its numeric buffers once, without repeated range-slice growth.
func flattenClusteringSearchHints(h *querypb.SegmentClusterSearchHints, nq int64) flatClusteringSearchHints {
	total, valid := clusteringHintCount(h, nq)
	if !valid {
		return flatClusteringSearchHints{}
	}
	ints := make([]int64, int(nq)+1+2*total)
	out := flatClusteringSearchHints{rows: h.GetNumRows(), presence: make([]byte, nq), boundaries: ints[:nq+1], offsets: ints[nq+1 : int(nq)+1+total], counts: ints[int(nq)+1+total:], distances: make([]float32, total)}
	cursor := 0
	for q, query := range h.GetQueries() {
		if hints := query.GetHints(); hints != nil {
			out.presence[q] = 1
			for _, hint := range hints.GetRanges() {
				out.offsets[cursor], out.counts[cursor], out.distances[cursor] = hint.GetRowOffset(), hint.GetRowCount(), hint.GetCentroidDistance()
				cursor++
			}
		}
		out.boundaries[q+1] = int64(cursor)
	}
	return out
}
