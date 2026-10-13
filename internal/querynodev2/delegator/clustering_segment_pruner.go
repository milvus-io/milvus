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

package delegator

import (
	"maps"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// pruneClusteringSegments drops a covered segment only if all queries have an
// explicitly empty candidate list. Missing or invalid hints retain the segment.
// Retained segments keep the full NQ and share their normal expression work.
func pruneClusteringSegments(sealed []SnapshotItem, rows map[int64]int64,
	hints map[int64]*querypb.SegmentClusterSearchHints, nq int64,
) ([]SnapshotItem, map[int64]int64) {
	if nq <= 0 || len(hints) == 0 {
		return sealed, rows
	}
	empty := func(id int64) bool {
		h := hints[id]
		if h == nil || h.GetSnapshotVersion() <= 0 || h.GetNumRows() <= 0 || h.GetNumRows() != rows[id] || int64(len(h.GetQueries())) != nq {
			return false
		}
		for _, q := range h.GetQueries() {
			if q == nil || q.GetHints() == nil || len(q.GetHints().GetRanges()) != 0 {
				return false
			}
		}
		return true
	}
	candidates := make([]SnapshotItem, 0, len(sealed))
	candidateRows := rows
	pruned := false
	for _, item := range sealed {
		segments := make([]SegmentEntry, 0, len(item.Segments))
		for _, segment := range item.Segments {
			if empty(segment.SegmentID) {
				if !pruned {
					// Partial-result accounting must use the remaining search scope, without
					// changing the pinned row map shared by other subsearches.
					candidateRows = maps.Clone(rows)
					pruned = true
				}
				delete(candidateRows, segment.SegmentID)
				continue
			}
			segments = append(segments, segment)
		}
		if len(segments) > 0 {
			candidates = append(candidates, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	if !pruned {
		return sealed, rows
	}
	return candidates, candidateRows
}
