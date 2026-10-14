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

package tasks

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func taskClusteringHints(t *SearchTask) map[int64]*querypb.SegmentClusterSearchHints {
	if t.mergedHints != nil {
		return t.mergedHints
	}
	return t.req.GetClusteringSearchHints()
}

func mergeClusteringSearchHints(left, right *SearchTask) (map[int64]*querypb.SegmentClusterSearchHints, bool) {
	a, b := taskClusteringHints(left), taskClusteringHints(right)
	if len(a) == 0 && len(b) == 0 {
		return nil, true
	}
	// Keep ordinary task compatibility unchanged, but a hinted batch must refer
	// to the same field, visibility/filter scope and query encoding.
	l, r := left.req.GetReq(), right.req.GetReq()
	if left.req.GetScope() != right.req.GetScope() || l.GetFieldId() != r.GetFieldId() || l.GetMetricType() != r.GetMetricType() || l.GetCollectionTtlTimestamps() != r.GetCollectionTtlTimestamps() || l.GetEntityTtlPhysicalTime() != r.GetEntityTtlPhysicalTime() || l.GetConsistencyLevel() != r.GetConsistencyLevel() {
		return nil, false
	}
	var kind commonpb.PlaceholderType
	var tag string
	var width int
	for i, task := range []*SearchTask{left, right} {
		// Each group retains its original placeholders until Execute; children are
		// flattened in exactly the same order as originNqs and the combined hints.
		for j, sub := range append([]*SearchTask{task}, task.others...) {
			var ph commonpb.PlaceholderGroup
			if proto.Unmarshal(sub.placeholderGroup, &ph) != nil || len(ph.Placeholders) != 1 {
				return nil, false
			}
			p := ph.Placeholders[0]
			expected := sub.req.GetReq().GetNq()
			if expected <= 0 || int64(len(p.Values)) != expected {
				return nil, false
			}
			if i == 0 && j == 0 {
				kind, tag, width = p.Type, p.Tag, len(p.Values[0])
			}
			if width == 0 || p.Type != kind || p.Tag != tag {
				return nil, false
			}
			for _, v := range p.Values {
				if len(v) != width {
					return nil, false
				}
			}
		}
	}
	scope := make(map[int64]bool, len(left.req.GetSegmentIDs()))
	for _, id := range left.req.GetSegmentIDs() {
		scope[id] = true
	}
	for i, hints := range []map[int64]*querypb.SegmentClusterSearchHints{a, b} {
		nq := left.nq
		if i == 1 {
			nq = right.nq
		}
		for id, h := range hints {
			if !scope[id] || !segcore.ValidateClusteringSearchHints(h, nq) {
				return nil, false
			}
		}
	}
	result := make(map[int64]*querypb.SegmentClusterSearchHints, len(a)+len(b))
	for _, hints := range []map[int64]*querypb.SegmentClusterSearchHints{a, b} {
		for id := range hints {
			if result[id] != nil {
				continue
			}
			x, y := a[id], b[id]
			if x != nil && y != nil && (x.SnapshotVersion != y.SnapshotVersion || x.NumRows != y.NumRows) {
				return nil, false
			}
			metadata := x
			if metadata == nil {
				metadata = y
			}
			queries := make([]*querypb.QueryClusterSearchHints, left.nq+right.nq)
			for i, h := range x.GetQueries() {
				if h != nil {
					queries[i] = proto.Clone(h).(*querypb.QueryClusterSearchHints)
				}
			}
			for i, h := range y.GetQueries() {
				if h != nil {
					queries[int(left.nq)+i] = proto.Clone(h).(*querypb.QueryClusterSearchHints)
				}
			}
			result[id] = &querypb.SegmentClusterSearchHints{SnapshotVersion: metadata.SnapshotVersion, NumRows: metadata.NumRows, Queries: queries}
		}
	}
	return result, true
}
