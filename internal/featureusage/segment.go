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

package featureusage

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Names of GroupSegment entries. These report what was materialized on disk,
// not what the user declared: the number of collections with at least one
// live segment carrying the trait.
const (
	SegmentStorageVersionPrefix = "storage_version="
	SegmentIsSorted             = "is_sorted"
	SegmentIsSortedByNamespace  = "is_sorted_by_namespace"
	SegmentTextStats            = "text_stats"
	SegmentJSONKeyStats         = "json_key_stats"
	SegmentBM25Stats            = "bm25_stats"
)

// ComputeSegmentEntries computes GroupSegment from segment metadata. Dropped,
// non-existent and invisible (compacted-away, not yet dropped) segments are
// skipped. storage_version is emitted per distinct version; the value is an
// integer the server wrote, never a user string.
func ComputeSegmentEntries(segments []*datapb.SegmentInfo) []*internalpb.FeatureEntry {
	c := newCollector()
	for _, name := range []string{SegmentIsSorted, SegmentIsSortedByNamespace, SegmentTextStats, SegmentJSONKeyStats, SegmentBM25Stats} {
		c.ensure(GroupSegment, name, "")
	}

	perCollection := make(map[int64]seen)
	for _, seg := range segments {
		if seg == nil || !segmentIsLive(seg) {
			continue
		}
		s, ok := perCollection[seg.GetCollectionID()]
		if !ok {
			s = newSeen()
			perCollection[seg.GetCollectionID()] = s
		}
		c.addOnce(s, GroupSegment, SegmentStorageVersionPrefix+strconv.FormatInt(seg.GetStorageVersion(), 10), "")
		if seg.GetIsSorted() {
			c.addOnce(s, GroupSegment, SegmentIsSorted, "")
		}
		if seg.GetIsSortedByNamespace() {
			c.addOnce(s, GroupSegment, SegmentIsSortedByNamespace, "")
		}
		if len(seg.GetTextStatsLogs()) > 0 {
			c.addOnce(s, GroupSegment, SegmentTextStats, "")
		}
		if len(seg.GetJsonKeyStats()) > 0 {
			c.addOnce(s, GroupSegment, SegmentJSONKeyStats, "")
		}
		if len(seg.GetBm25Statslogs()) > 0 {
			c.addOnce(s, GroupSegment, SegmentBM25Stats, "")
		}
	}
	return c.entries()
}

func segmentIsLive(seg *datapb.SegmentInfo) bool {
	if seg.GetIsInvisible() {
		return false
	}
	switch seg.GetState() {
	case commonpb.SegmentState_Dropped, commonpb.SegmentState_NotExist, commonpb.SegmentState_SegmentStateNone:
		return false
	}
	return true
}
