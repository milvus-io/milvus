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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestComputeSegmentEntries(t *testing.T) {
	segs := []*datapb.SegmentInfo{
		// collection 1: two live segments, v2 and v1, one sorted with text stats
		{
			ID: 1, CollectionID: 1, State: commonpb.SegmentState_Flushed, StorageVersion: 2, IsSorted: true,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{101: {}},
		},
		{
			ID: 2, CollectionID: 1, State: commonpb.SegmentState_Sealed, StorageVersion: 1,
			Bm25Statslogs: []*datapb.FieldBinlog{{FieldID: 102}},
		},
		// collection 1: dropped and invisible segments carry traits that must not count
		{ID: 3, CollectionID: 1, State: commonpb.SegmentState_Dropped, StorageVersion: 9, IsSortedByNamespace: true},
		{ID: 4, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsInvisible: true, StorageVersion: 9, JsonKeyStats: map[int64]*datapb.JsonKeyStats{103: {}}},
		// collection 2: growing, v2, json key stats, sorted by namespace
		{
			ID: 5, CollectionID: 2, State: commonpb.SegmentState_Growing, StorageVersion: 2, IsSortedByNamespace: true,
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{103: {}},
		},
		nil,
	}
	entries := indexGroup(ComputeSegmentEntries(segs), GroupSegment)

	assert.EqualValues(t, 2, entries["storage_version=2"].Value, "one per collection, two collections on v2")
	assert.EqualValues(t, 1, entries["storage_version=1"].Value)
	assert.NotContains(t, entries, "storage_version=9", "dropped / invisible segments are skipped")
	assert.EqualValues(t, 1, entries[SegmentIsSorted].Value)
	assert.EqualValues(t, 1, entries[SegmentIsSortedByNamespace].Value)
	assert.EqualValues(t, 1, entries[SegmentTextStats].Value)
	assert.EqualValues(t, 1, entries[SegmentJSONKeyStats].Value)
	assert.EqualValues(t, 1, entries[SegmentBM25Stats].Value)

	// Empty input still emits the boolean traits at zero, no storage_version rows.
	empty := indexGroup(ComputeSegmentEntries(nil), GroupSegment)
	assert.Len(t, empty, 5)
	for _, e := range empty {
		assert.Zero(t, e.Value)
	}
}
