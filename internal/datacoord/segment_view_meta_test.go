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

package datacoord

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func newTestSegmentViewMeta() *segmentViewMeta {
	return newSegmentViewMeta(context.Background(), nil)
}

func newTestSegmentInfo(id int64, collectionID int64, channel string) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collectionID,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Growing,
		},
	}
}

func TestSegmentViewMeta_GetSegment(t *testing.T) {
	m := newTestSegmentViewMeta()

	t.Run("not found", func(t *testing.T) {
		assert.Nil(t, m.GetSegment(999))
	})

	t.Run("found", func(t *testing.T) {
		seg := newTestSegmentInfo(1, 100, "ch-0")
		m.setSegment(1, seg)
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, int64(1), got.GetID())
		assert.Equal(t, int64(100), got.GetCollectionID())
	})
}

func TestSegmentViewMeta_GetSegments(t *testing.T) {
	m := newTestSegmentViewMeta()

	t.Run("empty", func(t *testing.T) {
		assert.Empty(t, m.GetSegments())
	})

	t.Run("multiple", func(t *testing.T) {
		m.setSegment(1, newTestSegmentInfo(1, 100, "ch-0"))
		m.setSegment(2, newTestSegmentInfo(2, 100, "ch-0"))
		m.setSegment(3, newTestSegmentInfo(3, 200, "ch-1"))
		assert.Len(t, m.GetSegments(), 3)
	})
}

func TestSegmentViewMeta_GetSegmentsBySelector(t *testing.T) {
	m := newTestSegmentViewMeta()

	seg1 := newTestSegmentInfo(1, 100, "ch-0")
	seg1.State = commonpb.SegmentState_Growing
	seg2 := newTestSegmentInfo(2, 100, "ch-1")
	seg2.State = commonpb.SegmentState_Flushed
	seg3 := newTestSegmentInfo(3, 200, "ch-0")
	seg3.State = commonpb.SegmentState_Flushed
	seg4 := newTestSegmentInfo(4, 200, "ch-2")
	seg4.State = commonpb.SegmentState_Growing

	m.setSegment(1, seg1)
	m.setSegment(2, seg2)
	m.setSegment(3, seg3)
	m.setSegment(4, seg4)

	t.Run("no filters returns all", func(t *testing.T) {
		result := m.GetSegmentsBySelector()
		assert.Len(t, result, 4)
	})

	t.Run("filter by collection", func(t *testing.T) {
		result := m.GetSegmentsBySelector(WithCollection(100))
		assert.Len(t, result, 2)
		for _, s := range result {
			assert.Equal(t, int64(100), s.GetCollectionID())
		}
	})

	t.Run("filter by channel", func(t *testing.T) {
		result := m.GetSegmentsBySelector(WithChannel("ch-0"))
		assert.Len(t, result, 2)
		for _, s := range result {
			assert.Equal(t, "ch-0", s.GetInsertChannel())
		}
	})

	t.Run("filter by collection and channel", func(t *testing.T) {
		result := m.GetSegmentsBySelector(WithCollection(100), WithChannel("ch-0"))
		assert.Len(t, result, 1)
		assert.Equal(t, int64(1), result[0].GetID())
	})

	t.Run("filter with SegmentFilterFunc", func(t *testing.T) {
		result := m.GetSegmentsBySelector(SegmentFilterFunc(func(s *SegmentInfo) bool {
			return s.GetState() == commonpb.SegmentState_Flushed
		}))
		assert.Len(t, result, 2)
	})

	t.Run("combined collection + func filter", func(t *testing.T) {
		result := m.GetSegmentsBySelector(
			WithCollection(200),
			SegmentFilterFunc(func(s *SegmentInfo) bool {
				return s.GetState() == commonpb.SegmentState_Growing
			}),
		)
		assert.Len(t, result, 1)
		assert.Equal(t, int64(4), result[0].GetID())
	})

	t.Run("no match", func(t *testing.T) {
		result := m.GetSegmentsBySelector(WithCollection(999))
		assert.Empty(t, result)
	})
}

func TestSegmentViewMeta_GetCompactionTo(t *testing.T) {
	m := newTestSegmentViewMeta()

	// seg3 is compacted from seg1 and seg2
	seg1 := newTestSegmentInfo(1, 100, "ch-0")
	seg2 := newTestSegmentInfo(2, 100, "ch-0")
	seg3 := newTestSegmentInfo(3, 100, "ch-0")
	seg3.CompactionFrom = []int64{1, 2}

	m.setSegment(1, seg1)
	m.setSegment(2, seg2)
	m.setSegment(3, seg3)

	t.Run("found compaction target", func(t *testing.T) {
		result, exist := m.GetCompactionTo(1)
		assert.True(t, exist)
		require.Len(t, result, 1)
		assert.Equal(t, int64(3), result[0].GetID())
	})

	t.Run("found segment but no compaction target", func(t *testing.T) {
		result, exist := m.GetCompactionTo(3)
		assert.True(t, exist)
		assert.Nil(t, result)
	})

	t.Run("segment not found", func(t *testing.T) {
		result, exist := m.GetCompactionTo(999)
		assert.False(t, exist)
		assert.Nil(t, result)
	})
}

func TestSegmentViewMeta_SetSegment(t *testing.T) {
	m := newTestSegmentViewMeta()

	t.Run("new segment", func(t *testing.T) {
		seg := newTestSegmentInfo(1, 100, "ch-0")
		m.setSegment(1, seg)
		assert.NotNil(t, m.GetSegment(1))
		assert.Equal(t, 1, m.segments.Len())
	})

	t.Run("overwrite existing segment updates indexes", func(t *testing.T) {
		// Insert a segment with collectionID=100, channel=ch-0
		seg1 := newTestSegmentInfo(1, 100, "ch-0")
		m2 := newTestSegmentViewMeta()
		m2.setSegment(1, seg1)

		// Verify initial indexes
		collMap, ok := m2.coll2Segments.Get(100)
		require.True(t, ok)
		assert.Equal(t, 1, len(collMap))

		// Overwrite with different collection and channel
		seg1Updated := newTestSegmentInfo(1, 200, "ch-1")
		m2.setSegment(1, seg1Updated)

		// Old indexes should be cleaned
		assert.False(t, m2.coll2Segments.Contain(100))

		// New indexes should exist
		collMap, ok = m2.coll2Segments.Get(200)
		require.True(t, ok)
		assert.Equal(t, 1, len(collMap))
	})
}

func TestSegmentViewMeta_dropSegmentFromMemory(t *testing.T) {
	m := newTestSegmentViewMeta()

	t.Run("drop non-existent segment", func(t *testing.T) {
		// Should not panic
		m.dropSegmentFromMemory(999)
	})

	t.Run("drop existing segment cleans indexes", func(t *testing.T) {
		seg := newTestSegmentInfo(1, 100, "ch-0")
		seg.CompactionFrom = []int64{10, 20}
		m.setSegment(1, seg)

		// Also add segments 10 and 20 for compactionTo to map
		m.setSegment(10, newTestSegmentInfo(10, 100, "ch-0"))
		m.setSegment(20, newTestSegmentInfo(20, 100, "ch-0"))

		// Verify compactionTo is set
		assert.True(t, m.compactionTo.Contain(10))
		assert.True(t, m.compactionTo.Contain(20))

		m.dropSegmentFromMemory(1)

		// Segment removed
		assert.Nil(t, m.GetSegment(1))

		// CompactionTo cleaned
		assert.False(t, m.compactionTo.Contain(10))
		assert.False(t, m.compactionTo.Contain(20))

		// Other segments still in collection index
		collMap, ok := m.coll2Segments.Get(100)
		require.True(t, ok)
		assert.Equal(t, 2, len(collMap)) // seg 10 and 20 still there
	})
}

func TestSegmentViewMeta_SecondaryIndexes(t *testing.T) {
	m := newTestSegmentViewMeta()

	seg1 := newTestSegmentInfo(1, 100, "ch-0")
	seg2 := newTestSegmentInfo(2, 100, "ch-0")
	seg3 := newTestSegmentInfo(3, 100, "ch-1")
	seg4 := newTestSegmentInfo(4, 200, "ch-0")

	m.setSegment(1, seg1)
	m.setSegment(2, seg2)
	m.setSegment(3, seg3)
	m.setSegment(4, seg4)

	t.Run("coll2Segments", func(t *testing.T) {
		collMap, ok := m.coll2Segments.Get(100)
		require.True(t, ok)
		assert.Equal(t, 3, len(collMap))

		collMap, ok = m.coll2Segments.Get(200)
		require.True(t, ok)
		assert.Equal(t, 1, len(collMap))
	})

	t.Run("remove all segments from collection cleans index", func(t *testing.T) {
		m.dropSegmentFromMemory(4)
		assert.False(t, m.coll2Segments.Contain(200))
	})
}

func TestSegmentViewMeta_CompactionToIndex(t *testing.T) {
	m := newTestSegmentViewMeta()

	// seg1 and seg2 are compacted into seg3
	seg1 := newTestSegmentInfo(1, 100, "ch-0")
	seg2 := newTestSegmentInfo(2, 100, "ch-0")
	seg3 := newTestSegmentInfo(3, 100, "ch-0")
	seg3.CompactionFrom = []int64{1, 2}

	m.setSegment(1, seg1)
	m.setSegment(2, seg2)
	m.setSegment(3, seg3)

	t.Run("compactionTo indexes established", func(t *testing.T) {
		tos, ok := m.compactionTo.Get(1)
		assert.True(t, ok)
		assert.Equal(t, []UniqueID{3}, tos)

		tos, ok = m.compactionTo.Get(2)
		assert.True(t, ok)
		assert.Equal(t, []UniqueID{3}, tos)
	})

	t.Run("overwrite compacted segment cleans old relations", func(t *testing.T) {
		// Replace seg3 with a new segment that has different CompactionFrom
		seg3New := newTestSegmentInfo(3, 100, "ch-0")
		seg3New.CompactionFrom = []int64{2}
		m.setSegment(3, seg3New)

		// seg1 should no longer map to anything
		assert.False(t, m.compactionTo.Contain(1))

		// seg2 still maps to seg3
		tos, ok := m.compactionTo.Get(2)
		assert.True(t, ok)
		assert.Equal(t, []UniqueID{3}, tos)
	})

	t.Run("drop compacted segment cleans relations", func(t *testing.T) {
		m.dropSegmentFromMemory(3)
		assert.False(t, m.compactionTo.Contain(2))
	})
}

func TestSegmentViewMeta_ModifySegments(t *testing.T) {
	m := newTestSegmentViewMeta()
	seg := newTestSegmentInfo(1, 100, "ch-0")
	m.setSegment(1, seg)

	t.Run("single segment - set allocations", func(t *testing.T) {
		allocs := []*Allocation{{NumOfRows: 10}, {NumOfRows: 20}}
		m.ModifySegments(100, []UniqueID{1}, SetAllocations(allocs))
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Len(t, got.allocations, 2)
	})

	t.Run("single segment - add allocation", func(t *testing.T) {
		m.ModifySegments(100, []UniqueID{1}, SetAllocations(nil)) // reset
		alloc := &Allocation{NumOfRows: 5, ExpireTime: 100}
		m.ModifySegments(100, []UniqueID{1}, AddAllocation(alloc))
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Len(t, got.allocations, 1)
		assert.Equal(t, uint64(100), got.GetLastExpireTime())
	})

	t.Run("single segment - set is compacting", func(t *testing.T) {
		m.ModifySegments(100, []UniqueID{1}, SetIsCompacting(true))
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.True(t, got.isCompacting)

		m.ModifySegments(100, []UniqueID{1}, SetIsCompacting(false))
		got = m.GetSegment(1)
		assert.False(t, got.isCompacting)
	})

	t.Run("single segment - set flush time", func(t *testing.T) {
		ft := time.Now()
		m.ModifySegments(100, []UniqueID{1}, SetFlushTime(ft))
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, ft, got.lastFlushTime)
	})

	t.Run("single segment - set last written time", func(t *testing.T) {
		before := time.Now()
		m.ModifySegments(100, []UniqueID{1}, SetLastWrittenTime())
		after := time.Now()
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.False(t, got.lastWrittenTime.Before(before))
		assert.False(t, got.lastWrittenTime.After(after))
	})

	t.Run("single segment - multiple operators at once", func(t *testing.T) {
		m.ModifySegments(100, []UniqueID{1}, SetIsCompacting(true), SetAllocations(nil))
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.True(t, got.isCompacting)
		assert.Empty(t, got.allocations)
	})

	t.Run("batch - same collection", func(t *testing.T) {
		seg2 := newTestSegmentInfo(2, 100, "ch-0")
		seg3 := newTestSegmentInfo(3, 100, "ch-1")
		m.setSegment(2, seg2)
		m.setSegment(3, seg3)

		m.ModifySegments(100, []UniqueID{1, 2, 3}, SetIsCompacting(true))
		for _, id := range []UniqueID{1, 2, 3} {
			got := m.GetSegment(id)
			require.NotNil(t, got)
			assert.True(t, got.isCompacting)
		}
	})

	t.Run("segment not found is skipped", func(t *testing.T) {
		m.ModifySegments(100, []UniqueID{999}, SetIsCompacting(true)) // should not panic
	})

	t.Run("empty segment IDs", func(t *testing.T) {
		m.ModifySegments(100, nil, SetIsCompacting(true)) // should not panic
	})
}

func TestSegmentViewMeta_CompareDataViewVersion(t *testing.T) {
	tests := []struct {
		name     string
		a, b     *viewpb.DataVersion
		expected int
	}{
		{"both nil", nil, nil, 0},
		{"a nil", nil, &viewpb.DataVersion{StreamingVersion: 1}, -1},
		{"b nil", &viewpb.DataVersion{StreamingVersion: 1}, nil, 1},
		{"equal", &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2}, &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2}, 0},
		{"streaming less", &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 5}, &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1}, -1},
		{"streaming greater", &viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 1}, &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 5}, 1},
		{"same streaming compact less", &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2}, -1},
		{"same streaming compact greater", &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 3}, &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, CompareDataViewVersion(tt.a, tt.b))
		})
	}
}

func TestCollectionDataView_AddSegment(t *testing.T) {
	t.Run("new shard and partition", func(t *testing.T) {
		cdv := &CollectionDataView{collectionID: 100, shards: make(map[string]*ShardDataView)}
		cdv.addSegment("ch-0", 1, 100)
		require.Len(t, cdv.shards, 1)
		sdv := cdv.shards["ch-0"]
		require.NotNil(t, sdv)
		require.Len(t, sdv.partitions, 1)
		segSet := sdv.partitions[1]
		require.Len(t, segSet, 1)
		_, ok := segSet[100]
		assert.True(t, ok)
	})

	t.Run("existing shard and partition", func(t *testing.T) {
		cdv := &CollectionDataView{collectionID: 100, shards: make(map[string]*ShardDataView)}
		cdv.addSegment("ch-0", 1, 100)
		cdv.addSegment("ch-0", 1, 200)
		sdv := cdv.shards["ch-0"]
		assert.Len(t, sdv.partitions[1], 2)
	})

	t.Run("existing shard new partition", func(t *testing.T) {
		cdv := &CollectionDataView{collectionID: 100, shards: make(map[string]*ShardDataView)}
		cdv.addSegment("ch-0", 1, 100)
		cdv.addSegment("ch-0", 2, 200)
		sdv := cdv.shards["ch-0"]
		assert.Len(t, sdv.partitions, 2)
	})

	t.Run("new shard", func(t *testing.T) {
		cdv := &CollectionDataView{collectionID: 100, shards: make(map[string]*ShardDataView)}
		cdv.addSegment("ch-0", 1, 100)
		cdv.addSegment("ch-1", 1, 200)
		assert.Len(t, cdv.shards, 2)
	})
}

func TestSegmentViewMeta_DataViewVersionKey(t *testing.T) {
	t.Run("nil version", func(t *testing.T) {
		key := newDataViewVersionKey(nil)
		assert.Equal(t, dataViewVersionKey{}, key)
	})

	t.Run("non-nil version", func(t *testing.T) {
		key := newDataViewVersionKey(&viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2})
		assert.Equal(t, dataViewVersionKey{StreamingVersion: 1, CompactVersion: 2}, key)
	})
}

func TestSegmentViewMeta_NewCollectionDataViews(t *testing.T) {
	dvc := newCollectionDataViews()
	assert.NotNil(t, dvc.views)
	assert.Nil(t, dvc.currentVersion)
	assert.Nil(t, dvc.versionList)
}

func TestSegmentViewMeta_MultipleCompactionTargets(t *testing.T) {
	m := newTestSegmentViewMeta()

	// seg1 is compacted into both seg2 and seg3
	seg1 := newTestSegmentInfo(1, 100, "ch-0")
	seg2 := newTestSegmentInfo(2, 100, "ch-0")
	seg2.CompactionFrom = []int64{1}
	seg3 := newTestSegmentInfo(3, 100, "ch-0")
	seg3.CompactionFrom = []int64{1}

	m.setSegment(1, seg1)
	m.setSegment(2, seg2)
	m.setSegment(3, seg3)

	result, exist := m.GetCompactionTo(1)
	assert.True(t, exist)
	assert.Len(t, result, 2)

	ids := make(map[int64]bool)
	for _, r := range result {
		ids[r.GetID()] = true
	}
	assert.True(t, ids[2])
	assert.True(t, ids[3])
}

func TestSegmentViewMeta_GetCompactionTo_BrokenRelation(t *testing.T) {
	m := newTestSegmentViewMeta()

	seg1 := newTestSegmentInfo(1, 100, "ch-0")
	seg2 := newTestSegmentInfo(2, 100, "ch-0")
	seg2.CompactionFrom = []int64{1}

	m.setSegment(1, seg1)
	m.setSegment(2, seg2)

	// Now remove seg2 directly from segments map (simulating broken relation)
	m.segments.Remove(2)

	result, exist := m.GetCompactionTo(1)
	assert.True(t, exist)
	assert.Nil(t, result) // broken relation returns nil
}

// ---------------------------------------------------------------------------
// DataView tests
// ---------------------------------------------------------------------------

func newTestDataView(sv, cv int64, segIDs ...int64) *CollectionDataView {
	cdv := &CollectionDataView{
		collectionID: 0,
		version: &viewpb.DataVersion{
			StreamingVersion: sv,
			CompactVersion:   cv,
		},
		shards: make(map[string]*ShardDataView),
	}
	for _, id := range segIDs {
		cdv.addSegment("ch-0", 1, id)
	}
	return cdv
}

func TestSegmentViewMeta_GetCurrentVersion(t *testing.T) {
	t.Run("collection not found", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		assert.Nil(t, m.GetCurrentVersion(999))
	})

	t.Run("no version set", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		dvc := newCollectionDataViews()
		m.dataViews.Insert(100, dvc)
		assert.Nil(t, m.GetCurrentVersion(100))
	})

	t.Run("returns deep clone", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0, 10))

		got := m.GetCurrentVersion(100)
		require.NotNil(t, got)
		assert.Equal(t, int64(1), got.GetStreamingVersion())

		// Modify returned value and verify original is unchanged.
		got.StreamingVersion = 999
		got2 := m.GetCurrentVersion(100)
		assert.Equal(t, int64(1), got2.GetStreamingVersion())
	})
}

func TestSegmentViewMeta_GetDataView(t *testing.T) {
	t.Run("collection not found", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		assert.Nil(t, m.GetDataView(999, &viewpb.DataVersion{StreamingVersion: 1}))
	})

	t.Run("version not found", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0, 10))
		assert.Nil(t, m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 2}))
	})

	t.Run("found and deep cloned", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		view := newTestDataView(1, 0, 10)
		m.addDataView(100, view)

		got := m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0})
		require.NotNil(t, got)
		assert.Equal(t, int64(1), got.GetDataVersion().GetStreamingVersion())
		require.Len(t, got.GetShards(), 1)

		// Modify returned value and verify original is unchanged.
		got.Shards = nil
		got2 := m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1})
		require.Len(t, got2.GetShards(), 1)
	})
}

func TestSegmentViewMeta_ListDataViews(t *testing.T) {
	t.Run("collection not found", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		assert.Nil(t, m.ListDataViews(999))
	})

	t.Run("empty version list", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		dvc := newCollectionDataViews()
		m.dataViews.Insert(100, dvc)
		assert.Nil(t, m.ListDataViews(100))
	})

	t.Run("multiple versions in ascending order", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0, 10))
		m.addDataView(100, newTestDataView(3, 0, 30))
		m.addDataView(100, newTestDataView(2, 0, 20))

		result := m.ListDataViews(100)
		require.Len(t, result, 3)
		assert.Equal(t, int64(1), result[0].GetDataVersion().GetStreamingVersion())
		assert.Equal(t, int64(2), result[1].GetDataVersion().GetStreamingVersion())
		assert.Equal(t, int64(3), result[2].GetDataVersion().GetStreamingVersion())
	})
}

func TestSegmentViewMeta_AddDataViewToMemory(t *testing.T) {
	t.Run("versions added in order", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0))
		m.addDataView(100, newTestDataView(2, 0))
		m.addDataView(100, newTestDataView(3, 0))

		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(3), cv.GetStreamingVersion())

		views := m.ListDataViews(100)
		require.Len(t, views, 3)
		for i, v := range views {
			assert.Equal(t, int64(i+1), v.GetDataVersion().GetStreamingVersion())
		}
	})

	t.Run("versions added out of order", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(3, 0))
		m.addDataView(100, newTestDataView(1, 0))
		m.addDataView(100, newTestDataView(2, 0))

		// currentVersion should be the max.
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(3), cv.GetStreamingVersion())

		// List should be sorted ascending.
		views := m.ListDataViews(100)
		require.Len(t, views, 3)
		assert.Equal(t, int64(1), views[0].GetDataVersion().GetStreamingVersion())
		assert.Equal(t, int64(2), views[1].GetDataVersion().GetStreamingVersion())
		assert.Equal(t, int64(3), views[2].GetDataVersion().GetStreamingVersion())
	})

	t.Run("creates collection entry if not exists", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(200, newTestDataView(1, 0))

		assert.True(t, m.dataViews.Contain(200))
		assert.NotNil(t, m.GetCurrentVersion(200))
	})

	t.Run("GetDataView returns proto copy", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		view := newTestDataView(1, 0, 10)
		m.addDataView(100, view)

		// GetDataView returns a proto copy; modifying it should not affect the stored data.
		got := m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1})
		require.NotNil(t, got)
		require.Len(t, got.GetShards(), 1)

		got.Shards = nil
		got2 := m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1})
		require.Len(t, got2.GetShards(), 1)
	})
}

func TestSegmentViewMeta_UpdateDataViewOnFlush(t *testing.T) {
	m := newTestSegmentViewMeta()
	view := newTestDataView(1, 0, 10)
	m.addDataView(100, view)

	// Verify view is stored.
	got := m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1})
	require.NotNil(t, got)

	// Verify dataViews entry exists.
	require.True(t, m.dataViews.Contain(100))
}

func TestSegmentViewMeta_UpdateDataViewOnCompaction(t *testing.T) {
	t.Run("nil view is no-op", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, nil)
		assert.False(t, m.dataViews.Contain(100))
	})

	t.Run("old segments removed, new segments added", func(t *testing.T) {
		m := newTestSegmentViewMeta()

		// Pre-populate with a flush view.
		flushView := newTestDataView(1, 0, 10, 20)
		m.addDataView(100, flushView)

		// Compaction: segments 10,20 -> segment 30.
		compactView := newTestDataView(1, 1, 30)
		m.addDataView(100, compactView)

		// Verify compaction view is stored.
		got := m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1})
		require.NotNil(t, got)
	})
}

func TestSegmentViewMeta_SegmentDVRefs(t *testing.T) {
	t.Run("addDataView increments refs", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0, 1, 2))

		assert.True(t, m.IsSegmentInDataView(100, 1))
		assert.True(t, m.IsSegmentInDataView(100, 2))
		assert.False(t, m.IsSegmentInDataView(100, 999))
	})

	t.Run("multiple views increment refs", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0, 1, 2))
		m.addDataView(100, newTestDataView(2, 0, 1, 3))

		assert.True(t, m.IsSegmentInDataView(100, 1)) // ref=2
		assert.True(t, m.IsSegmentInDataView(100, 2)) // ref=1
		assert.True(t, m.IsSegmentInDataView(100, 3)) // ref=1
	})

	t.Run("DropDataView decrements refs", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataView(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.addDataView(100, newTestDataView(1, 0, 1, 2))
		m.addDataView(100, newTestDataView(2, 0, 1, 3))

		// Drop version (1,0): segment 2 should lose its only ref
		err := m.DropDataView(context.Background(), 100, &viewpb.DataVersion{StreamingVersion: 1})
		assert.NoError(t, err)

		assert.True(t, m.IsSegmentInDataView(100, 1))  // still ref=1 from v(2,0)
		assert.False(t, m.IsSegmentInDataView(100, 2)) // ref=0, removed
		assert.True(t, m.IsSegmentInDataView(100, 3))  // ref=1
	})

	t.Run("DropDataViewsByCollection clears all refs", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataViewsByCollection(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.addDataView(100, newTestDataView(1, 0, 1, 2))
		assert.True(t, m.IsSegmentInDataView(100, 1))

		err := m.DropDataViewsByCollection(context.Background(), 100)
		assert.NoError(t, err)
		assert.False(t, m.IsSegmentInDataView(100, 1))
	})

	t.Run("different collection isolation", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		m.addDataView(100, newTestDataView(1, 0, 1))
		m.addDataView(200, newTestDataView(1, 0, 2))

		assert.True(t, m.IsSegmentInDataView(100, 1))
		assert.False(t, m.IsSegmentInDataView(100, 2))
		assert.False(t, m.IsSegmentInDataView(200, 1))
		assert.True(t, m.IsSegmentInDataView(200, 2))
	})
}

func TestSegmentViewMeta_DropDataView(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataView(mock.Anything, int64(100), mock.Anything).
			Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.DropDataView(context.Background(), 100, &viewpb.DataVersion{StreamingVersion: 1})
		assert.Error(t, err)
	})

	t.Run("collection not found after catalog success", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataView(mock.Anything, int64(100), mock.Anything).
			Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.DropDataView(context.Background(), 100, &viewpb.DataVersion{StreamingVersion: 1})
		assert.NoError(t, err)
	})

	t.Run("removes view and updates state", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataView(mock.Anything, int64(100), mock.Anything).
			Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Add two views.
		m.addDataView(100, newTestDataView(1, 0, 10))
		m.addDataView(100, newTestDataView(2, 0, 20))

		// Drop version (1, 0).
		err := m.DropDataView(context.Background(), 100, &viewpb.DataVersion{StreamingVersion: 1})
		assert.NoError(t, err)

		// Version (1, 0) should be gone.
		assert.Nil(t, m.GetDataView(100, &viewpb.DataVersion{StreamingVersion: 1}))

		// currentVersion should be (2, 0).
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(2), cv.GetStreamingVersion())

		// ListDataViews should have 1 entry.
		views := m.ListDataViews(100)
		assert.Len(t, views, 1)
	})

	t.Run("dropping last version sets nil currentVersion", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataView(mock.Anything, int64(100), mock.Anything).
			Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.addDataView(100, newTestDataView(1, 0, 10))

		err := m.DropDataView(context.Background(), 100, &viewpb.DataVersion{StreamingVersion: 1})
		assert.NoError(t, err)

		assert.Nil(t, m.GetCurrentVersion(100))
		assert.Nil(t, m.ListDataViews(100))
	})
}

func TestSegmentViewMeta_DropDataViewsByCollection(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataViewsByCollection(mock.Anything, int64(100)).
			Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.DropDataViewsByCollection(context.Background(), 100)
		assert.Error(t, err)
	})

	t.Run("removes all views for collection", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropDataViewsByCollection(mock.Anything, int64(100)).
			Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.addDataView(100, newTestDataView(1, 0, 10))
		m.addDataView(100, newTestDataView(2, 0, 20))

		err := m.DropDataViewsByCollection(context.Background(), 100)
		assert.NoError(t, err)

		assert.False(t, m.dataViews.Contain(100))
		assert.Nil(t, m.GetCurrentVersion(100))
	})
}

func TestSegmentViewMeta_ReloadDataViews(t *testing.T) {
	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListDataViews(mock.Anything).
			Return(nil, fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.reloadDataViews()
		assert.Error(t, err)
	})

	t.Run("empty catalog", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListDataViews(mock.Anything).
			Return(map[int64][]*viewpb.DataViewOfCollection{}, nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.reloadDataViews()
		assert.NoError(t, err)
		assert.False(t, m.dataViews.Contain(100))
	})

	t.Run("loads views and reconstructs state", func(t *testing.T) {
		v1 := newTestDataView(1, 0, 10, 20).toProto()
		v2 := newTestDataView(2, 0, 30).toProto()
		v3 := newTestDataView(3, 0, 40, 50).toProto()

		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListDataViews(mock.Anything).
			Return(map[int64][]*viewpb.DataViewOfCollection{
				100: {v3, v1, v2}, // deliberately unordered
			}, nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Pre-load segments.
		m.setSegment(10, newTestSegmentInfo(10, 100, "ch-0"))
		m.setSegment(20, newTestSegmentInfo(20, 100, "ch-0"))
		m.setSegment(30, newTestSegmentInfo(30, 100, "ch-0"))
		// Segment from a different collection should not appear.
		m.setSegment(99, newTestSegmentInfo(99, 200, "ch-0"))

		err := m.reloadDataViews()
		assert.NoError(t, err)

		// Verify views are loaded.
		require.True(t, m.dataViews.Contain(100))

		// Verify currentVersion is the max.
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(3), cv.GetStreamingVersion())

		// Verify versionList is sorted ascending.
		views := m.ListDataViews(100)
		require.Len(t, views, 3)
		assert.Equal(t, int64(1), views[0].GetDataVersion().GetStreamingVersion())
		assert.Equal(t, int64(2), views[1].GetDataVersion().GetStreamingVersion())
		assert.Equal(t, int64(3), views[2].GetDataVersion().GetStreamingVersion())

		// Collection 200 should not have data views.
		assert.False(t, m.dataViews.Contain(200))
	})

	t.Run("segments without DataVersion are skipped", func(t *testing.T) {
		v1Proto := newTestDataView(1, 0, 10).toProto()

		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListDataViews(mock.Anything).
			Return(map[int64][]*viewpb.DataViewOfCollection{
				100: {v1Proto},
			}, nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Segment without DataVersion.
		seg10 := newTestSegmentInfo(10, 100, "ch-0")
		m.setSegment(10, seg10)

		err := m.reloadDataViews()
		assert.NoError(t, err)

		require.True(t, m.dataViews.Contain(100))
	})
}

// ---------------------------------------------------------------------------
// Persistence method tests
// ---------------------------------------------------------------------------

func TestSegmentViewMeta_DropSegment(t *testing.T) {
	t.Run("drop existing segment", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegment(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := newTestSegmentInfo(1, 100, "ch-0")
		m.setSegment(1, seg)

		err := m.DropSegment(context.Background(), 100, 1)
		assert.NoError(t, err)
		assert.Nil(t, m.GetSegment(1))
	})

	t.Run("drop non-existent segment is no-op", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.DropSegment(context.Background(), 100, 999)
		assert.NoError(t, err)
	})

	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().DropSegment(mock.Anything, mock.Anything).
			Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := newTestSegmentInfo(1, 100, "ch-0")
		m.setSegment(1, seg)

		err := m.DropSegment(context.Background(), 100, 1)
		assert.Error(t, err)
		// Segment should still be in memory after catalog failure.
		assert.NotNil(t, m.GetSegment(1))
	})
}

func TestSegmentViewMeta_UpdateSegments(t *testing.T) {
	t.Run("update segment state", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				InsertChannel: "ch-0",
				State:         commonpb.SegmentState_Growing,
			},
		}
		m.setSegment(1, seg)

		err := m.UpdateSegments(context.Background(), 100,
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed))
		assert.NoError(t, err)

		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, commonpb.SegmentState_Flushed, got.GetState())
	})

	t.Run("no segments modified", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Operator on non-existent segment -> no changes.
		err := m.UpdateSegments(context.Background(), 100,
			UpdateStatusOperator(999, commonpb.SegmentState_Flushed))
		assert.NoError(t, err)
	})

	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).
			Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				InsertChannel: "ch-0",
				State:         commonpb.SegmentState_Growing,
			},
		}
		m.setSegment(1, seg)

		err := m.UpdateSegments(context.Background(), 100,
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed))
		assert.Error(t, err)

		// Segment state should NOT be updated in memory after catalog failure.
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, commonpb.SegmentState_Growing, got.GetState())
	})

	t.Run("collectionID cross-check rejects wrong collection", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  200,
				InsertChannel: "ch-0",
				State:         commonpb.SegmentState_Growing,
			},
		}
		m.setSegment(1, seg)

		// Segment belongs to collection 200, but UpdateSegments called with 100.
		err := m.UpdateSegments(context.Background(), 100,
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed))
		assert.NoError(t, err) // no segments modified, no error

		// State should be unchanged.
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, commonpb.SegmentState_Growing, got.GetState())
	})
}

func TestSegmentViewMeta_FlushSegments(t *testing.T) {
	t.Run("flush segment creates DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "ch-0",
				State:         commonpb.SegmentState_Growing,
			},
		}
		m.setSegment(1, seg)

		err := m.FlushSegments(context.Background(), 100, []int64{1},
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed))
		assert.NoError(t, err)

		// Verify segment state updated.
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, commonpb.SegmentState_Flushed, got.GetState())

		// Verify DataView created.
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(1), cv.GetStreamingVersion())
		assert.Equal(t, int64(0), cv.GetCompactVersion())

	})

	t.Run("flush increments streaming version", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Pre-populate with a DataView at version (1, 0).
		m.addDataView(100, newTestDataView(1, 0, 10))

		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "ch-0",
				State:         commonpb.SegmentState_Growing,
			},
		}
		m.setSegment(2, seg)

		err := m.FlushSegments(context.Background(), 100, []int64{2},
			UpdateStatusOperator(2, commonpb.SegmentState_Flushed))
		assert.NoError(t, err)

		// New version should be (2, 0).
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(2), cv.GetStreamingVersion())
		assert.Equal(t, int64(0), cv.GetCompactVersion())

		// DataView should contain both segments.
		view := m.GetDataView(100, cv)
		require.NotNil(t, view)
	})

	t.Run("multi-segment flush", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", State: commonpb.SegmentState_Growing,
		}}
		seg2 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 20, InsertChannel: "ch-0", State: commonpb.SegmentState_Growing,
		}}
		m.setSegment(1, seg1)
		m.setSegment(2, seg2)

		err := m.FlushSegments(context.Background(), 100, []int64{1, 2},
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed),
			UpdateStatusOperator(2, commonpb.SegmentState_Flushed))
		assert.NoError(t, err)

		// Both segments should be Flushed with same DataVersion.
		got1 := m.GetSegment(1)
		got2 := m.GetSegment(2)
		assert.Equal(t, commonpb.SegmentState_Flushed, got1.GetState())
		assert.Equal(t, commonpb.SegmentState_Flushed, got2.GetState())

		// Only one DataView version bump.
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(1), cv.GetStreamingVersion())
	})

	t.Run("no segments modified returns nil", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Operator on non-existent segment -> no changes.
		err := m.FlushSegments(context.Background(), 100, []int64{999},
			UpdateStatusOperator(999, commonpb.SegmentState_Flushed))
		assert.NoError(t, err)
	})

	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "ch-0",
				State:         commonpb.SegmentState_Growing,
			},
		}
		m.setSegment(1, seg)

		err := m.FlushSegments(context.Background(), 100, []int64{1},
			UpdateStatusOperator(1, commonpb.SegmentState_Flushed))
		assert.Error(t, err)

		// Segment state should NOT be updated in memory after failure.
		got := m.GetSegment(1)
		require.NotNil(t, got)
		assert.Equal(t, commonpb.SegmentState_Growing, got.GetState())

		// DataView should NOT exist.
		assert.Nil(t, m.GetCurrentVersion(100))
	})
}

func TestSegmentViewMeta_DropPartition(t *testing.T) {
	t.Run("drops segments and removes from DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Partition 10: segments 1,2; Partition 20: segment 3
		seg1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
		}}
		seg2 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
		}}
		seg3 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 3, CollectionID: 100, PartitionID: 20, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
		}}
		m.setSegment(1, seg1)
		m.setSegment(2, seg2)
		m.setSegment(3, seg3)

		// DataView with all 3 segments
		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		cdv.addSegment("ch-0", 10, 2)
		cdv.addSegment("ch-0", 20, 3)
		m.addDataView(100, cdv)

		// Drop partition 10
		err := m.DropPartition(context.Background(), 100, []int64{10})
		assert.NoError(t, err)

		// Segments 1,2 should be Dropped
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(1).GetState())
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(2).GetState())
		// Segment 3 should remain Flushed
		assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(3).GetState())

		// DataView should only have segment 3
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(2), cv.GetStreamingVersion())

		// Ref counts: segments 1,2 still referenced by old DataView version (1,0).
		// They will become unreferenced after old version is GC'd via DropDataView.
		assert.True(t, m.IsSegmentInDataView(100, 1))
		assert.True(t, m.IsSegmentInDataView(100, 2))
		assert.True(t, m.IsSegmentInDataView(100, 3))

		// Verify new DataView version does not contain partition 10 segments.
		view := m.GetDataView(100, cv)
		require.NotNil(t, view)
		for _, shard := range view.GetShards() {
			for _, part := range shard.GetPartitions() {
				assert.NotEqual(t, int64(10), part.GetPartitionId())
			}
		}
	})

	t.Run("no segments in partition", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.DropPartition(context.Background(), 100, []int64{999})
		assert.NoError(t, err)
	})

	t.Run("no DataView exists", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
		}}
		m.setSegment(1, seg)

		err := m.DropPartition(context.Background(), 100, []int64{10})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(1).GetState())
	})

	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed,
		}}
		m.setSegment(1, seg)

		err := m.DropPartition(context.Background(), 100, []int64{10})
		assert.Error(t, err)
		// Memory should not be updated on failure
		assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(1).GetState())
	})
}

// ---------------------------------------------------------------------------
// TruncateCollection tests
// ---------------------------------------------------------------------------

func TestSegmentViewMeta_TruncateCollection(t *testing.T) {
	t.Run("drops segments and removes from DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// ch-0: segments 1,2 (flushTs=100, DmlPosition before truncate point)
		// ch-1: segment 3 (flushTs=200, DmlPosition before truncate point)
		// ch-0: segment 4 (DmlPosition after truncate point, should NOT be dropped)
		seg1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed, DmlPosition: &msgpb.MsgPosition{Timestamp: 90},
		}}
		seg2 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 2, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed, DmlPosition: &msgpb.MsgPosition{Timestamp: 95},
		}}
		seg3 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 3, CollectionID: 100, PartitionID: 20, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, DmlPosition: &msgpb.MsgPosition{Timestamp: 180},
		}}
		seg4 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 4, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed, DmlPosition: &msgpb.MsgPosition{Timestamp: 150},
		}}
		m.setSegment(1, seg1)
		m.setSegment(2, seg2)
		m.setSegment(3, seg3)
		m.setSegment(4, seg4)

		// DataView with all 4 segments
		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		cdv.addSegment("ch-0", 10, 2)
		cdv.addSegment("ch-1", 20, 3)
		cdv.addSegment("ch-0", 10, 4)
		m.addDataView(100, cdv)

		// Truncate: segments 1,2,3 (pre-filtered by caller), flushTs per channel
		flushTsList := map[string]uint64{"ch-0": 100, "ch-1": 200}
		err := m.TruncateCollection(context.Background(), 100, []int64{1, 2, 3}, flushTsList)
		assert.NoError(t, err)

		// Segments 1,2,3 should be Dropped
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(1).GetState())
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(2).GetState())
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(3).GetState())
		// Segment 4 should remain Flushed
		assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(4).GetState())

		// DataView version should bump
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(2), cv.GetStreamingVersion())
		assert.Equal(t, int64(0), cv.GetCompactVersion())

		// New DataView should only contain segment 4
		view := m.GetDataView(100, cv)
		require.NotNil(t, view)
		var segIDs []int64
		for _, shard := range view.GetShards() {
			for _, part := range shard.GetPartitions() {
				segIDs = append(segIDs, part.GetSegmentIds()...)
			}
		}
		assert.ElementsMatch(t, []int64{4}, segIDs)

		// deleteApplyStartAfterTimetick should be updated
		for _, shard := range view.GetShards() {
			if shard.GetVchannel() == "ch-0" {
				assert.Equal(t, uint64(100), shard.GetDeleteApplyStartAfterTimetick())
			}
			if shard.GetVchannel() == "ch-1" {
				assert.Equal(t, uint64(200), shard.GetDeleteApplyStartAfterTimetick())
			}
		}
	})

	t.Run("skips already dropped segments", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Dropped,
		}}
		m.setSegment(1, seg)

		// No segments to drop, no catalog call expected
		err := m.TruncateCollection(context.Background(), 100, []int64{1}, map[string]uint64{"ch-0": 100})
		assert.NoError(t, err)
	})

	t.Run("skips nonexistent segments", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.TruncateCollection(context.Background(), 100, []int64{999}, map[string]uint64{"ch-0": 100})
		assert.NoError(t, err)
	})

	t.Run("no DataView exists", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed,
		}}
		m.setSegment(1, seg)

		err := m.TruncateCollection(context.Background(), 100, []int64{1}, map[string]uint64{"ch-0": 100})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(1).GetState())
	})

	t.Run("deleteApplyStartAfterTimetick uses max", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed,
		}}
		m.setSegment(1, seg)

		// DataView with existing deleteApplyStartAfterTimetick = 500
		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			shards: map[string]*ShardDataView{
				"ch-0": {
					vchannel:                      "ch-0",
					deleteApplyStartAfterTimetick: 500,
					partitions: map[int64]map[int64]struct{}{
						10: {1: {}},
					},
				},
			},
		}
		m.addDataView(100, cdv)

		// Truncate with flushTs=200, which is less than existing 500 — should keep 500
		err := m.TruncateCollection(context.Background(), 100, []int64{1}, map[string]uint64{"ch-0": 200})
		assert.NoError(t, err)

		view := m.GetDataView(100, m.GetCurrentVersion(100))
		require.NotNil(t, view)
		for _, shard := range view.GetShards() {
			if shard.GetVchannel() == "ch-0" {
				assert.Equal(t, uint64(500), shard.GetDeleteApplyStartAfterTimetick())
			}
		}
	})

	t.Run("catalog error rolls back", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch-0",
			State: commonpb.SegmentState_Flushed,
		}}
		m.setSegment(1, seg)

		err := m.TruncateCollection(context.Background(), 100, []int64{1}, map[string]uint64{"ch-0": 100})
		assert.Error(t, err)
		// Memory should not be updated on failure
		assert.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(1).GetState())
	})
}

// ---------------------------------------------------------------------------
// Compaction method tests
// ---------------------------------------------------------------------------

func newFlushedSegmentInfo(id, collectionID, partitionID int64, channel string) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			Level:         datapb.SegmentLevel_L1,
			StartPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 100},
			DmlPosition:   &msgpb.MsgPosition{ChannelName: channel, Timestamp: 200},
		},
	}
}

func TestSegmentViewMeta_BuildCompactionDataView(t *testing.T) {
	t.Run("no DataView for collection returns nil", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		result := m.buildCompactionDataView(100, "ch-0", 10, []int64{1}, []int64{2})
		assert.Nil(t, result)
	})

	t.Run("no current version returns nil", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		dvc := newCollectionDataViews()
		m.dataViews.Insert(100, dvc)
		result := m.buildCompactionDataView(100, "ch-0", 10, []int64{1}, []int64{2})
		assert.Nil(t, result)
	})

	t.Run("removes old segments and adds new", func(t *testing.T) {
		m := newTestSegmentViewMeta()
		// Set up a DataView with segments 1, 2, 3
		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		cdv.addSegment("ch-0", 10, 2)
		cdv.addSegment("ch-0", 10, 3)
		m.addDataView(100, cdv)

		// Compact segments 1, 2 -> segment 4
		result := m.buildCompactionDataView(100, "ch-0", 10, []int64{1, 2}, []int64{4})
		require.NotNil(t, result)

		// Version should have incremented compact version
		assert.Equal(t, int64(2), result.version.GetStreamingVersion())
		assert.Equal(t, int64(1), result.version.GetCompactVersion())

		// Should contain segments 3 and 4 (not 1 and 2)
		resultProto := result.toProto()
		var segIDs []int64
		for _, shard := range resultProto.GetShards() {
			for _, part := range shard.GetPartitions() {
				segIDs = append(segIDs, part.GetSegmentIds()...)
			}
		}
		assert.Contains(t, segIDs, int64(3))
		assert.Contains(t, segIDs, int64(4))
		assert.NotContains(t, segIDs, int64(1))
		assert.NotContains(t, segIDs, int64(2))
	})
}

func TestSegmentViewMeta_CompleteCompactionMutation_Mix(t *testing.T) {
	t.Run("successful mix compaction", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Set up input segments
		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))
		m.setSegment(2, newFlushedSegmentInfo(2, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        1000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1, 2},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 3,
					NumOfRows: 1500,
				},
			},
		}

		segments, mutation, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)
		assert.Equal(t, int64(3), segments[0].GetID())
		assert.Equal(t, commonpb.SegmentState_Flushed, segments[0].GetState())
		assert.NotNil(t, mutation)

		// Input segments should be marked as dropped
		seg1 := m.GetSegment(1)
		require.NotNil(t, seg1)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg1.GetState())
		assert.True(t, seg1.GetCompacted())

		seg2 := m.GetSegment(2)
		require.NotNil(t, seg2)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg2.GetState())
		assert.True(t, seg2.GetCompacted())

		// Output segment should be in memory
		seg3 := m.GetSegment(3)
		require.NotNil(t, seg3)
		assert.Equal(t, commonpb.SegmentState_Flushed, seg3.GetState())
		assert.Equal(t, []int64{1, 2}, seg3.GetCompactionFrom())
	})

	t.Run("mix compaction with zero rows drops output segment", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        1000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 0,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)
		assert.Equal(t, commonpb.SegmentState_Dropped, segments[0].GetState())
	})

	t.Run("mix compaction with input segment not found", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		task := &datapb.CompactionTask{
			PlanID:        1000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{999},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{}

		_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.Error(t, err)
	})

	t.Run("mix compaction catalog error on compactTo", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        1000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 500,
				},
			},
		}

		_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.Error(t, err)

		// Input segment should not be modified
		seg1 := m.GetSegment(1)
		require.NotNil(t, seg1)
		assert.Equal(t, commonpb.SegmentState_Flushed, seg1.GetState())
	})

	t.Run("mix compaction with DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Set up segments and DataView
		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))
		m.setSegment(2, newFlushedSegmentInfo(2, 100, 10, "ch-0"))

		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		cdv.addSegment("ch-0", 10, 2)
		m.addDataView(100, cdv)

		task := &datapb.CompactionTask{
			PlanID:        1000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_MixCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1, 2},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 3,
					NumOfRows: 1500,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)

		// Verify DataView was updated
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(2), cv.GetStreamingVersion())
		assert.Equal(t, int64(1), cv.GetCompactVersion())

		// CompactTo segment should have data_version set
	})
}

func TestSegmentViewMeta_CompleteCompactionMutation_Cluster(t *testing.T) {
	t.Run("successful cluster compaction", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))
		m.setSegment(2, newFlushedSegmentInfo(2, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        2000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1, 2},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 3,
					NumOfRows: 800,
				},
				{
					SegmentID: 4,
					NumOfRows: 700,
				},
			},
		}

		segments, mutation, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 2)
		assert.NotNil(t, mutation)

		// Output segments should be L2 and invisible
		for _, seg := range segments {
			assert.Equal(t, commonpb.SegmentState_Flushed, seg.GetState())
			assert.Equal(t, datapb.SegmentLevel_L2, seg.GetLevel())
			assert.True(t, seg.GetIsInvisible())
			assert.True(t, seg.GetCreatedByCompaction())
		}

		// Verify segments in memory
		assert.NotNil(t, m.GetSegment(3))
		assert.NotNil(t, m.GetSegment(4))
	})

	t.Run("cluster compaction with input segment not found", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		task := &datapb.CompactionTask{
			PlanID:        2000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{999},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{}

		_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.Error(t, err)
	})

	t.Run("cluster compaction catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        2000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 500,
				},
			},
		}

		_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.Error(t, err)
	})

	t.Run("cluster compaction with DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		m.addDataView(100, cdv)

		task := &datapb.CompactionTask{
			PlanID:        2000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_ClusteringCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 500,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)

		// Verify DataView was updated
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(1), cv.GetStreamingVersion())
		assert.Equal(t, int64(1), cv.GetCompactVersion())

	})
}

func TestSegmentViewMeta_CompleteCompactionMutation_Sort(t *testing.T) {
	t.Run("successful sort compaction", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 900,
				},
			},
		}

		segments, mutation, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)
		assert.Equal(t, int64(2), segments[0].GetID())
		assert.True(t, segments[0].GetIsSorted())
		assert.Equal(t, commonpb.SegmentState_Flushed, segments[0].GetState())
		assert.NotNil(t, mutation)

		// Input segment should be dropped
		seg1 := m.GetSegment(1)
		require.NotNil(t, seg1)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg1.GetState())
		assert.True(t, seg1.GetCompacted())

		// Output segment should be in memory
		seg2 := m.GetSegment(2)
		require.NotNil(t, seg2)
		assert.True(t, seg2.GetIsSorted())
	})

	t.Run("sort compaction with zero rows drops output segment", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 0,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)
		assert.Equal(t, commonpb.SegmentState_Dropped, segments[0].GetState())
	})

	t.Run("sort compaction with input segment not found", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{999},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 500,
				},
			},
		}

		_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.Error(t, err)
	})

	t.Run("sort compaction catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 500,
				},
			},
		}

		_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.Error(t, err)

		// Input segment should not be modified on catalog error
		seg1 := m.GetSegment(1)
		require.NotNil(t, seg1)
		assert.Equal(t, commonpb.SegmentState_Flushed, seg1.GetState())
	})

	t.Run("sort compaction with DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 1},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		m.addDataView(100, cdv)

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 900,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)

		// Verify DataView was updated
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(3), cv.GetStreamingVersion())
		assert.Equal(t, int64(2), cv.GetCompactVersion())

	})

	t.Run("sort compaction preserves invisible for compaction-created segment", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := newFlushedSegmentInfo(1, 100, 10, "ch-0")
		seg.CreatedByCompaction = true
		seg.IsInvisible = true
		m.setSegment(1, seg)

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 900,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)
		assert.True(t, segments[0].GetIsInvisible())
	})

	t.Run("sort compaction clears invisible for non-compaction-created segment", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := newFlushedSegmentInfo(1, 100, 10, "ch-0")
		seg.CreatedByCompaction = false
		seg.IsInvisible = true
		m.setSegment(1, seg)

		task := &datapb.CompactionTask{
			PlanID:        3000,
			CollectionID:  100,
			PartitionID:   10,
			Type:          datapb.CompactionType_SortCompaction,
			Channel:       "ch-0",
			InputSegments: []int64{1},
			StartTime:     time.Now().Unix(),
		}
		result := &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 2,
					NumOfRows: 900,
				},
			},
		}

		segments, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		assert.NoError(t, err)
		require.Len(t, segments, 1)
		assert.False(t, segments[0].GetIsInvisible())
	})
}

func TestSegmentViewMeta_CompleteCompactionMutation_InvalidType(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	m := newSegmentViewMeta(context.Background(), catalog)

	task := &datapb.CompactionTask{
		PlanID:        5000,
		CollectionID:  100,
		Type:          datapb.CompactionType_UndefinedCompaction,
		InputSegments: []int64{1},
	}
	result := &datapb.CompactionPlanResult{}

	_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// CompleteCompactionHandoff tests
// ---------------------------------------------------------------------------

func TestSegmentViewMeta_CompleteCompactionHandoff(t *testing.T) {
	t.Run("no DataView returns nil", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))

		err := m.CompleteCompactionHandoff(context.Background(), 100, []int64{1}, []int64{2}, "ch-0", 10)
		assert.NoError(t, err)
	})

	t.Run("successful handoff updates DataView and drops compactFrom", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Set up segments and DataView
		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))
		m.setSegment(2, newFlushedSegmentInfo(2, 100, 10, "ch-0"))
		seg3 := newFlushedSegmentInfo(3, 100, 10, "ch-0")
		seg3.CompactionFrom = []int64{1, 2}
		m.setSegment(3, seg3)

		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		cdv.addSegment("ch-0", 10, 2)
		m.addDataView(100, cdv)

		err := m.CompleteCompactionHandoff(context.Background(), 100, []int64{1, 2}, []int64{3}, "ch-0", 10)
		assert.NoError(t, err)

		// compactFrom should be dropped
		seg1 := m.GetSegment(1)
		require.NotNil(t, seg1)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg1.GetState())
		assert.True(t, seg1.GetCompacted())

		seg2 := m.GetSegment(2)
		require.NotNil(t, seg2)
		assert.Equal(t, commonpb.SegmentState_Dropped, seg2.GetState())

		// compactTo should have data_version set
		got3 := m.GetSegment(3)
		require.NotNil(t, got3)

		// DataView should be updated
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(2), cv.GetStreamingVersion())
		assert.Equal(t, int64(1), cv.GetCompactVersion())
	})

	t.Run("compactTo segment not found returns error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		m.setSegment(1, newFlushedSegmentInfo(1, 100, 10, "ch-0"))
		cdv := &CollectionDataView{
			collectionID: 100,
			version:      &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			shards:       make(map[string]*ShardDataView),
		}
		cdv.addSegment("ch-0", 10, 1)
		m.addDataView(100, cdv)

		err := m.CompleteCompactionHandoff(context.Background(), 100, []int64{1}, []int64{999}, "ch-0", 10)
		assert.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// Import tests
// ---------------------------------------------------------------------------

func newImportSegmentInfo(id, collectionID, partitionID int64, channel string) *SegmentInfo {
	return &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            id,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Importing,
			IsImporting:   true,
			Level:         datapb.SegmentLevel_L1,
		},
	}
}

func TestSegmentViewMeta_RegisterSegments(t *testing.T) {
	t.Run("batch creates import segments", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		segments := []*SegmentInfo{
			newImportSegmentInfo(1, 100, 10, "ch-0"),
			newImportSegmentInfo(2, 100, 10, "ch-0"),
			newImportSegmentInfo(3, 100, 20, "ch-1"),
		}

		err := m.RegisterSegments(context.Background(), 100, segments)
		assert.NoError(t, err)

		// All segments should be in memory
		assert.NotNil(t, m.GetSegment(1))
		assert.NotNil(t, m.GetSegment(2))
		assert.NotNil(t, m.GetSegment(3))
		assert.True(t, m.GetSegment(1).GetIsImporting())

		// No DataView should be created
		assert.Nil(t, m.GetCurrentVersion(100))
	})

	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.RegisterSegments(context.Background(), 100, []*SegmentInfo{
			newImportSegmentInfo(1, 100, 10, "ch-0"),
		})
		assert.Error(t, err)
		assert.Nil(t, m.GetSegment(1))
	})
}

func TestSegmentViewMeta_ActivateSegments(t *testing.T) {
	t.Run("clears IsImporting and creates DataView", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Pre-populate import segments (state=Flushed, IsImporting=true)
		seg1 := newFlushedSegmentInfo(1, 100, 10, "ch-0")
		seg1.IsImporting = true
		seg2 := newFlushedSegmentInfo(2, 100, 10, "ch-0")
		seg2.IsImporting = true
		seg3 := newFlushedSegmentInfo(3, 100, 20, "ch-1")
		seg3.IsImporting = true
		m.setSegment(1, seg1)
		m.setSegment(2, seg2)
		m.setSegment(3, seg3)

		err := m.ActivateSegments(context.Background(), 100, []int64{1, 2, 3})
		assert.NoError(t, err)

		// IsImporting should be cleared
		assert.False(t, m.GetSegment(1).GetIsImporting())
		assert.False(t, m.GetSegment(2).GetIsImporting())
		assert.False(t, m.GetSegment(3).GetIsImporting())

		// DataView should be created
		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(1), cv.GetStreamingVersion())
		assert.Equal(t, int64(0), cv.GetCompactVersion())

	})

	t.Run("with existing DataView increments version", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		m := newSegmentViewMeta(context.Background(), catalog)

		// Pre-populate with existing DataView
		m.addDataView(100, newTestDataView(2, 1, 10, 20))

		seg := newFlushedSegmentInfo(30, 100, 10, "ch-0")
		seg.IsImporting = true
		m.setSegment(30, seg)

		err := m.ActivateSegments(context.Background(), 100, []int64{30})
		assert.NoError(t, err)

		cv := m.GetCurrentVersion(100)
		require.NotNil(t, cv)
		assert.Equal(t, int64(3), cv.GetStreamingVersion())
		assert.Equal(t, int64(0), cv.GetCompactVersion()) // compact_version resets to 0 when streaming_version increments
	})

	t.Run("no segments to complete", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		m := newSegmentViewMeta(context.Background(), catalog)

		err := m.ActivateSegments(context.Background(), 100, []int64{999})
		assert.NoError(t, err)
	})

	t.Run("catalog error", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().AlterSegmentsAndSaveDataView(
			mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		).Return(fmt.Errorf("catalog error"))
		m := newSegmentViewMeta(context.Background(), catalog)

		seg := newFlushedSegmentInfo(1, 100, 10, "ch-0")
		seg.IsImporting = true
		m.setSegment(1, seg)

		err := m.ActivateSegments(context.Background(), 100, []int64{1})
		assert.Error(t, err)

		// IsImporting should NOT be cleared on failure
		assert.True(t, m.GetSegment(1).GetIsImporting())
	})
}
