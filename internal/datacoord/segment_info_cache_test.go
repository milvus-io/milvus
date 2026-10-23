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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSegmentsInfo_TombstoneBlocksStaleWrite(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1}}

	cache.SetSegment(1, seg, 10)
	cache.DropSegment(1, 20)

	cache.SetSegment(1, seg, 15)
	assert.Nil(t, cache.GetSegment(1), "stale write below tombstone version must be rejected")
}

func TestSegmentsInfo_NewerDeleteAdvancesTombstoneVersion(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 7}}

	cache.SetSegment(7, seg, 5)
	cache.DropSegment(7, 10)
	cache.DropSegment(7, 20)
	cache.SetSegment(7, seg, 15)

	assert.Nil(t, cache.GetSegment(7), "a newer delete must advance the tombstone version")
}

func TestSegmentsInfo_PruneRemovesTombstone(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2}}

	cache.SetSegment(2, seg, 10)
	cache.DropSegment(2, 20)
	cache.store.PruneSegment(2)

	cache.SetSegment(2, seg, 1)
	assert.NotNil(t, cache.GetSegment(2), "insert after prune must succeed, tombstone should be cleared")
}

func TestSegmentsInfo_LocalSeedOverwritesVersionZero(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))

	cache.SetSegment(3, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 10}}, 0)
	cache.SetSegment(3, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 20}}, 0)

	seg := cache.GetSegment(3)
	assert.NotNil(t, seg)
	assert.Equal(t, int64(20), seg.GetNumOfRows())
}

func TestSegmentsInfo_DropRejectsStaleVersion(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))
	seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4, CollectionID: 5, InsertChannel: "ch"}}

	cache.SetSegment(4, seg, 20)
	cache.DropSegment(4, 10)

	assert.NotNil(t, cache.GetSegment(4), "drop below current version must be rejected")
	assert.Len(t, cache.GetSegmentsBySelector(WithCollection(5), WithChannel("ch")), 1)
}

func TestSegmentsInfo_SetSegmentRejectsStaleVersion(t *testing.T) {
	cache := NewSegmentsInfo(metacache.NewMetaStore(nil))

	cache.SetSegment(5, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 5, CollectionID: 10, InsertChannel: "ch-1", NumOfRows: 100,
	}}, 20)
	cache.SetSegment(5, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 5, CollectionID: 11, InsertChannel: "ch-2", NumOfRows: 200,
	}}, 10)

	seg := cache.GetSegment(5)
	assert.Equal(t, int64(10), seg.GetCollectionID())
	assert.Equal(t, int64(100), seg.GetNumOfRows())
	assert.Len(t, cache.GetSegmentsBySelector(WithCollection(10), WithChannel("ch-1")), 1)
	assert.Empty(t, cache.GetSegmentsBySelector(WithCollection(11), WithChannel("ch-2")))
}

func newGrowingTestSegment(id int64) *SegmentInfo {
	return NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 1, InsertChannel: "ch", State: commonpb.SegmentState_Growing})
}

func TestSegmentsInfo_PublishKeepsTheDataCoordState(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.SetSegment(1, newGrowingTestSegment(1), 1)
	s.AddAllocation(1, &Allocation{SegmentID: 1, NumOfRows: 5, ExpireTime: 100})
	s.SetIsCompacting(1, true)

	// A writer publishes a value it cloned before the state changed.
	published := NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 1, InsertChannel: "ch", State: commonpb.SegmentState_Growing, NumOfRows: 7})
	old := s.PublishSegments([]*SegmentInfo{published}, map[int64]int64{1: 2})[1]
	require.NotNil(t, old)
	assert.Len(t, old.allocations, 1)

	got := s.GetSegment(1)
	assert.EqualValues(t, 7, got.GetNumOfRows())
	assert.Len(t, got.allocations, 1, "the allocations survive the publication")
	assert.True(t, got.isCompacting, "the compacting flag survives the publication")
	assert.EqualValues(t, 100, got.GetLastExpireTime(), "LastExpireTime does not move back")
}

func TestSegmentsInfo_NewSegmentBringsItsState(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.PublishSegments([]*SegmentInfo{newGrowingTestSegment(1)}, map[int64]int64{1: 1})

	got := s.GetSegment(1)
	require.NotNil(t, got)
	assert.False(t, got.lastFlushTime.IsZero(), "a new growing segment brings the state NewSegmentInfo gave it")
	assert.NotNil(t, got.allocations)
}

func TestSegmentsInfo_ZeroStateIsNotStored(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed}), 1)
	_, stored := s.dcState.Get(1)
	assert.False(t, stored, "a flushed segment has the zero state")

	s.SetIsCompacting(1, true)
	_, stored = s.dcState.Get(1)
	assert.True(t, stored)
	assert.True(t, s.GetSegment(1).isCompacting)

	s.SetIsCompacting(1, false)
	_, stored = s.dcState.Get(1)
	assert.False(t, stored, "a state back to zero is removed")
}

func TestSegmentsInfo_DropRemovesTheState(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.SetSegment(1, newGrowingTestSegment(1), 1)
	s.DropSegment(1, 2)
	_, stored := s.dcState.Get(1)
	assert.False(t, stored)

	s.SetIsCompacting(1, true)
	s.AddAllocation(1, &Allocation{SegmentID: 1})
	_, stored = s.dcState.Get(1)
	assert.False(t, stored, "a removed segment gets no state")
	assert.Nil(t, s.GetSegment(1))
}

func TestSegmentsInfo_ConcurrentAllocationsAreAllKept(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	s.SetSegment(1, newGrowingTestSegment(1), 1)
	const writers = 64
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.AddAllocation(1, &Allocation{SegmentID: 1, NumOfRows: int64(i)})
			_ = s.GetSegment(1).allocations
		}(i)
	}
	wg.Wait()
	assert.Len(t, s.GetSegment(1).allocations, writers)
}

func TestSegmentsInfo_SettersRacingADropLeaveNoState(t *testing.T) {
	for round := 0; round < 50; round++ {
		s := NewSegmentsInfo(metacache.NewMetaStore(nil))
		s.SetSegment(1, newGrowingTestSegment(1), 1)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.SetIsCompacting(1, true)
				s.SetLastWrittenTime(1)
				s.AddAllocation(1, &Allocation{SegmentID: 1})
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.DropSegment(1, 2)
		}()
		wg.Wait()
		_, stored := s.dcState.Get(1)
		require.False(t, stored, "round %d: a dropped segment kept a state", round)
	}
}

func TestSegmentsInfo_SharesTheSegmentsWithTheMetaView(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	s := NewSegmentsInfo(store)
	segment := newGrowingTestSegment(1)
	s.PublishSegments([]*SegmentInfo{segment}, map[int64]int64{1: 1})

	var view metacache.MetaView = store
	got, ok := view.GetSegment(1)
	require.True(t, ok)
	assert.Same(t, segment.SegmentInfo, got, "QueryCoord reads the proto DataCoord published")
	assert.Same(t, got, s.GetSegment(1).SegmentInfo)
	assert.Len(t, view.GetSegments(1), 1)
	assert.Len(t, view.GetSegmentsByChannel("ch"), 1)

	s.DropSegment(1, 2)
	_, ok = view.GetSegment(1)
	assert.False(t, ok)
}

func TestSegmentsInfo_SelectionReturnsDistinctSegments(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for id := int64(1); id <= 3; id++ {
		s.SetSegment(id, newGrowingTestSegment(id), 1)
	}
	s.SetIsCompacting(2, true)

	selected := s.GetSegmentsBySelector(WithCollection(1), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return !segment.isCompacting
	}))
	require.Len(t, selected, 2)
	assert.NotSame(t, selected[0], selected[1])
	ids := []int64{selected[0].GetID(), selected[1].GetID()}
	assert.ElementsMatch(t, []int64{1, 3}, ids)
	for _, segment := range selected {
		assert.False(t, segment.isCompacting)
		assert.False(t, segment.lastFlushTime.IsZero(), "a selected segment carries its state")
	}
}

// BenchmarkSegmentsInfoSelect measures the reads that visit many segments,
// with 1M segments in 10K collections.
func BenchmarkSegmentsInfoSelect(b *testing.B) {
	const collections, perCollection = 10_000, 100
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for c := int64(1); c <= collections; c++ {
		for i := int64(0); i < perCollection; i++ {
			id := c*perCollection + i
			s.SetSegment(id, NewSegmentInfo(&datapb.SegmentInfo{
				ID: id, CollectionID: c, InsertChannel: fmt.Sprint("ch-", c), State: commonpb.SegmentState_Flushed, NumOfRows: 1,
			}), 1)
		}
	}
	growing := SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing
	})
	b.Run("filter_all", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s.GetSegmentsBySelector(growing)
		}
	})
	b.Run("one_collection", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s.GetSegmentsBySelector(WithCollection(42))
		}
	})
	b.Run("get_all", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s.GetSegments()
		}
	})
	b.Run("range_reused", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			dropped := 0
			s.RangeReused(func(segment *SegmentInfo) bool {
				if segment.GetState() == commonpb.SegmentState_Dropped {
					dropped++
				}
				return true
			})
		}
	})
}

func TestSegmentsInfo_RangeReusedVisitsTheSelectedSegments(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for id := int64(1); id <= 4; id++ {
		segment := newGrowingTestSegment(id)
		segment.CollectionID = 1 + id%2
		s.SetSegment(id, segment, 1)
	}
	s.SetIsCompacting(3, true)

	var kept []*SegmentInfo
	s.RangeReused(func(segment *SegmentInfo) bool {
		kept = append(kept, segment.ShadowClone())
		return true
	}, WithCollection(2))
	require.Len(t, kept, 2)
	assert.ElementsMatch(t, []int64{1, 3}, []int64{kept[0].GetID(), kept[1].GetID()})
	for _, segment := range kept {
		assert.Equal(t, segment.GetID() == 3, segment.isCompacting, "a kept segment carries its own state")
	}

	visited := 0
	s.RangeReused(func(*SegmentInfo) bool {
		visited++
		return false
	})
	assert.Equal(t, 1, visited, "returning false stops the range")
}
