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

package meta

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSegmentsInfo_SelectionReturnsDistinctSegments(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	for id := int64(1); id <= 3; id++ {
		s.SetSegment(id, NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 1, State: commonpb.SegmentState_Growing}))
	}
	s.SetIsCompacting(2, true)

	selected := s.GetSegmentsBySelector(WithCollection(1), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return !segment.IsCompacting
	}))
	require.Len(t, selected, 2)
	assert.NotSame(t, selected[0], selected[1])
	ids := []int64{selected[0].GetID(), selected[1].GetID()}
	assert.ElementsMatch(t, []int64{1, 3}, ids)
	for _, segment := range selected {
		assert.False(t, segment.IsCompacting)
		assert.False(t, segment.LastFlushTime.IsZero(), "a selected segment carries its state")
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
			}))
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
		segment := NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 1, State: commonpb.SegmentState_Growing})
		segment.CollectionID = 1 + id%2
		s.SetSegment(id, segment)
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
		assert.Equal(t, segment.GetID() == 3, segment.IsCompacting, "a kept segment carries its own state")
	}

	visited := 0
	s.RangeReused(func(*SegmentInfo) bool {
		visited++
		return false
	})
	assert.Equal(t, 1, visited, "returning false stops the range")
}

func TestSegmentsInfo_RangeReusedClearsTransientState(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	s := NewSegmentsInfo(store)
	s.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 1, InsertChannel: "a", State: commonpb.SegmentState_Growing}))
	s.SetIsCompacting(1, true)
	store.LoadSegments([]*datapb.SegmentInfo{{ID: 2, CollectionID: 1, InsertChannel: "b"}, {ID: 3, CollectionID: 2, InsertChannel: "a"}})
	for range 20 {
		s.RangeReused(func(segment *SegmentInfo) bool {
			assert.Equal(t, segment.GetID() == 1, segment.IsCompacting)
			assert.Equal(t, segment.GetID() != 1, segment.LastFlushTime.IsZero())
			return true
		})
	}
	selected := s.GetSegmentsBySelector(WithCollection(1), WithChannel("a"))
	require.Len(t, selected, 1)
	assert.Equal(t, int64(1), selected[0].GetID())
}

func TestSegmentsInfo_RangeReusedPreservesCollectionPublication(t *testing.T) {
	s := NewSegmentsInfo(metacache.NewMetaStore(nil))
	publish := func(generation int32) {
		s.PublishSegments([]*SegmentInfo{
			NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 1, SchemaVersion: generation}),
			NewSegmentInfo(&datapb.SegmentInfo{ID: 2, CollectionID: 1, SchemaVersion: generation}),
		})
	}
	publish(0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for generation := int32(1); generation <= 500; generation++ {
			publish(generation)
		}
	}()
	for range 500 {
		var versions []int32
		s.RangeReused(func(segment *SegmentInfo) bool {
			versions = append(versions, segment.GetSchemaVersion())
			return true
		}, WithCollection(1))
		assert.Len(t, versions, 2)
		assert.Equal(t, versions[0], versions[1])
	}
	<-done
}
