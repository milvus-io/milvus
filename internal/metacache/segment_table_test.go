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

package metacache

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func liveRows(segments map[int64]*datapb.SegmentInfo) int64 {
	var rows int64
	for _, segment := range segments {
		if segment.GetState() != commonpb.SegmentState_Dropped {
			rows += segment.GetNumOfRows()
		}
	}
	return rows
}

// TestSegmentTable_ApplyIsAtomicForReadsOfSeveralSegments compacts the live
// segments of a collection into a new one over and over while readers check
// that the live rows never change: a read that saw the outputs without the
// retired inputs, or the inputs gone before the outputs, would count twice or
// miss them.
func TestSegmentTable_ApplyIsAtomicForReadsOfSeveralSegments(t *testing.T) {
	const (
		collectionID = int64(100)
		channel      = "ch-0"
		rows         = int64(10)
		inputs       = 8
		foreignID    = int64(1 << 40)
	)
	s := NewMetaStore(nil)
	for i := int64(1); i <= inputs; i++ {
		s.PutSegment(&datapb.SegmentInfo{ID: i, CollectionID: collectionID, InsertChannel: channel, State: commonpb.SegmentState_Flushed, NumOfRows: rows})
	}
	// An unrelated collection written meanwhile must not disturb the check.
	s.PutSegment(&datapb.SegmentInfo{ID: foreignID, CollectionID: 200, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed, NumOfRows: 1})
	total := rows * inputs

	var stop atomic.Bool
	var wg sync.WaitGroup
	reader := func(name string, read func() map[int64]*datapb.SegmentInfo) {
		defer wg.Done()
		for !stop.Load() {
			segments := read()
			if got := liveRows(segments); got != total {
				stop.Store(true)
				assert.Equal(t, total, got, "%s read %v", name, segments)
				return
			}
		}
	}
	wg.Add(4)
	go reader("GetSegments", func() map[int64]*datapb.SegmentInfo { return s.GetSegments(collectionID) })
	go reader("GetSegmentsByChannel", func() map[int64]*datapb.SegmentInfo { return s.GetSegmentsByChannel(channel) })
	go reader("GetAllSegments", func() map[int64]*datapb.SegmentInfo {
		all := s.GetAllSegments()
		delete(all, foreignID)
		return all
	})
	go reader("GetSegmentsByIDs", func() map[int64]*datapb.SegmentInfo {
		// Ask for segments the writer has not created yet too: they must
		// appear together with the retirement of their inputs.
		ids := make([]int64, 0, 4096)
		for id := int64(1); id < 4096; id++ {
			ids = append(ids, id)
		}
		segments := s.GetSegmentsByIDs(ids)
		delete(segments, foreignID)
		return segments
	})

	nextID := int64(inputs + 1)
	for round := 0; round < 2000 && !stop.Load(); round++ {
		live := make([]int64, 0)
		for id, segment := range s.GetSegments(collectionID) {
			if segment.GetState() != commonpb.SegmentState_Dropped {
				live = append(live, id)
			}
		}
		puts := []*datapb.SegmentInfo{{ID: nextID, CollectionID: collectionID, InsertChannel: channel, State: commonpb.SegmentState_Flushed, NumOfRows: total, CompactionFrom: live}}
		var removes []int64
		for _, id := range live {
			retired, _ := s.GetSegment(id)
			retired = &datapb.SegmentInfo{ID: id, CollectionID: collectionID, InsertChannel: channel, State: commonpb.SegmentState_Dropped, NumOfRows: retired.GetNumOfRows()}
			puts = append(puts, retired)
		}
		// Remove the segments retired two rounds ago, as GC would.
		for id, segment := range s.GetSegments(collectionID) {
			if segment.GetState() == commonpb.SegmentState_Dropped && id < nextID-1 {
				removes = append(removes, id)
			}
		}
		s.ApplySegments(puts, removes, nil)
		nextID++
	}
	stop.Store(true)
	wg.Wait()
	assert.Equal(t, total, liveRows(s.GetSegments(collectionID)))
}

// TestSegmentTable_HookAndViewSeeTheWriteTogether pins that a ViewCollection
// reader observes the state a hook installs together with the segments it
// accompanies, the way DataCoord keeps compaction relations.
func TestSegmentTable_HookAndViewSeeTheWriteTogether(t *testing.T) {
	const collectionID = int64(100)
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: collectionID, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})

	var relation atomic.Int64 // the output the input 1 was compacted into
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			s.ViewCollection(collectionID, func() {
				input, ok := s.GetSegment(1)
				if !assert.True(t, ok) {
					return
				}
				to := relation.Load()
				if input.GetState() == commonpb.SegmentState_Dropped {
					assert.NotZero(t, to, "a retired input must come with its relation")
					_, ok := s.GetSegment(to)
					assert.True(t, ok, "the relation must name a published output")
				} else {
					assert.Zero(t, to, "a live input must not have a relation yet")
				}
			})
		}
	}()
	for i := int64(2); i < 2000; i++ {
		s.ApplySegments([]*datapb.SegmentInfo{
			{ID: i, CollectionID: collectionID, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed, CompactionFrom: []int64{1}},
			{ID: 1, CollectionID: collectionID, InsertChannel: "ch-0", State: commonpb.SegmentState_Dropped},
		}, nil, func() { relation.Store(i) })
		// Revive the input and drop the relation, again as one write.
		s.ApplySegments([]*datapb.SegmentInfo{
			{ID: 1, CollectionID: collectionID, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed},
		}, []int64{i}, func() { relation.Store(0) })
	}
	stop.Store(true)
	wg.Wait()
}

// TestSegmentTable_RetiredShardIsNotWrittenTo removes and re-adds the only
// segment of a collection from many writers at once: a writer that loaded a
// shard just retired must move to the new one, so no segment is lost.
func TestSegmentTable_RetiredShardIsNotWrittenTo(t *testing.T) {
	const collectionID = int64(100)
	s := NewMetaStore(nil)
	var wg sync.WaitGroup
	for w := int64(0); w < 8; w++ {
		wg.Add(1)
		go func(w int64) {
			defer wg.Done()
			for i := int64(0); i < 500; i++ {
				id := w*1000 + i
				s.PutSegment(&datapb.SegmentInfo{ID: id, CollectionID: collectionID, InsertChannel: "ch-0"})
				_, ok := s.GetSegments(collectionID)[id]
				assert.True(t, ok)
				s.RemoveSegment(id)
			}
		}(w)
	}
	wg.Wait()
	assert.Empty(t, s.GetSegments(collectionID))
	for w := int64(0); w < 8; w++ {
		s.PutSegment(&datapb.SegmentInfo{ID: w, CollectionID: collectionID, InsertChannel: "ch-0"})
	}
	assert.Len(t, s.GetSegments(collectionID), 8)
	assert.Len(t, s.GetSegmentsByChannel("ch-0"), 8)
}

// TestSegmentTable_ChannelSharedByCollections pins master's channel index: a
// channel name reused by two collections returns the segments of both, and
// one collection leaving the channel does not hide the other's.
func TestSegmentTable_ChannelSharedByCollections(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0"})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 200, InsertChannel: "ch-0"})
	assert.Len(t, s.GetSegmentsByChannel("ch-0"), 2)

	s.RemoveSegment(1)
	segments := s.GetSegmentsByChannel("ch-0")
	assert.Len(t, segments, 1)
	assert.Contains(t, segments, int64(2))

	s.PutSegment(&datapb.SegmentInfo{ID: 3, CollectionID: 200, InsertChannel: "ch-0"})
	assert.Len(t, s.GetSegmentsByChannel("ch-0"), 2)
	s.RemoveSegment(2)
	s.RemoveSegment(3)
	assert.Empty(t, s.GetSegmentsByChannel("ch-0"))
}

// TestSegmentTable_ByIDsReturnsASegmentWhoseCollectionHasNoShard pins that a
// read by IDs terminates when a segment names a collection that has no shard,
// as a proto changed in place after it was stored does.
func TestSegmentTable_ByIDsReturnsASegmentWhoseCollectionHasNoShard(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0"})
	segment, _ := s.GetSegment(1)
	segment.CollectionID = 200

	segments := s.GetSegmentsByIDs([]int64{1})
	assert.Len(t, segments, 1)
}
