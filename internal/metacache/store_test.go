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
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStore_PutSegmentAndGet(t *testing.T) {
	s := NewMetaStore(nil)
	seg := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  100,
		PartitionID:   200,
		InsertChannel: "ch-0",
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     1000,
	}
	s.PutSegment(seg)

	got, ok := s.GetSegment(1)
	assert.True(t, ok)
	assert.Equal(t, int64(1), got.GetID())
	assert.Equal(t, int64(100), got.GetCollectionID())
}

func TestStore_RemoveSegment(t *testing.T) {
	s := NewMetaStore(nil)
	seg := &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Flushed,
	}
	s.PutSegment(seg)
	s.RemoveSegment(1)

	_, ok := s.GetSegment(1)
	assert.False(t, ok)
}

func TestStore_GetSegments(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 3, CollectionID: 200, InsertChannel: "ch-2", State: commonpb.SegmentState_Flushed})

	result := s.GetSegments(100)
	assert.Len(t, result, 2)
	assert.Contains(t, result, int64(1))
	assert.Contains(t, result, int64(2))
}

func TestStore_GetSegmentsByChannel(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})

	result := s.GetSegmentsByChannel("ch-0")
	assert.Len(t, result, 2)
}

func TestStore_GetSegmentsByIDs(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})

	result := s.GetSegmentsByIDs([]int64{1, 2, 999})
	assert.Len(t, result, 2)
}

func TestStore_GetAllSegments(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 200, InsertChannel: "ch-1", State: commonpb.SegmentState_Growing})

	result := s.GetAllSegments()
	assert.Len(t, result, 2)
}

func TestStore_LoadSegments(t *testing.T) {
	s := NewMetaStore(nil)
	segments := []*datapb.SegmentInfo{
		{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed, NumOfRows: 500},
		{ID: 2, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed, NumOfRows: 300},
	}
	s.LoadSegments(segments)

	got, ok := s.GetSegment(1)
	assert.True(t, ok)
	assert.Equal(t, int64(500), got.GetNumOfRows())

	result := s.GetSegments(100)
	assert.Len(t, result, 2)
}

// TestStore_PutSegmentUpdatesIndexes asserts that replacing a segment leaves
// exactly one entry in the collection and channel indexes, and that the stored
// value is the new one.
func TestStore_PutSegmentUpdatesIndexes(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Growing,
	})
	// Re-put the SAME id under a different collection and channel. If the old
	// index entries were not removed, the segment would still be reachable
	// under 100/"ch-0" -- which is the only thing that distinguishes a real
	// index update from a plain overwrite.
	s.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 200, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed,
	})

	assert.Empty(t, s.GetSegments(100), "the old collection index entry must be gone")
	assert.Empty(t, s.GetSegmentsByChannel("ch-0"), "the old channel index entry must be gone")
	assert.Len(t, s.GetSegments(200), 1)
	assert.Len(t, s.GetSegmentsByChannel("ch-1"), 1)

	seg, ok := s.GetSegment(1)
	assert.True(t, ok)
	assert.Equal(t, commonpb.SegmentState_Flushed, seg.GetState())
}

func TestStore_PutCollectionAndGet(t *testing.T) {
	s := NewMetaStore(nil)
	info := &CollectionInfo{
		ID:            100,
		DatabaseName:  "default",
		DatabaseID:    1,
		Schema:        &schemapb.CollectionSchema{Name: "test_coll"},
		Partitions:    []int64{10, 20},
		VChannelNames: []string{"ch-0", "ch-1"},
		Properties:    []*commonpb.KeyValuePair{{Key: "ttl", Value: "3600"}},
	}
	collectionsOf(s).Put(info)

	got, ok := s.GetCollection(100)
	assert.True(t, ok)
	assert.Equal(t, int64(100), got.ID)
	assert.Equal(t, "test_coll", got.Schema.GetName())
	assert.Equal(t, []int64{10, 20}, got.Partitions)
	assert.Equal(t, []string{"ch-0", "ch-1"}, got.VChannelNames)
}

func TestStore_RemoveCollection(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "c"}})
	collectionsOf(s).Remove(100)

	_, ok := s.GetCollection(100)
	assert.False(t, ok)
}

func TestStore_PutCollectionOverwrites(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "v1"}})
	collectionsOf(s).Put(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "v2"}})

	got, ok := s.GetCollection(100)
	assert.True(t, ok)
	assert.Equal(t, "v2", got.Schema.GetName())
	assert.Len(t, s.GetCollectionIDs(), 1)
}

func TestStore_GetCollections(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "a"}})
	collectionsOf(s).Put(&CollectionInfo{ID: 200, Schema: &schemapb.CollectionSchema{Name: "b"}})

	result := s.GetCollections()
	assert.Len(t, result, 2)
	ids := make([]int64, 0, len(result))
	for _, info := range result {
		ids = append(ids, info.ID)
	}
	assert.ElementsMatch(t, []int64{100, 200}, ids)
}

func TestStore_GetCollections_Empty(t *testing.T) {
	s := NewMetaStore(nil)
	result := s.GetCollections()
	assert.Len(t, result, 0)
}

func TestStore_GetCollectionIDs(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "a"}})
	collectionsOf(s).Put(&CollectionInfo{ID: 200, Schema: &schemapb.CollectionSchema{Name: "b"}})

	ids := s.GetCollectionIDs()
	assert.ElementsMatch(t, []int64{100, 200}, ids)
}

func TestStore_GetCollectionIDs_Empty(t *testing.T) {
	s := NewMetaStore(nil)
	ids := s.GetCollectionIDs()
	assert.Len(t, ids, 0)
}

// TestStore_RemoveSegmentDeletesEmptyIndexBuckets pins that the segment table
// sheds a collection's shard and a channel's bucket once their last segment
// leaves, rather than accumulating empty entries. Asserting only that a lookup
// returns nothing cannot see the difference -- an empty bucket and an absent
// one both answer "no segments" -- so this reaches into the table directly.
// Without it, every dropped collection leaks its entries for the lifetime of
// the process.
func TestStore_RemoveSegmentDeletesEmptyIndexBuckets(t *testing.T) {
	s := NewMetaStore(nil).(*metaStore)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0"})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-0"})
	s.PutSegment(&datapb.SegmentInfo{ID: 3, CollectionID: 200, InsertChannel: "ch-1"})

	table := s.segments.Load()
	require.Equal(t, 2, table.shards.Len())
	require.Len(t, table.channels, 2)

	// Collection 200 / ch-1 lose their only segment: both entries must go.
	retired, _ := table.shards.Get(200)
	s.RemoveSegment(3)
	assert.False(t, table.shards.Contain(200), "an emptied collection shard must be removed, not left empty")
	assert.True(t, retired.retired, "a removed shard must be retired for writers still holding it")
	_, chanStillThere := table.channels["ch-1"]
	assert.False(t, chanStillThere, "an emptied channel bucket must be removed, not left empty")

	// Collection 100 still holds segments 1 and 2, so its entries stay.
	shard, ok := table.shards.Get(100)
	require.True(t, ok)
	assert.Len(t, shard.ids, 2)
	assert.Len(t, shard.byChannel["ch-0"], 2)

	// Dropping one of the two shrinks the bucket but must not delete it.
	s.RemoveSegment(1)
	assert.Len(t, shard.ids, 1)
	assert.Len(t, shard.byChannel["ch-0"], 1)

	// The last one takes the entries with it.
	s.RemoveSegment(2)
	assert.Equal(t, 0, table.shards.Len())
	assert.Empty(t, table.channels)

	// A later write of the collection gets a fresh shard.
	s.PutSegment(&datapb.SegmentInfo{ID: 4, CollectionID: 100, InsertChannel: "ch-0"})
	assert.Len(t, s.GetSegments(100), 1)
	assert.Len(t, s.GetSegmentsByChannel("ch-0"), 1)
}

// fullNullCountsSchema has two not-nullable fields (100, 101) and one
// nullable field (102).
func fullNullCountsSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
}

func fullNullCounts() map[int64]int64 {
	return map[int64]int64{100: 0, 101: 0, 102: 3}
}

func TestNormalizeSegment_PrunesOnPut(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: fullNullCountsSchema()})

	s.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	})
	s.ApplySegments([]*datapb.SegmentInfo{{
		ID: 2, CollectionID: 100, InsertChannel: "ch-0",
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	}}, nil, nil)
	s.LoadSegments([]*datapb.SegmentInfo{{
		ID: 3, CollectionID: 100, InsertChannel: "ch-0",
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	}})

	for _, id := range []int64{1, 2, 3} {
		got, ok := s.GetSegment(id)
		require.True(t, ok)
		assert.Equal(t, map[int64]int64{102: 3}, got.GetStats().GetNullCounts(), "segment %d", id)
	}
}

func TestNormalizeSegment_PrunableFieldsCache(t *testing.T) {
	const collID = int64(100)
	s := NewMetaStore(nil).(*metaStore)
	altered := fullNullCountsSchema()
	altered.Fields[2].Nullable = false

	steps := []struct {
		name   string
		change func()
		want   map[int64]struct{}
	}{
		{
			name:   "unknown collection",
			change: func() {},
			want:   nil,
		},
		{
			name: "built from the schema",
			change: func() {
				collectionsOf(s).Put(&CollectionInfo{ID: collID, Schema: fullNullCountsSchema()})
			},
			want: map[int64]struct{}{100: {}, 101: {}},
		},
		{
			name:   "rebuilt when the schema is replaced",
			change: func() { collectionsOf(s).Put(&CollectionInfo{ID: collID, Schema: altered}) },
			want:   map[int64]struct{}{100: {}, 101: {}, 102: {}},
		},
		{
			name:   "dropped with the collection",
			change: func() { collectionsOf(s).Remove(collID) },
			want:   nil,
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.change()
			first := s.prunableNullCountFields(collID)
			assert.Equal(t, step.want, first)
			if step.want == nil {
				assert.False(t, s.prunable.Contain(collID))
				return
			}
			// Later lookups for the same schema reuse the cached set.
			second := s.prunableNullCountFields(collID)
			assert.Equal(t, reflect.ValueOf(first).UnsafePointer(), reflect.ValueOf(second).UnsafePointer())
		})
	}
}

// removeAfterGetSource returns a collection once and removes it right after,
// as if a drop ran between the lookup and the caller's next step.
type removeAfterGetSource struct {
	*Collections
	armed bool
}

func (s *removeAfterGetSource) GetCollection(collectionID int64) (*CollectionInfo, bool) {
	info, ok := s.Collections.GetCollection(collectionID)
	if ok && s.armed {
		s.armed = false
		s.Remove(collectionID)
	}
	return info, ok
}

// TestNormalizeSegment_PrunableFieldsDropRace pins that a lookup racing with
// the collection's removal does not leave a cache entry behind.
func TestNormalizeSegment_PrunableFieldsDropRace(t *testing.T) {
	const collID = int64(100)
	s := NewMetaStore(nil).(*metaStore)
	source := &removeAfterGetSource{Collections: NewCollections()}
	s.SetCollectionSource(source)
	source.Put(&CollectionInfo{ID: collID, Schema: fullNullCountsSchema()})
	source.armed = true

	assert.Equal(t, map[int64]struct{}{100: {}, 101: {}}, s.prunableNullCountFields(collID))
	_, ok := s.GetCollection(collID)
	require.False(t, ok)
	assert.False(t, s.prunable.Contain(collID))
}

func TestNormalizeSegment_UnknownCollection(t *testing.T) {
	s := NewMetaStore(nil)

	s.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	})

	got, ok := s.GetSegment(1)
	require.True(t, ok)
	assert.Equal(t, fullNullCounts(), got.GetStats().GetNullCounts())
}

// TestNormalizeSegment_ImportingKeepsNullCounts pins that an importing
// segment keeps all its null counts, on runtime puts and on catalog load, and
// that a put after IsImporting is cleared prunes them.
func TestNormalizeSegment_ImportingKeepsNullCounts(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything, int64(100)).Return([]*datapb.SegmentInfo{{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0", IsImporting: true,
		State: commonpb.SegmentState_Flushed,
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	}}, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(map[string]*msgpb.MsgPosition{}, nil)

	s := NewMetaStore(catalog)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: fullNullCountsSchema()})
	require.NoError(t, s.LoadFromCatalog(context.Background(), []int64{100}, nil))
	s.PutSegment(&datapb.SegmentInfo{
		ID: 2, CollectionID: 100, InsertChannel: "ch-0", IsImporting: true,
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	})
	for _, id := range []int64{1, 2} {
		got, ok := s.GetSegment(id)
		require.True(t, ok)
		assert.Equal(t, fullNullCounts(), got.GetStats().GetNullCounts(), "segment %d", id)
	}

	committed, _ := s.GetSegment(2)
	committed = proto.Clone(committed).(*datapb.SegmentInfo)
	committed.IsImporting = false
	s.PutSegment(committed)
	got, ok := s.GetSegment(2)
	require.True(t, ok)
	assert.Equal(t, map[int64]int64{102: 3}, got.GetStats().GetNullCounts())
}

func TestNormalizeSegment_LoadFromCatalog(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything, int64(100)).Return([]*datapb.SegmentInfo{{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Flushed,
		Stats: &datapb.Statistics{NullCounts: fullNullCounts()},
	}}, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(map[string]*msgpb.MsgPosition{}, nil)

	s := NewMetaStore(catalog)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: fullNullCountsSchema()})
	require.NoError(t, s.LoadFromCatalog(context.Background(), []int64{100}, nil))

	got, ok := s.GetSegment(1)
	require.True(t, ok)
	assert.Equal(t, map[int64]int64{102: 3}, got.GetStats().GetNullCounts())
}

// TestNormalizeSegment_SharedStatsNotMutated pins that pruning is
// copy-on-write: DataCoord may hand two segments one *Statistics, so putting
// one of them must not write the object the other still exposes. Under -race
// an in-place delete races with the reader goroutine; without -race the final
// assertion still catches it.
func TestNormalizeSegment_SharedStatsNotMutated(t *testing.T) {
	s := NewMetaStore(nil)
	collectionsOf(s).Put(&CollectionInfo{ID: 100, Schema: fullNullCountsSchema()})

	shared := &datapb.Statistics{NullCounts: fullNullCounts()}
	second := &datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-0", Stats: shared}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			sum := int64(0)
			for _, v := range second.GetStats().GetNullCounts() {
				sum += v
			}
			_ = sum
		}
	}()
	for i := 0; i < 100; i++ {
		// Each put hands over a fresh top-level SegmentInfo, as the
		// contract requires; only the nested Statistics is shared.
		s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", Stats: shared})
	}
	close(stop)
	<-done

	assert.Same(t, shared, second.GetStats())
	assert.Equal(t, fullNullCounts(), shared.GetNullCounts(), "the shared object keeps all its keys")
	got, ok := s.GetSegment(1)
	require.True(t, ok)
	assert.NotSame(t, shared, got.GetStats())
	assert.Equal(t, map[int64]int64{102: 3}, got.GetStats().GetNullCounts())
}
