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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	s.PutSegment(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Flushed,
	})

	assert.Len(t, s.GetSegments(100), 1)
	assert.Len(t, s.GetSegmentsByChannel("ch-0"), 1)

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
		Properties:    map[string]string{"ttl": "3600"},
	}
	s.PutCollection(info)

	got, ok := s.GetCollection(100)
	assert.True(t, ok)
	assert.Equal(t, int64(100), got.ID)
	assert.Equal(t, "test_coll", got.Schema.GetName())
	assert.Equal(t, []int64{10, 20}, got.Partitions)
	assert.Equal(t, []string{"ch-0", "ch-1"}, got.VChannelNames)
}

func TestStore_RemoveCollection(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutCollection(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "c"}})
	s.RemoveCollection(100)

	_, ok := s.GetCollection(100)
	assert.False(t, ok)
}

func TestStore_PutCollectionOverwrites(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutCollection(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "v1"}})
	s.PutCollection(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "v2"}})

	got, ok := s.GetCollection(100)
	assert.True(t, ok)
	assert.Equal(t, "v2", got.Schema.GetName())
	assert.Len(t, s.GetCollectionIDs(), 1)
}

func TestStore_GetCollections(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutCollection(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "a"}})
	s.PutCollection(&CollectionInfo{ID: 200, Schema: &schemapb.CollectionSchema{Name: "b"}})

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
	s.PutCollection(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "a"}})
	s.PutCollection(&CollectionInfo{ID: 200, Schema: &schemapb.CollectionSchema{Name: "b"}})

	ids := s.GetCollectionIDs()
	assert.ElementsMatch(t, []int64{100, 200}, ids)
}

func TestStore_GetCollectionIDs_Empty(t *testing.T) {
	s := NewMetaStore(nil)
	ids := s.GetCollectionIDs()
	assert.Len(t, ids, 0)
}
