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

func TestStore_GetSegmentsByState(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Growing})

	result := s.GetSegmentsByState(commonpb.SegmentState_Flushed)
	assert.Len(t, result, 1)
	assert.Contains(t, result, int64(1))
}

func TestStore_GetSegmentsByCollectionAndState(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Flushed})
	s.PutSegment(&datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Growing})
	s.PutSegment(&datapb.SegmentInfo{ID: 3, CollectionID: 200, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed})

	result := s.GetSegmentsByCollectionAndState(100, commonpb.SegmentState_Flushed)
	assert.Len(t, result, 1)
	assert.Contains(t, result, int64(1))
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

func TestStore_UpdateSegmentState(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch-0", State: commonpb.SegmentState_Growing})

	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Growing), 1)

	s.UpdateSegmentState(1, commonpb.SegmentState_Flushed)

	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Growing), 0)
	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Flushed), 1)
	seg, _ := s.GetSegment(1)
	assert.Equal(t, commonpb.SegmentState_Flushed, seg.GetState())
}

func TestStore_UpdateSegmentState_NonExistent(t *testing.T) {
	s := NewMetaStore(nil)
	s.UpdateSegmentState(999, commonpb.SegmentState_Flushed) // should not panic
}

func TestStore_PutSegmentUpdatesIndexes(t *testing.T) {
	s := NewMetaStore(nil)
	seg := &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Growing,
	}
	s.PutSegment(seg)

	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Growing), 1)
	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Flushed), 0)

	updated := &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch-0",
		State: commonpb.SegmentState_Flushed,
	}
	s.PutSegment(updated)

	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Growing), 0)
	assert.Len(t, s.GetSegmentsByState(commonpb.SegmentState_Flushed), 1)
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

func TestStore_GetCollectionsByDatabase(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutCollection(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "a"}})
	s.PutCollection(&CollectionInfo{ID: 200, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "b"}})
	s.PutCollection(&CollectionInfo{ID: 300, DatabaseID: 2, Schema: &schemapb.CollectionSchema{Name: "c"}})

	result := s.GetCollectionsByDatabase(1)
	assert.Len(t, result, 2)
	assert.Contains(t, result, int64(100))
	assert.Contains(t, result, int64(200))
}

func TestStore_GetAllCollections(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutCollection(&CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "a"}})
	s.PutCollection(&CollectionInfo{ID: 200, Schema: &schemapb.CollectionSchema{Name: "b"}})

	result := s.GetAllCollections()
	assert.Len(t, result, 2)
}

func TestStore_PutCollectionOverwrites(t *testing.T) {
	s := NewMetaStore(nil)
	s.PutCollection(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "v1"}})
	s.PutCollection(&CollectionInfo{ID: 100, DatabaseID: 1, Schema: &schemapb.CollectionSchema{Name: "v2"}})

	got, ok := s.GetCollection(100)
	assert.True(t, ok)
	assert.Equal(t, "v2", got.Schema.GetName())

	byDB := s.GetCollectionsByDatabase(1)
	assert.Len(t, byDB, 1)
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
