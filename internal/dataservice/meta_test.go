package dataservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	testSchema := newTestSchema()
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:         id,
		Schema:     testSchema,
		Partitions: []UniqueID{100},
	})
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:     id,
		Schema: testSchema,
	})
	assert.NotNil(t, err)
	has := meta.HasCollection(id)
	assert.True(t, has)
	collection, err := meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, id, collection.ID)
	assert.EqualValues(t, testSchema, collection.Schema)
	assert.EqualValues(t, 1, len(collection.Partitions))
	assert.EqualValues(t, 100, collection.Partitions[0])
	err = meta.DropCollection(id)
	assert.Nil(t, err)
	has = meta.HasCollection(id)
	assert.False(t, has)
	_, err = meta.GetCollection(id)
	assert.NotNil(t, err)
}

func TestSegment(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segmentInfo, err := BuildSegment(id, 100, segID, []string{"c1", "c2"})
	assert.Nil(t, err)
	err = meta.AddSegment(segmentInfo)
	assert.Nil(t, err)
	info, err := meta.GetSegment(segmentInfo.SegmentID)
	assert.Nil(t, err)
	assert.EqualValues(t, segmentInfo, info)
	ids := meta.GetSegmentsOfCollection(id)
	assert.EqualValues(t, 1, len(ids))
	assert.EqualValues(t, segmentInfo.SegmentID, ids[0])
	ids = meta.GetSegmentsOfPartition(id, 100)
	assert.EqualValues(t, 1, len(ids))
	assert.EqualValues(t, segmentInfo.SegmentID, ids[0])
	err = meta.SealSegment(segmentInfo.SegmentID, 100)
	assert.Nil(t, err)
	err = meta.FlushSegment(segmentInfo.SegmentID, 200)
	assert.Nil(t, err)
	info, err = meta.GetSegment(segmentInfo.SegmentID)
	assert.Nil(t, err)
	assert.NotZero(t, info.SealedTime)
	assert.NotZero(t, info.FlushedTime)
}

func TestPartition(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	testSchema := newTestSchema()
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)

	err = meta.AddPartition(id, 10)
	assert.NotNil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:         id,
		Schema:     testSchema,
		Partitions: []UniqueID{},
	})
	assert.Nil(t, err)
	err = meta.AddPartition(id, 10)
	assert.Nil(t, err)
	err = meta.AddPartition(id, 10)
	assert.NotNil(t, err)
	collection, err := meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 10, collection.Partitions[0])
	err = meta.DropPartition(id, 10)
	assert.Nil(t, err)
	collection, err = meta.GetCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, len(collection.Partitions))
	err = meta.DropPartition(id, 10)
	assert.NotNil(t, err)
}

func TestGetCount(t *testing.T) {
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	nums, err := meta.GetNumRowsOfCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, nums)
	segment, err := BuildSegment(id, 100, segID, []string{"c1"})
	assert.Nil(t, err)
	segment.NumRows = 100
	err = meta.AddSegment(segment)
	assert.Nil(t, err)
	segID, err = mockAllocator.allocID()
	assert.Nil(t, err)
	segment, err = BuildSegment(id, 100, segID, []string{"c1"})
	assert.Nil(t, err)
	segment.NumRows = 300
	err = meta.AddSegment(segment)
	assert.Nil(t, err)
	nums, err = meta.GetNumRowsOfCollection(id)
	assert.Nil(t, err)
	assert.EqualValues(t, 400, nums)
}
