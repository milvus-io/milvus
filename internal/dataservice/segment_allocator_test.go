package dataservice

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAllocSegment(t *testing.T) {
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segAllocator, err := newSegmentAllocator(meta, mockAllocator)
	assert.Nil(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:     collID,
		Schema: schema,
	})
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segmentInfo, err := BuildSegment(collID, 100, id, []string{"c1", "c2"})
	assert.Nil(t, err)
	err = meta.AddSegment(segmentInfo)
	assert.Nil(t, err)
	err = segAllocator.OpenSegment(segmentInfo)
	assert.Nil(t, err)

	cases := []struct {
		collectionID UniqueID
		partitionID  UniqueID
		channelName  string
		requestRows  int
		expectResult bool
	}{
		{collID, 100, "c1", 100, true},
		{collID + 1, 100, "c1", 100, false},
		{collID, 101, "c1", 100, false},
		{collID, 100, "c3", 100, false},
		{collID, 100, "c1", math.MaxInt64, false},
	}
	for _, c := range cases {
		id, count, expireTime, err := segAllocator.AllocSegment(c.collectionID, c.partitionID, c.channelName, c.requestRows)
		if c.expectResult {
			assert.Nil(t, err)
			assert.EqualValues(t, c.requestRows, count)
			assert.NotEqualValues(t, 0, id)
			assert.NotEqualValues(t, 0, expireTime)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func TestSealSegment(t *testing.T) {
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segAllocator, err := newSegmentAllocator(meta, mockAllocator)
	assert.Nil(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:     collID,
		Schema: schema,
	})
	assert.Nil(t, err)
	var lastSegID UniqueID
	for i := 0; i < 10; i++ {
		id, err := mockAllocator.allocID()
		assert.Nil(t, err)
		segmentInfo, err := BuildSegment(collID, 100, id, []string{"c" + strconv.Itoa(i)})
		assert.Nil(t, err)
		err = meta.AddSegment(segmentInfo)
		assert.Nil(t, err)
		err = segAllocator.OpenSegment(segmentInfo)
		assert.Nil(t, err)
		lastSegID = segmentInfo.SegmentID
	}

	err = segAllocator.SealSegment(lastSegID)
	assert.Nil(t, err)
	segAllocator.SealAllSegments(collID)
	sealedSegments, err := segAllocator.GetSealedSegments()
	assert.Nil(t, err)
	assert.EqualValues(t, 10, len(sealedSegments))
}

func TestExpireSegment(t *testing.T) {
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segAllocator, err := newSegmentAllocator(meta, mockAllocator)
	assert.Nil(t, err)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		ID:     collID,
		Schema: schema,
	})
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segmentInfo, err := BuildSegment(collID, 100, id, []string{"c1", "c2"})
	assert.Nil(t, err)
	err = meta.AddSegment(segmentInfo)
	assert.Nil(t, err)
	err = segAllocator.OpenSegment(segmentInfo)
	assert.Nil(t, err)

	id1, _, _, err := segAllocator.AllocSegment(collID, 100, "c1", 10)
	assert.Nil(t, err)
	time.Sleep(time.Duration(Params.SegIDAssignExpiration) * time.Millisecond)
	ts, err := mockAllocator.allocTimestamp()
	assert.Nil(t, err)
	err = segAllocator.ExpireAllocations(ts)
	assert.Nil(t, err)
	expired, err := segAllocator.IsAllocationsExpired(id1, ts)
	assert.Nil(t, err)
	assert.True(t, expired)
	assert.EqualValues(t, 0, len(segAllocator.segments[id1].allocations))
}
