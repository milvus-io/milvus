package dataservice

import (
	"context"
	"log"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"

	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"

	"github.com/stretchr/testify/assert"
)

func TestAllocSegment(t *testing.T) {
	ctx := context.Background()
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segAllocator := newSegmentAllocator(meta, mockAllocator)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&datapb.CollectionInfo{
		ID:     collID,
		Schema: schema,
	})
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segmentInfo, err := BuildSegment(collID, 100, id, "c1")
	assert.Nil(t, err)
	err = meta.AddSegment(segmentInfo)
	assert.Nil(t, err)
	err = segAllocator.OpenSegment(ctx, segmentInfo)
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
		id, count, expireTime, err := segAllocator.AllocSegment(ctx, c.collectionID, c.partitionID, c.channelName, c.requestRows)
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
	ctx := context.Background()
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segAllocator := newSegmentAllocator(meta, mockAllocator)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&datapb.CollectionInfo{
		ID:     collID,
		Schema: schema,
	})
	assert.Nil(t, err)
	var lastSegID UniqueID
	for i := 0; i < 10; i++ {
		id, err := mockAllocator.allocID()
		assert.Nil(t, err)
		segmentInfo, err := BuildSegment(collID, 100, id, "c"+strconv.Itoa(i))
		assert.Nil(t, err)
		err = meta.AddSegment(segmentInfo)
		assert.Nil(t, err)
		err = segAllocator.OpenSegment(ctx, segmentInfo)
		assert.Nil(t, err)
		lastSegID = segmentInfo.ID
	}

	err = segAllocator.SealSegment(ctx, lastSegID)
	assert.Nil(t, err)
	segAllocator.SealAllSegments(ctx, collID)
	sealedSegments, err := segAllocator.GetSealedSegments(ctx)
	assert.Nil(t, err)
	assert.EqualValues(t, 10, len(sealedSegments))
}

func TestExpireSegment(t *testing.T) {
	ctx := context.Background()
	Params.Init()
	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)
	segAllocator := newSegmentAllocator(meta, mockAllocator)

	schema := newTestSchema()
	collID, err := mockAllocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&datapb.CollectionInfo{
		ID:     collID,
		Schema: schema,
	})
	assert.Nil(t, err)
	id, err := mockAllocator.allocID()
	assert.Nil(t, err)
	segmentInfo, err := BuildSegment(collID, 100, id, "c1")
	assert.Nil(t, err)
	err = meta.AddSegment(segmentInfo)
	assert.Nil(t, err)
	err = segAllocator.OpenSegment(ctx, segmentInfo)
	assert.Nil(t, err)

	id1, _, et, err := segAllocator.AllocSegment(ctx, collID, 100, "c1", 10)
	ts2, _ := tsoutil.ParseTS(et)
	log.Printf("physical ts: %s", ts2.String())
	assert.Nil(t, err)

	ts, err := mockAllocator.allocTimestamp()
	assert.Nil(t, err)
	t1, _ := tsoutil.ParseTS(ts)
	log.Printf("before ts: %s", t1.String())
	time.Sleep(time.Duration(Params.SegIDAssignExpiration+1000) * time.Millisecond)
	ts, err = mockAllocator.allocTimestamp()
	assert.Nil(t, err)
	err = segAllocator.ExpireAllocations(ctx, ts)
	assert.Nil(t, err)
	expired, err := segAllocator.IsAllocationsExpired(ctx, id1, ts)
	if et > ts {
		tsPhy, _ := tsoutil.ParseTS(ts)
		log.Printf("ts %s", tsPhy.String())
	}
	assert.Nil(t, err)
	assert.True(t, expired)
	assert.EqualValues(t, 0, len(segAllocator.segments[id1].allocations))
}
