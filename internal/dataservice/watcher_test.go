package dataservice

import (
	"strconv"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	"github.com/stretchr/testify/assert"
)

func TestDataNodeTTWatcher(t *testing.T) {
	Params.Init()
	c := make(chan struct{})
	cluster := newDataNodeCluster(c)
	defer cluster.ShutDownClients()
	schema := newTestSchema()
	allocator := newMockAllocator()
	meta, err := newMemoryMeta(allocator)
	assert.Nil(t, err)
	segAllocator := newSegmentAllocator(meta, allocator)
	assert.Nil(t, err)
	watcher := newDataNodeTimeTickWatcher(meta, segAllocator, cluster)

	id, err := allocator.allocID()
	assert.Nil(t, err)
	err = meta.AddCollection(&collectionInfo{
		Schema: schema,
		ID:     id,
	})
	assert.Nil(t, err)

	cases := []struct {
		sealed     bool
		allocation bool
		expired    bool
		expected   bool
	}{
		{false, false, true, false},
		{false, true, true, false},
		{false, true, false, false},
		{true, false, true, true},
		{true, true, false, false},
		{true, true, true, true},
	}

	segmentIDs := make([]UniqueID, len(cases))
	for i, c := range cases {
		segID, err := allocator.allocID()
		segmentIDs[i] = segID
		assert.Nil(t, err)
		segmentInfo, err := BuildSegment(id, 100, segID, "channel"+strconv.Itoa(i))
		assert.Nil(t, err)
		err = meta.AddSegment(segmentInfo)
		assert.Nil(t, err)
		err = segAllocator.OpenSegment(segmentInfo)
		assert.Nil(t, err)
		if c.allocation && c.expired {
			_, _, _, err := segAllocator.AllocSegment(id, 100, "channel"+strconv.Itoa(i), 100)
			assert.Nil(t, err)
		}
	}

	time.Sleep(time.Duration(Params.SegIDAssignExpiration+1000) * time.Millisecond)
	for i, c := range cases {
		if c.allocation && !c.expired {
			_, _, _, err := segAllocator.AllocSegment(id, 100, "channel"+strconv.Itoa(i), 100)
			assert.Nil(t, err)
		}
		if c.sealed {
			err := segAllocator.SealSegment(segmentIDs[i])
			assert.Nil(t, err)
		}
	}
	ts, err := allocator.allocTimestamp()
	assert.Nil(t, err)

	err = watcher.handleTimeTickMsg(&msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				Timestamp: ts,
			},
		},
	})
	assert.Nil(t, err)
	for i, c := range cases {
		_, ok := segAllocator.segments[segmentIDs[i]]
		assert.EqualValues(t, !c.expected, ok)
	}
}
