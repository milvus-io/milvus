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

package datanode

import (
	"container/heap"
	"context"
	"testing"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

var deleteNodeTestDir = "/tmp/milvus_test/deleteNode"

func TestFlowGraphDeleteNode_newDeleteNode(te *testing.T) {
	tests := []struct {
		ctx    context.Context
		config *nodeConfig

		description string
	}{
		{context.Background(), &nodeConfig{}, "pointer of channel"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			dn, err := newDeleteNode(test.ctx, nil, nil, make(chan string, 1), test.config)
			assert.NoError(t, err)

			assert.NotNil(t, dn)
			assert.Equal(t, "deleteNode-"+dn.channelName, dn.Name())
			dn.Close()
		})
	}
}

func genMockChannel(segIDs []int64, pks []primaryKey, chanName string) *ChannelMeta {
	pkStat1 := &storage.PkStatistics{
		PkFilter: bloom.NewWithEstimates(1000000, 0.01),
	}

	pkStat2 := &storage.PkStatistics{
		PkFilter: bloom.NewWithEstimates(1000000, 0.01),
	}

	for i := 0; i < 3; i++ {
		pkStat1.UpdateMinMax(pks[i])
		buf := make([]byte, 8)
		for _, pk := range pks {
			switch pk.Type() {
			case schemapb.DataType_Int64:
				int64Value := pk.(*int64PrimaryKey).Value
				common.Endian.PutUint64(buf, uint64(int64Value))
				pkStat1.PkFilter.Add(buf)
			case schemapb.DataType_VarChar:
				stringValue := pk.(*varCharPrimaryKey).Value
				pkStat1.PkFilter.AddString(stringValue)
			default:
			}
		}
	}

	for i := 3; i < 5; i++ {
		pkStat2.UpdateMinMax(pks[i])
		buf := make([]byte, 8)
		for _, pk := range pks {
			switch pk.Type() {
			case schemapb.DataType_Int64:
				int64Value := pk.(*int64PrimaryKey).Value
				common.Endian.PutUint64(buf, uint64(int64Value))
				pkStat2.PkFilter.Add(buf)
			case schemapb.DataType_VarChar:
				stringValue := pk.(*varCharPrimaryKey).Value
				pkStat2.PkFilter.AddString(stringValue)
			default:
			}
		}
	}

	segTypes := []datapb.SegmentType{
		datapb.SegmentType_New,
		datapb.SegmentType_New,
		datapb.SegmentType_Normal,
		datapb.SegmentType_Normal,
		datapb.SegmentType_Flushed,
		datapb.SegmentType_Flushed,
	}

	channel := &ChannelMeta{
		channelName: chanName,
		segments:    make(map[UniqueID]*Segment),
	}
	for i := range segIDs {
		seg := Segment{
			segmentID: segIDs[i],
		}
		seg.setType(segTypes[i])
		if i < 3 {
			seg.currentStat = pkStat1
		} else {
			seg.currentStat = pkStat2
		}
		channel.segments[segIDs[i]] = &seg
	}

	return channel
}

func TestFlowGraphDeleteNode_Operate(t *testing.T) {
	ctx := context.Background()
	t.Run("Test deleteNode Operate invalid Msg", func(te *testing.T) {
		invalidInTests := []struct {
			in   []Msg
			desc string
		}{
			{[]Msg{},
				"Invalid input length == 0"},
			{[]Msg{&flowGraphMsg{}, &flowGraphMsg{}, &flowGraphMsg{}},
				"Invalid input length == 3"},
			{[]Msg{&flowgraph.MsgStreamMsg{}},
				"Invalid input length == 1 but input message is not flowGraphMsg"},
		}

		for _, test := range invalidInTests {
			te.Run(test.desc, func(t *testing.T) {
				dn := deleteNode{}
				assert.False(t, dn.IsValidInMsg(test.in))
			})
		}
	})

	const (
		chanName = "channel-test"
	)
	var (
		segIDs   = []int64{11, 22, 33, 44, 55}
		int64Pks = []primaryKey{
			newInt64PrimaryKey(3),
			newInt64PrimaryKey(17),
			newInt64PrimaryKey(44),
			newInt64PrimaryKey(190),
			newInt64PrimaryKey(425),
		}
		varCharPks = []primaryKey{
			newVarCharPrimaryKey("ab"),
			newVarCharPrimaryKey("ac"),
			newVarCharPrimaryKey("bcd"),
			newVarCharPrimaryKey("gggg"),
			newVarCharPrimaryKey("milvus"),
		}
		tss = []uint64{1, 1, 1, 1, 1}
	)
	cm := storage.NewLocalChunkManager(storage.RootPath(deleteNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	t.Run("Test get segment by varChar primary keys", func(te *testing.T) {
		channel := genMockChannel(segIDs, varCharPks, chanName)
		alloc := allocator.NewMockAllocator(t)
		fm := NewRendezvousFlushManager(alloc, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)
		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}

		dn, err := newDeleteNode(context.Background(), fm, delBufManager, make(chan string, 1), c)
		assert.NoError(t, err)

		segID2Pks, _ := dn.filterSegmentByPK(0, varCharPks, tss)
		expected := map[int64][]primaryKey{
			segIDs[0]: varCharPks[0:3],
			segIDs[1]: varCharPks[0:3],
			segIDs[2]: varCharPks[0:3],
			segIDs[3]: varCharPks[3:5],
			segIDs[4]: varCharPks[3:5],
		}
		for segmentID, expectedPks := range expected {
			filterPks := segID2Pks[segmentID]
			assert.Equal(t, len(expectedPks), len(filterPks))
			for index, pk := range expectedPks {
				assert.Equal(t, true, pk.EQ(filterPks[index]))
			}
		}
	})

	channel := genMockChannel(segIDs, int64Pks, chanName)
	alloc := allocator.NewMockAllocator(t)
	fm := NewRendezvousFlushManager(alloc, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)
	t.Run("Test get segment by int64 primary keys", func(te *testing.T) {
		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}

		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}

		dn, err := newDeleteNode(context.Background(), fm, delBufManager, make(chan string, 1), c)
		assert.NoError(t, err)

		segID2Pks, _ := dn.filterSegmentByPK(0, int64Pks, tss)
		t.Log(segID2Pks)
		expected := map[int64][]primaryKey{
			segIDs[0]: int64Pks[0:3],
			segIDs[1]: int64Pks[0:3],
			segIDs[2]: int64Pks[0:3],
			segIDs[3]: int64Pks[3:5],
			segIDs[4]: int64Pks[3:5],
		}
		for segmentID, expectedPks := range expected {
			filterPks := segID2Pks[segmentID]
			assert.Equal(t, len(expectedPks), len(filterPks))
			for index, pk := range expectedPks {
				assert.Equal(t, true, pk.EQ(filterPks[index]))
			}
		}
	})

	t.Run("Test deleteNode Operate valid Msg with failure", func(te *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}
		delNode, err := newDeleteNode(ctx, fm, delBufManager, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = segIDs
		// this will fail since ts = 0 will trigger mocked error
		var fgMsg flowgraph.Msg = &msg
		delNode.Operate([]flowgraph.Msg{fgMsg})
	})
	t.Run("Test deleteNode Operate valid Msg with failure", func(te *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}
		delNode, err := newDeleteNode(ctx, fm, delBufManager, make(chan string, 1), c)
		assert.Nil(te, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = segIDs

		msg.endPositions[0].Timestamp = 100 // set to normal timestamp
		var fgMsg flowgraph.Msg = &msg
		delNode.Operate([]flowgraph.Msg{fgMsg})

		msg.deleteMessages = []*msgstream.DeleteMsg{}
		// send again shall trigger empty buffer flush
		delNode.Operate([]flowgraph.Msg{fgMsg})
	})

	t.Run("Test deleteNode Operate valid with dropCollection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-operate"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}
		sig := make(chan string, 1)
		delNode, err := newDeleteNode(ctx, fm, delBufManager, sig, c)
		assert.NoError(t, err)

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.segmentsToSync = segIDs

		msg.endPositions[0].Timestamp = 100 // set to normal timestamp
		msg.dropCollection = true
		assert.NotPanics(t, func() {
			fm.startDropping()
			delNode.Operate([]flowgraph.Msg{&msg})
		})
		select {
		case <-time.After(time.Millisecond):
			t.FailNow()
		case <-sig:
		}
	})

	t.Run("Test issue#18565", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-issue18565"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))

		channel := &ChannelMeta{
			segments: make(map[UniqueID]*Segment),
		}

		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}
		delNode, err := newDeleteNode(ctx, fm, delBufManager, make(chan string, 1), c)
		assert.NoError(t, err)

		compactedSegment := UniqueID(10020987)
		seg := Segment{
			segmentID:   compactedSegment,
			compactedTo: 100,
		}
		seg.setType(datapb.SegmentType_Compacted)
		channel.segments[compactedSegment] = &seg

		msg := genFlowGraphDeleteMsg(int64Pks, chanName)
		msg.deleteMessages = []*msgstream.DeleteMsg{}
		msg.segmentsToSync = []UniqueID{compactedSegment}

		bufItem := &Item{memorySize: 0}
		delNode.delBufferManager.updateMeta(compactedSegment,
			&DelDataBuf{delData: &DeleteData{}, item: bufItem})
		heap.Push(delNode.delBufferManager.delBufHeap, bufItem)

		delNode.flushManager = NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, channel,
			func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

		var fgMsg flowgraph.Msg = &msg
		setFlowGraphRetryOpt(retry.Attempts(1))
		assert.NotPanics(t, func() {
			delNode.Operate([]flowgraph.Msg{fgMsg})
		})

		_, ok := delNode.delBufferManager.Load(100)
		assert.False(t, ok)
		_, ok = delNode.delBufferManager.Load(compactedSegment)
		assert.False(t, ok)
	})

	t.Run("Test deleteNode auto flush function", func(t *testing.T) {
		//for issue
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		chanName := "datanode-test-FlowGraphDeletenode-autoflush"
		testPath := "/test/datanode/root/meta"
		assert.NoError(t, clearEtcd(testPath))
		Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

		c := &nodeConfig{
			channel:      channel,
			vChannelName: chanName,
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}
		mockFlushManager := &mockFlushManager{
			recordFlushedSeg: true,
		}
		delNode, err := newDeleteNode(ctx, mockFlushManager, delBufManager, make(chan string, 1), c)
		assert.NoError(t, err)

		//2. here we set flushing segments inside fgmsg to empty
		//in order to verify the validity of auto flush function
		msg := genFlowGraphDeleteMsg(int64Pks, chanName)

		// delete has to match segment partition ID
		for _, msg := range msg.deleteMessages {
			msg.PartitionID = 0
		}
		msg.segmentsToSync = []UniqueID{}

		var fgMsg flowgraph.Msg = &msg
		//1. here we set buffer bytes to a relatively high level
		//and the sum of memory consumption in this case is 208
		//so no segments will be flushed
		paramtable.Get().Save(Params.DataNodeCfg.FlushDeleteBufferBytes.Key, "300")
		fgMsg.(*flowGraphMsg).segmentsToSync = delNode.delBufferManager.ShouldFlushSegments()
		delNode.Operate([]flowgraph.Msg{fgMsg})
		assert.Equal(t, 0, len(mockFlushManager.flushedSegIDs))
		assert.Equal(t, int64(208), delNode.delBufferManager.usedMemory.Load())
		assert.Equal(t, 5, delNode.delBufferManager.delBufHeap.Len())

		//3. note that the whole memory size used by 5 segments will be 208
		//so when setting delete buffer size equal to 200
		//there will only be one segment to be flushed then the
		//memory consumption will be reduced to 160(under 200)
		msg.deleteMessages = []*msgstream.DeleteMsg{}
		msg.segmentsToSync = []UniqueID{}
		paramtable.Get().Save(Params.DataNodeCfg.FlushDeleteBufferBytes.Key, "200")
		fgMsg.(*flowGraphMsg).segmentsToSync = delNode.delBufferManager.ShouldFlushSegments()
		delNode.Operate([]flowgraph.Msg{fgMsg})
		assert.Equal(t, 1, len(mockFlushManager.flushedSegIDs))
		assert.Equal(t, int64(160), delNode.delBufferManager.usedMemory.Load())
		assert.Equal(t, 4, delNode.delBufferManager.delBufHeap.Len())

		//4. there is no new delete msg and delBufferSize is still 200
		//we expect there will not be any auto flush del
		fgMsg.(*flowGraphMsg).segmentsToSync = delNode.delBufferManager.ShouldFlushSegments()
		delNode.Operate([]flowgraph.Msg{fgMsg})
		assert.Equal(t, 1, len(mockFlushManager.flushedSegIDs))
		assert.Equal(t, int64(160), delNode.delBufferManager.usedMemory.Load())
		assert.Equal(t, 4, delNode.delBufferManager.delBufHeap.Len())

		//5. we reset buffer bytes to 150, then we expect there would be one more
		//segment which is 48 in size to be flushed, so the remained del memory size
		//will be 112
		paramtable.Get().Save(Params.DataNodeCfg.FlushDeleteBufferBytes.Key, "150")
		fgMsg.(*flowGraphMsg).segmentsToSync = delNode.delBufferManager.ShouldFlushSegments()
		delNode.Operate([]flowgraph.Msg{fgMsg})
		assert.Equal(t, 2, len(mockFlushManager.flushedSegIDs))
		assert.Equal(t, int64(112), delNode.delBufferManager.usedMemory.Load())
		assert.Equal(t, 3, delNode.delBufferManager.delBufHeap.Len())

		//6. we reset buffer bytes to 60, then most of the segments will be flushed
		//except for the smallest entry with size equaling to 32
		paramtable.Get().Save(Params.DataNodeCfg.FlushDeleteBufferBytes.Key, "60")
		fgMsg.(*flowGraphMsg).segmentsToSync = delNode.delBufferManager.ShouldFlushSegments()
		delNode.Operate([]flowgraph.Msg{fgMsg})
		assert.Equal(t, 4, len(mockFlushManager.flushedSegIDs))
		assert.Equal(t, int64(32), delNode.delBufferManager.usedMemory.Load())
		assert.Equal(t, 1, delNode.delBufferManager.delBufHeap.Len())

		//7. we reset buffer bytes to 20, then as all segment-memory consumption
		//is more than 20, so all five segments will be flushed and the remained
		//del memory will be lowered to zero
		paramtable.Get().Save(Params.DataNodeCfg.FlushDeleteBufferBytes.Key, "20")
		fgMsg.(*flowGraphMsg).segmentsToSync = delNode.delBufferManager.ShouldFlushSegments()
		delNode.Operate([]flowgraph.Msg{fgMsg})
		assert.Equal(t, 5, len(mockFlushManager.flushedSegIDs))
		assert.Equal(t, int64(0), delNode.delBufferManager.usedMemory.Load())
		assert.Equal(t, 0, delNode.delBufferManager.delBufHeap.Len())
	})
}

func TestFlowGraphDeleteNode_showDelBuf(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cm := storage.NewLocalChunkManager(storage.RootPath(deleteNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	fm := NewRendezvousFlushManager(allocator.NewMockAllocator(t), cm, nil, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	chanName := "datanode-test-FlowGraphDeletenode-showDelBuf"
	testPath := "/test/datanode/root/meta"
	assert.NoError(t, clearEtcd(testPath))
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	channel := &ChannelMeta{
		segments: make(map[UniqueID]*Segment),
	}
	c := &nodeConfig{
		channel:      channel,
		vChannelName: chanName,
	}
	delBufManager := &DeltaBufferManager{
		channel:    channel,
		delBufHeap: &PriorityQueue{},
	}
	delNode, err := newDeleteNode(ctx, fm, delBufManager, make(chan string, 1), c)
	require.NoError(t, err)

	tests := []struct {
		seg     UniqueID
		numRows int64
	}{
		{111, 10},
		{112, 10},
		{113, 1},
	}

	for _, test := range tests {
		delBuf := newDelDataBuf(test.seg)
		delBuf.accumulateEntriesNum(test.numRows)
		delNode.delBufferManager.updateMeta(test.seg, delBuf)
		heap.Push(delNode.delBufferManager.delBufHeap, delBuf.item)
	}

	delNode.showDelBuf([]UniqueID{111, 112, 113}, 100)
}
