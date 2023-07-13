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
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var insertNodeTestDir = "/tmp/milvus_test/insert_node"

// CDFMsFactory count down fails msg factory
type CDFMsFactory struct {
	msgstream.Factory
	cd int
}

func (f *CDFMsFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	f.cd--
	if f.cd < 0 {
		return nil, errors.New("fail")
	}
	return f.Factory.NewMsgStream(ctx)
}

func TestFlowGraphInsertBufferNodeCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-create"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	channel := newChannel(insertChannelName, collMeta.ID, collMeta.Schema, mockRootCoord, cm)
	err = channel.addSegment(
		addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       1,
			collID:      collMeta.ID,
			partitionID: 0,
			startPos:    &msgpb.MsgPosition{},
			endPos:      &msgpb.MsgPosition{},
		})
	require.NoError(t, err)

	factory := dependency.NewDefaultFactory(true)

	alloc := allocator.NewMockAllocator(t)
	fm := NewRendezvousFlushManager(alloc, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)

	c := &nodeConfig{
		channel:      channel,
		msFactory:    factory,
		allocator:    alloc,
		vChannelName: "string",
	}
	delBufManager := &DeltaBufferManager{
		channel:    channel,
		delBufHeap: &PriorityQueue{},
	}

	dataCoord := &DataCoordFactory{}
	atimeTickSender := newTimeTickSender(dataCoord, 0)
	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, delBufManager, flushChan, resendTTChan, fm, newCache(), c, atimeTickSender)
	assert.NotNil(t, iBNode)
	require.NoError(t, err)
}

type mockMsg struct {
	BaseMsg
}

func (*mockMsg) TimeTick() Timestamp {
	return 0
}

func (*mockMsg) IsClose() bool {
	return false
}

func TestFlowGraphInsertBufferNode_Operate(t *testing.T) {
	t.Run("Test iBNode Operate invalid Msg", func(te *testing.T) {
		invalidInTests := []struct {
			in          []Msg
			description string
		}{
			{[]Msg{},
				"Invalid input length == 0"},
			{[]Msg{&flowGraphMsg{}, &flowGraphMsg{}, &flowGraphMsg{}},
				"Invalid input length == 3"},
			{[]Msg{&mockMsg{}},
				"Invalid input length == 1 but input message is not flowGraphMsg"},
		}

		for _, test := range invalidInTests {
			te.Run(test.description, func(t0 *testing.T) {
				ibn := &insertBufferNode{}
				assert.False(t0, ibn.IsValidInMsg(test.in))
			})
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-operate"

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	channel := newChannel(insertChannelName, collMeta.ID, collMeta.Schema, mockRootCoord, cm)

	err = channel.addSegment(
		addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       1,
			collID:      collMeta.ID,
			partitionID: 0,
			startPos:    &msgpb.MsgPosition{},
			endPos:      &msgpb.MsgPosition{},
		})
	require.NoError(t, err)

	factory := dependency.NewDefaultFactory(true)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().Alloc(mock.Anything).Call.Return(int64(22222),
		func(count uint32) int64 {
			return int64(22222 + count)
		}, nil)
	fm := NewRendezvousFlushManager(alloc, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	c := &nodeConfig{
		channel:      channel,
		msFactory:    factory,
		allocator:    alloc,
		vChannelName: "string",
	}
	delBufManager := &DeltaBufferManager{
		channel:    channel,
		delBufHeap: &PriorityQueue{},
	}

	dataCoord := &DataCoordFactory{}
	atimeTickSender := newTimeTickSender(dataCoord, 0)
	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, delBufManager, flushChan, resendTTChan, fm, newCache(), c, atimeTickSender)
	require.NoError(t, err)

	flushChan <- flushMsg{
		msgID:        1,
		timestamp:    2000,
		segmentID:    UniqueID(1),
		collectionID: UniqueID(1),
	}

	inMsg := genFlowGraphInsertMsg(insertChannelName)
	iBNode.channel.setSegmentLastSyncTs(UniqueID(1), tsoutil.ComposeTSByTime(time.Now().Add(-11*time.Minute), 0))
	assert.NotPanics(t, func() {
		res := iBNode.Operate([]flowgraph.Msg{&inMsg})
		assert.Subset(t, res[0].(*flowGraphMsg).segmentsToSync, []UniqueID{1})
	})
	assert.NotSubset(t, iBNode.channel.listSegmentIDsToSync(tsoutil.ComposeTSByTime(time.Now(), 0)), []UniqueID{1})

	resendTTChan <- resendTTMsg{
		segmentIDs: []int64{0, 1, 2},
	}

	inMsg = genFlowGraphInsertMsg(insertChannelName)
	assert.NotPanics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })

	// test drop collection operate
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	inMsg.dropCollection = true
	assert.NotPanics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })

	// test consume old message
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	timestampBak := iBNode.lastTimestamp
	iBNode.lastTimestamp = typeutil.MaxTimestamp
	assert.Panics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })
	iBNode.lastTimestamp = timestampBak

	// test addSegmentAndUpdateRowNum failed
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	inMsg.insertMessages[0].CollectionID = UniqueID(-1)
	inMsg.insertMessages[0].SegmentID = UniqueID(-1)
	assert.NoError(t, err)
	assert.Panics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })

	// test bufferInsertMsg failed
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	inMsg.insertMessages[0].Timestamps = []Timestamp{1, 2}
	inMsg.insertMessages[0].RowIDs = []int64{1}
	assert.Panics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })

	// test flushBufferData failed
	segment := channel.getSegment(UniqueID(1))
	segment.setSyncing(false)
	setFlowGraphRetryOpt(retry.Attempts(1))
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	iBNode.flushManager = &mockFlushManager{returnError: true}
	iBNode.channel.setCurInsertBuffer(inMsg.insertMessages[0].SegmentID, &BufferData{})
	assert.Panics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })
	iBNode.flushManager = fm
}

func genCollectionMeta(collectionID UniqueID, collectionName string) *etcdpb.CollectionMeta {
	sch := schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "test collection by meta factory",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     0,
				Name:        "RowID",
				Description: "RowID field",
				DataType:    schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f0_tk1",
						Value: "f0_tv1",
					},
				},
			},
			{
				FieldID:     1,
				Name:        "Timestamp",
				Description: "Timestamp field",
				DataType:    schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "f1_tk1",
						Value: "f1_tv1",
					},
				},
			},
			{
				FieldID:     107,
				Name:        "float32_field",
				Description: "field 107",
				DataType:    schemapb.DataType_Float,
				TypeParams:  []*commonpb.KeyValuePair{},
				IndexParams: []*commonpb.KeyValuePair{},
			},
		},
	}

	collection := etcdpb.CollectionMeta{
		ID:           collectionID,
		Schema:       &sch,
		CreateTime:   Timestamp(1),
		SegmentIDs:   make([]UniqueID, 0),
		PartitionIDs: []UniqueID{0},
	}
	return &collection
}

func TestFlowGraphInsertBufferNode_AutoFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	dataFactory := NewDataFactory()

	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	channel := &ChannelMeta{
		collectionID: collMeta.ID,
		segments:     make(map[UniqueID]*Segment),
		needToSync:   atomic.NewBool(false),
	}

	channel.metaService = newMetaService(mockRootCoord, collMeta.ID)

	factory := dependency.NewDefaultFactory(true)

	flushPacks := []*segmentFlushPack{}
	fpMut := sync.Mutex{}
	wg := sync.WaitGroup{}

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().Alloc(mock.Anything).Call.Return(int64(22222),
		func(count uint32) int64 {
			return int64(22222 + count)
		}, nil)
	fm := NewRendezvousFlushManager(alloc, cm, channel, func(pack *segmentFlushPack) {
		fpMut.Lock()
		flushPacks = append(flushPacks, pack)
		fpMut.Unlock()
		startPos := channel.listNewSegmentsStartPositions()
		channel.transferNewSegments(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) UniqueID {
			return pos.GetSegmentID()
		}))
		if pack.flushed || pack.dropped {
			channel.segmentFlushed(pack.segmentID)
		}
		wg.Done()
	}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	c := &nodeConfig{
		channel:      channel,
		msFactory:    factory,
		allocator:    alloc,
		vChannelName: "string",
	}
	delBufManager := &DeltaBufferManager{
		channel:    channel,
		delBufHeap: &PriorityQueue{},
	}
	dataCoord := &DataCoordFactory{}
	atimeTickSender := newTimeTickSender(dataCoord, 0)
	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, delBufManager, flushChan, resendTTChan, fm, newCache(), c, atimeTickSender)
	require.NoError(t, err)

	// Auto flush number of rows set to 2

	inMsg := genFlowGraphInsertMsg("datanode-03-test-autoflush")
	inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(2)
	var iMsg flowgraph.Msg = &inMsg

	t.Run("Pure auto flush", func(t *testing.T) {
		// iBNode.insertBuffer.maxSize = 2
		tmp := Params.DataNodeCfg.FlushInsertBufferSize
		paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, "200")
		defer func() {
			Params.DataNodeCfg.FlushInsertBufferSize = tmp
		}()

		for i := range inMsg.insertMessages {
			inMsg.insertMessages[i].SegmentID = int64(i%2) + 1
		}
		inMsg.startPositions = []*msgpb.MsgPosition{{Timestamp: 100}}
		inMsg.endPositions = []*msgpb.MsgPosition{{Timestamp: 123}}

		type Test struct {
			expectedSegID       UniqueID
			expectedNumOfRows   int64
			expectedStartPosTs  Timestamp
			expectedEndPosTs    Timestamp
			expectedCpNumOfRows int64
			expectedCpPosTs     Timestamp
		}

		beforeAutoFlushTests := []Test{
			// segID, numOfRow, startTs, endTs, cp.numOfRow, cp.Ts
			{1, 1, 100, 123, 0, 100},
			{2, 1, 100, 123, 0, 100},
		}
		iBNode.Operate([]flowgraph.Msg{iMsg})

		assert.Equal(t, 0, len(flushPacks))

		for i, test := range beforeAutoFlushTests {
			channel.segMu.Lock()
			seg, ok := channel.segments[UniqueID(i+1)]
			channel.segMu.Unlock()
			assert.True(t, ok)
			assert.Equal(t, datapb.SegmentType_New, seg.getType())
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
		}

		for i := range inMsg.insertMessages {
			inMsg.insertMessages[i].SegmentID = int64(i%2) + 2
		}
		inMsg.startPositions = []*msgpb.MsgPosition{{Timestamp: 200}}
		inMsg.endPositions = []*msgpb.MsgPosition{{Timestamp: 234}}
		iMsg = &inMsg

		// Triger auto flush
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToSync))
		t.Log("segments to flush", fgm.segmentsToSync)

		for _, im := range fgm.segmentsToSync {
			// send del done signal
			err = fm.flushDelData(nil, im, fgm.endPositions[0])
			assert.NoError(t, err)
		}
		wg.Wait()

		assert.Equal(t, 1, len(flushPacks))
		assert.Less(t, 0, len(flushPacks[0].insertLogs))
		assert.False(t, flushPacks[0].flushed)

		afterAutoFlushTests := []Test{
			// segID, numOfRow, startTs, endTs, cp.numOfRow, cp.Ts
			{1, 1, 100, 123, 0, 100},
			{2, 2, 100, 234, 2, 234},
			{3, 1, 200, 234, 0, 200},
		}

		for i, test := range afterAutoFlushTests {
			seg, ok := channel.segments[UniqueID(i+1)]
			assert.True(t, ok)
			assert.Equal(t, datapb.SegmentType_Normal, seg.getType())
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())

			if i == 1 {
				assert.Equal(t, test.expectedSegID, flushPacks[0].segmentID)
				// assert.Equal(t, int64(0), iBNode.insertBuffer.size(UniqueID(i+1)))
			}
			// else {
			//     // assert.Equal(t, int64(1), iBNode.insertBuffer.size(UniqueID(i+1)))
			// }
		}

	})

	t.Run("Auto with manual flush", func(t *testing.T) {
		tmp := Params.DataNodeCfg.FlushInsertBufferSize
		paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, "200")
		defer func() {
			Params.DataNodeCfg.FlushInsertBufferSize = tmp
		}()

		fpMut.Lock()
		flushPacks = flushPacks[:0]
		fpMut.Unlock()

		inMsg := genFlowGraphInsertMsg("datanode-03-test-autoflush")
		inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(2)

		for i := range inMsg.insertMessages {
			inMsg.insertMessages[i].SegmentID = UniqueID(10 + i)
		}
		inMsg.startPositions = []*msgpb.MsgPosition{{Timestamp: 300}}
		inMsg.endPositions = []*msgpb.MsgPosition{{Timestamp: 323}}
		var iMsg flowgraph.Msg = &inMsg

		type Test struct {
			expectedSegID       UniqueID
			expectedNumOfRows   int64
			expectedStartPosTs  Timestamp
			expectedEndPosTs    Timestamp
			expectedCpNumOfRows int64
			expectedCpPosTs     Timestamp
		}

		beforeAutoFlushTests := []Test{
			// segID, numOfRow, startTs, endTs, cp.numOfRow, cp.Ts
			{10, 1, 300, 323, 0, 300},
			{11, 1, 300, 323, 0, 300},
		}
		iBNode.Operate([]flowgraph.Msg{iMsg})

		assert.Equal(t, 0, len(flushPacks))

		for _, test := range beforeAutoFlushTests {
			channel.segMu.Lock()
			seg, ok := channel.segments[test.expectedSegID]
			channel.segMu.Unlock()
			assert.True(t, ok)
			assert.Equal(t, datapb.SegmentType_New, seg.getType())
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
		}

		inMsg.startPositions = []*msgpb.MsgPosition{{Timestamp: 400}}
		inMsg.endPositions = []*msgpb.MsgPosition{{Timestamp: 434}}

		// trigger manual flush
		flushChan <- flushMsg{segmentID: 10}

		// trigger auto flush since buffer full
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToSync))
		for _, im := range fgm.segmentsToSync {
			// send del done signal
			err = fm.flushDelData(nil, im, fgm.endPositions[0])
			assert.NoError(t, err)
		}
		wg.Wait()

		assert.Equal(t, 2, len(flushPacks))
		for _, pack := range flushPacks {
			if pack.segmentID == 10 {
				assert.Equal(t, true, pack.flushed)
			} else {
				assert.Equal(t, false, pack.flushed)
			}
		}

	})
}

func TestInsertBufferNodeRollBF(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	dataFactory := NewDataFactory()

	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	channel := &ChannelMeta{
		collectionID: collMeta.ID,
		segments:     make(map[UniqueID]*Segment),
		needToSync:   atomic.NewBool(false),
	}

	channel.metaService = newMetaService(mockRootCoord, collMeta.ID)

	factory := dependency.NewDefaultFactory(true)

	flushPacks := []*segmentFlushPack{}
	fpMut := sync.Mutex{}
	wg := sync.WaitGroup{}

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().Alloc(mock.Anything).Call.Return(int64(22222),
		func(count uint32) int64 {
			return int64(22222 + count)
		}, nil)
	fm := NewRendezvousFlushManager(alloc, cm, channel, func(pack *segmentFlushPack) {
		fpMut.Lock()
		flushPacks = append(flushPacks, pack)
		fpMut.Unlock()
		startPos := channel.listNewSegmentsStartPositions()
		channel.transferNewSegments(lo.Map(startPos, func(pos *datapb.SegmentStartPosition, _ int) UniqueID {
			return pos.GetSegmentID()
		}))
		if pack.flushed || pack.dropped {
			channel.segmentFlushed(pack.segmentID)
		}
		wg.Done()
	}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	c := &nodeConfig{
		channel:      channel,
		msFactory:    factory,
		allocator:    alloc,
		vChannelName: "string",
	}
	delBufManager := &DeltaBufferManager{
		channel:    channel,
		delBufHeap: &PriorityQueue{},
	}

	dataCoord := &DataCoordFactory{}
	atimeTickSender := newTimeTickSender(dataCoord, 0)
	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, delBufManager, flushChan, resendTTChan, fm, newCache(), c, atimeTickSender)
	require.NoError(t, err)

	// Auto flush number of rows set to 2

	inMsg := genFlowGraphInsertMsg("")
	inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(1)
	var iMsg flowgraph.Msg = &inMsg

	t.Run("Pure roll BF", func(t *testing.T) {
		tmp := Params.DataNodeCfg.FlushInsertBufferSize
		paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, "200")
		defer func() {
			Params.DataNodeCfg.FlushInsertBufferSize = tmp
		}()

		inMsg.startPositions = []*msgpb.MsgPosition{{Timestamp: 100}}
		inMsg.endPositions = []*msgpb.MsgPosition{{Timestamp: 123}}

		type Test struct {
			expectedSegID       UniqueID
			expectedNumOfRows   int64
			expectedStartPosTs  Timestamp
			expectedEndPosTs    Timestamp
			expectedCpNumOfRows int64
			expectedCpPosTs     Timestamp
		}

		iBNode.Operate([]flowgraph.Msg{iMsg})

		assert.Equal(t, 0, len(flushPacks))

		// should not be flushed with only 1 one
		channel.segMu.Lock()
		seg, ok := channel.segments[UniqueID(1)]
		channel.segMu.Unlock()
		assert.True(t, ok)
		assert.Equal(t, datapb.SegmentType_New, seg.getType())
		assert.Equal(t, int64(1), seg.numRows)
		assert.Equal(t, uint64(100), seg.startPos.GetTimestamp())
		// because this is the origincal
		assert.True(t, seg.currentStat.PkFilter.Cap() > uint(1000000))

		inMsg.startPositions = []*msgpb.MsgPosition{{Timestamp: 200}}
		inMsg.endPositions = []*msgpb.MsgPosition{{Timestamp: 234}}
		iMsg = &inMsg

		// Triger auto flush
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToSync))
		t.Log("segments to flush", fgm.segmentsToSync)

		for _, im := range fgm.segmentsToSync {
			// send del done signal
			err = fm.flushDelData(nil, im, fgm.endPositions[0])
			assert.NoError(t, err)
		}
		wg.Wait()

		assert.Equal(t, 1, len(flushPacks))
		assert.Less(t, 0, len(flushPacks[0].insertLogs))
		assert.False(t, flushPacks[0].flushed)

		assert.True(t, ok)
		assert.Equal(t, datapb.SegmentType_Normal, seg.getType())
		assert.Equal(t, int64(2), seg.numRows)
		assert.Equal(t, uint64(100), seg.startPos.GetTimestamp())
		// filter should be rolled

		assert.Nil(t, seg.currentStat)
		assert.True(t, len(seg.historyStats) == 1)
		assert.True(t, seg.historyStats[0].PkFilter.Cap() < 100)
	})
}

type InsertBufferNodeSuite struct {
	suite.Suite

	channel       *ChannelMeta
	delBufManager *DeltaBufferManager

	collID UniqueID
	partID UniqueID
	cm     *storage.LocalChunkManager

	originalConfig int64
}

func (s *InsertBufferNodeSuite) SetupSuite() {
	insertBufferNodeTestDir := "/tmp/milvus_test/insert_buffer_node"
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	s.collID = 1
	s.partID = 10
	s.channel = newChannel("channel", s.collID, nil, rc, s.cm)

	s.delBufManager = &DeltaBufferManager{
		channel:    s.channel,
		delBufHeap: &PriorityQueue{},
	}
	s.cm = storage.NewLocalChunkManager(storage.RootPath(insertBufferNodeTestDir))

	s.originalConfig = Params.DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	// change flushing size to 2
	paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, "200")
}

func (s *InsertBufferNodeSuite) TearDownSuite() {
	s.cm.RemoveWithPrefix(context.Background(), s.cm.RootPath())
	paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, strconv.FormatInt(s.originalConfig, 10))
}

func (s *InsertBufferNodeSuite) SetupTest() {
	segs := []struct {
		segID UniqueID
		sType datapb.SegmentType
	}{
		{1, datapb.SegmentType_New},
		{2, datapb.SegmentType_Normal},
		{3, datapb.SegmentType_Flushed},
	}

	for _, seg := range segs {
		err := s.channel.addSegment(addSegmentReq{
			segType:     seg.sType,
			segID:       seg.segID,
			collID:      s.collID,
			partitionID: s.partID,
			startPos:    new(msgpb.MsgPosition),
			endPos:      new(msgpb.MsgPosition),
		})
		s.Require().NoError(err)
	}
}

func (s *InsertBufferNodeSuite) TearDownTest() {
	s.channel.removeSegments(1, 2, 3)
}

func (s *InsertBufferNodeSuite) TestFillInSyncTasks() {
	s.Run("drop collection", func() {
		fgMsg := &flowGraphMsg{dropCollection: true}

		node := &insertBufferNode{
			channelName:      s.channel.channelName,
			channel:          s.channel,
			delBufferManager: s.delBufManager,
			flushChan:        make(chan flushMsg, 100),
		}

		syncTasks := node.FillInSyncTasks(fgMsg, nil)
		s.Assert().NotEmpty(syncTasks)
		s.Assert().Equal(3, len(syncTasks))
		for _, task := range syncTasks {
			s.Assert().True(task.dropped)
			s.Assert().False(task.flushed)
			s.Assert().Nil(task.buffer)
		}
	})

	s.Run("auto sync", func() {
		segToFlush := []UniqueID{1, 2}

		node := &insertBufferNode{
			channelName:      s.channel.channelName,
			channel:          s.channel,
			delBufferManager: s.delBufManager,
			flushChan:        make(chan flushMsg, 100),
		}

		buffer := BufferData{
			buffer: nil,
			size:   2,
			limit:  2,
		}
		node.channel.setCurInsertBuffer(UniqueID(1), &buffer)

		syncTasks := node.FillInSyncTasks(&flowGraphMsg{endPositions: []*msgpb.MsgPosition{{Timestamp: 100}}}, segToFlush)
		s.Assert().NotEmpty(syncTasks)
		s.Assert().Equal(1, len(syncTasks))

		task, ok := syncTasks[UniqueID(1)]
		s.Assert().True(ok)
		s.Assert().Equal(UniqueID(1), task.segmentID)
		s.Assert().True(task.auto)
		s.Assert().False(task.flushed)
		s.Assert().False(task.dropped)
	})

	s.Run("drop partition", func() {
		fgMsg := flowGraphMsg{dropPartitions: []UniqueID{s.partID}, endPositions: []*msgpb.MsgPosition{{Timestamp: 100}}}
		node := &insertBufferNode{
			channelName:      s.channel.channelName,
			channel:          s.channel,
			delBufferManager: s.delBufManager,
			flushChan:        make(chan flushMsg, 100),
		}

		syncTasks := node.FillInSyncTasks(&fgMsg, nil)
		s.Assert().NotEmpty(syncTasks)
		s.Assert().Equal(3, len(syncTasks))

		for _, task := range syncTasks {
			s.Assert().False(task.auto)
			s.Assert().True(task.flushed)
			s.Assert().True(task.dropped)
		}
	})

	s.Run("manual sync", func() {
		flushCh := make(chan flushMsg, 100)
		node := &insertBufferNode{
			channelName:      s.channel.channelName,
			channel:          s.channel,
			delBufferManager: s.delBufManager,
			flushChan:        flushCh,
		}

		for i := 1; i <= 3; i++ {
			msg := flushMsg{
				segmentID:    UniqueID(i),
				collectionID: s.collID,
			}

			flushCh <- msg
		}

		syncTasks := node.FillInSyncTasks(&flowGraphMsg{endPositions: []*msgpb.MsgPosition{{Timestamp: 100}}}, nil)
		s.Assert().NotEmpty(syncTasks)

		for _, task := range syncTasks {
			s.Assert().True(task.flushed)
			s.Assert().False(task.auto)
			s.Assert().False(task.dropped)
		}
	})

	s.Run("manual sync over load", func() {
		flushCh := make(chan flushMsg, 100)
		node := &insertBufferNode{
			channelName:      s.channel.channelName,
			channel:          s.channel,
			delBufferManager: s.delBufManager,
			flushChan:        flushCh,
		}

		for i := 1; i <= 100; i++ {
			msg := flushMsg{
				segmentID:    UniqueID(i),
				collectionID: s.collID,
			}

			flushCh <- msg
		}

		syncTasks := node.FillInSyncTasks(&flowGraphMsg{endPositions: []*msgpb.MsgPosition{{Timestamp: 100}}}, nil)
		s.Assert().NotEmpty(syncTasks)
		s.Assert().Equal(10, len(syncTasks)) // 10 is max batch

		for _, task := range syncTasks {
			s.Assert().True(task.flushed)
			s.Assert().False(task.auto)
			s.Assert().False(task.dropped)
		}
	})

	s.Run("test close", func() {
		fgMsg := &flowGraphMsg{BaseMsg: flowgraph.NewBaseMsg(true)}

		node := &insertBufferNode{
			channelName:      s.channel.channelName,
			channel:          s.channel,
			delBufferManager: s.delBufManager,
			flushChan:        make(chan flushMsg, 100),
		}

		syncTasks := node.FillInSyncTasks(fgMsg, nil)
		s.Assert().Equal(1, len(syncTasks))
		for _, task := range syncTasks {
			s.Assert().Equal(task.segmentID, int64(1))
			s.Assert().False(task.dropped)
			s.Assert().False(task.flushed)
			s.Assert().True(task.auto)
		}
	})

}

func TestInsertBufferNodeSuite(t *testing.T) {
	suite.Run(t, new(InsertBufferNodeSuite))
}

// CompactedRootCoord has meta info compacted at ts
type CompactedRootCoord struct {
	types.RootCoord
	compactTs Timestamp
}

func (m *CompactedRootCoord) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if in.TimeStamp != 0 && in.GetTimeStamp() <= m.compactTs {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "meta compacted",
			},
		}, nil
	}
	return m.RootCoord.DescribeCollection(ctx, in)
}

func TestInsertBufferNode_bufferInsertMsg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-operate"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.BaseTable.Save("etcd.rootPath", "/test/datanode/root")

	Factory := &MetaFactory{}
	tests := []struct {
		collID      UniqueID
		pkType      schemapb.DataType
		description string
	}{
		{0, schemapb.DataType_Int64, "int64PrimaryData"},
		{0, schemapb.DataType_VarChar, "varCharPrimaryData"},
	}

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	for _, test := range tests {
		collMeta := Factory.GetCollectionMeta(test.collID, "collection", test.pkType)
		rcf := &RootCoordFactory{
			pkType: test.pkType,
		}
		mockRootCoord := &CompactedRootCoord{
			RootCoord: rcf,
			compactTs: 100,
		}

		channel := newChannel(insertChannelName, collMeta.ID, collMeta.Schema, mockRootCoord, cm)
		err = channel.addSegment(
			addSegmentReq{
				segType:     datapb.SegmentType_New,
				segID:       1,
				collID:      collMeta.ID,
				partitionID: 0,
				startPos:    &msgpb.MsgPosition{},
				endPos:      &msgpb.MsgPosition{Timestamp: 101},
			})
		require.NoError(t, err)

		factory := dependency.NewDefaultFactory(true)

		alloc := allocator.NewMockAllocator(t)
		fm := NewRendezvousFlushManager(alloc, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

		flushChan := make(chan flushMsg, 100)
		resendTTChan := make(chan resendTTMsg, 100)
		c := &nodeConfig{
			channel:      channel,
			msFactory:    factory,
			allocator:    alloc,
			vChannelName: "string",
		}
		delBufManager := &DeltaBufferManager{
			channel:    channel,
			delBufHeap: &PriorityQueue{},
		}

		dataCoord := &DataCoordFactory{}
		atimeTickSender := newTimeTickSender(dataCoord, 0)
		iBNode, err := newInsertBufferNode(ctx, collMeta.ID, delBufManager, flushChan, resendTTChan, fm, newCache(), c, atimeTickSender)
		require.NoError(t, err)

		inMsg := genFlowGraphInsertMsg(insertChannelName)
		for _, msg := range inMsg.insertMessages {
			msg.EndTimestamp = 101 // ts valid
			err = iBNode.bufferInsertMsg(msg, &msgpb.MsgPosition{}, &msgpb.MsgPosition{})
			assert.NoError(t, err)
		}

		for _, msg := range inMsg.insertMessages {
			msg.EndTimestamp = 101 // ts valid
			msg.RowIDs = []int64{} //misaligned data
			err = iBNode.bufferInsertMsg(msg, &msgpb.MsgPosition{}, &msgpb.MsgPosition{})
			assert.Error(t, err)
		}
	}
}

func TestInsertBufferNode_updateSegmentStates(te *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	invalideTests := []struct {
		channelCollID UniqueID

		inCollID    UniqueID
		segID       UniqueID
		description string
	}{
		{1, 9, 100, "collectionID mismatch"},
	}

	for _, test := range invalideTests {
		channel := newChannel("channel", test.channelCollID, nil, &RootCoordFactory{pkType: schemapb.DataType_Int64}, cm)

		ibNode := &insertBufferNode{
			channel: channel,
		}

		im := []*msgstream.InsertMsg{
			{
				InsertRequest: msgpb.InsertRequest{
					CollectionID: test.inCollID,
					SegmentID:    test.segID,
				},
			},
		}

		seg, err := ibNode.addSegmentAndUpdateRowNum(im, &msgpb.MsgPosition{}, &msgpb.MsgPosition{})

		assert.Error(te, err)
		assert.Empty(te, seg)
	}
}

func TestInsertBufferNode_getTimestampRange(t *testing.T) {

	type testCase struct {
		tag string

		timestamps []int64
		expectFrom Timestamp
		expectTo   Timestamp
	}

	cases := []testCase{
		{
			tag:        "no input",
			timestamps: []int64{},
			expectFrom: math.MaxUint64,
			expectTo:   0,
		},
		{
			tag:        "only one input",
			timestamps: []int64{1234},
			expectFrom: 1234,
			expectTo:   1234,
		},
		{
			tag:        "input reverse order",
			timestamps: []int64{3, 2, 1},
			expectFrom: 1,
			expectTo:   3,
		},
	}

	ibNode := &insertBufferNode{}
	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			tr := ibNode.getTimestampRange(&storage.Int64FieldData{
				Data: tc.timestamps,
			})

			assert.Equal(t, tc.expectFrom, tr.timestampMin)
			assert.Equal(t, tc.expectTo, tr.timestampMax)
		})
	}
}

func TestInsertBufferNode_collectSegmentsToSync(t *testing.T) {
	tests := []struct {
		description    string
		inFlushMsgNum  int
		expectedOutNum int
	}{
		{"batch 1 < maxBatch 10", 1, 1},
		{"batch 5 < maxBatch 10", 5, 5},
		{"batch 10 = maxBatch 10", 10, 10},
		{"batch 20 = maxBatch 10", 20, 10},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			flushCh := make(chan flushMsg, 100)
			node := &insertBufferNode{
				flushChan:   flushCh,
				channelName: "channel" + test.description,
			}

			for i := 0; i < test.inFlushMsgNum; i++ {
				flushCh <- flushMsg{segmentID: UniqueID(i)}
			}

			flushedSegs := node.CollectSegmentsToSync()
			assert.Equal(t, test.expectedOutNum, len(flushedSegs))
		})
	}
}

func TestInsertBufferNode_task_pool_is_full(t *testing.T) {
	ctx := context.Background()
	flushCh := make(chan flushMsg, 100)

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer func() {
		err := cm.RemoveWithPrefix(ctx, cm.RootPath())
		assert.NoError(t, err)
	}()

	channelName := "test_task_pool_is_full_mock_ch1"
	collection := UniqueID(0)
	segmentID := UniqueID(100)

	channel := newChannel(channelName, collection, nil, &RootCoordFactory{pkType: schemapb.DataType_Int64}, cm)
	err := channel.addSegment(addSegmentReq{
		segType:  datapb.SegmentType_New,
		segID:    segmentID,
		collID:   collection,
		startPos: new(msgpb.MsgPosition),
		endPos:   new(msgpb.MsgPosition),
	})
	assert.NoError(t, err)
	channel.setCurInsertBuffer(segmentID, &BufferData{size: 100})

	fManager := &mockFlushManager{
		full: true,
	}

	dManager := &DeltaBufferManager{
		channel:    channel,
		delBufHeap: &PriorityQueue{},
	}

	node := &insertBufferNode{
		flushChan:        flushCh,
		channelName:      channelName,
		channel:          channel,
		flushManager:     fManager,
		delBufferManager: dManager,
	}
	inMsg := genFlowGraphInsertMsg(channelName)
	inMsg.BaseMsg = flowgraph.NewBaseMsg(true) // trigger sync task

	segmentsToSync := node.Sync(&inMsg, []UniqueID{segmentID}, nil)
	assert.Len(t, segmentsToSync, 0)

	fManager.full = false
	segment := channel.getSegment(segmentID)
	segment.setSyncing(true)
	segmentsToSync = node.Sync(&inMsg, []UniqueID{segmentID}, nil)
	assert.Len(t, segmentsToSync, 0)
}
