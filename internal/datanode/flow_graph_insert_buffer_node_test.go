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
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	defer cm.RemoveWithPrefix(ctx, "")
	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-create"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.EtcdCfg.MetaRootPath = testPath

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
			startPos:    &internalpb.MsgPosition{},
			endPos:      &internalpb.MsgPosition{},
		})
	require.NoError(t, err)

	factory := dependency.NewDefaultFactory(true)

	fm := NewRendezvousFlushManager(&allocator{}, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)

	c := &nodeConfig{
		channel:      channel,
		msFactory:    factory,
		allocator:    NewAllocatorFactory(),
		vChannelName: "string",
	}

	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, flushChan, resendTTChan, fm, newCache(), c)
	assert.NotNil(t, iBNode)
	require.NoError(t, err)

	/*ctxDone, cancel := context.WithCancel(ctx)
	cancel() // cancel now to make context done
	_, err = newInsertBufferNode(ctxDone, flushChan, fm, newCache(), c)
	assert.Error(t, err)*/

	c.msFactory = &CDFMsFactory{
		Factory: factory,
		cd:      0,
	}

	_, err = newInsertBufferNode(ctx, collMeta.ID, flushChan, resendTTChan, fm, newCache(), c)
	assert.Error(t, err)
}

type mockMsg struct{}

func (*mockMsg) TimeTick() Timestamp {
	return 0
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
				ibn := &insertBufferNode{
					ttMerger: newMergedTimeTickerSender(func(Timestamp, []int64) error { return nil }),
				}
				rt := ibn.Operate(test.in)
				assert.Empty(t0, rt)
			})
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-operate"

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, "")
	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.EtcdCfg.MetaRootPath = testPath

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
			startPos:    &internalpb.MsgPosition{},
			endPos:      &internalpb.MsgPosition{},
		})
	require.NoError(t, err)

	factory := dependency.NewDefaultFactory(true)

	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	c := &nodeConfig{
		channel:      channel,
		msFactory:    factory,
		allocator:    NewAllocatorFactory(),
		vChannelName: "string",
	}

	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, flushChan, resendTTChan, fm, newCache(), c)
	require.NoError(t, err)

	// trigger log ts
	iBNode.ttLogger.counter.Store(999)

	flushChan <- flushMsg{
		msgID:        1,
		timestamp:    2000,
		segmentID:    UniqueID(1),
		collectionID: UniqueID(1),
	}

	inMsg := genFlowGraphInsertMsg(insertChannelName)
	assert.NotPanics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })

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

	// test updateSegmentStates failed
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
	setFlowGraphRetryOpt(retry.Attempts(1))
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	iBNode.flushManager = &mockFlushManager{returnError: true}
	iBNode.insertBuffer.Store(inMsg.insertMessages[0].SegmentID, &BufferData{})
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
	Params.EtcdCfg.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	dataFactory := NewDataFactory()

	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	channel := &ChannelMeta{
		collectionID: collMeta.ID,
		segments:     make(map[UniqueID]*Segment),
	}

	channel.metaService = newMetaService(mockRootCoord, collMeta.ID)

	factory := dependency.NewDefaultFactory(true)

	flushPacks := []*segmentFlushPack{}
	fpMut := sync.Mutex{}
	wg := sync.WaitGroup{}

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, "")
	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, channel, func(pack *segmentFlushPack) {
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
		allocator:    NewAllocatorFactory(),
		vChannelName: "string",
	}
	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, flushChan, resendTTChan, fm, newCache(), c)
	require.NoError(t, err)

	// Auto flush number of rows set to 2

	inMsg := genFlowGraphInsertMsg("datanode-03-test-autoflush")
	inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(2)
	var iMsg flowgraph.Msg = &inMsg

	t.Run("Pure auto flush", func(t *testing.T) {
		// iBNode.insertBuffer.maxSize = 2
		tmp := Params.DataNodeCfg.FlushInsertBufferSize
		Params.DataNodeCfg.FlushInsertBufferSize = 4 * 4
		defer func() {
			Params.DataNodeCfg.FlushInsertBufferSize = tmp
		}()

		for i := range inMsg.insertMessages {
			inMsg.insertMessages[i].SegmentID = int64(i%2) + 1
		}
		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 100}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 123}}

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
		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 200}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 234}}
		iMsg = &inMsg

		// Triger auto flush
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToFlush))
		t.Log("segments to flush", fgm.segmentsToFlush)

		for _, im := range fgm.segmentsToFlush {
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
		Params.DataNodeCfg.FlushInsertBufferSize = 4 * 4
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
		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 300}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 323}}
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

		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 400}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 434}}

		// trigger manual flush
		flushChan <- flushMsg{
			segmentID: 10,
			flushed:   true,
		}

		// trigger auto flush since buffer full
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToFlush))
		for _, im := range fgm.segmentsToFlush {
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

func TestFlowGraphInsertBufferNode_DropPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	partitionID := int64(1)

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.EtcdCfg.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	dataFactory := NewDataFactory()

	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	channel := &ChannelMeta{
		collectionID: collMeta.ID,
		segments:     make(map[UniqueID]*Segment),
	}

	channel.metaService = newMetaService(mockRootCoord, collMeta.ID)

	factory := dependency.NewDefaultFactory(true)

	flushPacks := []*segmentFlushPack{}
	fpMut := sync.Mutex{}
	wg := sync.WaitGroup{}

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, "")
	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, channel, func(pack *segmentFlushPack) {
		fpMut.Lock()
		flushPacks = append(flushPacks, pack)
		fpMut.Unlock()
		channel.listNewSegmentsStartPositions()
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
		allocator:    NewAllocatorFactory(),
		vChannelName: "string",
	}
	iBNode, err := newInsertBufferNode(ctx, collMeta.ID, flushChan, resendTTChan, fm, newCache(), c)
	require.NoError(t, err)

	// Auto flush number of rows set to 2

	inMsg := genFlowGraphInsertMsg("datanode-03-test-autoflush")
	inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(2)
	var iMsg flowgraph.Msg = &inMsg

	t.Run("Only drop partition", func(t *testing.T) {
		// iBNode.insertBuffer.maxSize = 2
		tmp := Params.DataNodeCfg.FlushInsertBufferSize
		Params.DataNodeCfg.FlushInsertBufferSize = 4 * 4
		defer func() {
			Params.DataNodeCfg.FlushInsertBufferSize = tmp
		}()

		for i := range inMsg.insertMessages {
			inMsg.insertMessages[i].SegmentID = int64(i%2) + 1
			inMsg.insertMessages[i].PartitionID = partitionID
		}
		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 100}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 123}}

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
			assert.Equal(t, partitionID, seg.partitionID)
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
		}

		inMsg.insertMessages = nil
		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 200}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 234}}
		inMsg.dropPartitions = []int64{partitionID}
		iMsg = &inMsg

		// Triger drop paritition
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToFlush))
		t.Log("segments to flush", fgm.segmentsToFlush)

		for _, im := range fgm.segmentsToFlush {
			// send del done signal
			err = fm.flushDelData(nil, im, fgm.endPositions[0])
			assert.NoError(t, err)
		}
		wg.Wait()

		assert.Equal(t, 2, len(flushPacks))
		assert.Less(t, 0, len(flushPacks[0].insertLogs))
		for _, flushPack := range flushPacks {
			assert.True(t, flushPack.flushed)
			assert.True(t, flushPack.dropped)
		}

	})
	t.Run("drop partition with flush", func(t *testing.T) {

		tmp := Params.DataNodeCfg.FlushInsertBufferSize
		Params.DataNodeCfg.FlushInsertBufferSize = 4 * 4
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
			inMsg.insertMessages[i].PartitionID = partitionID
		}
		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 300}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 323}}
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
			assert.Equal(t, partitionID, seg.partitionID)
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
		}

		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 400}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 434}}
		inMsg.dropPartitions = []int64{partitionID}

		// trigger manual flush
		flushChan <- flushMsg{
			segmentID: 10,
			flushed:   true,
		}

		// trigger auto flush since buffer full
		output := iBNode.Operate([]flowgraph.Msg{iMsg})
		fgm := output[0].(*flowGraphMsg)
		wg.Add(len(fgm.segmentsToFlush))
		for _, im := range fgm.segmentsToFlush {
			// send del done signal
			err = fm.flushDelData(nil, im, fgm.endPositions[0])
			assert.NoError(t, err)
		}
		wg.Wait()

		assert.Equal(t, 4, len(flushPacks))
		for _, pack := range flushPacks {
			assert.True(t, pack.flushed)
			assert.True(t, pack.dropped)
		}
	})

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
	Params.EtcdCfg.MetaRootPath = testPath

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
	defer cm.RemoveWithPrefix(ctx, "")
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
				startPos:    &internalpb.MsgPosition{},
				endPos:      &internalpb.MsgPosition{Timestamp: 101},
			})
		require.NoError(t, err)

		factory := dependency.NewDefaultFactory(true)

		fm := NewRendezvousFlushManager(&allocator{}, cm, channel, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

		flushChan := make(chan flushMsg, 100)
		resendTTChan := make(chan resendTTMsg, 100)
		c := &nodeConfig{
			channel:      channel,
			msFactory:    factory,
			allocator:    NewAllocatorFactory(),
			vChannelName: "string",
		}
		iBNode, err := newInsertBufferNode(ctx, collMeta.ID, flushChan, resendTTChan, fm, newCache(), c)
		require.NoError(t, err)

		inMsg := genFlowGraphInsertMsg(insertChannelName)
		for _, msg := range inMsg.insertMessages {
			msg.EndTimestamp = 101 // ts valid
			err = iBNode.bufferInsertMsg(msg, &internalpb.MsgPosition{})
			assert.Nil(t, err)
		}

		for _, msg := range inMsg.insertMessages {
			msg.EndTimestamp = 101 // ts valid
			msg.RowIDs = []int64{} //misaligned data
			err = iBNode.bufferInsertMsg(msg, &internalpb.MsgPosition{})
			assert.NotNil(t, err)
		}
	}
}

func TestInsertBufferNode_updateSegmentStates(te *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, "")
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
				InsertRequest: internalpb.InsertRequest{
					CollectionID: test.inCollID,
					SegmentID:    test.segID,
				},
			},
		}

		seg, err := ibNode.updateSegmentStates(im, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})

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
