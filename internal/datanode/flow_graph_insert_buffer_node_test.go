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
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/retry"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/flowgraph"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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
	defer cm.RemoveWithPrefix("")
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

	replica, err := newReplica(ctx, mockRootCoord, cm, collMeta.ID)
	assert.Nil(t, err)

	err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)

	factory := dependency.NewDefaultFactory(true)

	fm := NewRendezvousFlushManager(&allocator{}, cm, replica, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)

	c := &nodeConfig{
		replica:      replica,
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
	defer cm.RemoveWithPrefix("")
	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.EtcdCfg.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	replica, err := newReplica(ctx, mockRootCoord, cm, collMeta.ID)
	assert.Nil(t, err)

	err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)

	factory := dependency.NewDefaultFactory(true)

	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, replica, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	c := &nodeConfig{
		replica:      replica,
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

	// test updateSegStatesInReplica failed
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
	flowGraphRetryOpt = retry.Attempts(1)
	inMsg = genFlowGraphInsertMsg(insertChannelName)
	iBNode.flushManager = &mockFlushManager{returnError: true}
	iBNode.insertBuffer.Store(inMsg.insertMessages[0].SegmentID, &BufferData{})
	assert.Panics(t, func() { iBNode.Operate([]flowgraph.Msg{&inMsg}) })
	iBNode.flushManager = fm
}

/*
func TestFlushSegment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	idAllocMock := NewAllocatorFactory(1)
	mockMinIO := memkv.NewMemoryKV()
	insertChannelName := "datanode-02-test-flushsegment"

	segmentID, _ := idAllocMock.allocID()
	partitionID, _ := idAllocMock.allocID()
	collectionID, _ := idAllocMock.allocID()
	fmt.Printf("generate segmentID, partitionID, collectionID: %v, %v, %v\n",
		segmentID, partitionID, collectionID)

	collMeta := genCollectionMeta(collectionID, "test_flush_segment_txn")
	flushMap := sync.Map{}
	mockRootCoord := &RootCoordFactory{}

	replica, err := newReplica(ctx, mockRootCoord, collMeta.ID)
	assert.Nil(t, err)

	err = replica.addNewSegment(segmentID, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)
	replica.updateSegmentEndPosition(segmentID, &internalpb.MsgPosition{ChannelName: "TestChannel"})

	finishCh := make(chan segmentFlushUnit, 1)

	insertData := &InsertData{
		Data: make(map[storage.FieldID]storage.FieldData),
	}
	insertData.Data[0] = &storage.Int64FieldData{
		NumRows: []int64{10},
		Data:    []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	insertData.Data[1] = &storage.Int64FieldData{
		NumRows: []int64{10},
		Data:    []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	insertData.Data[107] = &storage.FloatFieldData{
		NumRows: []int64{10},
		Data:    make([]float32, 10),
	}
	flushMap.Store(segmentID, insertData)

	factory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = factory.SetParams(m)
	assert.Nil(t, err)
	flushChan := make(chan flushMsg, 100)

	memkv := memkv.NewMemoryKV()

	fm := NewRendezvousFlushManager(&allocator{}, memkv, replica, func(*segmentFlushPack) error {
		return nil
	})

	c := &nodeConfig{
		replica:      replica,
		factory:    factory,
		allocator:    NewAllocatorFactory(),
		vChannelName: "string",
	}
	ibNode, err := newInsertBufferNode(ctx, flushChan, fm, newCache(), c)
	require.NoError(t, err)

	flushSegment(collMeta,
		segmentID,
		partitionID,
		collectionID,
		&flushMap,
		mockMinIO,
		finishCh,
		nil,
		ibNode,
		idAllocMock)

	fu := <-finishCh
	assert.NotNil(t, fu.field2Path)
	assert.Equal(t, fu.segID, segmentID)

    k := JoinIDPath(collectionID, partitionID, segmentID, 0)
	key := path.Join(Params.StatsBinlogRootPath, k)
	_, values, _ := mockMinIO.LoadWithPrefix(key)
	assert.Equal(t, len(values), 1)
}*/

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

	colRep := &SegmentReplica{
		collectionID:    collMeta.ID,
		newSegments:     make(map[UniqueID]*Segment),
		normalSegments:  make(map[UniqueID]*Segment),
		flushedSegments: make(map[UniqueID]*Segment),
	}

	colRep.metaService = newMetaService(mockRootCoord, collMeta.ID)

	factory := dependency.NewDefaultFactory(true)

	flushPacks := []*segmentFlushPack{}
	fpMut := sync.Mutex{}
	wg := sync.WaitGroup{}

	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix("")
	fm := NewRendezvousFlushManager(NewAllocatorFactory(), cm, colRep, func(pack *segmentFlushPack) {
		fpMut.Lock()
		flushPacks = append(flushPacks, pack)
		fpMut.Unlock()
		colRep.listNewSegmentsStartPositions()
		colRep.listSegmentsCheckPoints()
		if pack.flushed || pack.dropped {
			colRep.segmentFlushed(pack.segmentID)
		}
		wg.Done()
	}, emptyFlushAndDropFunc)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	c := &nodeConfig{
		replica:      colRep,
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

		require.Equal(t, 2, len(colRep.newSegments))
		require.Equal(t, 0, len(colRep.normalSegments))
		assert.Equal(t, 0, len(flushPacks))

		for i, test := range beforeAutoFlushTests {
			colRep.segMu.Lock()
			seg, ok := colRep.newSegments[UniqueID(i+1)]
			colRep.segMu.Unlock()
			assert.True(t, ok)
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
			assert.Equal(t, test.expectedCpNumOfRows, seg.checkPoint.numRows)
			assert.Equal(t, test.expectedCpPosTs, seg.checkPoint.pos.GetTimestamp())
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
		require.Equal(t, 0, len(colRep.newSegments))
		require.Equal(t, 3, len(colRep.normalSegments))

		assert.Equal(t, 1, len(flushPacks))
		//		assert.Equal(t, 3, len(flushUnit[0].checkPoint))
		assert.Less(t, 0, len(flushPacks[0].insertLogs))
		assert.False(t, flushPacks[0].flushed)

		afterAutoFlushTests := []Test{
			// segID, numOfRow, startTs, endTs, cp.numOfRow, cp.Ts
			{1, 1, 100, 123, 0, 100},
			{2, 2, 100, 234, 2, 234},
			{3, 1, 200, 234, 0, 200},
		}

		for i, test := range afterAutoFlushTests {
			seg, ok := colRep.normalSegments[UniqueID(i+1)]
			assert.True(t, ok)
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
			assert.Equal(t, test.expectedCpNumOfRows, seg.checkPoint.numRows)
			assert.Equal(t, test.expectedCpPosTs, seg.checkPoint.pos.GetTimestamp())

			//			assert.Equal(t, test.expectedCpNumOfRows, flushPacks[0].checkPoint[UniqueID(i+1)].numRows)
			//		assert.Equal(t, test.expectedCpPosTs, flushPacks[0].checkPoint[UniqueID(i+1)].pos.Timestamp)

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

		require.Equal(t, 2, len(colRep.newSegments))
		require.Equal(t, 3, len(colRep.normalSegments))
		assert.Equal(t, 0, len(flushPacks))

		for _, test := range beforeAutoFlushTests {
			colRep.segMu.Lock()
			seg, ok := colRep.newSegments[test.expectedSegID]
			colRep.segMu.Unlock()
			assert.True(t, ok)
			assert.Equal(t, test.expectedSegID, seg.segmentID)
			assert.Equal(t, test.expectedNumOfRows, seg.numRows)
			assert.Equal(t, test.expectedStartPosTs, seg.startPos.GetTimestamp())
			assert.Equal(t, test.expectedCpNumOfRows, seg.checkPoint.numRows)
			assert.Equal(t, test.expectedCpPosTs, seg.checkPoint.pos.GetTimestamp())
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
		require.Equal(t, 0, len(colRep.newSegments))
		require.Equal(t, 4, len(colRep.normalSegments))
		require.Equal(t, 1, len(colRep.flushedSegments))

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
	defer cm.RemoveWithPrefix("")
	for _, test := range tests {
		collMeta := Factory.GetCollectionMeta(test.collID, "collection", test.pkType)
		rcf := &RootCoordFactory{
			pkType: test.pkType,
		}
		mockRootCoord := &CompactedRootCoord{
			RootCoord: rcf,
			compactTs: 100,
		}

		replica, err := newReplica(ctx, mockRootCoord, cm, collMeta.ID)
		assert.Nil(t, err)

		err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{Timestamp: 101})
		require.NoError(t, err)

		factory := dependency.NewDefaultFactory(true)

		fm := NewRendezvousFlushManager(&allocator{}, cm, replica, func(*segmentFlushPack) {}, emptyFlushAndDropFunc)

		flushChan := make(chan flushMsg, 100)
		resendTTChan := make(chan resendTTMsg, 100)
		c := &nodeConfig{
			replica:      replica,
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

func TestInsertBufferNode_updateSegStatesInReplica(te *testing.T) {
	cm := storage.NewLocalChunkManager(storage.RootPath(insertNodeTestDir))
	defer cm.RemoveWithPrefix("")
	invalideTests := []struct {
		replicaCollID UniqueID

		inCollID    UniqueID
		segID       UniqueID
		description string
	}{
		{1, 9, 100, "collectionID mismatch"},
	}

	for _, test := range invalideTests {
		replica, err := newReplica(context.Background(), &RootCoordFactory{pkType: schemapb.DataType_Int64}, cm, test.replicaCollID)
		assert.Nil(te, err)

		ibNode := &insertBufferNode{
			replica: replica,
		}

		im := []*msgstream.InsertMsg{
			{
				InsertRequest: internalpb.InsertRequest{
					CollectionID: test.inCollID,
					SegmentID:    test.segID,
				},
			},
		}

		seg, err := ibNode.updateSegStatesInReplica(im, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})

		assert.Error(te, err)
		assert.Empty(te, seg)

	}

}

func TestInsertBufferNode_BufferData(te *testing.T) {
	Params.DataNodeCfg.FlushInsertBufferSize = 16 * (1 << 20) // 16 MB

	tests := []struct {
		isValid bool

		indim         int64
		expectedLimit int64

		description string
	}{
		{true, 1, 4194304, "Smallest of the DIM"},
		{true, 128, 32768, "Normal DIM"},
		{true, 32768, 128, "Largest DIM"},
		{false, 0, 0, "Illegal DIM"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			idata, err := newBufferData(test.indim)

			if test.isValid {
				assert.NoError(t, err)
				assert.NotNil(t, idata)

				assert.Equal(t, test.expectedLimit, idata.limit)
				assert.Zero(t, idata.size)

				capacity := idata.effectiveCap()
				assert.Equal(t, test.expectedLimit, capacity)
			} else {
				assert.Error(t, err)
				assert.Nil(t, idata)
			}
		})

	}
}
