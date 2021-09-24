// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/flowgraph"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

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

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-create"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")
	mockRootCoord := &RootCoordFactory{}

	replica := newReplica(mockRootCoord, collMeta.ID)

	err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	saveBinlog := func(fu *segmentFlushUnit) error {
		t.Log(fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode, err := newInsertBufferNode(ctx, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	assert.NotNil(t, iBNode)
	require.NoError(t, err)

	ctxDone, cancel := context.WithCancel(ctx)
	cancel() // cancel now to make context done
	_, err = newInsertBufferNode(ctxDone, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	assert.Error(t, err)

	cdf := &CDFMsFactory{
		Factory: msFactory,
		cd:      0,
	}

	_, err = newInsertBufferNode(ctx, replica, cdf, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	assert.Error(t, err)
	cdf = &CDFMsFactory{
		Factory: msFactory,
		cd:      1,
	}
	_, err = newInsertBufferNode(ctx, replica, cdf, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
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
				ibn := &insertBufferNode{}
				rt := ibn.Operate(test.in)
				assert.Empty(t0, rt)
			})
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-operate"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")
	mockRootCoord := &RootCoordFactory{}

	replica := newReplica(mockRootCoord, collMeta.ID)

	err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	saveBinlog := func(fu *segmentFlushUnit) error {
		t.Log(fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode, err := newInsertBufferNode(ctx, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	require.NoError(t, err)

	flushChan <- &flushMsg{
		msgID:        1,
		timestamp:    2000,
		segmentID:    UniqueID(1),
		collectionID: UniqueID(1),
	}

	inMsg := genFlowGraphMsg(insertChannelName)
	var fgMsg flowgraph.Msg = &inMsg
	iBNode.Operate([]flowgraph.Msg{fgMsg})
}

func genFlowGraphMsg(insertChannelName string) flowGraphMsg {

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*internalpb.MsgPosition{
		{
			ChannelName: insertChannelName,
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	var iMsg = &flowGraphMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		timeRange: TimeRange{
			timestampMin: timeRange.timestampMin,
			timestampMax: timeRange.timestampMax,
		},
		startPositions: startPos,
		endPositions:   startPos,
	}

	dataFactory := NewDataFactory()
	iMsg.insertMessages = append(iMsg.insertMessages, dataFactory.GetMsgStreamInsertMsgs(2)...)

	return *iMsg

}

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

	replica := newReplica(mockRootCoord, collMeta.ID)

	err := replica.addNewSegment(segmentID, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
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

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)
	flushChan := make(chan *flushMsg, 100)
	saveBinlog := func(*segmentFlushUnit) error {
		return nil
	}
	ibNode, err := newInsertBufferNode(ctx, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
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

	k, _ := idAllocMock.genKey(false, collectionID, partitionID, segmentID, 0)
	key := path.Join(Params.StatsBinlogRootPath, k)
	_, values, _ := mockMinIO.LoadWithPrefix(key)
	assert.Equal(t, len(values), 1)
	assert.Equal(t, values[0], `{"max":9,"min":0}`)
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
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")
	dataFactory := NewDataFactory()

	mockRootCoord := &RootCoordFactory{}

	colRep := &SegmentReplica{
		collectionID:    collMeta.ID,
		newSegments:     make(map[UniqueID]*Segment),
		normalSegments:  make(map[UniqueID]*Segment),
		flushedSegments: make(map[UniqueID]*Segment),
	}

	colRep.metaService = newMetaService(mockRootCoord, collMeta.ID)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	flushUnit := []segmentFlushUnit{}
	saveBinlog := func(fu *segmentFlushUnit) error {
		flushUnit = append(flushUnit, *fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode, err := newInsertBufferNode(ctx, colRep, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	require.NoError(t, err)

	// Auto flush number of rows set to 2

	inMsg := genFlowGraphMsg("datanode-03-test-autoflush")
	inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(2)
	var iMsg flowgraph.Msg = &inMsg

	t.Run("Pure auto flush", func(t *testing.T) {
		// iBNode.insertBuffer.maxSize = 2
		tmp := Params.FlushInsertBufferSize
		Params.FlushInsertBufferSize = 4 * 4
		defer func() {
			Params.FlushInsertBufferSize = tmp
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
		assert.Equal(t, 0, len(flushUnit))

		for i, test := range beforeAutoFlushTests {
			seg, ok := colRep.newSegments[UniqueID(i+1)]
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
		iBNode.Operate([]flowgraph.Msg{iMsg})
		require.Equal(t, 0, len(colRep.newSegments))
		require.Equal(t, 3, len(colRep.normalSegments))

		assert.Equal(t, 1, len(flushUnit))
		assert.Equal(t, 3, len(flushUnit[0].checkPoint))
		assert.Less(t, 0, len(flushUnit[0].field2Path))
		assert.False(t, flushUnit[0].flushed)

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

			assert.Equal(t, test.expectedCpNumOfRows, flushUnit[0].checkPoint[UniqueID(i+1)].numRows)
			assert.Equal(t, test.expectedCpPosTs, flushUnit[0].checkPoint[UniqueID(i+1)].pos.Timestamp)

			if i == 1 {
				assert.Equal(t, test.expectedSegID, flushUnit[0].segID)
				// assert.Equal(t, int64(0), iBNode.insertBuffer.size(UniqueID(i+1)))
			}
			// else {
			//     // assert.Equal(t, int64(1), iBNode.insertBuffer.size(UniqueID(i+1)))
			// }
		}

	})

	t.Run("Auto with manul flush", func(t *testing.T) {
		t.Skipf("Skip, fix later")
		for i := range inMsg.insertMessages {
			inMsg.insertMessages[i].SegmentID = 1
		}

		inMsg.startPositions = []*internalpb.MsgPosition{{Timestamp: 234}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 345}}
		iBNode.Operate([]flowgraph.Msg{iMsg})

		assert.Equal(t, len(flushUnit), 2)
		assert.Equal(t, flushUnit[1].segID, int64(1))
		assert.Equal(t, len(flushUnit[1].checkPoint), 3)
		assert.Equal(t, flushUnit[1].checkPoint[1].numRows, int64(50+16000+100+32000))
		assert.Equal(t, flushUnit[1].checkPoint[2].numRows, int64(100+32000))
		assert.Equal(t, flushUnit[1].checkPoint[3].numRows, int64(0))
		assert.Equal(t, flushUnit[1].checkPoint[1].pos.Timestamp, Timestamp(345))
		assert.Equal(t, flushUnit[1].checkPoint[2].pos.Timestamp, Timestamp(234))
		assert.Equal(t, flushUnit[1].checkPoint[3].pos.Timestamp, Timestamp(123))
		assert.False(t, flushUnit[1].flushed)
		assert.Greater(t, len(flushUnit[1].field2Path), 0)
		// assert.Equal(t, len(iBNode.insertBuffer.insertData), 1)
		// assert.Equal(t, iBNode.insertBuffer.size(3), int32(50+16000))

		flushChan <- &flushMsg{
			msgID:        3,
			timestamp:    456,
			segmentID:    UniqueID(1),
			collectionID: UniqueID(1),
		}

		inMsg.insertMessages = []*msgstream.InsertMsg{}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 345}}
		inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 456}}
		iBNode.Operate([]flowgraph.Msg{iMsg})

		assert.Equal(t, len(flushUnit), 3)
		assert.Equal(t, flushUnit[2].segID, int64(1))
		assert.Equal(t, len(flushUnit[2].checkPoint), 3)
		assert.Equal(t, flushUnit[2].checkPoint[1].numRows, int64(50+16000+100+32000))
		assert.Equal(t, flushUnit[2].checkPoint[2].numRows, int64(100+32000))
		assert.Equal(t, flushUnit[2].checkPoint[3].numRows, int64(0))
		assert.Equal(t, flushUnit[2].checkPoint[1].pos.Timestamp, Timestamp(345))
		assert.Equal(t, flushUnit[2].checkPoint[2].pos.Timestamp, Timestamp(234))
		assert.Equal(t, flushUnit[2].checkPoint[3].pos.Timestamp, Timestamp(123))
		assert.Equal(t, len(flushUnit[2].field2Path), 0)
		assert.NotNil(t, flushUnit[2].field2Path)
		assert.True(t, flushUnit[2].flushed)
		// assert.Equal(t, len(iBNode.insertBuffer.insertData), 1)
		// assert.Equal(t, iBNode.insertBuffer.size(3), int32(50+16000))

		flushChan <- &flushMsg{
			msgID:        4,
			timestamp:    567,
			segmentID:    UniqueID(3),
			collectionID: UniqueID(3),
		}
		iBNode.Operate([]flowgraph.Msg{iMsg})

		assert.Equal(t, len(flushUnit), 4)
		assert.Equal(t, flushUnit[3].segID, int64(3))
		assert.Equal(t, len(flushUnit[3].checkPoint), 2)
		assert.Equal(t, flushUnit[3].checkPoint[3].numRows, int64(50+16000))
		assert.Equal(t, flushUnit[3].checkPoint[2].numRows, int64(100+32000))
		assert.Equal(t, flushUnit[3].checkPoint[3].pos.Timestamp, Timestamp(234))
		assert.Equal(t, flushUnit[3].checkPoint[2].pos.Timestamp, Timestamp(234))
		assert.Greater(t, len(flushUnit[3].field2Path), 0)
		assert.NotNil(t, flushUnit[3].field2Path)
		assert.True(t, flushUnit[3].flushed)
		// assert.Equal(t, len(iBNode.insertBuffer.insertData), 0)

	})
}

// CompactedRootCoord has meta info compacted at ts
type CompactedRootCoord struct {
	types.RootCoord
	compactTs Timestamp
}

func (m *CompactedRootCoord) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	if in.GetTimeStamp() <= m.compactTs {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "meta compacted",
			},
		}, nil
	}
	return m.RootCoord.DescribeCollection(ctx, in)
}

func TestInsertBufferNode_getCollMetaBySegID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-operate"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")

	rcf := &RootCoordFactory{}
	mockRootCoord := &CompactedRootCoord{
		RootCoord: rcf,
		compactTs: 100,
	}

	replica := newReplica(mockRootCoord, collMeta.ID)

	err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	saveBinlog := func(fu *segmentFlushUnit) error {
		t.Log(fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode, err := newInsertBufferNode(ctx, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	require.NoError(t, err)

	meta, err := iBNode.getCollMetabySegID(1, 101)
	assert.Nil(t, err)
	assert.Equal(t, collMeta.ID, meta.ID)

	_, err = iBNode.getCollMetabySegID(2, 101)
	assert.NotNil(t, err)

	meta, err = iBNode.getCollMetabySegID(1, 99)
	assert.NotNil(t, err)
}

func TestInsertBufferNode_bufferInsertMsg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertChannelName := "datanode-01-test-flowgraphinsertbuffernode-operate"

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")

	rcf := &RootCoordFactory{}
	mockRootCoord := &CompactedRootCoord{
		RootCoord: rcf,
		compactTs: 100,
	}

	replica := newReplica(mockRootCoord, collMeta.ID)

	err = replica.addNewSegment(1, collMeta.ID, 0, insertChannelName, &internalpb.MsgPosition{}, &internalpb.MsgPosition{})
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	saveBinlog := func(fu *segmentFlushUnit) error {
		t.Log(fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode, err := newInsertBufferNode(ctx, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog, "string", newCache())
	require.NoError(t, err)

	inMsg := genFlowGraphMsg(insertChannelName)
	for _, msg := range inMsg.insertMessages {
		msg.EndTimestamp = 101 // ts valid
		err = iBNode.bufferInsertMsg(msg, &internalpb.MsgPosition{})
		assert.Nil(t, err)
	}

	for _, msg := range inMsg.insertMessages {
		msg.EndTimestamp = 99 // ts invalid
		err = iBNode.bufferInsertMsg(msg, &internalpb.MsgPosition{})
		assert.NotNil(t, err)
	}

	for _, msg := range inMsg.insertMessages {
		msg.EndTimestamp = 101 // ts valid
		msg.RowIDs = []int64{} //misaligned data
		err = iBNode.bufferInsertMsg(msg, &internalpb.MsgPosition{})
		assert.NotNil(t, err)
	}
}

func TestInsertBufferNode_updateSegStatesInReplica(te *testing.T) {
	invalideTests := []struct {
		replicaCollID UniqueID

		inCollID    UniqueID
		segID       UniqueID
		description string
	}{
		{1, 9, 100, "collectionID mismatch"},
	}

	for _, test := range invalideTests {
		ibNode := &insertBufferNode{
			replica: newReplica(&RootCoordFactory{}, test.replicaCollID),
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
	Params.FlushInsertBufferSize = 16 * (1 << 20) // 16 MB

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
