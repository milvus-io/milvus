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
	"fmt"
	"math"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func TestFlowGraphInsertBufferNode_Operate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")

	replica := newReplica()
	err = replica.addCollection(collMeta.ID, collMeta.Schema)
	require.NoError(t, err)
	err = replica.addSegment(1, collMeta.ID, 0, Params.InsertChannelNames[0])
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	saveBinlog := func(fu *autoFlushUnit) error {
		t.Log(fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode := newInsertBufferNode(ctx, replica, msFactory, NewAllocatorFactory(), flushChan, saveBinlog)

	dmlFlushedCh := make(chan []*datapb.ID2PathList, 1)

	flushChan <- &flushMsg{
		msgID:        1,
		timestamp:    2000,
		segmentID:    UniqueID(1),
		collectionID: UniqueID(1),
		dmlFlushedCh: dmlFlushedCh,
	}

	inMsg := genInsertMsg()
	var iMsg flowgraph.Msg = &inMsg
	iBNode.Operate([]flowgraph.Msg{iMsg})
	isflushed := <-dmlFlushedCh
	assert.NotNil(t, isflushed)
	log.Debug("DML binlog paths", zap.Any("paths", isflushed))
}

func genInsertMsg() insertMsg {

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*internalpb.MsgPosition{
		{
			ChannelName: Params.InsertChannelNames[0],
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	var iMsg = &insertMsg{
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
	idAllocMock := NewAllocatorFactory(1)
	mockMinIO := memkv.NewMemoryKV()

	segmentID, _ := idAllocMock.allocID()
	partitionID, _ := idAllocMock.allocID()
	collectionID, _ := idAllocMock.allocID()
	fmt.Printf("generate segmentID, partitionID, collectionID: %v, %v, %v\n",
		segmentID, partitionID, collectionID)

	collMeta := genCollectionMeta(collectionID, "test_flush_segment_txn")
	flushMap := sync.Map{}
	replica := newReplica()
	replica.setEndPositions(segmentID, []*internalpb.MsgPosition{{ChannelName: "TestChannel"}})

	finishCh := make(chan autoFlushUnit, 1)

	insertData := &InsertData{
		Data: make(map[storage.FieldID]storage.FieldData),
	}
	insertData.Data[0] = &storage.Int64FieldData{
		NumRows: 10,
		Data:    []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	insertData.Data[1] = &storage.Int64FieldData{
		NumRows: 10,
		Data:    []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}
	insertData.Data[107] = &storage.FloatFieldData{
		NumRows: 10,
		Data:    make([]float32, 10),
	}
	flushMap.Store(segmentID, insertData)

	flushSegment(collMeta,
		segmentID,
		partitionID,
		collectionID,
		&flushMap,
		mockMinIO,
		finishCh,
		nil,
		replica,
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

	colRep := &CollectionSegmentReplica{
		segments:              make(map[UniqueID]*Segment),
		collections:           make(map[UniqueID]*Collection),
		startPositions:        make(map[UniqueID][]*internalpb.MsgPosition),
		endPositions:          make(map[UniqueID][]*internalpb.MsgPosition),
		openSegmentCheckPoint: make(map[UniqueID]internalpb.MsgPosition),
	}
	err = colRep.addCollection(collMeta.ID, collMeta.Schema)
	require.NoError(t, err)
	err = colRep.addSegment(1, collMeta.ID, 0, Params.InsertChannelNames[0])
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	flushUnit := []autoFlushUnit{}
	saveBinlog := func(fu *autoFlushUnit) error {
		flushUnit = append(flushUnit, *fu)
		return nil
	}

	flushChan := make(chan *flushMsg, 100)
	iBNode := newInsertBufferNode(ctx, colRep, msFactory, NewAllocatorFactory(), flushChan, saveBinlog)

	inMsg := genInsertMsg()
	inMsg.insertMessages = dataFactory.GetMsgStreamInsertMsgs(100)
	inMsg.insertMessages = append(inMsg.insertMessages, dataFactory.GetMsgStreamInsertMsgs(32000)...)
	for i := range inMsg.insertMessages {
		inMsg.insertMessages[i].SegmentID = int64(i%2) + 1
	}
	inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 123}}

	var iMsg flowgraph.Msg = &inMsg
	iBNode.Operate([]flowgraph.Msg{iMsg})
	assert.Equal(t, len(colRep.endPositions), 2)
	assert.Equal(t, colRep.endPositions[1][0].Timestamp, Timestamp(123))
	assert.Equal(t, colRep.endPositions[2][0].Timestamp, Timestamp(123))
	assert.Equal(t, len(colRep.openSegmentCheckPoint), 0)
	assert.Equal(t, len(iBNode.insertBuffer.insertData), 2)
	assert.Equal(t, iBNode.insertBuffer.size(1), int32(50+16000))
	assert.Equal(t, iBNode.insertBuffer.size(2), int32(50+16000))

	for i := range inMsg.insertMessages {
		inMsg.insertMessages[i].SegmentID = int64(i%2) + 2
	}
	inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 234}}
	iBNode.Operate([]flowgraph.Msg{iMsg})
	assert.Equal(t, len(colRep.endPositions), 3)
	assert.Equal(t, colRep.endPositions[1][0].Timestamp, Timestamp(123))
	assert.Equal(t, colRep.endPositions[2][0].Timestamp, Timestamp(234))
	assert.Equal(t, colRep.endPositions[3][0].Timestamp, Timestamp(234))
	assert.Equal(t, len(colRep.openSegmentCheckPoint), 1)
	assert.Equal(t, colRep.openSegmentCheckPoint[2].Timestamp, Timestamp(234))
	assert.Equal(t, len(flushUnit), 1)
	assert.Equal(t, flushUnit[0].segID, int64(2))
	assert.Equal(t, len(flushUnit[0].numRows), 1)
	assert.Equal(t, flushUnit[0].numRows[2], int64(100+32000))
	assert.Equal(t, len(flushUnit[0].openSegCheckpoints), 1)
	assert.Equal(t, flushUnit[0].openSegCheckpoints[2].Timestamp, Timestamp(234))
	assert.Greater(t, len(flushUnit[0].field2Path), 0)
	assert.False(t, flushUnit[0].flushed)
	assert.Equal(t, len(iBNode.insertBuffer.insertData), 2)
	assert.Equal(t, iBNode.insertBuffer.size(1), int32(50+16000))
	assert.Equal(t, iBNode.insertBuffer.size(3), int32(50+16000))

	for i := range inMsg.insertMessages {
		inMsg.insertMessages[i].SegmentID = 1
	}
	inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 345}}
	iBNode.Operate([]flowgraph.Msg{iMsg})
	assert.Equal(t, len(colRep.endPositions), 3)
	assert.Equal(t, colRep.endPositions[1][0].Timestamp, Timestamp(345))
	assert.Equal(t, colRep.endPositions[2][0].Timestamp, Timestamp(234))
	assert.Equal(t, colRep.endPositions[3][0].Timestamp, Timestamp(234))
	assert.Equal(t, len(colRep.openSegmentCheckPoint), 2)
	assert.Equal(t, colRep.openSegmentCheckPoint[1].Timestamp, Timestamp(345))
	assert.Equal(t, colRep.openSegmentCheckPoint[2].Timestamp, Timestamp(234))
	assert.Equal(t, len(flushUnit), 2)
	assert.Equal(t, flushUnit[1].segID, int64(1))
	assert.Equal(t, len(flushUnit[1].numRows), 2)
	assert.Equal(t, flushUnit[1].numRows[2], int64(100+32000))
	assert.Equal(t, flushUnit[1].numRows[1], int64(50+16000+100+32000))
	assert.Equal(t, len(flushUnit[1].openSegCheckpoints), 2)
	assert.Equal(t, flushUnit[1].openSegCheckpoints[1].Timestamp, Timestamp(345))
	assert.Equal(t, flushUnit[1].openSegCheckpoints[2].Timestamp, Timestamp(234))
	assert.False(t, flushUnit[1].flushed)
	assert.Greater(t, len(flushUnit[1].field2Path), 0)
	assert.Equal(t, len(iBNode.insertBuffer.insertData), 1)
	assert.Equal(t, iBNode.insertBuffer.size(3), int32(50+16000))

	dmlFlushedCh := make(chan []*datapb.ID2PathList, 1)

	flushChan <- &flushMsg{
		msgID:        3,
		timestamp:    456,
		segmentID:    UniqueID(1),
		collectionID: UniqueID(1),
		dmlFlushedCh: dmlFlushedCh,
	}

	inMsg.insertMessages = []*msgstream.InsertMsg{}
	inMsg.endPositions = []*internalpb.MsgPosition{{Timestamp: 456}}
	iBNode.Operate([]flowgraph.Msg{iMsg})

	flushSeg := <-dmlFlushedCh
	assert.NotNil(t, flushSeg)
	assert.Equal(t, len(flushSeg), 1)
	assert.Equal(t, flushSeg[0].ID, int64(1))
	assert.NotNil(t, flushSeg[0].Paths)
	assert.Equal(t, len(colRep.endPositions), 3)
	assert.Equal(t, colRep.endPositions[1][0].Timestamp, Timestamp(345))
	assert.Equal(t, colRep.endPositions[2][0].Timestamp, Timestamp(234))
	assert.Equal(t, colRep.endPositions[3][0].Timestamp, Timestamp(234))
	assert.Equal(t, len(colRep.openSegmentCheckPoint), 1)
	assert.Equal(t, colRep.openSegmentCheckPoint[2].Timestamp, Timestamp(234))
	assert.Equal(t, len(flushUnit), 3)
	assert.Equal(t, flushUnit[2].segID, int64(1))
	assert.Equal(t, len(flushUnit[2].numRows), 2)
	assert.Equal(t, flushUnit[2].numRows[2], int64(100+32000))
	assert.Equal(t, flushUnit[2].numRows[1], int64(50+16000+100+32000))
	t.Log(flushUnit[2].openSegCheckpoints)
	assert.Equal(t, len(flushUnit[2].openSegCheckpoints), 2)
	assert.Equal(t, flushUnit[2].openSegCheckpoints[1].Timestamp, Timestamp(345))
	assert.Equal(t, flushUnit[2].openSegCheckpoints[2].Timestamp, Timestamp(234))
	assert.Equal(t, len(flushUnit[2].field2Path), 0)
	assert.NotNil(t, flushUnit[2].field2Path)
	assert.True(t, flushUnit[2].flushed)

	flushChan <- &flushMsg{
		msgID:        4,
		timestamp:    567,
		segmentID:    UniqueID(3),
		collectionID: UniqueID(3),
		dmlFlushedCh: dmlFlushedCh,
	}
	iBNode.Operate([]flowgraph.Msg{iMsg})
	flushSeg = <-dmlFlushedCh
	assert.NotNil(t, flushSeg)
	assert.Equal(t, len(flushSeg), 1)
	assert.Equal(t, flushSeg[0].ID, int64(3))
	assert.NotNil(t, flushSeg[0].Paths)
	assert.Equal(t, len(colRep.endPositions), 3)
	assert.Equal(t, colRep.endPositions[1][0].Timestamp, Timestamp(345))
	assert.Equal(t, colRep.endPositions[2][0].Timestamp, Timestamp(234))
	assert.Equal(t, colRep.endPositions[3][0].Timestamp, Timestamp(234))
	assert.Equal(t, len(colRep.openSegmentCheckPoint), 1)
	assert.Equal(t, colRep.openSegmentCheckPoint[2].Timestamp, Timestamp(234))
	assert.Equal(t, len(flushUnit), 4)
	assert.Equal(t, flushUnit[3].segID, int64(3))
	assert.Equal(t, len(flushUnit[3].numRows), 2)
	assert.Equal(t, flushUnit[3].numRows[3], int64(50+16000))
	assert.Equal(t, flushUnit[3].numRows[2], int64(100+32000))
	assert.Greater(t, len(flushUnit[3].field2Path), 0)
	assert.NotNil(t, flushUnit[3].field2Path)
	assert.True(t, flushUnit[3].flushed)
	assert.Equal(t, len(iBNode.insertBuffer.insertData), 0)
}
