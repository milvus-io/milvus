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

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

func TestFlowGraphInsertBufferNode_Operate(t *testing.T) {
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = false
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

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

	iBNode := newInsertBufferNode(ctx, newBinlogMeta(), replica, msFactory, NewAllocatorFactory())

	ddlFlushedCh := make(chan bool)
	dmlFlushedCh := make(chan bool)

	inMsg := genInsertMsg(ddlFlushedCh, dmlFlushedCh)
	var iMsg flowgraph.Msg = &inMsg
	iBNode.Operate([]flowgraph.Msg{iMsg})

	isflushed := <-dmlFlushedCh
	assert.True(t, isflushed)
}

func genInsertMsg(ddlFlushedCh, dmlFlushedCh chan<- bool) insertMsg {

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
		// flushMessages:  make([]*flushMsg, 0),
		timeRange: TimeRange{
			timestampMin: timeRange.timestampMin,
			timestampMax: timeRange.timestampMax,
		},
		startPositions: startPos,
		endPositions:   startPos,
	}

	dataFactory := NewDataFactory()
	iMsg.insertMessages = append(iMsg.insertMessages, dataFactory.GetMsgStreamInsertMsgs(2)...)

	iMsg.flushMessage = &flushMsg{
		msgID:        1,
		timestamp:    2000,
		segmentID:    UniqueID(1),
		collectionID: UniqueID(1),
		ddlFlushedCh: ddlFlushedCh,
		dmlFlushedCh: dmlFlushedCh,
	}

	return *iMsg

}

func TestFlushSegmentTxn(t *testing.T) {
	idAllocMock := NewAllocatorFactory(1)
	mockMinIO := memkv.NewMemoryKV()

	segmentID, _ := idAllocMock.allocID()
	partitionID, _ := idAllocMock.allocID()
	collectionID, _ := idAllocMock.allocID()
	fmt.Printf("generate segmentID, partitionID, collectionID: %v, %v, %v\n",
		segmentID, partitionID, collectionID)

	collMeta := genCollectionMeta(collectionID, "test_flush_segment_txn")
	flushMap := sync.Map{}
	flushMeta := newBinlogMeta()

	finishCh := make(chan map[UniqueID]string)

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

	go func(wait <-chan map[UniqueID]string) {
		field2Path := <-wait
		assert.NotNil(t, field2Path)
	}(finishCh)

	flushSegment(collMeta,
		segmentID,
		partitionID,
		collectionID,
		&flushMap,
		mockMinIO,
		finishCh,
		idAllocMock)

	k, _ := flushMeta.genKey(false, collectionID, partitionID, segmentID, 0)
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
