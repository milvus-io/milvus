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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
)

var dataSyncServiceTestDir = "/tmp/milvus_test/data_sync_service"

func getVchanInfo(info *testInfo) *datapb.VchannelInfo {
	var ufs []*datapb.SegmentInfo
	var fs []*datapb.SegmentInfo
	if info.isValidCase {
		ufs = []*datapb.SegmentInfo{{
			CollectionID:  info.ufCollID,
			PartitionID:   1,
			InsertChannel: info.ufchanName,
			ID:            info.ufSegID,
			NumOfRows:     info.ufNor,
			DmlPosition:   &internalpb.MsgPosition{},
		}}
		fs = []*datapb.SegmentInfo{{
			CollectionID:  info.fCollID,
			PartitionID:   1,
			InsertChannel: info.fchanName,
			ID:            info.fSegID,
			NumOfRows:     info.fNor,
			DmlPosition:   &internalpb.MsgPosition{},
		}}
	} else {
		ufs = []*datapb.SegmentInfo{}
	}

	var ufsIds []int64
	var fsIds []int64
	for _, segmentInfo := range ufs {
		ufsIds = append(ufsIds, segmentInfo.ID)
	}
	for _, segmentInfo := range fs {
		fsIds = append(fsIds, segmentInfo.ID)
	}
	vi := &datapb.VchannelInfo{
		CollectionID:        info.collID,
		ChannelName:         info.chanName,
		SeekPosition:        &internalpb.MsgPosition{},
		UnflushedSegmentIds: ufsIds,
		FlushedSegmentIds:   fsIds,
	}
	return vi
}

type testInfo struct {
	isValidCase  bool
	replicaNil   bool
	inMsgFactory msgstream.Factory

	collID   UniqueID
	chanName string

	ufCollID   UniqueID
	ufSegID    UniqueID
	ufchanName string
	ufNor      int64

	fCollID   UniqueID
	fSegID    UniqueID
	fchanName string
	fNor      int64

	description string
}

func TestDataSyncService_newDataSyncService(te *testing.T) {

	ctx := context.Background()

	tests := []*testInfo{
		{true, false, &mockMsgStreamFactory{false, true},
			0, "by-dev-rootcoord-dml-test_v0",
			0, 0, "", 0,
			0, 0, "", 0,
			"SetParamsReturnError"},
		{true, false, &mockMsgStreamFactory{true, true},
			0, "by-dev-rootcoord-dml-test_v0",
			1, 0, "", 0,
			1, 1, "", 0,
			"CollID 0 mismach with seginfo collID 1"},
		{true, false, &mockMsgStreamFactory{true, true},
			1, "by-dev-rootcoord-dml-test_v1",
			1, 0, "by-dev-rootcoord-dml-test_v2", 0,
			1, 1, "by-dev-rootcoord-dml-test_v3", 0,
			"chanName c1 mismach with seginfo chanName c2"},
		{true, false, &mockMsgStreamFactory{true, true},
			1, "by-dev-rootcoord-dml-test_v1",
			1, 0, "by-dev-rootcoord-dml-test_v1", 0,
			1, 1, "by-dev-rootcoord-dml-test_v2", 0,
			"add normal segments"},
		{false, false, &mockMsgStreamFactory{true, false},
			0, "by-dev-rootcoord-dml-test_v0",
			0, 0, "", 0,
			0, 0, "", 0,
			"error when newinsertbufernode"},
		{false, true, &mockMsgStreamFactory{true, false},
			0, "by-dev-rootcoord-dml-test_v0",
			0, 0, "", 0,
			0, 0, "", 0,
			"replica nil"},
	}
	cm := storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer cm.RemoveWithPrefix("")

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			df := &DataCoordFactory{}

			replica, err := newReplica(context.Background(), &RootCoordFactory{pkType: schemapb.DataType_Int64}, cm, test.collID)
			assert.Nil(t, err)
			if test.replicaNil {
				replica = nil
			}

			ds, err := newDataSyncService(ctx,
				make(chan flushMsg),
				make(chan resendTTMsg),
				replica,
				NewAllocatorFactory(),
				test.inMsgFactory,
				getVchanInfo(test),
				make(chan string),
				df,
				newCache(),
				cm,
				newCompactionExecutor(),
			)

			if !test.isValidCase {
				assert.Error(t, err)
				assert.Nil(t, ds)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ds)

				// start
				ds.fg = nil
				ds.start()
			}
		})
	}

}

// NOTE: start pulsar before test
func TestDataSyncService_Start(t *testing.T) {
	t.Skip()
	const ctxTimeInMillisecond = 2000

	delay := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), delay)
	defer cancel()

	// init data node

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	mockRootCoord := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	collectionID := UniqueID(1)

	flushChan := make(chan flushMsg, 100)
	resendTTChan := make(chan resendTTMsg, 100)
	cm := storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer cm.RemoveWithPrefix("")
	replica, err := newReplica(context.Background(), mockRootCoord, cm, collectionID)
	assert.Nil(t, err)

	allocFactory := NewAllocatorFactory(1)
	factory := dependency.NewDefaultFactory(true)

	insertChannelName := "data_sync_service_test_dml"
	ddlChannelName := "data_sync_service_test_ddl"
	Params.DataNodeCfg.FlushInsertBufferSize = 1

	ufs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		InsertChannel: insertChannelName,
		ID:            0,
		NumOfRows:     0,
		DmlPosition:   &internalpb.MsgPosition{},
	}}
	fs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		PartitionID:   1,
		InsertChannel: insertChannelName,
		ID:            1,
		NumOfRows:     0,
		DmlPosition:   &internalpb.MsgPosition{},
	}}
	var ufsIds []int64
	var fsIds []int64
	for _, segmentInfo := range ufs {
		ufsIds = append(ufsIds, segmentInfo.ID)
	}
	for _, segmentInfo := range fs {
		fsIds = append(fsIds, segmentInfo.ID)
	}
	vchan := &datapb.VchannelInfo{
		CollectionID:        collMeta.ID,
		ChannelName:         insertChannelName,
		UnflushedSegmentIds: ufsIds,
		FlushedSegmentIds:   fsIds,
	}

	signalCh := make(chan string, 100)
	sync, err := newDataSyncService(ctx, flushChan, resendTTChan, replica, allocFactory, factory, vchan, signalCh, &DataCoordFactory{}, newCache(), cm, newCompactionExecutor())

	assert.Nil(t, err)
	// sync.replica.addCollection(collMeta.ID, collMeta.Schema)
	sync.start()

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}
	dataFactory := NewDataFactory()
	insertMessages := dataFactory.GetMsgStreamTsInsertMsgs(2, insertChannelName)

	msgPack := msgstream.MsgPack{
		BeginTs: timeRange.timestampMin,
		EndTs:   timeRange.timestampMax,
		Msgs:    insertMessages,
		StartPositions: []*internalpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
		EndPositions: []*internalpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
	}

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}

	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: Timestamp(0),
			EndTimestamp:   Timestamp(0),
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     UniqueID(0),
				Timestamp: math.MaxUint64,
				SourceID:  0,
			},
		},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	assert.NoError(t, err)
	insertStream, _ := factory.NewMsgStream(ctx)
	insertStream.AsProducer([]string{insertChannelName})

	ddStream, _ := factory.NewMsgStream(ctx)
	ddStream.AsProducer([]string{ddlChannelName})

	var insertMsgStream msgstream.MsgStream = insertStream
	insertMsgStream.Start()

	var ddMsgStream msgstream.MsgStream = ddStream
	ddMsgStream.Start()

	err = insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)
	err = ddMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	// dataSync
	<-sync.ctx.Done()

	sync.close()
}

func genBytes() (rawData []byte) {
	const DIM = 2
	const N = 1

	// Float vector
	var fvector = [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	var bvector = []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)

	// Bool
	var fieldBool = true
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, common.Endian, fieldBool); err != nil {
		panic(err)
	}

	rawData = append(rawData, buf.Bytes()...)

	// int8
	var dataInt8 int8 = 100
	bint8 := new(bytes.Buffer)
	if err := binary.Write(bint8, common.Endian, dataInt8); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint8.Bytes()...)
	log.Debug("Rawdata length:", zap.Int("Length of rawData", len(rawData)))
	return
}

func TestBytesReader(t *testing.T) {
	rawData := genBytes()

	// Bytes Reader is able to recording the position
	rawDataReader := bytes.NewReader(rawData)

	var fvector = make([]float32, 2)
	err := binary.Read(rawDataReader, common.Endian, &fvector)
	assert.Nil(t, err)
	assert.ElementsMatch(t, fvector, []float32{1, 2})

	var bvector = make([]byte, 4)
	err = binary.Read(rawDataReader, common.Endian, &bvector)
	assert.Nil(t, err)
	assert.ElementsMatch(t, bvector, []byte{255, 255, 255, 0})

	var fieldBool bool
	err = binary.Read(rawDataReader, common.Endian, &fieldBool)
	assert.Nil(t, err)
	assert.Equal(t, true, fieldBool)

	var dataInt8 int8
	err = binary.Read(rawDataReader, common.Endian, &dataInt8)
	assert.Nil(t, err)
	assert.Equal(t, int8(100), dataInt8)
}

func TestGetSegmentInfos(t *testing.T) {
	dataCoord := &DataCoordFactory{}
	dsService := &dataSyncService{
		dataCoord: dataCoord,
	}
	segmentInfos, err := dsService.getSegmentInfos([]int64{1})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(segmentInfos))

	dataCoord.GetSegmentInfosError = true
	segmentInfos2, err := dsService.getSegmentInfos([]int64{1})
	assert.Error(t, err)
	assert.Empty(t, segmentInfos2)

	dataCoord.GetSegmentInfosError = false
	dataCoord.GetSegmentInfosNotSuccess = true
	segmentInfos3, err := dsService.getSegmentInfos([]int64{1})
	assert.Error(t, err)
	assert.Empty(t, segmentInfos3)
}
