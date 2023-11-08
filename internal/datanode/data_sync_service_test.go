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
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

var dataSyncServiceTestDir = "/tmp/milvus_test/data_sync_service"

func init() {
	paramtable.Init()
}

func getWatchInfo(info *testInfo) *datapb.ChannelWatchInfo {
	return &datapb.ChannelWatchInfo{
		Vchan: getVchanInfo(info),
	}
}

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
			DmlPosition:   &msgpb.MsgPosition{},
		}}
		fs = []*datapb.SegmentInfo{{
			CollectionID:  info.fCollID,
			PartitionID:   1,
			InsertChannel: info.fchanName,
			ID:            info.fSegID,
			NumOfRows:     info.fNor,
			DmlPosition:   &msgpb.MsgPosition{},
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
		SeekPosition:        &msgpb.MsgPosition{},
		UnflushedSegmentIds: ufsIds,
		FlushedSegmentIds:   fsIds,
	}
	return vi
}

type testInfo struct {
	isValidCase  bool
	channelNil   bool
	inMsgFactory dependency.Factory

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

func TestDataSyncService_newDataSyncService(t *testing.T) {
	ctx := context.Background()

	tests := []*testInfo{
		{
			true, false, &mockMsgStreamFactory{false, true},
			1, "by-dev-rootcoord-dml-test_v0",
			1, 0, "", 0,
			1, 0, "", 0,
			"SetParamsReturnError",
		},
		{
			true, false, &mockMsgStreamFactory{true, true},
			1, "by-dev-rootcoord-dml-test_v1",
			1, 0, "by-dev-rootcoord-dml-test_v1", 0,
			1, 1, "by-dev-rootcoord-dml-test_v2", 0,
			"add normal segments",
		},
		{
			true, false, &mockMsgStreamFactory{true, true},
			1, "by-dev-rootcoord-dml-test_v1",
			1, 1, "by-dev-rootcoord-dml-test_v1", 0,
			1, 2, "by-dev-rootcoord-dml-test_v1", 0,
			"add un-flushed and flushed segments",
		},
	}
	cm := storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			node.factory = test.inMsgFactory
			ds, err := newServiceWithEtcdTickler(
				ctx,
				node,
				getWatchInfo(test),
				genTestTickler(),
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
	const ctxTimeInMillisecond = 10000
	os.RemoveAll("/tmp/milvus")
	defer os.RemoveAll("/tmp/milvus")

	delay := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), delay)
	defer cancel()

	node := newIDLEDataNodeMock(context.Background(), schemapb.DataType_Int64)
	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer node.chunkManager.RemoveWithPrefix(ctx, node.chunkManager.RootPath())

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	broker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	node.broker = broker

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().Alloc(mock.Anything).Call.Return(int64(22222),
		func(count uint32) int64 {
			return int64(22222 + count)
		}, nil)
	node.allocator = alloc

	var (
		insertChannelName = fmt.Sprintf("by-dev-rootcoord-dml-%d", rand.Int())

		Factory  = &MetaFactory{}
		collMeta = Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	)

	paramtable.Get().Save(Params.DataNodeCfg.FlushInsertBufferSize.Key, "1")

	ufs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		PartitionID:   1,
		InsertChannel: insertChannelName,
		ID:            0,
		NumOfRows:     0,
		DmlPosition:   &msgpb.MsgPosition{},
	}}
	fs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		PartitionID:   1,
		InsertChannel: insertChannelName,
		ID:            1,
		NumOfRows:     0,
		DmlPosition:   &msgpb.MsgPosition{},
	}}
	var ufsIds []int64
	var fsIds []int64
	for _, segmentInfo := range ufs {
		ufsIds = append(ufsIds, segmentInfo.ID)
	}
	for _, segmentInfo := range fs {
		fsIds = append(fsIds, segmentInfo.ID)
	}

	watchInfo := &datapb.ChannelWatchInfo{
		Schema: collMeta.GetSchema(),
		Vchan: &datapb.VchannelInfo{
			CollectionID:        collMeta.ID,
			ChannelName:         insertChannelName,
			UnflushedSegmentIds: ufsIds,
			FlushedSegmentIds:   fsIds,
		},
	}

	broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Call.Return(
		func(_ context.Context, segmentIDs []int64) []*datapb.SegmentInfo {
			data := map[int64]*datapb.SegmentInfo{
				0: {
					ID:            0,
					CollectionID:  collMeta.ID,
					PartitionID:   1,
					InsertChannel: insertChannelName,
				},

				1: {
					ID:            1,
					CollectionID:  collMeta.ID,
					PartitionID:   1,
					InsertChannel: insertChannelName,
				},
			}
			return lo.FilterMap(segmentIDs, func(id int64, _ int) (*datapb.SegmentInfo, bool) {
				item, ok := data[id]
				return item, ok
			})
		}, nil)

	sync, err := newServiceWithEtcdTickler(
		ctx,
		node,
		watchInfo,
		genTestTickler(),
	)
	require.NoError(t, err)
	require.NotNil(t, sync)

	sync.flushListener = make(chan *segmentFlushPack)
	defer close(sync.flushListener)
	sync.start()
	defer sync.close()

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64 - 1,
	}
	dataFactory := NewDataFactory()
	insertMessages := dataFactory.GetMsgStreamTsInsertMsgs(2, insertChannelName, tsoutil.GetCurrentTime())

	msgPack := msgstream.MsgPack{
		BeginTs: timeRange.timestampMin,
		EndTs:   timeRange.timestampMax,
		Msgs:    insertMessages,
		StartPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
		EndPositions: []*msgpb.MsgPosition{{
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
		TimeTickMsg: msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     UniqueID(0),
				Timestamp: math.MaxUint64 - 1,
				SourceID:  0,
			},
		},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	assert.NoError(t, err)
	insertStream, _ := node.factory.NewMsgStream(ctx)
	insertStream.AsProducer([]string{insertChannelName})

	var insertMsgStream msgstream.MsgStream = insertStream

	err = insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	_, err = insertMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	select {
	case flushPack := <-sync.flushListener:
		assert.True(t, flushPack.segmentID == 1)
		return
	case <-sync.ctx.Done():
		assert.Fail(t, "test timeout")
	}
}

func TestDataSyncService_Close(t *testing.T) {
	const ctxTimeInMillisecond = 1000000

	delay := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), delay)
	defer cancel()

	os.RemoveAll("/tmp/milvus")
	defer os.RemoveAll("/tmp/milvus")

	// init data node
	var (
		insertChannelName = "by-dev-rootcoord-dml2"

		metaFactory = &MetaFactory{}
		collMeta    = metaFactory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
		node        = newIDLEDataNodeMock(context.Background(), schemapb.DataType_Int64)
	)
	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer node.chunkManager.RemoveWithPrefix(ctx, node.chunkManager.RootPath())

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ReportTimeTick(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).Return(nil).Maybe()
	broker.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	broker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	node.broker = broker

	ufs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		PartitionID:   1,
		InsertChannel: insertChannelName,
		ID:            1,
		NumOfRows:     1,
		DmlPosition:   &msgpb.MsgPosition{},
	}}
	fs := []*datapb.SegmentInfo{{
		CollectionID:  collMeta.ID,
		PartitionID:   1,
		InsertChannel: insertChannelName,
		ID:            0,
		NumOfRows:     1,
		DmlPosition:   &msgpb.MsgPosition{},
	}}
	var ufsIds []int64
	var fsIds []int64
	for _, segmentInfo := range ufs {
		ufsIds = append(ufsIds, segmentInfo.ID)
	}
	for _, segmentInfo := range fs {
		fsIds = append(fsIds, segmentInfo.ID)
	}
	watchInfo := &datapb.ChannelWatchInfo{
		Schema: collMeta.GetSchema(),
		Vchan: &datapb.VchannelInfo{
			CollectionID:        collMeta.ID,
			ChannelName:         insertChannelName,
			UnflushedSegmentIds: ufsIds,
			FlushedSegmentIds:   fsIds,
		},
	}

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil).Maybe()
	alloc.EXPECT().Alloc(mock.Anything).Call.Return(int64(22222),
		func(count uint32) int64 {
			return int64(22222 + count)
		}, nil).Maybe()
	node.allocator = alloc

	broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Call.Return(
		func(_ context.Context, segmentIDs []int64) []*datapb.SegmentInfo {
			data := map[int64]*datapb.SegmentInfo{
				0: {
					ID:            0,
					CollectionID:  collMeta.ID,
					PartitionID:   1,
					InsertChannel: insertChannelName,
				},

				1: {
					ID:            1,
					CollectionID:  collMeta.ID,
					PartitionID:   1,
					InsertChannel: insertChannelName,
				},
			}
			segments := lo.FilterMap(segmentIDs, func(id int64, _ int) (*datapb.SegmentInfo, bool) {
				item, ok := data[id]
				return item, ok
			})
			return segments
		}, nil).Maybe()

	// No Auto flush
	paramtable.Get().Reset(Params.DataNodeCfg.FlushInsertBufferSize.Key)

	syncService, err := newServiceWithEtcdTickler(
		context.Background(),
		node,
		watchInfo,
		genTestTickler(),
	)
	require.NoError(t, err)
	assert.NotNil(t, syncService)
	syncService.channel.(*ChannelMeta).syncPolicies = []segmentSyncPolicy{
		syncMemoryTooHigh(),
	}

	syncService.flushListener = make(chan *segmentFlushPack, 10)
	defer close(syncService.flushListener)

	syncService.start()

	var (
		dataFactory = NewDataFactory()
		ts          = tsoutil.GetCurrentTime()
	)
	insertMessages := dataFactory.GetMsgStreamTsInsertMsgs(2, insertChannelName, ts)
	msgPack := msgstream.MsgPack{
		BeginTs: ts,
		EndTs:   ts,
		Msgs:    insertMessages,
		StartPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
		EndPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
	}

	// 400 is the actual data
	int64Pks := []primaryKey{newInt64PrimaryKey(400)}
	deleteMessages := dataFactory.GenMsgStreamDeleteMsgWithTs(0, int64Pks, insertChannelName, ts+1)
	inMsgs := make([]msgstream.TsMsg, 0)
	inMsgs = append(inMsgs, deleteMessages)

	msgPackDelete := msgstream.MsgPack{
		BeginTs: ts + 1,
		EndTs:   ts + 1,
		Msgs:    inMsgs,
		StartPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
		EndPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
	}

	// generate timeTick
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts + 2,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     UniqueID(2),
				Timestamp: ts + 2,
				SourceID:  0,
			},
		},
	}

	timeTickMsgPack := msgstream.MsgPack{
		BeginTs: ts + 2,
		EndTs:   ts + 2,
		StartPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
		EndPositions: []*msgpb.MsgPosition{{
			ChannelName: insertChannelName,
		}},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	// pulsar produce
	assert.NoError(t, err)
	insertStream, _ := node.factory.NewMsgStream(ctx)
	insertStream.AsProducer([]string{insertChannelName})

	var insertMsgStream msgstream.MsgStream = insertStream

	err = insertMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	err = insertMsgStream.Produce(&msgPackDelete)
	assert.NoError(t, err)

	_, err = insertMsgStream.Broadcast(&timeTickMsgPack)
	assert.NoError(t, err)

	// wait for delete, no auto flush leads to all data in buffer.
	require.Eventually(t, func() bool { return syncService.delBufferManager.GetEntriesNum(1) == 1 },
		5*time.Second, 100*time.Millisecond)
	assert.Equal(t, 0, len(syncService.flushListener))

	// close will trigger a force sync
	syncService.GracefullyClose()
	assert.Eventually(t, func() bool { return len(syncService.flushListener) == 1 },
		5*time.Second, 100*time.Millisecond)
	flushPack, ok := <-syncService.flushListener
	assert.True(t, ok)
	assert.Equal(t, UniqueID(1), flushPack.segmentID)
	assert.True(t, len(flushPack.insertLogs) == 12)
	assert.True(t, len(flushPack.statsLogs) == 1)
	assert.True(t, len(flushPack.deltaLogs) == 1)

	<-syncService.ctx.Done()

	// Double close is safe
	syncService.GracefullyClose()
	<-syncService.ctx.Done()
}

func genBytes() (rawData []byte) {
	const DIM = 2
	const N = 1

	// Float vector
	fvector := [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	bvector := []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)

	// Bool
	fieldBool := true
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

	fvector := make([]float32, 2)
	err := binary.Read(rawDataReader, common.Endian, &fvector)
	assert.NoError(t, err)
	assert.ElementsMatch(t, fvector, []float32{1, 2})

	bvector := make([]byte, 4)
	err = binary.Read(rawDataReader, common.Endian, &bvector)
	assert.NoError(t, err)
	assert.ElementsMatch(t, bvector, []byte{255, 255, 255, 0})

	var fieldBool bool
	err = binary.Read(rawDataReader, common.Endian, &fieldBool)
	assert.NoError(t, err)
	assert.Equal(t, true, fieldBool)

	var dataInt8 int8
	err = binary.Read(rawDataReader, common.Endian, &dataInt8)
	assert.NoError(t, err)
	assert.Equal(t, int8(100), dataInt8)
}

func TestClearGlobalFlushingCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	meta := NewMetaFactory().GetCollectionMeta(1, "test_collection", schemapb.DataType_Int64)
	broker := broker.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{
			Status:         merr.Status(nil),
			CollectionID:   1,
			CollectionName: "test_collection",
			Schema:         meta.GetSchema(),
		}, nil)
	cm := storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	channel := newChannel("channel", 1, nil, broker, cm)
	var err error

	cache := newCache()
	dsService := &dataSyncService{
		broker:           broker,
		channel:          channel,
		flushingSegCache: cache,
	}

	err = channel.addSegment(
		context.TODO(),
		addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       1,
			collID:      1,
			partitionID: 1,
			startPos:    &msgpb.MsgPosition{},
			endPos:      &msgpb.MsgPosition{},
		})
	assert.NoError(t, err)

	err = channel.addSegment(
		context.TODO(),
		addSegmentReq{
			segType:      datapb.SegmentType_Flushed,
			segID:        2,
			collID:       1,
			partitionID:  1,
			numOfRows:    0,
			statsBinLogs: nil,
			recoverTs:    0,
		})
	assert.NoError(t, err)

	err = channel.addSegment(
		context.TODO(),
		addSegmentReq{
			segType:      datapb.SegmentType_Normal,
			segID:        3,
			collID:       1,
			partitionID:  1,
			numOfRows:    0,
			statsBinLogs: nil,
			recoverTs:    0,
		})
	assert.NoError(t, err)

	cache.checkOrCache(1)
	cache.checkOrCache(2)
	cache.checkOrCache(4)

	dsService.clearGlobalFlushingCache()

	assert.False(t, cache.checkIfCached(1))
	assert.False(t, cache.checkIfCached(2))
	assert.False(t, cache.checkIfCached(3))
	assert.True(t, cache.checkIfCached(4))
}

func TestGetChannelLatestMsgID(t *testing.T) {
	delay := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), delay)
	defer cancel()
	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)

	dmlChannelName := "fake-by-dev-rootcoord-dml-channel_12345v0"

	insertStream, _ := node.factory.NewMsgStream(ctx)
	insertStream.AsProducer([]string{dmlChannelName})
	id, err := node.getChannelLatestMsgID(ctx, dmlChannelName, 0)
	assert.NoError(t, err)
	assert.NotNil(t, id)
}

func TestGetChannelWithTickler(t *testing.T) {
	channelName := "by-dev-rootcoord-dml-0"
	info := getWatchInfoByOpID(100, channelName, datapb.ChannelWatchState_ToWatch)
	node := newIDLEDataNodeMock(context.Background(), schemapb.DataType_Int64)
	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer node.chunkManager.RemoveWithPrefix(context.Background(), node.chunkManager.RootPath())

	meta := NewMetaFactory().GetCollectionMeta(1, "test_collection", schemapb.DataType_Int64)
	broker := broker.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{
			Status:         merr.Status(nil),
			CollectionID:   1,
			CollectionName: "test_collection",
			Schema:         meta.GetSchema(),
		}, nil)
	node.broker = broker

	unflushed := []*datapb.SegmentInfo{
		{
			ID:           100,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
		},
		{
			ID:           101,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
		},
	}

	flushed := []*datapb.SegmentInfo{
		{
			ID:           200,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
		},
		{
			ID:           201,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
		},
	}

	channel, err := getChannelWithTickler(context.TODO(), node, info, newTickler(), unflushed, flushed)
	assert.NoError(t, err)
	assert.NotNil(t, channel)
	assert.Equal(t, channelName, channel.getChannelName(100))
	assert.Equal(t, int64(1), channel.getCollectionID())
	assert.True(t, channel.hasSegment(100, true))
	assert.True(t, channel.hasSegment(101, true))
	assert.True(t, channel.hasSegment(200, true))
	assert.True(t, channel.hasSegment(201, true))
}
