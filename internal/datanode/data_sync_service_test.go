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
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var dataSyncServiceTestDir = "/tmp/milvus_test/data_sync_service"

func init() {
	paramtable.Init()
}

func getWatchInfo(info *testInfo) *datapb.ChannelWatchInfo {
	return &datapb.ChannelWatchInfo{
		Vchan: getVchanInfo(info),
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
				},
				{
					FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
				},
				{
					FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
				},
				{
					FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "128"},
					},
				},
			},
		},
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

	var ufsIDs []int64
	var fsIDs []int64
	for _, segmentInfo := range ufs {
		ufsIDs = append(ufsIDs, segmentInfo.ID)
	}
	for _, segmentInfo := range fs {
		fsIDs = append(fsIDs, segmentInfo.ID)
	}
	vi := &datapb.VchannelInfo{
		CollectionID:        info.collID,
		ChannelName:         info.chanName,
		SeekPosition:        &msgpb.MsgPosition{},
		UnflushedSegmentIds: ufsIDs,
		FlushedSegmentIds:   fsIDs,
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
	node.allocator = allocator.NewMockAllocator(t)

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			node.factory = test.inMsgFactory
			defer node.tryToReleaseFlowgraph(test.chanName)
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

func TestDataSyncService_newDataSyncService_DuplicatedChannel(t *testing.T) {
	ctx := context.Background()

	test := &testInfo{
		true, false, &mockMsgStreamFactory{true, true},
		1, "by-dev-rootcoord-dml-test_v1",
		1, 1, "by-dev-rootcoord-dml-test_v1", 0,
		1, 2, "by-dev-rootcoord-dml-test_v1", 0,
		"add un-flushed and flushed segments",
	}
	cm := storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	watchInfo := getWatchInfo(test)

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	node.allocator = allocator.NewMockAllocator(t)
	node.factory = test.inMsgFactory
	metacache := metacache.NewMockMetaCache(t)
	metacache.EXPECT().Collection().Return(test.collID)
	metacache.EXPECT().Schema().Return(watchInfo.GetSchema())
	node.writeBufferManager.Register(test.chanName, metacache, nil, writebuffer.WithIDAllocator(allocator.NewMockAllocator(t)))
	ds, err := newServiceWithEtcdTickler(
		ctx,
		node,
		watchInfo,
		genTestTickler(),
	)

	assert.Error(t, err)
	assert.Nil(t, ds)
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

func TestGetChannelWithTickler(t *testing.T) {
	channelName := "by-dev-rootcoord-dml-0"
	info := getWatchInfoByOpID(100, channelName, datapb.ChannelWatchState_ToWatch)
	node := newIDLEDataNodeMock(context.Background(), schemapb.DataType_Int64)
	node.chunkManager = storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer node.chunkManager.RemoveWithPrefix(context.Background(), node.chunkManager.RootPath())

	meta := NewMetaFactory().GetCollectionMeta(1, "test_collection", schemapb.DataType_Int64)
	broker := broker.NewMockBroker(t)
	node.broker = broker
	info.Schema = meta.GetSchema()

	unflushed := []*datapb.SegmentInfo{
		{
			ID:           100,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
			State:        commonpb.SegmentState_Growing,
		},
		{
			ID:           101,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
			State:        commonpb.SegmentState_Growing,
		},
	}

	flushed := []*datapb.SegmentInfo{
		{
			ID:           200,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
			State:        commonpb.SegmentState_Flushed,
		},
		{
			ID:           201,
			CollectionID: 1,
			PartitionID:  10,
			NumOfRows:    20,
			State:        commonpb.SegmentState_Flushed,
		},
	}

	metaCache, err := getMetaCacheWithTickler(context.TODO(), node, info, newTickler(), unflushed, flushed, nil)
	assert.NoError(t, err)
	assert.NotNil(t, metaCache)
	assert.Equal(t, int64(1), metaCache.Collection())
	assert.Equal(t, 2, len(metaCache.GetSegmentsBy(metacache.WithSegmentIDs(100, 101), metacache.WithSegmentState(commonpb.SegmentState_Growing))))
	assert.Equal(t, 2, len(metaCache.GetSegmentsBy(metacache.WithSegmentIDs(200, 201), metacache.WithSegmentState(commonpb.SegmentState_Flushed))))
}

type DataSyncServiceSuite struct {
	suite.Suite
	MockDataSuiteBase

	node         *DataNode // node param
	chunkManager *mocks.ChunkManager
	broker       *broker.MockBroker
	allocator    *allocator.MockAllocator
	wbManager    *writebuffer.MockBufferManager

	factory *dependency.MockFactory
	ms      *msgstream.MockMsgStream
	msChan  chan *msgstream.MsgPack
}

func (s *DataSyncServiceSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.MockDataSuiteBase.prepareData()
}

func (s *DataSyncServiceSuite) SetupTest() {
	s.node = &DataNode{}

	s.chunkManager = mocks.NewChunkManager(s.T())
	s.broker = broker.NewMockBroker(s.T())
	s.allocator = allocator.NewMockAllocator(s.T())
	s.wbManager = writebuffer.NewMockBufferManager(s.T())

	s.broker.EXPECT().UpdateSegmentStatistics(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.node.chunkManager = s.chunkManager
	s.node.broker = s.broker
	s.node.allocator = s.allocator
	s.node.writeBufferManager = s.wbManager
	s.node.session = &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 1,
		},
	}
	s.node.ctx = context.Background()
	s.node.channelCheckpointUpdater = newChannelCheckpointUpdater(s.node)
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.ChannelCheckpointUpdateTickInSeconds.Key, "0.01")
	defer paramtable.Get().Save(paramtable.Get().DataNodeCfg.ChannelCheckpointUpdateTickInSeconds.Key, "10")
	go s.node.channelCheckpointUpdater.start()
	s.msChan = make(chan *msgstream.MsgPack)

	s.factory = dependency.NewMockFactory(s.T())
	s.ms = msgstream.NewMockMsgStream(s.T())
	s.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(s.ms, nil)
	s.ms.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.ms.EXPECT().Chan().Return(s.msChan)
	s.ms.EXPECT().Close().Return()

	s.node.factory = s.factory
	s.node.dispClient = msgdispatcher.NewClient(s.factory, typeutil.DataNodeRole, 1)

	s.node.timeTickSender = newTimeTickSender(s.broker, 0)
}

func (s *DataSyncServiceSuite) TestStartStop() {
	var (
		insertChannelName = fmt.Sprintf("by-dev-rootcoord-dml-%d", rand.Int())

		Factory  = &MetaFactory{}
		collMeta = Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Call.Return(
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
	s.wbManager.EXPECT().Register(insertChannelName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

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
	var ufsIDs []int64
	var fsIDs []int64
	for _, segmentInfo := range ufs {
		ufsIDs = append(ufsIDs, segmentInfo.ID)
	}
	for _, segmentInfo := range fs {
		fsIDs = append(fsIDs, segmentInfo.ID)
	}

	watchInfo := &datapb.ChannelWatchInfo{
		Schema: collMeta.GetSchema(),
		Vchan: &datapb.VchannelInfo{
			CollectionID:        collMeta.ID,
			ChannelName:         insertChannelName,
			UnflushedSegmentIds: ufsIDs,
			FlushedSegmentIds:   fsIDs,
		},
	}

	sync, err := newServiceWithEtcdTickler(
		ctx,
		s.node,
		watchInfo,
		genTestTickler(),
	)
	s.Require().NoError(err)
	s.Require().NotNil(sync)

	sync.start()
	defer sync.close()

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64 - 1,
	}

	msgTs := tsoutil.GetCurrentTime()
	dataFactory := NewDataFactory()
	insertMessages := dataFactory.GetMsgStreamTsInsertMsgs(2, insertChannelName, msgTs)

	msgPack := msgstream.MsgPack{
		BeginTs: timeRange.timestampMin,
		EndTs:   timeRange.timestampMax,
		Msgs:    insertMessages,
		StartPositions: []*msgpb.MsgPosition{{
			Timestamp:   msgTs,
			ChannelName: insertChannelName,
		}},
		EndPositions: []*msgpb.MsgPosition{{
			Timestamp:   msgTs,
			ChannelName: insertChannelName,
		}},
	}

	// generate timeTick
	timeTickMsgPack := msgstream.MsgPack{}

	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: tsoutil.GetCurrentTime(),
			EndTimestamp:   tsoutil.GetCurrentTime(),
			HashValues:     []uint32{0},
		},
		TimeTickMsg: &msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
				MsgID:     UniqueID(0),
				Timestamp: tsoutil.GetCurrentTime(),
				SourceID:  0,
			},
		},
	}
	timeTickMsgPack.Msgs = append(timeTickMsgPack.Msgs, timeTickMsg)

	s.wbManager.EXPECT().BufferData(insertChannelName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.wbManager.EXPECT().GetCheckpoint(insertChannelName).Return(&msgpb.MsgPosition{Timestamp: msgTs, ChannelName: insertChannelName, MsgID: []byte{0}}, true, nil)
	s.wbManager.EXPECT().NotifyCheckpointUpdated(insertChannelName, msgTs).Return().Maybe()

	ch := make(chan struct{})

	s.broker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ []*msgpb.MsgPosition) error {
		close(ch)
		return nil
	})
	s.msChan <- &msgPack
	s.msChan <- &timeTickMsgPack
	<-ch
}

func TestDataSyncService(t *testing.T) {
	suite.Run(t, new(DataSyncServiceSuite))
}
