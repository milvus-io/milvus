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
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var dataSyncServiceTestDir = "/tmp/milvus_test/data_sync_service"

func init() {
	paramtable.Init()
}

type testInfo struct {
	isValidCase  bool
	inMsgFactory dependency.Factory

	description string
}

func TestDataSyncService_newDataSyncService(t *testing.T) {
	ctx := context.Background()

	tests := []*testInfo{
		{
			true, &mockMsgStreamFactory{false, true},
			"SetParamsReturnError",
		},
		{
			true, &mockMsgStreamFactory{true, true},
			"add normal segments",
		},
		{
			true, &mockMsgStreamFactory{true, true},
			"add un-flushed and flushed segments",
		},
	}
	cm := storage.NewLocalChunkManager(storage.RootPath(dataSyncServiceTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	node := newIDLEDataNodeMock(ctx, schemapb.DataType_Int64)
	dataCh := make(chan *datapb.SegmentInfo, 10)

	insertChannelName := fmt.Sprintf("by-dev-rootcoord-dml-%d", rand.Int())

	segs := []*datapb.SegmentInfo{
		{
			PartitionID:   1,
			InsertChannel: insertChannelName,
			ID:            0,
			DmlPosition:   &msgpb.MsgPosition{},
			State:         commonpb.SegmentState_Growing,
		},
		{
			PartitionID:   1,
			InsertChannel: insertChannelName,
			ID:            1,
			DmlPosition:   &msgpb.MsgPosition{},
			State:         commonpb.SegmentState_Flushed,
		},
	}

	for i := range segs {
		dataCh <- segs[i]
	}

	mockClientStream := broker.NewMockListChanSegInfoClient(t)
	mockClientStream.EXPECT().Recv().RunAndReturn(
		func() (*datapb.SegmentInfo, error) {
			if len(dataCh) > 0 {
				return <-dataCh, nil
			}
			return nil, io.EOF
		})

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ListChannelSegmentInfo(mock.Anything, mock.Anything).Return(mockClientStream, nil)
	node.broker = broker

	Factory := &MetaFactory{}
	collMeta := Factory.GetCollectionMeta(UniqueID(0), "coll1", schemapb.DataType_Int64)

	watchInfo := &datapb.ChannelWatchInfo{
		Schema: collMeta.GetSchema(),
		Vchan: &datapb.VchannelInfo{
			CollectionID: collMeta.ID,
			ChannelName:  insertChannelName,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			node.factory = test.inMsgFactory
			ds, err := newServiceWithEtcdTickler(
				ctx,
				node,
				watchInfo,
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
	s.wbManager.EXPECT().Register(insertChannelName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	dataCh := make(chan *datapb.SegmentInfo, 10)
	segs := []*datapb.SegmentInfo{
		{
			CollectionID:  collMeta.ID,
			PartitionID:   1,
			InsertChannel: insertChannelName,
			ID:            0,
			NumOfRows:     0,
			DmlPosition:   &msgpb.MsgPosition{},
			State:         commonpb.SegmentState_Growing,
		},
		{
			CollectionID:  collMeta.ID,
			PartitionID:   1,
			InsertChannel: insertChannelName,
			ID:            1,
			NumOfRows:     0,
			DmlPosition:   &msgpb.MsgPosition{},
			State:         commonpb.SegmentState_Flushed,
		},
	}

	for i := range segs {
		dataCh <- segs[i]
	}

	mockClientStream := broker.NewMockListChanSegInfoClient(s.T())
	mockClientStream.EXPECT().Recv().RunAndReturn(
		func() (*datapb.SegmentInfo, error) {
			if len(dataCh) > 0 {
				return <-dataCh, nil
			}
			return nil, io.EOF
		})

	s.broker.EXPECT().ListChannelSegmentInfo(mock.Anything, mock.Anything).Return(mockClientStream, nil)

	watchInfo := &datapb.ChannelWatchInfo{
		Schema: collMeta.GetSchema(),
		Vchan: &datapb.VchannelInfo{
			CollectionID: collMeta.ID,
			ChannelName:  insertChannelName,
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
		TimeTickMsg: msgpb.TimeTickMsg{
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
	s.wbManager.EXPECT().GetCheckpoint(insertChannelName).Return(&msgpb.MsgPosition{Timestamp: msgTs}, true, nil)
	s.wbManager.EXPECT().NotifyCheckpointUpdated(insertChannelName, msgTs).Return().Maybe()

	ch := make(chan struct{})

	s.broker.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ string, _ *msgpb.MsgPosition) error {
		close(ch)
		return nil
	})
	s.msChan <- &msgPack
	s.msChan <- &timeTickMsgPack
	<-ch
}

func TestDataSyncServiceSuite(t *testing.T) {
	suite.Run(t, new(DataSyncServiceSuite))
}
