// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package querynodev2

import (
	"context"
	"io"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ServiceSuite struct {
	suite.Suite
	// Data
	msgChan        chan *msgstream.MsgPack
	collectionID   int64
	collectionName string
	schema         *schemapb.CollectionSchema
	partitionIDs   []int64
	// Test segments
	validSegmentIDs     []int64
	flushedSegmentIDs   []int64
	droppedSegmentIDs   []int64
	levelZeroSegmentIDs []int64
	// Test channel
	vchannel string
	pchannel string
	channel  metautil.Channel
	mapper   metautil.ChannelMapper
	position *msgpb.MsgPosition

	// Dependency
	node                *QueryNode
	etcdClient          *clientv3.Client
	rootPath            string
	chunkManagerFactory *storage.ChunkManagerFactory

	// Mock
	factory   *dependency.MockFactory
	msgStream *msgstream.MockMsgStream
}

func (suite *ServiceSuite) SetupSuite() {
	// collection and segments data
	// init param
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.GCEnabled.Key, "false")

	suite.rootPath = suite.T().Name()
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.partitionIDs = []int64{222}
	suite.validSegmentIDs = []int64{1, 2, 3}
	suite.flushedSegmentIDs = []int64{5, 6}
	suite.droppedSegmentIDs = []int64{7, 8, 9}
	suite.levelZeroSegmentIDs = []int64{4}

	var err error
	suite.mapper = metautil.NewDynChannelMapper()
	// channel data
	suite.vchannel = "by-dev-rootcoord-dml_0_111v0"
	suite.pchannel = funcutil.ToPhysicalChannel(suite.vchannel)
	suite.channel, err = metautil.ParseChannel(suite.vchannel, suite.mapper)
	suite.Require().NoError(err)

	suite.position = &msgpb.MsgPosition{
		ChannelName: suite.vchannel,
		MsgID:       []byte{0, 0, 0, 0, 0, 0, 0, 0},
	}
}

func (suite *ServiceSuite) SetupTest() {
	ctx := context.Background()
	// init mock
	suite.factory = dependency.NewMockFactory(suite.T())
	suite.msgStream = msgstream.NewMockMsgStream(suite.T())
	// TODO:: cpp chunk manager not support local chunk manager
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, suite.T().TempDir())
	// suite.chunkManagerFactory = storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus-test"))
	suite.chunkManagerFactory = storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.factory.EXPECT().Init(mock.Anything).Return()
	suite.factory.EXPECT().NewPersistentStorageChunkManager(mock.Anything).Return(suite.chunkManagerFactory.NewPersistentStorageChunkManager(ctx))

	var err error
	suite.node = NewQueryNode(ctx, suite.factory)
	// init etcd
	suite.etcdClient, err = etcd.GetEtcdClient(
		paramtable.Get().EtcdCfg.UseEmbedEtcd.GetAsBool(),
		paramtable.Get().EtcdCfg.EtcdUseSSL.GetAsBool(),
		paramtable.Get().EtcdCfg.Endpoints.GetAsStrings(),
		paramtable.Get().EtcdCfg.EtcdTLSCert.GetValue(),
		paramtable.Get().EtcdCfg.EtcdTLSKey.GetValue(),
		paramtable.Get().EtcdCfg.EtcdTLSCACert.GetValue(),
		paramtable.Get().EtcdCfg.EtcdTLSMinVersion.GetValue())
	suite.NoError(err)
	suite.node.SetEtcdClient(suite.etcdClient)
	// init node
	err = suite.node.Init()
	suite.NoError(err)
	// start node
	err = suite.node.Start()
	suite.NoError(err)
}

func (suite *ServiceSuite) TearDownTest() {
	suite.node.UpdateStateCode(commonpb.StateCode_Healthy)
	ctx := context.Background()
	// ReleaseSegment, avoid throwing an instance of 'std::system_error' when stop node
	resp, err := suite.node.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_ReleaseSegments,
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		SegmentIDs:   suite.validSegmentIDs,
		NodeID:       suite.node.session.ServerID,
		Scope:        querypb.DataScope_All,
		Shard:        suite.vchannel,
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	suite.node.chunkManager.RemoveWithPrefix(ctx, paramtable.Get().LocalStorageCfg.Path.GetValue())
	suite.node.Stop()
	suite.etcdClient.Close()
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
}

func (suite *ServiceSuite) TestGetComponentStatesNormal() {
	ctx := context.Background()
	suite.node.session.UpdateRegistered(true)
	rsp, err := suite.node.GetComponentStates(ctx, nil)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
	suite.Equal(commonpb.StateCode_Healthy, rsp.State.StateCode)

	// after update
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	rsp, err = suite.node.GetComponentStates(ctx, nil)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
	suite.Equal(commonpb.StateCode_Abnormal, rsp.State.StateCode)
}

func (suite *ServiceSuite) TestGetTimeTiclChannel_Normal() {
	ctx := context.Background()
	rsp, err := suite.node.GetTimeTickChannel(ctx, nil)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestGetStatisChannel_Normal() {
	ctx := context.Background()
	rsp, err := suite.node.GetStatisticsChannel(ctx, nil)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestGetStatistics_Normal() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	req := &querypb.GetStatisticsRequest{
		Req: &internalpb.GetStatisticsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchDmChannels,
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{},
		},
		DmlChannels: []string{suite.vchannel},
		SegmentIDs:  suite.validSegmentIDs,
	}

	rsp, err := suite.node.GetStatistics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestGetStatistics_Failed() {
	ctx := context.Background()
	// prepare
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	req := &querypb.GetStatisticsRequest{
		Req: &internalpb.GetStatisticsRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_WatchDmChannels,
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{},
		},
		DmlChannels: []string{suite.vchannel},
		SegmentIDs:  suite.validSegmentIDs,
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := suite.node.GetStatistics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestWatchDmChannelsInt64() {
	ctx := context.Background()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	deltaLogs, err := mock_segcore.SaveDeltaLog(suite.collectionID,
		suite.partitionIDs[0],
		suite.flushedSegmentIDs[0],
		suite.node.chunkManager,
	)
	suite.NoError(err)

	req := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchDmChannels,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		Infos: []*datapb.VchannelInfo{
			{
				CollectionID:        suite.collectionID,
				ChannelName:         suite.vchannel,
				SeekPosition:        suite.position,
				FlushedSegmentIds:   suite.flushedSegmentIDs,
				DroppedSegmentIds:   suite.droppedSegmentIDs,
				LevelZeroSegmentIds: suite.levelZeroSegmentIDs,
			},
		},
		SegmentInfos: map[int64]*datapb.SegmentInfo{
			suite.levelZeroSegmentIDs[0]: {
				ID:            suite.levelZeroSegmentIDs[0],
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionIDs[0],
				InsertChannel: suite.vchannel,
				Deltalogs:     deltaLogs,
				Level:         datapb.SegmentLevel_L0,
			},
		},
		Schema: schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
			MetricType:   defaultMetricType,
		},
		IndexInfoList: mock_segcore.GenTestIndexInfoList(suite.collectionID, schema),
	}

	// mocks
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer(mock.Anything, []string{suite.pchannel}, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Chan().Return(suite.msgChan)
	suite.msgStream.EXPECT().Close()

	// watchDmChannels
	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)

	// watch channel exist
	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func (suite *ServiceSuite) TestWatchDmChannelsVarchar() {
	ctx := context.Background()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_VarChar, false)

	req := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchDmChannels,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		Infos: []*datapb.VchannelInfo{
			{
				CollectionID:      suite.collectionID,
				ChannelName:       suite.vchannel,
				SeekPosition:      suite.position,
				FlushedSegmentIds: suite.flushedSegmentIDs,
				DroppedSegmentIds: suite.droppedSegmentIDs,
			},
		},
		Schema: schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
			MetricType:   defaultMetricType,
		},
		IndexInfoList: mock_segcore.GenTestIndexInfoList(suite.collectionID, schema),
	}

	// mocks
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer(mock.Anything, []string{suite.pchannel}, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Chan().Return(suite.msgChan)
	suite.msgStream.EXPECT().Close()

	// watchDmChannels
	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)

	// watch channel exist
	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func (suite *ServiceSuite) TestWatchDmChannels_BadIndexMeta() {
	ctx := context.Background()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	deltaLogs, err := mock_segcore.SaveDeltaLog(suite.collectionID,
		suite.partitionIDs[0],
		suite.flushedSegmentIDs[0],
		suite.node.chunkManager,
	)
	suite.NoError(err)

	req := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchDmChannels,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		Infos: []*datapb.VchannelInfo{
			{
				CollectionID:        suite.collectionID,
				ChannelName:         suite.vchannel,
				SeekPosition:        suite.position,
				FlushedSegmentIds:   suite.flushedSegmentIDs,
				DroppedSegmentIds:   suite.droppedSegmentIDs,
				LevelZeroSegmentIds: suite.levelZeroSegmentIDs,
			},
		},
		SegmentInfos: map[int64]*datapb.SegmentInfo{
			suite.levelZeroSegmentIDs[0]: {
				ID:            suite.levelZeroSegmentIDs[0],
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionIDs[0],
				InsertChannel: suite.vchannel,
				Deltalogs:     deltaLogs,
				Level:         datapb.SegmentLevel_L0,
			},
		},
		Schema: schema,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
			MetricType:   defaultMetricType,
		},
		IndexInfoList: []*indexpb.IndexInfo{{
			IndexName: "bad_index",
			FieldID:   100,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dup_key", Value: "val"},
				{Key: "dup_key", Value: "val"},
			},
		}},
	}

	// watchDmChannels
	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.Error(merr.CheckRPCCall(status, err))
}

func (suite *ServiceSuite) TestWatchDmChannels_Failed() {
	ctx := context.Background()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)

	indexInfos := mock_segcore.GenTestIndexInfoList(suite.collectionID, schema)

	infos := suite.genSegmentLoadInfos(schema, indexInfos)
	segmentInfos := lo.SliceToMap(infos, func(info *querypb.SegmentLoadInfo) (int64, *datapb.SegmentInfo) {
		return info.SegmentID, &datapb.SegmentInfo{
			ID:            info.SegmentID,
			CollectionID:  info.CollectionID,
			PartitionID:   info.PartitionID,
			InsertChannel: info.InsertChannel,
			Binlogs:       info.BinlogPaths,
			Statslogs:     info.Statslogs,
			Deltalogs:     info.Deltalogs,
			Level:         info.Level,
		}
	})

	req := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_WatchDmChannels,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		Infos: []*datapb.VchannelInfo{
			{
				CollectionID:      suite.collectionID,
				ChannelName:       suite.vchannel,
				SeekPosition:      suite.position,
				FlushedSegmentIds: suite.flushedSegmentIDs,
				DroppedSegmentIds: suite.droppedSegmentIDs,
			},
		},
		Schema: schema,
		LoadMeta: &querypb.LoadMetaInfo{
			MetricType: defaultMetricType,
		},
		SegmentInfos:  segmentInfos,
		IndexInfoList: indexInfos,
	}

	// test channel is unsubscribing
	suite.node.unsubscribingChannels.Insert(suite.vchannel)
	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrChannelReduplicate)
	suite.node.unsubscribingChannels.Remove(suite.vchannel)

	// init msgstream failed
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer(mock.Anything, []string{suite.pchannel}, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Close().Return()
	suite.msgStream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

	// load growing failed
	badSegmentReq := typeutil.Clone(req)
	for _, info := range badSegmentReq.SegmentInfos {
		for _, fbl := range info.Binlogs {
			for _, binlog := range fbl.Binlogs {
				binlog.LogPath += "bad_suffix"
			}
		}
	}
	for _, channel := range badSegmentReq.Infos {
		channel.UnflushedSegmentIds = lo.Keys(badSegmentReq.SegmentInfos)
	}
	status, err = suite.node.WatchDmChannels(ctx, badSegmentReq)
	err = merr.CheckRPCCall(status, err)
	suite.Error(err)

	// empty index
	req.IndexInfoList = nil
	status, err = suite.node.WatchDmChannels(ctx, req)
	err = merr.CheckRPCCall(status, err)
	suite.ErrorIs(err, merr.ErrIndexNotFound)

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) TestUnsubDmChannels_Normal() {
	ctx := context.Background()

	// prepate
	suite.TestWatchDmChannelsInt64()

	l0Segment := segments.NewMockSegment(suite.T())
	l0Segment.EXPECT().ID().Return(10000)
	l0Segment.EXPECT().Collection().Return(suite.collectionID)
	l0Segment.EXPECT().Level().Return(datapb.SegmentLevel_L0)
	l0Segment.EXPECT().Type().Return(commonpb.SegmentState_Sealed)
	l0Segment.EXPECT().Shard().Return(suite.channel)
	l0Segment.EXPECT().Release(ctx).Return()

	suite.node.manager.Segment.Put(ctx, segments.SegmentTypeSealed, l0Segment)

	// data
	req := &querypb.UnsubDmChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_UnsubDmChannel,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		ChannelName:  suite.vchannel,
	}

	status, err := suite.node.UnsubDmChannel(ctx, req)
	suite.NoError(merr.CheckRPCCall(status, err))

	suite.Len(suite.node.manager.Segment.GetBy(
		segments.WithChannel(suite.vchannel),
		segments.WithLevel(datapb.SegmentLevel_L0)), 0)
}

func (suite *ServiceSuite) TestUnsubDmChannels_Failed() {
	ctx := context.Background()
	// prepate
	suite.TestWatchDmChannelsInt64()

	// data
	req := &querypb.UnsubDmChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_UnsubDmChannel,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		NodeID:       suite.node.session.ServerID,
		CollectionID: suite.collectionID,
		ChannelName:  suite.vchannel,
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := suite.node.UnsubDmChannel(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) genSegmentLoadInfos(schema *schemapb.CollectionSchema,
	indexInfos []*indexpb.IndexInfo,
) []*querypb.SegmentLoadInfo {
	ctx := context.Background()

	segNum := len(suite.validSegmentIDs)
	partNum := len(suite.partitionIDs)
	infos := make([]*querypb.SegmentLoadInfo, 0)
	for i := 0; i < segNum; i++ {
		binlogs, statslogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionIDs[i%partNum],
			suite.validSegmentIDs[i],
			1000,
			schema,
			suite.node.chunkManager,
		)
		suite.Require().NoError(err)

		vectorFieldSchemas := typeutil.GetVectorFieldSchemas(schema)
		indexes := make([]*querypb.FieldIndexInfo, 0)
		for offset, field := range vectorFieldSchemas {
			indexInfo := lo.FindOrElse(indexInfos, nil, func(info *indexpb.IndexInfo) bool { return info.FieldID == field.GetFieldID() })
			if indexInfo != nil {
				index, err := mock_segcore.GenAndSaveIndexV2(
					suite.collectionID,
					suite.partitionIDs[i%partNum],
					suite.validSegmentIDs[i],
					int64(offset),
					field,
					indexInfo,
					suite.node.chunkManager,
					1000,
				)
				suite.Require().NoError(err)
				indexes = append(indexes, index)
			}
		}

		info := &querypb.SegmentLoadInfo{
			SegmentID:     suite.validSegmentIDs[i],
			PartitionID:   suite.partitionIDs[i%partNum],
			CollectionID:  suite.collectionID,
			InsertChannel: suite.vchannel,
			NumOfRows:     1000,
			BinlogPaths:   binlogs,
			Statslogs:     statslogs,
			IndexInfos:    indexes,
			StartPosition: &msgpb.MsgPosition{Timestamp: 20000},
			DeltaPosition: &msgpb.MsgPosition{Timestamp: 20000},
		}
		infos = append(infos, info)
	}
	return infos
}

func (suite *ServiceSuite) TestLoadSegments_Int64() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	indexInfos := mock_segcore.GenTestIndexInfoList(suite.collectionID, schema)
	infos := suite.genSegmentLoadInfos(schema, indexInfos)
	for _, info := range infos {
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:   suite.collectionID,
			DstNodeID:      suite.node.session.ServerID,
			Infos:          []*querypb.SegmentLoadInfo{info},
			Schema:         schema,
			DeltaPositions: []*msgpb.MsgPosition{{Timestamp: 20000}},
			NeedTransfer:   true,
			IndexInfoList:  indexInfos,
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	}
}

func (suite *ServiceSuite) TestLoadSegments_VarChar() {
	ctx := context.Background()
	suite.TestWatchDmChannelsVarchar()
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_VarChar, false)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
	}
	suite.node.manager.Collection = segments.NewCollectionManager()
	suite.node.manager.Collection.PutOrRef(suite.collectionID, schema, nil, loadMeta)

	infos := suite.genSegmentLoadInfos(schema, nil)
	for _, info := range infos {
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:   suite.collectionID,
			DstNodeID:      suite.node.session.ServerID,
			Infos:          []*querypb.SegmentLoadInfo{info},
			Schema:         schema,
			DeltaPositions: []*msgpb.MsgPosition{{Timestamp: 20000}},
			NeedTransfer:   true,
			LoadMeta:       loadMeta,
			IndexInfoList:  []*indexpb.IndexInfo{{}},
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	}
}

func (suite *ServiceSuite) TestLoadSegments_BadIndexMeta() {
	ctx := context.Background()
	suite.TestWatchDmChannelsVarchar()
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_VarChar, false)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
	}
	suite.node.manager.Collection = segments.NewCollectionManager()
	// suite.node.manager.Collection.PutOrRef(suite.collectionID, schema, nil, loadMeta)

	infos := suite.genSegmentLoadInfos(schema, nil)
	for _, info := range infos {
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:   suite.collectionID,
			DstNodeID:      suite.node.session.ServerID,
			Infos:          []*querypb.SegmentLoadInfo{info},
			Schema:         schema,
			DeltaPositions: []*msgpb.MsgPosition{{Timestamp: 20000}},
			NeedTransfer:   true,
			LoadMeta:       loadMeta,
			IndexInfoList: []*indexpb.IndexInfo{{
				IndexName: "bad_index",
				FieldID:   100,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dup_key", Value: "val"},
					{Key: "dup_key", Value: "val"},
				},
			}},
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.Error(merr.CheckRPCCall(status, err))
	}
}

func (suite *ServiceSuite) TestLoadDeltaInt64() {
	ctx := context.Background()
	suite.TestLoadSegments_Int64()
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:  suite.collectionID,
		DstNodeID:     suite.node.session.ServerID,
		Infos:         suite.genSegmentLoadInfos(schema, nil),
		Schema:        schema,
		NeedTransfer:  true,
		LoadScope:     querypb.LoadScope_Delta,
		IndexInfoList: []*indexpb.IndexInfo{{}},
	}

	// LoadSegment
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
}

func (suite *ServiceSuite) TestLoadDeltaVarchar() {
	ctx := context.Background()
	suite.TestLoadSegments_VarChar()
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:  suite.collectionID,
		DstNodeID:     suite.node.session.ServerID,
		Infos:         suite.genSegmentLoadInfos(schema, nil),
		Schema:        schema,
		NeedTransfer:  true,
		LoadScope:     querypb.LoadScope_Delta,
		IndexInfoList: []*indexpb.IndexInfo{{}},
	}

	// LoadSegment
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
}

func (suite *ServiceSuite) TestLoadIndex_Success() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)

	indexInfos := mock_segcore.GenTestIndexInfoList(suite.collectionID, schema)
	infos := suite.genSegmentLoadInfos(schema, indexInfos)
	infos = lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *querypb.SegmentLoadInfo {
		info.SegmentID = info.SegmentID + 1000
		return info
	})
	rawInfo := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *querypb.SegmentLoadInfo {
		info = typeutil.Clone(info)
		info.IndexInfos = nil
		return info
	})

	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:  suite.collectionID,
		DstNodeID:     suite.node.session.ServerID,
		Infos:         rawInfo,
		Schema:        schema,
		NeedTransfer:  false,
		LoadScope:     querypb.LoadScope_Full,
		IndexInfoList: indexInfos,
	}

	// Load segment
	status, err := suite.node.LoadSegments(ctx, req)
	suite.Require().NoError(err)
	suite.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode())

	for _, segmentID := range lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
		return info.GetSegmentID()
	}) {
		suite.Equal(0, len(suite.node.manager.Segment.Get(segmentID).Indexes()))
	}

	req = &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:  suite.collectionID,
		DstNodeID:     suite.node.session.ServerID,
		Infos:         infos,
		Schema:        schema,
		NeedTransfer:  false,
		LoadScope:     querypb.LoadScope_Index,
		IndexInfoList: []*indexpb.IndexInfo{{}},
	}

	// Load segment
	status, err = suite.node.LoadSegments(ctx, req)
	suite.Require().NoError(err)
	suite.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode())

	for _, segmentID := range lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
		return info.GetSegmentID()
	}) {
		suite.T().Log(segmentID)
		suite.T().Log(len(suite.node.manager.Segment.Get(segmentID).Indexes()))
		suite.Greater(len(suite.node.manager.Segment.Get(segmentID).Indexes()), 0)
	}
}

func (suite *ServiceSuite) TestLoadIndex_Failed() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)

	suite.Run("load_non_exist_segment", func() {
		indexInfos := mock_segcore.GenTestIndexInfoList(suite.collectionID, schema)
		infos := suite.genSegmentLoadInfos(schema, indexInfos)
		infos = lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *querypb.SegmentLoadInfo {
			info.SegmentID = info.SegmentID + 1000
			return info
		})
		rawInfo := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *querypb.SegmentLoadInfo {
			info = typeutil.Clone(info)
			info.IndexInfos = nil
			return info
		})
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:  suite.collectionID,
			DstNodeID:     suite.node.session.ServerID,
			Infos:         rawInfo,
			Schema:        schema,
			NeedTransfer:  false,
			LoadScope:     querypb.LoadScope_Index,
			IndexInfoList: indexInfos,
		}

		// Load segment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.Require().NoError(err)
		// Ignore segment missing
		suite.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	suite.Run("loader_returns_error", func() {
		suite.TestLoadSegments_Int64()
		loader := suite.node.loader
		mockLoader := segments.NewMockLoader(suite.T())
		suite.node.loader = mockLoader
		defer func() {
			suite.node.loader = loader
		}()

		mockLoader.EXPECT().LoadIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mocked error"))

		indexInfos := mock_segcore.GenTestIndexInfoList(suite.collectionID, schema)
		infos := suite.genSegmentLoadInfos(schema, indexInfos)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:  suite.collectionID,
			DstNodeID:     suite.node.session.ServerID,
			Infos:         infos,
			Schema:        schema,
			NeedTransfer:  false,
			LoadScope:     querypb.LoadScope_Index,
			IndexInfoList: indexInfos,
		}

		// Load segment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.Require().NoError(err)
		suite.Require().NotEqual(commonpb.ErrorCode_Success, status.GetErrorCode())
	})
}

func (suite *ServiceSuite) TestLoadSegments_Failed() {
	ctx := context.Background()
	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		DstNodeID:    suite.node.session.ServerID,
		Infos:        suite.genSegmentLoadInfos(schema, nil),
		Schema:       schema,
		NeedTransfer: true,
		IndexInfoList: []*indexpb.IndexInfo{
			{},
		},
	}

	// Delegator not found
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrChannelNotFound)

	// IndexIndex not found
	nonIndexReq := typeutil.Clone(req)
	nonIndexReq.IndexInfoList = nil
	status, err = suite.node.LoadSegments(ctx, nonIndexReq)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrIndexNotFound)

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrServiceNotReady)
}

func (suite *ServiceSuite) TestLoadSegments_Transfer() {
	ctx := context.Background()
	suite.Run("normal_run", func() {
		delegator := &delegator.MockShardDelegator{}
		suite.node.delegators.Insert(suite.vchannel, delegator)
		defer suite.node.delegators.GetAndRemove(suite.vchannel)

		delegator.EXPECT().AddExcludedSegments(mock.Anything).Maybe()
		delegator.EXPECT().VerifyExcludedSegments(mock.Anything, mock.Anything).Return(true).Maybe()
		delegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Maybe()
		delegator.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).Return(nil)
		// data
		schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:  suite.collectionID,
			DstNodeID:     suite.node.session.ServerID,
			Infos:         suite.genSegmentLoadInfos(schema, nil),
			Schema:        schema,
			NeedTransfer:  true,
			IndexInfoList: []*indexpb.IndexInfo{{}},
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	suite.Run("delegator_not_found", func() {
		// data
		schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:  suite.collectionID,
			DstNodeID:     suite.node.session.ServerID,
			Infos:         suite.genSegmentLoadInfos(schema, nil),
			Schema:        schema,
			NeedTransfer:  true,
			IndexInfoList: []*indexpb.IndexInfo{{}},
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	suite.Run("delegator_return_error", func() {
		delegator := &delegator.MockShardDelegator{}
		suite.node.delegators.Insert(suite.vchannel, delegator)
		defer suite.node.delegators.GetAndRemove(suite.vchannel)
		delegator.EXPECT().AddExcludedSegments(mock.Anything).Maybe()
		delegator.EXPECT().VerifyExcludedSegments(mock.Anything, mock.Anything).Return(true).Maybe()
		delegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Maybe()
		delegator.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(errors.New("mocked error"))
		// data
		schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID:  suite.collectionID,
			DstNodeID:     suite.node.session.ServerID,
			Infos:         suite.genSegmentLoadInfos(schema, nil),
			Schema:        schema,
			NeedTransfer:  true,
			IndexInfoList: []*indexpb.IndexInfo{{}},
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func (suite *ServiceSuite) TestReleaseCollection_Normal() {
	ctx := context.Background()
	req := &querypb.ReleaseCollectionRequest{}
	status, err := suite.node.ReleaseCollection(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
}

func (suite *ServiceSuite) TestReleaseCollection_Failed() {
	ctx := context.Background()
	req := &querypb.ReleaseCollectionRequest{}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := suite.node.ReleaseCollection(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) TestReleasePartitions_Normal() {
	ctx := context.Background()

	suite.TestWatchDmChannelsInt64()
	req := &querypb.ReleasePartitionsRequest{
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
	}
	status, err := suite.node.ReleasePartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	collection := suite.node.manager.Collection.Get(suite.collectionID)
	for _, partition := range suite.partitionIDs {
		suite.False(collection.ExistPartition(partition))
	}
}

func (suite *ServiceSuite) TestReleasePartitions_Failed() {
	ctx := context.Background()
	req := &querypb.ReleasePartitionsRequest{}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := suite.node.ReleasePartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) TestReleaseSegments_Normal() {
	ctx := context.Background()
	suite.TestLoadSegments_Int64()

	req := &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		SegmentIDs:   suite.validSegmentIDs,
	}

	status, err := suite.node.ReleaseSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
}

func (suite *ServiceSuite) TestReleaseSegments_Failed() {
	ctx := context.Background()
	suite.TestLoadSegments_Int64()

	req := &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		SegmentIDs:   suite.validSegmentIDs,
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := suite.node.ReleaseSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) TestReleaseSegments_Transfer() {
	suite.Run("delegator_not_found", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req := &querypb.ReleaseSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			Shard:        suite.vchannel,
			CollectionID: suite.collectionID,
			SegmentIDs:   suite.validSegmentIDs,
			NeedTransfer: true,
			NodeID:       paramtable.GetNodeID(),
		}

		status, err := suite.node.ReleaseSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})

	suite.Run("normal_run", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		suite.TestLoadSegments_Int64()
		req := &querypb.ReleaseSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			Shard:        suite.vchannel,
			CollectionID: suite.collectionID,
			SegmentIDs:   suite.validSegmentIDs,
			NeedTransfer: true,
			NodeID:       paramtable.GetNodeID(),
		}

		status, err := suite.node.ReleaseSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	suite.Run("delegator_return_error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		delegator := &delegator.MockShardDelegator{}
		suite.node.delegators.Insert(suite.vchannel, delegator)
		defer suite.node.delegators.GetAndRemove(suite.vchannel)

		delegator.EXPECT().AddExcludedSegments(mock.Anything).Maybe()
		delegator.EXPECT().VerifyExcludedSegments(mock.Anything, mock.Anything).Return(true).Maybe()
		delegator.EXPECT().TryCleanExcludedSegments(mock.Anything).Maybe()
		delegator.EXPECT().ReleaseSegments(mock.Anything, mock.AnythingOfType("*querypb.ReleaseSegmentsRequest"), false).
			Return(errors.New("mocked error"))

		req := &querypb.ReleaseSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			Shard:        suite.vchannel,
			CollectionID: suite.collectionID,
			SegmentIDs:   suite.validSegmentIDs,
			NeedTransfer: true,
			NodeID:       paramtable.GetNodeID(),
		}

		status, err := suite.node.ReleaseSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	})
}

func (suite *ServiceSuite) TestGetSegmentInfo_Normal() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	req := &querypb.GetSegmentInfoRequest{
		SegmentIDs: suite.validSegmentIDs,
	}

	rsp, err := suite.node.GetSegmentInfo(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestGetSegmentInfo_Failed() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	req := &querypb.GetSegmentInfoRequest{
		SegmentIDs: suite.validSegmentIDs,
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	rsp, err := suite.node.GetSegmentInfo(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) syncDistribution(ctx context.Context) {
	suite.node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{
		Channel:      suite.vchannel,
		CollectionID: suite.collectionID,
		LoadMeta: &querypb.LoadMetaInfo{
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
		},
		Actions: []*querypb.SyncAction{
			{Type: querypb.SyncType_UpdateVersion, SealedInTarget: suite.validSegmentIDs, TargetVersion: time.Now().UnixNano()},
		},
	})
}

// Test Search
func (suite *ServiceSuite) genCSearchRequest(nq int64, dataType schemapb.DataType, fieldID int64, metricType string, isTopkReduce bool, isRecallEvaluation bool) (*internalpb.SearchRequest, error) {
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}

	plan := genSearchPlan(dataType, fieldID, metricType)
	serializedPlan, err2 := proto.Marshal(plan)
	if err2 != nil {
		return nil, err2
	}
	return &internalpb.SearchRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_Search,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:       suite.collectionID,
		PartitionIDs:       suite.partitionIDs,
		SerializedExprPlan: serializedPlan,
		PlaceholderGroup:   placeHolder,
		DslType:            commonpb.DslType_BoolExprV1,
		Nq:                 nq,
		MvccTimestamp:      typeutil.MaxTimestamp,
		IsTopkReduce:       isTopkReduce,
		IsRecallEvaluation: isRecallEvaluation,
	}, nil
}

func (suite *ServiceSuite) TestSearch_Normal() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	suite.syncDistribution(ctx)

	creq, err := suite.genCSearchRequest(10, schemapb.DataType_FloatVector, 107, defaultMetricType, false, false)
	req := &querypb.SearchRequest{
		Req: creq,

		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}
	suite.NoError(err)

	rsp, err := suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestSearch_Concurrent() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	suite.syncDistribution(ctx)

	concurrency := 16
	futures := make([]*conc.Future[*internalpb.SearchResults], 0, concurrency)
	for i := 0; i < concurrency; i++ {
		future := conc.Go(func() (*internalpb.SearchResults, error) {
			creq, err := suite.genCSearchRequest(30, schemapb.DataType_FloatVector, 107, defaultMetricType, false, false)
			req := &querypb.SearchRequest{
				Req: creq,

				DmlChannels:     []string{suite.vchannel},
				TotalChannelNum: 2,
			}
			suite.NoError(err)
			return suite.node.Search(ctx, req)
		})
		futures = append(futures, future)
	}

	err := conc.AwaitAll(futures...)
	suite.NoError(err)

	for i := range futures {
		suite.True(merr.Ok(futures[i].Value().GetStatus()))
	}
}

func (suite *ServiceSuite) TestSearch_Failed() {
	ctx := context.Background()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCSearchRequest(10, schemapb.DataType_FloatVector, 107, "invalidMetricType", false, false)
	req := &querypb.SearchRequest{
		Req: creq,

		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}
	suite.NoError(err)

	// collection not exist
	resp, err := suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(merr.Code(merr.ErrCollectionNotLoaded), resp.GetStatus().GetCode())
	suite.Contains(resp.GetStatus().GetReason(), merr.ErrCollectionNotLoaded.Error())

	// metric type mismatch
	LoadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
	}
	indexMeta := suite.node.composeIndexMeta(ctx, mock_segcore.GenTestIndexInfoList(suite.collectionID, schema), schema)
	suite.node.manager.Collection.PutOrRef(suite.collectionID, schema, indexMeta, LoadMeta)

	// Delegator not found
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrChannelNotFound)

	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	// suite.syncDistribution(ctx)

	// sync segment data
	syncReq := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		Channel:      suite.vchannel,
		LoadMeta: &querypb.LoadMetaInfo{
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
		},
	}

	syncVersionAction := &querypb.SyncAction{
		Type:           querypb.SyncType_UpdateVersion,
		SealedInTarget: []int64{1, 2, 3, 4},
		TargetVersion:  time.Now().UnixMilli(),
	}

	syncReq.Actions = []*querypb.SyncAction{syncVersionAction}
	status, err := suite.node.SyncDistribution(ctx, syncReq)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)

	// metric type not match
	req.GetReq().MetricType = "IP"
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.GetStatus().GetReason(), "metric type not match")
	req.GetReq().MetricType = "L2"

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestSearchSegments_Unhealthy() {
	ctx := context.Background()

	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)

	req := &querypb.SearchRequest{
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(false, rsp.GetIsTopkReduce())
	suite.Equal(commonpb.ErrorCode_NotReadyServe, rsp.GetStatus().GetErrorCode())
	suite.Equal(merr.Code(merr.ErrServiceNotReady), rsp.GetStatus().GetCode())
}

func (suite *ServiceSuite) TestSearchSegments_Failed() {
	ctx := context.Background()

	// collection found
	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID: -1, // not exist collection id
		},

		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(false, rsp.GetIsTopkReduce())
	suite.Equal(commonpb.ErrorCode_UnexpectedError, rsp.GetStatus().GetErrorCode())
	suite.Equal(merr.Code(merr.ErrCollectionNotLoaded), rsp.GetStatus().GetCode())

	suite.TestWatchDmChannelsInt64()

	req.Req.CollectionID = suite.collectionID

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	rsp, err = suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestSearchSegments_Normal() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	creq, err := suite.genCSearchRequest(10, schemapb.DataType_FloatVector, 107, defaultMetricType, false, false)
	req := &querypb.SearchRequest{
		Req: creq,

		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}
	suite.NoError(err)

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(rsp.GetIsTopkReduce(), false)
	suite.Equal(rsp.GetIsRecallEvaluation(), false)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())

	req.Req, err = suite.genCSearchRequest(10, schemapb.DataType_FloatVector, 107, defaultMetricType, true, true)
	suite.NoError(err)
	rsp, err = suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(rsp.GetIsTopkReduce(), true)
	suite.Equal(rsp.GetIsRecallEvaluation(), true)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestStreamingSearch() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.UseStreamComputing.Key, "true")
	creq, err := suite.genCSearchRequest(10, schemapb.DataType_FloatVector, 107, defaultMetricType, false, true)
	req := &querypb.SearchRequest{
		Req:             creq,
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
		SegmentIDs:      suite.validSegmentIDs,
		Scope:           querypb.DataScope_Historical,
	}
	suite.NoError(err)

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(false, rsp.GetIsTopkReduce())
	suite.Equal(true, rsp.GetIsRecallEvaluation())
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestStreamingSearchGrowing() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.UseStreamComputing.Key, "true")
	creq, err := suite.genCSearchRequest(10, schemapb.DataType_FloatVector, 107, defaultMetricType, false, false)
	req := &querypb.SearchRequest{
		Req:             creq,
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
		Scope:           querypb.DataScope_Streaming,
	}
	suite.NoError(err)

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

// Test Query
func (suite *ServiceSuite) genCQueryRequest(nq int64, indexType string, schema *schemapb.CollectionSchema) (*internalpb.RetrieveRequest, error) {
	expr, err := genSimpleRetrievePlanExpr(schema)
	if err != nil {
		return nil, err
	}

	return &internalpb.RetrieveRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_Retrieve,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		// 106 is fieldID of int64 pk
		OutputFieldsId:     []int64{106, 100},
		SerializedExprPlan: expr,
	}, nil
}

func (suite *ServiceSuite) TestQuery_Normal() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	suite.syncDistribution(ctx)

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req: creq,

		DmlChannels: []string{suite.vchannel},
	}

	rsp, err := suite.node.Query(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestQuery_Failed() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req: creq,

		DmlChannels: []string{suite.vchannel},
	}

	// Delegator not found
	resp, err := suite.node.Query(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrChannelNotFound)

	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.Query(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestQuerySegments_Failed() {
	ctx := context.Background()

	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID: -1,
		},

		DmlChannels: []string{suite.vchannel},
	}

	rsp, err := suite.node.QuerySegments(ctx, req)
	suite.NoError(err)

	suite.Equal(commonpb.ErrorCode_UnexpectedError, rsp.GetStatus().GetErrorCode())
	suite.Equal(merr.Code(merr.ErrCollectionNotLoaded), rsp.GetStatus().GetCode())

	suite.TestWatchDmChannelsInt64()

	req.Req.CollectionID = suite.collectionID

	ctx, cancel := context.WithCancel(ctx)
	cancel()

	rsp, err = suite.node.QuerySegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestQueryStream_Normal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prepare
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	suite.syncDistribution(ctx)

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req: creq,

		DmlChannels: []string{suite.vchannel},
	}

	client := streamrpc.NewLocalQueryClient(ctx)
	server := client.CreateServer()

	go func() {
		err := suite.node.QueryStream(req, server)
		suite.NoError(err)
		server.FinishSend(err)
	}()

	for {
		result, err := client.Recv()
		if err == io.EOF {
			break
		}
		suite.NoError(err)

		err = merr.Error(result.GetStatus())
		suite.NoError(err)
	}
}

func (suite *ServiceSuite) TestQueryStream_Failed() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req: creq,

		DmlChannels: []string{suite.vchannel},
	}

	queryFunc := func(wg *sync.WaitGroup, req *querypb.QueryRequest, client *streamrpc.LocalQueryClient) {
		server := client.CreateServer()

		defer wg.Done()
		err := suite.node.QueryStream(req, server)
		suite.NoError(err)
		server.FinishSend(err)
	}

	// Delegator not found
	suite.Run("delegator not found", func() {
		client := streamrpc.NewLocalQueryClient(ctx)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go queryFunc(wg, req, client)

		for {
			result, err := client.Recv()
			if err == io.EOF {
				break
			}
			suite.NoError(err)

			err = merr.Error(result.GetStatus())
			// Check result
			if err != nil {
				suite.ErrorIs(err, merr.ErrChannelNotFound)
			}
		}
		wg.Wait()
	})

	// prepare
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// node not healthy
	suite.Run("node not healthy", func() {
		suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
		client := streamrpc.NewLocalQueryClient(ctx)
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go queryFunc(wg, req, client)

		for {
			result, err := client.Recv()
			if err == io.EOF {
				break
			}
			suite.NoError(err)

			err = merr.Error(result.GetStatus())
			if err != nil {
				suite.True(errors.Is(err, merr.ErrServiceNotReady))
			}
		}
		wg.Wait()
	})
}

func (suite *ServiceSuite) TestQuerySegments_Normal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req: creq,

		DmlChannels: []string{suite.vchannel},
	}

	rsp, err := suite.node.QuerySegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestQueryStreamSegments_Normal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// data
	schema := mock_segcore.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64, false)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req: creq,

		DmlChannels: []string{suite.vchannel},
	}

	client := streamrpc.NewLocalQueryClient(ctx)
	server := client.CreateServer()

	go func() {
		err := suite.node.QueryStreamSegments(req, server)
		suite.NoError(err)
		server.FinishSend(err)
	}()

	for {
		result, err := client.Recv()
		if err == io.EOF {
			break
		}
		suite.NoError(err)

		err = merr.Error(result.GetStatus())
		suite.NoError(err)
		// Check result
		if !errors.Is(err, nil) {
			suite.NoError(err)
			break
		}
	}
}

func (suite *ServiceSuite) TestSyncReplicaSegments_Normal() {
	ctx := context.Background()
	req := &querypb.SyncReplicaSegmentsRequest{}
	status, err := suite.node.SyncReplicaSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func (suite *ServiceSuite) TestShowConfigurations_Normal() {
	ctx := context.Background()
	req := &internalpb.ShowConfigurationsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		Pattern: "mmap.growingMmapEnabled",
	}

	resp, err := suite.node.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	suite.Equal(1, len(resp.Configuations))
}

func (suite *ServiceSuite) TestShowConfigurations_Failed() {
	ctx := context.Background()
	req := &internalpb.ShowConfigurationsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		Pattern: "mmap.growingMmapEnabled",
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := suite.node.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestGetMetric_Normal() {
	ctx := context.Background()
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	suite.NoError(err)

	sd1 := delegator.NewMockShardDelegator(suite.T())
	sd1.EXPECT().Collection().Return(100)
	sd1.EXPECT().GetDeleteBufferSize().Return(10, 1000)
	sd1.EXPECT().GetTSafe().Return(100)
	sd1.EXPECT().Close().Maybe()
	suite.node.delegators.Insert("qn_unitest_dml_0_100v0", sd1)
	defer suite.node.delegators.GetAndRemove("qn_unitest_dml_0_100v0")

	sd2 := delegator.NewMockShardDelegator(suite.T())
	sd2.EXPECT().Collection().Return(100)
	sd2.EXPECT().GetTSafe().Return(200)
	sd2.EXPECT().GetDeleteBufferSize().Return(10, 1000)
	sd2.EXPECT().Close().Maybe()
	suite.node.delegators.Insert("qn_unitest_dml_1_100v1", sd2)
	defer suite.node.delegators.GetAndRemove("qn_unitest_dml_1_100v1")

	resp, err := suite.node.GetMetrics(ctx, req)
	err = merr.CheckRPCCall(resp, err)
	suite.NoError(err)

	info := &metricsinfo.QueryNodeInfos{}
	err = metricsinfo.UnmarshalComponentInfos(resp.GetResponse(), info)
	suite.NoError(err)

	entryNum, ok := info.QuotaMetrics.DeleteBufferInfo.CollectionDeleteBufferNum[100]
	suite.True(ok)
	suite.EqualValues(20, entryNum)
	memorySize, ok := info.QuotaMetrics.DeleteBufferInfo.CollectionDeleteBufferSize[100]
	suite.True(ok)
	suite.EqualValues(2000, memorySize)
}

func (suite *ServiceSuite) TestGetMetric_Failed() {
	ctx := context.Background()
	// invalid metric type
	metricReq := make(map[string]string)
	metricReq[metricsinfo.MetricTypeKey] = "invalidType"
	mReq, err := json.Marshal(metricReq)
	suite.NoError(err)

	req := &milvuspb.GetMetricsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		Request: string(mReq),
	}

	resp, err := suite.node.GetMetrics(ctx, req)
	suite.NoError(err)
	err = merr.Error(resp.GetStatus())
	suite.Contains(err.Error(), metricsinfo.MsgUnimplementedMetric)

	// metric parse failed
	req.Request = "---"
	resp, err = suite.node.GetMetrics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())

	// node unhealthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.GetMetrics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestGetDataDistribution_Normal() {
	ctx := context.Background()
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	req := &querypb.GetDataDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
	}

	resp, err := suite.node.GetDataDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestGetDataDistribution_Failed() {
	ctx := context.Background()
	req := &querypb.GetDataDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := suite.node.GetDataDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestSyncDistribution_Normal() {
	ctx := context.Background()
	// prepare
	// watch dmchannel and load some segments
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// data
	req := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		Channel:      suite.vchannel,
	}

	releaseAction := &querypb.SyncAction{
		Type:      querypb.SyncType_Remove,
		SegmentID: suite.validSegmentIDs[0],
	}

	setAction := &querypb.SyncAction{
		Type:        querypb.SyncType_Set,
		SegmentID:   suite.validSegmentIDs[0],
		NodeID:      0,
		PartitionID: suite.partitionIDs[0],
	}

	req.Actions = []*querypb.SyncAction{releaseAction, setAction}
	status, err := suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)

	// test sync targte version
	syncVersionAction := &querypb.SyncAction{
		Type:            querypb.SyncType_UpdateVersion,
		SealedInTarget:  []int64{3},
		GrowingInTarget: []int64{4},
		DroppedInTarget: []int64{1, 2},
		TargetVersion:   time.Now().UnixMilli(),
	}

	req.Actions = []*querypb.SyncAction{syncVersionAction}
	status, err = suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())

	// test sync segments
	segmentVersion := int64(111)
	syncSegmentVersion := &querypb.SyncAction{
		Type:        querypb.SyncType_Set,
		SegmentID:   suite.validSegmentIDs[0],
		NodeID:      0,
		PartitionID: suite.partitionIDs[0],
		Info:        &querypb.SegmentLoadInfo{},
		Version:     segmentVersion,
	}
	req.Actions = []*querypb.SyncAction{syncSegmentVersion}

	testChannel := "test_sync_segment"
	req.Channel = testChannel

	// expected call load segment with right segment version
	var versionMatch bool
	mockDelegator := delegator.NewMockShardDelegator(suite.T())
	mockDelegator.EXPECT().LoadSegments(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
			log.Info("version", zap.Int64("versionInload", req.GetVersion()))
			versionMatch = req.GetVersion() == segmentVersion
			return nil
		})
	suite.node.delegators.Insert(testChannel, mockDelegator)
	defer suite.node.delegators.GetAndRemove(testChannel)

	status, err = suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	suite.True(versionMatch)
}

func (suite *ServiceSuite) TestSyncDistribution_UpdatePartitionStats() {
	ctx := context.Background()
	// prepare
	// watch dmchannel and load some segments
	suite.TestWatchDmChannelsInt64()

	// write partitionStats file
	partitionID := suite.partitionIDs[0]
	newVersion := int64(100)
	idPath := metautil.JoinIDPath(suite.collectionID, partitionID)
	idPath = path.Join(idPath, suite.vchannel)
	statsFilePath := path.Join(suite.node.chunkManager.RootPath(), common.PartitionStatsPath, idPath, strconv.FormatInt(newVersion, 10))
	segStats := make(map[typeutil.UniqueID]storage.SegmentStats)
	partitionStats := &storage.PartitionStatsSnapshot{
		SegmentStats: segStats,
	}
	statsData, err := storage.SerializePartitionStatsSnapshot(partitionStats)
	suite.NoError(err)
	suite.node.chunkManager.Write(context.Background(), statsFilePath, statsData)
	defer suite.node.chunkManager.Remove(context.Background(), statsFilePath)

	// sync part stats
	req := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		Channel:      suite.vchannel,
	}

	partVersionsMap := make(map[int64]int64)
	partVersionsMap[partitionID] = newVersion
	updatePartStatsAction := &querypb.SyncAction{
		Type:                   querypb.SyncType_UpdatePartitionStats,
		PartitionStatsVersions: partVersionsMap,
	}
	req.Actions = []*querypb.SyncAction{updatePartStatsAction}
	status, err := suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)

	getReq := &querypb.GetDataDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID: rand.Int63(),
		},
	}
	distribution, err := suite.node.GetDataDistribution(ctx, getReq)
	suite.NoError(err)
	suite.Equal(1, len(distribution.LeaderViews))
	leaderView := distribution.LeaderViews[0]
	latestPartStats := leaderView.GetPartitionStatsVersions()
	suite.Equal(latestPartStats[partitionID], newVersion)
}

func (suite *ServiceSuite) TestSyncDistribution_ReleaseResultCheck() {
	ctx := context.Background()
	// prepare
	// watch dmchannel and load some segments
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	delegator, ok := suite.node.delegators.Get(suite.vchannel)
	suite.True(ok)
	sealedSegments, _ := delegator.GetSegmentInfo(false)
	// 1 level 0 + 3 sealed segments
	suite.Len(sealedSegments[0].Segments, 4)

	// data
	req := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		Channel:      suite.vchannel,
	}

	releaseAction := &querypb.SyncAction{
		Type:      querypb.SyncType_Remove,
		SegmentID: sealedSegments[0].Segments[0].SegmentID,
		NodeID:    100,
	}

	// expect one segments in distribution
	req.Actions = []*querypb.SyncAction{releaseAction}
	status, err := suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
	sealedSegments, _ = delegator.GetSegmentInfo(false)
	suite.Len(sealedSegments[0].Segments, 3)

	releaseAction = &querypb.SyncAction{
		Type:      querypb.SyncType_Remove,
		SegmentID: sealedSegments[0].Segments[0].SegmentID,
		NodeID:    sealedSegments[0].Segments[0].NodeID,
	}

	// expect one segments in distribution
	req.Actions = []*querypb.SyncAction{releaseAction}
	status, err = suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
	sealedSegments, _ = delegator.GetSegmentInfo(false)
	suite.Len(sealedSegments[0].Segments, 2)
}

func (suite *ServiceSuite) TestSyncDistribution_Failed() {
	ctx := context.Background()
	// prepare
	// watch dmchannel and load some segments
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// data
	req := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		Channel:      suite.vchannel,
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) TestDelete_Int64() {
	ctx := context.Background()
	// prepare
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	// data
	req := &querypb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionId: suite.collectionID,
		PartitionId:  suite.partitionIDs[0],
		SegmentId:    suite.validSegmentIDs[0],
		VchannelName: suite.vchannel,
		Timestamps:   []uint64{0},
		Scope:        querypb.DataScope_Historical,
	}

	// type int
	req.PrimaryKeys = &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{111},
			},
		},
	}
	status, err := suite.node.Delete(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func (suite *ServiceSuite) TestDelete_VarChar() {
	ctx := context.Background()
	// prepare
	suite.TestWatchDmChannelsVarchar()
	suite.TestLoadSegments_VarChar()
	// data
	req := &querypb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionId: suite.collectionID,
		PartitionId:  suite.partitionIDs[0],
		SegmentId:    suite.validSegmentIDs[0],
		VchannelName: suite.vchannel,
		Timestamps:   []uint64{2000},
	}

	// type int
	req.PrimaryKeys = &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: []string{"111"},
			},
		},
	}
	status, err := suite.node.Delete(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func (suite *ServiceSuite) TestDelete_Failed() {
	ctx := context.Background()
	// prepare
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()
	// data
	req := &querypb.DeleteRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionId: suite.collectionID,
		PartitionId:  suite.partitionIDs[0],
		SegmentId:    suite.validSegmentIDs[0],
		VchannelName: suite.vchannel,
		Timestamps:   []uint64{0},
	}

	// type int
	req.PrimaryKeys = &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: []int64{111},
			},
		},
	}

	// segment not found
	req.Scope = querypb.DataScope_Streaming
	status, err := suite.node.Delete(ctx, req)
	suite.NoError(err)
	suite.False(merr.Ok(status))

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.Delete(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) TestLoadPartition() {
	ctx := context.Background()
	req := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
	suite.node.UpdateStateCode(commonpb.StateCode_Healthy)

	// collection existed
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func TestQueryNodeService(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}
