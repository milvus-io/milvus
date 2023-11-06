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
	"encoding/json"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	validSegmentIDs   []int64
	flushedSegmentIDs []int64
	droppedSegmentIDs []int64
	// Test channel
	vchannel string
	pchannel string
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
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GCEnabled.Key, "false")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.CacheEnabled.Key, "false")

	suite.rootPath = suite.T().Name()
	suite.collectionID = 111
	suite.collectionName = "test-collection"
	suite.partitionIDs = []int64{222}
	suite.validSegmentIDs = []int64{1, 2, 3}
	suite.flushedSegmentIDs = []int64{4, 5, 6}
	suite.droppedSegmentIDs = []int64{7, 8, 9}

	// channel data
	suite.vchannel = "test-channel"
	suite.pchannel = funcutil.ToPhysicalChannel(suite.vchannel)
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
	// suite.chunkManagerFactory = storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus-test"))
	suite.chunkManagerFactory = segments.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
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
	suite.node.vectorStorage.RemoveWithPrefix(ctx, suite.rootPath)

	suite.node.Stop()
	suite.etcdClient.Close()
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

	// target not match
	req.Req.Base.TargetID = -1
	resp, err := suite.node.GetStatistics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, resp.Status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.GetStatistics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestWatchDmChannelsInt64() {
	ctx := context.Background()

	// data
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
		Schema: segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64),
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
			MetricType:   defaultMetricType,
		},
		IndexInfoList: []*indexpb.IndexInfo{
			{},
		},
	}

	// mocks
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer(mock.Anything, []string{suite.pchannel}, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Seek(mock.Anything, mock.Anything).Return(nil)
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
		Schema: segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_VarChar),
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: suite.partitionIDs,
			MetricType:   defaultMetricType,
		},
		IndexInfoList: []*indexpb.IndexInfo{
			{},
		},
	}

	// mocks
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer(mock.Anything, []string{suite.pchannel}, mock.Anything, mock.Anything).Return(nil)
	suite.msgStream.EXPECT().Seek(mock.Anything, mock.Anything).Return(nil)
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

func (suite *ServiceSuite) TestWatchDmChannels_Failed() {
	ctx := context.Background()

	// data
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
		Schema: segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64),
		LoadMeta: &querypb.LoadMetaInfo{
			MetricType: defaultMetricType,
		},
		IndexInfoList: []*indexpb.IndexInfo{
			{},
		},
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
	suite.msgStream.EXPECT().Seek(mock.Anything, mock.Anything).Return(errors.New("mock error"))

	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

	// empty index
	req.IndexInfoList = nil
	status, err = suite.node.WatchDmChannels(ctx, req)
	err = merr.CheckRPCCall(status, err)
	suite.ErrorIs(err, merr.ErrIndexNotFound)

	// target not match
	req.Base.TargetID = -1
	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())

	// empty metric type
	req.LoadMeta.MetricType = ""
	req.Base.TargetID = paramtable.GetNodeID()
	suite.node.UpdateStateCode(commonpb.StateCode_Healthy)
	status, err = suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func (suite *ServiceSuite) TestUnsubDmChannels_Normal() {
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

	status, err := suite.node.UnsubDmChannel(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
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

	// target not match
	req.Base.TargetID = -1
	status, err := suite.node.UnsubDmChannel(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.UnsubDmChannel(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())
}

func (suite *ServiceSuite) genSegmentLoadInfos(schema *schemapb.CollectionSchema) []*querypb.SegmentLoadInfo {
	ctx := context.Background()

	segNum := len(suite.validSegmentIDs)
	partNum := len(suite.partitionIDs)
	infos := make([]*querypb.SegmentLoadInfo, 0)
	for i := 0; i < segNum; i++ {
		binlogs, statslogs, err := segments.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionIDs[i%partNum],
			suite.validSegmentIDs[i],
			1000,
			schema,
			suite.node.vectorStorage,
		)
		suite.Require().NoError(err)

		vecFieldIDs := funcutil.GetVecFieldIDs(schema)
		indexes, err := segments.GenAndSaveIndex(
			suite.collectionID,
			suite.partitionIDs[i%partNum],
			suite.validSegmentIDs[i],
			vecFieldIDs[0],
			1000,
			segments.IndexFaissIVFFlat,
			metric.L2,
			suite.node.vectorStorage,
		)
		suite.Require().NoError(err)

		info := &querypb.SegmentLoadInfo{
			SegmentID:     suite.validSegmentIDs[i],
			PartitionID:   suite.partitionIDs[i%partNum],
			CollectionID:  suite.collectionID,
			InsertChannel: suite.vchannel,
			NumOfRows:     1000,
			BinlogPaths:   binlogs,
			Statslogs:     statslogs,
			IndexInfos:    []*querypb.FieldIndexInfo{indexes},
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	infos := suite.genSegmentLoadInfos(schema)
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_VarChar)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
	}
	suite.node.manager.Collection = segments.NewCollectionManager()
	suite.node.manager.Collection.PutOrRef(suite.collectionID, schema, nil, loadMeta)

	infos := suite.genSegmentLoadInfos(schema)
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
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	}
}

func (suite *ServiceSuite) TestLoadDeltaInt64() {
	ctx := context.Background()
	suite.TestLoadSegments_Int64()
	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		DstNodeID:    suite.node.session.ServerID,
		Infos:        suite.genSegmentLoadInfos(schema),
		Schema:       schema,
		NeedTransfer: true,
		LoadScope:    querypb.LoadScope_Delta,
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		DstNodeID:    suite.node.session.ServerID,
		Infos:        suite.genSegmentLoadInfos(schema),
		Schema:       schema,
		NeedTransfer: true,
		LoadScope:    querypb.LoadScope_Delta,
	}

	// LoadSegment
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
}

func (suite *ServiceSuite) TestLoadIndex_Success() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)

	infos := suite.genSegmentLoadInfos(schema)
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
		CollectionID: suite.collectionID,
		DstNodeID:    suite.node.session.ServerID,
		Infos:        rawInfo,
		Schema:       schema,
		NeedTransfer: false,
		LoadScope:    querypb.LoadScope_Full,
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
		CollectionID: suite.collectionID,
		DstNodeID:    suite.node.session.ServerID,
		Infos:        infos,
		Schema:       schema,
		NeedTransfer: false,
		LoadScope:    querypb.LoadScope_Index,
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

	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)

	suite.Run("load_non_exist_segment", func() {
		infos := suite.genSegmentLoadInfos(schema)
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
			CollectionID: suite.collectionID,
			DstNodeID:    suite.node.session.ServerID,
			Infos:        rawInfo,
			Schema:       schema,
			NeedTransfer: false,
			LoadScope:    querypb.LoadScope_Index,
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

		infos := suite.genSegmentLoadInfos(schema)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID: suite.collectionID,
			DstNodeID:    suite.node.session.ServerID,
			Infos:        infos,
			Schema:       schema,
			NeedTransfer: false,
			LoadScope:    querypb.LoadScope_Index,
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID: suite.collectionID,
		DstNodeID:    suite.node.session.ServerID,
		Infos:        suite.genSegmentLoadInfos(schema),
		Schema:       schema,
		NeedTransfer: true,
	}

	// Delegator not found
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrChannelNotFound)

	// target not match
	req.Base.TargetID = -1
	status, err = suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(status), merr.ErrNodeNotMatch)

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

		delegator.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(nil)
		// data
		schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID: suite.collectionID,
			DstNodeID:    suite.node.session.ServerID,
			Infos:        suite.genSegmentLoadInfos(schema),
			Schema:       schema,
			NeedTransfer: true,
		}

		// LoadSegment
		status, err := suite.node.LoadSegments(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	})

	suite.Run("delegator_not_found", func() {
		// data
		schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID: suite.collectionID,
			DstNodeID:    suite.node.session.ServerID,
			Infos:        suite.genSegmentLoadInfos(schema),
			Schema:       schema,
			NeedTransfer: true,
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
		delegator.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).
			Return(errors.New("mocked error"))
		// data
		schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgID:    rand.Int63(),
				TargetID: suite.node.session.ServerID,
			},
			CollectionID: suite.collectionID,
			DstNodeID:    suite.node.session.ServerID,
			Infos:        suite.genSegmentLoadInfos(schema),
			Schema:       schema,
			NeedTransfer: true,
		}

		// LoadSegment
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

	// target not match
	req.Base.TargetID = -1
	status, err := suite.node.ReleaseSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.ReleaseSegments(ctx, req)
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

// Test Search
func (suite *ServiceSuite) genCSearchRequest(nq int64, indexType string, schema *schemapb.CollectionSchema) (*internalpb.SearchRequest, error) {
	placeHolder, err := genPlaceHolderGroup(nq)
	if err != nil {
		return nil, err
	}
	planStr, err := genDSLByIndexType(schema, indexType)
	if err != nil {
		return nil, err
	}
	var planpb planpb.PlanNode
	proto.UnmarshalText(planStr, &planpb)
	serializedPlan, err2 := proto.Marshal(&planpb)
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
	}, nil
}

func (suite *ServiceSuite) TestSearch_Normal() {
	ctx := context.Background()
	// pre
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCSearchRequest(10, IndexFaissIDMap, schema)
	req := &querypb.SearchRequest{
		Req:             creq,
		FromShardLeader: false,
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

	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)

	concurrency := 16
	futures := make([]*conc.Future[*internalpb.SearchResults], 0, concurrency)
	for i := 0; i < concurrency; i++ {
		future := conc.Go(func() (*internalpb.SearchResults, error) {
			creq, err := suite.genCSearchRequest(30, IndexFaissIDMap, schema)
			req := &querypb.SearchRequest{
				Req:             creq,
				FromShardLeader: false,
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCSearchRequest(10, IndexFaissIDMap, schema)
	req := &querypb.SearchRequest{
		Req:             creq,
		FromShardLeader: false,
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}
	suite.NoError(err)

	// collection not exist
	resp, err := suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_CollectionNotExists, resp.GetStatus().GetErrorCode())
	suite.Contains(resp.GetStatus().GetReason(), merr.ErrCollectionNotFound.Error())

	// metric type mismatch
	LoadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: suite.partitionIDs,
		MetricType:   "L2",
	}
	suite.node.manager.Collection.PutOrRef(suite.collectionID, schema, nil, LoadMeta)
	req.GetReq().MetricType = "IP"
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrParameterInvalid)
	suite.Contains(resp.GetStatus().GetReason(), merr.ErrParameterInvalid.Error())
	req.GetReq().MetricType = "L2"

	// Delegator not found
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrChannelNotFound)

	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// target not match
	req.Req.Base.TargetID = -1
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, resp.Status.GetErrorCode())

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
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
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
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
	}

	rsp, err := suite.node.SearchSegments(ctx, req)
	suite.NoError(err)
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

	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCSearchRequest(10, IndexFaissIDMap, schema)
	req := &querypb.SearchRequest{
		Req:             creq,
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
		TotalChannelNum: 2,
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

	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req:             creq,
		FromShardLeader: false,
		DmlChannels:     []string{suite.vchannel},
	}

	rsp, err := suite.node.Query(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
}

func (suite *ServiceSuite) TestQuery_Failed() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req:             creq,
		FromShardLeader: false,
		DmlChannels:     []string{suite.vchannel},
	}

	// Delegator not found
	resp, err := suite.node.Query(ctx, req)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrChannelNotFound)

	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	// target not match
	req.Req.Base.TargetID = -1
	resp, err = suite.node.Query(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, resp.Status.GetErrorCode())

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
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
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

	// data
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req:             creq,
		FromShardLeader: false,
		DmlChannels:     []string{suite.vchannel},
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req:             creq,
		FromShardLeader: false,
		DmlChannels:     []string{suite.vchannel},
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

	// target not match
	suite.Run("target not match", func() {
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
				suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, result.GetStatus().GetErrorCode())
			}
		}
		wg.Wait()
	})

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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req:             creq,
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
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
	schema := segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	creq, err := suite.genCQueryRequest(10, IndexFaissIDMap, schema)
	suite.NoError(err)
	req := &querypb.QueryRequest{
		Req:             creq,
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
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
		Pattern: "Cache.enabled",
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
		Pattern: "Cache.enabled",
	}

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := suite.node.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.GetErrorCode())
}

func (suite *ServiceSuite) TestGetMetric_Normal() {
	ctx := context.Background()
	metricReq := make(map[string]string)
	metricReq[metricsinfo.MetricTypeKey] = metricsinfo.SystemInfoMetrics
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
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
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
	suite.ErrorIs(err, merr.ErrMetricNotFound)

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

	// target not match
	req.Base.TargetID = -1
	resp, err := suite.node.GetDataDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, resp.Status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.GetDataDistribution(ctx, req)
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

	status, err = suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
	suite.True(versionMatch)
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
	suite.Len(sealedSegments[0].Segments, 3)

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

	// target not match
	req.Base.TargetID = -1
	status, err := suite.node.SyncDistribution(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.SyncDistribution(ctx, req)
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

	// target not match
	req.Base.TargetID = -1
	status, err := suite.node.Delete(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, status.GetErrorCode())

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
