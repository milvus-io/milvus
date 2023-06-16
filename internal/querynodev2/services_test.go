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
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	suite.chunkManagerFactory = storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus-test"))
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
	suite.node.Stop()
	suite.etcdClient.Close()
	os.RemoveAll("/tmp/milvus-test")
}

func (suite *ServiceSuite) TestGetComponentStatesNormal() {
	ctx := context.Background()
	suite.node.session.UpdateRegistered(true)
	rsp, err := suite.node.GetComponentStates(ctx)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	suite.Equal(commonpb.StateCode_Healthy, rsp.State.StateCode)

	// after update
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	rsp, err = suite.node.GetComponentStates(ctx)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
	suite.Equal(commonpb.StateCode_Abnormal, rsp.State.StateCode)
}

func (suite *ServiceSuite) TestGetTimeTiclChannel_Normal() {
	ctx := context.Background()
	rsp, err := suite.node.GetTimeTickChannel(ctx)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
}

func (suite *ServiceSuite) TestGetStatisChannel_Normal() {
	ctx := context.Background()
	rsp, err := suite.node.GetStatisticsChannel(ctx)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
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
	suite.Equal(commonpb.ErrorCode_Success, rsp.Status.ErrorCode)
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
	}

	// mocks
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer([]string{suite.pchannel}, mock.Anything, mock.Anything).Return()
	suite.msgStream.EXPECT().Seek(mock.Anything).Return(nil)
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
	}

	// mocks
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer([]string{suite.pchannel}, mock.Anything, mock.Anything).Return()
	suite.msgStream.EXPECT().Seek(mock.Anything).Return(nil)
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
	}

	// init msgstream failed
	suite.factory.EXPECT().NewTtMsgStream(mock.Anything).Return(suite.msgStream, nil)
	suite.msgStream.EXPECT().AsConsumer([]string{suite.pchannel}, mock.Anything, mock.Anything).Return()
	suite.msgStream.EXPECT().Seek(mock.Anything).Return(errors.New("mock error"))

	status, err := suite.node.WatchDmChannels(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

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
			segments.L2,
			suite.node.vectorStorage,
		)
		suite.Require().NoError(err)

		info := &querypb.SegmentLoadInfo{
			SegmentID:     suite.validSegmentIDs[i],
			PartitionID:   suite.partitionIDs[i%partNum],
			CollectionID:  suite.collectionID,
			InsertChannel: suite.vchannel,
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
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:   suite.collectionID,
		DstNodeID:      suite.node.session.ServerID,
		Infos:          suite.genSegmentLoadInfos(schema),
		Schema:         schema,
		DeltaPositions: []*msgpb.MsgPosition{{Timestamp: 20000}},
		NeedTransfer:   true,
		LoadMeta: &querypb.LoadMetaInfo{
			MetricType: defaultMetricType,
		},
	}

	// LoadSegment
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
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
		MetricType:   defaultMetricType,
	}
	suite.node.manager.Collection = segments.NewCollectionManager()
	suite.node.manager.Collection.Put(suite.collectionID, schema, nil, loadMeta)
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:   suite.collectionID,
		DstNodeID:      suite.node.session.ServerID,
		Infos:          suite.genSegmentLoadInfos(schema),
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
		LoadMeta: &querypb.LoadMetaInfo{
			MetricType: defaultMetricType,
		},
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
		LoadMeta: &querypb.LoadMetaInfo{
			MetricType: defaultMetricType,
		},
	}

	// LoadSegment
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.GetErrorCode())
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
		LoadMeta: &querypb.LoadMetaInfo{
			MetricType: defaultMetricType,
		},
	}

	// Delegator not found
	status, err := suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Contains(status.GetReason(), "failed to load segments, delegator not found")

	// target not match
	req.Base.TargetID = -1
	status, err = suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NodeIDNotMatch, status.GetErrorCode())

	// node not healthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, status.GetErrorCode())

	// no metric type
	req.LoadMeta = nil
	req.Base.TargetID = paramtable.GetNodeID()
	suite.node.UpdateStateCode(commonpb.StateCode_Healthy)
	status, err = suite.node.LoadSegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
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
			LoadMeta: &querypb.LoadMetaInfo{
				MetricType: defaultMetricType,
			},
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
			LoadMeta: &querypb.LoadMetaInfo{
				MetricType: defaultMetricType,
			},
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
			LoadMeta: &querypb.LoadMetaInfo{
				MetricType: defaultMetricType,
			},
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
	suite.TestLoadPartition()
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
	simpleDSL, err2 := genDSLByIndexType(schema, indexType)
	if err2 != nil {
		return nil, err2
	}
	return &internalpb.SearchRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_Search,
			MsgID:    rand.Int63(),
			TargetID: suite.node.session.ServerID,
		},
		CollectionID:     suite.collectionID,
		PartitionIDs:     suite.partitionIDs,
		Dsl:              simpleDSL,
		PlaceholderGroup: placeHolder,
		DslType:          commonpb.DslType_Dsl,
		Nq:               nq,
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
	suite.node.manager.Collection.Put(suite.collectionID, schema, nil, LoadMeta)
	req.GetReq().MetricType = "IP"
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	suite.Contains(resp.GetStatus().GetReason(), merr.ErrParameterInvalid.Error())
	req.GetReq().MetricType = "L2"

	// Delegator not found
	resp, err = suite.node.Search(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	suite.Contains(resp.GetStatus().GetReason(), "failed to get query shard delegator")

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
		TravelTimestamp:    uint64(1000),
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
	ctx := context.Background()

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
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	suite.Contains(resp.GetStatus().GetReason(), "failed to get query shard delegator")

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

func (suite *ServiceSuite) TestQuerySegments_Normal() {
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
		FromShardLeader: true,
		DmlChannels:     []string{suite.vchannel},
	}

	rsp, err := suite.node.QuerySegments(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, rsp.GetStatus().GetErrorCode())
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
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
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
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
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
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)
	suite.Equal(metricsinfo.MsgUnimplementedMetric, resp.Status.Reason)

	// metric parse failed
	req.Request = "---"
	resp, err = suite.node.GetMetrics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)

	// node unhealthy
	suite.node.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.node.GetMetrics(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_NotReadyServe, resp.Status.ErrorCode)
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
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
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
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.ErrorCode)
}

func (suite *ServiceSuite) TestSyncDistribution_ReleaseResultCheck() {
	ctx := context.Background()
	// prepare
	// watch dmchannel and load some segments
	suite.TestWatchDmChannelsInt64()
	suite.TestLoadSegments_Int64()

	delegator, ok := suite.node.delegators.Get(suite.vchannel)
	suite.True(ok)
	sealedSegments, _ := delegator.GetSegmentInfo()
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
	sealedSegments, _ = delegator.GetSegmentInfo()
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
	sealedSegments, _ = delegator.GetSegmentInfo()
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

	// collection not exist and schema is nil
	suite.node.manager.Collection = segments.NewCollectionManager()
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

	// no vec field in schema
	req.Schema = &schemapb.CollectionSchema{}
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

	// no indexInfo
	req.Schema = segments.GenTestCollectionSchema(suite.collectionName, schemapb.DataType_Int64)
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

	// no metric type
	vecField, err := typeutil.GetVectorFieldSchema(req.GetSchema())
	suite.NoError(err)
	req.IndexInfoList = []*indexpb.IndexInfo{
		{
			CollectionID: suite.collectionID,
			FieldID:      vecField.GetFieldID(),
			IndexParams:  []*commonpb.KeyValuePair{},
		},
	}
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())

	// empty metric type
	req.IndexInfoList = []*indexpb.IndexInfo{
		{
			CollectionID: suite.collectionID,
			FieldID:      vecField.GetFieldID(),
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MetricTypeKey,
					Value: "",
				},
			},
		},
	}
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.ErrorCode)

	// collection not exist and schema is not nil
	req.IndexInfoList = []*indexpb.IndexInfo{
		{
			CollectionID: suite.collectionID,
			FieldID:      vecField.GetFieldID(),
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MetricTypeKey,
					Value: "L2",
				},
			},
		},
	}
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)

	// collection existed
	status, err = suite.node.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
}

func TestQueryNodeService(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}
