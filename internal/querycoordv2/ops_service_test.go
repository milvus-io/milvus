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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	walbalancer "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	walbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type OpsServiceSuite struct {
	suite.Suite

	// Dependencies
	kv             kv.MetaKv
	store          metastore.QueryCoordCatalog
	dist           *meta.DistributionManager
	meta           *meta.Meta
	targetMgr      *meta.TargetManager
	broker         *meta.MockBroker
	targetObserver *observers.TargetObserver
	cluster        *session.MockCluster
	nodeMgr        *session.NodeManager
	jobScheduler   *job.Scheduler
	taskScheduler  *task.MockScheduler
	balancer       balance.Balance
	proxyManager   *proxyutil.MockProxyClientManager

	distMgr           *meta.DistributionManager
	distController    *dist.MockController
	checkerController *checkers.CheckerController

	// Test object
	server *Server
}

func (suite *OpsServiceSuite) SetupSuite() {
	paramtable.Init()
	suite.proxyManager = proxyutil.NewMockProxyClientManager(suite.T())
	suite.proxyManager.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
}

func (suite *OpsServiceSuite) SetupTest() {
	config := params.GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	suite.store = querycoord.NewCatalog(suite.kv)
	suite.dist = meta.NewDistributionManager(suite.nodeMgr)
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(params.RandomIncrementIDAllocator(), suite.store, suite.nodeMgr, metacache.NewMetaStore(nil))
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta, metacache.NewMetaStore(nil))
	suite.targetObserver = observers.NewTargetObserver(
		suite.meta,
		suite.targetMgr,
		suite.dist,
		suite.broker,
		suite.cluster,
		suite.nodeMgr,
	)
	suite.cluster = session.NewMockCluster(suite.T())
	suite.jobScheduler = job.NewScheduler()
	suite.taskScheduler = task.NewMockScheduler(suite.T())
	suite.taskScheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	suite.jobScheduler.Start()
	assign.InitGlobalAssignPolicyFactory(suite.taskScheduler, suite.nodeMgr, suite.dist, suite.meta, suite.targetMgr)
	suite.balancer = balance.NewScoreBasedBalancer(
		suite.taskScheduler,
		suite.nodeMgr,
		suite.dist,
		suite.targetMgr,
	)
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	suite.distMgr = meta.NewDistributionManager(suite.nodeMgr)
	suite.distController = dist.NewMockController(suite.T())

	suite.checkerController = checkers.NewCheckerController(suite.meta, suite.distMgr,
		suite.targetMgr, suite.nodeMgr, suite.taskScheduler, suite.broker)

	suite.server = &Server{
		kv:                  suite.kv,
		store:               suite.store,
		session:             sessionutil.NewSessionWithEtcd(context.Background(), Params.EtcdCfg.MetaRootPath.GetValue(), cli),
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		dist:                suite.dist,
		meta:                suite.meta,
		targetMgr:           suite.targetMgr,
		broker:              suite.broker,
		targetObserver:      suite.targetObserver,
		nodeMgr:             suite.nodeMgr,
		cluster:             suite.cluster,
		jobScheduler:        suite.jobScheduler,
		taskScheduler:       suite.taskScheduler,
		distController:      suite.distController,
		ctx:                 context.Background(),
		checkerController:   suite.checkerController,
	}
	suite.server.collectionObserver = observers.NewCollectionObserver(
		suite.server.dist,
		suite.server.meta,
		suite.server.targetMgr,
		suite.targetObserver,
		&checkers.CheckerController{},
		suite.proxyManager,
	)

	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
}

func (suite *OpsServiceSuite) TestDeprecatedCheckers() {
	// These compatibility endpoints must not touch the retired controller.
	suite.server.checkerController = nil
	ctx := suite.T().Context()
	for _, state := range []commonpb.StateCode{commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal} {
		suite.server.UpdateStateCode(state)
		resp, err := suite.server.ListCheckers(ctx, &querypb.ListCheckersRequest{})
		suite.NoError(err)
		suite.ErrorIs(merr.Error(resp.GetStatus()), merr.ErrServiceUnimplemented)
		suite.Contains(resp.GetStatus().GetReason(), "deprecated")
		suite.Empty(resp.GetCheckerInfos())
		status, err := suite.server.ActivateChecker(ctx, &querypb.ActivateCheckerRequest{CheckerID: int32(utils.BalanceChecker)})
		suite.NoError(err)
		suite.ErrorIs(merr.Error(status), merr.ErrServiceUnimplemented)
		suite.Contains(status.GetReason(), "ResumeBalance")
		status, err = suite.server.DeactivateChecker(ctx, &querypb.DeactivateCheckerRequest{CheckerID: int32(utils.BalanceChecker)})
		suite.NoError(err)
		suite.ErrorIs(merr.Error(status), merr.ErrServiceUnimplemented)
		suite.Contains(status.GetReason(), "SuspendBalance")
	}
}

func (suite *OpsServiceSuite) TestListQueryNode() {
	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()
	resp, err := suite.server.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	suite.NoError(err)
	suite.Equal(0, len(resp.GetNodeInfos()))
	suite.False(merr.Ok(resp.Status))
	// test server healthy
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   111,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	resp, err = suite.server.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	suite.NoError(err)
	suite.Equal(1, len(resp.GetNodeInfos()))
}

func (suite *OpsServiceSuite) TestGetQueryNodeDistribution() {
	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()
	resp, err := suite.server.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp.Status))

	// test node not found
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp.Status))

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	registry := &coordview.ShardViewRegistry{}
	suite.server.qviewsRuntime = &qviewsRuntime{shardViewRegistry: registry}
	defer func() { suite.server.qviewsRuntime = nil }()
	patch := mockey.Mock((*coordview.ShardViewRegistry).LoadedSegmentsOnNode).To(func(r *coordview.ShardViewRegistry, nodeID int64) []int64 {
		suite.Same(registry, r)
		suite.Equal(int64(1), nodeID)
		return []int64{1, 2}
	}).Build()
	defer patch.UnPatch()

	resp, err = suite.server.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
		NodeID: 1,
	})

	suite.NoError(err)
	suite.True(merr.Ok(resp.Status))
	suite.Empty(resp.GetChannelNames())
	suite.Equal(2, len(resp.GetSealedSegmentIDs()))
}

func (suite *OpsServiceSuite) TestCheckQueryNodeDistributionDeprecated() {
	// A deprecated endpoint must not read the retired distribution or Target,
	// nor report success merely because the old resource sets are empty.
	suite.server.nodeMgr = nil
	suite.server.dist = nil
	suite.server.targetMgr = nil
	for _, state := range []commonpb.StateCode{commonpb.StateCode_Healthy, commonpb.StateCode_Abnormal} {
		suite.server.UpdateStateCode(state)
		for _, req := range []*querypb.CheckQueryNodeDistributionRequest{
			{},
			{SourceNodeID: 1, TargetNodeID: 2},
			{SourceNodeID: 1, TargetNodeID: 1},
		} {
			resp, err := suite.server.CheckQueryNodeDistribution(suite.T().Context(), req)
			suite.NoError(err)
			suite.ErrorIs(merr.Error(resp), merr.ErrServiceUnimplemented)
			suite.Contains(resp.GetReason(), "deprecated")
			suite.Contains(resp.GetReason(), "LoadBalance and TransferSegment")
		}
	}
}

func (suite *OpsServiceSuite) TestSuspendAndResumeNode() {
	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()
	resp, err := suite.server.SuspendNode(ctx, &querypb.SuspendNodeRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.server.ResumeNode(ctx, &querypb.ResumeNodeRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	// test node not found
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.SuspendNode(ctx, &querypb.SuspendNodeRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	resp, err = suite.server.ResumeNode(ctx, &querypb.ResumeNodeRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.HandleNodeUp(ctx, 1)
	nodes, err := suite.meta.GetNodes(ctx, meta.DefaultResourceGroupName)
	suite.NoError(err)
	suite.Contains(nodes, int64(1))
	// test success
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.SuspendNode(ctx, &querypb.SuspendNodeRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	nodes, err = suite.meta.GetNodes(ctx, meta.DefaultResourceGroupName)
	suite.NoError(err)
	suite.NotContains(nodes, int64(1))

	resp, err = suite.server.ResumeNode(ctx, &querypb.ResumeNodeRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	nodes, err = suite.meta.GetNodes(ctx, meta.DefaultResourceGroupName)
	suite.NoError(err)
	suite.Contains(nodes, int64(1))
}

func (suite *OpsServiceSuite) TestTransferSegment() {
	calls := 0
	var failure error
	patch := mockey.Mock((*Server).manualMoveSegments).To(func(_ *Server, _ context.Context, collection, source int64, destinations, segments []int64) error {
		calls++
		suite.Zero(collection)
		suite.Equal(int64(1), source)
		if calls == 1 {
			suite.Equal([]int64{2}, destinations)
			suite.Equal([]int64{100}, segments)
		} else {
			suite.Empty(destinations)
			suite.Empty(segments)
		}
		return failure
	}).Build()
	defer patch.UnPatch()
	req := &querypb.TransferSegmentRequest{SourceNodeID: 1, TargetNodeID: 2, SegmentID: 100}
	resp, err := suite.server.TransferSegment(suite.T().Context(), req)
	suite.NoError(merr.CheckRPCCall(resp, err))
	req.ToAllNodes, req.TransferAll = true, true
	failure = merr.ErrServiceUnavailable
	resp, err = suite.server.TransferSegment(suite.T().Context(), req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), failure)
	req.CopyMode = true
	resp, err = suite.server.TransferSegment(suite.T().Context(), req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), failure)
	suite.Equal(3, calls)
	suite.server.UpdateStateCode(commonpb.StateCode_Initializing)
	resp, err = suite.server.TransferSegment(suite.T().Context(), req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrServiceNotReady)
	suite.Equal(3, calls)
}

func (suite *OpsServiceSuite) TestTransferChannel() {
	ctx := suite.T().Context()
	b := &mock_balancer.MockBalancer{}
	get := mockey.Mock(walbalance.GetWithContext).Return(b, nil).Build()
	defer get.UnPatch()
	var received walbalancer.TransferChannelsRequest
	calls := 0
	transfer := mockey.Mock((*mock_balancer.MockBalancer).TransferChannels).To(func(_ *mock_balancer.MockBalancer, _ context.Context, req walbalancer.TransferChannelsRequest) error {
		received = req
		calls++
		return nil
	}).Build()
	defer transfer.UnPatch()

	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	req := &querypb.TransferChannelRequest{SourceNodeID: 11, TargetNodeID: 12, ChannelName: "p1_100v0"}
	resp, err := suite.server.TransferChannel(ctx, req)
	suite.NoError(merr.CheckRPCCall(resp, err))
	suite.Equal(walbalancer.TransferChannelsRequest{SourceNodeID: 11, TargetNodeID: 12, ChannelName: "p1_100v0"}, received)

	req.TransferAll, req.ToAllNodes = true, true
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.NoError(merr.CheckRPCCall(resp, err))
	suite.Equal(walbalancer.TransferChannelsRequest{SourceNodeID: 11}, received)
	suite.Equal(2, calls)

	req.CopyMode = true
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrServiceUnimplemented)
	req.CopyMode = false
	req.SourceNodeID = 0
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	req.SourceNodeID = 11
	req.ToAllNodes = false
	req.TargetNodeID = 11
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	req.TargetNodeID = 12
	req.TransferAll = false
	req.ChannelName = ""
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	suite.Equal(2, calls)

	req.TransferAll = true
	transfer.UnPatch()
	transfer = mockey.Mock((*mock_balancer.MockBalancer).TransferChannels).Return(merr.ErrServiceUnavailable).Build()
	defer transfer.UnPatch()
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrServiceUnavailable)

	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.server.TransferChannel(ctx, req)
	suite.ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrServiceNotReady)
}

func TestOpsService(t *testing.T) {
	suite.Run(t, new(OpsServiceSuite))
}
