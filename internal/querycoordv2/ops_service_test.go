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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
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
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	suite.dist = meta.NewDistributionManager()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(params.RandomIncrementIDAllocator(), suite.store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
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
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	suite.jobScheduler.Start()
	suite.balancer = balance.NewScoreBasedBalancer(
		suite.taskScheduler,
		suite.nodeMgr,
		suite.dist,
		suite.meta,
		suite.targetMgr,
	)
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	suite.distMgr = meta.NewDistributionManager()
	suite.distController = dist.NewMockController(suite.T())

	suite.checkerController = checkers.NewCheckerController(suite.meta, suite.distMgr,
		suite.targetMgr, suite.nodeMgr, suite.taskScheduler, suite.broker, func() balance.Balance { return suite.balancer })

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
		getBalancerFunc:     func() balance.Balance { return suite.balancer },
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

func (suite *OpsServiceSuite) TestActiveCheckers() {
	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()
	resp, err := suite.server.ListCheckers(ctx, &querypb.ListCheckersRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp.Status))

	resp1, err := suite.server.DeactivateChecker(ctx, &querypb.DeactivateCheckerRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp1))

	resp2, err := suite.server.ActivateChecker(ctx, &querypb.ActivateCheckerRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp2))

	// test active success
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.ListCheckers(ctx, &querypb.ListCheckersRequest{})
	suite.NoError(err)
	suite.True(merr.Ok(resp.Status))
	suite.Len(resp.GetCheckerInfos(), 5)

	resp4, err := suite.server.DeactivateChecker(ctx, &querypb.DeactivateCheckerRequest{
		CheckerID: int32(utils.ChannelChecker),
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp4))
	suite.False(suite.checkerController.IsActive(utils.ChannelChecker))

	resp5, err := suite.server.ActivateChecker(ctx, &querypb.ActivateCheckerRequest{
		CheckerID: int32(utils.ChannelChecker),
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp5))
	suite.True(suite.checkerController.IsActive(utils.ChannelChecker))
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
	// test success
	channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 1,
				ChannelName:  "channel1",
			},
			Node: 1,
		},
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 1,
				ChannelName:  "channel2",
			},
			Node: 1,
		},
	}

	segments := []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: "channel1",
			},
			Node: 1,
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: "channel2",
			},
			Node: 1,
		},
	}
	suite.dist.ChannelDistManager.Update(1, channels...)
	suite.dist.SegmentDistManager.Update(1, segments...)

	resp, err = suite.server.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
		NodeID: 1,
	})

	suite.NoError(err)
	suite.True(merr.Ok(resp.Status))
	suite.Equal(2, len(resp.GetChannelNames()))
	suite.Equal(2, len(resp.GetSealedSegmentIDs()))
}

func (suite *OpsServiceSuite) TestCheckQueryNodeDistribution() {
	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()
	resp, err := suite.server.CheckQueryNodeDistribution(ctx, &querypb.CheckQueryNodeDistributionRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	// test node not found
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.CheckQueryNodeDistribution(ctx, &querypb.CheckQueryNodeDistributionRequest{
		TargetNodeID: 2,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	resp, err = suite.server.CheckQueryNodeDistribution(ctx, &querypb.CheckQueryNodeDistributionRequest{
		SourceNodeID: 1,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	// test success
	channels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 1,
				ChannelName:  "channel1",
			},
			Node: 1,
		},
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: 1,
				ChannelName:  "channel2",
			},
			Node: 1,
		},
	}

	segments := []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: "channel1",
			},
			Node: 1,
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: "channel2",
			},
			Node: 1,
		},
	}
	suite.dist.ChannelDistManager.Update(1, channels...)
	suite.dist.SegmentDistManager.Update(1, segments...)

	resp, err = suite.server.CheckQueryNodeDistribution(ctx, &querypb.CheckQueryNodeDistributionRequest{
		SourceNodeID: 1,
		TargetNodeID: 2,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.dist.ChannelDistManager.Update(2, channels...)
	suite.dist.SegmentDistManager.Update(2, segments...)
	resp, err = suite.server.CheckQueryNodeDistribution(ctx, &querypb.CheckQueryNodeDistributionRequest{
		SourceNodeID: 1,
		TargetNodeID: 1,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
}

func (suite *OpsServiceSuite) TestSuspendAndResumeBalance() {
	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	ctx := context.Background()
	resp, err := suite.server.SuspendBalance(ctx, &querypb.SuspendBalanceRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	resp, err = suite.server.ResumeBalance(ctx, &querypb.ResumeBalanceRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	// test suspend success
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.SuspendBalance(ctx, &querypb.SuspendBalanceRequest{})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	suite.False(suite.checkerController.IsActive(utils.BalanceChecker))

	resp, err = suite.server.ResumeBalance(ctx, &querypb.ResumeBalanceRequest{})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	suite.True(suite.checkerController.IsActive(utils.BalanceChecker))
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
	suite.meta.ResourceManager.HandleNodeUp(1)
	nodes, err := suite.meta.ResourceManager.GetNodes(meta.DefaultResourceGroupName)
	suite.NoError(err)
	suite.Contains(nodes, int64(1))
	// test success
	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = suite.server.SuspendNode(ctx, &querypb.SuspendNodeRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	nodes, err = suite.meta.ResourceManager.GetNodes(meta.DefaultResourceGroupName)
	suite.NoError(err)
	suite.NotContains(nodes, int64(1))

	resp, err = suite.server.ResumeNode(ctx, &querypb.ResumeNodeRequest{
		NodeID: 1,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	nodes, err = suite.meta.ResourceManager.GetNodes(meta.DefaultResourceGroupName)
	suite.NoError(err)
	suite.Contains(nodes, int64(1))
}

func (suite *OpsServiceSuite) TestTransferSegment() {
	ctx := context.Background()

	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	// test source node not healthy
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: 1,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	collectionID := int64(1)
	partitionID := int64(1)
	replicaID := int64(1)
	nodes := []int64{1, 2, 3, 4}
	replica := utils.CreateTestReplica(replicaID, collectionID, nodes)
	suite.meta.ReplicaManager.Put(replica)
	collection := utils.CreateTestCollection(collectionID, 1)
	partition := utils.CreateTestPartition(partitionID, collectionID)
	suite.meta.PutCollection(collection, partition)
	segmentIDs := []int64{1, 2, 3, 4}
	channelNames := []string{"channel-1", "channel-2", "channel-3", "channel-4"}

	// test target node not healthy
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	// test segment not exist in node
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		SegmentID:    segmentIDs[0],
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	segments := []*datapb.SegmentInfo{
		{
			ID:            segmentIDs[0],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[0],
			NumOfRows:     1,
		},
		{
			ID:            segmentIDs[1],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[1],
			NumOfRows:     1,
		},
		{
			ID:            segmentIDs[2],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[2],
			NumOfRows:     1,
		},
		{
			ID:            segmentIDs[3],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[3],
			NumOfRows:     1,
		},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[0],
		},
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[1],
		},
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[2],
		},
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[3],
		},
	}
	segmentInfos := lo.Map(segments, func(segment *datapb.SegmentInfo, _ int) *meta.Segment {
		return &meta.Segment{
			SegmentInfo: segment,
			Node:        nodes[0],
		}
	})
	chanenlInfos := lo.Map(channels, func(channel *datapb.VchannelInfo, _ int) *meta.DmChannel {
		return &meta.DmChannel{
			VchannelInfo: channel,
			Node:         nodes[0],
		}
	})
	suite.dist.SegmentDistManager.Update(1, segmentInfos[0])

	// test segment not exist in current target, expect no task assign and success
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		SegmentID:    segmentIDs[0],
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(channels, segments, nil)
	suite.targetMgr.UpdateCollectionNextTarget(1)
	suite.targetMgr.UpdateCollectionCurrentTarget(1)
	suite.dist.SegmentDistManager.Update(1, segmentInfos...)
	suite.dist.ChannelDistManager.Update(1, chanenlInfos...)

	for _, node := range nodes {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.meta.ResourceManager.HandleNodeUp(node)
	}

	// test transfer segment success, expect generate 1 balance segment task
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 2)
		suite.Equal(actions[0].Node(), int64(2))
		return nil
	})
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		SegmentID:    segmentIDs[0],
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))

	// test copy mode, expect generate 1 load segment task
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 1)
		suite.Equal(actions[0].Node(), int64(2))
		return nil
	})
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		SegmentID:    segmentIDs[0],
		CopyMode:     true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))

	// test transfer all segments, expect generate 4 load segment task
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	counter := atomic.NewInt64(0)
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 2)
		suite.Equal(actions[0].Node(), int64(2))
		counter.Inc()
		return nil
	})
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		TransferAll:  true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	suite.Equal(counter.Load(), int64(4))

	// test transfer all segment to all nodes, expect generate 4 load segment task
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	counter = atomic.NewInt64(0)
	nodeSet := typeutil.NewUniqueSet()
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 2)
		nodeSet.Insert(actions[0].Node())
		counter.Inc()
		return nil
	})
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TransferAll:  true,
		ToAllNodes:   true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	suite.Equal(counter.Load(), int64(4))
	suite.Len(nodeSet.Collect(), 3)

	// test transfer segment idempotent
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	counter = atomic.NewInt64(0)
	taskIDSet := typeutil.NewUniqueSet()
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		if taskIDSet.Contain(t.ID()) {
			return errors.New("duplicate task")
		}
		return nil
	})
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TransferAll:  true,
		ToAllNodes:   true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	resp, err = suite.server.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: nodes[0],
		TransferAll:  true,
		ToAllNodes:   true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
}

func (suite *OpsServiceSuite) TestTransferChannel() {
	ctx := context.Background()

	// test server unhealthy
	suite.server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err := suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
	// test source node not healthy
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: 1,
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	collectionID := int64(1)
	partitionID := int64(1)
	replicaID := int64(1)
	nodes := []int64{1, 2, 3, 4}
	replica := utils.CreateTestReplica(replicaID, collectionID, nodes)
	suite.meta.ReplicaManager.Put(replica)
	collection := utils.CreateTestCollection(collectionID, 1)
	partition := utils.CreateTestPartition(partitionID, collectionID)
	suite.meta.PutCollection(collection, partition)
	segmentIDs := []int64{1, 2, 3, 4}
	channelNames := []string{"channel-1", "channel-2", "channel-3", "channel-4"}

	// test target node not healthy
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	segments := []*datapb.SegmentInfo{
		{
			ID:            segmentIDs[0],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[0],
			NumOfRows:     1,
		},
		{
			ID:            segmentIDs[1],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[1],
			NumOfRows:     1,
		},
		{
			ID:            segmentIDs[2],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[2],
			NumOfRows:     1,
		},
		{
			ID:            segmentIDs[3],
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelNames[3],
			NumOfRows:     1,
		},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[0],
		},
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[1],
		},
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[2],
		},
		{
			CollectionID: collectionID,
			ChannelName:  channelNames[3],
		},
	}
	segmentInfos := lo.Map(segments, func(segment *datapb.SegmentInfo, _ int) *meta.Segment {
		return &meta.Segment{
			SegmentInfo: segment,
			Node:        nodes[0],
		}
	})
	suite.dist.SegmentDistManager.Update(1, segmentInfos...)
	chanenlInfos := lo.Map(channels, func(channel *datapb.VchannelInfo, _ int) *meta.DmChannel {
		return &meta.DmChannel{
			VchannelInfo: channel,
			Node:         nodes[0],
		}
	})

	// test channel not exist in node
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		ChannelName:  channelNames[0],
	})
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	suite.dist.ChannelDistManager.Update(1, chanenlInfos[0])

	// test channel not exist in current target, expect no task assign and success
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		ChannelName:  channelNames[0],
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(channels, segments, nil)
	suite.targetMgr.UpdateCollectionNextTarget(1)
	suite.targetMgr.UpdateCollectionCurrentTarget(1)
	suite.dist.SegmentDistManager.Update(1, segmentInfos...)
	suite.dist.ChannelDistManager.Update(1, chanenlInfos...)

	for _, node := range nodes {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "localhost",
			Hostname: "localhost",
		}))
		suite.meta.ResourceManager.HandleNodeUp(node)
	}

	// test transfer channel success, expect generate 1 balance channel task
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 2)
		suite.Equal(actions[0].Node(), int64(2))
		return nil
	})
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		ChannelName:  channelNames[0],
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))

	// test copy mode, expect generate 1 load segment task
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 1)
		suite.Equal(actions[0].Node(), int64(2))
		return nil
	})
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		ChannelName:  channelNames[0],
		CopyMode:     true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))

	// test transfer all channels, expect generate 4 load segment task
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	counter := atomic.NewInt64(0)
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 2)
		suite.Equal(actions[0].Node(), int64(2))
		counter.Inc()
		return nil
	})
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TargetNodeID: nodes[1],
		TransferAll:  true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	suite.Equal(counter.Load(), int64(4))

	// test transfer all channels to all nodes, expect generate 4 load segment task
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	counter = atomic.NewInt64(0)
	nodeSet := typeutil.NewUniqueSet()
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		actions := t.Actions()
		suite.Equal(len(actions), 2)
		nodeSet.Insert(actions[0].Node())
		counter.Inc()
		return nil
	})
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TransferAll:  true,
		ToAllNodes:   true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	suite.Equal(counter.Load(), int64(4))
	suite.Len(nodeSet.Collect(), 3)

	// test transfer channel idempotent
	suite.taskScheduler.ExpectedCalls = nil
	suite.taskScheduler.EXPECT().GetSegmentTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	suite.taskScheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	counter = atomic.NewInt64(0)
	taskIDSet := typeutil.NewUniqueSet()
	suite.taskScheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(t task.Task) error {
		if taskIDSet.Contain(t.ID()) {
			return errors.New("duplicate task")
		}
		return nil
	})

	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TransferAll:  true,
		ToAllNodes:   true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
	resp, err = suite.server.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: nodes[0],
		TransferAll:  true,
		ToAllNodes:   true,
	})
	suite.NoError(err)
	suite.True(merr.Ok(resp))
}

func TestOpsService(t *testing.T) {
	suite.Run(t, new(OpsServiceSuite))
}
