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

package balance

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type StoppingBalancerTestSuite struct {
	suite.Suite
	balancer    *StoppingBalancer
	kv          kv.MetaKv
	broker      *meta.MockBroker
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
	targetMgr   *meta.TargetManager
	scheduler   *task.MockScheduler
}

func (suite *StoppingBalancerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *StoppingBalancerTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
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
	suite.broker = meta.NewMockBroker(suite.T())

	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeManager = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeManager)
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.dist = meta.NewDistributionManager(suite.nodeManager)
	suite.scheduler = task.NewMockScheduler(suite.T())
	suite.scheduler.EXPECT().GetSegmentTaskNum(mock.Anything, mock.Anything, mock.Anything).Return(0).Maybe()

	// Create a mock assign policy for testing
	mockAssignPolicy := assign.NewMockAssignPolicy(suite.T())
	// Set up default expectations for the mock
	mockAssignPolicy.EXPECT().AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]assign.SegmentAssignPlan{}).Maybe()
	mockAssignPolicy.EXPECT().AssignChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]assign.ChannelAssignPlan{}).Maybe()

	suite.balancer = NewStoppingBalancer(suite.dist, suite.targetMgr, mockAssignPolicy, suite.nodeManager, suite.scheduler)
}

func (suite *StoppingBalancerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *StoppingBalancerTestSuite) TestBalanceReplica_NoRONodes() {
	ctx := context.Background()

	// Create a collection and replica with only RW nodes
	collectionID := int64(1)
	replicaID := int64(101)

	suite.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})

	suite.meta.Put(ctx, utils.CreateTestReplica(replicaID, collectionID, []int64{1, 2, 3}))

	// Add nodes to node manager
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost:1",
		Hostname: "localhost",
	}))
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost:2",
		Hostname: "localhost",
	}))
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   3,
		Address:  "localhost:3",
		Hostname: "localhost",
	}))

	replica := suite.meta.Get(ctx, replicaID)
	segmentPlans, channelPlans := suite.balancer.BalanceReplica(ctx, replica)

	// Should return empty plans since there are no RO nodes
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
}

func (suite *StoppingBalancerTestSuite) TestBalanceReplica_WithRONodes() {
	ctx := context.Background()

	// Create a collection and replica with RO nodes
	collectionID := int64(1)
	replicaID := int64(101)
	channelName := "test-channel"

	suite.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})

	replica := meta.NewReplica(
		&querypb.Replica{
			ID:           replicaID,
			CollectionID: collectionID,
			Nodes:        []int64{1, 2}, // RW nodes
			RoNodes:      []int64{3, 4}, // RO nodes (stopping)
		},
		typeutil.NewUniqueSet([]int64{1, 2, 3, 4}...),
	)
	suite.meta.Put(ctx, replica)

	// Add nodes to node manager
	for i := int64(1); i <= 4; i++ {
		suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   i,
			Address:  "localhost",
			Hostname: "localhost",
		}))
	}

	// Add some segments on RO nodes
	segments := []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            101,
				CollectionID:  collectionID,
				PartitionID:   10,
				InsertChannel: channelName,
				NumOfRows:     1000,
			},
			Node: 3, // On RO node
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            102,
				CollectionID:  collectionID,
				PartitionID:   10,
				InsertChannel: channelName,
				NumOfRows:     1000,
			},
			Node: 4, // On RO node
		},
	}

	for _, segment := range segments {
		suite.dist.SegmentDistManager.Update(segment.Node, segment)
	}

	// Add target segments
	suite.broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{
		segments[0].SegmentInfo,
		segments[1].SegmentInfo,
	}, nil).Maybe()
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
	suite.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{10}, nil).Maybe()

	suite.meta.PutPartition(ctx, utils.CreateTestPartition(collectionID, 10))
	suite.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID)

	// Add channels on RO nodes
	dmChannels := []*meta.DmChannel{
		{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  channelName,
			},
			Node: 3, // On RO node
		},
	}

	for _, channel := range dmChannels {
		suite.dist.ChannelDistManager.Update(channel.Node, channel)
	}

	// Set up mock assign policy to return some plans
	mockPolicy := suite.balancer.assignPolicy.(*assign.MockAssignPolicy)

	// Mock segment assignment - won't be called if channels are balanced
	mockPolicy.ExpectedCalls = nil // Clear default expectations
	mockPolicy.EXPECT().AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(nodes []int64) bool {
		return len(nodes) == 2 && nodes[0] == 1 && nodes[1] == 2 // RW nodes
	}), mock.Anything).Return([]assign.SegmentAssignPlan{
		{Segment: segments[0], To: 1}, // Move segment 101 from node 3 to node 1
		{Segment: segments[1], To: 2}, // Move segment 102 from node 4 to node 2
	}).Maybe() // May or may not be called depending on channel balance

	// Mock channel assignment - Only node 3 has a channel, so called once
	mockPolicy.EXPECT().AssignChannel(mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(nodes []int64) bool {
		return len(nodes) == 2 && nodes[0] == 1 && nodes[1] == 2 // RW nodes
	}), mock.Anything).Return([]assign.ChannelAssignPlan{
		{Channel: dmChannels[0], To: 1}, // Move channel from node 3 to node 1
	}).Once() // Only called once for node 3 which has the channel

	gotReplica := suite.meta.Get(ctx, replicaID)
	segmentPlans, channelPlans := suite.balancer.BalanceReplica(ctx, gotReplica)

	// StoppingBalancer prioritizes channel balance over segment balance
	// If channels are generated, segments won't be generated (to avoid concurrent operations)
	// So we should have either channel plans OR segment plans, but not necessarily both
	suite.True(len(segmentPlans) > 0 || len(channelPlans) > 0,
		"Should generate either segment or channel plans for stopping balance")

	// If channel plans were generated, verify them
	if len(channelPlans) > 0 {
		suite.NotEmpty(channelPlans, "Should generate channel plans to move channels from RO nodes")
		for _, plan := range channelPlans {
			suite.NotZero(plan.From, "Channel plan should have From node set")
			suite.NotNil(plan.Replica, "Channel plan should have Replica set")
			suite.Equal(replicaID, plan.Replica.GetID())
		}
	}

	// If segment plans were generated, verify them
	if len(segmentPlans) > 0 {
		suite.NotEmpty(segmentPlans, "Should generate segment plans to move segments from RO nodes")
		for _, plan := range segmentPlans {
			suite.NotZero(plan.From, "Segment plan should have From node set")
			suite.NotNil(plan.Replica, "Segment plan should have Replica set")
			suite.Equal(replicaID, plan.Replica.GetID())
		}
	}
}

func (suite *StoppingBalancerTestSuite) TestConcurrentStreamingAndQueryNodeScaleDownDefersChannelUntilSegmentBalanceDone() {
	ctx := context.Background()
	originalStreamingEnabled := os.Getenv(streamingutil.MilvusStreamingServiceEnabled)
	streamingutil.SetStreamingServiceEnabled()
	defer func() {
		suite.Require().NoError(os.Setenv(streamingutil.MilvusStreamingServiceEnabled, originalStreamingEnabled))
	}()

	const (
		collectionID = int64(467055916946244779)
		replicaID    = int64(467055916958089221)
		segmentID    = int64(467055916977292383)
		channel      = "milvus-008wbh5tims8-rootcoord-dml_1_467055916946244779v0"
	)

	pendingSegmentMove := atomic.NewInt32(0)
	suite.scheduler.ExpectedCalls = nil
	suite.scheduler.EXPECT().GetSegmentTaskNum(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(...task.TaskFilter) int {
		return int(pendingSegmentMove.Load())
	}).Maybe()

	collection := utils.CreateTestCollection(collectionID, 1)
	collection.Status = querypb.LoadStatus_Loaded
	collection.LoadPercentage = 100
	suite.meta.PutCollection(ctx, collection)
	suite.meta.PutPartition(ctx, utils.CreateTestPartition(collectionID, collectionID))

	targetChannel := &datapb.VchannelInfo{
		CollectionID:        collectionID,
		ChannelName:         channel,
		FlushedSegmentIds:   []int64{segmentID},
		UnflushedSegmentIds: []int64{},
	}
	targetSegment := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   collectionID,
		InsertChannel: channel,
		NumOfRows:     100,
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		[]*datapb.VchannelInfo{targetChannel},
		[]*datapb.SegmentInfo{targetSegment},
		nil,
	)
	suite.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{collectionID}, nil).Maybe()
	suite.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID)

	for _, nodeID := range []int64{40, 43, 44, 50, 57, 38} {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1:0",
			Hostname: "localhost",
			Version:  common.Version,
		})
		suite.nodeManager.Add(nodeInfo)
	}

	mockPolicy := suite.balancer.assignPolicy.(*assign.MockAssignPolicy)
	mockPolicy.ExpectedCalls = nil
	mockPolicy.EXPECT().AssignSegment(mock.Anything, collectionID, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, segments []*meta.Segment, nodes []int64, _ bool) []assign.SegmentAssignPlan {
			suite.Require().NotEmpty(segments)
			suite.Require().NotEmpty(nodes)
			return []assign.SegmentAssignPlan{{Segment: segments[0], To: nodes[0]}}
		}).Maybe()
	channelAssignCalls := atomic.NewInt32(0)
	mockPolicy.EXPECT().AssignChannel(mock.Anything, collectionID, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, channels []*meta.DmChannel, _ []int64, _ bool) []assign.ChannelAssignPlan {
			channelAssignCalls.Inc()
			suite.Require().Len(channels, 1)
			return []assign.ChannelAssignPlan{{Channel: channels[0], To: 44}}
		}).Maybe()

	replicaTick1 := meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  collectionID,
		Nodes:         []int64{40, 43},
		RoNodes:       []int64{50},
		RwSqNodes:     []int64{44, 57},
		ResourceGroup: meta.DefaultResourceGroupName,
	})
	suite.dist.SegmentDistManager.Update(50, &meta.Segment{SegmentInfo: targetSegment, Node: 50})

	segmentPlans, channelPlans := suite.balancer.BalanceReplica(ctx, replicaTick1)
	suite.Empty(channelPlans)
	suite.Len(segmentPlans, 1)
	suite.Equal(segmentID, segmentPlans[0].Segment.GetID())
	suite.Equal(int64(50), segmentPlans[0].From)
	suite.Equal(int64(40), segmentPlans[0].To)
	suite.Equal(int32(0), channelAssignCalls.Load())

	suite.nodeManager.Stopping(40)
	replicaTick2 := meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  collectionID,
		Nodes:         []int64{43},
		RoNodes:       []int64{50},
		RwSqNodes:     []int64{44, 57},
		RoSqNodes:     []int64{38},
		ResourceGroup: meta.DefaultResourceGroupName,
	})
	suite.dist.ChannelDistManager.Update(38, &meta.DmChannel{
		VchannelInfo: targetChannel,
		Node:         38,
		View: &meta.LeaderView{
			ID:           38,
			CollectionID: collectionID,
			Channel:      channel,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	segmentPlans, channelPlans = suite.balancer.BalanceReplica(ctx, replicaTick2)
	suite.Empty(channelPlans)
	suite.Len(segmentPlans, 1)
	suite.Equal(segmentID, segmentPlans[0].Segment.GetID())
	suite.Equal(int64(50), segmentPlans[0].From)
	suite.Equal(int64(43), segmentPlans[0].To)
	suite.Equal(int32(0), channelAssignCalls.Load())

	replicaTick3 := meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  collectionID,
		Nodes:         []int64{43},
		RwSqNodes:     []int64{44, 57},
		RoSqNodes:     []int64{38},
		ResourceGroup: meta.DefaultResourceGroupName,
	})
	suite.dist.SegmentDistManager.Update(50)
	suite.dist.SegmentDistManager.Update(43, &meta.Segment{SegmentInfo: targetSegment, Node: 43})

	pendingSegmentMove.Store(1)
	segmentPlans, channelPlans = suite.balancer.BalanceReplica(ctx, replicaTick3)
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
	suite.Equal(int32(0), channelAssignCalls.Load())

	pendingSegmentMove.Store(0)
	segmentPlans, channelPlans = suite.balancer.BalanceReplica(ctx, replicaTick3)
	suite.Empty(segmentPlans)
	suite.Len(channelPlans, 1)
	suite.Equal(channel, channelPlans[0].Channel.GetChannelName())
	suite.Equal(int64(38), channelPlans[0].From)
	suite.Equal(int64(44), channelPlans[0].To)
	suite.Equal(int32(1), channelAssignCalls.Load())
}

func (suite *StoppingBalancerTestSuite) TestBalanceReplica_NoRWNodes() {
	ctx := context.Background()

	// Create a collection and replica with only RO nodes (edge case)
	collectionID := int64(1)
	replicaID := int64(101)

	suite.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})

	replica := meta.NewReplica(
		&querypb.Replica{
			ID:           replicaID,
			CollectionID: collectionID,
			Nodes:        []int64{},     // No RW nodes
			RoNodes:      []int64{1, 2}, // Only RO nodes
		},
		typeutil.NewUniqueSet([]int64{1, 2}...),
	)
	suite.meta.Put(ctx, replica)

	// Add nodes to node manager
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost:1",
		Hostname: "localhost",
	}))
	suite.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost:2",
		Hostname: "localhost",
	}))

	gotReplica2 := suite.meta.Get(ctx, replicaID)
	segmentPlans, channelPlans := suite.balancer.BalanceReplica(ctx, gotReplica2)

	// Should return empty plans since there are no RW nodes to move to
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
}

func TestStoppingBalancerSuite(t *testing.T) {
	suite.Run(t, new(StoppingBalancerTestSuite))
}
