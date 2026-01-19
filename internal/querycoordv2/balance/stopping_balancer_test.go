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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	// Create a mock assign policy for testing
	mockAssignPolicy := assign.NewMockAssignPolicy(suite.T())
	// Set up default expectations for the mock
	mockAssignPolicy.EXPECT().AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]assign.SegmentAssignPlan{}).Maybe()
	mockAssignPolicy.EXPECT().AssignChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]assign.ChannelAssignPlan{}).Maybe()

	suite.balancer = NewStoppingBalancer(suite.dist, suite.targetMgr, mockAssignPolicy, suite.nodeManager)
}

func (suite *StoppingBalancerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *StoppingBalancerTestSuite) TestBalanceReplica_NoRONodes() {
	ctx := context.Background()

	// Create a collection and replica with only RW nodes
	collectionID := int64(1)
	replicaID := int64(101)

	suite.meta.CollectionManager.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})

	suite.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(replicaID, collectionID, []int64{1, 2, 3}))

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

	replica := suite.meta.ReplicaManager.Get(ctx, replicaID)
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

	suite.meta.CollectionManager.PutCollection(ctx, &meta.Collection{
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
	suite.meta.ReplicaManager.Put(ctx, replica)

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

	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(collectionID, 10))
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

	gotReplica := suite.meta.ReplicaManager.Get(ctx, replicaID)
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

func (suite *StoppingBalancerTestSuite) TestBalanceReplica_NoRWNodes() {
	ctx := context.Background()

	// Create a collection and replica with only RO nodes (edge case)
	collectionID := int64(1)
	replicaID := int64(101)

	suite.meta.CollectionManager.PutCollection(ctx, &meta.Collection{
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
	suite.meta.ReplicaManager.Put(ctx, replica)

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

	gotReplica2 := suite.meta.ReplicaManager.Get(ctx, replicaID)
	segmentPlans, channelPlans := suite.balancer.BalanceReplica(ctx, gotReplica2)

	// Should return empty plans since there are no RW nodes to move to
	suite.Empty(segmentPlans)
	suite.Empty(channelPlans)
}

func TestStoppingBalancerSuite(t *testing.T) {
	suite.Run(t, new(StoppingBalancerTestSuite))
}
