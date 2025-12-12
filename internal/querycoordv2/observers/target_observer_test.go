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

package observers

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type TargetObserverSuite struct {
	suite.Suite

	kv kv.MetaKv
	// dependency
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    *meta.MockBroker
	cluster   *session.MockCluster

	observer *TargetObserver

	collectionID       int64
	partitionID        int64
	nextTargetSegments []*datapb.SegmentInfo
	nextTargetChannels []*datapb.VchannelInfo
	ctx                context.Context
}

func (suite *TargetObserverSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.UpdateNextTargetInterval.Key, "3")
}

func (suite *TargetObserverSuite) SetupTest() {
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
	suite.ctx = context.Background()

	// meta
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 1,
	}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 2,
	}))
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.meta = meta.NewMeta(idAllocator, store, nodeMgr)

	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.distMgr = meta.NewDistributionManager(nodeMgr)
	suite.cluster = session.NewMockCluster(suite.T())
	suite.observer = NewTargetObserver(suite.meta, suite.targetMgr, suite.distMgr, suite.broker, suite.cluster, nodeMgr)
	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)

	testCollection := utils.CreateTestCollection(suite.collectionID, 1)
	testCollection.Status = querypb.LoadStatus_Loaded
	err = suite.meta.CollectionManager.PutCollection(suite.ctx, testCollection)
	suite.NoError(err)
	err = suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(suite.collectionID, suite.partitionID))
	suite.NoError(err)
	replicas, err := suite.meta.ReplicaManager.Spawn(suite.ctx, suite.collectionID, map[string]int{meta.DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	replicas[0].AddRWNode(2)
	err = suite.meta.ReplicaManager.Put(suite.ctx, replicas...)
	suite.NoError(err)

	suite.nextTargetChannels = []*datapb.VchannelInfo{
		{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-1",
			DeleteCheckpoint: &msgpb.MsgPosition{
				Timestamp: 200,
			},
		},
		{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-2",
			DeleteCheckpoint: &msgpb.MsgPosition{
				Timestamp: 200,
			},
		},
	}

	suite.nextTargetSegments = []*datapb.SegmentInfo{
		{
			ID:            11,
			PartitionID:   suite.partitionID,
			InsertChannel: "channel-1",
		},
		{
			ID:            12,
			PartitionID:   suite.partitionID,
			InsertChannel: "channel-2",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(suite.nextTargetChannels, suite.nextTargetSegments, nil)
	suite.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	suite.observer.Start()
}

func (suite *TargetObserverSuite) TestTriggerUpdateTarget() {
	ctx := suite.ctx

	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetSealedSegmentsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2 &&
			len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2
	}, 5*time.Second, 1*time.Second)

	suite.distMgr.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-1",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-1",
			Segments: map[int64]*querypb.SegmentDist{
				11: {NodeID: 2},
			},
		},
	}, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-2",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-2",
			Segments: map[int64]*querypb.SegmentDist{
				12: {NodeID: 2},
			},
		},
	})

	// Never update current target if it's empty, even the next target is ready
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.CurrentTarget)) == 0
	}, 3*time.Second, 1*time.Second)

	suite.broker.AssertExpectations(suite.T())
	suite.broker.ExpectedCalls = suite.broker.ExpectedCalls[:0]
	suite.nextTargetSegments = append(suite.nextTargetSegments, &datapb.SegmentInfo{
		ID:            13,
		PartitionID:   suite.partitionID,
		InsertChannel: "channel-1",
	})
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, suite.collectionID)

	// Pull next again
	suite.broker.EXPECT().
		GetRecoveryInfoV2(mock.Anything, mock.Anything).
		Return(suite.nextTargetChannels, suite.nextTargetSegments, nil)

	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetSealedSegmentsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 3 &&
			len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2
	}, 7*time.Second, 1*time.Second)
	suite.broker.AssertExpectations(suite.T())

	// Manually update next target
	ready, err := suite.observer.UpdateNextTarget(suite.collectionID)
	suite.NoError(err)
	suite.distMgr.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-1",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-1",
			Segments: map[int64]*querypb.SegmentDist{
				11: {NodeID: 2},
				13: {NodeID: 2},
			},
		},
	}, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-2",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-2",
			Segments: map[int64]*querypb.SegmentDist{
				12: {NodeID: 2},
			},
		},
	})

	suite.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Able to update current if it's not empty
	suite.Eventually(func() bool {
		isReady := false
		select {
		case <-ready:
			isReady = true
		default:
		}
		return isReady &&
			len(suite.targetMgr.GetSealedSegmentsByCollection(ctx, suite.collectionID, meta.CurrentTarget)) == 3 &&
			len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.CurrentTarget)) == 2
	}, 7*time.Second, 1*time.Second)

	ch1View := suite.distMgr.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel("channel-1"))[0].View
	action := suite.observer.genSyncAction(ctx, ch1View, 100)
	suite.Equal(action.GetDeleteCP().Timestamp, uint64(200))
}

func (suite *TargetObserverSuite) TestTriggerRelease() {
	ctx := suite.ctx
	// Manually update next target
	_, err := suite.observer.UpdateNextTarget(suite.collectionID)
	suite.NoError(err)

	// manually release partition
	partitions := suite.meta.CollectionManager.GetPartitionsByCollection(ctx, suite.collectionID)
	partitionIDs := lo.Map(partitions, func(partition *meta.Partition, _ int) int64 { return partition.PartitionID })
	suite.observer.ReleasePartition(suite.collectionID, partitionIDs[0])

	// manually release collection
	suite.observer.ReleaseCollection(suite.collectionID)
}

func (suite *TargetObserverSuite) TearDownTest() {
	suite.kv.Close()
	suite.observer.Stop()
}

type TargetObserverCheckSuite struct {
	suite.Suite

	kv kv.MetaKv
	// dependency
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    *meta.MockBroker
	cluster   *session.MockCluster

	observer *TargetObserver

	collectionID int64
	partitionID  int64
	ctx          context.Context
}

func (suite *TargetObserverCheckSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *TargetObserverCheckSuite) SetupTest() {
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
	suite.ctx = context.Background()

	// meta
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	nodeMgr := session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, nodeMgr)

	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.distMgr = meta.NewDistributionManager(nodeMgr)
	suite.cluster = session.NewMockCluster(suite.T())
	suite.observer = NewTargetObserver(
		suite.meta,
		suite.targetMgr,
		suite.distMgr,
		suite.broker,
		suite.cluster,
		nodeMgr,
	)
	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)

	err = suite.meta.CollectionManager.PutCollection(suite.ctx, utils.CreateTestCollection(suite.collectionID, 1))
	suite.NoError(err)
	err = suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(suite.collectionID, suite.partitionID))
	suite.NoError(err)
	replicas, err := suite.meta.ReplicaManager.Spawn(suite.ctx, suite.collectionID, map[string]int{meta.DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	replicas[0].AddRWNode(2)
	err = suite.meta.ReplicaManager.Put(suite.ctx, replicas...)
	suite.NoError(err)
}

func (s *TargetObserverCheckSuite) TestCheck() {
	r := s.observer.Check(context.Background(), s.collectionID, common.AllPartitionsID)
	s.False(r)
	s.False(s.observer.loadedDispatcher.tasks.Contain(s.collectionID))
	s.True(s.observer.loadingDispatcher.tasks.Contain(s.collectionID))
}

// TestShouldUpdateCurrentTarget_EmptyNextTarget tests when next target is empty
func TestShouldUpdateCurrentTarget_EmptyNextTarget(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)

	nodeMgr := session.NewNodeManager()
	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Use a minimal meta without CollectionManager since we only test targetMgr behavior
	metaInstance := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
	}

	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, broker, cluster, nodeMgr)

	// Return empty channels to simulate empty next target
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(map[string]*meta.DmChannel{}).Maybe()

	result := observer.shouldUpdateCurrentTarget(ctx, collectionID)
	assert.False(t, result)
}

// TestShouldUpdateCurrentTarget_ReplicaReadiness tests the replica-based readiness check
func TestShouldUpdateCurrentTarget_ReplicaReadiness(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))

	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Create mock replicas
	replica1 := meta.NewMockReplica(t)
	replica1.EXPECT().GetID().Return(int64(1)).Maybe()
	replica1.EXPECT().GetCollectionID().Return(collectionID).Maybe()
	replica1.EXPECT().GetNodes().Return([]int64{1}).Maybe()
	replica1.EXPECT().Contains(int64(1)).Return(true).Maybe()
	replica1.EXPECT().Contains(int64(2)).Return(false).Maybe()

	replica2 := meta.NewMockReplica(t)
	replica2.EXPECT().GetID().Return(int64(2)).Maybe()
	replica2.EXPECT().GetCollectionID().Return(collectionID).Maybe()
	replica2.EXPECT().GetNodes().Return([]int64{2}).Maybe()
	replica2.EXPECT().Contains(int64(1)).Return(false).Maybe()
	replica2.EXPECT().Contains(int64(2)).Return(true).Maybe()

	// Create mock ReplicaManager
	replicaMgr := meta.NewReplicaManager(nil, nil)

	metaInstance := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
		ReplicaManager:    replicaMgr,
	}

	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, broker, cluster, nodeMgr)

	// Setup mock expectations
	channelNames := map[string]*meta.DmChannel{
		"channel-1": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-1"},
		},
		"channel-2": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-2"},
		},
	}
	newVersion := int64(100)

	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(channelNames).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Test case: replica1 (node1) has both channels ready
	distMgr.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:            1,
			CollectionID:  collectionID,
			Channel:       "channel-1",
			TargetVersion: newVersion,
			Segments: map[int64]*querypb.SegmentDist{
				11: {NodeID: 1},
			},
		},
	}, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-2",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:            1,
			CollectionID:  collectionID,
			Channel:       "channel-2",
			TargetVersion: newVersion,
			Segments: map[int64]*querypb.SegmentDist{
				12: {NodeID: 1},
			},
		},
	})

	// replica2 (node2) only has channel-1, missing channel-2
	// This simulates the "replica lack of nodes" scenario
	distMgr.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:            2,
			CollectionID:  collectionID,
			Channel:       "channel-1",
			TargetVersion: newVersion,
			Segments: map[int64]*querypb.SegmentDist{
				11: {NodeID: 2},
			},
		},
	})

	// With new implementation:
	// - replica1 is ready (has both channels)
	// - replica2 is NOT ready (missing channel-2)
	// Since ReplicaManager.GetByCollection returns empty (no replicas in the mock manager),
	// readyDelegatorsInCollection will be empty, and shouldUpdateCurrentTarget returns false.
	result := observer.shouldUpdateCurrentTarget(ctx, collectionID)
	assert.False(t, result)
}

// TestShouldUpdateCurrentTarget_OnlyReadyDelegatorsSynced verifies that only ready delegators
// are included in the sync operation. This test specifically validates the fix for the bug where
// all delegators (including non-ready ones) were being added to readyDelegatorsInReplica.
func TestShouldUpdateCurrentTarget_OnlyReadyDelegatorsSynced(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)
	newVersion := int64(100)

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))

	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Create a real replica with node 1 and node 2
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{1, 2},
	})

	// Create mock catalog for ReplicaManager
	mockCatalog := mocks.NewQueryCoordCatalog(t)
	mockCatalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Create real ReplicaManager with mock catalog and put the replica into it
	replicaMgr := meta.NewReplicaManager(nil, mockCatalog)
	err := replicaMgr.Put(ctx, replica)
	assert.NoError(t, err)

	metaInstance := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
		ReplicaManager:    replicaMgr,
	}

	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, broker, cluster, nodeMgr)

	// Setup target manager expectations
	channelNames := map[string]*meta.DmChannel{
		"channel-1": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-1"},
		},
	}

	// Define a segment that exists in target but only node 1 has it loaded
	segmentID := int64(100)
	targetSegments := map[int64]*datapb.SegmentInfo{
		segmentID: {ID: segmentID, CollectionID: collectionID, InsertChannel: "channel-1"},
	}

	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(channelNames).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	// Return a segment in target - this will be checked by CheckDelegatorDataReady
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(targetSegments).Maybe()
	targetMgr.EXPECT().GetGrowingSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDroppedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetPartitions(mock.Anything, collectionID, mock.Anything).Return([]int64{}, nil).Maybe()

	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	// Track which nodes receive SyncDistribution calls
	syncedNodes := make([]int64, 0)
	cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
			syncedNodes = append(syncedNodes, nodeID)
			return merr.Success(), nil
		}).Maybe()

	// Node 1: READY delegator
	// - Has the target segment loaded (segment 100)
	// - CheckDelegatorDataReady will return nil (ready)
	distMgr.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: collectionID,
			Channel:      "channel-1",
			Segments: map[int64]*querypb.SegmentDist{
				segmentID: {NodeID: 1}, // Has the required segment
			},
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	// Node 2: NOT READY delegator
	// - Does NOT have the target segment loaded (missing segment 100)
	// - CheckDelegatorDataReady will return error (not ready)
	distMgr.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: collectionID,
			Channel:      "channel-1",
			Segments:     map[int64]*querypb.SegmentDist{}, // Missing the required segment!
			Status:       &querypb.LeaderViewStatus{Serviceable: false},
		},
	})

	// Execute the function under test
	result := observer.shouldUpdateCurrentTarget(ctx, collectionID)

	// Verify the result is true (at least one ready delegator exists)
	assert.True(t, result)

	// Verify that ONLY node 1 received SyncDistribution call
	// This is the key assertion: if the bug existed (using delegatorList instead of readyDelegatorsInChannel),
	// node 2 would also receive a SyncDistribution call
	assert.Equal(t, 1, len(syncedNodes), "Expected only 1 SyncDistribution call for the ready delegator")
	assert.Contains(t, syncedNodes, int64(1), "Expected node 1 (ready delegator) to receive SyncDistribution")
	assert.NotContains(t, syncedNodes, int64(2), "Node 2 (not ready delegator) should NOT receive SyncDistribution")
}

func TestTargetObserver(t *testing.T) {
	suite.Run(t, new(TargetObserverSuite))
	suite.Run(t, new(TargetObserverCheckSuite))
}
