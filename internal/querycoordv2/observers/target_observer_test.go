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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	err = suite.meta.PutCollection(suite.ctx, testCollection)
	suite.NoError(err)
	err = suite.meta.PutPartition(suite.ctx, utils.CreateTestPartition(suite.collectionID, suite.partitionID))
	suite.NoError(err)
	replicas, err := suite.meta.Spawn(suite.ctx, suite.collectionID, map[string]int{meta.DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	replicas[0].AddRWNode(2)
	err = suite.meta.Put(suite.ctx, replicas...)
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

// TestInitialLoad_ShouldNotUpdateCurrentTarget verifies that when CurrentTarget is empty,
// it should NOT be updated even when NextTarget is ready and all nodes have loaded the data.
// This is a critical safety mechanism to ensure the system doesn't expose incomplete data.
func (suite *TargetObserverSuite) TestInitialLoad_ShouldNotUpdateCurrentTarget() {
	ctx := suite.ctx

	// Wait for observer to automatically update NextTarget with initial 2 segments
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetSealedSegmentsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2 &&
			len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2
	}, 5*time.Second, 1*time.Second)

	// Simulate distributed environment: Node 2 has loaded all channels and segments
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

	// Key verification: CurrentTarget should remain empty even though NextTarget is ready
	// This ensures we don't expose data before explicitly updating CurrentTarget
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.CurrentTarget)) == 0
	}, 3*time.Second, 1*time.Second)

	// Verify all expected broker calls were made
	suite.broker.AssertExpectations(suite.T())
}

// TestIncrementalUpdate_WithNewSegment verifies that when CurrentTarget is not empty,
// the observer can automatically detect and update to include new segments.
// This simulates a real-world scenario where data is continuously ingested.
func (suite *TargetObserverSuite) TestIncrementalUpdate_WithNewSegment() {
	ctx := suite.ctx

	// Wait for initial load: 2 segments in NextTarget
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetSealedSegmentsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2 &&
			len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2
	}, 5*time.Second, 1*time.Second)

	// Add initial segment distribution for CheckSegmentDataReady
	suite.distMgr.SegmentDistManager.Update(2,
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            11,
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionID,
				InsertChannel: "channel-1",
			},
			Node: 2,
		},
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            12,
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionID,
				InsertChannel: "channel-2",
			},
			Node: 2,
		},
	)

	// Manually set CurrentTarget to non-empty (simulating previous successful load)
	// This is the precondition for incremental updates to work
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, suite.collectionID)

	// Clear previous mock expectations and prepare for new segment
	suite.broker.AssertExpectations(suite.T())
	suite.broker.ExpectedCalls = suite.broker.ExpectedCalls[:0]

	// Simulate new data arrival: Add segment 13 to the segment list
	suite.nextTargetSegments = append(suite.nextTargetSegments, &datapb.SegmentInfo{
		ID:            13,
		PartitionID:   suite.partitionID,
		InsertChannel: "channel-1",
	})

	// Setup mocks for the new segment discovery phase
	// These mocks will be used by the background goroutine when it polls for updates
	suite.broker.EXPECT().
		GetRecoveryInfoV2(mock.Anything, mock.Anything).
		Return(suite.nextTargetChannels, suite.nextTargetSegments, nil)
	suite.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	// Manually trigger update so the observer discovers the new segment immediately
	// instead of waiting for the background ticker (default interval 10s, which would
	// make Eventually flaky when timeout < ticker interval).
	ready, err := suite.observer.UpdateNextTarget(suite.collectionID)
	suite.NoError(err)

	// Verify the observer picked up segment 13 in NextTarget
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetSealedSegmentsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 3 &&
			len(suite.targetMgr.GetDmChannelsByCollection(ctx, suite.collectionID, meta.NextTarget)) == 2
	}, 7*time.Second, 1*time.Second)
	suite.broker.AssertExpectations(suite.T())

	// Simulate nodes loading the new segment 13
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
				13: {NodeID: 2}, // New segment loaded
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
	// Add segments to SegmentDistManager for CheckSegmentDataReady
	suite.distMgr.SegmentDistManager.Update(2,
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            11,
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionID,
				InsertChannel: "channel-1",
			},
			Node: 2,
		},
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            12,
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionID,
				InsertChannel: "channel-2",
			},
			Node: 2,
		},
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            13,
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionID,
				InsertChannel: "channel-1",
			},
			Node: 2,
		},
	)

	suite.cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Verify that CurrentTarget is updated to include all 3 segments
	// Since CurrentTarget is not empty, the update should proceed successfully
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

	// Verify sync action contains correct checkpoint information
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
	partitions := suite.meta.GetPartitionsByCollection(ctx, suite.collectionID)
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

	err = suite.meta.PutCollection(suite.ctx, utils.CreateTestCollection(suite.collectionID, 1))
	suite.NoError(err)
	err = suite.meta.PutPartition(suite.ctx, utils.CreateTestPartition(suite.collectionID, suite.partitionID))
	suite.NoError(err)
	replicas, err := suite.meta.Spawn(suite.ctx, suite.collectionID, map[string]int{meta.DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	replicas[0].AddRWNode(2)
	err = suite.meta.Put(suite.ctx, replicas...)
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
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(map[int64]*datapb.SegmentInfo{}).Maybe()

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
	// no split window: the next target marks no window targets.
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, collectionID, meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetSplitWindowExclusions(mock.Anything, collectionID, meta.NextTarget).Return(nil, true).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(map[int64]*datapb.SegmentInfo{}).Maybe()
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{}, nil).Maybe()
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
	schemaBarrierTs := uint64(200)
	schema := &schemapb.CollectionSchema{Version: 1}

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
	// no split window: the next target marks no window targets.
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, collectionID, meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetSplitWindowExclusions(mock.Anything, collectionID, meta.NextTarget).Return(nil, true).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	// Return a segment in target - this will be checked by CheckDelegatorDataReady
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(targetSegments).Maybe()
	targetMgr.EXPECT().GetGrowingSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDroppedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetPartitions(mock.Anything, collectionID, mock.Anything).Return([]int64{}, nil).Maybe()
	// Return segments for CheckSegmentDataReady
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(targetSegments).Maybe()

	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{
		Schema:          schema,
		UpdateTimestamp: schemaBarrierTs,
	}, nil).Once()

	// Track which nodes receive SyncDistribution calls
	syncedNodes := make([]int64, 0)
	cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
			syncedNodes = append(syncedNodes, nodeID)
			assert.Same(t, schema, req.GetSchema())
			assert.Equal(t, schemaBarrierTs, req.GetLoadMeta().GetSchemaBarrierTs())
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
	// Add segment to SegmentDistManager for CheckSegmentDataReady
	distMgr.SegmentDistManager.Update(1, &meta.Segment{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collectionID,
			InsertChannel: "channel-1",
		},
		Node: 1,
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

// TestShouldUpdateCurrentTarget_AllChannelsSynced tests that shouldUpdateCurrentTarget returns true
// only when ALL channels are synced successfully. This validates the fix where we check:
// syncSuccess && lo.Every(syncedChannelNames, lo.Keys(channelNames))
func TestShouldUpdateCurrentTarget_AllChannelsSynced(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)
	newVersion := int64(100)

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))

	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Create a real replica with node 1
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{1},
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

	// Setup target manager expectations - TWO channels
	channelNames := map[string]*meta.DmChannel{
		"channel-1": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-1"},
		},
		"channel-2": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-2"},
		},
	}

	// Define segments for both channels
	segmentID1 := int64(100)
	segmentID2 := int64(101)
	targetSegments1 := map[int64]*datapb.SegmentInfo{
		segmentID1: {ID: segmentID1, CollectionID: collectionID, InsertChannel: "channel-1"},
	}
	targetSegments2 := map[int64]*datapb.SegmentInfo{
		segmentID2: {ID: segmentID2, CollectionID: collectionID, InsertChannel: "channel-2"},
	}

	// All segments for CheckSegmentDataReady
	allSegments := map[int64]*datapb.SegmentInfo{
		segmentID1: {ID: segmentID1, CollectionID: collectionID, InsertChannel: "channel-1"},
		segmentID2: {ID: segmentID2, CollectionID: collectionID, InsertChannel: "channel-2"},
	}

	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(channelNames).Maybe()
	// no split window: the next target marks no window targets.
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, collectionID, meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetSplitWindowExclusions(mock.Anything, collectionID, meta.NextTarget).Return(nil, true).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(targetSegments1).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-2", mock.Anything).Return(targetSegments2).Maybe()
	targetMgr.EXPECT().GetGrowingSegmentsByChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDroppedSegmentsByChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetPartitions(mock.Anything, collectionID, mock.Anything).Return([]int64{}, nil).Maybe()
	// Return segments for CheckSegmentDataReady
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(allSegments).Maybe()

	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{}, nil).Maybe()
	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Node 1 has BOTH channels ready
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
				segmentID1: {NodeID: 1},
			},
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	}, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-2",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: collectionID,
			Channel:      "channel-2",
			Segments: map[int64]*querypb.SegmentDist{
				segmentID2: {NodeID: 1},
			},
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	})
	// Add segments to SegmentDistManager for CheckSegmentDataReady
	distMgr.SegmentDistManager.Update(1,
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID1,
				CollectionID:  collectionID,
				InsertChannel: "channel-1",
			},
			Node: 1,
		},
		&meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID2,
				CollectionID:  collectionID,
				InsertChannel: "channel-2",
			},
			Node: 1,
		},
	)

	// Execute the function under test
	result := observer.shouldUpdateCurrentTarget(ctx, collectionID)

	// When all channels are synced, should return true
	assert.True(t, result, "Expected true when ALL channels are synced successfully")
}

// TestShouldUpdateCurrentTarget_PartialChannelsSynced tests that shouldUpdateCurrentTarget returns false
// when only some channels have ready delegators. This is the core behavior of the fix.
func TestShouldUpdateCurrentTarget_PartialChannelsSynced(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)
	newVersion := int64(100)

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))

	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Create a real replica with node 1
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{1},
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

	// Setup target manager expectations - TWO channels in target
	channelNames := map[string]*meta.DmChannel{
		"channel-1": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-1"},
		},
		"channel-2": {
			VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-2"},
		},
	}

	// Define segments
	segmentID1 := int64(100)
	segmentID2 := int64(101)
	targetSegments1 := map[int64]*datapb.SegmentInfo{
		segmentID1: {ID: segmentID1, CollectionID: collectionID, InsertChannel: "channel-1"},
	}
	targetSegments2 := map[int64]*datapb.SegmentInfo{
		segmentID2: {ID: segmentID2, CollectionID: collectionID, InsertChannel: "channel-2"},
	}

	// All segments for CheckSegmentDataReady
	allSegments := map[int64]*datapb.SegmentInfo{
		segmentID1: {ID: segmentID1, CollectionID: collectionID, InsertChannel: "channel-1"},
		segmentID2: {ID: segmentID2, CollectionID: collectionID, InsertChannel: "channel-2"},
	}

	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(channelNames).Maybe()
	// no split window: the next target marks no window targets.
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, collectionID, meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetSplitWindowExclusions(mock.Anything, collectionID, meta.NextTarget).Return(nil, true).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(targetSegments1).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-2", mock.Anything).Return(targetSegments2).Maybe()
	targetMgr.EXPECT().GetGrowingSegmentsByChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDroppedSegmentsByChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetPartitions(mock.Anything, collectionID, mock.Anything).Return([]int64{}, nil).Maybe()
	// Return segments for CheckSegmentDataReady - this will fail since segment distribution is incomplete
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(allSegments).Maybe()

	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{}, nil).Maybe()
	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Node 1 has ONLY channel-1 ready, channel-2 is NOT ready (missing segment)
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
				segmentID1: {NodeID: 1}, // Has the required segment for channel-1
			},
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	}, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  "channel-2",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: collectionID,
			Channel:      "channel-2",
			Segments:     map[int64]*querypb.SegmentDist{}, // Missing the required segment for channel-2!
			Status:       &querypb.LeaderViewStatus{Serviceable: false},
		},
	})

	// Execute the function under test
	result := observer.shouldUpdateCurrentTarget(ctx, collectionID)

	// When only partial channels are synced, should return false
	// This is the key behavior being tested - with the new fix:
	// syncedChannelNames = ["channel-1"] (only channel-1 has ready delegator)
	// channelNames keys = ["channel-1", "channel-2"]
	// lo.Every(["channel-1"], ["channel-1", "channel-2"]) = false
	assert.False(t, result, "Expected false when only PARTIAL channels are synced")
}

// TestShouldUpdateCurrentTarget_NoReadyDelegators tests that shouldUpdateCurrentTarget returns false
// when there are no ready delegators at all.
func TestShouldUpdateCurrentTarget_NoReadyDelegators(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)
	newVersion := int64(100)

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))

	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Create a real replica with node 1
	replica := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{1},
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

	// Define segment that no delegator has
	segmentID := int64(100)
	targetSegments := map[int64]*datapb.SegmentInfo{
		segmentID: {ID: segmentID, CollectionID: collectionID, InsertChannel: "channel-1"},
	}

	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(channelNames).Maybe()
	// no split window: the next target marks no window targets.
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, collectionID, meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetSplitWindowExclusions(mock.Anything, collectionID, meta.NextTarget).Return(nil, true).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.NextTarget).Return(newVersion).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByChannel(mock.Anything, collectionID, "channel-1", mock.Anything).Return(targetSegments).Maybe()
	targetMgr.EXPECT().GetGrowingSegmentsByChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDroppedSegmentsByChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannel(mock.Anything, collectionID, mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().GetPartitions(mock.Anything, collectionID, mock.Anything).Return([]int64{}, nil).Maybe()
	// Return segments for CheckSegmentDataReady - this will fail since no segment in distribution
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, meta.NextTarget).Return(targetSegments).Maybe()

	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{}, nil).Maybe()
	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Node 1 has channel-1 but NOT ready (missing segment)
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
			Segments:     map[int64]*querypb.SegmentDist{}, // Missing the required segment!
			Status:       &querypb.LeaderViewStatus{Serviceable: false},
		},
	})

	// Execute the function under test
	result := observer.shouldUpdateCurrentTarget(ctx, collectionID)

	// When no ready delegators exist, should return false
	// syncedChannelNames = [] (no ready delegators)
	// channelNames keys = ["channel-1"]
	// lo.Every([], ["channel-1"]) = false (empty does not contain all)
	assert.False(t, result, "Expected false when NO ready delegators exist")
}

// TestUpdateAllReplicasCheckpointMetric tests the all-replicas checkpoint metric behavior
func TestUpdateAllReplicasCheckpointMetric(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)
	currentVersion := int64(100)

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))

	targetMgr := meta.NewMockTargetManager(t)
	distMgr := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	cluster := session.NewMockCluster(t)

	// Create two replicas: replica1 on node1, replica2 on node2
	replica1 := meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{1},
	})
	replica2 := meta.NewReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{2},
	})

	mockCatalog := mocks.NewQueryCoordCatalog(t)
	mockCatalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockCatalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	replicaMgr := meta.NewReplicaManager(nil, mockCatalog)
	err := replicaMgr.Put(ctx, replica1, replica2)
	assert.NoError(t, err)

	metaInstance := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
		ReplicaManager:    replicaMgr,
	}

	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, broker, cluster, nodeMgr)

	channelName := "channel-1"
	seekTimestamp := uint64(1000 << 18) // some timestamp
	channels := map[string]*meta.DmChannel{
		channelName: {
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  channelName,
				SeekPosition: &msgpb.MsgPosition{
					Timestamp: seekTimestamp,
				},
			},
		},
	}

	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, collectionID, meta.CurrentTarget).Return(channels)
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, collectionID, meta.CurrentTarget).Return(currentVersion)

	// Reset metric before test
	metrics.QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds.Reset()

	// Case 1: Only replica1 ready, replica2 not ready -> metric should NOT update
	distMgr.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channelName,
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:            1,
			CollectionID:  collectionID,
			Channel:       channelName,
			TargetVersion: currentVersion,
			Status:        &querypb.LeaderViewStatus{Serviceable: true},
		},
	})
	// Node 2 has no delegator for channel-1

	observer.updateAllReplicasCheckpointMetric(ctx, collectionID)

	// Metric should not have been set (no gauge value or still 0)
	gauge, err := metrics.QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), channelName,
	)
	assert.NoError(t, err)
	dto := &io_prometheus_client.Metric{}
	gauge.Write(dto)
	assert.Equal(t, float64(0), dto.GetGauge().GetValue(),
		"metric should not update when not all replicas are ready")

	// Case 2: Both replicas ready -> metric should update
	distMgr.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channelName,
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:            2,
			CollectionID:  collectionID,
			Channel:       channelName,
			TargetVersion: currentVersion,
			Status:        &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	observer.updateAllReplicasCheckpointMetric(ctx, collectionID)

	gauge, err = metrics.QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), channelName,
	)
	assert.NoError(t, err)
	dto = &io_prometheus_client.Metric{}
	gauge.Write(dto)
	assert.Greater(t, dto.GetGauge().GetValue(), float64(0),
		"metric should update when all replicas are ready")
}

func TestTargetObserver(t *testing.T) {
	suite.Run(t, new(TargetObserverSuite))
	suite.Run(t, new(TargetObserverCheckSuite))
}

// splitHandoffFixture drives a real TargetManager and TargetObserver through a
// 1->2 shard split: the broker plays both rootcoord (shard states) and datacoord
// (recovery info), and the cluster records which channels get synced.
type splitHandoffFixture struct {
	t            *testing.T
	ctx          context.Context
	collectionID int64

	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	observer  *TargetObserver

	describe    *milvuspb.DescribeCollectionResponse
	describeErr error // when set, DescribeCollection fails instead of returning describe
	channels    []*datapb.VchannelInfo
	segments    []*datapb.SegmentInfo
	synced      []string
}

func newSplitHandoffFixture(t *testing.T) *splitHandoffFixture {
	paramtable.Init()
	f := &splitHandoffFixture{t: t, ctx: context.Background(), collectionID: 1000}

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	f.distMgr = meta.NewDistributionManager(nodeMgr)

	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	replicaMgr := meta.NewReplicaManager(nil, catalog)
	assert.NoError(t, replicaMgr.Put(f.ctx, meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: f.collectionID, ResourceGroup: meta.DefaultResourceGroupName, Nodes: []int64{1},
	})))
	f.meta = &meta.Meta{CollectionManager: meta.NewCollectionManager(catalog), ReplicaManager: replicaMgr}
	assert.NoError(t, f.meta.PutCollection(f.ctx, utils.CreateTestCollection(f.collectionID, 1)))

	broker := meta.NewMockBroker(t)
	describe := func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
		if f.describeErr != nil {
			return nil, f.describeErr
		}
		return f.describe, nil
	}
	broker.EXPECT().DescribeCollection(mock.Anything, f.collectionID).RunAndReturn(describe).Maybe()
	// the shard split state cache reads through the internal describe.
	broker.EXPECT().DescribeCollectionInternal(mock.Anything, f.collectionID).RunAndReturn(describe).Maybe()
	broker.EXPECT().GetRecoveryInfoV2(mock.Anything, f.collectionID).RunAndReturn(
		func(context.Context, int64, ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
			return f.channels, f.segments, nil
		}).Maybe()
	broker.EXPECT().ListIndexes(mock.Anything, f.collectionID).Return(nil, nil).Maybe()

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().SyncDistribution(mock.Anything, int64(1), mock.Anything).RunAndReturn(
		func(_ context.Context, _ int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
			f.synced = append(f.synced, req.GetChannel())
			return merr.Success(), nil
		}).Maybe()

	// TTL 0: every cached state query sees the states the step just set.
	splitState := meta.NewShardSplitStateCache(broker, 0)
	f.targetMgr = meta.NewTargetManagerWithSplitState(broker, f.meta, splitState)
	f.observer = NewTargetObserverWithSplitState(f.meta, f.targetMgr, f.distMgr, broker, cluster, nodeMgr, splitState)
	return f
}

// setShards sets the collection's vchannels and their shard states as rootcoord
// reports them.
func (f *splitHandoffFixture) setShards(channels []string, states ...schemapb.ShardState) {
	infos := lo.Map(states, func(s schemapb.ShardState, _ int) *schemapb.CollectionShardInfo {
		return &schemapb.CollectionShardInfo{State: s}
	})
	f.describe = &milvuspb.DescribeCollectionResponse{
		Schema:              &schemapb.CollectionSchema{Name: "split"},
		VirtualChannelNames: channels,
		ShardInfos:          infos,
	}
}

// setRecovery sets what datacoord's recovery view returns: the channels, and each
// flushed segment under the channel it is attributed to.
func (f *splitHandoffFixture) setRecovery(channels []string, segmentChannels map[int64]string) {
	f.channels = lo.Map(channels, func(name string, _ int) *datapb.VchannelInfo {
		return &datapb.VchannelInfo{CollectionID: f.collectionID, ChannelName: name}
	})
	f.segments = lo.MapToSlice(segmentChannels, func(id int64, channel string) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID: id, CollectionID: f.collectionID, PartitionID: common.AllPartitionsID, InsertChannel: channel, NumOfRows: 10,
		}
	})
}

// setDist sets node 1's delegators, each with the sealed segments it has loaded.
func (f *splitHandoffFixture) setDist(delegators map[string][]int64) {
	channels := make([]*meta.DmChannel, 0, len(delegators))
	segments := make([]*meta.Segment, 0)
	for channel, ids := range delegators {
		loaded := make(map[int64]*querypb.SegmentDist, len(ids))
		for _, id := range ids {
			loaded[id] = &querypb.SegmentDist{NodeID: 1}
			segments = append(segments, &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: f.collectionID, InsertChannel: channel},
				Node:        1,
			})
		}
		channels = append(channels, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: f.collectionID, ChannelName: channel},
			Node:         1,
			View: &meta.LeaderView{
				ID: 1, CollectionID: f.collectionID, Channel: channel, Segments: loaded,
				Status: &querypb.LeaderViewStatus{Serviceable: true},
			},
		})
	}
	f.distMgr.ChannelDistManager.Update(1, channels...)
	f.distMgr.SegmentDistManager.Update(1, segments...)
}

func (f *splitHandoffFixture) currentChannels() []string {
	return lo.Keys(f.targetMgr.GetDmChannelsByCollection(f.ctx, f.collectionID, meta.CurrentTarget))
}

// TestSplitWindowTargetsHeldBackUntilWindowEnds pins QC2 and QC3: a split target
// is never synced from, nor promoted through, a next target pulled inside the
// split window -- even after adoption put it in dist and made it trivially
// data-ready -- while the source keeps being synced; once the window ends the
// next target is refreshed on the next check, and that post-delisting snapshot
// syncs the targets and flips the current target.
func TestSplitWindowTargetsHeldBackUntilWindowEnds(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segS, segO1, segO2 = int64(1), int64(11), int64(12)

	// before the split: one shard serves segment S.
	f.setShards([]string{"src"}, schemapb.ShardState_ShardNormal)
	f.setRecovery([]string{"src"}, map[int64]string{segS: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	f.setDist(map[string][]int64{"src": {segS}})
	assert.True(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	f.observer.updateCurrentTarget(ctx, collectionID)
	assert.Equal(t, []string{"src"}, f.currentChannels())
	currentVersion := f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)

	// inside the window: the rewrite outputs are attributed to the listed source.
	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"src", "t1", "t2"}, map[int64]string{segO1: "src", segO2: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())
	// still inside the window, a fresh next target is not due.
	assert.False(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))

	// adoption: the targets turn Normal and the source is delisted, but the next
	// target is still the window snapshot. The adopted children are in dist and,
	// owning no sealed segment in that snapshot, data-ready against it.
	f.setShards([]string{"t1", "t2"}, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal)
	f.setDist(map[string][]int64{"src": {segO1, segO2}, "t1": {}, "t2": {}})
	f.synced = nil
	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	// the source is still synced (it serves the window); the targets are not.
	assert.Equal(t, []string{"src"}, f.synced)
	assert.Equal(t, currentVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))
	assert.Equal(t, []string{"src"}, f.currentChannels())

	// the window is over, so the next check refreshes the next target at once.
	assert.True(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))
	f.setRecovery([]string{"t1", "t2"}, map[int64]string{segO1: "t1", segO2: "t2"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.Empty(t, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget))
	assert.False(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))

	// the post-delisting snapshot syncs both targets, then the flip.
	f.setDist(map[string][]int64{"src": {segO1, segO2}, "t1": {segO1}, "t2": {segO2}})
	f.synced = nil
	assert.True(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.synced)
	f.observer.updateCurrentTarget(ctx, collectionID)
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.currentChannels())
}

// serveBeforeSplit promotes a single Normal shard "src" serving segment segS to
// the current target, and returns that current target's version.
func (f *splitHandoffFixture) serveBeforeSplit(segS int64) int64 {
	f.setShards([]string{"src"}, schemapb.ShardState_ShardNormal)
	f.setRecovery([]string{"src"}, map[int64]string{segS: "src"})
	assert.NoError(f.t, f.observer.updateNextTarget(f.ctx, f.collectionID))
	f.setDist(map[string][]int64{"src": {segS}})
	assert.True(f.t, f.observer.shouldUpdateCurrentTarget(f.ctx, f.collectionID))
	f.observer.updateCurrentTarget(f.ctx, f.collectionID)
	assert.Equal(f.t, []string{"src"}, f.currentChannels())
	return f.targetMgr.GetCollectionTargetVersion(f.ctx, f.collectionID, meta.CurrentTarget)
}

// refreshAfterDelistingAndFlip pulls the post-delisting snapshot (O1 under t1,
// O2 under t2) and checks it syncs both targets and flips the current target.
func (f *splitHandoffFixture) refreshAfterDelistingAndFlip(segO1, segO2 int64) {
	f.setRecovery([]string{"t1", "t2"}, map[int64]string{segO1: "t1", segO2: "t2"})
	assert.NoError(f.t, f.observer.updateNextTarget(f.ctx, f.collectionID))
	assert.Empty(f.t, f.targetMgr.GetSplitWindowTargets(f.ctx, f.collectionID, meta.NextTarget))
	assert.False(f.t, f.observer.shouldUpdateNextTarget(f.ctx, f.collectionID))

	f.setDist(map[string][]int64{"src": {segO1, segO2}, "t1": {segO1}, "t2": {segO2}})
	f.synced = nil
	assert.True(f.t, f.observer.shouldUpdateCurrentTarget(f.ctx, f.collectionID))
	assert.ElementsMatch(f.t, []string{"t1", "t2"}, f.synced)
	f.observer.updateCurrentTarget(f.ctx, f.collectionID)
	assert.ElementsMatch(f.t, []string{"t1", "t2"}, f.currentChannels())
}

// TestSplitWindowFenceRaceSnapshotHeldBack pins the mark's complement rule end
// to end: the state read commits before the fence (only "src", Normal) and the
// pull after it (src, t1, t2, outputs under src). The targets are marked because
// the read did not list them, so after adoption they are neither synced nor
// promoted through that snapshot: no {src, t1, t2} current target.
func TestSplitWindowFenceRaceSnapshotHeldBack(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segS, segO1, segO2 = int64(1), int64(11), int64(12)
	currentVersion := f.serveBeforeSplit(segS)

	// the describe still returns the pre-fence meta; the pull sees the fence.
	f.setRecovery([]string{"src", "t1", "t2"}, map[int64]string{segO1: "src", segO2: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())

	// the fence has committed: inside the window no refresh is due.
	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	assert.False(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))

	// adoption, with the racy snapshot still the next target.
	f.setShards([]string{"t1", "t2"}, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal)
	f.setDist(map[string][]int64{"src": {segO1, segO2}, "t1": {}, "t2": {}})
	f.synced = nil
	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.Equal(t, []string{"src"}, f.synced)
	assert.Equal(t, currentVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))
	assert.Equal(t, []string{"src"}, f.currentChannels())

	assert.True(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))
	f.refreshAfterDelistingAndFlip(segO1, segO2)
}

// TestSplitWindowAdoptionRaceOverMarkLifted pins the other race: the state read
// commits before the adoption (targets Creating) and the pull after it (source
// delisted, outputs under their targets). The targets are over-marked, so the
// complete snapshot is held back until the window-end refresh re-pulls it
// unmarked; then the targets sync and the current target flips.
func TestSplitWindowAdoptionRaceOverMarkLifted(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segS, segO1, segO2 = int64(1), int64(11), int64(12)
	currentVersion := f.serveBeforeSplit(segS)

	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"t1", "t2"}, map[int64]string{segO1: "t1", segO2: "t2"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())

	// the adoption has committed; the over-marked snapshot is not promoted.
	f.setShards([]string{"t1", "t2"}, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal)
	f.setDist(map[string][]int64{"src": {segS}, "t1": {segO1}, "t2": {segO2}})
	f.synced = nil
	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.NotContains(t, f.synced, "t1")
	assert.NotContains(t, f.synced, "t2")
	assert.Equal(t, currentVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))

	// one window-end refresh lifts the over-mark.
	assert.True(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))
	f.refreshAfterDelistingAndFlip(segO1, segO2)
}

// TestSplitWindowFenceRaceWithDescribeDownNeverBusyLoops pins the liveness fix:
// a pull that raced the fence (marking t1, t2 from a pre-fence read) must not
// re-pull on every single check for as long as DescribeCollection keeps
// failing afterwards. The stale pre-fence entry is all ReadShardStates' cache
// fallback can produce while the coordinator is down, and it never lists
// t1/t2 as Creating (it does not list them at all), so the old rule declared
// the window over on every check. Once the coordinator recovers, the next
// fresh read still ends the window normally.
func TestSplitWindowFenceRaceWithDescribeDownNeverBusyLoops(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segS, segO1, segO2 = int64(1), int64(11), int64(12)
	currentVersion := f.serveBeforeSplit(segS)

	// the coordinator goes down right after caching the pre-fence read; the
	// pull still succeeds (it goes through datacoord, not this broker call),
	// and ReadShardStates falls back to the cached pre-fence entry to mark it.
	f.describeErr = errors.New("rootcoord down")
	f.setRecovery([]string{"src", "t1", "t2"}, map[int64]string{segO1: "src", segO2: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())

	// the coordinator stays down for many checks: every one of them must still
	// see the window as not over, because the only entry available predates
	// this pull. The old rule would flip to "over" on the very first check.
	for i := 0; i < 30; i++ {
		assert.False(t, f.observer.shouldUpdateNextTarget(ctx, collectionID),
			"check %d: describe is down, the window must not look over", i)
	}
	// nothing was promoted while the window was (wrongly) never confirmed over.
	assert.Equal(t, currentVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))
	assert.Equal(t, []string{"src"}, f.currentChannels())

	// the coordinator recovers and the adoption commits; the normal window-end
	// refresh must still fire on the next check.
	f.describeErr = nil
	f.setShards([]string{"t1", "t2"}, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal)
	f.setDist(map[string][]int64{"src": {segO1, segO2}, "t1": {}, "t2": {}})
	f.synced = nil
	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.Equal(t, []string{"src"}, f.synced)
	assert.Equal(t, currentVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))

	assert.True(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))
	f.refreshAfterDelistingAndFlip(segO1, segO2)
}

// TestCheck_NoCurrentTargetSkipsSplitWindowTargets pins QC2 on the other sync
// path: a collection with no current target gets its next target synced right
// after the pull (partial search), and a window target is left out there too.
func TestCheck_NoCurrentTargetSkipsSplitWindowTargets(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID

	f.setShards([]string{"src", "t1"}, schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"src", "t1"}, map[int64]string{11: "src"})
	f.setDist(map[string][]int64{"t1": {}})

	f.observer.check(ctx, collectionID)

	assert.Equal(t, []string{"t1"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())
	assert.Empty(t, f.synced)
	assert.Empty(t, f.currentChannels())
}

// TestNewTargetObserverWithSplitState_RefusesNilCache pins that the split-aware
// constructor never builds an observer silently missing the window-end refresh.
func TestNewTargetObserverWithSplitState_RefusesNilCache(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		NewTargetObserverWithSplitState(nil, nil, nil, nil, nil, nil, nil)
	}()
	err, ok := recovered.(error)
	assert.True(t, ok, "expected a panic with an error, got %v", recovered)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

// TestIsSplitWindowOver covers the window-end refresh's guards.
func TestIsSplitWindowOver(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	collectionID := int64(1000)

	t.Run("without a split state cache there is no window to end", func(t *testing.T) {
		targetMgr := meta.NewMockTargetManager(t)
		ob := NewTargetObserver(nil, targetMgr, nil, nil, nil, nil)
		assert.False(t, ob.isSplitWindowOver(ctx, collectionID))
	})

	t.Run("a next target pulled outside a window never triggers it", func(t *testing.T) {
		targetMgr := meta.NewMockTargetManager(t)
		targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, collectionID, meta.NextTarget).Return(nil).Once()
		// the broker is not consulted: no DescribeCollection expectation.
		broker := meta.NewMockBroker(t)
		ob := NewTargetObserverWithSplitState(nil, targetMgr, nil, broker, nil, nil, meta.NewShardSplitStateCache(broker, 0))
		assert.False(t, ob.isSplitWindowOver(ctx, collectionID))
	})
}

// TestSplitWindowCurrentTargetServesTheSource pins Task 17: a collection loaded
// for the first time INSIDE a split window reaches a usable current target --
// the source alone -- instead of waiting out the window. The hold-back is
// unchanged: the window targets are still never synced, and never appear in the
// current target GetShardLeaders enumerates.
func TestSplitWindowCurrentTargetServesTheSource(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segO1, segO2 = int64(11), int64(12)

	// the fence lands before the load: the first target this collection ever has
	// is a window snapshot, with the outputs attributed to the listed source.
	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"src", "t1", "t2"}, map[int64]string{segO1: "src", segO2: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())

	// only the source has a delegator: the unadopted children are hidden from
	// GetDataDistribution for the whole window.
	f.setDist(map[string][]int64{"src": {segO1, segO2}})
	f.synced = nil
	assert.True(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID),
		"the source is loaded and synced, so the collection is serviceable")
	assert.Equal(t, []string{"src"}, f.synced, "a window target is never synced")

	f.observer.updateCurrentTarget(ctx, collectionID)
	assert.Equal(t, []string{"src"}, f.currentChannels(), "a window target is never routed to")
	assert.True(t, f.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID))
	windowVersion := f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)
	// both outputs are served, through the source they are attributed to.
	assert.ElementsMatch(t, []int64{segO1, segO2},
		lo.Keys(f.targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.CurrentTarget)))

	// the next target is kept, so the window targets stay in a target: the
	// channel checker releases exactly the channels that are in neither.
	assert.True(t, f.targetMgr.IsNextTargetExist(ctx, collectionID))
	assert.ElementsMatch(t, []string{"src", "t1", "t2"},
		lo.Keys(f.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)))
	assert.False(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))

	// a second round re-promotes nothing and leaves everything where it is.
	f.synced = nil
	assert.True(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	f.observer.updateCurrentTarget(ctx, collectionID)
	assert.Equal(t, windowVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))
	assert.Equal(t, []string{"src"}, f.currentChannels())

	// adoption: the targets turn Normal and are in dist, empty and data-ready
	// against the window snapshot. They must still not be synced or promoted,
	// and the current target must stay on the source.
	f.setShards([]string{"t1", "t2"}, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal)
	f.setDist(map[string][]int64{"src": {segO1, segO2}, "t1": {}, "t2": {}})
	f.synced = nil
	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.Equal(t, []string{"src"}, f.synced)
	assert.Equal(t, []string{"src"}, f.currentChannels())
	assert.Equal(t, windowVersion, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget))

	// the window is over: the ordinary refresh and flip take it from here.
	assert.True(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))
	f.refreshAfterDelistingAndFlip(segO1, segO2)
}

// TestSplitWindowCurrentTargetAdvancesAnAlreadyLoadedCollection pins the other
// entry into the window: the collection was already serving when the fence
// landed, so a current target exists. The window snapshot still advances it --
// same channel, now carrying the outputs attributed to the source.
func TestSplitWindowCurrentTargetAdvancesAnAlreadyLoadedCollection(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segS, segO1, segO2 = int64(1), int64(11), int64(12)

	preFenceVersion := f.serveBeforeSplit(segS)

	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"src", "t1", "t2"}, map[int64]string{segO1: "src", segO2: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	f.setDist(map[string][]int64{"src": {segO1, segO2}})

	f.synced = nil
	assert.True(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.Equal(t, []string{"src"}, f.synced)
	f.observer.updateCurrentTarget(ctx, collectionID)
	assert.Equal(t, []string{"src"}, f.currentChannels())
	assert.Greater(t, f.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget), preFenceVersion)
	assert.ElementsMatch(t, []int64{segO1, segO2},
		lo.Keys(f.targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.CurrentTarget)))

	f.setShards([]string{"t1", "t2"}, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal)
	assert.True(t, f.observer.shouldUpdateNextTarget(ctx, collectionID))
	f.refreshAfterDelistingAndFlip(segO1, segO2)
}

// TestSplitWindowCurrentTargetRefusesAnEmptyServingSet pins the defense in
// depth in shouldUpdateCurrentTarget: the exclusion rule guarantees a fenced
// source stays, so an exclusion covering every channel cannot happen -- but
// lo.Every over an empty list is vacuously true, so if it ever did, the current
// target would advance to no channel at all. It must refuse instead.
func TestSplitWindowCurrentTargetRefusesAnEmptyServingSet(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID

	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"src", "t1", "t2"}, map[int64]string{11: "src"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	f.setDist(map[string][]int64{"src": {11}})

	everything := typeutil.NewSet("src", "t1", "t2")
	mockExclusions := mockey.Mock((*meta.TargetManager).GetSplitWindowExclusions).
		Return(everything, true).Build()
	defer mockExclusions.UnPatch()

	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.Empty(t, f.currentChannels())
}

// TestSplitWindowCurrentTargetRefusedWhenTheSourceIsGone pins the guard on a
// state read that is behind the pull: the states still call "src" a fenced
// source, but the pull no longer lists it, so the targets ARE the servers now.
// Excluding them would advance the current target to nothing and report a
// collection ready that serves no row of the split key range.
func TestSplitWindowCurrentTargetRefusedWhenTheSourceIsGone(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	const segO1, segO2 = int64(11), int64(12)

	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.setRecovery([]string{"t1", "t2"}, map[int64]string{segO1: "t1", segO2: "t2"})
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())

	f.setDist(map[string][]int64{"t1": {segO1}, "t2": {segO2}})
	f.synced = nil
	assert.False(t, f.observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.Empty(t, f.synced, "the marked targets are still held back from sync")
	assert.Empty(t, f.currentChannels())
}

// M1: GetShardLeaders enumerates the current target, and proxies cache what it
// answered. When the current target changes its channel set -- a shard split's
// flip drops the retired source and adds its targets -- the proxies' cached
// leaders must be dropped at once, not when the source's release reaches
// them. A flip that keeps the channel set invalidates nothing.
func TestCurrentTargetChannelSetChangeInvalidatesShardLeaders(t *testing.T) {
	ctx := context.Background()
	channels := func(names ...string) map[string]*meta.DmChannel {
		out := make(map[string]*meta.DmChannel, len(names))
		for _, name := range names {
			out[name] = &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: name}}
		}
		return out
	}
	run := func(t *testing.T, before, after map[string]*meta.DmChannel, promoted bool) []int64 {
		targetMgr := meta.NewMockTargetManager(t)
		targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.CurrentTarget).Return(before).Once()
		targetMgr.EXPECT().UpdateCollectionCurrentTarget(mock.Anything, int64(1)).Return(promoted).Once()
		targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.CurrentTarget).Return(after).Maybe()
		ob := NewTargetObserver(nil, targetMgr, nil, nil, nil, nil)
		var invalidated []int64
		ob.SetShardLeaderInvalidator(func(collectionIDs ...int64) {
			invalidated = append(invalidated, collectionIDs...)
		})
		ob.updateCurrentTarget(ctx, 1)
		return invalidated
	}

	assert.Equal(t, []int64{1}, run(t, channels("v0"), channels("v1", "v2"), true), "the flip past a split source")
	assert.Equal(t, []int64{1}, run(t, nil, channels("v0"), true), "the first current target")
	assert.Empty(t, run(t, channels("v0", "v1"), channels("v1", "v0"), true), "same channel set")
	assert.Empty(t, run(t, channels("v0"), channels("v1"), false), "nothing promoted")
}

// AV-L6-H1: when the observer sees a next target's split window marks change,
// the shard states the checkers freeze by are refreshed on their next read
// rather than served from a cached read the marks show is out of date. Here
// the pull's own fresh state read failed, so the pull marked its window from a
// pre-fence read the cache still holds within its TTL.
func TestSplitWindowMarkChangeRefreshesTheCachedShardStates(t *testing.T) {
	f := newSplitHandoffFixture(t)
	ctx, collectionID := f.ctx, f.collectionID
	// a long TTL: only an invalidation makes the next read go to the coordinator.
	splitState := meta.NewShardSplitStateCache(f.observer.broker, time.Hour)
	f.targetMgr = meta.NewTargetManagerWithSplitState(f.observer.broker, f.meta, splitState)
	f.observer = NewTargetObserverWithSplitState(f.meta, f.targetMgr, f.distMgr, f.observer.broker, f.observer.cluster, f.observer.nodeMgr, splitState)

	f.setShards([]string{"src"}, schemapb.ShardState_ShardNormal)
	f.setRecovery([]string{"src"}, nil)
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	states, ok := splitState.ChannelStates(ctx, collectionID)
	assert.True(t, ok)
	assert.False(t, states.Splitting())

	// the fence: the pull's state read fails and falls back to the pre-fence read.
	f.setShards([]string{"src", "t1", "t2"},
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	f.describeErr = errors.New("rootcoord busy")
	f.setRecovery([]string{"src", "t1", "t2"}, nil)
	assert.NoError(t, f.observer.updateNextTarget(ctx, collectionID))
	assert.ElementsMatch(t, []string{"t1", "t2"}, f.targetMgr.GetSplitWindowTargets(ctx, collectionID, meta.NextTarget).Collect())

	f.describeErr = nil
	states, ok = splitState.ChannelStates(ctx, collectionID)
	assert.True(t, ok)
	assert.True(t, states.Splitting(), "the checkers must see the fence the marks gave away, not the cached pre-fence read")
}
