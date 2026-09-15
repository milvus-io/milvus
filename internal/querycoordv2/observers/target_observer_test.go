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
	querycoordtask "github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

type mockeyTargetManager struct{ meta.TargetManagerInterface }

type mockeyBroker struct{ meta.Broker }

func TestInitSerializesTargetRefresh(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	collectionID := int64(1000)
	collectionMgr := meta.NewCollectionManager(nil)
	assert.NoError(t, collectionMgr.PutCollectionWithoutSave(ctx, utils.CreateTestCollection(collectionID, 1)))
	metaInstance := &meta.Meta{
		CollectionManager: collectionMgr,
		ReplicaManager:    meta.NewReplicaManager(nil, nil),
	}

	updateEntered := make(chan struct{})
	allowUpdateReturn := make(chan struct{})
	nextTargetExists := false
	nextTargetVersion := int64(0)
	targetMgr := &mockeyTargetManager{}
	mockNextTargetExists := mockey.Mock((*mockeyTargetManager).IsNextTargetExist).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64) bool {
			return nextTargetExists
		}).
		Build()
	defer mockNextTargetExists.UnPatch()
	mockUpdateNextTarget := mockey.Mock((*mockeyTargetManager).UpdateCollectionNextTarget).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64) error {
			close(updateEntered)
			<-allowUpdateReturn
			nextTargetExists = true
			nextTargetVersion = 1
			return nil
		}).
		Build()
	defer mockUpdateNextTarget.UnPatch()
	mockNextTargetVersion := mockey.Mock((*mockeyTargetManager).GetCollectionTargetVersion).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64, _ meta.TargetScope) int64 {
			return nextTargetVersion
		}).
		Build()
	defer mockNextTargetVersion.UnPatch()
	mockNextTargetSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByCollection).
		Return(map[int64]*datapb.SegmentInfo{}).
		Build()
	defer mockNextTargetSegments.UnPatch()
	mockNextTargetChannels := mockey.Mock((*mockeyTargetManager).GetDmChannelsByCollection).
		Return(map[string]*meta.DmChannel{}).
		Build()
	defer mockNextTargetChannels.UnPatch()

	nodeMgr := session.NewNodeManager()
	observer := NewTargetObserver(
		metaInstance,
		targetMgr,
		meta.NewDistributionManager(nodeMgr),
		nil,
		nil,
		nodeMgr,
	)

	initDone := make(chan struct{})
	go func() {
		defer close(initDone)
		observer.init(ctx, collectionID)
	}()

	select {
	case <-updateEntered:
	case <-time.After(5 * time.Second):
		close(allowUpdateReturn)
		t.Fatal("target refresh did not start")
	}
	lockAcquiredDuringRefresh := observer.keylocks.TryLock(collectionID)
	if lockAcquiredDuringRefresh {
		observer.keylocks.Unlock(collectionID)
	}
	close(allowUpdateReturn)

	select {
	case <-initDone:
	case <-time.After(5 * time.Second):
		t.Fatal("target observer init did not finish")
	}
	assert.False(t, lockAcquiredDuringRefresh)
}

func setNextTargetProgressLastUpdated(
	t *testing.T,
	observer *TargetObserver,
	collectionID int64,
	lastUpdated time.Time,
) {
	t.Helper()
	progress, ok := observer.nextTargetProgresses.Get(collectionID)
	if !assert.True(t, ok) {
		return
	}
	progress.lastUpdated = lastUpdated
	observer.nextTargetProgresses.Insert(collectionID, progress)
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
// non-ready delegators were included in the collection-wide sync list.
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
	// This is the key assertion: if the unfiltered delegator list were used,
	// node 2 would also receive a SyncDistribution call
	assert.Equal(t, 1, len(syncedNodes), "Expected only 1 SyncDistribution call for the ready delegator")
	assert.Contains(t, syncedNodes, int64(1), "Expected node 1 (ready delegator) to receive SyncDistribution")
	assert.NotContains(t, syncedNodes, int64(2), "Node 2 (not ready delegator) should NOT receive SyncDistribution")
}

func TestShouldUpdateNextTarget_TracksSegmentReplicaProgress(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.NextTargetSurviveTime.Key, "1")
	defer paramtable.Get().Reset(Params.QueryCoordCfg.NextTargetSurviveTime.Key)

	ctx := context.Background()
	collectionID := int64(1000)
	nextVersion := int64(100)
	segment1 := &datapb.SegmentInfo{ID: 1, CollectionID: collectionID}
	segment2 := &datapb.SegmentInfo{ID: 2, CollectionID: collectionID}

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	replica1 := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: collectionID, Nodes: []int64{1}})
	replica2 := meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: collectionID, Nodes: []int64{2}})
	replicaMgr := meta.NewReplicaManager(nil, nil)
	mockReplicas := mockey.Mock((*meta.ReplicaManager).GetByCollection).Return([]*meta.Replica{replica1, replica2}).Build()
	defer mockReplicas.UnPatch()
	metaInstance := &meta.Meta{ReplicaManager: replicaMgr}

	targetMgr := &mockeyTargetManager{}
	nextTargetSegments := map[int64]*datapb.SegmentInfo{
		segment1.GetID(): segment1,
		segment2.GetID(): segment2,
	}
	mockNextTargetExists := mockey.Mock((*mockeyTargetManager).IsNextTargetExist).Return(true).Build()
	defer mockNextTargetExists.UnPatch()
	mockNextTargetVersion := mockey.Mock((*mockeyTargetManager).GetCollectionTargetVersion).Return(nextVersion).Build()
	defer mockNextTargetVersion.UnPatch()
	mockNextTargetSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByCollection).Return(nextTargetSegments).Build()
	defer mockNextTargetSegments.UnPatch()
	mockNextTargetChannels := mockey.Mock((*mockeyTargetManager).GetDmChannelsByCollection).Return(map[string]*meta.DmChannel{}).Build()
	defer mockNextTargetChannels.UnPatch()

	distMgr := meta.NewDistributionManager(nodeMgr)
	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, nil, nil, nodeMgr)

	distMgr.SegmentDistManager.Update(1, meta.SegmentFromInfo(segment1), meta.SegmentFromInfo(segment2))
	distMgr.SegmentDistManager.Update(2, meta.SegmentFromInfo(segment1))
	observer.recordNextTargetProgress(collectionID, observer.sampleNextTargetProgress(ctx, collectionID, nextVersion))
	setNextTargetProgressLastUpdated(t, observer, collectionID, time.Now().Add(-time.Hour))

	assert.True(t, observer.shouldUpdateNextTarget(ctx, collectionID))

	distMgr.SegmentDistManager.Update(2, meta.SegmentFromInfo(segment1), meta.SegmentFromInfo(segment2))
	setNextTargetProgressLastUpdated(t, observer, collectionID, time.Now().Add(-time.Hour))

	assert.False(t, observer.shouldUpdateNextTarget(ctx, collectionID))
	progress, ok := observer.nextTargetProgresses.Get(collectionID)
	assert.True(t, ok)
	assert.Equal(t, nextVersion, progress.targetVersion)
	assert.Equal(t, 4, progress.readySegmentReplicas)
	assert.Equal(t, 0, progress.readyReplicaChannels)
	assert.WithinDuration(t, time.Now(), progress.lastUpdated, time.Second)
}

func TestShouldUpdateNextTarget_TracksDataVersionProgress(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.NextTargetSurviveTime.Key, "1")
	defer paramtable.Get().Reset(Params.QueryCoordCfg.NextTargetSurviveTime.Key)

	ctx := context.Background()
	collectionID := int64(1000)
	nextVersion := int64(100)
	targetSegment := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   collectionID,
		DataVersion:    2,
		StorageVersion: storage.StorageV2,
	}

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	replica1 := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: collectionID, Nodes: []int64{1, 2}})
	replicaMgr := meta.NewReplicaManager(nil, nil)
	mockReplicas := mockey.Mock((*meta.ReplicaManager).GetByCollection).Return([]*meta.Replica{replica1}).Build()
	defer mockReplicas.UnPatch()
	metaInstance := &meta.Meta{ReplicaManager: replicaMgr}

	targetMgr := &mockeyTargetManager{}
	mockNextTargetExists := mockey.Mock((*mockeyTargetManager).IsNextTargetExist).Return(true).Build()
	defer mockNextTargetExists.UnPatch()
	mockNextTargetVersion := mockey.Mock((*mockeyTargetManager).GetCollectionTargetVersion).Return(nextVersion).Build()
	defer mockNextTargetVersion.UnPatch()
	mockNextTargetSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByCollection).
		Return(map[int64]*datapb.SegmentInfo{targetSegment.GetID(): targetSegment}).Build()
	defer mockNextTargetSegments.UnPatch()
	mockNextTargetChannels := mockey.Mock((*mockeyTargetManager).GetDmChannelsByCollection).Return(map[string]*meta.DmChannel{}).Build()
	defer mockNextTargetChannels.UnPatch()

	distMgr := meta.NewDistributionManager(nodeMgr)
	oldDataVersion := int32(1)
	distMgr.SegmentDistManager.Update(2, &meta.Segment{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             targetSegment.GetID(),
			CollectionID:   collectionID,
			StorageVersion: storage.StorageV2,
		},
		DataVersion: &oldDataVersion,
	})
	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, nil, nil, nodeMgr)
	observer.recordNextTargetProgress(collectionID, observer.sampleNextTargetProgress(ctx, collectionID, nextVersion))
	setNextTargetProgressLastUpdated(t, observer, collectionID, time.Now().Add(-time.Hour))

	assert.True(t, observer.shouldUpdateNextTarget(ctx, collectionID))

	newDataVersion := int32(2)
	distMgr.SegmentDistManager.Update(1, &meta.Segment{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             targetSegment.GetID(),
			CollectionID:   collectionID,
			StorageVersion: storage.StorageV2,
		},
		DataVersion: &newDataVersion,
	})
	setNextTargetProgressLastUpdated(t, observer, collectionID, time.Now().Add(-time.Hour))

	assert.False(t, observer.shouldUpdateNextTarget(ctx, collectionID))
	progress, ok := observer.nextTargetProgresses.Get(collectionID)
	assert.True(t, ok)
	assert.Equal(t, 1, progress.readySegmentReplicas)
}

func TestShouldUpdateNextTarget_TracksReplicaChannelProgress(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.NextTargetSurviveTime.Key, "1")
	defer paramtable.Get().Reset(Params.QueryCoordCfg.NextTargetSurviveTime.Key)

	ctx := context.Background()
	collectionID := int64(1000)
	nextVersion := int64(100)
	channelName := "channel-1"

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	replica1 := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: collectionID, Nodes: []int64{1}})
	replica2 := meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: collectionID, Nodes: []int64{2}})
	replicaMgr := meta.NewReplicaManager(nil, nil)
	mockReplicas := mockey.Mock((*meta.ReplicaManager).GetByCollection).Return([]*meta.Replica{replica1, replica2}).Build()
	defer mockReplicas.UnPatch()
	metaInstance := &meta.Meta{ReplicaManager: replicaMgr}

	targetMgr := &mockeyTargetManager{}
	mockNextTargetExists := mockey.Mock((*mockeyTargetManager).IsNextTargetExist).Return(true).Build()
	defer mockNextTargetExists.UnPatch()
	mockNextTargetVersion := mockey.Mock((*mockeyTargetManager).GetCollectionTargetVersion).Return(nextVersion).Build()
	defer mockNextTargetVersion.UnPatch()
	mockNextTargetSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByCollection).Return(map[int64]*datapb.SegmentInfo{}).Build()
	defer mockNextTargetSegments.UnPatch()
	mockNextTargetChannels := mockey.Mock((*mockeyTargetManager).GetDmChannelsByCollection).
		Return(map[string]*meta.DmChannel{channelName: {VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName}}}).Build()
	defer mockNextTargetChannels.UnPatch()
	mockChannelSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByChannel).Return(map[int64]*datapb.SegmentInfo{}).Build()
	defer mockChannelSegments.UnPatch()

	distMgr := meta.NewDistributionManager(nodeMgr)
	distMgr.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName},
		Node:         1,
		View: &meta.LeaderView{
			ID:            1,
			CollectionID:  collectionID,
			Channel:       channelName,
			TargetVersion: 0,
			Status:        &querypb.LeaderViewStatus{Serviceable: false},
		},
	})
	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, nil, nil, nodeMgr)
	observer.recordNextTargetProgress(collectionID, nextTargetProgress{
		targetVersion:        nextVersion,
		readySegmentReplicas: 1,
	})

	distMgr.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName},
		Node:         2,
		View: &meta.LeaderView{
			ID:            2,
			CollectionID:  collectionID,
			Channel:       channelName,
			TargetVersion: nextVersion,
			Status: &querypb.LeaderViewStatus{
				Serviceable:             true,
				CatchingUpStreamingData: true,
			},
		},
	})
	setNextTargetProgressLastUpdated(t, observer, collectionID, time.Now().Add(-time.Hour))
	assert.False(t, observer.shouldUpdateNextTarget(ctx, collectionID))
	progress, ok := observer.nextTargetProgresses.Get(collectionID)
	assert.True(t, ok)
	assert.Equal(t, 1, progress.readySegmentReplicas)
	assert.Equal(t, 2, progress.readyReplicaChannels)
}

func TestShouldUpdateNextTarget_ResetsProgressForNewVersion(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	collectionID := int64(1000)
	segment := &datapb.SegmentInfo{ID: 1, CollectionID: collectionID}

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: collectionID, Nodes: []int64{1}})
	replicaMgr := meta.NewReplicaManager(nil, nil)
	mockReplicas := mockey.Mock((*meta.ReplicaManager).GetByCollection).Return([]*meta.Replica{replica}).Build()
	defer mockReplicas.UnPatch()
	targetMgr := &mockeyTargetManager{}
	nextTargetSegments := map[int64]*datapb.SegmentInfo{
		segment.GetID(): segment,
	}
	mockNextTargetExists := mockey.Mock((*mockeyTargetManager).IsNextTargetExist).Return(true).Build()
	defer mockNextTargetExists.UnPatch()
	mockNextTargetVersion := mockey.Mock((*mockeyTargetManager).GetCollectionTargetVersion).Return(int64(101)).Build()
	defer mockNextTargetVersion.UnPatch()
	mockNextTargetSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByCollection).Return(nextTargetSegments).Build()
	defer mockNextTargetSegments.UnPatch()
	mockNextTargetChannels := mockey.Mock((*mockeyTargetManager).GetDmChannelsByCollection).Return(map[string]*meta.DmChannel{}).Build()
	defer mockNextTargetChannels.UnPatch()

	distMgr := meta.NewDistributionManager(nodeMgr)
	distMgr.SegmentDistManager.Update(1, meta.SegmentFromInfo(segment))
	metaInstance := &meta.Meta{ReplicaManager: replicaMgr}
	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, nil, nil, nodeMgr)
	observer.recordNextTargetProgress(collectionID, nextTargetProgress{
		targetVersion:        100,
		readySegmentReplicas: 5,
		readyReplicaChannels: 5,
	})

	assert.False(t, observer.shouldUpdateNextTarget(ctx, collectionID))
	progress, ok := observer.nextTargetProgresses.Get(collectionID)
	assert.True(t, ok)
	assert.Equal(t, int64(101), progress.targetVersion)
	assert.Equal(t, 1, progress.readySegmentReplicas)
	assert.Equal(t, 0, progress.readyReplicaChannels)
	assert.WithinDuration(t, time.Now(), progress.lastUpdated, time.Second)
}

func TestNextTargetStale_RefreshLifecycle(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	collectionID := int64(1000)
	segment := &datapb.SegmentInfo{ID: 1, CollectionID: collectionID}
	nextTargetVersion := int64(100)
	var updateNextTargetErr error
	advanceTargetVersion := false
	markDuringUpdate := false
	targetContainsSegment := true

	nodeMgr := session.NewNodeManager()
	targetMgr := &mockeyTargetManager{}
	var observer *TargetObserver
	nextTargetSegments := map[int64]*datapb.SegmentInfo{
		segment.GetID(): segment,
	}
	mockUpdateNextTarget := mockey.Mock((*mockeyTargetManager).UpdateCollectionNextTarget).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64) error {
			if updateNextTargetErr != nil {
				return updateNextTargetErr
			}
			if markDuringUpdate {
				observer.MarkNextTargetStale(collectionID, segment.GetID())
			}
			if advanceTargetVersion {
				nextTargetVersion++
			}
			return nil
		}).
		Build()
	defer mockUpdateNextTarget.UnPatch()
	mockNextTargetVersion := mockey.Mock((*mockeyTargetManager).GetCollectionTargetVersion).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64, _ meta.TargetScope) int64 {
			return nextTargetVersion
		}).
		Build()
	defer mockNextTargetVersion.UnPatch()
	mockNextTargetSegments := mockey.Mock((*mockeyTargetManager).GetSealedSegmentsByCollection).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64, _ meta.TargetScope) map[int64]*datapb.SegmentInfo {
			if targetContainsSegment {
				return nextTargetSegments
			}
			return map[int64]*datapb.SegmentInfo{}
		}).
		Build()
	defer mockNextTargetSegments.UnPatch()
	mockNextTargetChannels := mockey.Mock((*mockeyTargetManager).GetDmChannelsByCollection).Return(map[string]*meta.DmChannel{}).Build()
	defer mockNextTargetChannels.UnPatch()
	mockNextTargetExists := mockey.Mock((*mockeyTargetManager).IsNextTargetExist).Return(true).Build()
	defer mockNextTargetExists.UnPatch()
	mockNextTargetSegment := mockey.Mock((*mockeyTargetManager).GetSealedSegment).
		To(func(_ *mockeyTargetManager, _ context.Context, _ int64, segmentID int64, _ meta.TargetScope) *datapb.SegmentInfo {
			if targetContainsSegment && segmentID == segment.GetID() {
				return segment
			}
			return nil
		}).
		Build()
	defer mockNextTargetSegment.UnPatch()

	distMgr := meta.NewDistributionManager(nodeMgr)
	metaInstance := &meta.Meta{ReplicaManager: meta.NewReplicaManager(nil, nil)}
	observer = NewTargetObserver(metaInstance, targetMgr, distMgr, nil, nil, nodeMgr)
	observer.recordNextTargetProgress(collectionID, nextTargetProgress{
		targetVersion: nextTargetVersion,
	})

	observer.MarkNextTargetStale(collectionID, segment.GetID())
	assert.False(t, observer.shouldUpdateCurrentTarget(ctx, collectionID))
	assert.True(t, observer.shouldUpdateNextTarget(ctx, collectionID))

	updateNextTargetErr = assert.AnError
	assert.ErrorIs(t, observer.updateNextTarget(ctx, collectionID), assert.AnError)
	assert.True(t, observer.isNextTargetStale(collectionID))

	updateNextTargetErr = nil
	previousProgress, ok := observer.nextTargetProgresses.Get(collectionID)
	assert.True(t, ok)
	assert.NoError(t, observer.updateNextTarget(ctx, collectionID))
	assert.True(t, observer.isNextTargetStale(collectionID))
	currentProgress, ok := observer.nextTargetProgresses.Get(collectionID)
	assert.True(t, ok)
	assert.Equal(t, previousProgress, currentProgress)

	advanceTargetVersion = true
	markDuringUpdate = true
	assert.NoError(t, observer.updateNextTarget(ctx, collectionID))
	assert.True(t, observer.isNextTargetStale(collectionID))

	markDuringUpdate = false
	assert.NoError(t, observer.updateNextTarget(ctx, collectionID))
	assert.True(t, observer.isNextTargetStale(collectionID))

	targetContainsSegment = false
	assert.NoError(t, observer.updateNextTarget(ctx, collectionID))
	assert.False(t, observer.isNextTargetStale(collectionID))

	observer.MarkNextTargetStale(collectionID, segment.GetID())
	assert.False(t, observer.isNextTargetStale(collectionID))
}

func TestNextTargetStale_IdenticalRecoveryKeepsCause(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	collectionID := int64(1000)
	partitionID := int64(100)
	segmentID := int64(1)
	channelName := "channel-1"
	channel := &datapb.VchannelInfo{
		CollectionID: collectionID,
		ChannelName:  channelName,
		SeekPosition: &msgpb.MsgPosition{Timestamp: 1},
	}
	segment := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
	}

	collectionMgr := meta.NewCollectionManager(nil)
	assert.NoError(t, collectionMgr.PutCollectionWithoutSave(ctx, utils.CreateTestCollection(collectionID, 1)))
	assert.NoError(t, collectionMgr.PutPartitionWithoutSave(ctx, utils.CreateTestPartition(collectionID, partitionID)))
	metaInstance := &meta.Meta{
		CollectionManager: collectionMgr,
		ReplicaManager:    meta.NewReplicaManager(nil, nil),
	}

	broker := &mockeyBroker{}
	mockRecovery := mockey.Mock((*mockeyBroker).GetRecoveryInfoV2).
		To(func(_ *mockeyBroker, _ context.Context, actualCollectionID int64, _ ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
			assert.Equal(t, collectionID, actualCollectionID)
			return []*datapb.VchannelInfo{channel}, []*datapb.SegmentInfo{segment}, nil
		}).
		Build()
	defer mockRecovery.UnPatch()

	nodeMgr := session.NewNodeManager()
	targetMgr := meta.NewTargetManager(broker, metaInstance)
	observer := NewTargetObserver(
		metaInstance,
		targetMgr,
		meta.NewDistributionManager(nodeMgr),
		broker,
		nil,
		nodeMgr,
	)

	assert.NoError(t, targetMgr.UpdateCollectionNextTarget(ctx, collectionID))
	oldVersion := targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	observer.MarkNextTargetStale(collectionID, segmentID)

	// Real TargetManager uses a fresh local timestamp even when recovery returns
	// identical data. The failed segment is therefore the stale identity, not the
	// target version.
	time.Sleep(time.Millisecond)
	assert.NoError(t, observer.updateNextTarget(ctx, collectionID))
	newVersion := targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	assert.Greater(t, newVersion, oldVersion)
	assert.NotNil(t, targetMgr.GetSealedSegment(ctx, collectionID, segmentID, meta.NextTarget))
	assert.True(t, observer.isNextTargetStale(collectionID))
}

func TestNextTargetStale_ErrSegmentNotFoundRefreshesRealTarget(t *testing.T) {
	paramtable.Init()

	ctx := context.Background()
	collectionID := int64(1000)
	partitionID := int64(100)
	replicaID := int64(10)
	nodeID := int64(1)
	segmentID := int64(1)
	channelName := "channel-1"
	channel := &datapb.VchannelInfo{
		CollectionID: collectionID,
		ChannelName:  channelName,
		SeekPosition: &msgpb.MsgPosition{Timestamp: 1},
	}
	segment := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
	}
	recoverySegments := []*datapb.SegmentInfo{segment}

	collectionMgr := meta.NewCollectionManager(nil)
	assert.NoError(t, collectionMgr.PutCollectionWithoutSave(
		ctx,
		utils.CreateTestCollectionWithStatus(collectionID, 1, querypb.LoadStatus_Loaded),
	))
	assert.NoError(t, collectionMgr.PutPartitionWithoutSave(ctx, utils.CreateTestPartition(collectionID, partitionID)))
	replicaMgr := meta.NewReplicaManager(nil, nil)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  collectionID,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{nodeID},
	})
	mockGetReplica := mockey.Mock((*meta.ReplicaManager).Get).Return(replica).Build()
	defer mockGetReplica.UnPatch()
	mockGetReplicas := mockey.Mock((*meta.ReplicaManager).GetByCollection).Return([]*meta.Replica{replica}).Build()
	defer mockGetReplicas.UnPatch()
	metaInstance := &meta.Meta{
		CollectionManager: collectionMgr,
		ReplicaManager:    replicaMgr,
	}

	broker := &mockeyBroker{}
	recoveryCalls := 0
	mockRecovery := mockey.Mock((*mockeyBroker).GetRecoveryInfoV2).
		To(func(_ *mockeyBroker, _ context.Context, actualCollectionID int64, _ ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
			assert.Equal(t, collectionID, actualCollectionID)
			recoveryCalls++
			return []*datapb.VchannelInfo{channel}, recoverySegments, nil
		}).
		Build()
	defer mockRecovery.UnPatch()
	mockDescribeCollection := mockey.Mock((*mockeyBroker).DescribeCollection).
		Return(&milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{}}, nil).
		Build()
	defer mockDescribeCollection.UnPatch()
	mockGetSegmentInfo := mockey.Mock((*mockeyBroker).GetSegmentInfo).
		To(func(_ *mockeyBroker, _ context.Context, segmentIDs ...int64) ([]*datapb.SegmentInfo, error) {
			assert.Equal(t, []int64{segmentID}, segmentIDs)
			return nil, merr.WrapErrSegmentNotFound(segmentID)
		}).
		Build()
	defer mockGetSegmentInfo.UnPatch()

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID}))
	distMgr := meta.NewDistributionManager(nodeMgr)
	targetMgr := meta.NewTargetManager(broker, metaInstance)
	assert.NoError(t, targetMgr.UpdateCollectionNextTarget(ctx, collectionID))
	assert.True(t, targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID))
	time.Sleep(time.Millisecond)
	assert.NoError(t, targetMgr.UpdateCollectionNextTarget(ctx, collectionID))

	observer := NewTargetObserver(metaInstance, targetMgr, distMgr, broker, nil, nodeMgr)
	nextVersion := targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	observer.recordNextTargetProgress(collectionID, observer.sampleNextTargetProgress(ctx, collectionID, nextVersion))
	assert.False(t, observer.shouldUpdateNextTarget(ctx, collectionID))

	oldFailedLoadCache := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	defer func() {
		meta.GlobalFailedLoadCache = oldFailedLoadCache
	}()

	scheduler := querycoordtask.NewScheduler(ctx, metaInstance, distMgr, targetMgr, broker, nil, nodeMgr)
	scheduler.SetNextTargetStaleHandler(observer.MarkNextTargetStale)
	scheduler.AddExecutor(nodeID)
	defer scheduler.Stop()

	loadTask, err := querycoordtask.NewSegmentTask(
		ctx,
		10*time.Second,
		querycoordtask.WrapIDSource(0),
		collectionID,
		replica,
		commonpb.LoadPriority_LOW,
		querycoordtask.NewSegmentActionWithScope(
			nodeID,
			querycoordtask.ActionTypeGrow,
			channelName,
			segmentID,
			querypb.DataScope_Historical,
			100,
		),
	)
	assert.NoError(t, err)
	assert.NoError(t, scheduler.Add(loadTask))

	// The first dispatch injects ErrSegmentNotFound through the real executor.
	// The second removes the failed task and invokes the observer callback.
	scheduler.Dispatch(nodeID)
	taskResult := make(chan error, 1)
	go func() {
		taskResult <- loadTask.Wait()
	}()
	select {
	case err := <-taskResult:
		assert.ErrorIs(t, err, merr.ErrSegmentNotFound)
	case <-time.After(5 * time.Second):
		t.Fatal("segment task did not finish")
	}
	scheduler.Dispatch(nodeID)
	assert.True(t, observer.isNextTargetStale(collectionID))

	oldVersion := targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	time.Sleep(time.Millisecond)
	observer.check(ctx, collectionID)
	newVersion := targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.NextTarget)
	assert.Greater(t, newVersion, oldVersion)
	assert.Equal(t, 3, recoveryCalls)
	assert.True(t, observer.isNextTargetStale(collectionID))
	assert.NotNil(t, targetMgr.GetSealedSegment(ctx, collectionID, segmentID, meta.NextTarget))

	// Once recovery no longer contains the failed segment, the next refresh
	// resolves the stale cause.
	recoverySegments = nil
	time.Sleep(time.Millisecond)
	observer.check(ctx, collectionID)
	assert.Equal(t, 4, recoveryCalls)
	assert.False(t, observer.isNextTargetStale(collectionID))
	assert.Nil(t, targetMgr.GetSealedSegment(ctx, collectionID, segmentID, meta.NextTarget))
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
