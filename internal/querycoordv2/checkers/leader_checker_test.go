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

package checkers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type LeaderCheckerTestSuite struct {
	suite.Suite
	checker *LeaderChecker
	kv      kv.MetaKv

	meta    *meta.Meta
	broker  *meta.MockBroker
	nodeMgr *session.NodeManager
}

func (suite *LeaderCheckerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *LeaderCheckerTestSuite) SetupTest() {
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

	// meta
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())

	distManager := meta.NewDistributionManager()
	targetManager := meta.NewTargetManager(suite.broker, suite.meta)
	suite.checker = NewLeaderChecker(suite.meta, distManager, targetManager, suite.nodeMgr)
}

func (suite *LeaderCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *LeaderCheckerTestSuite) TestSyncLoadedSegments() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)

	// before target ready, should skip check collection
	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 0)

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	// test leader view lack of segments
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	loadVersion := time.Now().UnixMilli()
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, loadVersion, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	tasks = suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeGrow)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(1))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(1))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)

	// test segment's version in leader view doesn't match segment's version in dist
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, 1, "test-insert-channel"))
	view = utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	view.Segments[1] = &querypb.SegmentDist{
		NodeID:  0,
		Version: time.Now().UnixMilli() - 1,
	}
	observer.dist.LeaderViewManager.Update(2, view)

	tasks = suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeGrow)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(1))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(1))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)

	// test skip sync l0 segment
	segments = []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
			Level:         datapb.SegmentLevel_L0,
		},
	}
	suite.broker.ExpectedCalls = nil
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	// mock l0 segment exist on non delegator node, doesn't set to leader view
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, loadVersion, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view = utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)
	tasks = suite.checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *LeaderCheckerTestSuite) TestActivation() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	suite.checker.Deactivate()
	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 0)
	suite.checker.Activate()
	tasks = suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeGrow)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(1))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(1))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)
}

func (suite *LeaderCheckerTestSuite) TestStoppingNode() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	replica := utils.CreateTestReplica(1, 1, []int64{1, 2})
	observer.meta.ReplicaManager.Put(replica)
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	mutableReplica := replica.CopyForWrite()
	mutableReplica.AddRONode(2)
	observer.meta.ReplicaManager.Put(mutableReplica.IntoReplica())

	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *LeaderCheckerTestSuite) TestIgnoreSyncLoadedSegments() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, 1, "test-insert-channel"),
		utils.CreateTestSegment(1, 1, 2, 2, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeGrow)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(1))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(1))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)
}

func (suite *LeaderCheckerTestSuite) TestSyncLoadedSegmentsWithReplicas() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 2))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(2, 1, []int64{3, 4}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))

	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 0, "test-insert-channel"))
	observer.dist.SegmentDistManager.Update(4, utils.CreateTestSegment(1, 1, 1, 4, 0, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(4, utils.CreateTestChannel(1, 4, 2, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)
	view2 := utils.CreateTestLeaderView(4, 1, "test-insert-channel", map[int64]int64{1: 4}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(4, view2)

	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Equal(tasks[0].ReplicaID(), int64(1))
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeGrow)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(1))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(1))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)
}

func (suite *LeaderCheckerTestSuite) TestSyncRemovedSegments() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)

	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 1}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Equal(tasks[0].ReplicaID(), int64(1))
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeReduce)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(2))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(3))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).Version(), int64(0))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)

	// skip sync l0 segments
	segments := []*datapb.SegmentInfo{
		{
			ID:            3,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
			Level:         datapb.SegmentLevel_L0,
		},
	}
	suite.broker.ExpectedCalls = nil
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)

	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)

	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view = utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 1}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	tasks = suite.checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *LeaderCheckerTestSuite) TestIgnoreSyncRemovedSegments() {
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	segments := []*datapb.SegmentInfo{
		{
			ID:            2,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))

	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	observer.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2, 2: 2}, map[int64]*meta.Segment{}))

	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Equal(tasks[0].ReplicaID(), int64(1))
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeReduce)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(2))
	suite.Equal(tasks[0].Actions()[0].(*task.LeaderAction).SegmentID(), int64(3))
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)
}

func (suite *LeaderCheckerTestSuite) TestUpdatePartitionStats() {
	testChannel := "test-insert-channel"
	leaderID := int64(2)
	observer := suite.checker
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: testChannel,
		},
	}
	// latest partition stats is 101
	newPartitionStatsMap := make(map[int64]int64)
	newPartitionStatsMap[1] = 101
	channels := []*datapb.VchannelInfo{
		{
			CollectionID:           1,
			ChannelName:            testChannel,
			PartitionStatsVersions: newPartitionStatsMap,
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)

	// before target ready, should skip check collection
	tasks := suite.checker.Check(context.TODO())
	suite.Len(tasks, 0)

	// try to update cur/next target
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	loadVersion := time.Now().UnixMilli()
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, loadVersion, testChannel))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, testChannel))
	view := utils.CreateTestLeaderView(2, 1, testChannel, map[int64]int64{2: 1}, map[int64]*meta.Segment{})
	view.PartitionStatsVersions = map[int64]int64{
		1: 100,
	}
	// current partition stat version in leader view is version100 for partition1
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(leaderID, view)

	tasks = suite.checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Equal(tasks[0].Source(), utils.LeaderChecker)
	suite.Len(tasks[0].Actions(), 1)
	suite.Equal(tasks[0].Actions()[0].Type(), task.ActionTypeUpdate)
	suite.Equal(tasks[0].Actions()[0].Node(), int64(2))
}

func TestLeaderCheckerSuite(t *testing.T) {
	suite.Run(t, new(LeaderCheckerTestSuite))
}
