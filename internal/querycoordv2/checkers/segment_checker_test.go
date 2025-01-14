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
	"sort"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type SegmentCheckerTestSuite struct {
	suite.Suite
	kv      kv.MetaKv
	checker *SegmentChecker
	meta    *meta.Meta
	broker  *meta.MockBroker
	nodeMgr *session.NodeManager
}

func (suite *SegmentCheckerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *SegmentCheckerTestSuite) SetupTest() {
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
	distManager := meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	targetManager := meta.NewTargetManager(suite.broker, suite.meta)

	balancer := suite.createMockBalancer()
	suite.checker = NewSegmentChecker(suite.meta, distManager, targetManager, suite.nodeMgr, func() balance.Balance { return balancer })

	suite.broker.EXPECT().GetPartitions(mock.Anything, int64(1)).Return([]int64{1}, nil).Maybe()
}

func (suite *SegmentCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *SegmentCheckerTestSuite) createMockBalancer() balance.Balance {
	balancer := balance.NewMockBalancer(suite.T())
	balancer.EXPECT().AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Return(func(collectionID int64, segments []*meta.Segment, nodes []int64, _ bool) []balance.SegmentAssignPlan {
		plans := make([]balance.SegmentAssignPlan, 0, len(segments))
		for i, s := range segments {
			plan := balance.SegmentAssignPlan{
				Segment: s,
				From:    -1,
				To:      nodes[i%len(nodes)],
				Replica: meta.NilReplica,
			}
			plans = append(plans, plan)
		}
		return plans
	})
	return balancer
}

func (suite *SegmentCheckerTestSuite) TestLoadSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(1)
	checker.meta.ResourceManager.HandleNodeUp(2)

	// set target
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
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{}))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	// test activation
	checker.Deactivate()
	suite.False(checker.IsActive())
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 0)

	checker.Activate()
	suite.True(checker.IsActive())
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
}

func (suite *SegmentCheckerTestSuite) TestLoadL0Segments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
		Version:  common.Version,
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
		Version:  common.Version,
	}))
	checker.meta.ResourceManager.HandleNodeUp(1)
	checker.meta.ResourceManager.HandleNodeUp(2)

	// set target
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
			Level:         datapb.SegmentLevel_L0,
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
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{}))

	// test load l0 segments in next target
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	checker.targetMgr.UpdateCollectionCurrentTarget(int64(1))
	// test load l0 segments in current target
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok = tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	// seg l0 segment exist on a non delegator node
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	// test load l0 segments to delegator
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok = tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseL0Segments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(1)
	checker.meta.ResourceManager.HandleNodeUp(2)

	// set target
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
			Level:         datapb.SegmentLevel_L0,
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
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(int64(1))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{}))

	// seg l0 segment exist on a non delegator node
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 1, 2, 100, "test-insert-channel"))

	// release duplicate l0 segment
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)

	checker.dist.SegmentDistManager.Update(1)

	// test release l0 segment which doesn't exist in target
	suite.broker.ExpectedCalls = nil
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestSkipLoadSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(1)
	checker.meta.ResourceManager.HandleNodeUp(2)

	// set target
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
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	// when channel not subscribed, segment_checker won't generate load segment task
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *SegmentCheckerTestSuite) TestReleaseSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set target
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{}))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.SegmentID())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseRepeatedSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set target
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
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, map[int64]*meta.Segment{}))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 1, 1, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(1, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)

	// test less version exist on leader
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 1}, map[int64]*meta.Segment{}))
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *SegmentCheckerTestSuite) TestReleaseDirtySegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1}))
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

	// set target
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
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, map[int64]*meta.Segment{}))
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(-1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestSkipReleaseSealedSegments() {
	checker := suite.checker

	collectionID := int64(1)
	partitionID := int64(1)
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(collectionID, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(collectionID, partitionID))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, collectionID, []int64{1, 2}))

	// set target
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
			SeekPosition: &msgpb.MsgPosition{Timestamp: 10},
		},
	}
	segments := []*datapb.SegmentInfo{}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	checker.targetMgr.UpdateCollectionNextTarget(collectionID)
	checker.targetMgr.UpdateCollectionCurrentTarget(collectionID)
	checker.targetMgr.UpdateCollectionNextTarget(collectionID)
	readableVersion := checker.targetMgr.GetCollectionTargetVersion(collectionID, meta.CurrentTarget)

	// test less target version exist on leader,meet segment doesn't exit in target, segment should be released
	nodeID := int64(2)
	segmentID := int64(1)
	checker.dist.ChannelDistManager.Update(nodeID, utils.CreateTestChannel(collectionID, nodeID, segmentID, "test-insert-channel"))
	view := utils.CreateTestLeaderView(nodeID, collectionID, "test-insert-channel", map[int64]int64{segmentID: 2}, map[int64]*meta.Segment{})
	view.TargetVersion = readableVersion - 1
	checker.dist.LeaderViewManager.Update(nodeID, view)
	checker.dist.SegmentDistManager.Update(nodeID, utils.CreateTestSegment(collectionID, partitionID, segmentID, nodeID, 2, "test-insert-channel"))
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)

	// test leader's target version update to latest,meet segment doesn't exit in target, segment should be released
	view = utils.CreateTestLeaderView(nodeID, collectionID, "test-insert-channel", map[int64]int64{1: 3}, map[int64]*meta.Segment{})
	view.TargetVersion = readableVersion
	checker.dist.LeaderViewManager.Update(2, view)
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(segmentID, action.SegmentID())
	suite.EqualValues(nodeID, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	// test leader with initialTargetVersion, meet segment doesn't exit in target, segment should be released
	view = utils.CreateTestLeaderView(nodeID, collectionID, "test-insert-channel", map[int64]int64{1: 3}, map[int64]*meta.Segment{})
	view.TargetVersion = initialTargetVersion
	checker.dist.LeaderViewManager.Update(2, view)
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok = tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(segmentID, action.SegmentID())
	suite.EqualValues(nodeID, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseGrowingSegments() {
	checker := suite.checker
	// segment3 is compacted from segment2, and node2 has growing segments 2 and 3. checker should generate
	// 2 tasks to reduce segment 2 and 3.
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	segments := []*datapb.SegmentInfo{
		{
			ID:            3,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
			SeekPosition: &msgpb.MsgPosition{Timestamp: 10},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	growingSegments := make(map[int64]*meta.Segment)
	growingSegments[2] = utils.CreateTestSegment(1, 1, 2, 2, 0, "test-insert-channel")
	growingSegments[2].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 2}
	growingSegments[3] = utils.CreateTestSegment(1, 1, 3, 2, 1, "test-insert-channel")
	growingSegments[3].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 3}
	growingSegments[4] = utils.CreateTestSegment(1, 1, 4, 2, 1, "test-insert-channel")
	growingSegments[4].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 11}

	dmChannel := utils.CreateTestChannel(1, 2, 1, "test-insert-channel")
	dmChannel.UnflushedSegmentIds = []int64{2, 3}
	checker.dist.ChannelDistManager.Update(2, dmChannel)
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2}, growingSegments)
	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(int64(1), meta.CurrentTarget)
	checker.dist.LeaderViewManager.Update(2, view)
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 2)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Actions()[0].(*task.SegmentAction).SegmentID() < tasks[j].Actions()[0].(*task.SegmentAction).SegmentID()
	})
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	suite.Len(tasks[1].Actions(), 1)
	action, ok = tasks[1].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[1].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(3, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[1].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseCompactedGrowingSegments() {
	checker := suite.checker

	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	segments := []*datapb.SegmentInfo{
		{
			ID:            3,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID:      1,
			ChannelName:       "test-insert-channel",
			SeekPosition:      &msgpb.MsgPosition{Timestamp: 10},
			DroppedSegmentIds: []int64{4},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	growingSegments := make(map[int64]*meta.Segment)
	// segment start pos after chekcpoint
	growingSegments[4] = utils.CreateTestSegment(1, 1, 4, 2, 1, "test-insert-channel")
	growingSegments[4].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 11}

	dmChannel := utils.CreateTestChannel(1, 2, 1, "test-insert-channel")
	dmChannel.UnflushedSegmentIds = []int64{2, 3}
	checker.dist.ChannelDistManager.Update(2, dmChannel)
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2}, growingSegments)
	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(int64(1), meta.CurrentTarget)
	checker.dist.LeaderViewManager.Update(2, view)
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Actions()[0].(*task.SegmentAction).SegmentID() < tasks[j].Actions()[0].(*task.SegmentAction).SegmentID()
	})
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(4, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestSkipReleaseGrowingSegments() {
	checker := suite.checker
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	segments := []*datapb.SegmentInfo{}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
			SeekPosition: &msgpb.MsgPosition{Timestamp: 10},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(int64(1))

	growingSegments := make(map[int64]*meta.Segment)
	growingSegments[2] = utils.CreateTestSegment(1, 1, 2, 2, 0, "test-insert-channel")
	growingSegments[2].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 2}

	dmChannel := utils.CreateTestChannel(1, 2, 1, "test-insert-channel")
	dmChannel.UnflushedSegmentIds = []int64{2, 3}
	checker.dist.ChannelDistManager.Update(2, dmChannel)
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, growingSegments)
	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(int64(1), meta.CurrentTarget) - 1
	checker.dist.LeaderViewManager.Update(2, view)

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)

	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(int64(1), meta.CurrentTarget)
	checker.dist.LeaderViewManager.Update(2, view)
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.SegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseDroppedSegments() {
	checker := suite.checker
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(-1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(1, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func TestSegmentCheckerSuite(t *testing.T) {
	suite.Run(t, new(SegmentCheckerTestSuite))
}
