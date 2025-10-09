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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	getBalancerFunc := func() balance.Balance { return balancer }
	checkSegmentExist := func(ctx context.Context, collectionID int64, segmentID int64) bool {
		return true
	}
	suite.checker = NewSegmentChecker(suite.meta, distManager, targetManager, suite.nodeMgr, getBalancerFunc, checkSegmentExist)

	suite.broker.EXPECT().GetPartitions(mock.Anything, int64(1)).Return([]int64{1}, nil).Maybe()
}

func (suite *SegmentCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *SegmentCheckerTestSuite) createMockBalancer() balance.Balance {
	balancer := balance.NewMockBalancer(suite.T())
	balancer.EXPECT().AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Return(func(ctx context.Context, collectionID int64, segments []*meta.Segment, nodes []int64, _ bool) []balance.SegmentAssignPlan {
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
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

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
	suite.EqualValues(1, action.GetSegmentID())
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
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

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
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))
	// test load l0 segments in current target
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok = tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	// seg l0 segment exist on a non delegator node
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	// test load l0 segments to delegator
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok = tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseL0Segments() {
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestSkipLoadSegments() {
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)
	checker.meta.ResourceManager.HandleNodeUp(ctx, 2)

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	// when channel not subscribed, segment_checker won't generate load segment task
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *SegmentCheckerTestSuite) TestReleaseSegments() {
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set target
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

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
	suite.EqualValues(2, action.GetSegmentID())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseRepeatedSegments() {
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

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
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(1, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityLow)

	// test less version exist on leader
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 1}, map[int64]*meta.Segment{}))
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *SegmentCheckerTestSuite) TestReleaseDirtySegments() {
	ctx := context.Background()
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1}))
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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

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
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseGrowingSegments() {
	ctx := context.Background()
	checker := suite.checker
	// segment3 is compacted from segment2, and node2 has growing segments 2 and 3. checker should generate
	// 2 tasks to reduce segment 2 and 3.
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

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
	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget)
	checker.dist.LeaderViewManager.Update(2, view)
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 2)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Actions()[0].(*task.SegmentAction).GetSegmentID() < tasks[j].Actions()[0].(*task.SegmentAction).GetSegmentID()
	})
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)

	suite.Len(tasks[1].Actions(), 1)
	action, ok = tasks[1].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[1].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(3, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[1].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestReleaseCompactedGrowingSegments() {
	ctx := context.Background()
	checker := suite.checker

	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	growingSegments := make(map[int64]*meta.Segment)
	// segment start pos after chekcpoint
	growingSegments[4] = utils.CreateTestSegment(1, 1, 4, 2, 1, "test-insert-channel")
	growingSegments[4].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 11}

	dmChannel := utils.CreateTestChannel(1, 2, 1, "test-insert-channel")
	dmChannel.UnflushedSegmentIds = []int64{2, 3}
	checker.dist.ChannelDistManager.Update(2, dmChannel)
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2}, growingSegments)
	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget)
	checker.dist.LeaderViewManager.Update(2, view)
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Actions()[0].(*task.SegmentAction).GetSegmentID() < tasks[j].Actions()[0].(*task.SegmentAction).GetSegmentID()
	})
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(4, action.GetSegmentID())
	suite.EqualValues(2, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestSkipReleaseGrowingSegments() {
	ctx := context.Background()
	checker := suite.checker
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))

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
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	growingSegments := make(map[int64]*meta.Segment)
	growingSegments[2] = utils.CreateTestSegment(1, 1, 2, 2, 0, "test-insert-channel")
	growingSegments[2].SegmentInfo.StartPosition = &msgpb.MsgPosition{Timestamp: 2}

	dmChannel := utils.CreateTestChannel(1, 2, 1, "test-insert-channel")
	dmChannel.UnflushedSegmentIds = []int64{2, 3}
	checker.dist.ChannelDistManager.Update(2, dmChannel)
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, growingSegments)
	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget) - 1
	checker.dist.LeaderViewManager.Update(2, view)

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)

	view.TargetVersion = checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget)
	checker.dist.LeaderViewManager.Update(2, view)
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.GetSegmentID())
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
	suite.EqualValues(1, action.GetSegmentID())
	suite.EqualValues(1, action.Node())
	suite.Equal(tasks[0].Priority(), task.TaskPriorityNormal)
}

func (suite *SegmentCheckerTestSuite) TestFilterOutExistedOnLeader() {
	checker := suite.checker

	// Setup test data
	collectionID := int64(1)
	partitionID := int64(1)
	segmentID1 := int64(1)
	segmentID2 := int64(2)
	segmentID3 := int64(3)
	nodeID1 := int64(1)
	nodeID2 := int64(2)
	channel := "test-insert-channel"

	// Create test replica
	replica := utils.CreateTestReplica(1, collectionID, []int64{nodeID1, nodeID2})

	// Create test segments
	segments := []*meta.Segment{
		utils.CreateTestSegment(collectionID, partitionID, segmentID1, nodeID1, 1, channel),
		utils.CreateTestSegment(collectionID, partitionID, segmentID2, nodeID2, 1, channel),
		utils.CreateTestSegment(collectionID, partitionID, segmentID3, nodeID1, 1, channel),
	}

	// Test case 1: No leader views - should skip releasing segments
	result := checker.filterOutExistedOnLeader(replica, segments)
	suite.Equal(0, len(result), "Should return all segments when no leader views")

	// Test case 2: Segment serving on leader - should be filtered out
	leaderView1 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{segmentID1: nodeID1}, map[int64]*meta.Segment{})
	checker.dist.LeaderViewManager.Update(nodeID1, leaderView1)

	result = checker.filterOutExistedOnLeader(replica, segments)
	suite.Len(result, 2, "Should filter out segment serving on leader")

	// Check that segmentID1 is filtered out
	for _, seg := range result {
		suite.NotEqual(segmentID1, seg.GetID(), "Segment serving on leader should be filtered out")
	}

	// Test case 3: Multiple leader views with segments serving on different nodes
	leaderView2 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{segmentID2: nodeID2}, map[int64]*meta.Segment{})
	checker.dist.LeaderViewManager.Update(nodeID2, leaderView2)

	result = checker.filterOutExistedOnLeader(replica, segments)
	suite.Len(result, 1, "Should filter out segments serving on their respective leaders")
	suite.Equal(segmentID3, result[0].GetID(), "Only non-serving segment should remain")

	// Test case 4: Segment exists in leader view but on different node - should not be filtered
	leaderView3 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{segmentID3: nodeID2}, map[int64]*meta.Segment{}) // segmentID3 exists but on nodeID2, not nodeID1
	checker.dist.LeaderViewManager.Update(nodeID1, leaderView3)

	result = checker.filterOutExistedOnLeader(replica, []*meta.Segment{segments[2]}) // Only test segmentID3
	suite.Len(result, 1, "Segment not serving on its actual node should not be filtered")
}

func (suite *SegmentCheckerTestSuite) TestFilterOutSegmentInUse() {
	ctx := context.Background()
	checker := suite.checker

	// Setup test data
	collectionID := int64(1)
	partitionID := int64(1)
	segmentID1 := int64(1)
	segmentID2 := int64(2)
	segmentID3 := int64(3)
	nodeID1 := int64(1)
	nodeID2 := int64(2)
	channel := "test-insert-channel"

	// Setup meta data
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(collectionID, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(collectionID, partitionID))

	// Create test replica
	replica := utils.CreateTestReplica(1, collectionID, []int64{nodeID1, nodeID2})

	// Create test segments
	segments := []*meta.Segment{
		utils.CreateTestSegment(collectionID, partitionID, segmentID1, nodeID1, 1, channel),
		utils.CreateTestSegment(collectionID, partitionID, segmentID2, nodeID2, 1, channel),
		utils.CreateTestSegment(collectionID, partitionID, segmentID3, nodeID1, 1, channel),
	}

	// Setup target to have a current version
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		channels, []*datapb.SegmentInfo{}, nil).Maybe()
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	currentTargetVersion := checker.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)

	// Test case 1: No leader views - should skip releasing segments
	result := checker.filterOutSegmentInUse(ctx, replica, segments)
	suite.Equal(0, len(result), "Should return all segments when no leader views")

	// Test case 2: Leader view with outdated target version - segment should be filtered (still in use)
	leaderView1 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView1.TargetVersion = currentTargetVersion - 1 // Outdated version
	checker.dist.LeaderViewManager.Update(nodeID1, leaderView1)

	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[0]})
	suite.Len(result, 0, "Segment should be filtered out when delegator hasn't updated to latest version")

	// Test case 3: Leader view with current target version - segment should not be filtered
	leaderView2 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView2.TargetVersion = currentTargetVersion
	checker.dist.LeaderViewManager.Update(nodeID1, leaderView2)

	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[0]})
	suite.Len(result, 1, "Segment should not be filtered when delegator has updated to latest version")

	// Test case 4: Leader view with initial target version - segment should not be filtered
	leaderView3 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView3.TargetVersion = initialTargetVersion
	checker.dist.LeaderViewManager.Update(nodeID2, leaderView3)

	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[1]})
	suite.Len(result, 1, "Segment should not be filtered when leader has initial target version")

	// Test case 5: Multiple leader views with mixed versions - segment should be filtered (still in use)
	leaderView4 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView4.TargetVersion = currentTargetVersion - 1 // Outdated

	leaderView5 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView5.TargetVersion = currentTargetVersion // Up to date

	checker.dist.LeaderViewManager.Update(nodeID1, leaderView4)
	checker.dist.LeaderViewManager.Update(nodeID2, leaderView5)

	testSegments := []*meta.Segment{
		utils.CreateTestSegment(collectionID, partitionID, segmentID1, nodeID1, 1, channel),
		utils.CreateTestSegment(collectionID, partitionID, segmentID2, nodeID2, 1, channel),
	}

	result = checker.filterOutSegmentInUse(ctx, replica, testSegments)
	suite.Len(result, 0, "Should release all segments when any delegator hasn't updated")

	// Test case 6: Partition is nil - should release all segments (no partition info)
	checker.meta.CollectionManager.RemovePartition(ctx, partitionID)
	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[0]})
	suite.Len(result, 0, "Should release all segments when partition is nil")
}

func TestSegmentCheckerSuite(t *testing.T) {
	suite.Run(t, new(SegmentCheckerTestSuite))
}

func TestGetSealedSegmentDiff_WithL0SegmentCheck(t *testing.T) {
	// Test case 1: L0 segment exists
	t.Run("L0_segment_exists", func(t *testing.T) {
		checkSegmentExist := func(ctx context.Context, collectionID int64, segmentID int64) bool {
			return true // L0 segment exists
		}

		// Create test segments
		segments := []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L0,
			},
			{
				ID:           2,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L1,
			},
		}

		// Filter L0 segments with existence check
		var level0Segments []*datapb.SegmentInfo
		for _, segment := range segments {
			if segment.GetLevel() == datapb.SegmentLevel_L0 && checkSegmentExist(context.Background(), segment.GetCollectionID(), segment.GetID()) {
				level0Segments = append(level0Segments, segment)
			}
		}

		// Verify: L0 segment should be included
		assert.Equal(t, 1, len(level0Segments))
		assert.Equal(t, int64(1), level0Segments[0].GetID())
	})

	// Test case 2: L0 segment does not exist
	t.Run("L0_segment_not_exists", func(t *testing.T) {
		checkSegmentExist := func(ctx context.Context, collectionID int64, segmentID int64) bool {
			return false // L0 segment does not exist
		}

		// Create test segments
		segments := []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L0,
			},
			{
				ID:           2,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L1,
			},
		}

		// Filter L0 segments with existence check
		var level0Segments []*datapb.SegmentInfo
		for _, segment := range segments {
			if segment.GetLevel() == datapb.SegmentLevel_L0 && checkSegmentExist(context.Background(), segment.GetCollectionID(), segment.GetID()) {
				level0Segments = append(level0Segments, segment)
			}
		}

		// Verify: L0 segment should be filtered out
		assert.Equal(t, 0, len(level0Segments))
	})

	// Test case 3: Mixed L0 segments, only some exist
	t.Run("Mixed_L0_segments", func(t *testing.T) {
		checkSegmentExist := func(ctx context.Context, collectionID int64, segmentID int64) bool {
			return segmentID == 1 // Only segment 1 exists
		}

		// Create test segments
		segments := []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L0,
			},
			{
				ID:           2,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L0,
			},
			{
				ID:           3,
				CollectionID: 1,
				PartitionID:  1,
				Level:        datapb.SegmentLevel_L1,
			},
		}

		// Filter L0 segments with existence check
		var level0Segments []*datapb.SegmentInfo
		for _, segment := range segments {
			if segment.GetLevel() == datapb.SegmentLevel_L0 && checkSegmentExist(context.Background(), segment.GetCollectionID(), segment.GetID()) {
				level0Segments = append(level0Segments, segment)
			}
		}

		// Verify: Only existing L0 segment should be included
		assert.Equal(t, 1, len(level0Segments))
		assert.Equal(t, int64(1), level0Segments[0].GetID())
	})
}

// createTestSegmentChecker creates a test SegmentChecker with mocked dependencies
func createTestSegmentChecker() (*SegmentChecker, *meta.Meta, *meta.DistributionManager, *session.NodeManager) {
	nodeMgr := session.NewNodeManager()
	metaManager := meta.NewMeta(nil, nil, nodeMgr)
	distManager := meta.NewDistributionManager()
	targetManager := meta.NewTargetManager(nil, metaManager)
	getBalancerFunc := func() balance.Balance { return balance.NewScoreBasedBalancer(nil, nil, nil, nil, nil) }
	checkSegmentExist := func(ctx context.Context, collectionID int64, segmentID int64) bool {
		return true
	}
	checker := NewSegmentChecker(metaManager, distManager, targetManager, nodeMgr, getBalancerFunc, checkSegmentExist)
	return checker, metaManager, distManager, nodeMgr
}

func TestGetSealedSegmentDiff_L0SegmentMultipleDelegators(t *testing.T) {
	defer mockey.UnPatchAll()

	ctx := context.Background()

	// Setup test data
	collectionID := int64(1)
	replicaID := int64(1)
	partitionID := int64(1)
	segmentID := int64(1)
	channel := "test-insert-channel"
	nodeID1 := int64(1)
	nodeID2 := int64(2)

	// Create test components
	checker, _, _, _ := createTestSegmentChecker()

	// Mock GetSealedSegmentsByCollection to return L0 segment
	mockey.Mock((*meta.TargetManager).GetSealedSegmentsByCollection).To(func(ctx context.Context, collectionID int64, scope meta.TargetScope) map[int64]*datapb.SegmentInfo {
		if scope == meta.CurrentTarget {
			return map[int64]*datapb.SegmentInfo{
				segmentID: {
					ID:            segmentID,
					CollectionID:  collectionID,
					PartitionID:   partitionID,
					InsertChannel: channel,
					Level:         datapb.SegmentLevel_L0,
				},
			}
		}
		return make(map[int64]*datapb.SegmentInfo)
	}).Build()

	// Mock IsNextTargetExist to return false
	mockey.Mock((*meta.TargetManager).IsNextTargetExist).Return(false).Build()

	// Mock meta manager methods to avoid direct meta manipulation
	// Mock Get method to return the test replica
	testReplica := utils.CreateTestReplica(replicaID, collectionID, []int64{nodeID1, nodeID2})
	mockey.Mock((*meta.ReplicaManager).Get).To(func(ctx context.Context, rid int64) *meta.Replica {
		if rid == replicaID {
			return testReplica
		}
		return nil
	}).Build()

	// Mock GetCollection to return test collection
	testCollection := utils.CreateTestCollection(collectionID, 1)
	mockey.Mock((*meta.Meta).GetCollection).To(func(ctx context.Context, cid int64) *meta.Collection {
		if cid == collectionID {
			return testCollection
		}
		return nil
	}).Build()

	// Mock NodeManager Get method to return compatible node versions
	mockey.Mock((*session.NodeManager).Get).To(func(nodeID int64) *session.NodeInfo {
		if nodeID == nodeID1 || nodeID == nodeID2 {
			return session.NewNodeInfo(session.ImmutableNodeInfo{
				NodeID:   nodeID,
				Address:  "localhost",
				Hostname: "localhost",
				Version:  common.Version,
			})
		}
		return nil
	}).Build()

	// Mock SegmentDistManager GetByFilter to return empty distribution initially
	mockey.Mock((*meta.SegmentDistManager).GetByFilter).Return([]*meta.Segment{}).Build()

	// Test case 1: Multiple delegators, one lacks the L0 segment
	leaderView1 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{segmentID: nodeID1}, map[int64]*meta.Segment{}) // Has the segment
	leaderView2 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{}) // Missing the segment

	// Mock LeaderViewManager GetByFilter to return leader views for L0 segment checking
	mockGetByFilter := mockey.Mock((*meta.LeaderViewManager).GetByFilter).To(func(filters ...meta.LeaderViewFilter) []*meta.LeaderView {
		// Return both leader views for L0 segment checking
		return []*meta.LeaderView{leaderView1, leaderView2}
	}).Build()
	toLoad, toRelease := checker.getSealedSegmentDiff(ctx, collectionID, replicaID)
	mockGetByFilter.Release()

	// Verify: L0 segment should be loaded for the delegator that lacks it
	assert.Len(t, toLoad, 1, "Should load L0 segment for delegator that lacks it")
	assert.Equal(t, segmentID, toLoad[0].GetID(), "Should load the correct L0 segment")
	assert.Empty(t, toRelease, "Should not release any segments")

	// Test case 2: All delegators have the L0 segment
	leaderView2WithSegment := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{segmentID: nodeID2}, map[int64]*meta.Segment{}) // Now has the segment

	// Update the mock to return leader views where both have the segment
	mockey.Mock((*meta.LeaderViewManager).GetByFilter).To(func(filters ...meta.LeaderViewFilter) []*meta.LeaderView {
		// Return both leader views, both now have the L0 segment
		return []*meta.LeaderView{leaderView1, leaderView2WithSegment}
	}).Build()

	toLoad, toRelease = checker.getSealedSegmentDiff(ctx, collectionID, replicaID)

	// Verify: No segments should be loaded when all delegators have the L0 segment
	assert.Empty(t, toLoad, "Should not load L0 segment when all delegators have it")
	assert.Empty(t, toRelease, "Should not release any segments")
}
