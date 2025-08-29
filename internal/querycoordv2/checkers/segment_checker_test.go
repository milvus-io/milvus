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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
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
	distManager := meta.NewDistributionManager(suite.nodeMgr)
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
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		View:    &meta.LeaderView{ID: 2, CollectionID: 1, Channel: "test-insert-channel", Version: 1, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})

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
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		View:    &meta.LeaderView{ID: 2, CollectionID: 1, Channel: "test-insert-channel", Version: 1, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})

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
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 1, 1, 2, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		View:    utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, map[int64]*meta.Segment{}),
	})

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
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		View:    utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 1}, map[int64]*meta.Segment{}),
	})
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
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		View:    utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, map[int64]*meta.Segment{}),
	})

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

	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 2, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         "test-insert-channel",
			UnflushedSegmentIds: []int64{2, 3},
		},
		Node:    2,
		Version: 1,
		View: &meta.LeaderView{
			ID:              2,
			CollectionID:    1,
			Channel:         "test-insert-channel",
			TargetVersion:   checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget),
			Segments:        map[int64]*querypb.SegmentDist{3: {NodeID: 2}},
			GrowingSegments: growingSegments,
			Status:          &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

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

	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 2, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         "test-insert-channel",
			UnflushedSegmentIds: []int64{2, 3},
		},
		Node:    2,
		Version: 1,
		View: &meta.LeaderView{
			ID:              2,
			CollectionID:    1,
			Channel:         "test-insert-channel",
			TargetVersion:   checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget),
			Segments:        map[int64]*querypb.SegmentDist{3: {NodeID: 2}},
			GrowingSegments: growingSegments,
			Status:          &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

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

	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         "test-insert-channel",
			UnflushedSegmentIds: []int64{2, 3},
		},
		Node:    2,
		Version: 1,
		View: &meta.LeaderView{
			ID:              2,
			CollectionID:    1,
			Channel:         "test-insert-channel",
			TargetVersion:   checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget) - 1,
			Segments:        map[int64]*querypb.SegmentDist{3: {NodeID: 2}},
			GrowingSegments: growingSegments,
			Status:          &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)

	checker.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         "test-insert-channel",
			UnflushedSegmentIds: []int64{2, 3},
		},
		Node:    2,
		Version: 1,
		View: &meta.LeaderView{
			ID:              2,
			CollectionID:    1,
			Channel:         "test-insert-channel",
			TargetVersion:   checker.targetMgr.GetCollectionTargetVersion(ctx, int64(1), meta.CurrentTarget),
			Segments:        map[int64]*querypb.SegmentDist{3: {NodeID: 2}},
			GrowingSegments: growingSegments,
		},
	})
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

func (suite *SegmentCheckerTestSuite) TestLoadPriority() {
	ctx := context.Background()
	collectionID := int64(1)
	replicaID := int64(1)

	// prepare replica
	replica := meta.NewReplicaWithPriority(&querypb.Replica{
		ID:           replicaID,
		CollectionID: collectionID,
		Nodes:        []int64{1, 2},
	}, commonpb.LoadPriority_LOW)
	suite.meta.ReplicaManager.Put(ctx, replica)

	// prepare segments
	segment1 := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  collectionID,
		PartitionID:   -1,
		InsertChannel: "channel1",
		State:         commonpb.SegmentState_Sealed,
		NumOfRows:     100,
		StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		DmlPosition:   &msgpb.MsgPosition{Timestamp: 200},
	}
	segment2 := &datapb.SegmentInfo{
		ID:            2,
		CollectionID:  collectionID,
		PartitionID:   -1,
		InsertChannel: "channel1",
		State:         commonpb.SegmentState_Sealed,
		NumOfRows:     100,
		StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		DmlPosition:   &msgpb.MsgPosition{Timestamp: 200},
	}

	// set up current target
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		[]*datapb.VchannelInfo{
			{
				CollectionID: collectionID,
				ChannelName:  "channel1",
			},
		},
		[]*datapb.SegmentInfo{segment1},
		nil,
	).Once()
	suite.checker.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.checker.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	// set up next target with segment1 and segment2
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		[]*datapb.VchannelInfo{
			{
				CollectionID: collectionID,
				ChannelName:  "channel1",
			},
		},
		[]*datapb.SegmentInfo{segment1, segment2},
		nil,
	).Once()
	suite.checker.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)

	// test getSealedSegmentDiff
	toLoad, loadPriorities, toRelease := suite.checker.getSealedSegmentDiff(ctx, collectionID, replicaID)

	// verify results
	suite.Equal(2, len(toLoad))
	suite.Equal(2, len(loadPriorities))
	suite.Equal(0, len(toRelease))

	// segment2 not in current target, should use replica's priority
	suite.True(segment2.GetID() == toLoad[0].GetID() || segment2.GetID() == toLoad[1].GetID())
	suite.True(segment1.GetID() == toLoad[0].GetID() || segment1.GetID() == toLoad[1].GetID())
	if segment2.GetID() == toLoad[0].GetID() {
		suite.Equal(commonpb.LoadPriority_LOW, loadPriorities[0])
		suite.Equal(commonpb.LoadPriority_HIGH, loadPriorities[1])
	} else {
		suite.Equal(commonpb.LoadPriority_HIGH, loadPriorities[0])
		suite.Equal(commonpb.LoadPriority_LOW, loadPriorities[1])
	}

	// update current target to include segment2
	suite.checker.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	// test again
	toLoad, loadPriorities, toRelease = suite.checker.getSealedSegmentDiff(ctx, collectionID, replicaID)
	// verify results
	suite.Equal(0, len(toLoad))
	suite.Equal(0, len(loadPriorities))
	suite.Equal(0, len(toRelease))
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
	checker.dist.ChannelDistManager.Update(nodeID1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID1,
		View: leaderView1,
	})

	result = checker.filterOutExistedOnLeader(replica, segments)
	suite.Len(result, 2, "Should filter out segment serving on leader")

	// Check that segmentID1 is filtered out
	for _, seg := range result {
		suite.NotEqual(segmentID1, seg.GetID(), "Segment serving on leader should be filtered out")
	}

	// Test case 3: Multiple leader views with segments serving on different nodes
	leaderView2 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{segmentID2: nodeID2}, map[int64]*meta.Segment{})
	checker.dist.ChannelDistManager.Update(nodeID2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID2,
		View: leaderView2,
	})

	result = checker.filterOutExistedOnLeader(replica, segments)
	suite.Len(result, 1, "Should filter out segments serving on their respective leaders")
	suite.Equal(segmentID3, result[0].GetID(), "Only non-serving segment should remain")

	// Test case 4: Segment exists in leader view but on different node - should not be filtered
	leaderView3 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{segmentID3: nodeID2}, map[int64]*meta.Segment{}) // segmentID3 exists but on nodeID2, not nodeID1
	checker.dist.ChannelDistManager.Update(nodeID1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID1,
		View: leaderView3,
	})

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
	checker.dist.ChannelDistManager.Update(nodeID1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID1,
		View: leaderView1,
	})

	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[0]})
	suite.Len(result, 0, "Segment should be filtered out when delegator hasn't updated to latest version")

	// Test case 3: Leader view with current target version - segment should not be filtered
	leaderView2 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView2.TargetVersion = currentTargetVersion
	checker.dist.ChannelDistManager.Update(nodeID1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID1,
		View: leaderView2,
	})

	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[0]})
	suite.Len(result, 1, "Segment should not be filtered when delegator has updated to latest version")

	// Test case 4: Leader view with initial target version - segment should not be filtered
	leaderView3 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView3.TargetVersion = initialTargetVersion
	checker.dist.ChannelDistManager.Update(nodeID2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID2,
		View: leaderView3,
	})

	result = checker.filterOutSegmentInUse(ctx, replica, []*meta.Segment{segments[1]})
	suite.Len(result, 1, "Segment should not be filtered when leader has initial target version")

	// Test case 5: Multiple leader views with mixed versions - segment should be filtered (still in use)
	leaderView4 := utils.CreateTestLeaderView(nodeID1, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView4.TargetVersion = currentTargetVersion - 1 // Outdated

	leaderView5 := utils.CreateTestLeaderView(nodeID2, collectionID, channel,
		map[int64]int64{}, map[int64]*meta.Segment{})
	leaderView5.TargetVersion = currentTargetVersion // Up to date

	checker.dist.ChannelDistManager.Update(nodeID1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID1,
		View: leaderView4,
	})
	checker.dist.ChannelDistManager.Update(nodeID2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node: nodeID2,
		View: leaderView5,
	})
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
