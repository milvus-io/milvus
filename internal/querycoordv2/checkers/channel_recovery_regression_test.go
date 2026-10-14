// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const recoveryTestChannel = "recovery_1v0"

func (suite *ChannelCheckerTestSuite) prepareChannelRecoveryTarget(replicas ...*meta.Replica) {
	ctx := context.Background()
	suite.Require().NoError(suite.meta.PutCollection(ctx, utils.CreateTestCollection(1, int32(len(replicas)))))
	suite.Require().NoError(suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1)))
	suite.Require().NoError(suite.meta.Put(ctx, replicas...))
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1, ChannelName: recoveryTestChannel}}, nil, nil)
	suite.Require().NoError(suite.checker.targetMgr.UpdateCollectionNextTarget(ctx, 1))
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryAcrossReplicas() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(
		utils.CreateTestReplica(1, 1, []int64{1}),
		utils.CreateTestReplica(2, 1, []int64{2}),
	)
	// An unchanged distribution means both replicas still need the channel on
	// the next tick too. The first replica must not consume the second's claim.
	for tick := 0; tick < 2; tick++ {
		tasks := suite.checker.Check(context.Background())
		suite.Require().Len(tasks, 2)
		targets := make(map[int64]int64)
		for _, work := range tasks {
			defer work.Cancel(nil)
			suite.Require().Len(work.Actions(), 1)
			action := work.Actions()[0].(*task.ChannelAction)
			suite.Equal(task.ActionTypeGrow, action.Type())
			suite.Equal(recoveryTestChannel, action.ChannelName())
			targets[work.ReplicaID()] = action.Node()
		}
		suite.Equal(map[int64]int64{1: 1, 2: 2}, targets)
	}
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoverySkipsUnknownReport() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	suite.nodeMgr.BeginLoadReportSync(0)
	for _, node := range suite.nodeMgr.GetAll() {
		node.UpdateLoadReport(nil, true, true, time.Now())
	}
	suite.True(suite.nodeMgr.LoadReportSyncReady(), "the startup wait has expired")
	suite.Empty(suite.checker.Check(context.Background()), "unknown reports cannot admit recovery")

	// The candidate with the smaller ID stays unknown. A report-only update
	// makes node 2 usable without changing target/distribution versions.
	suite.nodeMgr.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
	suite.False(suite.nodeMgr.Get(1).LoadAdmissionReady())
	suite.True(suite.nodeMgr.Get(2).LoadAdmissionReady())
	tasks := suite.checker.Check(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.EqualValues(2, tasks[0].Actions()[0].Node())
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryFallsBackFromUnknownWALOwner() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")
	snmanager.ResetStreamingNodeManager()
	suite.T().Cleanup(snmanager.ResetStreamingNodeManager)
	b := mock_balancer.NewMockBalancer(suite.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		cb(balancer.WatchChannelAssignmentsCallbackParam{
			Version:            typeutil.VersionInt64Pair{Global: 1, Local: 1},
			CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "recovery"}},
			Relations: []types.PChannelInfoAssigned{{
				Channel: types.PChannelInfo{Name: "recovery", Term: 1},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			}},
		})
		<-ctx.Done()
		return context.Cause(ctx)
	}).Maybe()
	streamingNodes := make(map[int64]*types.StreamingNodeInfoWithResourceGroup)
	for _, node := range []int64{1, 2} {
		streamingNodes[node] = &types.StreamingNodeInfoWithResourceGroup{
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: node, Address: "localhost:1"},
		}
	}
	b.EXPECT().GetAllStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().Close().Return().Maybe()
	b.EXPECT().GetLatestWALLocated(mock.Anything, "recovery").Return(int64(1), true).Maybe()
	balance.Register(b)

	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: meta.DefaultResourceGroupName, RwSqNodes: []int64{1, 2},
	}))
	suite.nodeMgr.BeginLoadReportSync(0)
	suite.nodeMgr.Get(1).UpdateLoadReport(nil, true, true, time.Now())
	suite.nodeMgr.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
	suite.EqualValues(1, snmanager.StaticStreamingNodeManager.GetWALLocated(recoveryTestChannel))
	tasks := suite.checker.Check(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.EqualValues(2, tasks[0].Actions()[0].Node(), "an unknown WAL owner must not hide the ready fallback")
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryStatusIncludesPlannedAndUnplacedRGs() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1)
	suite.prepareChannelRecoveryTarget(
		meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, ResourceGroup: "ready", Nodes: []int64{1}}),
		meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 1, ResourceGroup: "no-node"}),
	)
	tasks, recovering := suite.checker.CheckWithRecovery(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.Contains(recovering, "ready", "a planned watch is still recovery")
	suite.Contains(recovering, "no-node", "no task does not mean recovery finished")
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryCurrentTargetAndReplicaCache() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "first", Nodes: []int64{1},
	}))
	ctx := context.Background()
	suite.True(suite.checker.targetMgr.UpdateCollectionCurrentTarget(ctx, 1))
	suite.False(suite.checker.targetMgr.IsNextTargetExist(ctx, 1))
	tasks, recovering := suite.checker.CheckWithRecovery(ctx)
	suite.Require().Len(tasks, 1, "Current alone still requires its missing channel")
	defer tasks[0].Cancel(nil)
	suite.Contains(recovering, "first")
	suite.checker.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: recoveryTestChannel}, Node: 1,
		View: &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})
	tasks, recovering = suite.checker.CheckWithRecovery(ctx)
	suite.Empty(tasks)
	suite.Empty(recovering)
	suite.Contains(suite.checker.versionCache, int64(1))

	// Replica additions do not alter target/distribution versions.
	suite.Require().NoError(suite.meta.Put(ctx, meta.NewReplica(&querypb.Replica{
		ID: 2, CollectionID: 1, ResourceGroup: "second", Nodes: []int64{2},
	})))
	tasks, recovering = suite.checker.CheckWithRecovery(ctx)
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.EqualValues(2, tasks[0].ReplicaID())
	suite.Contains(recovering, "second")
	suite.NotContains(recovering, "first")
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryMissingWALReturnsAndRetries() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")
	snmanager.ResetStreamingNodeManager()
	var known atomic.Bool
	var releaseOnce sync.Once
	release := make(chan struct{})
	b := mock_balancer.NewMockBalancer(suite.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		cb(balancer.WatchChannelAssignmentsCallbackParam{
			Version:            typeutil.VersionInt64Pair{Global: 1, Local: 1},
			CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "recovery"}},
		})
		// Also make an old blocking lookup releasable on test failure.
		select {
		case <-release:
			cb(balancer.WatchChannelAssignmentsCallbackParam{
				Version:            typeutil.VersionInt64Pair{Global: 1, Local: 2},
				CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "recovery"}},
				Relations:          []types.PChannelInfoAssigned{{Channel: types.PChannelInfo{Name: "recovery", Term: 1}, Node: types.StreamingNodeInfo{ServerID: 1}}},
			})
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		<-ctx.Done()
		return context.Cause(ctx)
	}).Maybe()
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}},
	}
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(nodes, nil).Maybe()
	b.EXPECT().GetLatestWALLocated(mock.Anything, "recovery").RunAndReturn(func(context.Context, string) (int64, bool) {
		return 1, known.Load()
	}).Maybe()
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)
	suite.setNodeAvailable(1)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "waiting-wal", RwSqNodes: []int64{1},
	}))
	done := make(chan struct{})
	var tasks []task.Task
	var recovering map[string]struct{}
	suite.T().Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(time.Second):
			suite.T().Error("WAL recovery checker did not exit during cleanup")
		}
		snmanager.ResetStreamingNodeManager()
	})
	go func() {
		tasks, recovering = suite.checker.CheckWithRecovery(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		suite.FailNow("missing WAL assignment blocked the recovery checker")
	}
	suite.Empty(tasks, "missing assignment must not invent a destination")
	suite.Contains(recovering, "waiting-wal")
	suite.NotContains(suite.checker.versionCache, int64(1))
	// Only WAL metadata changes: the next check must retry the same target.
	known.Store(true)
	tasks, recovering = suite.checker.CheckWithRecovery(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.EqualValues(1, tasks[0].Actions()[0].Node())
	suite.Contains(recovering, "waiting-wal")
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryNextDoesNotRestoreRetiredChannel() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1)
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, ResourceGroup: "rg", Nodes: []int64{1}})
	suite.prepareChannelRecoveryTarget(replica)
	target := meta.NewMockTargetManager(suite.T())
	old := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "retired"}}
	next := &meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "replacement"}}
	target.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.CurrentTarget).Return(map[string]*meta.DmChannel{"retired": old})
	target.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.NextTarget).Return(map[string]*meta.DmChannel{"replacement": next})
	target.EXPECT().IsNextTargetExist(mock.Anything, int64(1)).Return(true)
	suite.checker.targetMgr = target
	lacking, _ := suite.checker.getDmChannelDiff(context.Background(), 1, replica.GetID())
	suite.Equal([]*meta.DmChannel{next}, lacking)
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryWaitsForReportedLeaderServiceability() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "recovering-view", Nodes: []int64{1},
	}))
	ctx := context.Background()
	report := func(serviceable bool) {
		suite.checker.dist.ChannelDistManager.Update(1, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: recoveryTestChannel},
			Node:         1,
			View: &meta.LeaderView{
				ID: 1, CollectionID: 1, Channel: recoveryTestChannel,
				Status: &querypb.LeaderViewStatus{Serviceable: serviceable},
			},
		})
	}
	// This is the state recovered from QN after QC restart: the delegator is
	// running and reported, but its query view is not yet serviceable. There
	// is no local Watch task to represent this outstanding recovery.
	report(false)
	for tick := 0; tick < 2; tick++ {
		tasks, recovering := suite.checker.CheckWithRecovery(ctx)
		suite.Empty(tasks, "an existing delegator must not be watched again")
		suite.Contains(recovering, "recovering-view")
		suite.NotContains(suite.checker.versionCache, int64(1), "unserviceable distribution is not converged")
	}
	report(true)
	tasks, recovering := suite.checker.CheckWithRecovery(ctx)
	suite.Empty(tasks)
	suite.Empty(recovering)
	suite.Contains(suite.checker.versionCache, int64(1))
	// A subsequent reported loss of serviceability invalidates the cached
	// distribution version and must close balance admission again.
	report(false)
	tasks, recovering = suite.checker.CheckWithRecovery(ctx)
	suite.Empty(tasks)
	suite.Contains(recovering, "recovering-view")
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryBeforeBalancerRegistration() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(suite.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		cb(balancer.WatchChannelAssignmentsCallbackParam{
			Version:            typeutil.VersionInt64Pair{Global: 1, Local: 1},
			CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "recovery"}},
			Relations: []types.PChannelInfoAssigned{{
				Channel: types.PChannelInfo{Name: "recovery", Term: 1},
				Node:    types.StreamingNodeInfo{ServerID: 1},
			}},
		})
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1}},
	}, nil).Maybe()
	b.EXPECT().GetLatestWALLocated(mock.Anything, "recovery").Return(int64(1), true).Maybe()
	suite.setNodeAvailable(1)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "startup", RwSqNodes: []int64{1},
	}))
	var register sync.Once
	done := make(chan struct{})
	var tasks []task.Task
	var recovering map[string]struct{}
	suite.T().Cleanup(func() {
		register.Do(func() { balance.Register(b) })
		select {
		case <-done:
		case <-time.After(time.Second):
			suite.T().Error("channel planning did not finish after balancer registration")
		}
		snmanager.ResetStreamingNodeManager()
	})
	go func() {
		tasks, recovering = suite.checker.CheckWithRecovery(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		suite.FailNow("unregistered streaming balancer blocked channel planning")
	}
	suite.Empty(tasks)
	suite.Contains(recovering, "startup")
	suite.NotContains(suite.checker.versionCache, int64(1))
	register.Do(func() { balance.Register(b) })
	tasks, recovering = suite.checker.CheckWithRecovery(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.Contains(recovering, "startup")
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryCacheInvalidatesWhenNextPromotes() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "rg", Nodes: []int64{1},
	}))
	ctx := context.Background()
	targets := suite.checker.targetMgr
	suite.Require().True(targets.UpdateCollectionCurrentTarget(ctx, 1))
	oldVersion := targets.GetCollectionTargetVersion(ctx, 1, meta.CurrentTarget)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Unset()
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1, ChannelName: "replacement"}}, nil, nil)
	suite.Require().NoError(targets.UpdateCollectionNextTarget(ctx, 1))
	newVersion := targets.GetCollectionTargetVersion(ctx, 1, meta.NextTarget)
	suite.Require().NotEqual(oldVersion, newVersion)
	channels := make([]*meta.DmChannel, 0, 2)
	for _, name := range []string{recoveryTestChannel, "replacement"} {
		channels = append(channels, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: name}, Node: 1,
			View: &meta.LeaderView{ID: 1, CollectionID: 1, Channel: name, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})
	}
	suite.checker.dist.ChannelDistManager.Update(1, channels...)
	distVersion := suite.checker.dist.ChannelDistManager.GetVersion()
	tasks, recovering := suite.checker.CheckWithRecovery(ctx)
	suite.Empty(tasks, "Current still protects the old channel while Next is loading")
	suite.Empty(recovering)
	suite.Contains(suite.checker.versionCache, int64(1))

	suite.Require().True(targets.UpdateCollectionCurrentTarget(ctx, 1))
	suite.Equal(newVersion, targets.GetCollectionTargetVersion(ctx, 1, meta.NextTargetFirst), "selected target version is unchanged by promotion")
	suite.Equal(distVersion, suite.checker.dist.ChannelDistManager.GetVersion())
	tasks, recovering = suite.checker.CheckWithRecovery(ctx)
	suite.Require().Len(tasks, 1, "promotion must recheck the changed Current/Next release union")
	defer tasks[0].Cancel(nil)
	suite.Empty(recovering)
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.Equal(recoveryTestChannel, action.ChannelName())
}
