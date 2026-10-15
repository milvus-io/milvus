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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CheckerControllerSuite struct {
	suite.Suite
	kv            kv.MetaKv
	meta          *meta.Meta
	broker        *meta.MockBroker
	nodeMgr       *session.NodeManager
	dist          *meta.DistributionManager
	targetManager *meta.TargetManager
	scheduler     *task.MockScheduler
	balancer      *balance.MockBalancer

	controller *CheckerController
}

func (suite *CheckerControllerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *CheckerControllerSuite) SetupTest() {
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
	suite.dist = meta.NewDistributionManager(suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetManager = meta.NewTargetManager(suite.broker, suite.meta)

	suite.balancer = balance.NewMockBalancer(suite.T())
	suite.scheduler = task.NewMockScheduler(suite.T())

	// Initialize global factories before creating checkers
	assign.InitGlobalAssignPolicyFactory(suite.scheduler, suite.nodeMgr, suite.dist, suite.meta, suite.targetManager)
	balance.InitGlobalBalancerFactory(suite.scheduler, suite.nodeMgr, suite.dist, suite.targetManager)

	suite.controller = NewCheckerController(suite.meta, suite.dist, suite.targetManager, suite.nodeMgr, suite.scheduler, suite.broker)
}

func (suite *CheckerControllerSuite) TearDownTest() {
	suite.kv.Close()
	assign.ResetGlobalAssignPolicyFactoryForTest()
	balance.ResetGlobalBalancerFactoryForTest()
}

func (suite *CheckerControllerSuite) TestBasic() {
	ctx := context.Background()
	// set meta
	suite.meta.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	suite.meta.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	suite.meta.HandleNodeUp(ctx, 1)
	suite.meta.HandleNodeUp(ctx, 2)

	// set target
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel2",
		},
	}

	segments := []*datapb.SegmentInfo{
		{
			ID:            3,
			PartitionID:   1,
			InsertChannel: "test-insert-channel2",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	suite.targetManager.UpdateCollectionNextTarget(ctx, int64(1))

	// set dist
	suite.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		// View:    utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, map[int64]*meta.Segment{}),
		View: &meta.LeaderView{
			ID:      2,
			Channel: "test-insert-channel",
			Version: 1,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	})

	channelTaskCounter := atomic.NewInt64(0)
	segmentTaskCounter := atomic.NewInt64(0)

	// Track channel and segment tasks separately
	suite.scheduler.EXPECT().Add(mock.Anything).Run(func(t task.Task) {
		switch t.Actions()[0].Type() {
		case task.ActionTypeGrow:
			if _, ok := t.(*task.ChannelTask); ok {
				channelTaskCounter.Inc()
			} else if _, ok := t.(*task.SegmentTask); ok {
				segmentTaskCounter.Inc()
			}
		}
	}).Return(nil)
	suite.scheduler.EXPECT().GetSegmentTaskNum().Return(0).Maybe()
	suite.scheduler.EXPECT().GetChannelTaskNum().Return(0).Maybe()
	suite.scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	suite.scheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()

	suite.controller.Start()
	defer suite.controller.Stop()

	// expect channel task first
	suite.Eventually(func() bool {
		suite.controller.Check()
		return channelTaskCounter.Load() > 0
	}, 3*time.Second, 1*time.Millisecond)

	// until new channel has been subscribed
	suite.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel2",
		},
		Node:    1,
		Version: 1,
		View: &meta.LeaderView{
			ID:      1,
			Channel: "test-insert-channel2",
			Version: 1,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	})

	// expect segment task after channel has been subscribed
	suite.Eventually(func() bool {
		suite.controller.Check()
		return segmentTaskCounter.Load() > 0
	}, 3*time.Second, 1*time.Millisecond)
}

func TestCheckControllerSuite(t *testing.T) {
	suite.Run(t, new(CheckerControllerSuite))
}

type controllerTestChecker struct {
	Checker
	runCheck func(context.Context) []task.Task
}

func (checker *controllerTestChecker) Check(ctx context.Context) []task.Task {
	return checker.runCheck(ctx)
}

func waitControllerCheck(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestControllerMetadataCheckDoesNotBlockPlacement(t *testing.T) {
	for _, metadataType := range []utils.CheckerType{utils.IndexChecker, utils.LeaderChecker} {
		t.Run(metadataType.String(), func(t *testing.T) {
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var placementDone []<-chan struct{}
			var releaseOnce sync.Once
			scheduler := task.NewMockScheduler(t)
			scheduler.EXPECT().Add(mock.Anything).Return(nil).Times(3)
			controller := &CheckerController{
				scheduler: scheduler,
				checkers: map[utils.CheckerType]Checker{
					metadataType: &controllerTestChecker{runCheck: func(context.Context) []task.Task {
						close(started)
						<-release // For example, an IndexChecker metadata RPC is stalled.
						return nil
					}},
				},
			}
			placementTypes := []utils.CheckerType{utils.SegmentChecker, utils.ChannelChecker, utils.BalanceChecker}
			for _, checkType := range placementTypes {
				work, err := task.NewSegmentTask(context.Background(), time.Minute, checkType, 1, nil, 0,
					task.NewSegmentAction(2, task.ActionTypeGrow, "shard", 10))
				require.NoError(t, err)
				t.Cleanup(func() { work.Cancel(context.Canceled) })
				controller.checkers[checkType] = &controllerTestChecker{runCheck: func(context.Context) []task.Task {
					return []task.Task{work}
				}}
			}
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				waitControllerCheck(t, done, "metadata checker cleanup")
				for _, admitted := range placementDone {
					waitControllerCheck(t, admitted, "placement checker cleanup")
				}
			})
			go func() {
				defer close(done)
				controller.check(context.Background(), metadataType)
			}()
			waitControllerCheck(t, started, "metadata checker")
			for _, checkType := range placementTypes {
				admitted := make(chan struct{})
				placementDone = append(placementDone, admitted)
				go func() {
					defer close(admitted)
					controller.check(context.Background(), checkType)
				}()
				waitControllerCheck(t, admitted, checkType.String()+" admission while metadata is blocked")
			}
		})
	}
}

func TestControllerPlacementSerializesThroughAdmission(t *testing.T) {
	placementTypes := []utils.CheckerType{utils.SegmentChecker, utils.ChannelChecker, utils.BalanceChecker}
	for _, firstType := range placementTypes {
		for _, secondType := range placementTypes {
			if firstType == secondType {
				continue
			}
			t.Run(firstType.String()+"_before_"+secondType.String(), func(t *testing.T) {
				planningStarted, releasePlanning := make(chan struct{}), make(chan struct{})
				admissionStarted, releaseAdmission := make(chan struct{}), make(chan struct{})
				firstDone, secondDone := make(chan struct{}), make(chan struct{})
				secondStarted := make(chan struct{})
				committed := make(chan bool, 1)
				var planningOnce, admissionOnce sync.Once
				key := assign.PlanningLoadKey{NodeID: 2, CollectionID: 1, SegmentID: 10}
				ledger := assign.NewPlanningLedgerState(map[int64]assign.PlanningNode{
					2: {Ready: true, CapacityBytes: 1000},
				}, nil, nil, nil, 8)
				require.True(t, ledger.Reserve(assign.PlanningLoad{Key: key, From: -1, Rows: 10}))
				ctx := assign.WithPlanningLedger(context.Background(), ledger)
				work, err := task.NewSegmentTask(ctx, time.Minute, firstType, 1, nil, 0,
					task.NewSegmentAction(2, task.ActionTypeGrow, "shard", 10))
				require.NoError(t, err)
				t.Cleanup(func() { work.Cancel(context.Canceled) })
				scheduler := task.NewMockScheduler(t)
				scheduler.EXPECT().Add(work).Run(func(task.Task) {
					close(admissionStarted)
					<-releaseAdmission
				}).Return(nil).Once()
				controller := &CheckerController{
					scheduler: scheduler,
					checkers: map[utils.CheckerType]Checker{
						firstType: &controllerTestChecker{runCheck: func(context.Context) []task.Task {
							close(planningStarted)
							<-releasePlanning
							return []task.Task{work}
						}},
						secondType: &controllerTestChecker{runCheck: func(context.Context) []task.Task {
							close(secondStarted)
							// The preceding admission must have committed its ledger
							// reservation before the next placement checker starts.
							committed <- !ledger.Undo(key)
							return nil
						}},
					},
				}
				t.Cleanup(func() {
					planningOnce.Do(func() { close(releasePlanning) })
					admissionOnce.Do(func() { close(releaseAdmission) })
					waitControllerCheck(t, firstDone, "first checker cleanup")
					waitControllerCheck(t, secondDone, "second checker cleanup")
				})
				go func() {
					defer close(firstDone)
					controller.check(context.Background(), firstType)
				}()
				waitControllerCheck(t, planningStarted, "first checker planning")
				go func() {
					defer close(secondDone)
					controller.check(context.Background(), secondType)
				}()
				for _, phase := range []string{"planning", "admission"} {
					select {
					case <-secondStarted:
						t.Fatalf("second placement checker started during first checker %s", phase)
					case <-time.After(50 * time.Millisecond):
					}
					if phase == "planning" {
						planningOnce.Do(func() { close(releasePlanning) })
						waitControllerCheck(t, admissionStarted, "first checker admission")
					}
				}
				admissionOnce.Do(func() { close(releaseAdmission) })
				waitControllerCheck(t, firstDone, "first checker completion")
				waitControllerCheck(t, secondDone, "second checker completion")
				require.True(t, <-committed, "next planner must observe committed admission")
			})
		}
	}
}

func TestControllerAllCheckersWaitForInitialLoadReport(t *testing.T) {
	paramtable.Init()
	nodes := session.NewNodeManager()
	node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
	nodes.Add(node)
	nodes.BeginLoadReportSync(time.Hour)
	controller := &CheckerController{nodeMgr: nodes, checkers: make(map[utils.CheckerType]Checker)}
	checks := 0
	checkTypes := []utils.CheckerType{utils.SegmentChecker, utils.ChannelChecker, utils.BalanceChecker, utils.IndexChecker, utils.LeaderChecker}
	for _, checkType := range checkTypes {
		controller.checkers[checkType] = &controllerTestChecker{runCheck: func(context.Context) []task.Task {
			checks++
			return nil
		}}
	}
	checkAll := func() {
		for _, checkType := range checkTypes {
			controller.check(context.Background(), checkType)
		}
	}
	checkAll()
	require.Zero(t, checks)
	node.UpdateLoadReport(nil, true, true, time.Now())
	checkAll()
	require.Zero(t, checks, "a failed initial resource report must keep every checker gated")
	node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
	checkAll()
	require.Equal(t, len(checkTypes), checks)
}
