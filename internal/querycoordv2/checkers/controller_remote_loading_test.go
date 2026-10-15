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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func controllerRemoteLoadMeta(t *testing.T, replicas ...*meta.Replica) *meta.Meta {
	t.Helper()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil)
	metadata := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(nil),
		ReplicaManager:    meta.NewReplicaManager(nil, catalog),
	}
	for _, replica := range replicas {
		require.NoError(t, metadata.PutCollectionWithoutSave(context.Background(), &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: replica.GetCollectionID(), Status: querypb.LoadStatus_Loaded},
		}))
		require.NoError(t, metadata.Put(context.Background(), replica))
	}
	return metadata
}

func TestControllerRemoteLoadingGatePreventsRestartOvermove(t *testing.T) {
	for _, policy := range []string{meta.ScoreBasedBalancerName, meta.RowCountBasedBalancerName} {
		t.Run(policy, func(t *testing.T) {
			paramtable.Init()
			t.Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
			cfg := &paramtable.Get().QueryCoordCfg
			for key, value := range map[string]string{
				cfg.Balancer.Key: policy, cfg.AutoBalance.Key: "true", cfg.AutoBalanceChannel.Key: "false",
				cfg.EnableStoppingBalance.Key: "false", cfg.BalanceSegmentBatchSize.Key: "10",
				cfg.BalanceChannelBatchSize.Key: "1", cfg.BalanceCheckCollectionMaxCount.Key: "100",
				cfg.BalanceMaxPendingLoads.Key: "10", cfg.GlobalRowCountFactor.Key: "1",
				cfg.DelegatorMemoryOverloadFactor.Key: "0", cfg.ScoreUnbalanceTolerationFactor.Key: "0.05",
				cfg.ReverseUnbalanceTolerationFactor.Key: "1.3",
			} {
				previous := paramtable.GetBaseTable().Get(key)
				require.NoError(t, paramtable.Get().Save(key, value))
				t.Cleanup(func() {
					if previous == "" {
						paramtable.Get().Reset(key)
					} else {
						paramtable.Get().Save(key, previous)
					}
				})
			}
			replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, Nodes: []int64{1, 2}, ResourceGroup: "rg"})
			metadata := controllerRemoteLoadMeta(t, replica)
			nodes := session.NewNodeManager()
			for _, id := range []int64{1, 2} {
				nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id}))
			}
			nodes.BeginLoadReportSync(0)
			nodes.Get(1).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
			nodes.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{
				LoadingSegments: []*querypb.LoadingSegmentInfo{{CollectionId: 1, SegmentId: 3, NumRows: 500}},
			}, true, true, time.Now())
			dist := meta.NewDistributionManager(nodes)
			var segments []*meta.Segment
			for i, rows := range []int64{200, 300, 500} {
				segments = append(segments, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
					ID: int64(i + 1), CollectionID: 1, NumOfRows: rows, InsertChannel: "ch",
				}})
			}
			dist.SegmentDistManager.Update(1, segments...)
			dist.ChannelDistManager.Update(1, &meta.DmChannel{
				VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "ch"},
				View:         &meta.LeaderView{ID: 1, CollectionID: 1, Channel: "ch", Status: &querypb.LeaderViewStatus{Serviceable: true}},
			})
			targets := meta.NewMockTargetManager(t)
			targets.EXPECT().CanSegmentBeMoved(mock.Anything, int64(1), mock.Anything).Return(true)
			targets.EXPECT().IsNextTargetExist(mock.Anything, int64(1)).Return(true).Maybe()
			targets.EXPECT().IsCurrentTargetReady(mock.Anything, int64(1)).Return(true).Maybe()
			targets.EXPECT().GetCollectionRowCount(mock.Anything, int64(1), meta.CurrentTargetFirst).Return(int64(1000)).Maybe()
			scheduler := task.NewMockScheduler(t)
			scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil))
			scheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0)
			var accepted []task.Task
			scheduler.EXPECT().Add(mock.Anything).Run(func(work task.Task) { accepted = append(accepted, work) }).Return(nil).Maybe()
			t.Cleanup(func() {
				for _, work := range accepted {
					work.Cancel(nil)
				}
			})
			assign.ResetGlobalAssignPolicyFactoryForTest()
			assign.InitGlobalAssignPolicyFactory(scheduler, nodes, dist, metadata, targets)
			t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)
			balance.ResetGlobalBalancerFactoryForTest()
			balance.InitGlobalBalancerFactory(scheduler, nodes, dist, targets)
			t.Cleanup(balance.ResetGlobalBalancerFactoryForTest)
			checker := NewBalanceChecker(metadata, dist, targets, nodes, scheduler)

			// A restart lost the old Move's source delta and epoch. Even with a
			// real ledger, the real policies see A=1000/B=500 and move more data.
			ledger := assign.NewPlanningLedger(nodes, dist, scheduler, 10)
			plans, _ := balance.GetGlobalBalancerFactory().GetBalancer().BalanceReplica(assign.WithPlanningLedger(context.Background(), ledger), replica)
			var extraRows int64
			for _, plan := range plans {
				require.EqualValues(t, 1, plan.From)
				require.EqualValues(t, 2, plan.To)
				extraRows += plan.Segment.GetNumOfRows()
			}
			if policy == meta.ScoreBasedBalancerName {
				require.EqualValues(t, 300, extraRows)
			} else {
				require.EqualValues(t, 500, extraRows)
			}
			recoveryChecks := 0
			noGap := func(context.Context) ([]task.Task, map[string]struct{}) {
				recoveryChecks++
				return nil, nil // All target segments are still present on A.
			}
			controller := &CheckerController{meta: metadata, nodeMgr: nodes, scheduler: scheduler, checkers: map[utils.CheckerType]Checker{
				utils.ChannelChecker: &controllerRecoveryTestChecker{check: noGap},
				utils.SegmentChecker: &controllerRecoveryTestChecker{check: noGap},
				utils.BalanceChecker: checker,
			}}
			require.True(t, checker.IsActive())
			require.True(t, checker.readyToCheck(context.Background(), 1))
			require.Equal(t, querypb.LoadStatus_Loaded, metadata.GetCollection(context.Background(), 1).GetStatus())
			require.True(t, targets.IsCurrentTargetReady(context.Background(), 1))
			channels := dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(1))
			require.NotEmpty(t, channels)
			for _, channel := range channels {
				require.True(t, channel.IsServiceable())
			}
			require.Zero(t, checker.normalBalanceQueue.Len(), "preserve the constructor's empty queue for the first check")
			config := checker.loadBalanceConfig()
			require.Positive(t, config.channelBatchSize)
			require.Positive(t, config.maxCheckCollectionCount)
			controller.checkPlacement(context.Background(), true)
			require.Equal(t, 2, recoveryChecks, "remote work must not stop recovery checks")
			require.Empty(t, accepted, "the ordinary RG gate must prevent additional Moves after restart")
			require.Empty(t, checker.epochs)
			require.Equal(t, 1, checker.normalBalanceQueue.Len(), "remote work must defer the collection")

			// If the old load fails, a fresh empty report permits a new epoch.
			nodes.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, false, time.Now())
			for _, node := range nodes.GetAll() {
				require.True(t, node.LoadAdmissionReady())
				require.Empty(t, node.LoadResourceReport().GetLoadingSegments())
			}
			checker.autoBalanceTs = time.Time{}
			controller.checkPlacement(context.Background(), true)
			require.NotEmpty(t, accepted)
		})
	}
}

func TestControllerRemoteLoadingGateFreshnessAndReplicaMembership(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	metadata := controllerRemoteLoadMeta(t,
		meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, ResourceGroup: "old", RoNodes: []int64{1}}),
		meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 2, ResourceGroup: "new", Nodes: []int64{1}}),
		meta.NewReplica(&querypb.Replica{ID: 3, CollectionID: 3, ResourceGroup: "other", Nodes: []int64{2}}),
		meta.NewReplica(&querypb.Replica{ID: 4, CollectionID: 4, ResourceGroup: "sq", RwSqNodes: []int64{1}}),
	)
	for _, state := range []string{"loading", "zero-row-loading", "uninitialized", "stale-empty", "sample-failed", "fresh-empty", "legacy", "removed"} {
		t.Run(state, func(t *testing.T) {
			nodes := session.NewNodeManager()
			node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
			nodes.Add(node)
			nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
			nodes.BeginLoadReportSync(0) // The bounded global startup wait has elapsed.
			nodes.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
			switch state {
			case "loading", "zero-row-loading":
				rows := int64(10)
				if state == "zero-row-loading" {
					rows = 0
				}
				node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{LoadingSegments: []*querypb.LoadingSegmentInfo{
					{CollectionId: 99, SegmentId: 7, NumRows: rows},
				}}, true, true, time.Now())
			case "stale-empty":
				node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now().Add(-time.Hour))
			case "sample-failed":
				node.UpdateLoadReport(nil, true, true, time.Now())
			case "fresh-empty":
				node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
			case "legacy":
				node.UpdateLoadReport(nil, false, true, time.Now())
			case "removed":
				nodes.Remove(1)
			}
			checker := &BalanceChecker{}
			controller := &CheckerController{meta: metadata, nodeMgr: nodes, checkers: map[utils.CheckerType]Checker{
				utils.BalanceChecker: &controllerTestChecker{runCheck: func(ctx context.Context) []task.Task {
					blocked := ctx.Value(recoveryBlockedRGsKey{}).(map[string]struct{})
					if state == "fresh-empty" || state == "legacy" || state == "removed" {
						require.Empty(t, blocked)
					} else {
						require.Equal(t, map[string]struct{}{"old": {}, "new": {}, "sq": {}}, blocked)
					}
					for _, rid := range []int64{1, 2, 3, 4} {
						replica := metadata.Get(ctx, rid)
						require.False(t, checker.planningBlocks(ctx, replica, true), "stopping evacuation ignores the remote gate")
						if rid == 3 {
							require.False(t, checker.planningBlocks(ctx, replica, false), "another RG remains eligible")
						}
					}
					return nil
				}},
			}}
			controller.checkPlacement(ctx, true)
		})
	}
}
