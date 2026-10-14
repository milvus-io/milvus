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
	"strconv"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func setStoppingDemandTestConfig(t *testing.T, key, value string) {
	t.Helper()
	params := paramtable.Get()
	previous, lookupErr := paramtable.GetBaseTable().Load(key)
	require.NoError(t, params.Save(key, value))
	t.Cleanup(func() {
		if lookupErr != nil {
			require.NoError(t, params.Reset(key))
		} else {
			require.NoError(t, params.Save(key, previous))
		}
	})
}

func TestStoppingDemandGatePreventsOrdinaryEpochFromDelayingDrain(t *testing.T) {
	for _, tc := range []struct {
		name         string
		maxChecks    int
		targetIsFull bool
	}{
		{name: "zero-admission", maxChecks: 100, targetIsFull: true},
		{name: "drain-beyond-check-budget", maxChecks: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paramtable.Init()
			t.Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
			cfg := &paramtable.Get().QueryCoordCfg
			for key, value := range map[string]string{
				cfg.Balancer.Key: meta.RowCountBasedBalancerName, cfg.StoppingBalanceAssignPolicy.Key: assign.PolicyTypeRoundRobin,
				cfg.AutoBalance.Key: "true", cfg.AutoBalanceChannel.Key: "false", cfg.EnableStoppingBalance.Key: "true",
				cfg.BalanceSegmentBatchSize.Key: "1", cfg.BalanceChannelBatchSize.Key: "1",
				cfg.BalanceCheckCollectionMaxCount.Key: strconv.Itoa(tc.maxChecks), cfg.EnableBalanceOnMultipleCollections.Key: "true",
				cfg.BalanceTriggerOrder.Key: "ByRowCount",
			} {
				setStoppingDemandTestConfig(t, key, value)
			}
			metadata := controllerRemoteLoadMeta(t,
				meta.NewReplica(&querypb.Replica{ID: 11, CollectionID: 1, ResourceGroup: "shared", Nodes: []int64{1}, RoNodes: []int64{4}}),
				meta.NewReplica(&querypb.Replica{ID: 21, CollectionID: 2, ResourceGroup: "shared", Nodes: []int64{2, 3}}),
				meta.NewReplica(&querypb.Replica{ID: 31, CollectionID: 3, ResourceGroup: "other", Nodes: []int64{5, 6}}),
			)
			nodes := session.NewNodeManager()
			for nodeID := int64(1); nodeID <= 6; nodeID++ {
				node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID})
				node.UpdateStats(session.WithMemCapacity(1))
				nodes.Add(node)
			}
			nodes.BeginLoadReportSync(0)
			for _, node := range nodes.GetAll() {
				usage := uint64(0)
				if node.ID() == 1 && tc.targetIsFull {
					usage = 1024 * 1024
				}
				node.UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{MemoryUsageBytes: usage}, true, true, time.Now())
			}
			dist := meta.NewDistributionManager(nodes)
			segment := func(id, collectionID, rows, bytes int64) *meta.Segment {
				return &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
					ID: id, CollectionID: collectionID, NumOfRows: rows, InsertChannel: "ch-" + strconv.FormatInt(collectionID, 10),
					Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: bytes, LogSize: bytes}}}},
				}}
			}
			drainingSegment := segment(101, 1, 10, 1024*1024)
			dist.SegmentDistManager.Update(4, drainingSegment)
			dist.SegmentDistManager.Update(2, segment(201, 2, 500, 1), segment(202, 2, 1000, 1))
			dist.SegmentDistManager.Update(5, segment(301, 3, 100, 1), segment(302, 3, 200, 1))
			for nodeID, collectionID := range map[int64]int64{1: 1, 2: 2, 5: 3} {
				dist.ChannelDistManager.Update(nodeID, &meta.DmChannel{
					VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "ch-" + strconv.FormatInt(collectionID, 10)},
					View:         &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}},
				})
			}
			targets := meta.NewMockTargetManager(t)
			targets.EXPECT().CanSegmentBeMoved(mock.Anything, mock.Anything, mock.Anything).Return(true)
			targets.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true)
			targets.EXPECT().IsCurrentTargetReady(mock.Anything, mock.Anything).Return(true)
			targets.EXPECT().GetCollectionRowCount(mock.Anything, mock.Anything, meta.CurrentTargetFirst).
				RunAndReturn(func(_ context.Context, collectionID int64, _ int32) int64 {
					return map[int64]int64{1: 10, 2: 1500, 3: 300}[collectionID]
				})
			scheduler := task.NewMockScheduler(t)
			scheduler.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil))
			scheduler.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0)
			var accepted []task.Task
			scheduler.EXPECT().Add(mock.Anything).Run(func(work task.Task) { accepted = append(accepted, work) }).Return(nil)
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
			tick := func() {
				checker.autoBalanceTs = time.Time{}
				checker.Check(WithRecoveryBlockedRGs(context.Background(), nil))
			}

			tick()
			require.Len(t, accepted, 1)
			require.EqualValues(t, 3, accepted[0].CollectionID(), "only the other RG may start ordinary balance while draining is pending")
			require.Equal(t, commonpb.LoadPriority_LOW, accepted[0].(*task.SegmentTask).LoadPriority())
			if tc.targetIsFull {
				nodes.Get(1).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, false, time.Now())
			} else {
				// The old stopping queue next checks collection 3 before reaching
				// collection 1. Its pending drain must be protected throughout.
				tick()
				require.Len(t, accepted, 1)
			}
			tick()
			require.Len(t, accepted, 2)
			require.EqualValues(t, 1, accepted[1].CollectionID(), "stopping must start without waiting for the other RG's epoch")
			require.Equal(t, commonpb.LoadPriority_HIGH, accepted[1].(*task.SegmentTask).LoadPriority())

			// The RO member remains in metadata, but its actual distribution has
			// drained. It must no longer prevent ordinary balance in this RG.
			accepted[1].SetStatus(task.TaskStatusSucceeded)
			dist.SegmentDistManager.Update(4)
			dist.SegmentDistManager.Update(1, drainingSegment)
			for i := 0; i < 3 && len(accepted) < 3; i++ {
				tick()
			}
			require.Len(t, accepted, 3)
			require.EqualValues(t, 2, accepted[2].CollectionID())
			require.Equal(t, commonpb.LoadPriority_LOW, accepted[2].(*task.SegmentTask).LoadPriority())
		})
	}
}

func TestStoppingDemandGateChannelNodeModesAndDistributionScope(t *testing.T) {
	for _, tc := range []struct {
		name      string
		streaming bool
		legacy    bool
		foreign   bool
		wantBlock bool
	}{
		{name: "ordinary-RO-channel", wantBlock: true},
		{name: "streaming-SQ-RO-channel", streaming: true, wantBlock: true},
		{name: "streaming-old-RW-channel", streaming: true, legacy: true, wantBlock: true},
		{name: "form-old-RW-is-not-channel-RO", legacy: true},
		{name: "another-collection-on-RO-is-not-this-replica-demand", foreign: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newEpochQueueFixture(t, 1, 100, map[int64]int64{1: 1000, 2: 100},
				epochQueueReplica(11, 1, "shared"), epochQueueReplica(21, 2, "shared"))
			replica := &querypb.Replica{ID: 11, CollectionID: 1, ResourceGroup: "shared", Nodes: []int64{111, 112}}
			source := int64(113)
			if tc.legacy {
				source = 111
				fixture.checker.nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: source, Version: semver.MustParse("2.5.0")}))
			} else if tc.streaming {
				replica.RoSqNodes = []int64{source}
			} else {
				replica.RoNodes = []int64{source}
			}
			require.NoError(t, fixture.checker.meta.Put(context.Background(), meta.NewReplica(replica)))
			collectionID := int64(1)
			if tc.foreign {
				collectionID = 99
			}
			fixture.checker.dist.ChannelDistManager.Update(source, &meta.DmChannel{
				VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "draining"},
				View:         &meta.LeaderView{Status: &querypb.LeaderViewStatus{Serviceable: true}},
			})
			setStoppingDemandTestConfig(t, paramtable.Get().QueryCoordCfg.EnableStoppingBalance.Key, "true")
			streaming := mockey.Mock(streamingutil.UseStreamingQueryNodeAsDelegator).Return(tc.streaming).Build()
			t.Cleanup(func() { streaming.UnPatch() })
			stopPlan := mockey.Mock((*balance.StoppingBalancer).BalanceReplica).Return(nil, nil).Build()
			t.Cleanup(func() { stopPlan.UnPatch() })
			stopFactory := mockey.Mock((*balance.BalancerFactory).GetStoppingBalancer).Return(&balance.StoppingBalancer{}).Build()
			t.Cleanup(func() { stopFactory.UnPatch() })
			tickBalanceWithRecovery(fixture, nil)
			if tc.wantBlock {
				require.Empty(t, fixture.accepted)
			} else {
				require.Len(t, fixture.accepted, 1)
			}
		})
	}
}

func TestStoppingDemandGatePreservesDirectPendingLoadGuard(t *testing.T) {
	t.Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	fixture := newEpochQueueFixture(t, 1, 100, map[int64]int64{1: 1000, 2: 100},
		epochQueueReplica(11, 1, "draining"), epochQueueReplica(21, 2, "loading"))
	require.NoError(t, fixture.checker.meta.Put(context.Background(), meta.NewReplica(&querypb.Replica{
		ID: 11, CollectionID: 1, ResourceGroup: "draining", Nodes: []int64{111, 112}, RoNodes: []int64{113},
	})))
	fixture.checker.dist.SegmentDistManager.Update(113, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, CollectionID: 1}})
	fixture.checker.scheduler = &pendingGateScheduler{Scheduler: fixture.checker.scheduler, pending: map[int64][]*querypb.LoadingSegmentInfo{
		212: {{CollectionId: 2, SegmentId: 8, NumRows: 1}},
	}}
	setStoppingDemandTestConfig(t, paramtable.Get().QueryCoordCfg.EnableStoppingBalance.Key, "true")
	stopPlan := mockey.Mock((*balance.StoppingBalancer).BalanceReplica).Return(nil, nil).Build()
	t.Cleanup(func() { stopPlan.UnPatch() })
	stopFactory := mockey.Mock((*balance.BalancerFactory).GetStoppingBalancer).Return(&balance.StoppingBalancer{}).Build()
	t.Cleanup(func() { stopFactory.UnPatch() })
	fixture.tick() // No explicit recovery context: the older load guard still applies.
	require.Empty(t, fixture.accepted)
}
