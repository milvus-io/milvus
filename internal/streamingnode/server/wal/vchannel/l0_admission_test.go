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

package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestExplicitL0IntentSurvivesSnapshotAndRestart(t *testing.T) {
	ctx := context.Background()
	summary := walsummary.NewManager(walsummary.ManagerConfig{})
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	var tasks []nodescheduler.Task
	submit := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle { tasks = append(tasks, task); return nil }).Build()
	defer submit.UnPatch()
	writer := mockey.Mock((*l0materializer.SyncMaterializer).Materialize).Return(nil).Build()
	defer writer.UnPatch()
	// Segment flushing is tested by its own suite. Keep this dependency pending
	// to exercise captured intent, restart and final-commit notification.
	flush := mockey.Mock((*segment.SegmentView).Flush).Return(false).Build()
	defer flush.UnPatch()
	config := ModuleConfig{
		PChannel: "p1", VChannel: "v1",
		VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		Segments:     map[int64]*streamingpb.SegmentAssignmentMeta{1: newMaterializationBlockerMeta(1, 100, false)},
		Runtime:      moduleapi.Runtime{Scheduler: scheduler}, SummaryReader: summary,
		L0Materializer: &l0materializer.SyncMaterializer{},
	}
	module, err := NewModule(config)
	require.NoError(t, err)
	observeVChannelDelete(t, module, "v1", 120, summary)
	observeVChannelBarrier(t, module, "v1", 200, summary)
	require.Empty(t, tasks, "sub-target API flush waits for L1 final commit")
	snapshots := module.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpUpsertBase, snapshots[0].Op())
	saved := snapshots[0].Payload().(*streamingpb.VChannelMeta)
	require.Equal(t, uint64(200), saved.GetL0FlushTimeTick())
	// A newer request cannot mutate the in-flight snapshot or disappear when
	// that older snapshot is acknowledged.
	observeVChannelBarrier(t, module, "v1", 250, summary)
	require.Equal(t, uint64(200), saved.GetL0FlushTimeTick())
	snapshots[0].MarkPersisted()
	next := module.ConsumeDirtySnapshots()
	require.Len(t, next, 1)
	require.Equal(t, uint64(250), next[0].Payload().(*streamingpb.VChannelMeta).GetL0FlushTimeTick())
	// Simulate the crash after publishing the first checkpoint. Replay starts
	// after request@200, so RecoveryBarrier must wake the persisted intent.
	wire, err := proto.Marshal(saved)
	require.NoError(t, err)
	config.VChannelMeta = &streamingpb.VChannelMeta{}
	require.NoError(t, proto.Unmarshal(wire, config.VChannelMeta))
	restored, err := NewModule(config)
	require.NoError(t, err)
	raw := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
		WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable().
		WithTimeTick(300).WithLastConfirmed(walimplstest.NewTestMessageID(299)).IntoImmutableMessage(walimplstest.NewTestMessageID(300))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	summary.ObserveMessage(ctx, raw)
	require.True(t, restored.ObserveMessage(ctx, retained))
	retained.Release()
	owner.Release()
	require.Empty(t, tasks)
	committed := segment.NewSegmentViewFromMetaWithConfig(newMaterializationBlockerMeta(1, 100, true), nil, restored.segmentViewConfig())
	restored.mu.Lock()
	restored.segments[1] = committed
	restored.mu.Unlock()
	restored.SegmentDataUpdated(1, committed)
	require.Len(t, tasks, 1, "final-commit notification wakes L0 without a new message")
	require.NoError(t, tasks[0].Execute(ctx))
	require.Equal(t, uint64(200), restored.l0Materializer.MaterializedTimeTick())
	require.Zero(t, restored.FlushCheckpointTimeTick(), "output alone is not a persisted flush checkpoint")
	persistDirtySnapshots(restored)
	require.Equal(t, uint64(200), restored.vchannelView.PersistedMaterializedTimeTick())
}

func TestSummaryBacklogWiringDoesNotInventReplayCoverage(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	var tasks []nodescheduler.Task
	submit := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle { tasks = append(tasks, task); return nil }).Build()
	defer submit.UnPatch()
	writer := mockey.Mock((*l0materializer.SyncMaterializer).Materialize).Return(nil).Build()
	defer writer.UnPatch()
	var reported uint64
	report := mockey.Mock((*walsummary.Manager).ReportMaterialized).To(func(_ *walsummary.Manager, vc string, tt uint64) { require.Equal(t, "v1", vc); reported = tt }).Build()
	defer report.UnPatch()
	summary := walsummary.NewManager(walsummary.ManagerConfig{})
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p1", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": {Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL}},
		SummaryManager: summary, Runtime: moduleapi.Runtime{Scheduler: scheduler}, L0Materializer: &l0materializer.SyncMaterializer{},
	})
	require.NoError(t, err)
	defer manager.Close()
	manager.RequestMaterializationThrough("missing", 100)
	manager.RequestMaterializationThrough("v1", 200)
	require.Empty(t, tasks, "backlog ahead of ordered replay cannot extend W")
	module := manager.Module("v1")
	observeVChannelDelete(t, module, "v1", 100, summary)
	require.Len(t, tasks, 1)
	require.NoError(t, tasks[0].Execute(context.Background()))
	require.Equal(t, uint64(100), reported)
	require.NotEmpty(t, manager.ConsumeDirtySnapshots(), "completion callback tracks the dirty VChannel")
	module.mu.Lock()
	module.removed = true
	module.mu.Unlock()
	manager.RequestMaterializationThrough("v1", 300)
	require.Len(t, tasks, 1)
}
