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

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestExplicitFlushPinsCheckpointAndReplaysAfterRestart(t *testing.T) {
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
	initial := utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(119), TimeTick: 120}
	tracker := messageack.NewTracker(initial, nil, nil)
	raw := message.NewManualFlushMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().
		WithTimeTick(200).WithLastConfirmed(walimplstest.NewTestMessageID(199)).IntoImmutableMessage(walimplstest.NewTestMessageID(200))
	dispatch := func(module *VChannelRecoveryModule, tracker *messageack.Tracker) {
		owner := tracker.Track(raw)
		retained := owner.Clone()
		summary.ObserveMessage(ctx, raw)
		require.True(t, module.ObserveMessage(ctx, retained))
		retained.Release()
		owner.Release()
	}
	dispatch(module, tracker)
	require.Empty(t, tasks, "sub-target API flush waits for L1 final commit")
	later := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
		WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).MustBuildMutable().
		WithTimeTick(300).WithLastConfirmed(walimplstest.NewTestMessageID(299)).IntoImmutableMessage(walimplstest.NewTestMessageID(300))
	tracker.Track(later).Release()
	require.Equal(t, uint64(120), tracker.CompletedPoint().TimeTick, "later completed messages cannot bypass unfinished Flush")
	require.Empty(t, module.ConsumeDirtySnapshots(), "pending intent needs no metadata field")
	// Crash before L0: checkpoint remains before Flush@200, so WAL replay
	// reconstructs the request. Summary still owns Delete@120 before checkpoint.
	restored, err := NewModule(config)
	require.NoError(t, err)
	recoveredTracker := messageack.NewTracker(tracker.CompletedPoint(), nil, nil)
	require.Greater(t, raw.TimeTick(), recoveredTracker.CompletedPoint().TimeTick)
	dispatch(restored, recoveredTracker)
	require.Empty(t, tasks)
	committed := segment.NewSegmentViewFromMetaWithConfig(newMaterializationBlockerMeta(1, 100, true), nil, restored.segmentViewConfig())
	restored.mu.Lock()
	restored.segments[1] = committed
	restored.mu.Unlock()
	restored.SegmentDataUpdated(1, committed)
	require.Len(t, tasks, 1, "final-commit notification wakes L0 without a new message")
	require.NoError(t, tasks[0].Execute(ctx))
	require.Equal(t, uint64(200), restored.l0Materializer.MaterializedTimeTick())
	require.Equal(t, uint64(200), recoveredTracker.CompletedPoint().TimeTick, "handle releases after L0 and dirty metadata")
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
