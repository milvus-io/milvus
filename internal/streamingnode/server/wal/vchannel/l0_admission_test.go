package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFlushJoinsIndependentL1AndL0Completion(t *testing.T) {
	for _, l0First := range []bool{false, true} {
		t.Run(map[bool]string{false: "L1 first", true: "L0 first"}[l0First], func(t *testing.T) {
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			var tasks []nodescheduler.Task
			patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle { tasks = append(tasks, task); return nil }).Build()
			defer patch.UnPatch()
			lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
			commit := mockey.Mock(mockey.GetMethod(lifecycle, "CommitL1Segment")).Return(nil).Build()
			defer commit.UnPatch()
			meta := newMaterializationBlockerMeta(1, 100, false)
			meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
			module, err := NewModule(ModuleConfig{
				PChannel: "p1", VChannel: "v1", VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1"},
				Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1: meta}, Runtime: moduleapi.Runtime{Scheduler: scheduler}, SegmentLifecycle: lifecycle,
			})
			require.NoError(t, err)
			raw := message.NewManualFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(200).WithLastConfirmed(walimplstest.NewTestMessageID(199)).IntoImmutableMessage(walimplstest.NewTestMessageID(200))
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			owner := tracker.Track(raw)
			retained := owner.Clone()
			module.ObserveMessage(context.Background(), retained)
			retained.Release()
			owner.Release()
			require.Len(t, tasks, 2)
			first, second := 0, 1
			if l0First {
				first, second = 1, 0
			}
			require.NoError(t, tasks[first].Execute(context.Background()))
			require.Zero(t, tracker.CompletedPoint().TimeTick)
			require.NoError(t, tasks[second].Execute(context.Background()))
			require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
			persistDirtySnapshots(module)
			require.Equal(t, uint64(200), module.vchannelView.PersistedMaterializedTimeTick())
		})
	}
}

func TestManagerFlushesIdleDeletesWhileL1IsGrowing(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.SyncPeriod.Key, "0"))
	defer params.Reset(params.DataNodeCfg.SyncPeriod.Key)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	tasks := make(chan nodescheduler.Task, 2)
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle { tasks <- task; return nil }).Build()
	defer patch.UnPatch()
	writer := mockey.Mock((*l0materializer.SyncMaterializer).Materialize).Return(nil).Build()
	defer writer.UnPatch()
	meta := newMaterializationBlockerMeta(1, 50, false)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": {Vchannel: "v1"}}, Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1: meta}, Runtime: moduleapi.Runtime{Scheduler: scheduler}, L0Materializer: &l0materializer.SyncMaterializer{}})
	require.NoError(t, err)
	defer manager.Close()
	module := manager.Module("v1")
	observeVChannelDelete(t, module, "v1", 100)
	require.Empty(t, tasks)
	manager.Start()
	select {
	case task := <-tasks:
		require.NoError(t, task.Execute(context.Background()))
	case <-time.After(5 * time.Second):
		t.Fatal("idle Delete did not materialize")
	}
	require.True(t, module.segments[1].IsGrowing())
	require.Equal(t, uint64(100), module.l0Materializer.MaterializedTimeTick())
	require.NotEmpty(t, manager.ConsumeDirtySnapshots())
	manager.Close()
}

func TestL0WaitsForGrowingRegistrationOnly(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	var tasks []nodescheduler.Task
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task) nodescheduler.TaskHandle { tasks = append(tasks, task); return nil }).Build()
	defer patch.UnPatch()
	lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
	ensure := mockey.Mock(mockey.GetMethod(lifecycle, "EnsureGrowingSegment")).Return(nil).Build()
	defer ensure.UnPatch()
	writer := mockey.Mock((*l0materializer.SyncMaterializer).Materialize).Return(nil).Build()
	defer writer.UnPatch()
	module, err := NewModule(ModuleConfig{PChannel: "p1", VChannel: "v1", VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1"}, Runtime: moduleapi.Runtime{Scheduler: scheduler}, SegmentLifecycle: lifecycle, L0Materializer: &l0materializer.SyncMaterializer{}, L0MaterializeBytes: 1})
	require.NoError(t, err)
	raw := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 1, SegmentId: 1}).WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(50).WithLastConfirmed(walimplstest.NewTestMessageID(49)).IntoImmutableMessage(walimplstest.NewTestMessageID(50))
	module.segments[1] = segment.NewSegmentViewFromCreateSegmentMessageWithConfig(message.MustAsImmutableCreateSegmentMessageV2(raw), nil, module.segmentViewConfig())
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	module.ObserveMessage(context.Background(), retained)
	retained.Release()
	owner.Release()
	observeVChannelDelete(t, module, "v1", 100)
	require.Len(t, tasks, 2)
	require.ErrorIs(t, tasks[1].Execute(context.Background()), nodescheduler.ErrDelay)
	require.NoError(t, tasks[0].Execute(context.Background()))
	require.True(t, module.segments[1].Registered())
	require.NoError(t, tasks[1].Execute(context.Background()))
	require.True(t, module.segments[1].IsGrowing())
	require.Equal(t, uint64(100), module.l0Materializer.MaterializedTimeTick())
}
