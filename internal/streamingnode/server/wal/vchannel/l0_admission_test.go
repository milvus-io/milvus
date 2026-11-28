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
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFlushWaitsForL1BeforeL0Completion(t *testing.T) {
	for _, l0First := range []bool{false, true} {
		t.Run(map[bool]string{false: "L1 first", true: "L0 first"}[l0First], func(t *testing.T) {
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			var tasks []nodescheduler.Task
			patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task, _ ...nodescheduler.Option) nodescheduler.TaskHandle {
				tasks = append(tasks, task)
				return nil
			}).Build()
			defer patch.UnPatch()
			lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
			commit := mockey.Mock(mockey.GetMethod(lifecycle, "CommitL1SegmentWithPublication")).Return(&viewpb.DataVersion{StreamingVersion: 1}, false, nil).Build()
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
			if l0First {
				require.ErrorIs(t, tasks[first].Execute(context.Background()), nodescheduler.ErrDelay)
				require.Zero(t, tracker.CompletedPoint().TimeTick)
				require.NoError(t, tasks[second].Execute(context.Background()))
				require.NoError(t, tasks[first].Execute(context.Background()))
			} else {
				require.NoError(t, tasks[first].Execute(context.Background()))
				require.Zero(t, tracker.CompletedPoint().TimeTick)
				require.NoError(t, tasks[second].Execute(context.Background()))
			}
			require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
			persistDirtySnapshots(module)
			require.Equal(t, uint64(200), module.vchannelView.PersistedMaterializedTimeTick())
		})
	}
}

func TestTruncateAckDoesNotWaitForPendingFlushPublication(t *testing.T) {
	ctx := t.Context()
	scheduler := &recordingVChannelScheduler{}
	lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
	commit := mockey.Mock(mockey.GetMethod(lifecycle, "CommitL1SegmentWithPublication")).Return(&viewpb.DataVersion{StreamingVersion: 1}, true, nil).Build()
	defer commit.UnPatch()
	meta := newMaterializationBlockerMeta(1, 100, false)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", VChannelMeta: &streamingpb.VChannelMeta{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1: meta}, Runtime: moduleapi.Runtime{Scheduler: scheduler}, SegmentLifecycle: lifecycle,
	})
	require.NoError(t, err)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	observe := func(raw message.ImmutableMessage) {
		t.Helper()
		owner := tracker.Track(raw)
		retained := owner.Clone()
		require.True(t, module.ObserveMessage(ctx, retained))
		retained.Release()
		owner.Release()
	}
	flush := message.NewManualFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.ManualFlushMessageHeader{}).
		WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(200).
		WithLastConfirmed(walimplstest.NewTestMessageID(199)).IntoImmutableMessage(walimplstest.NewTestMessageID(200))
	observe(flush)
	require.Len(t, scheduler.tasks, 2, "L1 registration and L0 materialization")
	require.NoError(t, scheduler.tasks[0].Execute(ctx))
	require.Zero(t, tracker.CompletedPoint().TimeTick, "L0 still owns the Flush ACK")
	require.NoError(t, scheduler.tasks[1].Execute(ctx))
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Len(t, scheduler.tasks, 3, "query publication remains queued")
	truncate := message.NewTruncateCollectionMessageBuilderV2().WithVChannel("v1").WithHeader(&message.TruncateCollectionMessageHeader{}).
		WithBody(&message.TruncateCollectionMessageBody{}).MustBuildMutable().WithTimeTick(300).
		WithLastConfirmed(walimplstest.NewTestMessageID(299)).IntoImmutableMessage(walimplstest.NewTestMessageID(300))
	observe(truncate)
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Len(t, scheduler.tasks, 4, "Truncate only needs a new L0 boundary")
	require.NoError(t, scheduler.tasks[3].Execute(ctx))
	require.Equal(t, uint64(300), tracker.CompletedPoint().TimeTick, "Truncate can ACK before its callback retires the pending group")
	require.Nil(t, module.segments[1].AssignmentMeta().GetSealedAtDataVersion(), "ACK cannot prematurely publish query handoff")
}

func TestManagerDefersIdleDeletesWhileL1IsGrowing(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.SyncPeriod.Key, "0"))
	defer params.Reset(params.DataNodeCfg.SyncPeriod.Key)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	tasks := make(chan nodescheduler.Task, 2)
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task, _ ...nodescheduler.Option) nodescheduler.TaskHandle {
		tasks <- task
		return nil
	}).Build()
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
		require.ErrorIs(t, task.Execute(context.Background()), nodescheduler.ErrDelay)
	case <-time.After(5 * time.Second):
		t.Fatal("idle Delete did not materialize")
	}
	require.True(t, module.segments[1].IsGrowing())
	require.Zero(t, module.l0Materializer.MaterializedTimeTick())
	manager.Close()
}

func TestL0WaitsForFinalPublicationAfterGrowingRegistration(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	var tasks []nodescheduler.Task
	patch := mockey.Mock(mockey.GetMethod(scheduler, "Submit")).To(func(task nodescheduler.Task, _ ...nodescheduler.Option) nodescheduler.TaskHandle {
		tasks = append(tasks, task)
		return nil
	}).Build()
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
	require.ErrorIs(t, tasks[1].Execute(context.Background()), nodescheduler.ErrDelay)
	require.True(t, module.segments[1].IsGrowing())
	require.Zero(t, module.l0Materializer.MaterializedTimeTick())
	// Recovered final publication is sufficient even before the Segment's
	// tombstone is removed from the owner map.
	meta := newMaterializationBlockerMeta(1, 50, true)
	module.segments[1] = segment.NewSegmentViewFromMetaWithConfig(meta, nil, module.segmentViewConfig())
	require.NoError(t, tasks[1].Execute(context.Background()))
	require.Equal(t, uint64(100), module.l0Materializer.MaterializedTimeTick())
}
