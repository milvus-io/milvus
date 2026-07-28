package vchannel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestVChannelRecoveryModuleObservesOnlyItsVChannel(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	require.NotNil(t, module.vchannelView)
	require.NotNil(t, module.transformLog)
	assert.Empty(t, module.segments)
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(ctx, newTestDeleteMessage(t, "v2", 10))
	assert.Nil(t, result.Meta)
	assert.Nil(t, result.Data)
	assert.Empty(t, module.ConsumeDirtySnapshots())

	result = module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))
	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestVChannelRecoveryModuleRecoveryBarrierFlushesOwnedTransformLog(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	result := module.ObserveMessage(ctx, newTestRecoveryBarrierMessage(t, 30))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(0), result.Data.TimeTick())
}

func TestVChannelRecoveryModuleEmptyRecoveryBarrierDoesNotDirtyTransformLog(t *testing.T) {
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(context.Background(), newTestRecoveryBarrierMessage(t, 30))

	require.NotNil(t, result.Data)
	assert.Equal(t, uint64(30), result.Data.TimeTick())
	for _, snapshot := range module.ConsumeDirtySnapshots() {
		assert.NotEqual(t, moduleapi.ModuleNameTransformLog, snapshot.ModuleName())
	}
}

func TestVChannelRecoveryModuleReturnsOwnedDataFrontier(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(ctx, newTestDeleteMessage(t, "v1", 20))

	frontier := module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v1",
	})

	require.NotNil(t, frontier)
	assert.Equal(t, uint64(0), frontier.TimeTick())
	assert.Nil(t, module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v2",
	}))
}

func TestVChannelRecoveryModuleRuntimeCreatedSegmentInheritsMetaAndData(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingScheduler{}
	module := newTestModule(t, "p1", "v1")
	module.runtime.Scheduler = scheduler
	module.SwitchIntoMetaAndData()

	result := module.ObserveMessage(ctx, newTestCreateSegmentMessage(t, "v1", 10, 20))

	require.NotNil(t, result.Data)
	require.Len(t, scheduler.tasks, 1)
	require.NotNil(t, module.segments[10])

	result = module.ObserveMessage(ctx, newTestManualFlushMessage(t, "v1", 30))

	require.NotNil(t, result.Data)
	assert.Len(t, scheduler.tasks, 2)
}

func TestVChannelRecoveryModuleRefreshesFrontierAfterTransformSnapshotPersisted(t *testing.T) {
	var frontierUpdates atomic.Int32
	module, err := NewModule(ModuleConfig{
		PChannel: "p1",
		VChannel: "v1",
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
			},
		},
		TransformLogMeta: &streamingpb.VChannelTransformLogMeta{},
		OnFrontierUpdated: func() {
			frontierUpdates.Add(1)
		},
	})
	require.NoError(t, err)
	before := frontierUpdates.Load()
	module.markTransformSnapshotPersisted(module.transformLog.SnapshotMeta())
	assert.Greater(t, frontierUpdates.Load(), before)
}

func TestVChannelRecoveryModuleConsumesOnlyDirtySegments(t *testing.T) {
	module := newTestModule(t, "p1", "v1")
	module.runtime.Scheduler = &recordingScheduler{}
	module.SwitchIntoMetaAndData()
	module.ObserveMessage(context.Background(), newTestCreateSegmentMessage(t, "v1", 10, 20))
	module.ObserveMessage(context.Background(), newTestCreateSegmentMessage(t, "v1", 20, 21))

	for _, snapshot := range module.ConsumeDirtySnapshots() {
		snapshot.MarkPersisted()
	}
	assert.Empty(t, module.ConsumeDirtySnapshots())

	module.ObserveMessage(context.Background(), newTestCreateSegmentMessage(t, "v1", 30, 22))
	segmentIDs := make([]int64, 0)
	for _, snapshot := range module.ConsumeDirtySnapshots() {
		if snapshot.ModuleName() == moduleapi.ModuleNameSegment {
			segmentIDs = append(segmentIDs, snapshot.Key().SegmentID)
		}
	}
	assert.Equal(t, []int64{30}, segmentIDs)
}

func TestVChannelRecoveryModuleConcurrentObserveAndSnapshot(t *testing.T) {
	ctx := context.Background()
	module := newTestModule(t, "p1", "v1")
	module.runtime.Scheduler = &recordingScheduler{}
	module.SwitchIntoMetaAndData()

	const segmentCount = 500
	messages := make([]message.ImmutableMessage, 0, segmentCount)
	for i := 0; i < segmentCount; i++ {
		messages = append(messages, newTestCreateSegmentMessage(t, "v1", int64(i+1), uint64(i+10)))
	}
	frontier := module.DataFrontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     moduleapi.DataProgressDurable,
		VChannel: "v1",
	})
	require.NotNil(t, frontier)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, msg := range messages {
			module.ObserveMessage(ctx, msg)
		}
	}()
	go func() {
		defer wg.Done()
		for range messages {
			module.ConsumeDirtySnapshots()
			frontier.TimeTick()
			module.IsActive()
		}
	}()
	wg.Wait()

	assert.Len(t, module.segments, segmentCount)
}

func TestRecoveredDurableSegmentRetriesMissingFinalCommit(t *testing.T) {
	for name, state := range map[string]streamingpb.SegmentAssignmentState{
		"flushed":    streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		"tombstoned": streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			taskScheduler := &recordingScheduler{}
			lifecycle := &recordingSegmentLifecycle{}
			module := newTestModule(t, "p1", "v1")
			module.runtime.Scheduler = taskScheduler
			module.segmentLifecycle = lifecycle

			meta := newTestGrowingSegmentMeta(10, 10)
			meta.State = state
			meta.CheckpointTimeTick = 30
			meta.DataCheckpointTimeTick = 30
			if state == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED {
				meta.TombstoneTimeTick = 30
			}
			var view *segment.SegmentView
			view = segment.NewSegmentViewFromMetaWithOptions(
				meta,
				&schemapb.CollectionSchema{Name: "c100"},
				module.segmentOptions(10, func() *segment.SegmentView { return view })...,
			)
			module.segments = map[int64]*segment.SegmentView{10: view}

			module.SwitchIntoMetaAndData()

			require.Len(t, taskScheduler.tasks, 1)
			frontier := module.DataFrontier(moduleapi.Scope{
				Type:     moduleapi.ScopeVChannel,
				Kind:     moduleapi.DataProgressDurable,
				VChannel: "v1",
			})
			require.NotNil(t, frontier)
			assert.Equal(t, uint64(29), frontier.TimeTick())
			require.NoError(t, taskScheduler.tasks[0].Execute(ctx))

			assert.Equal(t, []int64{10}, lifecycle.committedSegmentIDs)
			assert.NotNil(t, view.AssignmentMeta().GetSealedAtDataVersion())
		})
	}
}

func TestManualFlushDeduplicatesPendingSegmentFinalCommit(t *testing.T) {
	ctx := context.Background()
	taskScheduler := &recordingScheduler{}
	lifecycle := &recordingSegmentLifecycle{}
	module := newTestModule(t, "p1", "v1")
	module.runtime.Scheduler = taskScheduler
	module.segmentLifecycle = lifecycle
	newSegment := func(segmentID int64, createTimeTick uint64) *segment.SegmentView {
		var view *segment.SegmentView
		view = segment.NewSegmentViewFromMetaWithOptions(
			newTestGrowingSegmentMeta(segmentID, createTimeTick),
			&schemapb.CollectionSchema{Name: "c100"},
			module.segmentOptions(segmentID, func() *segment.SegmentView { return view })...,
		)
		return view
	}
	module.segments = map[int64]*segment.SegmentView{
		10: newSegment(10, 10),
		20: newSegment(20, 35),
	}
	module.SwitchIntoMetaAndData()

	first := module.ObserveMessage(ctx, newTestManualFlushMessage(t, "v1", 30))
	second := module.ObserveMessage(ctx, newTestManualFlushMessage(t, "v1", 40))

	require.NotNil(t, first.Data)
	require.NotNil(t, second.Data)
	require.Len(t, taskScheduler.tasks, 2)
	for _, task := range taskScheduler.tasks {
		require.NoError(t, task.Execute(ctx))
	}

	assert.Equal(t, []int64{10, 20}, lifecycle.committedSegmentIDs)
	assert.Equal(t, int64(1), module.segments[10].AssignmentMeta().GetSealedAtDataVersion().GetStreamingVersion())
	assert.Equal(t, int64(2), module.segments[20].AssignmentMeta().GetSealedAtDataVersion().GetStreamingVersion())
}

func newTestModule(t *testing.T, pchannel string, vchannel string) *VChannelRecoveryModule {
	t.Helper()
	module, err := NewModule(ModuleConfig{
		PChannel: pchannel,
		VChannel: vchannel,
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel:           vchannel,
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 1,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 10,
						State:       streamingpb.PartitionState_PARTITION_STATE_NORMAL,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema: &schemapb.CollectionSchema{Name: "c100"},
						State:  streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
		TransformLogMeta: &streamingpb.VChannelTransformLogMeta{},
		Runtime:          moduleapi.Runtime{},
	})
	require.NoError(t, err)
	return module
}

func newTestCreateSegmentMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId:   100,
			PartitionId:    10,
			SegmentId:      segmentID,
			StorageVersion: 1,
			Level:          datapb.SegmentLevel_L1,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestManualFlushMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: 100,
			SegmentIds:   []int64{10},
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestDeleteMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.DeleteRequest{}).
		WithVChannel(vchannel).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

type recordingScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return taskHandle{}
}

type taskHandle struct{}

func (taskHandle) Cancel() {}

func (taskHandle) Wait(context.Context) error { return nil }

type recordingSegmentLifecycle struct {
	committedSegmentIDs []int64
}

func (l *recordingSegmentLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return nil
}

func (l *recordingSegmentLifecycle) CommitL1Segment(_ context.Context, meta *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	l.committedSegmentIDs = append(l.committedSegmentIDs, meta.GetSegmentId())
	return &viewpb.DataVersion{StreamingVersion: int64(len(l.committedSegmentIDs))}, nil
}

func newTestGrowingSegmentMeta(segmentID int64, createTimeTick uint64) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		CollectionId:           100,
		PartitionId:            10,
		SegmentId:              segmentID,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick:     createTimeTick,
		DataCheckpointTimeTick: createTimeTick,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		Stat: &streamingpb.SegmentAssignmentStat{
			CreateSegmentTimeTick: createTimeTick,
			Level:                 datapb.SegmentLevel_L1,
		},
	}
}

func newTestRecoveryBarrierMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}
