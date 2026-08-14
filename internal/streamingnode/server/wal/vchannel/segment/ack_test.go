package segment

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestEnsureGrowingRetainsRefUntilLifecycleSucceeds(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	lifecycle := &failingSegmentLifecycle{err: errors.New("not ready")}
	msg, controller := newSegmentAckCreateMessage(t, 10)
	probe := controller.Clone()
	timetick := msg.TimeTick()
	view := NewSegmentViewFromCreateSegmentMessage(
		msg,
		nil,
		runtimeConfig{
			lifecycle:   lifecycle,
			metaAndData: true,
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)
	dispatch := controller.Clone()
	assert.True(t, view.ObserveCreateSegmentMessageV2(context.Background(), message.MustAsRetainedImmutableCreateSegmentMessageV2(dispatch)))
	dispatch.Release()
	controller.Release()
	require.Len(t, scheduler.tasks, 1)
	assert.NotPanics(t, func() { _ = probe.Message().TimeTick() })

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.NotPanics(t, func() { _ = probe.Message().TimeTick() })

	lifecycle.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	probe.Release()
	assert.Panics(t, func() { _ = controller.Message() })
	assert.Equal(t, timetick, view.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.True(t, view.HasDirty())
}

func TestInsertChunkReleasesEveryCoveredMessageRefAfterDurableMetadataUpdate(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	writer := &recordingPackWriter{}
	view := NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:     1,
			PartitionId:      10,
			SegmentId:        100,
			Vchannel:         "v1",
			State:            streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			PersistedStorage: &streamingpb.L1SegmentPersistedStorage{},
			Stat:             &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
		},
		0,
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			packWriter:  writer,
			metaAndData: true,
			flushPolicy: newWriteOnlyFlushPolicy(2, 0, 0),
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)
	_, firstAssignment, first := newSegmentAckInsertMessage(t, 10, 1)
	_, secondAssignment, second := newSegmentAckInsertMessage(t, 20, 2)
	firstProbe := first.Clone()
	secondProbe := second.Clone()

	firstDispatch := first.Clone()
	assert.True(t, view.ObserveInsertMessageV1(context.Background(), message.MustAsRetainedImmutableInsertMessageV1(firstDispatch), firstAssignment))
	firstDispatch.Release()
	first.Release()
	secondDispatch := second.Clone()
	assert.True(t, view.ObserveInsertMessageV1(context.Background(), message.MustAsRetainedImmutableInsertMessageV1(secondDispatch), secondAssignment))
	secondDispatch.Release()
	second.Release()
	require.Len(t, scheduler.tasks, 1)
	assert.NotPanics(t, func() { _ = firstProbe.Message().TimeTick() })
	assert.NotPanics(t, func() { _ = secondProbe.Message().TimeTick() })

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	assert.Equal(t, 1, writer.calls)
	assert.NotPanics(t, func() { _ = firstProbe.Message() })
	assert.NotPanics(t, func() { _ = secondProbe.Message() })
	firstProbe.Release()
	secondProbe.Release()
	assert.Panics(t, func() { _ = first.Message() })
	assert.Panics(t, func() { _ = second.Message() })
	assert.Equal(t, uint64(20), view.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.True(t, view.HasDirty())
}

func TestFinalCommitRetainsRefUntilLifecycleSucceeds(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	lifecycle := &failingSegmentLifecycle{err: errors.New("not ready")}
	view := newSegmentAckGrowingView(scheduler, lifecycle)
	_, controller := newSegmentAckDataMessage(t, 20)
	probe := controller.Clone()

	dispatch := controller.Clone()
	assert.True(t, view.Flush(context.Background(), dispatch))
	dispatch.Release()
	controller.Release()
	require.Len(t, scheduler.tasks, 1)
	assert.NotPanics(t, func() { _ = probe.Message().TimeTick() })

	err := scheduler.tasks[0].Execute(context.Background())
	assert.True(t, errors.Is(err, nodescheduler.ErrDelay))
	assert.NotPanics(t, func() { _ = probe.Message().TimeTick() })
	assert.Equal(t, uint64(1), view.AssignmentMeta().GetDataCheckpointTimeTick())

	lifecycle.err = nil
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	probe.Release()
	assert.Panics(t, func() { _ = controller.Message() })
	assert.Equal(t, 2, lifecycle.commitCalls)
	assert.Equal(t, uint64(20), view.AssignmentMeta().GetDataCheckpointTimeTick())
	assert.NotNil(t, view.AssignmentMeta().GetSealedAtDataVersion())
	assert.True(t, view.HasDirty())
}

func TestRepeatedFlushRefsSharePendingFinalCommit(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	lifecycle := &failingSegmentLifecycle{}
	view := newSegmentAckGrowingView(scheduler, lifecycle)
	_, first := newSegmentAckDataMessage(t, 20)
	_, second := newSegmentAckDataMessage(t, 30)
	firstProbe := first.Clone()
	secondProbe := second.Clone()

	firstDispatch := first.Clone()
	assert.True(t, view.Flush(context.Background(), firstDispatch))
	firstDispatch.Release()
	first.Release()
	secondDispatch := second.Clone()
	assert.False(t, view.Flush(context.Background(), secondDispatch))
	secondDispatch.Release()
	second.Release()
	require.Len(t, scheduler.tasks, 1)
	assert.NotPanics(t, func() { _ = firstProbe.Message().TimeTick() })
	assert.NotPanics(t, func() { _ = secondProbe.Message().TimeTick() })

	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	firstProbe.Release()
	secondProbe.Release()
	assert.Panics(t, func() { _ = first.Message() })
	assert.Panics(t, func() { _ = second.Message() })
	assert.Equal(t, 1, lifecycle.commitCalls)
}

func newSegmentAckGrowingView(scheduler *recordingSegmentScheduler, lifecycle *failingSegmentLifecycle) *SegmentView {
	return NewSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			CollectionId:           1,
			PartitionId:            10,
			SegmentId:              100,
			Vchannel:               "v1",
			State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick:     1,
			DataCheckpointTimeTick: 1,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
			Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 1},
		},
		1,
		1,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			lifecycle:   lifecycle,
			metaAndData: true,
			runtime:     moduleapi.Runtime{Scheduler: scheduler},
			owner:       &recordingSegmentViewOwner{},
		},
	)
}

type recordingSegmentScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingSegmentScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingSegmentTaskHandle{}
}

type recordingSegmentTaskHandle struct{}

func (recordingSegmentTaskHandle) Cancel()                    {}
func (recordingSegmentTaskHandle) Wait(context.Context) error { return nil }

type failingSegmentLifecycle struct {
	err         error
	commitCalls int
}

func (l *failingSegmentLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return l.err
}

func (l *failingSegmentLifecycle) CommitL1Segment(context.Context, *streamingpb.SegmentAssignmentMeta) (*viewpb.DataVersion, error) {
	l.commitCalls++
	return &viewpb.DataVersion{StreamingVersion: 1}, l.err
}

type recordingPackWriter struct {
	calls int
}

func (w *recordingPackWriter) FlushInsertBuffer(context.Context, *flushPack) (*flushResult, error) {
	w.calls++
	return &flushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{}}, nil
}

func newSegmentAckCreateMessage(
	t *testing.T,
	timetick uint64,
) (message.ImmutableCreateSegmentMessageV2, message.OwnedImmutableMessage) {
	t.Helper()
	mutable := message.NewCreateSegmentMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{
			CollectionId: 1,
			PartitionId:  10,
			SegmentId:    100,
			MaxRows:      1000,
		}).
		WithBody(&message.CreateSegmentMessageBody{}).
		MustBuildMutable()
	raw := mutable.
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	return message.MustAsImmutableCreateSegmentMessageV2(raw), owner
}

func newSegmentAckInsertMessage(
	t *testing.T,
	timetick uint64,
	messageID int64,
) (message.ImmutableInsertMessageV1, *messagespb.PartitionSegmentAssignment, message.OwnedImmutableMessage) {
	t.Helper()
	assignment := &messagespb.PartitionSegmentAssignment{
		PartitionId: 10,
		Rows:        1,
		BinarySize:  1,
		SegmentAssignment: &messagespb.SegmentAssignment{
			SegmentId: 100,
		},
	}
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions:   []*messagespb.PartitionSegmentAssignment{assignment},
		}).
		WithBody(&msgpb.InsertRequest{NumRows: 1}).
		BuildMutable()
	require.NoError(t, err)
	raw := mutable.
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(messageID - 1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(messageID))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	return message.MustAsImmutableInsertMessageV1(raw), assignment, owner
}

func newSegmentAckDataMessage(
	t *testing.T,
	timetick uint64,
) (message.ImmutableMessage, message.OwnedImmutableMessage) {
	t.Helper()
	raw := message.CreateTestTimeTickSyncMessage(t, 1, timetick, walimplstest.NewTestMessageID(int64(timetick-1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	return raw, owner
}
