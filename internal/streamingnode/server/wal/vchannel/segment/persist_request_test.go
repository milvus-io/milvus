package segment

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type recordingSegmentScheduler struct {
	tasks []nodescheduler.Task
}

func (s *recordingSegmentScheduler) Submit(task nodescheduler.Task, opts ...nodescheduler.Option) nodescheduler.TaskHandle {
	s.tasks = append(s.tasks, task)
	return recordingSegmentTaskHandle{}
}

type recordingSegmentTaskHandle struct{}

func (recordingSegmentTaskHandle) Cancel() {}

func (recordingSegmentTaskHandle) Wait(context.Context) error { return nil }

func TestSegmentViewRequestPersistThrough(t *testing.T) {
	first := message.CreateTestInsertMessage(t, 100, 1, 20, walimplstest.NewTestMessageID(10)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	second := message.CreateTestInsertMessage(t, 100, 1, 21, walimplstest.NewTestMessageID(11)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(12))
	firstOwner := message.NewOwnedImmutableMessage(first, nil)
	secondOwner := message.NewOwnedImmutableMessage(second, nil)
	pending := writeOnlyInsertBuffer{}
	pending.appendMessage(firstOwner.Clone(), 1, 1)
	pending.appendMessage(secondOwner.Clone(), 1, 1)
	firstOwner.Release()
	secondOwner.Release()

	scheduler := &recordingSegmentScheduler{}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		pending,
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)

	assert.False(t, view.RequestPersistThrough(19))
	assert.Empty(t, scheduler.tasks)
	require.True(t, view.RequestPersistThrough(20))
	require.Len(t, scheduler.tasks, 1)
	task, ok := scheduler.tasks[0].(*flushL1BufferTask)
	require.True(t, ok)
	assert.Equal(t, uint64(21), task.timetick)

	// The first request moved the whole buffer into the scheduled task, so a
	// repeated request cannot enqueue duplicate work.
	assert.False(t, view.RequestPersistThrough(20))
	require.Len(t, scheduler.tasks, 1)

	view.mu.Lock()
	for _, chunk := range view.pendingFlushChunks {
		releaseMessages(chunk.retainedHandles())
	}
	view.pendingFlushChunks = nil
	view.mu.Unlock()
}

func TestPressurePromotesOnlyRequiredSerialPrefix(t *testing.T) {
	promoted := make(map[*moduleapi.DataTaskPriority]bool)
	patch := mockey.Mock((*moduleapi.DataTaskPriority).Promote).To(func(p *moduleapi.DataTaskPriority) { promoted[p] = true }).Build()
	defer patch.UnPatch()
	view := &SegmentView{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}}
	first := &ensureGrowingSegmentTask{segmentTaskBase: segmentTaskBase{segment: view}, timetick: 10}
	second := &flushL1BufferTask{segmentTaskBase: segmentTaskBase{segment: view}, timetick: 20}
	later := &commitL1SegmentTask{segmentTaskBase: segmentTaskBase{segment: view}, timetick: 30}
	view.pendingTasks = []segmentTask{first, second, later}
	view.pendingFlushChunks = []writeOnlyInsertBuffer{{fromTimeTick: 15, toTimeTick: 20}}
	view.maybeSubmitNextLocked()
	require.False(t, view.RequestPersistThroughUnderPressure(14))
	require.Empty(t, promoted)
	require.False(t, view.RequestPersistThroughUnderPressure(20), "already sealed data reuses the queue")
	require.True(t, promoted[first.taskPriority()])
	require.True(t, promoted[second.taskPriority()])
	require.False(t, promoted[later.taskPriority()])
	clear(promoted)
	view.RequestPersistThrough(30)
	require.Empty(t, promoted, "ordinary stall does not promote")
	view.pendingFinalCommit = later
	view.createSegmentTimeTick = 10
	view.RequestPersistThroughUnderPressure(20)
	require.True(t, promoted[later.taskPriority()], "L0 depends on earlier L1 final publication even beyond the requested boundary")
}
