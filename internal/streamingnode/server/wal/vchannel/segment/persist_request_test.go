package segment

import (
	"context"
	"testing"

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

func (s *recordingSegmentScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
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
