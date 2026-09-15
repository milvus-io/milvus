package segment

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// fakeSegmentTask runs a configurable fn result once executed, so the test can
// drive the serial submission and failure-propagation logic without a real
// flush/commit pipeline. executed is set inside the fn, so a task that is
// short-circuited by a segment failure never marks itself executed.
type fakeSegmentTask struct {
	segmentTaskBase
	executed bool
	failErr  error
}

func (t *fakeSegmentTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		t.executed = true
		return t.failErr
	})
}

// TestSegmentTaskSerialSubmissionAndTerminalFailurePropagation covers the
// serialization contract: only the head task is submitted, a finished task
// (success or terminal failure) leaves the queue and submits the next one, and
// a terminal failure marks the segment so later tasks fail fast instead of
// executing.
func TestSegmentTaskSerialSubmissionAndTerminalFailurePropagation(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)

	view.mu.Lock()
	first := &fakeSegmentTask{segmentTaskBase: view.newSegmentTaskBaseLocked("fake"), failErr: retry.Unrecoverable(errors.New("terminal boom"))}
	second := &fakeSegmentTask{segmentTaskBase: view.newSegmentTaskBaseLocked("fake")}
	view.pendingTasks = append(view.pendingTasks, first, second)
	view.maybeSubmitNextLocked()
	view.mu.Unlock()
	require.Len(t, scheduler.tasks, 1, "only the head task is submitted")
	assert.Same(t, first, scheduler.tasks[0])

	// The head finishes with a terminal failure: it leaves the queue, marks the
	// segment failed, and the next task is submitted.
	require.Error(t, first.Execute(context.Background()))
	require.NotNil(t, view.unrecoverableErr())

	view.mu.Lock()
	require.Len(t, view.pendingTasks, 1)
	assert.Same(t, second, view.pendingTasks[0])
	require.Len(t, scheduler.tasks, 2, "finishing the head submits the next task")
	view.mu.Unlock()

	// The successor fails fast with the propagated error and does not execute.
	// cockroachdb errors.Is is used because ErrDelay/unrecoverable rely on the
	// cockroachdb mark mechanism, which stdlib errors.Is does not recognize.
	ferr := view.unrecoverableErr()
	require.True(t, errors.Is(second.Execute(context.Background()), ferr))
	assert.False(t, second.executed, "a task after a terminal failure must not execute")

	view.mu.Lock()
	assert.Empty(t, view.pendingTasks)
	view.mu.Unlock()
}

// TestSegmentTaskSerialSubmissionAfterSuccess covers the happy path: finishing
// the head task without error submits the next one, and a repeated submit
// attempt is a no-op while the head is already submitted.
func TestSegmentTaskSerialSubmissionAfterSuccess(t *testing.T) {
	scheduler := &recordingSegmentScheduler{}
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: scheduler}},
	)

	view.mu.Lock()
	first := &fakeSegmentTask{segmentTaskBase: view.newSegmentTaskBaseLocked("fake")}
	second := &fakeSegmentTask{segmentTaskBase: view.newSegmentTaskBaseLocked("fake")}
	view.pendingTasks = append(view.pendingTasks, first, second)
	view.maybeSubmitNextLocked()
	view.mu.Unlock()
	require.Len(t, scheduler.tasks, 1, "only the head task is submitted")
	assert.Same(t, first, scheduler.tasks[0])

	require.NoError(t, first.Execute(context.Background()))
	view.mu.Lock()
	require.Len(t, view.pendingTasks, 1)
	assert.Same(t, second, view.pendingTasks[0])
	view.mu.Unlock()
	require.Len(t, scheduler.tasks, 2, "finishing the head submits the next task")
	assert.Same(t, second, scheduler.tasks[1])

	// A repeated submit attempt is a no-op: the head is already submitted.
	view.mu.Lock()
	view.maybeSubmitNextLocked()
	view.mu.Unlock()
	require.Len(t, scheduler.tasks, 2)
}

// TestSegmentTaskUnclassifiedErrorIsRetryable covers the classification
// contract: only errors explicitly marked unrecoverable fail the segment; an
// arbitrary unclassified error is treated as retryable and never takes the
// segment down.
func TestSegmentTaskUnclassifiedErrorIsRetryable(t *testing.T) {
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{runtime: moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}}},
	)

	view.mu.Lock()
	task := &fakeSegmentTask{segmentTaskBase: view.newSegmentTaskBaseLocked("fake"), failErr: errors.New("plain unclassified error")}
	view.pendingTasks = append(view.pendingTasks, task)
	view.mu.Unlock()

	err := task.Execute(context.Background())
	require.True(t, errors.Is(err, nodescheduler.ErrDelay), "an unclassified error is treated as retryable")
	require.Nil(t, view.unrecoverableErr(), "an unclassified error must not fail the segment")
}

// failingLifecycle fails both lifecycle calls with an unrecoverable
// (InputError-class) error, simulating a permanent coordinator rejection.
type failingLifecycle struct{}

func (failingLifecycle) EnsureGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return retry.Unrecoverable(merr.WrapErrParameterInvalid("v2", "v3"))
}

func (failingLifecycle) CommitL1Segment(context.Context, *streamingpb.SegmentAssignmentMeta) error {
	return retry.Unrecoverable(merr.WrapErrParameterInvalid("v2", "v3"))
}

// TestSegmentTaskTerminalErrorNotMarkedDelay covers the CRITICAL contract: an
// unrecoverable error leaving the task must NOT carry an ErrDelay mark, or the
// node scheduler would requeue an already-finished task — breaking the serial
// queue and eventually panicking on an empty pending queue. The lifecycle
// closures hand classification to execute(), which marks only retryable errors
// with ErrDelay.
func TestSegmentTaskTerminalErrorNotMarkedDelay(t *testing.T) {
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			lifecycle: failingLifecycle{},
			runtime:   moduleapi.Runtime{Scheduler: &recordingSegmentScheduler{}},
		},
	)

	view.mu.Lock()
	task := view.newEnsureGrowingSegmentTaskLocked(0)
	view.maybeSubmitNextLocked()
	view.mu.Unlock()

	err := task.Execute(context.Background())
	require.Error(t, err)
	require.False(t, errors.Is(err, nodescheduler.ErrDelay),
		"terminal error must not carry an ErrDelay mark or the scheduler requeues a finished task")
	require.Error(t, view.unrecoverableErr(), "segment must be marked unrecoverable")
	view.mu.Lock()
	require.Empty(t, view.pendingTasks, "finished task must leave the queue")
	view.mu.Unlock()
}

// TestSegmentTaskTerminalFailureWithRealScheduler drives the same scenario
// through a real node scheduler: a terminal failure must finish the task (not
// requeue it) and drain the pending queue without panicking.
func TestSegmentTaskTerminalFailureWithRealScheduler(t *testing.T) {
	sched := nodescheduler.New(1)
	defer sched.Close()

	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{SegmentId: 1, Vchannel: "v1"},
		0,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{
			lifecycle: failingLifecycle{},
			runtime:   moduleapi.Runtime{Scheduler: sched},
		},
	)

	view.mu.Lock()
	view.newEnsureGrowingSegmentTaskLocked(0)
	view.maybeSubmitNextLocked()
	view.mu.Unlock()

	require.Eventually(t, func() bool {
		view.mu.Lock()
		defer view.mu.Unlock()
		return len(view.pendingTasks) == 0 && view.unrecoverableErr() != nil
	}, 5*time.Second, 10*time.Millisecond, "terminal failure must finish the task and drain the queue")
}
