package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func setQueueWait(t *testing.T, ratio, limit string) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg
	paramtable.Get().Save(cfg.MaxQueueWaitRatio.Key, ratio)
	paramtable.Get().Save(cfg.MaxQueueWait.Key, limit)
	t.Cleanup(func() {
		paramtable.Get().Reset(cfg.MaxQueueWaitRatio.Key)
		paramtable.Get().Reset(cfg.MaxQueueWait.Key)
	})
}

func taskWithTimeout(t *testing.T, now time.Time, timeout time.Duration) Task {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, now.Add(timeout))
		t.Cleanup(cancel)
	}
	return newMockTask(mockTaskConfig{ctx: ctx, executeCost: time.Millisecond})
}

func TestMaxQueueWait(t *testing.T) {
	now := time.Now()

	// Off by default: a task waits until it is executed or expires.
	paramtable.Init()
	assert.Zero(t, maxQueueWait(taskWithTimeout(t, now, 2*time.Second), now))

	setQueueWait(t, "0.1", "1s")
	// A tenth of the time the task has left ...
	assert.Equal(t, 200*time.Millisecond, maxQueueWait(taskWithTimeout(t, now, 2*time.Second), now))
	// ... at most maxQueueWait ...
	assert.Equal(t, time.Second, maxQueueWait(taskWithTimeout(t, now, time.Minute), now))
	// ... which is also what a task without a deadline gets.
	assert.Equal(t, time.Second, maxQueueWait(taskWithTimeout(t, now, 0), now))
	// A task that is already late keeps a limit, zero would lift it.
	assert.Positive(t, maxQueueWait(taskWithTimeout(t, now, time.Second), now.Add(2*time.Second)))
}

func TestSchedulerFailsTaskThatWaitedTooLong(t *testing.T) {
	setQueueWait(t, "0.1", "1s")
	now := time.Now()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()

	add := func(task Task, at time.Time) {
		errCh := make(chan error, 1)
		s.handleAddTaskRequest(addTaskReq{task: task, err: errCh}, 0, at)
		require.NoError(t, <-errCh)
	}
	// May wait 200ms and has waited 300ms.
	late := taskWithTimeout(t, now.Add(-300*time.Millisecond), 2*time.Second)
	add(late, now.Add(-300*time.Millisecond))
	// May wait 200ms and has waited 100ms.
	inTime := taskWithTimeout(t, now.Add(-100*time.Millisecond), 2*time.Second)
	add(inTime, now.Add(-100*time.Millisecond))
	require.EqualValues(t, 2, s.GetWaitingTaskTotal())

	next, _, execChan := s.setupExecListener(nil, now)

	require.True(t, next.valid())
	assert.Same(t, inTime, next.Task)
	assert.NotNil(t, execChan)
	assert.ErrorIs(t, late.Wait(), merr.ErrServiceTooManyRequests)
	// The failed task no longer counts as waiting, the next one does until
	// it is handed to a worker.
	assert.EqualValues(t, 1, s.GetWaitingTaskTotal())
}

func TestSchedulerKeepsWaitingTaskWithoutLimit(t *testing.T) {
	setQueueWait(t, "0", "1s")
	now := time.Now()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()

	task := taskWithTimeout(t, now.Add(-time.Minute), time.Hour)
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: task, err: errCh}, 0, now.Add(-time.Minute))
	require.NoError(t, <-errCh)

	next, _, _ := s.setupExecListener(nil, now)
	require.True(t, next.valid())
	assert.Same(t, task, next.Task)
}

func TestQueuePace(t *testing.T) {
	now := time.Now()
	waiting := func(since time.Time) *queuedTask {
		return newQueuedTask(newMockTask(mockTaskConfig{executeCost: time.Millisecond}), since)
	}
	var p queuePace
	// Nothing known: no wait expected.
	assert.Zero(t, p.wait(10))

	p.sent(waiting(now), now)
	assert.Zero(t, p.wait(10))
	// A task that waited since before the previous one left: a worker takes
	// 10ms to come free.
	p.sent(waiting(now), now.Add(10*time.Millisecond))
	assert.Equal(t, 100*time.Millisecond, p.wait(10))
	// A task that arrived after the previous one left tells nothing: the
	// queue was empty in between.
	p.sent(waiting(now.Add(5*time.Second)), now.Add(5*time.Second))
	assert.Equal(t, 100*time.Millisecond, p.wait(10))
	// The mean follows the pace.
	p.sent(waiting(now), now.Add(5*time.Second+60*time.Millisecond))
	assert.Equal(t, 200*time.Millisecond, p.wait(10))
}

func TestSchedulerRefusesTaskThatWouldWaitTooLong(t *testing.T) {
	setQueueWait(t, "0.1", "1s")
	now := time.Now()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()
	// A worker comes free every 50ms.
	s.pace = queuePace{lastSent: now, interval: 50 * time.Millisecond}

	add := func(timeout time.Duration) error {
		errCh := make(chan error, 1)
		s.handleAddTaskRequest(addTaskReq{task: taskWithTimeout(t, now, timeout), err: errCh}, 0, now)
		return <-errCh
	}
	// May wait 200ms: accepted behind 0 .. 4 tasks, which is up to 200ms.
	for i := 0; i < 5; i++ {
		require.NoError(t, add(2*time.Second), i)
	}
	// Behind 5 tasks it would wait 250ms.
	assert.ErrorIs(t, add(2*time.Second), merr.ErrServiceTooManyRequests)
	assert.EqualValues(t, 5, s.GetWaitingTaskTotal())
	// A request with more time may wait longer.
	assert.NoError(t, add(10*time.Second))
}
