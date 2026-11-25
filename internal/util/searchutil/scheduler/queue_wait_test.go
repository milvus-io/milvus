package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// add queues task at the time at, as the schedule loop does.
func addAt(s *scheduler, task Task, at time.Time) error {
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: task, err: errCh}, 0, at)
	return <-errCh
}

// A task is refused when it is queued if the task that leaves the queue next
// has already waited for more than half of what the new task may wait.
func TestSchedulerRefusesTaskBehindHeadThatWaitedLong(t *testing.T) {
	setQueueWait(t, "0.1", "1s")
	now := time.Now()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()

	// Nothing waits: let in.
	require.NoError(t, addAt(s, taskWithTimeout(t, now, 2*time.Second), now))
	// The schedule loop takes it from the policy and holds it for a worker.
	head, _, _ := s.setupExecListener(nil, now)
	require.True(t, head.valid())

	// The head has waited 80ms; a task with 2s left may wait 200ms: let in.
	at := now.Add(80 * time.Millisecond)
	require.NoError(t, addAt(s, taskWithTimeout(t, at, 2*time.Second), at))
	// The head has waited 120ms, more than half of 200ms: refused.
	at = now.Add(120 * time.Millisecond)
	assert.ErrorIs(t, addAt(s, taskWithTimeout(t, at, 2*time.Second), at), merr.ErrServiceTooManyRequests)
	// A request with more time may wait longer, 1s at most.
	assert.NoError(t, addAt(s, taskWithTimeout(t, at, time.Minute), at))
	at = now.Add(600 * time.Millisecond)
	assert.ErrorIs(t, addAt(s, taskWithTimeout(t, at, time.Minute), at), merr.ErrServiceTooManyRequests)

	// The head left for a worker and a task queued later is next: let in
	// again. Nothing has to be learned back, the judgment is on the task
	// that waits now.
	head, _, _ = s.setupExecListener(nil, at)
	require.True(t, head.valid())
	assert.NoError(t, addAt(s, taskWithTimeout(t, at, time.Minute), at))
}

// With the limit off, nothing is refused for the wait of the head.
func TestSchedulerLetsTaskInWithoutLimit(t *testing.T) {
	setQueueWait(t, "0", "1s")
	now := time.Now()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()
	require.NoError(t, addAt(s, taskWithTimeout(t, now, time.Hour), now))
	head, _, _ := s.setupExecListener(nil, now)
	require.True(t, head.valid())
	at := now.Add(10 * time.Minute)
	assert.NoError(t, addAt(s, taskWithTimeout(t, at, time.Hour), at))
}

// A task whose deadline passed is out of time, not refused for the load on
// the node, also when its context does not say so yet.
func TestSchedulerFailsTaskOutOfTimeAsExpired(t *testing.T) {
	setQueueWait(t, "0.1", "1s")
	now := time.Now()
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()

	// When it is queued: behind a head that waited for long, where it would
	// be refused.
	require.NoError(t, addAt(s, taskWithTimeout(t, now, time.Hour), now))
	head, _, _ := s.setupExecListener(nil, now)
	require.True(t, head.valid())
	err := addAt(s, taskWithTimeout(t, now, time.Minute), now.Add(2*time.Minute))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, merr.ErrServiceTooManyRequests)

	// When it leaves the queue, where it has also waited for longer than it
	// may.
	s2 := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s2.Stop()
	late := taskWithTimeout(t, now, 10*time.Minute)
	errCh := make(chan error, 1)
	s2.handleAddTaskRequest(addTaskReq{task: late, err: errCh}, 0, now)
	require.NoError(t, <-errCh)
	next, _, _ := s2.setupExecListener(nil, now.Add(30*time.Minute))
	assert.False(t, next.valid())
	assert.Zero(t, s2.GetWaitingTaskTotal())
	err = late.Wait()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, merr.ErrServiceTooManyRequests)
}

// A task that arrives after the node was idle has not waited: the time the
// schedule loop spent blocked before the task came in is not queue time.
func TestSchedulerRunsTaskThatArrivesAfterIdleTime(t *testing.T) {
	setQueueWait(t, "0.1", "20ms")
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	s.Start()
	defer s.Stop()

	for i := 0; i < 3; i++ {
		// idle for several times what a task may wait
		time.Sleep(100 * time.Millisecond)
		task := taskWithTimeout(t, time.Now(), 10*time.Second)
		require.NoError(t, s.Add(task))
		assert.NoError(t, task.Wait())
	}
}

// A worker can become available after the task passed its dequeue check.
func TestQueueBudgetCheckedAfterWorkerWait(t *testing.T) {
	setQueueWait(t, "0.1", "20ms")
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	s.pool.Release()
	s.pool = conc.NewPool[any](1)
	occupied := make(chan struct{})
	release := make(chan struct{})
	blocker := s.pool.Submit(func() (any, error) {
		close(occupied)
		<-release
		return nil, nil
	})
	<-occupied
	s.wg.Add(1)
	go s.exec()
	executed := false
	task := newMockTask(mockTaskConfig{executeCost: time.Millisecond, execution: func(context.Context) error {
		executed = true
		return nil
	}})
	queued := newQueuedTask(task, time.Now())
	require.False(t, queued.overloaded(time.Now()))
	s.execChan <- queued
	// Hold the only worker until the task's budget has actually expired.
	time.Sleep(30 * time.Millisecond)
	close(release)
	_, err := blocker.Await()
	require.NoError(t, err)
	require.ErrorIs(t, task.Wait(), merr.ErrServiceTooManyRequests)
	require.False(t, executed)
	close(s.execChan)
	s.wg.Wait()
	s.pool.Release()
	s.gpuPool.Release()
}

func TestMergePreservesAbsoluteQueueExpiry(t *testing.T) {
	setQueueWait(t, "0.1", "1s")
	now := time.Now()
	for _, budgets := range []struct {
		owner, incoming time.Duration
		merge           bool
	}{
		{100 * time.Millisecond, 30 * time.Millisecond, false},
		{100 * time.Millisecond, 20 * time.Millisecond, true},
		{0, 20 * time.Millisecond, false},
		{0, 0, true},
	} {
		q := newMergeTaskQueue("")
		owner := &queuedTask{Task: newMockTask(mockTaskConfig{mergeAble: true}), enqueueTime: now, maxWait: budgets.owner}
		incoming := &queuedTask{Task: newMockTask(mockTaskConfig{mergeAble: true}), enqueueTime: now.Add(80 * time.Millisecond), maxWait: budgets.incoming}
		q.push(owner)
		require.Equal(t, budgets.merge, q.tryMerge(incoming, 100, 10, -1))
		if budgets.merge && budgets.owner > 0 {
			require.False(t, owner.overloaded(now.Add(99*time.Millisecond)))
			require.True(t, owner.overloaded(now.Add(101*time.Millisecond)))
		}
	}
}

func TestQueueBudgetAfterCanceledOwnerPromotion(t *testing.T) {
	setQueueWait(t, "0.1", "1s")
	s := newScheduler(newFIFOPolicy()).(*scheduler)
	defer s.Stop()
	now := time.Now()
	survivor := newMockTask(mockTaskConfig{nq: 2, executeCost: time.Millisecond})
	owner := newMockTask(mockTaskConfig{nq: 5, prune: func() (Task, int, error) {
		return survivor, 1, context.Canceled
	}})
	require.NoError(t, addAt(s, owner, now))
	next, nq, _ := s.setupExecListener(nil, now.Add(500*time.Millisecond))
	require.Same(t, survivor, next.Task)
	require.EqualValues(t, 5, nq)
	require.NoError(t, next.Task.Context().Err())
	s.updateWaitingTaskCounter(-1, -nq)
	require.Zero(t, s.GetWaitingTaskTotalNQ())
	require.False(t, next.overloaded(now.Add(900*time.Millisecond)))
	require.True(t, next.overloaded(now.Add(1100*time.Millisecond)))
}
