package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func edfTestTask(t *testing.T, deadline time.Time, requery bool) *queuedTask {
	t.Helper()
	ctx := context.Background()
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		t.Cleanup(cancel)
	}
	if requery {
		ctx = contextutil.WithQueryLabel(ctx, metrics.ReQueryLabel)
	}
	task := newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx}), time.Now())
	task.schedulingDeadline = deadline
	return task
}

func TestRequeryEDFOrdering(t *testing.T) {
	paramtable.Init()
	now := time.Now()
	early, late := now.Add(time.Minute), now.Add(2*time.Minute)
	for _, tc := range []struct {
		name             string
		regular, requery time.Time
		wantRegular      bool
	}{
		{"regular earlier", early, late, true},
		{"requery earlier", late, early, false},
		{"equal deadlines", early, early, false},
		{"regular without deadline", time.Time{}, early, false},
		{"requery without deadline", early, time.Time{}, true},
		{"no deadlines", time.Time{}, time.Time{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newRequeryEDFPolicy()
			r := edfTestTask(t, tc.regular, false)
			q := edfTestTask(t, tc.requery, true)
			r.enqueueTime, q.enqueueTime = now, now.Add(time.Millisecond)
			rTask, qTask := r.Task, q.Task
			_, err := p.Push(r)
			require.NoError(t, err)
			_, err = p.Push(q)
			require.NoError(t, err)
			first, second := p.Pop(now), p.Pop(now)
			if tc.wantRegular {
				require.Same(t, rTask, first.Task)
				require.Same(t, qTask, second.Task)
			} else {
				require.Same(t, qTask, first.Task)
				require.Same(t, rTask, second.Task)
			}
			require.Nil(t, p.Pop(now))
		})
	}
}

func TestRequeryEDFMergedDeadlineDoesNotChangeContext(t *testing.T) {
	paramtable.Init()
	p := newRequeryEDFPolicy()
	now := time.Now()
	late := now.Add(time.Minute)
	early := late.Add(-time.Millisecond)
	for _, deadline := range []time.Time{late, early} {
		task := edfTestTask(t, deadline, false)
		task.Task.(*MockTask).mergeAble = true
		_, err := p.Push(task)
		require.NoError(t, err)
	}
	require.Equal(t, 1, p.Len())
	task := p.Pop(now)
	require.Equal(t, early, task.schedulingDeadline)
	contextDeadline, _ := task.Context().Deadline()
	require.Equal(t, late, contextDeadline)
	require.EqualValues(t, 2, task.NQ())
}

func TestRequeryEDFIndependentCapacity(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg
	require.NoError(t, paramtable.Get().Save(cfg.MaxUnsolvedQueueSize.Key, "1"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.MaxUnsolvedQueueSize.Key) })
	p := newRequeryEDFPolicy()
	now := time.Now()
	_, err := p.Push(edfTestTask(t, now.Add(time.Minute), false))
	require.NoError(t, err)
	_, err = p.Push(edfTestTask(t, now.Add(time.Minute), false))
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	for i := int64(0); i < p.requeryCapacity; i++ {
		_, err = p.Push(edfTestTask(t, now.Add(time.Minute), true))
		require.NoError(t, err)
	}
	_, err = p.Push(edfTestTask(t, now.Add(time.Minute), true))
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.EqualValues(t, 1025, p.Len())
	require.NoError(t, paramtable.Get().Save(cfg.RequeryUnsolvedQueueSize.Key, "2048"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.RequeryUnsolvedQueueSize.Key) })
	require.EqualValues(t, 1024, p.requeryCapacity)
}

func TestRequeryEDFExpiredHeadWithoutSlot(t *testing.T) {
	paramtable.Init()
	p := newRequeryEDFPolicy()
	now := time.Now()
	expired := edfTestTask(t, now.Add(-time.Second), true)
	_, err := p.Push(expired)
	require.NoError(t, err)
	_, err = p.Push(edfTestTask(t, now.Add(time.Minute), false))
	require.NoError(t, err)
	task := p.popReady(now, false, false)
	require.True(t, task.cleanupReady(now))
	require.Nil(t, p.popReady(now, false, false))
	require.NotNil(t, p.popReady(now, true, false))
}

type edfExecutionTask struct {
	Task
	run func() error
	gpu bool
}

func (t *edfExecutionTask) Execute() error { return t.run() }

func (t *edfExecutionTask) IsGpuIndex() bool { return t.gpu }

func TestRequeryEDFSkipsBusyPoolHead(t *testing.T) {
	paramtable.Init()
	p := newRequeryEDFPolicy()
	now := time.Now()
	gpuTask := edfTestTask(t, now.Add(time.Minute), false)
	gpuTask.Task = &edfExecutionTask{Task: gpuTask.Task, gpu: true}
	_, err := p.Push(gpuTask)
	require.NoError(t, err)
	_, err = p.Push(edfTestTask(t, now.Add(2*time.Minute), true))
	require.NoError(t, err)
	task := p.popReady(now, true, false)
	require.Equal(t, metrics.ReQueryLabel, contextutil.GetQueryLabel(task.Context()))
	require.Nil(t, p.popReady(now, true, false))
	require.True(t, p.popReady(now, false, true).IsGpuIndex())
}

func newEDFSingleSlotScheduler(t *testing.T) *requeryEDFScheduler {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().QueryNodeCfg
	// This parameter is a CPU ratio, clamped to at least one execution slot.
	require.NoError(t, paramtable.Get().Save(cfg.MaxReadConcurrency.Key, "0.001"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.MaxReadConcurrency.Key) })
	s := NewScheduler(schedulePolicyNameRequeryEDF).(*requeryEDFScheduler)
	require.Equal(t, 1, s.pool.Cap())
	s.Start()
	return s
}

func TestRequeryEDFSelectsAfterSlotReleased(t *testing.T) {
	s := newEDFSingleSlotScheduler(t)
	started, release := make(chan struct{}), make(chan struct{})
	blocker := &edfExecutionTask{
		Task: edfTestTask(t, time.Now().Add(time.Minute), false).Task,
		run: func() error {
			close(started)
			<-release
			return nil
		},
	}
	// Always release the worker before Stop, including on an assertion failure.
	defer s.Stop()
	defer close(release)
	require.NoError(t, s.Add(blocker))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	order := make(chan string, 2)
	regular := &edfExecutionTask{
		Task: edfTestTask(t, time.Now().Add(2*time.Minute), false).Task,
		run:  func() error { order <- "regular"; return nil },
	}
	requery := &edfExecutionTask{
		Task: edfTestTask(t, time.Now().Add(time.Minute), true).Task,
		run:  func() error { order <- "requery"; return nil },
	}
	require.NoError(t, s.Add(regular))
	require.NoError(t, s.Add(requery))
	require.EqualValues(t, 2, s.GetWaitingTaskTotal())
	// A value releases this worker, while the deferred close remains safe.
	release <- struct{}{}
	for _, want := range []string{"requery", "regular"} {
		select {
		case got := <-order:
			require.Equal(t, want, got)
		case <-time.After(5 * time.Second):
			t.Fatal("queued work did not execute")
		}
	}
	require.NoError(t, blocker.Wait())
	require.NoError(t, requery.Wait())
	require.NoError(t, regular.Wait())
	require.Zero(t, s.GetWaitingTaskTotal())
	require.Zero(t, s.GetWaitingTaskTotalNQ())
}

func TestRequeryEDFClearAndStop(t *testing.T) {
	s := newEDFSingleSlotScheduler(t)
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(s.Stop) }
	defer stop()
	started, release := make(chan struct{}), make(chan struct{})
	blocker := &edfExecutionTask{
		Task: edfTestTask(t, time.Time{}, false).Task,
		run:  func() error { close(started); <-release; return nil },
	}
	defer close(release)
	require.NoError(t, s.Add(blocker))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	queued := edfTestTask(t, time.Time{}, true).Task
	require.NoError(t, s.Add(queued))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := s.ClearQueued(ctx, nil, "test")
	require.NoError(t, err)
	require.EqualValues(t, 1, result.QueuedCleared)
	require.ErrorIs(t, queued.Wait(), context.Canceled)
	drainTask := &edfExecutionTask{
		Task: edfTestTask(t, time.Time{}, true).Task,
		run:  func() error { return nil },
	}
	require.NoError(t, s.Add(drainTask))
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before the executing task completed")
	default:
	}
	release <- struct{}{}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not drain the running task")
	}
	require.NoError(t, blocker.Wait())
	require.NoError(t, drainTask.Wait())
	require.Zero(t, s.GetWaitingTaskTotal())
	require.Zero(t, s.GetWaitingTaskTotalNQ())
}
