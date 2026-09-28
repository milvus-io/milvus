package scheduler

import (
	"context"
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
	return newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx}), time.Now())
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

func TestRequeryEDFExpiredHead(t *testing.T) {
	paramtable.Init()
	p := newRequeryEDFPolicy()
	now := time.Now()
	expired := edfTestTask(t, now.Add(-time.Second), true)
	_, err := p.Push(expired)
	require.NoError(t, err)
	_, err = p.Push(edfTestTask(t, now.Add(time.Minute), false))
	require.NoError(t, err)
	task := p.Pop(now)
	require.True(t, task.cleanupReady(now))
	require.NotNil(t, p.Pop(now))
	require.Nil(t, p.Pop(now))
}

func newEDFTestScheduler(t *testing.T) *scheduler {
	t.Helper()
	paramtable.Init()
	s, ok := NewScheduler(schedulePolicyNameRequeryEDF).(*scheduler)
	require.True(t, ok, "EDF must use the generic scheduler")
	require.True(t, s.policyOwnsQueueCapacity)
	t.Cleanup(s.Stop)
	return s
}

func admitEDFTestTask(s *scheduler, task Task, now time.Time) (bool, error) {
	errCh := make(chan error, 1)
	keepConsuming := s.handleAddTaskRequest(addTaskReq{task: task, err: errCh},
		paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.GetAsInt64(), now)
	return keepConsuming, <-errCh
}

func TestRequeryEDFAdmissionUsesIndependentCapacity(t *testing.T) {
	s := newEDFTestScheduler(t)
	cfg := &paramtable.Get().QueryNodeCfg
	require.NoError(t, paramtable.Get().Save(cfg.MaxUnsolvedQueueSize.Key, "1"))
	t.Cleanup(func() { paramtable.Get().Reset(cfg.MaxUnsolvedQueueSize.Key) })
	now := time.Now()
	keepConsuming, err := admitEDFTestTask(s, edfTestTask(t, time.Time{}, false).Task, now)
	require.NoError(t, err)
	require.True(t, keepConsuming)
	_, err = admitEDFTestTask(s, edfTestTask(t, time.Time{}, true).Task, now)
	require.NoError(t, err, "a full regular queue must not reject requery")
	keepConsuming, err = admitEDFTestTask(s, edfTestTask(t, time.Time{}, false).Task, now)
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	require.False(t, keepConsuming)
	_, err = admitEDFTestTask(s, edfTestTask(t, time.Time{}, true).Task, now)
	require.NoError(t, err)
	require.EqualValues(t, 3, s.GetWaitingTaskTotal())
	require.EqualValues(t, 3, s.GetWaitingTaskTotalNQ())
}

func TestRequeryEDFAdmissionCleansExpiredLane(t *testing.T) {
	for _, requery := range []bool{false, true} {
		name := "regular"
		if requery {
			name = "requery"
		}
		t.Run(name, func(t *testing.T) {
			s := newEDFTestScheduler(t)
			cfg := &paramtable.Get().QueryNodeCfg
			require.NoError(t, paramtable.Get().Save(cfg.MaxUnsolvedQueueSize.Key, "1"))
			t.Cleanup(func() { paramtable.Get().Reset(cfg.MaxUnsolvedQueueSize.Key) })
			capacity := int64(1)
			if requery {
				capacity = s.policy.(*requeryEDFPolicy).requeryCapacity
			}
			now := time.Now()
			expired := edfTestTask(t, now.Add(-time.Second), requery)
			expiredTask := expired.Task
			_, err := s.policy.Push(expired)
			require.NoError(t, err)
			s.updateWaitingTaskCounter(1, expired.NQ())
			for i := int64(1); i < capacity; i++ {
				_, err = admitEDFTestTask(s, edfTestTask(t, time.Time{}, requery).Task, now)
				require.NoError(t, err)
			}
			_, err = admitEDFTestTask(s, edfTestTask(t, time.Time{}, requery).Task, now)
			require.NoError(t, err)
			require.ErrorIs(t, expiredTask.Wait(), context.DeadlineExceeded)
			require.EqualValues(t, capacity, s.GetWaitingTaskTotal())
			require.EqualValues(t, capacity, s.GetWaitingTaskTotalNQ())
		})
	}
}

func TestRequeryEDFPreservesStagedTask(t *testing.T) {
	s := newEDFTestScheduler(t)
	now := time.Now()
	regular := edfTestTask(t, now.Add(2*time.Minute), false).Task
	_, err := admitEDFTestTask(s, regular, now)
	require.NoError(t, err)
	staged, _, _ := s.setupExecListener(nil, now)
	require.Same(t, regular, staged.Task)
	requery := edfTestTask(t, now.Add(time.Minute), true).Task
	_, err = admitEDFTestTask(s, requery, now)
	require.NoError(t, err)
	next, _, _ := s.setupExecListener(staged, now)
	require.Same(t, staged, next, "an earlier deadline must not replace an already popped task")
	result, remaining := s.clearQueuedTasks(nil, "test", staged, now)
	require.Nil(t, remaining)
	require.EqualValues(t, 2, result.QueuedCleared)
	require.ErrorIs(t, regular.Wait(), context.Canceled)
	require.ErrorIs(t, requery.Wait(), context.Canceled)
	require.Zero(t, s.GetWaitingTaskTotal())
	require.Zero(t, s.GetWaitingTaskTotalNQ())
}
