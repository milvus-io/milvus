package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func requeryCtx() context.Context {
	return contextutil.WithQueryLabel(context.Background(), metrics.ReQueryLabel)
}

func newUnstartedScheduler() *scheduler {
	return newScheduler(newFIFOPolicy()).(*scheduler)
}

func addTask(s *scheduler, task Task, now time.Time) error {
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: task, err: errCh}, paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.GetAsInt64(), now)
	return <-errCh
}

func TestRequeryLaneBypassesFullUnsolvedQueue(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldMax := pt.QueryNodeCfg.MaxUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.MaxUnsolvedQueueSize.SwapTempValue(oldMax)
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	now := time.Now()

	// Fill the regular unsolved queue to its limit.
	for i := 0; i < 2; i++ {
		assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{}), now))
	}
	// Regular task is rejected when the queue is full.
	err := addTask(s, newMockTask(mockTaskConfig{}), now)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)

	// Requery task is still admitted through the lane.
	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx()}), now))
	assert.Equal(t, 1, s.requeryQueue.len())
	// Lane tasks stay out of the policy waiting counters.
	assert.Equal(t, int64(2), s.GetWaitingTaskTotal())

	// The lane itself is bounded.
	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx()}), now))
	err = addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx()}), now)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.ErrorContains(t, err, pt.QueryNodeCfg.RequeryUnsolvedQueueSize.Key)
	assert.Equal(t, 2, s.requeryQueue.len())
}

func TestRequeryLaneStrictPriority(t *testing.T) {
	paramtable.Init()
	s := newUnstartedScheduler()
	now := time.Now()

	regular1 := newMockTask(mockTaskConfig{})
	regular2 := newMockTask(mockTaskConfig{})
	requery := newMockTask(mockTaskConfig{ctx: requeryCtx()})
	assert.NoError(t, addTask(s, regular1, now))
	assert.NoError(t, addTask(s, regular2, now))
	assert.NoError(t, addTask(s, requery, now))

	// The requery task is popped first even though it was queued last.
	popped := s.popTask(now)
	assert.True(t, popped.requery)
	assert.Same(t, requery, popped.Task)
	assert.Same(t, regular1, s.popTask(now).Task)
	assert.Same(t, regular2, s.popTask(now).Task)
	assert.False(t, s.popTask(now).valid())
}

func TestRequeryLaneDisabled(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("0")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	now := time.Now()

	// With the lane disabled the requery task falls back to the policy queue.
	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx()}), now))
	assert.Equal(t, 0, s.requeryQueue.len())
	assert.Equal(t, int64(1), s.GetWaitingTaskTotal())
	assert.False(t, s.popTask(now).requery)
}

func TestRequeryLaneCleanupExpired(t *testing.T) {
	paramtable.Init()
	s := newUnstartedScheduler()
	now := time.Now()

	ctx, cancel := context.WithCancel(requeryCtx())
	task := newMockTask(mockTaskConfig{ctx: ctx})
	assert.NoError(t, addTask(s, task, now))
	assert.Equal(t, 1, s.requeryQueue.len())

	cancel()
	s.cleanupExpiredTasks(time.Now())
	assert.Equal(t, 0, s.requeryQueue.len())
	assert.ErrorIs(t, task.Wait(), context.Canceled)
	// Lane cleanup must not corrupt the policy counters.
	assert.Equal(t, int64(0), s.GetWaitingTaskTotal())
	assert.Equal(t, int64(0), s.GetWaitingTaskTotalNQ())
}

func TestRequeryLaneCleared(t *testing.T) {
	paramtable.Init()
	s := newUnstartedScheduler()
	now := time.Now()

	task := newMockTask(mockTaskConfig{ctx: requeryCtx()})
	assert.NoError(t, addTask(s, task, now))

	result, _ := s.clearQueuedTasks(nil, "test clear", nil, time.Now())
	assert.Equal(t, int64(1), result.QueuedCleared)
	assert.Equal(t, 0, s.requeryQueue.len())
	assert.ErrorIs(t, task.Wait(), context.Canceled)
	assert.Equal(t, int64(0), s.GetWaitingTaskTotal())
}

func TestRequeryLaneEndToEnd(t *testing.T) {
	paramtable.Init()
	s := newScheduler(newFIFOPolicy())
	s.Start()
	defer s.Stop()

	regular := newMockTask(mockTaskConfig{executeCost: 10 * time.Millisecond})
	requery := newMockTask(mockTaskConfig{ctx: requeryCtx(), executeCost: 10 * time.Millisecond})
	assert.NoError(t, s.Add(regular))
	assert.NoError(t, s.Add(requery))
	assert.NoError(t, regular.Wait())
	assert.NoError(t, requery.Wait())
	assert.Equal(t, int64(0), s.GetWaitingTaskTotal())
	assert.Equal(t, int64(0), s.GetWaitingTaskTotalNQ())
}
