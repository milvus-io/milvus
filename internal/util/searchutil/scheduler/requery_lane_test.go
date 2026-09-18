package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func requeryCtx() context.Context {
	return contextutil.WithQueryLabel(context.Background(), metrics.ReQueryLabel)
}

func newUnstartedScheduler() *scheduler {
	return newScheduler(newRequeryPriorityPolicy(newFIFOPolicy())).(*scheduler)
}

func addTask(s *scheduler, task Task, now time.Time) error {
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: task, err: errCh}, paramtable.Get().QueryNodeCfg.MaxUnsolvedQueueSize.GetAsInt64(), now, nil)
	return <-errCh
}

func waitMockTask(t *testing.T, task Task) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		result <- task.Wait()
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task completion")
		return nil
	}
}

func TestRequeryLaneSeparateAdmissionAndExportedCounters(t *testing.T) {
	paramtable.Init()
	metrics.QueryNodeReadTaskRejectCnt.Reset()
	defer metrics.QueryNodeReadTaskRejectCnt.Reset()
	pt := paramtable.Get()
	oldMax := pt.QueryNodeCfg.MaxUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.MaxUnsolvedQueueSize.SwapTempValue(oldMax)
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	policy := s.policy.(*requeryPriorityPolicy)
	now := time.Now()

	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{nq: 3}), now))
	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{nq: 5}), now))
	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 7}), now))

	assert.Equal(t, int64(2), s.getRegularWaitingTaskTotal())
	assert.Equal(t, int64(8), s.getRegularWaitingTaskTotalNQ())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())
	assert.Equal(t, int64(7), s.getRequeryWaitingTaskTotalNQ())
	assert.Equal(t, int64(3), s.GetWaitingTaskTotal())
	assert.Equal(t, int64(15), s.GetWaitingTaskTotalNQ())
	assert.Equal(t, 1, policy.lane.len())

	// Lane backlog is visible externally but does not consume regular capacity.
	err := addTask(s, newMockTask(mockTaskConfig{}), now)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 11}), now))
	err = addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx()}), now)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.ErrorContains(t, err, pt.QueryNodeCfg.RequeryUnsolvedQueueSize.Key)

	assert.Equal(t, int64(2), s.getRegularWaitingTaskTotal())
	assert.Equal(t, int64(2), s.getRequeryWaitingTaskTotal())
	assert.Equal(t, int64(4), s.GetWaitingTaskTotal())
	assert.Equal(t, int64(26), s.GetWaitingTaskTotalNQ())

	nodeID := paramtable.GetStringNodeID()
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.QueryNodeReadTaskRejectCnt.WithLabelValues(nodeID, metrics.RegularLabel),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.QueryNodeReadTaskRejectCnt.WithLabelValues(nodeID, metrics.ReQueryLabel),
	))
}

func TestRequeryLaneCleanupRetry(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("1")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	policy := s.policy.(*requeryPriorityPolicy)
	now := time.Now()
	expiredCtx, cancel := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancel()
	expiredTask := newMockTask(mockTaskConfig{ctx: expiredCtx, nq: 2})
	expired := newQueuedTask(expiredTask, now.Add(-time.Second))
	expired.requery = true
	added, err := s.policy.Push(expired)
	require.NoError(t, err)
	s.updateWaitingTaskCounter(expired, int64(added), expired.NQ())

	fresh := newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 3})
	assert.NoError(t, addTask(s, fresh, now))
	assert.ErrorIs(t, waitMockTask(t, expiredTask), context.DeadlineExceeded)
	assert.Equal(t, 1, policy.lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())
	assert.Equal(t, int64(3), s.getRequeryWaitingTaskTotalNQ())
	assert.Equal(t, int64(1), s.GetWaitingTaskTotal())
}

func TestRequeryLaneFullStopsReceiveBatch(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("1")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	now := time.Now()
	require.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx()}), now))

	s.receiveChan = make(chan addTaskReq, 2)
	nextErr1 := make(chan error, 1)
	nextErr2 := make(chan error, 1)
	s.receiveChan <- addTaskReq{task: newMockTask(mockTaskConfig{}), err: nextErr1}
	s.receiveChan <- addTaskReq{task: newMockTask(mockTaskConfig{}), err: nextErr2}

	fullLaneErr := make(chan error, 1)
	s.consumeRecvChan(
		addTaskReq{task: newMockTask(mockTaskConfig{ctx: requeryCtx()}), err: fullLaneErr},
		maxReceiveChanBatchConsumeNum,
		now,
		nil,
	)
	assert.ErrorIs(t, <-fullLaneErr, merr.ErrServiceTooManyRequests)
	assert.Len(t, s.receiveChan, 2)
	select {
	case err := <-nextErr1:
		t.Fatalf("next request was unexpectedly consumed: %v", err)
	default:
	}
	select {
	case err := <-nextErr2:
		t.Fatalf("next request was unexpectedly consumed: %v", err)
	default:
	}
}

func TestRequeryLaneStagedTaskCountsAgainstCapacity(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	policy := s.policy.(*requeryPriorityPolicy)
	now := time.Now()
	first := newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 2})
	require.NoError(t, addTask(s, first, now))

	staged, nq, execChan := s.setupExecListener(nil, now)
	require.True(t, staged.valid())
	assert.Equal(t, first, staged.Task)
	assert.Equal(t, int64(2), nq)
	assert.Equal(t, s.execChan, execChan)
	assert.Zero(t, policy.lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())

	second := newMockTask(mockTaskConfig{ctx: requeryCtx()})
	capacity, available := s.waitingTaskCapacityAvailable(true, 1024)
	assert.Equal(t, int64(2), capacity)
	assert.True(t, available)

	pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("1")
	candidate := newQueuedTask(second, now)
	candidate.requery = true
	_, err := s.pushTaskOnce(candidate)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.Zero(t, policy.lane.len())

	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: second, err: errCh}, 1024, now, &staged)
	err = <-errCh
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
	assert.ErrorContains(t, err, pt.QueryNodeCfg.RequeryUnsolvedQueueSize.Key)
	assert.True(t, staged.valid())
	assert.Zero(t, policy.lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())

	s.onTaskDequeued(staged, nq)
	staged.Done(nil)
	assert.Zero(t, s.getRequeryWaitingTaskTotal())
	require.NoError(t, addTask(s, second, now))
	assert.Equal(t, 1, policy.lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())
}

func TestRequeryLaneStagedRegularDroppedGrantsNoCredit(t *testing.T) {
	paramtable.Init()

	s := newUnstartedScheduler()
	policy := s.policy.(*requeryPriorityPolicy)
	now := time.Now()

	expiringCtx, cancel := context.WithDeadline(context.Background(), now.Add(time.Hour))
	defer cancel()
	expiringRegular := newMockTask(mockTaskConfig{ctx: expiringCtx, nq: 1})
	require.NoError(t, addTask(s, expiringRegular, now))
	require.NoError(t, addTask(s, newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 1}), now))
	nextRegular := newMockTask(mockTaskConfig{nq: 1})
	require.NoError(t, addTask(s, nextRegular, now))
	policy.requeryCredit = 0

	staged, _, _ := s.setupExecListener(nil, now)
	require.True(t, staged.valid())
	require.Same(t, expiringRegular, staged.Task)
	assert.Zero(t, policy.requeryCredit)

	// The staged regular task expires before handoff; its drop must not open a
	// lane window, so the next selected task is still the regular one.
	s.cleanupExpiredTasks(now.Add(2*time.Hour), &staged)
	require.False(t, staged.valid())
	assert.ErrorIs(t, waitMockTask(t, expiringRegular), context.DeadlineExceeded)
	assert.Zero(t, policy.requeryCredit)

	staged, _, _ = s.setupExecListener(nil, now)
	require.True(t, staged.valid())
	assert.Same(t, nextRegular, staged.Task)

	// Only a real handoff grants a new lane window.
	s.onTaskServed(staged)
	assert.Equal(t, requeryPriorityBaseCredit, policy.requeryCredit)
}

func TestRequeryLaneCleansExpiredStagedTaskBeforeCapacityReject(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("1")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	policy := s.policy.(*requeryPriorityPolicy)
	now := time.Now()
	ctx, cancel := context.WithDeadline(requeryCtx(), now.Add(time.Hour))
	defer cancel()
	expiredTask := newMockTask(mockTaskConfig{ctx: ctx, nq: 2})
	require.NoError(t, addTask(s, expiredTask, now))

	staged, _, _ := s.setupExecListener(nil, now)
	require.True(t, staged.valid())
	assert.Zero(t, policy.lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())

	fresh := newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 3})
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: fresh, err: errCh}, 1024, now.Add(2*time.Hour), &staged)

	assert.NoError(t, <-errCh)
	assert.False(t, staged.valid())
	assert.ErrorIs(t, waitMockTask(t, expiredTask), context.DeadlineExceeded)
	assert.Equal(t, 1, policy.lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())
	assert.Equal(t, int64(3), s.getRequeryWaitingTaskTotalNQ())
}

func TestRequeryLaneDepthMetricIncludesStagedTask(t *testing.T) {
	paramtable.Init()
	metrics.QueryNodeReadTaskRequeryReadyLen.Reset()
	defer metrics.QueryNodeReadTaskRequeryReadyLen.Reset()
	metrics.QueryNodeReadTaskQueueDuration.Reset()
	defer metrics.QueryNodeReadTaskQueueDuration.Reset()
	metrics.QueryNodeReadTaskRequeryQueueDuration.Reset()
	defer metrics.QueryNodeReadTaskRequeryQueueDuration.Reset()

	s := newUnstartedScheduler()
	now := time.Now()
	task := newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 2})
	require.NoError(t, addTask(s, task, now))

	staged, nq, _ := s.setupExecListener(nil, now)
	require.True(t, staged.valid())
	assert.Zero(t, s.policy.(*requeryPriorityPolicy).lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())
	// The requery task lands in both the aggregate histogram and the
	// requery-only subset metric.
	assert.Equal(t, uint64(1), readTaskQueueDurationCount(readTaskQueueOutcomeScheduled))
	assert.Equal(t, uint64(1), readTaskRequeryQueueDurationCount(readTaskQueueOutcomeScheduled))

	s.setupReadyLenMetric()
	nodeID := paramtable.GetStringNodeID()
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.QueryNodeReadTaskRequeryReadyLen.WithLabelValues(nodeID),
	))

	s.onTaskDequeued(staged, nq)
	staged.Done(nil)
	s.setupReadyLenMetric()
	assert.Zero(t, testutil.ToFloat64(
		metrics.QueryNodeReadTaskRequeryReadyLen.WithLabelValues(nodeID),
	))
}

func TestUserTaskPollingQuotaRejectDoesNotTriggerGlobalCleanup(t *testing.T) {
	paramtable.Init()
	metrics.QueryNodeReadTaskRejectCnt.Reset()
	defer metrics.QueryNodeReadTaskRejectCnt.Reset()
	pt := paramtable.Get()
	oldLimit := pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.SwapTempValue("1")
	defer pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.SwapTempValue(oldLimit)

	now := time.Now()
	s := &scheduler{
		policy:           newUserTaskPollingPolicy(),
		schedulerCounter: schedulerCounter{},
	}
	userATask := newMockTask(mockTaskConfig{username: "user-a"})
	userA := newQueuedTask(userATask, now)
	added, err := s.policy.Push(userA)
	require.NoError(t, err)
	s.updateWaitingTaskCounter(userA, int64(added), userA.NQ())

	expiredCtx, cancel := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancel()
	expiredTask := newMockTask(mockTaskConfig{ctx: expiredCtx, username: "user-b"})
	expired := newQueuedTask(expiredTask, now.Add(-time.Second))
	added, err = s.policy.Push(expired)
	require.NoError(t, err)
	s.updateWaitingTaskCounter(expired, int64(added), expired.NQ())

	fresh := newMockTask(mockTaskConfig{username: "user-a"})
	errCh := make(chan error, 1)
	s.handleAddTaskRequest(addTaskReq{task: fresh, err: errCh}, 10, now, nil)
	assert.ErrorIs(t, <-errCh, merr.ErrServiceTooManyRequests)
	assert.Equal(t, 2, s.policy.Len())
	assert.Equal(t, int64(2), s.getRegularWaitingTaskTotal())
	assert.Zero(t, len(expiredTask.(*MockTask).notifier))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.QueryNodeReadTaskRejectCnt.WithLabelValues(paramtable.GetStringNodeID(), metrics.RegularLabel),
	))
}

func TestRequeryLaneDynamicDisableDrainsOldTasksWithoutCapBypass(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldMax := pt.QueryNodeCfg.MaxUnsolvedQueueSize.SwapTempValue("1")
	defer pt.QueryNodeCfg.MaxUnsolvedQueueSize.SwapTempValue(oldMax)
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("1")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := newUnstartedScheduler()
	now := time.Now()
	regular := newMockTask(mockTaskConfig{})
	oldRequery := newMockTask(mockTaskConfig{ctx: requeryCtx()})
	assert.NoError(t, addTask(s, regular, now))
	assert.NoError(t, addTask(s, oldRequery, now))

	pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("0")
	newRequery := newMockTask(mockTaskConfig{ctx: requeryCtx()})
	err := addTask(s, newRequery, now)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)

	popped := s.policy.Pop(now)
	assert.Equal(t, oldRequery, popped.Task)
	assert.True(t, popped.requery)
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())
	s.onTaskDequeued(popped, popped.NQ())
	assert.Zero(t, s.getRequeryWaitingTaskTotal())

	popped = s.policy.Pop(now)
	assert.Equal(t, regular, popped.Task)
	assert.False(t, popped.requery)
}

func TestRequeryLaneClearPoppedCurrentSettlesCounters(t *testing.T) {
	paramtable.Init()
	s := newUnstartedScheduler()
	now := time.Now()
	task := newMockTask(mockTaskConfig{ctx: requeryCtx(), nq: 7})
	assert.NoError(t, addTask(s, task, now))

	current, nq, execChan := s.setupExecListener(nil, now)
	require.True(t, current.valid())
	assert.True(t, current.requery)
	assert.Equal(t, int64(7), nq)
	assert.Equal(t, s.execChan, execChan)
	assert.Zero(t, s.policy.(*requeryPriorityPolicy).lane.len())
	assert.Equal(t, int64(1), s.getRequeryWaitingTaskTotal())

	result, remaining := s.clearQueuedTasks(nil, "test clear", current, now)
	assert.Equal(t, ClearResult{QueuedCleared: 1, QueuedNQCleared: 7}, result)
	assert.False(t, remaining.valid())
	assert.ErrorIs(t, waitMockTask(t, task), context.Canceled)
	assert.Zero(t, s.GetWaitingTaskTotal())
	assert.Zero(t, s.GetWaitingTaskTotalNQ())
}

func TestRequeryLaneWiringDisablesLaneForUserTaskPolling(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)
	oldLimit := pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.SwapTempValue("2")
	defer pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.SwapTempValue(oldLimit)

	defaultScheduler := NewScheduler("").(*scheduler)
	t.Cleanup(defaultScheduler.Stop)
	_, ok := defaultScheduler.policy.(*requeryPriorityPolicy)
	assert.True(t, ok)
	fifoScheduler := NewScheduler(schedulePolicyNameFIFO).(*scheduler)
	t.Cleanup(fifoScheduler.Stop)
	_, ok = fifoScheduler.policy.(*requeryPriorityPolicy)
	assert.True(t, ok)
	utpScheduler := NewScheduler(schedulePolicyNameUserTaskPolling).(*scheduler)
	t.Cleanup(utpScheduler.Stop)
	_, ok = utpScheduler.policy.(*userTaskPollingPolicy)
	assert.True(t, ok)
	_, ok = utpScheduler.policy.(laneClassifier)
	assert.False(t, ok)

	now := time.Now()
	userA1 := newMockTask(mockTaskConfig{username: "user-a"})
	userA2 := newMockTask(mockTaskConfig{ctx: requeryCtx(), username: "user-a"})
	userB1 := newMockTask(mockTaskConfig{ctx: requeryCtx(), username: "user-b"})
	userB2 := newMockTask(mockTaskConfig{username: "user-b"})
	assert.NoError(t, addTask(utpScheduler, userA1, now))
	assert.NoError(t, addTask(utpScheduler, userA2, now))
	assert.NoError(t, addTask(utpScheduler, userB1, now))
	assert.NoError(t, addTask(utpScheduler, userB2, now))
	err := addTask(utpScheduler, newMockTask(mockTaskConfig{ctx: requeryCtx(), username: "user-a"}), now)
	assert.ErrorIs(t, err, merr.ErrServiceTooManyRequests)

	for _, expected := range []Task{userA1, userB1, userA2, userB2} {
		popped := utpScheduler.policy.Pop(now)
		assert.Equal(t, expected, popped.Task)
		assert.False(t, popped.requery)
	}
}

func TestRequeryLaneShutdownDrainsCounters(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldConcurrency := pt.QueryNodeCfg.MaxReadConcurrency.SwapTempValue("1")
	defer pt.QueryNodeCfg.MaxReadConcurrency.SwapTempValue(oldConcurrency)
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("4")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	s := NewScheduler(schedulePolicyNameFIFO).(*scheduler)
	s.Start()
	started := make(chan struct{})
	gate := make(chan struct{})
	blocker := newMockTask(mockTaskConfig{
		executeCost: time.Millisecond,
		execution: func(context.Context) error {
			close(started)
			<-gate
			return nil
		},
	})
	lane1 := newMockTask(mockTaskConfig{ctx: requeryCtx(), executeCost: time.Millisecond})
	lane2 := newMockTask(mockTaskConfig{ctx: requeryCtx(), executeCost: time.Millisecond})
	lane3 := newMockTask(mockTaskConfig{ctx: requeryCtx(), executeCost: time.Millisecond})
	regular := newMockTask(mockTaskConfig{executeCost: time.Millisecond})

	require.NoError(t, s.Add(blocker))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocker to start")
	}
	require.NoError(t, s.Add(lane1))
	require.NoError(t, s.Add(lane2))
	require.NoError(t, s.Add(lane3))
	require.NoError(t, s.Add(regular))
	require.Eventually(t, func() bool {
		policy := s.policy.(*requeryPriorityPolicy)
		return policy.lane.len() > 0 && s.getRequeryWaitingTaskTotal() > 0
	}, 5*time.Second, 10*time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	close(gate)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler shutdown")
	}

	assert.NoError(t, waitMockTask(t, blocker))
	assert.NoError(t, waitMockTask(t, lane1))
	assert.NoError(t, waitMockTask(t, lane2))
	assert.NoError(t, waitMockTask(t, lane3))
	assert.NoError(t, waitMockTask(t, regular))
	assert.Zero(t, s.GetWaitingTaskTotal())
	assert.Zero(t, s.GetWaitingTaskTotalNQ())
}
