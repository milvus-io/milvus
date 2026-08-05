package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestScheduler(t *testing.T) {
	paramtable.Init()
	t.Run("user-task-polling", func(t *testing.T) {
		testScheduler(t, newUserTaskPollingPolicy())
	})
	t.Run("fifo", func(t *testing.T) {
		testScheduler(t, newFIFOPolicy())
	})
	t.Run("scheduler_not_working", func(t *testing.T) {
		scheduler := newScheduler(newFIFOPolicy())

		task := newMockTask(mockTaskConfig{
			nq:          1,
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				return nil
			},
		})

		err := scheduler.Add(task)
		assert.Error(t, err)

		scheduler.Stop()

		err = scheduler.Add(task)
		assert.Error(t, err)
	})

	suite.Run(t, new(SchedulerSuite))
}

func testScheduler(t *testing.T, policy schedulePolicy) {
	// start a new scheduler
	scheduler := newScheduler(policy)
	scheduler.Start()

	var cnt atomic.Int32
	n := 100
	nq := 0
	userN := 10
	// Test Push
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", rand.Int31n(int32(userN)))
		task := newMockTask(mockTaskConfig{
			username:    username,
			nq:          int64(i),
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				cnt.Inc()
				return nil
			},
		})
		nq += i
		assert.NoError(t, scheduler.Add(task))
		total := int(scheduler.GetWaitingTaskTotal())
		nqNow := int(scheduler.GetWaitingTaskTotalNQ())
		assert.LessOrEqual(t, total, i)
		assert.LessOrEqual(t, nqNow, nq)
	}
	time.Sleep(2 * time.Second)
	assert.Equal(t, cnt.Load(), int32(n))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotal()))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotalNQ()))

	// Test Push
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", rand.Int31n(int32(userN)))
		task := newMockTask(mockTaskConfig{
			username:    username,
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				cnt.Inc()
				return nil
			},
		})
		assert.NoError(t, scheduler.Add(task))
		total := int(scheduler.GetWaitingTaskTotal())
		nqNow := int(scheduler.GetWaitingTaskTotalNQ())
		assert.LessOrEqual(t, total, i)
		assert.LessOrEqual(t, nqNow, i)
	}

	time.Sleep(2 * time.Second)
	assert.Equal(t, cnt.Load(), int32(2*n))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotal()))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotalNQ()))
}

type SchedulerSuite struct {
	suite.Suite
}

type doneCallbackTask struct {
	Task
	callback func()
}

func (t *doneCallbackTask) Done(err error) {
	t.callback()
	t.Task.Done(err)
}

func (s *SchedulerSuite) TestConsumeRecvChan() {
	s.Run("consume_chan_closed", func() {
		ch := make(chan addTaskReq, 10)
		close(ch)
		scheduler := &scheduler{
			policy:           newFIFOPolicy(),
			receiveChan:      ch,
			execChan:         make(chan Task),
			pool:             conc.NewPool[any](10, conc.WithPreAlloc(true)),
			schedulerCounter: schedulerCounter{},
			lifetime:         lifetime.NewLifetime(lifetime.Initializing),
		}

		task := newMockTask(mockTaskConfig{
			nq:          1,
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				return nil
			},
		})

		s.NotPanics(func() {
			scheduler.consumeRecvChan(addTaskReq{
				task: task,
				err:  make(chan error, 1),
			}, maxReceiveChanBatchConsumeNum, time.Now(), nil)
		})
	})
}

func (s *SchedulerSuite) TestConsumeRecvChanUsesLoopTimestampForBatch() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		receiveChan:      make(chan addTaskReq, 1),
		schedulerCounter: schedulerCounter{},
	}

	firstErrCh := make(chan error, 1)
	secondErrCh := make(chan error, 1)
	secondTask := newMockTask(mockTaskConfig{nq: 1})
	scheduler.receiveChan <- addTaskReq{
		task: secondTask,
		err:  secondErrCh,
	}

	scheduler.consumeRecvChan(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  firstErrCh,
	}, 2, now, nil)

	s.NoError(<-firstErrCh)
	s.NoError(<-secondErrCh)

	first := scheduler.policy.Pop(now)
	second := scheduler.policy.Pop(now)
	s.True(first.valid())
	s.True(second.valid())
	s.Equal(now, first.enqueueTime)
	s.Equal(now, second.enqueueTime)
}

func (s *SchedulerSuite) TestHandleAddTaskRequestRejectsWhenWaitingQueueFull() {
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, time.Now(), nil)
	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())

	errCh = make(chan error, 1)
	keepConsuming = scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, time.Now(), nil)
	s.False(keepConsuming)
	s.ErrorIs(<-errCh, merr.ErrServiceTooManyRequests)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
}

func (s *SchedulerSuite) TestHandleAddTaskRequestCleansExpiredTasksBeforeQueueLimit() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancelExpired()
	expiredTask := newMockTask(mockTaskConfig{ctx: expiredCtx, nq: 1})
	queued := newQueuedTask(expiredTask, now.Add(-time.Second))
	added, err := scheduler.policy.Push(queued)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queued, int64(added), queued.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, now, nil)

	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.ErrorIs(expiredTask.Wait(), context.DeadlineExceeded)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
}

func (s *SchedulerSuite) TestHandleAddTaskRequestRechecksCancellationAfterCleanupBeforeMerge() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	active := newQueuedTask(newMockTask(mockTaskConfig{mergeAble: true, nq: 1}), now)
	added, err := scheduler.policy.Push(active)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(active, int64(added), active.NQ())

	incomingCtx, cancelIncoming := context.WithCancel(context.Background())
	defer cancelIncoming()
	incoming := newMockTask(mockTaskConfig{ctx: incomingCtx, mergeAble: true, nq: 2})

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancelExpired()
	expiredBase := newMockTask(mockTaskConfig{ctx: expiredCtx, nq: 1})
	expired := newQueuedTask(&doneCallbackTask{
		Task:     expiredBase,
		callback: cancelIncoming,
	}, now.Add(-time.Second))
	added, err = scheduler.policy.Push(expired)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(expired, int64(added), expired.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: incoming,
		err:  errCh,
	}, 2, now, nil)

	s.True(keepConsuming)
	s.ErrorIs(<-errCh, context.Canceled)
	s.ErrorIs(expiredBase.Wait(), context.DeadlineExceeded)
	remaining := scheduler.policy.Pop(now)
	s.Equal(int64(1), remaining.NQ())
	s.Equal(1, remaining.originalRequestCount)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
	s.Equal(int64(1), scheduler.GetWaitingTaskTotalNQ())
}

func (s *SchedulerSuite) TestHandleAddTaskRequestReturnsCancellationWhenCleanupLeavesQueueFull() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	active := newQueuedTask(newMockTask(mockTaskConfig{nq: 1}), now)
	added, err := scheduler.policy.Push(active)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(active, int64(added), active.NQ())

	incomingCtx, cancelIncoming := context.WithCancel(context.Background())
	defer cancelIncoming()
	incoming := newMockTask(mockTaskConfig{ctx: incomingCtx, nq: 1})

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancelExpired()
	expiredBase := newMockTask(mockTaskConfig{ctx: expiredCtx, nq: 1})
	staged := newQueuedTask(&doneCallbackTask{
		Task:     expiredBase,
		callback: cancelIncoming,
	}, now.Add(-time.Second))
	scheduler.updateWaitingTaskCounter(staged, 1, staged.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: incoming,
		err:  errCh,
	}, 1, now, &staged)

	s.False(keepConsuming)
	s.ErrorIs(<-errCh, context.Canceled)
	s.False(staged.valid())
	s.ErrorIs(expiredBase.Wait(), context.DeadlineExceeded)
	s.Equal(1, scheduler.policy.Len())
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
}

func (s *SchedulerSuite) TestHandleAddTaskRequestSkipsCleanupBeforeQueueFull() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancelExpired()
	expiredTask := newMockTask(mockTaskConfig{ctx: expiredCtx, nq: 1})
	queued := newQueuedTask(expiredTask, now.Add(-time.Second))
	added, err := scheduler.policy.Push(queued)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queued, int64(added), queued.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 2, now, nil)

	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.Equal(int64(2), scheduler.GetWaitingTaskTotal())
	s.Equal(0, len(expiredTask.(*MockTask).notifier))
}

func (s *SchedulerSuite) TestHandleAddTaskRequestCleansTasksNearDeadlineBeforeQueueLimit() {
	paramtable.Init()
	old := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskDeadlineAdvance.SwapTempValue("50ms")
	defer paramtable.Get().QueryNodeCfg.SchedulePolicyTaskDeadlineAdvance.SwapTempValue(old)

	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(30*time.Millisecond))
	defer cancel()
	nearDeadlineTask := newMockTask(mockTaskConfig{ctx: ctx, nq: 1})
	queued := newQueuedTask(nearDeadlineTask, now.Add(-time.Second))
	added, err := scheduler.policy.Push(queued)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queued, int64(added), queued.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, now, nil)

	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.ErrorIs(nearDeadlineTask.Wait(), context.DeadlineExceeded)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
}

func (s *SchedulerSuite) TestHandleAddTaskRequestCleansExpiredStagedTaskBeforeQueueLimit() {
	paramtable.Init()
	metrics.QueryNodeReadTaskQueueDuration.Reset()
	defer metrics.QueryNodeReadTaskQueueDuration.Reset()

	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		execChan:         make(chan Task),
		schedulerCounter: schedulerCounter{},
	}

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(time.Hour))
	defer cancel()
	expiredTask := newMockTask(mockTaskConfig{ctx: ctx, nq: 2})
	queued := newQueuedTask(expiredTask, now)
	added, err := scheduler.policy.Push(queued)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queued, int64(added), queued.NQ())

	staged, _, _ := scheduler.setupExecListener(nil, now)
	s.True(staged.valid())
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 3}),
		err:  errCh,
	}, 1, now.Add(2*time.Hour), &staged)

	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.False(staged.valid())
	s.ErrorIs(expiredTask.Wait(), context.DeadlineExceeded)
	s.Equal(1, scheduler.policy.Len())
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
	s.Equal(int64(3), scheduler.GetWaitingTaskTotalNQ())
	s.Equal(uint64(1), readTaskQueueDurationCount(readTaskQueueOutcomeScheduled))
	s.Equal(uint64(0), readTaskQueueDurationCount(readTaskQueueOutcomeExpired))
}

func (s *SchedulerSuite) TestAddReturnsContextErrorWhenReceiveBlocks() {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		receiveChan:      make(chan addTaskReq),
		schedulerCounter: schedulerCounter{},
		lifetime:         lifetime.NewLifetime(lifetime.Working),
	}

	err := scheduler.Add(newMockTask(mockTaskConfig{ctx: ctx, nq: 1}))
	s.ErrorIs(err, context.DeadlineExceeded)
}

func (s *SchedulerSuite) TestHandleAddTaskRequestDoesNotRejectByQueueDelayDeadline() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	queued := newQueuedTask(newMockTask(mockTaskConfig{nq: 1}), now.Add(-time.Second))
	newTaskAdded, err := scheduler.policy.Push(queued)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queued, int64(newTaskAdded), queued.NQ())

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(100*time.Millisecond))
	defer cancel()

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{ctx: ctx, nq: 1}),
		err:  errCh,
	}, 0, now, nil)

	s.True(keepConsuming)
	s.NoError(<-errCh)
	s.Equal(int64(2), scheduler.GetWaitingTaskTotal())
}

func (s *SchedulerSuite) TestHandleAddTaskRequestAcceptsDeadlineWhenQueueEmpty() {
	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(100*time.Millisecond))
	defer cancel()

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{ctx: ctx, nq: 1}),
		err:  errCh,
	}, 0, now, nil)

	s.True(keepConsuming)
	s.NoError(<-errCh)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
}

func (s *SchedulerSuite) TestSetupExecListenerRecordsPoppedExpiredTask() {
	paramtable.Init()
	metrics.QueryNodeReadTaskQueueDuration.Reset()
	defer metrics.QueryNodeReadTaskQueueDuration.Reset()

	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		execChan:         make(chan Task),
		schedulerCounter: schedulerCounter{},
	}

	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancelExpired()
	expiredTask := newMockTask(mockTaskConfig{ctx: expiredCtx, nq: 1})
	queued := newQueuedTask(expiredTask, now.Add(-time.Second))
	added, err := scheduler.policy.Push(queued)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queued, int64(added), queued.NQ())

	task, nq, execChan := scheduler.setupExecListener(nil, now)

	s.False(task.valid())
	s.Zero(nq)
	s.Nil(execChan)
	s.Equal(int64(0), scheduler.GetWaitingTaskTotal())
	s.ErrorIs(expiredTask.Wait(), context.DeadlineExceeded)
	s.Equal(uint64(1), readTaskQueueDurationCount(readTaskQueueOutcomeExpired))
	s.Equal(uint64(0), readTaskQueueDurationCount(readTaskQueueOutcomeScheduled))
}

func (s *SchedulerSuite) TestClearQueuedTasksRemovesPolicyAndCurrentTask() {
	paramtable.Init()
	metrics.QueryNodeReadTaskQueueDuration.Reset()
	defer metrics.QueryNodeReadTaskQueueDuration.Reset()

	now := time.Now()
	scheduler := &scheduler{
		policy:           newFIFOPolicy(),
		execChan:         make(chan Task),
		schedulerCounter: schedulerCounter{},
	}

	policyTask := newMockTask(mockTaskConfig{username: "clear", nq: 3})
	keepTask := newMockTask(mockTaskConfig{username: "keep", nq: 5})
	currentTask := newQueuedTask(newMockTask(mockTaskConfig{username: "clear", nq: 7}), now.Add(-time.Second))

	queuedPolicyTask := newQueuedTask(policyTask, now.Add(-time.Second))
	added, err := scheduler.policy.Push(queuedPolicyTask)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queuedPolicyTask, int64(added), queuedPolicyTask.NQ())
	queuedKeepTask := newQueuedTask(keepTask, now.Add(-time.Second))
	added, err = scheduler.policy.Push(queuedKeepTask)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(queuedKeepTask, int64(added), queuedKeepTask.NQ())
	scheduler.updateWaitingTaskCounter(currentTask, 1, currentTask.NQ())

	result, remaining := scheduler.clearQueuedTasks(func(task Task) bool {
		return task.Username() == "clear"
	}, "test", currentTask, now)

	s.Equal(ClearResult{QueuedCleared: 2, QueuedNQCleared: 10}, result)
	s.False(remaining.valid())
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
	s.Equal(int64(5), scheduler.GetWaitingTaskTotalNQ())
	s.ErrorIs(policyTask.Wait(), context.Canceled)
	s.ErrorContains(currentTask.Task.(*MockTask).Wait(), "read task queue cleared by admin: test")
	s.Equal(uint64(2), readTaskQueueDurationCount(readTaskQueueOutcomeCleared))
	s.Same(keepTask, scheduler.policy.Pop(now).Task)
}

func (s *SchedulerSuite) TestExecRecordsReadTaskExecuteDuration() {
	paramtable.Init()
	metrics.QueryNodeReadTaskExecuteDuration.Reset()
	defer metrics.QueryNodeReadTaskExecuteDuration.Reset()

	scheduler := newScheduler(newFIFOPolicy())
	scheduler.Start()
	defer scheduler.Stop()

	successTask := newMockTask(mockTaskConfig{
		executeCost: time.Millisecond,
		execution: func(ctx context.Context) error {
			return nil
		},
	})
	s.NoError(scheduler.Add(successTask))
	s.NoError(successTask.(*MockTask).Wait())

	expectedErr := errors.New("mock execute failure")
	failedTask := newMockTask(mockTaskConfig{
		executeCost: time.Millisecond,
		execution: func(ctx context.Context) error {
			return expectedErr
		},
	})
	s.NoError(scheduler.Add(failedTask))
	s.ErrorIs(failedTask.(*MockTask).Wait(), expectedErr)

	canceledTask := newMockTask(mockTaskConfig{
		executeCost: time.Millisecond,
		execution: func(ctx context.Context) error {
			return context.DeadlineExceeded
		},
	})
	s.NoError(scheduler.Add(canceledTask))
	s.ErrorIs(canceledTask.(*MockTask).Wait(), context.DeadlineExceeded)

	s.Equal(uint64(1), readTaskExecuteDurationCount(metrics.SuccessLabel))
	s.Equal(uint64(1), readTaskExecuteDurationCount(metrics.FailLabel))
	s.Equal(uint64(1), readTaskExecuteDurationCount(metrics.CancelLabel))
}

func (s *SchedulerSuite) TestQueuedTaskTimingHelpers() {
	now := time.Now()
	invalid := &queuedTask{}
	s.Zero(invalid.queueDuration(now))
	s.False(invalid.cleanupReady(now))

	taskWithoutEnqueueTime := newQueuedTask(newMockTask(mockTaskConfig{nq: 1}), time.Time{})
	s.Zero(taskWithoutEnqueueTime.queueDuration(now))
	s.False(taskWithoutEnqueueTime.cleanupReady(now))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceledTask := newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx, nq: 1}), now.Add(-time.Millisecond))
	s.True(canceledTask.cleanupReady(now))
}

func (s *SchedulerSuite) TestRecordReadTaskQueueDurationSkipsInvalidTask() {
	paramtable.Init()
	metrics.QueryNodeReadTaskQueueDuration.Reset()
	defer metrics.QueryNodeReadTaskQueueDuration.Reset()

	scheduler := &scheduler{}
	scheduler.recordReadTaskQueueDuration(&queuedTask{}, time.Now(), readTaskQueueOutcomeScheduled)

	observer := metrics.QueryNodeReadTaskQueueDuration.WithLabelValues(paramtable.GetStringNodeID(), readTaskQueueOutcomeScheduled)
	metric := &dto.Metric{}
	s.NoError(observer.(interface{ Write(*dto.Metric) error }).Write(metric))
	s.Equal(uint64(0), metric.GetHistogram().GetSampleCount())
}

func readTaskRequeryQueueDurationCount(outcome string) uint64 {
	observer := metrics.QueryNodeReadTaskRequeryQueueDuration.WithLabelValues(paramtable.GetStringNodeID(), outcome)
	metric := &dto.Metric{}
	if err := observer.(interface{ Write(*dto.Metric) error }).Write(metric); err != nil {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func readTaskExecuteDurationCount(outcome string) uint64 {
	observer := metrics.QueryNodeReadTaskExecuteDuration.WithLabelValues(paramtable.GetStringNodeID(), outcome)
	metric := &dto.Metric{}
	if err := observer.(interface{ Write(*dto.Metric) error }).Write(metric); err != nil {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}

func readTaskQueueDurationCount(outcome string) uint64 {
	observer := metrics.QueryNodeReadTaskQueueDuration.WithLabelValues(paramtable.GetStringNodeID(), outcome)
	metric := &dto.Metric{}
	if err := observer.(interface{ Write(*dto.Metric) error }).Write(metric); err != nil {
		return 0
	}
	return metric.GetHistogram().GetSampleCount()
}
