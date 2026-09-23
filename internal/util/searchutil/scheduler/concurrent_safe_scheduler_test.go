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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
			}, maxReceiveChanBatchConsumeNum, time.Now())
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
	}, 2, now)

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
	}, 1, time.Now())
	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())

	errCh = make(chan error, 1)
	keepConsuming = scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, time.Now())
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
	scheduler.updateWaitingTaskCounter(int64(added), queued.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, now)

	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.ErrorIs(expiredTask.Wait(), context.DeadlineExceeded)
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
	scheduler.updateWaitingTaskCounter(int64(added), queued.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 2, now)

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
	scheduler.updateWaitingTaskCounter(int64(added), queued.NQ())

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{nq: 1}),
		err:  errCh,
	}, 1, now)

	s.False(keepConsuming)
	s.NoError(<-errCh)
	s.ErrorIs(nearDeadlineTask.Wait(), context.DeadlineExceeded)
	s.Equal(int64(1), scheduler.GetWaitingTaskTotal())
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
	scheduler.updateWaitingTaskCounter(int64(newTaskAdded), queued.NQ())

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(100*time.Millisecond))
	defer cancel()

	errCh := make(chan error, 1)
	keepConsuming := scheduler.handleAddTaskRequest(addTaskReq{
		task: newMockTask(mockTaskConfig{ctx: ctx, nq: 1}),
		err:  errCh,
	}, 0, now)

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
	}, 0, now)

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
	scheduler.updateWaitingTaskCounter(int64(added), queued.NQ())

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
	scheduler.updateWaitingTaskCounter(int64(added), queuedPolicyTask.NQ())
	queuedKeepTask := newQueuedTask(keepTask, now.Add(-time.Second))
	added, err = scheduler.policy.Push(queuedKeepTask)
	s.NoError(err)
	scheduler.updateWaitingTaskCounter(int64(added), queuedKeepTask.NQ())
	scheduler.updateWaitingTaskCounter(1, currentTask.NQ())

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

// TestPruneCanceledBeforeExec verifies that a prunable task is asked to drop
// its canceled members at dequeue and again right before execution, that a
// group with no survivor is never executed, that the survivor is what gets
// executed, and that the waiting counters are debited with the NQ they were
// credited with, not with the pruned NQ.
func (s *SchedulerSuite) TestPruneCanceledBeforeExec() {
	paramtable.Init()
	scheduler := newScheduler(newFIFOPolicy())
	scheduler.Start()
	defer scheduler.Stop()

	s.Run("no survivor is not executed", func() {
		executed := false
		task := newMockTask(mockTaskConfig{
			nq:          5,
			executeCost: time.Millisecond,
			execution: func(ctx context.Context) error {
				executed = true
				return nil
			},
		})
		mock := task.(*MockTask)
		mock.prune = func() (Task, int, error) {
			mock.Done(context.Canceled)
			return nil, 1, context.Canceled
		}
		s.NoError(scheduler.Add(task))
		s.ErrorIs(mock.Wait(), context.Canceled)
		s.False(executed)
		s.Eventually(func() bool {
			return scheduler.GetWaitingTaskTotal() == 0 && scheduler.GetWaitingTaskTotalNQ() == 0
		}, time.Second, 10*time.Millisecond, "counters must return to zero after the pruned NQ is debited")
	})

	s.Run("survivor replaces the dequeued task", func() {
		survivor := newMockTask(mockTaskConfig{
			nq:          2,
			executeCost: time.Millisecond,
			execution: func(ctx context.Context) error {
				return nil
			},
		})
		group := newMockTask(mockTaskConfig{
			nq:          5,
			executeCost: time.Millisecond,
			execution: func(ctx context.Context) error {
				s.Fail("the pruned owner must not execute")
				return nil
			},
		})
		group.(*MockTask).prune = func() (Task, int, error) {
			// the owner was canceled: it is told so, the survivor goes on
			group.(*MockTask).Done(context.Canceled)
			return survivor, 1, context.Canceled
		}
		s.NoError(scheduler.Add(group))
		s.ErrorIs(group.(*MockTask).Wait(), context.Canceled)
		s.NoError(survivor.(*MockTask).Wait())
		s.Eventually(func() bool {
			return scheduler.GetWaitingTaskTotal() == 0 && scheduler.GetWaitingTaskTotalNQ() == 0
		}, time.Second, 10*time.Millisecond, "the group was credited with 5 and must be debited with 5")
	})
}

// TestPruneCanceledRightBeforeExec covers the second pruning point on its own:
// the task is still live when it leaves the queue and is canceled before the
// executor takes it. That point also has to say why it dropped what it
// dropped, since the cause is what separates a client that went away from a
// request that ran out of time.
func (s *SchedulerSuite) TestPruneCanceledRightBeforeExec() {
	paramtable.Init()
	logs := mlog.CaptureGlobalLogs(s.T(), &mlog.Config{
		Level:             "debug",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	})
	scheduler := newScheduler(newFIFOPolicy())
	scheduler.Start()
	defer scheduler.Stop()

	s.Run("dropped whole before it runs", func() {
		task := newMockTask(mockTaskConfig{
			nq:          5,
			executeCost: time.Millisecond,
			execution: func(ctx context.Context) error {
				s.Fail("a task canceled before execution must not run")
				return nil
			},
		})
		mock := task.(*MockTask)
		calls := 0
		mock.prune = func() (Task, int, error) {
			calls++
			if calls == 1 {
				return mock, 0, nil // live when it leaves the queue
			}
			mock.Done(context.Canceled)
			return nil, 1, context.Canceled // canceled before the executor takes it
		}
		s.NoError(scheduler.Add(task))
		s.ErrorIs(mock.Wait(), context.Canceled)
		s.Equal(2, calls, "pruned once at dequeue and once before execution")
		s.Eventually(func() bool {
			return scheduler.GetWaitingTaskTotal() == 0 && scheduler.GetWaitingTaskTotalNQ() == 0
		}, time.Second, 10*time.Millisecond)
		s.Contains(logs.String(), "task canceled before executing")
		s.Contains(logs.String(), context.Canceled.Error())
	})

	s.Run("part of a group dropped, the rest runs", func() {
		survivor := newMockTask(mockTaskConfig{
			nq:          2,
			executeCost: time.Millisecond,
			execution: func(ctx context.Context) error {
				return nil
			},
		})
		group := newMockTask(mockTaskConfig{
			nq:          5,
			executeCost: time.Millisecond,
			execution: func(ctx context.Context) error {
				s.Fail("the canceled owner must not run")
				return nil
			},
		})
		calls := 0
		group.(*MockTask).prune = func() (Task, int, error) {
			calls++
			if calls == 1 {
				return group, 0, nil
			}
			group.(*MockTask).Done(context.Canceled)
			return survivor, 1, context.Canceled
		}
		s.NoError(scheduler.Add(group))
		s.ErrorIs(group.(*MockTask).Wait(), context.Canceled)
		s.NoError(survivor.(*MockTask).Wait(), "the survivor runs")
		s.Eventually(func() bool {
			return scheduler.GetWaitingTaskTotal() == 0 && scheduler.GetWaitingTaskTotalNQ() == 0
		}, time.Second, 10*time.Millisecond, "the group was credited with 5 and must be debited with 5")
		s.Contains(logs.String(), "canceled requests dropped from a search group before executing")
	})
}

// expirableMock stands for a merged group that decides for itself whether all
// of it is done, as a search group does.
type expirableMock struct {
	*MockTask
	ready    bool
	finished int
}

func (m *expirableMock) ExpiryReady(time.Time) bool {
	return m.ready
}

func (m *expirableMock) FinishExpired() {
	m.finished++
	m.Done(context.DeadlineExceeded)
}

// TestExpirySweepAsksTheGroup checks that the sweep, which runs once the queue
// is full, asks a group whether all of it is done instead of reading the
// owner's context, which is the only context the queue holds.
func (s *SchedulerSuite) TestExpirySweepAsksTheGroup() {
	paramtable.Init()
	now := time.Now()
	sched := &scheduler{
		policy:           newFIFOPolicy(),
		schedulerCounter: schedulerCounter{},
	}

	// The owner's client is gone, but another request in the group is not.
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	cancelOwner()
	group := &expirableMock{MockTask: newMockTask(mockTaskConfig{ctx: ownerCtx, nq: 3}).(*MockTask)}
	queued := newQueuedTask(group, now.Add(-time.Second))
	added, err := sched.policy.Push(queued)
	s.NoError(err)
	sched.updateWaitingTaskCounter(int64(added), queued.NQ())

	sched.cleanupExpiredTasks(now)
	s.Zero(group.finished, "a group with a live request stays in the queue")
	s.Equal(int64(1), sched.GetWaitingTaskTotal())
	s.Equal(int64(3), sched.GetWaitingTaskTotalNQ())

	group.ready = true
	sched.cleanupExpiredTasks(now)
	s.Equal(1, group.finished, "once all of it is done it is taken out, as a group")
	s.Zero(sched.GetWaitingTaskTotal())
	s.Zero(sched.GetWaitingTaskTotalNQ())
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
