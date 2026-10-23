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

func requeryPriorityTestTask(requery bool) *queuedTask {
	ctx := context.Background()
	if requery {
		ctx = contextutil.WithQueryLabel(ctx, metrics.ReQueryLabel)
	}
	return newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx}), time.Now())
}

func pushPriorityTasks(t *testing.T, policy schedulePolicy, regular, requery int) {
	t.Helper()
	for range regular {
		_, err := policy.Push(requeryPriorityTestTask(false))
		require.NoError(t, err)
	}
	for range requery {
		_, err := policy.Push(requeryPriorityTestTask(true))
		require.NoError(t, err)
	}
}

func priorityTaskLane(task *queuedTask) string {
	if contextutil.GetQueryLabel(task.Context()) == metrics.ReQueryLabel {
		return "Q"
	}
	return "R"
}

func TestNewSchedulerSupportsRequeryPriority(t *testing.T) {
	paramtable.Init()
	var scheduler Scheduler
	require.NotPanics(t, func() {
		scheduler = NewScheduler(schedulePolicyNameRequeryPriority)
	})
	require.NotNil(t, scheduler)
	scheduler.Stop()
	require.Panics(t, func() { NewScheduler("unknown") })
}

func TestRequeryPriorityCreditSequence(t *testing.T) {
	paramtable.Init()
	credit := &paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit
	old := credit.SwapTempValue("2")
	t.Cleanup(func() { credit.SwapTempValue(old) })

	policy := newRequeryPriorityPolicy()
	pushPriorityTasks(t, policy, 3, 6)
	want := []string{"Q", "Q", "R", "Q", "Q", "R"}
	for _, lane := range want {
		task := policy.Pop(time.Now())
		require.Equal(t, lane, priorityTaskLane(task))
		policy.onTaskServed(task)
	}
}

func TestRequeryPriorityIsWorkConserving(t *testing.T) {
	paramtable.Init()
	credit := &paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit
	old := credit.SwapTempValue("2")
	t.Cleanup(func() { credit.SwapTempValue(old) })

	t.Run("requery only does not consume credit", func(t *testing.T) {
		policy := newRequeryPriorityPolicy()
		pushPriorityTasks(t, policy, 0, 3)
		for range 3 {
			require.Equal(t, "Q", priorityTaskLane(policy.Pop(time.Now())))
		}
		require.EqualValues(t, 2, policy.remainingCredit)
	})

	t.Run("regular only runs immediately", func(t *testing.T) {
		policy := newRequeryPriorityPolicy()
		pushPriorityTasks(t, policy, 2, 0)
		for range 2 {
			require.Equal(t, "R", priorityTaskLane(policy.Pop(time.Now())))
		}
	})
}

func TestRequeryPriorityHotAppliesCreditWithoutFreshBurst(t *testing.T) {
	paramtable.Init()
	credit := &paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit
	old := credit.SwapTempValue("3")
	t.Cleanup(func() { credit.SwapTempValue(old) })

	t.Run("increase preserves consumed credit", func(t *testing.T) {
		policy := newRequeryPriorityPolicy()
		pushPriorityTasks(t, policy, 4, 6)
		require.Equal(t, "Q", priorityTaskLane(policy.Pop(time.Now())))
		require.Equal(t, "Q", priorityTaskLane(policy.Pop(time.Now())))
		credit.SwapTempValue("5")
		require.Equal(t, "Q", priorityTaskLane(policy.Pop(time.Now())))
		require.EqualValues(t, 5, policy.configuredCredit)
		require.EqualValues(t, 2, policy.remainingCredit)
	})

	t.Run("decrease can require regular immediately", func(t *testing.T) {
		credit.SwapTempValue("3")
		policy := newRequeryPriorityPolicy()
		pushPriorityTasks(t, policy, 4, 6)
		require.Equal(t, "Q", priorityTaskLane(policy.Pop(time.Now())))
		require.Equal(t, "Q", priorityTaskLane(policy.Pop(time.Now())))
		credit.SwapTempValue("1")
		require.Equal(t, "R", priorityTaskLane(policy.Pop(time.Now())))
		require.Zero(t, policy.remainingCredit)
	})
}

func TestRequeryPriorityReplenishesOnlyAfterRegularHandoff(t *testing.T) {
	paramtable.Init()
	credit := &paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit
	old := credit.SwapTempValue("1")
	t.Cleanup(func() { credit.SwapTempValue(old) })

	policy := newRequeryPriorityPolicy()
	pushPriorityTasks(t, policy, 2, 2)
	requery := policy.Pop(time.Now())
	require.Equal(t, "Q", priorityTaskLane(requery))
	require.Zero(t, policy.remainingCredit)
	policy.onTaskServed(requery)
	require.Zero(t, policy.remainingCredit)

	regular := policy.Pop(time.Now())
	require.Equal(t, "R", priorityTaskLane(regular))
	require.Zero(t, policy.remainingCredit, "Pop is not a successful handoff")
	policy.onTaskServed(regular)
	require.EqualValues(t, 1, policy.remainingCredit)
}

func TestRequeryPriorityLanesCleanupRemoveAndCapacity(t *testing.T) {
	paramtable.Init()
	policy := newRequeryPriorityPolicy()
	now := time.Now()
	expiredCtx, cancel := context.WithDeadline(context.Background(), now.Add(-time.Second))
	t.Cleanup(cancel)
	expired := newQueuedTask(newMockTask(mockTaskConfig{ctx: contextutil.WithQueryLabel(expiredCtx, metrics.ReQueryLabel)}), now)
	expiredTask := expired.Task
	_, err := policy.Push(expired)
	require.NoError(t, err)
	regular := requeryPriorityTestTask(false)
	regularTask := regular.Task
	_, err = policy.Push(regular)
	require.NoError(t, err)

	removed := policy.Cleanup(now)
	require.Len(t, removed, 1)
	require.Same(t, expiredTask, removed[0].Task)
	require.Equal(t, 1, policy.Len())
	removed = policy.Remove(nil, now)
	require.Len(t, removed, 1)
	require.Same(t, regularTask, removed[0].Task)
	require.Zero(t, policy.Len())

	for i := int64(0); i < policy.requeryCapacity; i++ {
		task := requeryPriorityTestTask(true)
		require.NoError(t, policy.CheckAdmission(task.Task, int64(policy.Len())))
		_, err = policy.Push(task)
		require.NoError(t, err)
	}
	err = policy.CheckAdmission(requeryPriorityTestTask(true).Task, int64(policy.Len()))
	require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
}
