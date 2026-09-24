package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRequeryPriorityPolicyClassify(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("2")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	assert.Equal(t, taskClassRegular, policy.Classify(newMockTask(mockTaskConfig{})))
	assert.Equal(t, taskClassPriorityLane, policy.Classify(newMockTask(mockTaskConfig{ctx: requeryCtx()})))

	pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("0")
	assert.Equal(t, taskClassRegular, policy.Classify(newMockTask(mockTaskConfig{ctx: requeryCtx()})))
}

func TestRequeryPriorityPolicyRoutesByStamp(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	regularTask := newMockTask(mockTaskConfig{ctx: requeryCtx()})
	laneTask := newMockTask(mockTaskConfig{})
	regular := newQueuedTask(regularTask, time.Now())
	lane := newQueuedTask(laneTask, time.Now())
	lane.class = taskClassPriorityLane

	added, err := policy.Push(regular)
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	added, err = policy.Push(lane)
	require.NoError(t, err)
	assert.Equal(t, 1, added)

	assert.Equal(t, 1, policy.inner.Len())
	assert.Equal(t, 1, policy.lane.len())
	assert.Equal(t, laneTask, policy.Pop(time.Now()).Task)
	assert.Equal(t, regularTask, policy.Pop(time.Now()).Task)
}

// popServed pops a task and, when it is live, reports it served — mirroring
// the scheduler, which reports execChan handoff for live tasks and drops
// canceled ones without serving them.
func popServed(policy *requeryPriorityPolicy, now time.Time) *queuedTask {
	task := policy.Pop(now)
	if task.valid() && task.Context().Err() == nil {
		policy.onTaskServed(task)
	}
	return task
}

func TestRequeryPriorityPolicyBoundedBurst(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	regular1 := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	regular2 := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	laneTasks := make([]*queuedTask, 4)
	for i := range laneTasks {
		laneTasks[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		laneTasks[i].class = taskClassPriorityLane
	}

	for _, task := range append([]*queuedTask{regular1, regular2}, laneTasks...) {
		_, err := policy.Push(task)
		require.NoError(t, err)
	}

	expected := []Task{
		laneTasks[0].Task,
		laneTasks[1].Task,
		laneTasks[2].Task,
		regular1.Task,
		laneTasks[3].Task,
		regular2.Task,
	}
	for _, expectedTask := range expected {
		assert.Equal(t, expectedTask, popServed(policy, now).Task)
	}
	assert.False(t, policy.Pop(now).valid())
}

func TestRequeryPriorityPolicyFallsBackAndDrainsWhenDisabled(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	oldLane := pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("4")
	defer pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue(oldLane)

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	tasks := make([]*queuedTask, 3)
	for i := range tasks {
		tasks[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		tasks[i].class = taskClassPriorityLane
		_, err := policy.Push(tasks[i])
		require.NoError(t, err)
	}
	expected := []Task{tasks[0].Task, tasks[1].Task, tasks[2].Task}

	pt.QueryNodeCfg.RequeryUnsolvedQueueSize.SwapTempValue("0")
	for _, expectedTask := range expected {
		assert.Equal(t, expectedTask, popServed(policy, now).Task)
	}
	assert.False(t, policy.Pop(now).valid())
}

func TestRequeryPriorityPolicyCountsFallbackInNextBurst(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	laneTasks := make([]*queuedTask, 7)
	laneTaskValues := make([]Task, len(laneTasks))
	for i := range laneTasks {
		laneTasks[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		laneTaskValues[i] = laneTasks[i].Task
		laneTasks[i].class = taskClassPriorityLane
		_, err := policy.Push(laneTasks[i])
		require.NoError(t, err)
	}

	assert.Equal(t, laneTaskValues[0], popServed(policy, now).Task)
	assert.Equal(t, laneTaskValues[1], popServed(policy, now).Task)
	assert.Equal(t, laneTaskValues[2], popServed(policy, now).Task)
	// inner is empty and base credit is exhausted, so this task is selected
	// through the fallback path as the first task in a new base window.
	assert.Equal(t, laneTaskValues[3], popServed(policy, now).Task)

	regular := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	regularTask := regular.Task
	_, err := policy.Push(regular)
	require.NoError(t, err)

	// The fallback task already consumed one of the K=3 base credits. A late
	// regular task therefore waits for only two additional lane tasks.
	assert.Equal(t, laneTaskValues[4], popServed(policy, now).Task)
	assert.Equal(t, laneTaskValues[5], popServed(policy, now).Task)
	assert.Equal(t, regularTask, popServed(policy, now).Task)
	assert.Equal(t, laneTaskValues[6], popServed(policy, now).Task)
}

func TestRequeryPriorityPolicyCanceledRegularDoesNotRefreshCredit(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	canceledCtx, cancel := context.WithCancel(context.Background())
	canceled := newQueuedTask(newMockTask(mockTaskConfig{ctx: canceledCtx}), now)
	live := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	canceledTask := canceled.Task
	liveTask := live.Task
	for _, task := range []*queuedTask{canceled, live} {
		_, err := policy.Push(task)
		require.NoError(t, err)
	}

	laneTasks := make([]*queuedTask, 4)
	laneTaskValues := make([]Task, len(laneTasks))
	for i := range laneTasks {
		laneTasks[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		laneTaskValues[i] = laneTasks[i].Task
		laneTasks[i].class = taskClassPriorityLane
		_, err := policy.Push(laneTasks[i])
		require.NoError(t, err)
	}
	cancel()

	for i := 0; i < 3; i++ {
		assert.Equal(t, laneTaskValues[i], popServed(policy, now).Task)
	}
	assert.Equal(t, canceledTask, popServed(policy, now).Task)
	assert.Equal(t, liveTask, popServed(policy, now).Task)
	assert.Equal(t, laneTaskValues[3], popServed(policy, now).Task)
}

func TestRequeryPriorityPolicyUsesConfiguredBaseCredit(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	mergedRegulars := make([]*queuedTask, 5)
	for i := range mergedRegulars {
		mergedRegulars[i] = newQueuedTask(newMockTask(mockTaskConfig{mergeAble: true, nq: 1}), now)
		added, err := policy.Push(mergedRegulars[i])
		require.NoError(t, err)
		if i == 0 {
			assert.Equal(t, 1, added)
		} else {
			assert.Zero(t, added)
		}
	}
	nextRegular := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	mergedRegularTask := mergedRegulars[0].Task
	nextRegularTask := nextRegular.Task
	_, err := policy.Push(nextRegular)
	require.NoError(t, err)

	innerQueue := policy.inner.(*fifoPolicy).queue
	assert.Equal(t, 2, innerQueue.len())

	laneTasks := make([]*queuedTask, 9)
	laneTaskValues := make([]Task, len(laneTasks))
	for i := range laneTasks {
		laneTasks[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		laneTaskValues[i] = laneTasks[i].Task
		laneTasks[i].class = taskClassPriorityLane
		_, err := policy.Push(laneTasks[i])
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		assert.Equal(t, laneTaskValues[i], popServed(policy, now).Task)
	}
	assert.Equal(t, mergedRegularTask, popServed(policy, now).Task)
	for i := 3; i < 6; i++ {
		assert.Equal(t, laneTaskValues[i], popServed(policy, now).Task)
	}
	assert.Equal(t, nextRegularTask, popServed(policy, now).Task)
	for i := 6; i < 9; i++ {
		assert.Equal(t, laneTaskValues[i], popServed(policy, now).Task)
	}
}

func TestRequeryPriorityPolicyCreditGrantedAtServeNotPop(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	policy.requeryCredit = 0
	now := time.Now()
	regular1 := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	regular2 := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	lane := newQueuedTask(newMockTask(mockTaskConfig{}), now)
	lane.class = taskClassPriorityLane
	regular1Task, regular2Task, laneTask := regular1.Task, regular2.Task, lane.Task
	for _, task := range []*queuedTask{regular1, regular2, lane} {
		_, err := policy.Push(task)
		require.NoError(t, err)
	}

	// Popping a regular task grants nothing: a staged task can still be
	// dropped. Until one regular task is actually served, regular tasks keep
	// absolute priority over the lane.
	assert.Equal(t, regular1Task, policy.Pop(now).Task)
	assert.Zero(t, policy.requeryCredit)
	served := policy.Pop(now)
	assert.Equal(t, regular2Task, served.Task)
	assert.Zero(t, policy.requeryCredit)

	policy.onTaskServed(served)
	assert.Equal(t, requeryPriorityBaseCredit(), policy.requeryCredit)
	assert.Equal(t, laneTask, policy.Pop(now).Task)
	assert.Equal(t, requeryPriorityBaseCredit()-1, policy.requeryCredit)
}

func TestRequeryPriorityPolicyDynamicBaseCredit(t *testing.T) {
	paramtable.Init()
	item := &paramtable.Get().QueryNodeCfg.RequeryPriorityBaseCredit
	old := item.SwapTempValue("2")
	defer item.SwapTempValue(old)

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	assert.Equal(t, 2, policy.requeryCredit)
	now := time.Now()
	regular := make([]*queuedTask, 3)
	for i := range regular {
		regular[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		_, err := policy.Push(regular[i])
		require.NoError(t, err)
	}
	lane := make([]*queuedTask, 9)
	for i := range lane {
		lane[i] = newQueuedTask(newMockTask(mockTaskConfig{}), now)
		lane[i].class = taskClassPriorityLane
		_, err := policy.Push(lane[i])
		require.NoError(t, err)
	}

	for i := 0; i < 2; i++ {
		assert.Same(t, lane[i], policy.Pop(now))
	}
	served := policy.Pop(now)
	assert.Same(t, regular[0], served)
	item.SwapTempValue("5")
	policy.onTaskServed(served)
	assert.Equal(t, 5, policy.requeryCredit)
	for i := 2; i < 7; i++ {
		assert.Same(t, lane[i], policy.Pop(now))
	}

	served = policy.Pop(now)
	assert.Same(t, regular[1], served)
	item.SwapTempValue("1")
	policy.onTaskServed(served)
	assert.Equal(t, 1, policy.requeryCredit)
	assert.Same(t, lane[7], policy.Pop(now))
	assert.Same(t, regular[2], policy.Pop(now))
}

func TestRequeryPriorityPolicyPreservesInnerMergeAndKeepsLaneSeparate(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	regular1 := newQueuedTask(newMockTask(mockTaskConfig{mergeAble: true, nq: 1}), now)
	regular2 := newQueuedTask(newMockTask(mockTaskConfig{mergeAble: true, nq: 1}), now)
	added, err := policy.Push(regular1)
	require.NoError(t, err)
	assert.Equal(t, 1, added)
	added, err = policy.Push(regular2)
	require.NoError(t, err)
	assert.Equal(t, 0, added)
	assert.Equal(t, 1, policy.inner.Len())

	lane1 := newQueuedTask(newMockTask(mockTaskConfig{mergeAble: true, nq: 1}), now)
	lane2 := newQueuedTask(newMockTask(mockTaskConfig{mergeAble: true, nq: 1}), now)
	lane1.class = taskClassPriorityLane
	lane2.class = taskClassPriorityLane
	_, err = policy.Push(lane1)
	require.NoError(t, err)
	_, err = policy.Push(lane2)
	require.NoError(t, err)
	assert.Equal(t, 2, policy.lane.len())
	assert.Equal(t, 3, policy.Len())
}

func TestRequeryPriorityPolicyCleanupRemoveAndLen(t *testing.T) {
	paramtable.Init()

	policy := newRequeryPriorityPolicy(newFIFOPolicy())
	now := time.Now()
	canceledCtx, cancel := context.WithCancel(context.Background())
	canceledTask := newMockTask(mockTaskConfig{ctx: canceledCtx, username: "canceled"})
	removeTask := newMockTask(mockTaskConfig{username: "remove"})
	keepTask := newMockTask(mockTaskConfig{username: "keep"})
	canceled := newQueuedTask(canceledTask, now)
	canceled.class = taskClassPriorityLane
	remove := newQueuedTask(removeTask, now)
	keep := newQueuedTask(keepTask, now)
	keep.class = taskClassPriorityLane

	for _, task := range []*queuedTask{canceled, remove, keep} {
		_, err := policy.Push(task)
		require.NoError(t, err)
	}
	assert.Equal(t, 3, policy.Len())

	cancel()
	cleaned := policy.Cleanup(now)
	require.Len(t, cleaned, 1)
	assert.Equal(t, canceledTask, cleaned[0].Task)
	assert.Equal(t, 2, policy.Len())

	removed := policy.Remove(func(task Task) bool { return task.Username() == "remove" }, now)
	require.Len(t, removed, 1)
	assert.Equal(t, removeTask, removed[0].Task)
	assert.Equal(t, 1, policy.Len())
	assert.Equal(t, keepTask, policy.Pop(now).Task)
}
