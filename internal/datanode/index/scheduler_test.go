// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/taskcost"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGetStateFromError(t *testing.T) {
	t.Run("sparse conversion limit is terminal but allocation failures retry", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			code  merr.SegcoreCode
			state indexpb.JobState
		}{
			{"conversion budget exceeded", merr.CodeUnsupported, indexpb.JobState_JobStateFailed},
			{"allocation failed", merr.CodeMemAllocateFailed, indexpb.JobState_JobStateRetry},
			{"other configuration error", merr.CodeConfigInvalid, indexpb.JobState_JobStateRetry},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// Preserve the code through the same wrapper used by index builds.
				err := merr.Wrap(merr.SegcoreError(int32(tc.code), tc.name),
					"failed to create index, C Runtime Exception")
				assert.Equal(t, tc.state, getStateFromError(err))
				assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
			})
		}
	})

	t.Run("data format broken is terminal", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateFailed, getStateFromError(merr.SegcoreError(2024, "malformed vector data")))
	})

	t.Run("generic segcore error still retries", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry, getStateFromError(merr.SegcoreError(2001, "unexpected")))
	})

	t.Run("transient segcore error still retries", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry, getStateFromError(merr.SegcoreError(2045, "transient storage error")))
	})

	// Caller-input failures are a property of the request or of the source data, so
	// the task fails identically on every worker and on every attempt. Retrying them
	// turns one bad document into an unbounded re-dispatch loop.
	t.Run("caller input segcore errors are terminal", func(t *testing.T) {
		for _, tc := range []struct {
			code int32
			name string
		}{
			{2025, "JsonKeyInvalid"},
			{2028, "ExprInvalid"},
			{2032, "DimNotMatch"},
			{2042, "InvalidParameter"},
		} {
			assert.Equalf(t, indexpb.JobState_JobStateFailed,
				getStateFromError(merr.SegcoreError(tc.code, tc.name)),
				"segcore code %d (%s) must fail instead of retrying", tc.code, tc.name)
		}
	})

	// Codes the table marks permanent reproduce identically on every worker, so
	// the task must give up instead of burning a slot per attempt.
	t.Run("permanent segcore errors are terminal", func(t *testing.T) {
		for _, tc := range []struct {
			code int32
			name string
		}{
			{2016, "BucketInvalid"},
			{2017, "ObjectNotExist"},
			{2024, "DataFormatBroken"},
		} {
			assert.Equalf(t, indexpb.JobState_JobStateFailed,
				getStateFromError(merr.SegcoreError(tc.code, tc.name)),
				"segcore code %d (%s) must fail instead of retrying", tc.code, tc.name)
		}
		// wrapped the way indexcgowrapper reports it
		assert.Equal(t, indexpb.JobState_JobStateFailed,
			getStateFromError(errors.Wrap(merr.SegcoreError(2017, "object not exist"),
				"failed to create index, C Runtime Exception")))
	})

	// 2004 and 2044 are raised by broad "operation failed" branches that also
	// carry transient storage and per-node disk failures, so a re-dispatch to
	// another worker can still succeed.
	t.Run("broad failure codes keep retrying", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry,
			getStateFromError(merr.SegcoreError(2004, "failed to build disk index, disk file error")))
		assert.Equal(t, indexpb.JobState_JobStateRetry,
			getStateFromError(merr.SegcoreError(2044, "storage error")))
	})

	// A segment whose meta points at a binlog that is not there is Milvus state
	// being inconsistent, not a malformed request: it stays a system error and is
	// still terminal, because no retry can make the binlog appear.
	t.Run("data integrity failures are terminal but stay system errors", func(t *testing.T) {
		err := merr.WrapErrDataIntegrityMsg("field binlog not found for field %d", 116)
		assert.Equal(t, indexpb.JobState_JobStateFailed, getStateFromError(err))
		assert.NotEqual(t, merr.InputError, merr.GetErrorType(err))
	})

	// DataTypeInvalid / DataIsEmpty look like input validation but their C++
	// producers are predominantly internal guards, so classForCode keeps them
	// system errors and the scheduler keeps retrying them.
	t.Run("mixed semantics codes still retry", func(t *testing.T) {
		for _, code := range []int32{2007, 2023} {
			assert.Equalf(t, indexpb.JobState_JobStateRetry,
				getStateFromError(merr.SegcoreError(code, "internal guard")), "segcore code %d", code)
		}
	})

	t.Run("input error survives wrapping", func(t *testing.T) {
		err := errors.Wrap(merr.SegcoreError(2025, "bad json"), "failed to build json key index")
		assert.Equal(t, indexpb.JobState_JobStateFailed, getStateFromError(err))
	})

	t.Run("parameter invalid raised by the task itself is terminal", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateFailed,
			getStateFromError(merr.WrapErrParameterInvalidMsg("data insert path must be not empty")))
	})

	t.Run("cancel retries and pretend-finished finishes", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry, getStateFromError(errCancel))
		assert.Equal(t, indexpb.JobState_JobStateFinished, getStateFromError(merr.SegcoreError(2033, "cluster skip")))
	})

	t.Run("system errors are unaffected", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry,
			getStateFromError(merr.WrapErrServiceInternalMsg("internal")))
		assert.Equal(t, indexpb.JobState_JobStateFailed,
			getStateFromError(merr.WrapErrIoKeyNotFound("some/key")))
	})
}

type fakeTaskState int

const (
	fakeTaskInited = iota
	fakeTaskEnqueued
	fakeTaskPrepared
	fakeTaskLoadedData
	fakeTaskBuiltIndex
	fakeTaskSavedIndexes
)

type stagectx struct {
	mu           sync.Mutex
	curstate     fakeTaskState
	state2cancel fakeTaskState
	ch           chan struct{}
	closed       bool
}

var _ context.Context = &stagectx{}

func (s *stagectx) Deadline() (time.Time, bool) {
	return time.Now(), false
}

// Done cancels the context once the task reaches state2cancel. It is called
// more than once per stage (the slot gate's Acquire and processTask's wrap
// both look at it), so the close is guarded.
func (s *stagectx) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curstate == s.state2cancel && !s.closed {
		s.closed = true
		close(s.ch)
	}
	return s.ch
}

func (s *stagectx) Err() error {
	select {
	case <-s.ch:
		return errors.New("canceled")
	default:
		return nil
	}
}

func (s *stagectx) Value(k interface{}) interface{} {
	return nil
}

func (s *stagectx) setState(state fakeTaskState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.curstate = state
}

var _taskwg sync.WaitGroup

type fakeTask struct {
	id            int
	ctx           context.Context
	state         fakeTaskState
	reterr        map[fakeTaskState]error
	retstate      indexpb.JobState
	expectedState indexpb.JobState
	failReason    string
}

var _ Task = &fakeTask{}

func (t *fakeTask) Name() string {
	return fmt.Sprintf("fake-task-%d", t.id)
}

func (t *fakeTask) Ctx() context.Context {
	return t.ctx
}

func (t *fakeTask) GetSlot() int64 {
	return 1
}

func (t *fakeTask) GetResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: 1, Memory: 100}
}

func (t *fakeTask) OnEnqueue(ctx context.Context) error {
	_taskwg.Add(1)
	t.state = fakeTaskEnqueued
	t.ctx.(*stagectx).setState(t.state)
	return t.reterr[t.state]
}

func (t *fakeTask) PreExecute(ctx context.Context) error {
	t.state = fakeTaskPrepared
	t.ctx.(*stagectx).setState(t.state)
	return t.reterr[t.state]
}

func (t *fakeTask) LoadData(ctx context.Context) error {
	t.state = fakeTaskLoadedData
	t.ctx.(*stagectx).setState(t.state)
	return t.reterr[t.state]
}

func (t *fakeTask) Execute(ctx context.Context) error {
	t.state = fakeTaskBuiltIndex
	t.ctx.(*stagectx).setState(t.state)
	return t.reterr[t.state]
}

func (t *fakeTask) PostExecute(ctx context.Context) error {
	t.state = fakeTaskSavedIndexes
	t.ctx.(*stagectx).setState(t.state)
	return t.reterr[t.state]
}

func (t *fakeTask) Reset() {
	_taskwg.Done()
}

func (t *fakeTask) SetState(state indexpb.JobState, failReason string) {
	t.retstate = state
	t.failReason = failReason
}

func (t *fakeTask) GetState() indexpb.JobState {
	return t.retstate
}

func (t *fakeTask) IsVectorIndex() bool {
	return false
}

var (
	idLock sync.Mutex
	id     = 0
)

func newTask(cancelStage fakeTaskState, reterror map[fakeTaskState]error, expectedState indexpb.JobState) Task {
	idLock.Lock()
	newID := id
	id++
	idLock.Unlock()

	return &fakeTask{
		reterr: reterror,
		id:     newID,
		ctx: &stagectx{
			curstate:     fakeTaskInited,
			state2cancel: cancelStage,
			ch:           make(chan struct{}),
		},
		state:         fakeTaskInited,
		retstate:      indexpb.JobState_JobStateNone,
		expectedState: expectedState,
	}
}

func TestIndexTaskQueueBudget(t *testing.T) {
	t.Run("weighted queue budget transfers at dispatch", func(t *testing.T) {
		sched := NewTaskScheduler(context.Background())
		t.Cleanup(sched.Close)
		sched.totalSlots = func() int64 { return 128 }
		first := &pipelineTestTask{id: "first", slots: 96, ctx: context.Background()}
		second := &pipelineTestTask{id: "second", slots: 32, ctx: context.Background()}
		require.NoError(t, sched.TaskQueue.Enqueue(first))
		require.NoError(t, sched.TaskQueue.Enqueue(second))
		require.ErrorIs(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
			id: "rejected", slots: 1, ctx: context.Background(),
		}), merr.ErrServiceResourceInsufficient)
		used, queued := sched.TaskQueue.GetSlotUsage()
		require.Zero(t, used)
		require.EqualValues(t, 128, queued)
		require.Nil(t, sched.TaskQueue.PopUnissuedTask(false))
		require.Same(t, first, sched.TaskQueue.PopUnissuedTask(true))
		used, queued = sched.TaskQueue.GetSlotUsage()
		require.EqualValues(t, 96, used)
		require.EqualValues(t, 32, queued)
		sched.finishAdmission(first.Name())
		sched.releaseSlots(first.Name())
		require.Same(t, second, sched.TaskQueue.PopUnissuedTask(true))
		sched.finishAdmission(second.Name())
		sched.releaseSlots(second.Name())
		used, queued = sched.TaskQueue.GetSlotUsage()
		require.Zero(t, used)
		require.Zero(t, queued)
	})

	t.Run("one oversized task can enter an empty queue", func(t *testing.T) {
		sched := NewTaskScheduler(context.Background())
		t.Cleanup(sched.Close)
		sched.totalSlots = func() int64 { return 128 }
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		large := &pipelineTestTask{id: "large", slots: 256, ctx: ctx}
		require.NoError(t, sched.TaskQueue.Enqueue(large))
		require.ErrorIs(t, sched.TaskQueue.Enqueue(&pipelineTestTask{
			id: "later", ctx: context.Background(),
		}), merr.ErrServiceResourceInsufficient)
		cancel()
		require.Same(t, large, sched.TaskQueue.PopUnissuedTask(false))
		used, queued := sched.TaskQueue.GetSlotUsage()
		require.Zero(t, used)
		require.Zero(t, queued)
	})

	t.Run("concurrent receivers share one queue budget", func(t *testing.T) {
		sched := NewTaskScheduler(context.Background())
		t.Cleanup(sched.Close)
		sched.totalSlots = func() int64 { return 128 }
		var accepted atomic.Int32
		var wg sync.WaitGroup
		for id := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := sched.TaskQueue.Enqueue(&pipelineTestTask{
					id: fmt.Sprint(id), slots: 64, ctx: context.Background(),
				})
				if err == nil {
					accepted.Add(1)
				} else {
					assert.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
				}
			}()
		}
		wg.Wait()
		require.EqualValues(t, 2, accepted.Load())
		used, queued := sched.TaskQueue.GetSlotUsage()
		require.Zero(t, used)
		require.EqualValues(t, 128, queued)
	})
}

func TestIndexTaskQueueReleasesSlotsAfterTaskReset(t *testing.T) {
	scheduler := NewTaskScheduler(context.Background())
	t.Cleanup(scheduler.Close)
	scheduler.totalSlots = func() int64 { return 1 }
	task := &fakeTask{id: 1, ctx: context.Background()}
	assert.True(t, scheduler.acquireSlots(task))
	scheduler.TaskQueue.AddActiveTask(task)
	name := task.Name()
	// Slot ownership belongs to the queue key, even after task cleanup changes
	// the fields used to compute its name.
	task.id = 0
	assert.Same(t, task, scheduler.TaskQueue.PopActiveTask(name))
	assert.Zero(t, scheduler.TaskQueue.GetUsingSlot())
	assert.Empty(t, scheduler.pendingTasks)
}

func TestIndexTaskQueue_Resource(t *testing.T) {
	paramtable.Init()

	sched := NewTaskScheduler(context.Background())
	t.Cleanup(sched.Close)
	queue := NewIndexBuildTaskQueue(sched)
	task := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished)

	assert.NoError(t, queue.Enqueue(task))
	// Reset() balances the _taskwg.Add(1) that OnEnqueue did, so the other
	// tests in this package that wait on _taskwg are unaffected.
	defer task.Reset()

	// Slots are charged when a task is dispatched, so an enqueued task shows up
	// as queued demand. Its cpu/memory is booked from the moment the node
	// accepts it, which is what the coordinator subtracts from. It is booked
	// inside addUnissuedTask, before the scheduler can see the task, so a
	// release can never run ahead of it.
	_, queued := queue.GetSlotUsage()
	assert.Equal(t, int64(1), queued)
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())

	queue.AddActiveTask(task)
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())

	// Leaving the active list returns the slots but not the cpu/memory: that
	// booking belongs to the execution and is returned by ReleaseResource.
	queue.PopActiveTask(task.Name())
	assert.Equal(t, int64(0), queue.GetUsingSlot())
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())
	queue.ReleaseResource(task)
	assert.Equal(t, taskcommon.Resource{}, queue.GetUsingResource())

	// A task the queue rejects books nothing.
	queue.unissuedTasks.PushBack(task)
	queue.maxTaskNum = 1
	rejected := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished)
	assert.Error(t, queue.addUnissuedTask(rejected))
	assert.Equal(t, taskcommon.Resource{}, queue.GetUsingResource())
	queue.unissuedTasks.Init()

	// A counter driven below zero is reported as zero, never as negative: a
	// negative "used" would make QuerySlot report more free than the node has.
	queue.usingCPU.Sub(3)
	queue.usingMemory.Sub(1 << 30)
	assert.Equal(t, taskcommon.Resource{}, queue.GetUsingResource())
}

// A dropped task whose native build ignores cancellation keeps running while
// DataCoord dispatches the same clusterID/taskID again. Both executions are
// booked; the newer one replaces the older under the name in activeTasks.
// Each must return its own booking when it finishes, whichever order they
// finish in, or the node reports less free memory than it has until restart.
func TestIndexTaskQueue_SameNameOverlapReleasesEachBooking(t *testing.T) {
	paramtable.Init()

	for _, newerFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("newerFinishesFirst=%v", newerFirst), func(t *testing.T) {
			sched := NewTaskScheduler(context.Background())
			t.Cleanup(sched.Close)
			sched.totalSlots = func() int64 { return 16 }
			queue := sched.TaskQueue.(*IndexTaskQueue)

			older := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished).(*fakeTask)
			newer := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished).(*fakeTask)
			newer.id = older.id
			require.Equal(t, older.Name(), newer.Name())

			require.NoError(t, queue.Enqueue(older))
			require.Same(t, older, queue.PopUnissuedTask(true))
			olderExec := sched.beginTask(older)

			require.NoError(t, queue.Enqueue(newer))
			require.Same(t, newer, queue.PopUnissuedTask(true))
			newerExec := sched.beginTask(newer)
			assert.Equal(t, taskcommon.Resource{CPU: 2, Memory: 200}, queue.GetUsingResource())

			first, second := olderExec, newerExec
			if newerFirst {
				first, second = newerExec, olderExec
			}
			sched.finishTask(first, nil)
			assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())
			sched.finishTask(second, nil)
			assert.Equal(t, taskcommon.Resource{}, queue.GetUsingResource())
			assert.Zero(t, queue.GetUsingSlot())
		})
	}
}

// finishTask returns an execution's booking once even if it runs twice, so a
// second call cannot take back cpu/memory that another task booked.
func TestIndexTaskQueue_FinishTaskReleasesOnce(t *testing.T) {
	paramtable.Init()

	sched := NewTaskScheduler(context.Background())
	t.Cleanup(sched.Close)
	sched.totalSlots = func() int64 { return 16 }
	queue := sched.TaskQueue.(*IndexTaskQueue)

	done := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished)
	other := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished)
	require.NoError(t, queue.Enqueue(done))
	require.NoError(t, queue.Enqueue(other))
	defer other.Reset()
	require.Same(t, done, queue.PopUnissuedTask(true))
	exec := sched.beginTask(done)

	sched.finishTask(exec, nil)
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())
	_taskwg.Add(1) // the second call resets the fake task again
	sched.finishTask(exec, nil)
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())
}

func TestIndexTaskScheduler(t *testing.T) {
	paramtable.Init()

	scheduler := NewTaskScheduler(context.TODO())
	scheduler.totalSlots = func() int64 { return 1024 }
	scheduler.Start()

	tasks := make([]Task, 0)

	tasks = append(tasks,
		newTask(fakeTaskEnqueued, nil, indexpb.JobState_JobStateRetry),
		newTask(fakeTaskPrepared, nil, indexpb.JobState_JobStateRetry),
		newTask(fakeTaskBuiltIndex, nil, indexpb.JobState_JobStateRetry),
		newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished),
		newTask(fakeTaskSavedIndexes, map[fakeTaskState]error{fakeTaskSavedIndexes: errors.New("auth failed")}, indexpb.JobState_JobStateRetry))

	for _, task := range tasks {
		assert.Nil(t, scheduler.TaskQueue.Enqueue(task))
	}
	_taskwg.Wait()
	scheduler.Close()
	scheduler.Wait()

	for _, task := range tasks[:len(tasks)-1] {
		assert.Equal(t, task.GetState(), task.(*fakeTask).expectedState)
		assert.Equal(t, task.Ctx().(*stagectx).curstate, task.Ctx().(*stagectx).state2cancel)
	}

	assert.Equal(t, tasks[len(tasks)-1].GetState(), tasks[len(tasks)-1].(*fakeTask).expectedState)
	assert.Equal(t, tasks[len(tasks)-1].Ctx().(*stagectx).curstate, fakeTaskState(fakeTaskSavedIndexes))

	scheduler = NewTaskScheduler(context.TODO())
	scheduler.totalSlots = func() int64 { return 1024 }
	tasks = make([]Task, 0, 1024)
	for i := 0; i < 1024; i++ {
		tasks = append(tasks, newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished))
		assert.Nil(t, scheduler.TaskQueue.Enqueue(tasks[len(tasks)-1]))
	}
	failTask := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished)
	err := scheduler.TaskQueue.Enqueue(failTask)
	assert.Error(t, err)
	failTask.Reset()

	scheduler.Start()
	_taskwg.Wait()
	scheduler.Close()
	scheduler.Wait()
	for _, task := range tasks {
		assert.Equal(t, task.GetState(), indexpb.JobState_JobStateFinished)
	}
}

func newSchedulerIndexBuildTask(t *testing.T, manager *TaskManager, buildID int64) *indexBuildTask {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req := &workerpb.CreateJobRequest{
		ClusterID: "test-cluster",
		BuildID:   buildID,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "STL_SORT"},
		},
		Field: &schemapb.FieldSchema{
			FieldID:  100,
			DataType: schemapb.DataType_Int64,
		},
	}
	manager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &IndexTaskInfo{
		State: commonpb.IndexState_InProgress,
	})
	return NewIndexBuildTask(ctx, cancel, req, nil, manager, nil, taskcommon.Resource{})
}

func TestIndexTaskSchedulerRecordsIndexTaskCost(t *testing.T) {
	paramtable.Init()

	loadMock := mockey.Mock((*indexBuildTask).LoadData).Return(nil).Build()
	defer loadMock.UnPatch()
	reserveMock := mockey.Mock((*indexBuildTask).TryReserveBuild).Return(true, nil).Build()
	defer reserveMock.UnPatch()

	t.Run("success records execution cost", func(t *testing.T) {
		manager := NewTaskManager(context.Background())
		task := newSchedulerIndexBuildTask(t, manager, 1001)

		preMock := mockey.Mock((*indexBuildTask).PreExecute).Return(nil).Build()
		defer preMock.UnPatch()
		executeMock := mockey.Mock((*indexBuildTask).Execute).Return(nil).Build()
		defer executeMock.UnPatch()
		postMock := mockey.Mock((*indexBuildTask).PostExecute).Return(nil).Build()
		defer postMock.UnPatch()

		scheduler := newPipelineScheduler(t)
		assert.NoError(t, scheduler.TaskQueue.Enqueue(task))
		assert.Eventually(t, func() bool {
			return task.GetState() != indexpb.JobState(commonpb.IndexState_InProgress)
		}, 5*time.Second, time.Millisecond)
		scheduler.Close()

		info := manager.GetIndexTaskInfo("test-cluster", 1001)
		assert.NotNil(t, info)
		assert.Equal(t, commonpb.IndexState_Finished, info.State)
		assert.Greater(t, info.ExecStartMs, int64(0))
		assert.GreaterOrEqual(t, info.ExecEndMs, info.ExecStartMs)
		assert.GreaterOrEqual(t, info.CostTimeMs, int64(0))
		assert.Equal(t, int64(1), info.CostCPUNum)
	})

	t.Run("pre execute failure still records execution end", func(t *testing.T) {
		manager := NewTaskManager(context.Background())
		task := newSchedulerIndexBuildTask(t, manager, 1002)
		expectedErr := errors.New("pre execute failed")

		preMock := mockey.Mock((*indexBuildTask).PreExecute).Return(expectedErr).Build()
		defer preMock.UnPatch()

		scheduler := newPipelineScheduler(t)
		assert.NoError(t, scheduler.TaskQueue.Enqueue(task))
		assert.Eventually(t, func() bool {
			return task.GetState() != indexpb.JobState(commonpb.IndexState_InProgress)
		}, 5*time.Second, time.Millisecond)
		scheduler.Close()

		info := manager.GetIndexTaskInfo("test-cluster", 1002)
		assert.NotNil(t, info)
		assert.Equal(t, commonpb.IndexState_Retry, info.State)
		assert.Equal(t, expectedErr.Error(), info.FailReason)
		assert.Zero(t, info.ExecStartMs)
		assert.GreaterOrEqual(t, info.ExecEndMs, info.ExecStartMs)
		assert.Zero(t, info.CostTimeMs)
		assert.Zero(t, info.CostCPUNum)
	})

	t.Run("vector index records build pool cpu num", func(t *testing.T) {
		manager := NewTaskManager(context.Background())
		task := newSchedulerIndexBuildTask(t, manager, 1003)

		vecMock := mockey.Mock((*indexBuildTask).IsVectorIndex).Return(true).Build()
		defer vecMock.UnPatch()
		preMock := mockey.Mock((*indexBuildTask).PreExecute).Return(nil).Build()
		defer preMock.UnPatch()
		executeMock := mockey.Mock((*indexBuildTask).Execute).Return(nil).Build()
		defer executeMock.UnPatch()
		postMock := mockey.Mock((*indexBuildTask).PostExecute).Return(nil).Build()
		defer postMock.UnPatch()

		scheduler := newPipelineScheduler(t)
		assert.NoError(t, scheduler.TaskQueue.Enqueue(task))
		assert.Eventually(t, func() bool {
			return task.GetState() != indexpb.JobState(commonpb.IndexState_InProgress)
		}, 5*time.Second, time.Millisecond)
		scheduler.Close()

		info := manager.GetIndexTaskInfo("test-cluster", 1003)
		assert.NotNil(t, info)
		assert.Equal(t, commonpb.IndexState_Finished, info.State)
		assert.Greater(t, info.ExecStartMs, int64(0))
		assert.GreaterOrEqual(t, info.ExecEndMs, info.ExecStartMs)
		assert.GreaterOrEqual(t, info.CostTimeMs, int64(0))
		assert.Equal(t, int64(hardware.GetCPUNum()), info.CostCPUNum)
	})
}

func TestIndexTaskSchedulerCostExcludesPipelineWaits(t *testing.T) {
	paramtable.Init()
	manager := NewTaskManager(context.Background())
	task := newSchedulerIndexBuildTask(t, manager, 1004)
	var admitted atomic.Bool
	var loadGate, buildGate, uploadGate <-chan struct{}
	loading, building, uploading := make(chan struct{}), make(chan struct{}), make(chan struct{})
	reserveTried := make(chan struct{})
	var triedOnce sync.Once
	var uploadStart time.Time
	pre := mockey.Mock((*indexBuildTask).PreExecute).Return(nil).Build()
	t.Cleanup(func() { pre.UnPatch() })
	load := mockey.Mock((*indexBuildTask).LoadData).To(func(*indexBuildTask, context.Context) error {
		close(loading)
		<-loadGate
		return nil
	}).Build()
	t.Cleanup(func() { load.UnPatch() })
	reserve := mockey.Mock((*indexBuildTask).TryReserveBuild).To(func(*indexBuildTask) (bool, error) {
		triedOnce.Do(func() { close(reserveTried) })
		return admitted.Load(), nil
	}).Build()
	t.Cleanup(func() { reserve.UnPatch() })
	execute := mockey.Mock((*indexBuildTask).Execute).To(func(*indexBuildTask, context.Context) error {
		close(building)
		<-buildGate
		return nil
	}).Build()
	t.Cleanup(func() { execute.UnPatch() })
	post := mockey.Mock((*indexBuildTask).PostExecute).To(func(*indexBuildTask, context.Context) error {
		uploadStart = time.Now()
		close(uploading)
		<-uploadGate
		return nil
	}).Build()
	t.Cleanup(func() { post.UnPatch() })
	// Fix the measured build duration so assertions do not depend on wall-clock sleeps.
	elapsed := mockey.Mock(taskcost.ElapsedMs).Return(int64(17)).Build()
	t.Cleanup(func() { elapsed.UnPatch() })
	sched := newPipelineScheduler(t)
	var releaseLoad, releaseBuild, releaseUpload func()
	loadGate, releaseLoad = pipelineGate(t)
	buildGate, releaseBuild = pipelineGate(t)
	uploadGate, releaseUpload = pipelineGate(t)
	require.NoError(t, sched.TaskQueue.Enqueue(task))
	awaitPipeline(t, loading)
	info := manager.GetIndexTaskInfo("test-cluster", 1004)
	require.Zero(t, info.ExecStartMs)
	require.Zero(t, info.CostCPUNum)
	releaseLoad()
	awaitPipeline(t, reserveTried)
	info = manager.GetIndexTaskInfo("test-cluster", 1004)
	require.Zero(t, info.ExecStartMs)
	require.Zero(t, info.CostTimeMs)
	admittedAt := time.Now().UnixMilli()
	admitted.Store(true)
	sched.buildWakeup <- struct{}{}
	awaitPipeline(t, building)
	info = manager.GetIndexTaskInfo("test-cluster", 1004)
	require.GreaterOrEqual(t, info.ExecStartMs, admittedAt)
	require.EqualValues(t, 1, info.CostCPUNum)
	releaseBuild()
	awaitPipeline(t, uploading)
	require.Equal(t, 1, elapsed.Times())
	require.Zero(t, manager.GetIndexTaskInfo("test-cluster", 1004).ExecEndMs)
	releaseUpload()
	require.Eventually(t, func() bool { return task.GetState() == indexpb.JobState_JobStateFinished }, 5*time.Second, time.Millisecond)
	info = manager.GetIndexTaskInfo("test-cluster", 1004)
	require.EqualValues(t, 17, info.CostTimeMs)
	require.GreaterOrEqual(t, info.ExecEndMs, info.ExecStartMs)
	require.LessOrEqual(t, info.ExecEndMs, uploadStart.UnixMilli())
	require.Equal(t, 1, elapsed.Times())
}

// Every index-family task reports exactly the cpu/memory its constructor was
// handed -- the estimate DataCoord put in the CreateTask properties -- and
// nothing when the coordinator predates them.
func TestIndexTaskGetResource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewTaskManager(ctx)
	want := taskcommon.Resource{CPU: 2, Memory: 1 << 30}

	assert.Equal(t, want, NewIndexBuildTask(ctx, cancel,
		&workerpb.CreateJobRequest{}, nil, manager, nil, want).GetResource())
	assert.Equal(t, want, NewStatsTask(ctx, cancel,
		&workerpb.CreateStatsRequest{}, manager, nil, nil, want).GetResource())
	assert.Equal(t, want, NewAnalyzeTask(ctx, cancel,
		&workerpb.AnalyzeRequest{}, manager, nil, want).GetResource())

	// A coordinator that predates the properties books nothing.
	assert.True(t, NewIndexBuildTask(ctx, cancel,
		&workerpb.CreateJobRequest{}, nil, manager, nil, taskcommon.Resource{}).GetResource().IsZero())
	assert.True(t, NewStatsTask(ctx, cancel,
		&workerpb.CreateStatsRequest{}, manager, nil, nil, taskcommon.Resource{}).GetResource().IsZero())
	assert.True(t, NewAnalyzeTask(ctx, cancel,
		&workerpb.AnalyzeRequest{}, manager, nil, taskcommon.Resource{}).GetResource().IsZero())
}
