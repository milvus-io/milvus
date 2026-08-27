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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGetStateFromError(t *testing.T) {
	t.Run("data format broken is terminal", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateFailed, getStateFromError(merr.SegcoreError(2024, "malformed vector data")))
	})

	t.Run("generic segcore error still retries", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry, getStateFromError(merr.SegcoreError(2001, "unexpected")))
	})

	t.Run("transient segcore error still retries", func(t *testing.T) {
		assert.Equal(t, indexpb.JobState_JobStateRetry, getStateFromError(merr.SegcoreError(2045, "transient storage error")))
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
}

var _ context.Context = &stagectx{}

func (s *stagectx) Deadline() (time.Time, bool) {
	return time.Now(), false
}

func (s *stagectx) Done() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curstate == s.state2cancel {
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

func TestIndexTaskQueue_Resource(t *testing.T) {
	paramtable.Init()

	queue := NewIndexBuildTaskQueue(&TaskScheduler{ctx: context.Background()})
	task := newTask(fakeTaskSavedIndexes, nil, indexpb.JobState_JobStateFinished)

	assert.NoError(t, queue.Enqueue(task))
	// Reset() balances the _taskwg.Add(1) that OnEnqueue did, so the other
	// tests in this package that wait on _taskwg are unaffected.
	defer task.Reset()

	assert.Equal(t, int64(1), queue.GetUsingSlot())
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())

	queue.AddActiveTask(task)
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())

	queue.PopActiveTask(task.Name())
	assert.Equal(t, int64(0), queue.GetUsingSlot())
	assert.Equal(t, taskcommon.Resource{}, queue.GetUsingResource())
}

func TestIndexTaskScheduler(t *testing.T) {
	paramtable.Init()

	scheduler := NewTaskScheduler(context.TODO())
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
	scheduler.wg.Wait()

	for _, task := range tasks[:len(tasks)-1] {
		assert.Equal(t, task.GetState(), task.(*fakeTask).expectedState)
		assert.Equal(t, task.Ctx().(*stagectx).curstate, task.Ctx().(*stagectx).state2cancel)
	}

	assert.Equal(t, tasks[len(tasks)-1].GetState(), tasks[len(tasks)-1].(*fakeTask).expectedState)
	assert.Equal(t, tasks[len(tasks)-1].Ctx().(*stagectx).curstate, fakeTaskState(fakeTaskSavedIndexes))

	scheduler = NewTaskScheduler(context.TODO())
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
	scheduler.wg.Wait()
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
	return NewIndexBuildTask(ctx, cancel, req, nil, manager, nil)
}

func TestIndexTaskSchedulerRecordsIndexTaskCost(t *testing.T) {
	paramtable.Init()

	t.Run("success records execution cost", func(t *testing.T) {
		manager := NewTaskManager(context.Background())
		task := newSchedulerIndexBuildTask(t, manager, 1001)

		preMock := mockey.Mock((*indexBuildTask).PreExecute).Return(nil).Build()
		defer preMock.UnPatch()
		executeMock := mockey.Mock((*indexBuildTask).Execute).Return(nil).Build()
		defer executeMock.UnPatch()
		postMock := mockey.Mock((*indexBuildTask).PostExecute).Return(nil).Build()
		defer postMock.UnPatch()

		scheduler := NewTaskScheduler(context.Background())
		scheduler.processTask(task)

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

		scheduler := NewTaskScheduler(context.Background())
		scheduler.processTask(task)

		info := manager.GetIndexTaskInfo("test-cluster", 1002)
		assert.NotNil(t, info)
		assert.Equal(t, commonpb.IndexState_Retry, info.State)
		assert.Equal(t, expectedErr.Error(), info.FailReason)
		assert.Greater(t, info.ExecStartMs, int64(0))
		assert.GreaterOrEqual(t, info.ExecEndMs, info.ExecStartMs)
		assert.GreaterOrEqual(t, info.CostTimeMs, int64(0))
		assert.Equal(t, int64(1), info.CostCPUNum)
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

		scheduler := NewTaskScheduler(context.Background())
		scheduler.processTask(task)

		info := manager.GetIndexTaskInfo("test-cluster", 1003)
		assert.NotNil(t, info)
		assert.Equal(t, commonpb.IndexState_Finished, info.State)
		assert.Greater(t, info.ExecStartMs, int64(0))
		assert.GreaterOrEqual(t, info.ExecEndMs, info.ExecStartMs)
		assert.GreaterOrEqual(t, info.CostTimeMs, int64(0))
		assert.Equal(t, int64(hardware.GetCPUNum()), info.CostCPUNum)
	})
}

// Every index-family task reports exactly the cpu/memory its request carries,
// and nothing when the coordinator predates the fields.
func TestIndexTaskGetResource(t *testing.T) {
	want := taskcommon.Resource{CPU: 2, Memory: 1 << 30}

	assert.Equal(t, want, (&indexBuildTask{
		req: &workerpb.CreateJobRequest{Cpu: 2, Memory: 1 << 30},
	}).GetResource())
	assert.Equal(t, want, (&statsTask{
		req: &workerpb.CreateStatsRequest{Cpu: 2, Memory: 1 << 30},
	}).GetResource())
	assert.Equal(t, want, (&analyzeTask{
		req: &workerpb.AnalyzeRequest{Cpu: 2, Memory: 1 << 30},
	}).GetResource())

	assert.True(t, (&indexBuildTask{req: &workerpb.CreateJobRequest{}}).GetResource().IsZero())
	assert.True(t, (&statsTask{req: &workerpb.CreateStatsRequest{}}).GetResource().IsZero())
	assert.True(t, (&analyzeTask{req: &workerpb.AnalyzeRequest{}}).GetResource().IsZero())
}
