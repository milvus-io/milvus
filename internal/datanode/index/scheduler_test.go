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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/resource"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

func (t *fakeTask) GetTaskID() int64 {
	return int64(t.id)
}

func (t *fakeTask) GetTaskType() taskcommon.Type {
	return taskcommon.Index
}

func (t *fakeTask) GetResourceRequirement() taskresource.Requirement {
	// Small on purpose: this double exercises the scheduler's mechanics, and a
	// thousand of them are run at once below.
	return taskresource.Requirement{CPU: 0.1, Memory: 1 << 20}
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

func TestIndexTaskScheduler(t *testing.T) {
	paramtable.Init()
	// Every task below is admitted through the guard. Route that at a double:
	// the process-wide guard freezes admission from the host's live memory
	// reading, and a frozen Acquire parks on the task's context -- which for
	// these doubles only advances as stages run, so the wait would never end.
	useRecordingGuard(t)

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
	useRecordingGuard(t)

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

// useRecordingGuard routes the scheduler's admission calls at a double for the
// duration of the test. The process-wide guard is deliberately kept out of the
// unit tests: it samples the machine's real memory in the background, so a test
// that reserved from it would pass or hang depending on the host's mood.
func useRecordingGuard(t *testing.T) *resource.RecordingGuard {
	g := resource.NewRecordingGuard()
	mk := mockey.Mock(resource.GetGuard).Return(g).Build()
	t.Cleanup(func() { mk.UnPatch() })
	return g
}

// admissionProbeTask records each stage it reaches in the guard's own event
// log, so the order of admission relative to the work is observable.
type admissionProbeTask struct {
	ctx      context.Context
	guard    *resource.RecordingGuard
	taskID   int64
	taskType taskcommon.Type
	req      taskresource.Requirement
	failAt   string

	state  indexpb.JobState
	reason string
}

var _ Task = (*admissionProbeTask)(nil)

func (t *admissionProbeTask) stage(name string) error {
	t.guard.Note(name)
	if t.failAt == name {
		return errors.New(name + " failed")
	}
	return nil
}

func (t *admissionProbeTask) Ctx() context.Context              { return t.ctx }
func (t *admissionProbeTask) Name() string                      { return fmt.Sprintf("probe-%d", t.taskID) }
func (t *admissionProbeTask) OnEnqueue(context.Context) error   { return nil }
func (t *admissionProbeTask) PreExecute(context.Context) error  { return t.stage("preExecute") }
func (t *admissionProbeTask) Execute(context.Context) error     { return t.stage("execute") }
func (t *admissionProbeTask) PostExecute(context.Context) error { return t.stage("postExecute") }
func (t *admissionProbeTask) Reset()                            {}
func (t *admissionProbeTask) GetState() indexpb.JobState        { return t.state }
func (t *admissionProbeTask) GetSlot() int64                    { return 1 }
func (t *admissionProbeTask) IsVectorIndex() bool               { return false }
func (t *admissionProbeTask) GetTaskID() int64                  { return t.taskID }
func (t *admissionProbeTask) GetTaskType() taskcommon.Type      { return t.taskType }
func (t *admissionProbeTask) GetResourceRequirement() taskresource.Requirement {
	return t.req
}

func (t *admissionProbeTask) SetState(state indexpb.JobState, failReason string) {
	t.state = state
	t.reason = failReason
}

func newAdmissionProbeTask(ctx context.Context, g *resource.RecordingGuard) *admissionProbeTask {
	return &admissionProbeTask{
		ctx:      ctx,
		guard:    g,
		taskID:   7001,
		taskType: taskcommon.Stats,
		req:      taskresource.Requirement{CPU: 2, Memory: 3 << 30},
	}
}

func TestSchedulerAdmission(t *testing.T) {
	paramtable.Init()

	t.Run("reserves before the first stage and releases at the end", func(t *testing.T) {
		g := useRecordingGuard(t)
		task := newAdmissionProbeTask(context.Background(), g)

		NewTaskScheduler(context.Background()).processTask(task)

		assert.Equal(t, []string{"acquire", "preExecute", "execute", "postExecute", "release"}, g.Events())
		acquires := g.Acquires()
		require.Len(t, acquires, 1)
		assert.Equal(t, task.GetTaskID(), acquires[0].TaskID)
		assert.Equal(t, taskcommon.Stats, acquires[0].Type, "the task's own family must reach the ledger")
		assert.Equal(t, task.GetResourceRequirement(), acquires[0].Req)
		assert.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
	})

	t.Run("releases when a stage fails", func(t *testing.T) {
		g := useRecordingGuard(t)
		task := newAdmissionProbeTask(context.Background(), g)
		task.failAt = "execute"

		NewTaskScheduler(context.Background()).processTask(task)

		assert.Equal(t, []int64{task.GetTaskID()}, g.Releases(), "a failed task must not leak its reservation")
		assert.Equal(t, indexpb.JobState_JobStateRetry, task.GetState())
	})

	t.Run("gives up without releasing when the wait is cut short", func(t *testing.T) {
		g := useRecordingGuard(t)
		g.FailAcquire(context.Canceled)
		task := newAdmissionProbeTask(context.Background(), g)

		sched := NewTaskScheduler(context.Background())
		sched.processTask(task)

		assert.Empty(t, g.Releases(), "a task that never acquired must not release")
		assert.NotContains(t, g.Events(), "execute", "no work may run without a reservation")
		// The task goes back for another attempt rather than being failed: the
		// wait ending says nothing about the task itself.
		assert.Equal(t, indexpb.JobState_JobStateRetry, task.GetState())
		// The queue's own books must be back where they started.
		utNum, atNum := sched.TaskQueue.GetTaskNum()
		assert.Equal(t, 0, utNum)
		assert.Equal(t, 0, atNum)
	})

	t.Run("waits in Accept before running any stage", func(t *testing.T) {
		g := useRecordingGuard(t)
		g.Block()
		task := newAdmissionProbeTask(context.Background(), g)

		done := make(chan struct{})
		go func() {
			defer close(done)
			NewTaskScheduler(context.Background()).processTask(task)
		}()

		// No stage may run before the budget is granted...
		time.Sleep(100 * time.Millisecond)
		assert.NotContains(t, g.Events(), "preExecute")

		g.Unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.Fail(t, "task never ran after the guard admitted it")
		}
		assert.Equal(t, []string{"acquire", "preExecute", "execute", "postExecute", "release"}, g.Events())
	})
}

func TestTaskResourceRequirements(t *testing.T) {
	paramtable.Init()

	t.Run("index build", func(t *testing.T) {
		req := &workerpb.CreateJobRequest{
			ClusterID: "c",
			BuildID:   9001,
			Dim:       128,
			NumRows:   1000000,
			Field:     &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector},
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
		}
		task := NewIndexBuildTask(context.Background(), func() {}, req, nil, NewTaskManager(context.Background()), nil)

		assert.Equal(t, int64(9001), task.GetTaskID())
		assert.Equal(t, taskcommon.Index, task.GetTaskType())
		assert.Equal(t, taskresource.RequirementForIndex(req), task.GetResourceRequirement())
		assert.Greater(t, task.GetResourceRequirement().Memory, int64(0))
	})

	t.Run("stats", func(t *testing.T) {
		// The request has to carry real binlogs and a schema that selects a
		// field, or RequirementForStats returns the 64MiB floor and the
		// assertions below hold for any implementation that returns a
		// constant. 2GiB of matchable varchar is well clear of it.
		const fieldBytes = int64(2) << 30
		req := &workerpb.CreateStatsRequest{
			ClusterID:  "c",
			TaskID:     9002,
			SubJobType: indexpb.StatsSubJob_TextIndexJob,
			NumRows:    1000,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: 100, DataType: schemapb.DataType_VarChar,
						TypeParams: []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}},
					},
				},
			},
			InsertLogs: []*datapb.FieldBinlog{
				{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: fieldBytes}}},
			},
		}
		task := NewStatsTask(context.Background(), func() {}, req, NewTaskManager(context.Background()), nil, nil)

		got := task.GetResourceRequirement()
		assert.Equal(t, int64(9002), task.GetTaskID())
		assert.Equal(t, taskcommon.Stats, task.GetTaskType())
		assert.Equal(t, taskresource.RequirementForStats(req), got)

		factor := paramtable.Get().DataCoordCfg.ResourceTextIndexFactor.GetAsFloat()
		want := int64(float64(fieldBytes) * factor)
		require.Greater(t, want, int64(64)<<20, "setup: the expected value must not be the floor")
		assert.Equal(t, want, got.Memory)
	})

	t.Run("analyze", func(t *testing.T) {
		// EstimateAnalyze now reads the node, so both assertions below would
		// otherwise depend on the build agent's RAM: the big-analyze bound in
		// particular needs 0.8 x hostMemory to exceed the grant arm's 4GiB cap,
		// which fails on any agent with 5GiB or less.
		mkMem := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64 << 30)).Build()
		defer mkMem.UnPatch()

		req := &workerpb.AnalyzeRequest{
			ClusterID: "c",
			TaskID:    9003,
			Dim:       128,
			Field:     &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector},
			SegmentStats: map[int64]*indexpb.SegmentStats{
				1: {NumRows: 100000},
			},
		}
		task := NewAnalyzeTask(context.Background(), func() {}, req, NewTaskManager(context.Background()), nil)

		got := task.GetResourceRequirement()
		assert.Equal(t, int64(9003), task.GetTaskID())
		assert.Equal(t, taskcommon.Analyze, task.GetTaskType())
		assert.Equal(t, taskresource.RequirementForAnalyze(req), got)

		// 128 dims x 100k rows x 4 bytes is ~48.8MiB of training data, so the
		// answer is the 64MiB floor and "> 0" would hold for any constant. Pin
		// the value, and pin a second, much larger request to show the estimate
		// actually moves with the input.
		assert.Equal(t, int64(64)<<20, got.Memory, "this input really is floor-bound")

		big := &workerpb.AnalyzeRequest{
			ClusterID: "c", TaskID: 9004, Dim: 1024,
			Field:        &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector},
			SegmentStats: map[int64]*indexpb.SegmentStats{1: {NumRows: 2_000_000}},
		}
		bigTask := NewAnalyzeTask(context.Background(), func() {}, big, NewTaskManager(context.Background()), nil)
		// 1024 dims x 2,000,000 rows x 4B is 8.192e9 bytes of vectors, and the
		// training buffer is 0.8 x 64GiB, so the dataset is what binds and the
		// charge is the whole of it.
		assert.Equal(t, int64(1024)*2_000_000*4, bigTask.GetResourceRequirement().Memory,
			"8GB of vectors must not be charged the same as 48MiB")
	})
}
