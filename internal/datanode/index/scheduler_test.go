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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

	// Branch difference from master, pinned on purpose: segcoreCodeTable on this
	// branch registers DataTypeInvalid(2007), FieldIDInvalid(2020),
	// FieldAlreadyExist(2021), OpTypeInvalid(2022) and DataIsEmpty(2023) as
	// caller input, so they are terminal here. Master reclassified them as system
	// errors in #50768 after auditing their C++ producers; that reclassification
	// is not part of this fix and is not backported.
	t.Run("codes registered as caller input on this branch are terminal", func(t *testing.T) {
		for _, code := range []int32{2007, 2020, 2021, 2022, 2023} {
			assert.Equalf(t, indexpb.JobState_JobStateFailed,
				getStateFromError(merr.SegcoreError(code, "registered as input")), "segcore code %d", code)
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
