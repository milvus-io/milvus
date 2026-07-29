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

package datacoord

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ==================== Stub Implementations for Inspector Tests ====================

// stubScheduler is a simple stub implementation of GlobalScheduler for testing
type stubScheduler struct {
	task.GlobalScheduler
	enqueuedTasks []task.Task
	enqueueCount  atomic.Int32
}

func newStubScheduler() *stubScheduler {
	return &stubScheduler{
		enqueuedTasks: make([]task.Task, 0),
	}
}

func (s *stubScheduler) Enqueue(t task.Task) {
	s.enqueuedTasks = append(s.enqueuedTasks, t)
	s.enqueueCount.Add(1)
}

func (s *stubScheduler) GetEnqueueCount() int {
	return int(s.enqueueCount.Load())
}

// ==================== Test Functions ====================

func TestExternalCollectionRefreshInspector_NewInspector(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}

	// Mock catalog methods
	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
	defer mockListTasks.UnPatch()

	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	scheduler := newStubScheduler()
	alloc := &stubAllocator{}
	closeChan := make(chan struct{})

	inspector := newRefreshInspector(ctx, refreshMeta, nil, scheduler, alloc, closeChan)
	assert.NotNil(t, inspector)
}

func TestExternalCollectionRefreshInspector_Inspect(t *testing.T) {
	paramtable.Init()

	t.Run("enqueue_init_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should have called Enqueue for Init task
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("enqueue_retry_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateRetry},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should have called Enqueue for Retry task
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("skip_in_progress_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should NOT call Enqueue for InProgress task in inspect()
		assert.Equal(t, 0, scheduler.GetEnqueueCount())
	})

	t.Run("skip_finished_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should NOT call Enqueue for Finished task
		assert.Equal(t, 0, scheduler.GetEnqueueCount())
	})

	t.Run("skip_failed_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFailed},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should NOT call Enqueue for Failed task
		assert.Equal(t, 0, scheduler.GetEnqueueCount())
	})

	t.Run("multiple_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateRetry},
			{TaskId: 1003, JobId: 1, State: indexpb.JobState_JobStateInProgress},
			{TaskId: 1004, JobId: 1, State: indexpb.JobState_JobStateFinished},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should be called twice: once for Init and once for Retry
		assert.Equal(t, 2, scheduler.GetEnqueueCount())
	})

	t.Run("terminal_job_tasks_are_not_enqueued", func(t *testing.T) {
		// Startup recovery and async task creation may expose an Init task after
		// its owning job is already terminal. The inspector must fence that task
		// before it can consume a worker slot.
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 5, CollectionId: 100, State: indexpb.JobState_JobStateFailed},
			{JobId: 6, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 5, State: indexpb.JobState_JobStateInit},
			{TaskId: 1002, JobId: 6, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(jobs, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("in_progress_only_on_startup_recovery", func(t *testing.T) {
		// The single knob that separates the startup sweep from a steady-state
		// tick: after a restart an in-flight attempt must be re-adopted, but on a
		// normal tick the scheduler already holds it.
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 7, State: indexpb.JobState_JobStateInProgress},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}

		inspector.inspect()
		assert.Equal(t, 0, scheduler.GetEnqueueCount())

		inspector.reloadFromMeta()
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})
}

func TestExternalCollectionRefreshInspector_ReloadFromMeta(t *testing.T) {
	paramtable.Init()

	t.Run("reload_init_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should have called Enqueue for Init task
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("reload_retry_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateRetry},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should have called Enqueue for Retry task
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("reload_in_progress_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should have called Enqueue for InProgress task (for recovery)
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("skip_finished_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should NOT call Enqueue for Finished task
		assert.Equal(t, 0, scheduler.GetEnqueueCount())
	})

	t.Run("skip_failed_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFailed},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should NOT call Enqueue for Failed task
		assert.Equal(t, 0, scheduler.GetEnqueueCount())
	})

	t.Run("multiple_tasks_on_reload", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateRetry},
			{TaskId: 1003, JobId: 1, State: indexpb.JobState_JobStateInProgress},
			{TaskId: 1004, JobId: 1, State: indexpb.JobState_JobStateFinished},
			{TaskId: 1005, JobId: 1, State: indexpb.JobState_JobStateFailed},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(committedRefreshJobsForTasks(nil, tasks), nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		alloc := &stubAllocator{}
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, nil, scheduler, alloc, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should be called 3 times: Init, Retry, InProgress
		assert.Equal(t, 3, scheduler.GetEnqueueCount())
	})
}
