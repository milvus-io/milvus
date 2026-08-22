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
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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

func (s *stubScheduler) Finalize(_ int64, fn func()) {
	fn()
}

func (s *stubScheduler) GetEnqueueCount() int {
	return int(s.enqueueCount.Load())
}

func setInspectorTaskAllocator(inspector *externalCollectionRefreshInspector, lastID int64) {
	var nextID atomic.Int64
	nextID.Store(lastID)
	inspector.allocateTaskID = func(context.Context) (int64, error) {
		return nextID.Add(1), nil
	}
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
	closeChan := make(chan struct{})

	inspector := newRefreshInspector(ctx, refreshMeta, scheduler, closeChan)
	assert.NotNil(t, inspector)
}

func TestExternalCollectionRefreshInspector_Inspect(t *testing.T) {
	paramtable.Init()

	t.Run("enqueue_init_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		// Should have called Enqueue for Init task
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
	})

	t.Run("enqueue_retry_tasks", func(t *testing.T) {
		const maxRetryKey = "dataCoord.externalCollectionMaxRetryTimes"
		paramtable.Get().Save(maxRetryKey, "2")
		defer paramtable.Get().Reset(maxRetryKey)

		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{
				TaskId:               1001,
				JobId:                1,
				CollectionId:         100,
				Version:              3,
				NodeId:               10,
				State:                indexpb.JobState_JobStateRetry,
				FailReason:           "worker failure 1/10",
				ExternalSource:       "s3://bucket/path",
				ExternalSpec:         "iceberg",
				Progress:             50,
				ExploreManifestPath:  "manifests/1.pb",
				FileIndexBegin:       3,
				FileIndexEnd:         8,
				OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
				OwnedSegmentIds:      []int64{10, 20},
				KeptSegments:         []int64{10},
				ResultReady:          true,
				ResultStorageVersion: externalRefreshTaskResultStorageVersion,
				ResultPath:           "results/old.pb",
				ResultChecksum:       []byte{1, 2, 3},
			},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{
				ExternalCollectionRefreshTask: t,
				refreshMeta:                   refreshMeta,
			}
		}
		counter := &atomic.Int64{}
		counter.Store(1)
		refreshMeta.workerFailureCounts.Insert(1001, counter)
		setInspectorTaskAllocator(inspector, 2000)
		inspector.inspect()

		// Retry is replaced by a fresh Init task, not directly re-enqueued.
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
		if !assert.Len(t, scheduler.enqueuedTasks, 1) {
			return
		}
		replacement := scheduler.enqueuedTasks[0].(*refreshExternalCollectionTask)
		assert.Equal(t, int64(2001), replacement.GetTaskID())
		assert.Equal(t, taskcommon.Init, replacement.GetTaskState())
		assert.Equal(t, "manifests/1.pb", replacement.GetExploreManifestPath())
		assert.Equal(t, int64(3), replacement.GetFileIndexBegin())
		assert.Equal(t, int64(8), replacement.GetFileIndexEnd())
		assert.Equal(t, []int64{10, 20}, replacement.GetOwnedSegmentIds())
		assert.Zero(t, replacement.GetVersion())
		assert.Zero(t, replacement.GetNodeId())
		assert.False(t, replacement.GetResultReady())
		assert.Empty(t, replacement.GetResultPath())
		assert.Equal(t, indexpb.JobState_JobStateRetry, refreshMeta.GetTask(1001).GetState(), "retired record remains for job GC")
		assert.Equal(t, []int64{2001}, refreshMeta.GetJob(1).GetTaskIds())
		assert.NoError(t, replacement.retryWorkerFailure("worker failed again"))
		assert.Equal(t, indexpb.JobState_JobStateFailed, refreshMeta.GetTask(2001).GetState(), "replacement inherits the first failure and spends the configured limit")
	})

	t.Run("skip_in_progress_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001, 1002, 1003, 1004}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		setInspectorTaskAllocator(inspector, 2000)
		inspector.inspect()

		// Init is enqueued as-is; Retry is enqueued through its replacement.
		assert.Equal(t, 2, scheduler.GetEnqueueCount())
	})

	t.Run("skip_unpublished_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.inspect()

		assert.Equal(t, 1, scheduler.GetEnqueueCount())
		if assert.Len(t, scheduler.enqueuedTasks, 1) {
			assert.Equal(t, int64(1001), scheduler.enqueuedTasks[0].GetTaskID())
		}
	})
}

func TestExternalCollectionRefreshInspector_ReloadFromMeta(t *testing.T) {
	paramtable.Init()

	t.Run("reload_init_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		setInspectorTaskAllocator(inspector, 2000)
		inspector.reloadFromMeta()

		// Startup recovery replaces Retry before scheduling it.
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
		if assert.Len(t, scheduler.enqueuedTasks, 1) {
			assert.Equal(t, int64(2001), scheduler.enqueuedTasks[0].GetTaskID())
		}
	})

	t.Run("reload_in_progress_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{
				TaskId:               1001,
				JobId:                1,
				State:                indexpb.JobState_JobStateInProgress,
				FileIndexBegin:       3,
				FileIndexEnd:         8,
				OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
				OwnedSegmentIds:      []int64{10, 20},
			},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		inspector.reloadFromMeta()

		// Should have called Enqueue for InProgress task (for recovery)
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
		if assert.Len(t, scheduler.enqueuedTasks, 1) {
			reloaded := scheduler.enqueuedTasks[0].(*refreshExternalCollectionTask)
			assert.Equal(t, int64(3), reloaded.GetFileIndexBegin())
			assert.Equal(t, int64(8), reloaded.GetFileIndexEnd())
			assert.Equal(t, []int64{10, 20}, reloaded.GetOwnedSegmentIds())
		}
	})

	t.Run("skip_finished_tasks", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished},
		}

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
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

		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return([]*datapb.ExternalCollectionRefreshJob{{JobId: 1, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001, 1002, 1003, 1004, 1005}}}, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		refreshMeta, _ := newExternalCollectionRefreshMeta(context.Background(), catalog)
		scheduler := newStubScheduler()
		closeChan := make(chan struct{})

		inspector := newRefreshInspector(context.Background(), refreshMeta, scheduler, closeChan)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}
		setInspectorTaskAllocator(inspector, 2000)
		inspector.reloadFromMeta()

		// Init, replacement Init, and recovered InProgress are scheduled.
		assert.Equal(t, 3, scheduler.GetEnqueueCount())
	})

	t.Run("failed_job_recovers_only_in_progress_tasks_on_startup", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateRetry},
			{TaskId: 1003, JobId: 1, State: indexpb.JobState_JobStateInProgress},
			{TaskId: 1004, JobId: 1, State: indexpb.JobState_JobStateFinished},
			{TaskId: 1005, JobId: 1, State: indexpb.JobState_JobStateFailed},
		}
		catalog := &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId:   1,
				State:   indexpb.JobState_JobStateFailed,
				TaskIds: []int64{1001, 1002, 1003, 1004, 1005},
			}},
			tasks: tasks,
		}
		ctx := context.Background()
		refreshMeta, _ := newExternalCollectionRefreshMeta(ctx, catalog)
		scheduler := newStubScheduler()
		inspector := newRefreshInspector(
			ctx,
			refreshMeta,
			scheduler,
			make(chan struct{}),
		)
		inspector.wrapTask = func(t *datapb.ExternalCollectionRefreshTask) *refreshExternalCollectionTask {
			return &refreshExternalCollectionTask{ExternalCollectionRefreshTask: t}
		}

		inspector.inspect()
		assert.Equal(t, 0, scheduler.GetEnqueueCount())

		inspector.reloadFromMeta()
		assert.Equal(t, 1, scheduler.GetEnqueueCount())
		if assert.Len(t, scheduler.enqueuedTasks, 1) {
			assert.Equal(t, int64(1003), scheduler.enqueuedTasks[0].GetTaskID())
		}
	})
}
