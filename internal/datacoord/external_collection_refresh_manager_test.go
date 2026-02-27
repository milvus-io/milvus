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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ==================== Helper Functions for Manager Tests ====================

func createTestRefreshMeta(t *testing.T) *externalCollectionRefreshMeta {
	catalog := &stubCatalog{}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	return meta
}

func createTestRefreshMetaWithJobs(t *testing.T, jobs []*datapb.ExternalCollectionRefreshJob, tasks []*datapb.ExternalCollectionRefreshTask) *externalCollectionRefreshMeta {
	catalog := &stubCatalog{
		jobs:  jobs,
		tasks: tasks,
	}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	return meta
}

func testCollectionGetter(mt *meta) func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
	return func(_ context.Context, collectionID int64) (*collectionInfo, error) {
		coll := mt.GetCollection(collectionID)
		if coll == nil {
			return nil, errors.New("collection not found")
		}
		return coll, nil
	}
}

// ==================== Test Functions ====================

func TestExternalCollectionRefreshManager_NewManager(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)

	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)
	assert.NotNil(t, manager)
}

func TestExternalCollectionRefreshManager_StartStop(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)

	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

	// Mock inspector and checker run methods to avoid actual execution
	mockInspectorRun := mockey.Mock((*externalCollectionRefreshInspector).run).Return().Build()
	defer mockInspectorRun.UnPatch()

	mockCheckerRun := mockey.Mock((*externalCollectionRefreshChecker).run).Return().Build()
	defer mockCheckerRun.UnPatch()

	// Start should not panic
	manager.Start()

	// Stop should not panic and should be idempotent
	manager.Stop()
	manager.Stop() // Call again to verify idempotency
}

func TestExternalCollectionRefreshManager_SubmitRefreshJobWithID(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		// Create a mock meta with external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   "iceberg",
			},
		})
		mt := &meta{collections: collections}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, testCollectionGetter(mt))

		jobID, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), jobID)

		// Verify job was created
		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
		assert.Equal(t, int64(100), job.GetCollectionId())
	})

	t.Run("idempotent_job_exists", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			StartTime:    now,
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		// Should return without error if job already exists
		jobID, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), jobID)
	})

	t.Run("collection_not_found", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		// Empty meta, no collections
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		mt := &meta{collections: collections}

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, testCollectionGetter(mt))

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 999, "test_collection", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("not_external_collection", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		// Create a mock meta with non-external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "", // Not external
			},
		})
		mt := &meta{collections: collections}

		// Mock typeutil.IsExternalCollection to return false
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(false).Build()
		defer mockIsExternal.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, testCollectionGetter(mt))

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not an external collection")
	})

	t.Run("alloc_task_id_failed_rollback_job", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}

		// Mock AllocID to return error
		mockAllocID := mockey.Mock(mockey.GetMethod(alloc, "AllocID")).Return(int64(0), errors.New("alloc failed")).Build()
		defer mockAllocID.UnPatch()

		scheduler := newStubScheduler()

		// Create a mock meta with external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   "iceberg",
			},
		})
		mt := &meta{collections: collections}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, testCollectionGetter(mt))

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.Error(t, err)

		// Verify the job was rolled back and does not remain in meta
		job := refreshMeta.GetJob(1)
		assert.Nil(t, job, "job should be rolled back after task creation failure")
	})

	t.Run("reject_when_active_job_exists", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			StartTime:    now,
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		// Create a mock meta with external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   "iceberg",
			},
		})
		mt := &meta{collections: collections}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, testCollectionGetter(mt))

		// Submit a new job with different ID should fail
		_, err := manager.SubmitRefreshJobWithID(ctx, 2, 100, "test_collection", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already in progress")

		// Verify old job was NOT changed
		oldJob := refreshMeta.GetJob(1)
		assert.NotNil(t, oldJob)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, oldJob.GetState())
	})
}

func TestExternalCollectionRefreshManager_GetJobProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("job_exists", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
			StartTime:    now,
		}
		existingTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:   1001,
			JobId:    1,
			State:    indexpb.JobState_JobStateInProgress,
			Progress: 50,
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, []*datapb.ExternalCollectionRefreshTask{existingTask})

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
		// State should be aggregated from tasks
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
		assert.Equal(t, int64(50), job.GetProgress())
	})

	t.Run("job_exists_no_tasks_keeps_persisted_state", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
			StartTime:    now,
		}
		// No tasks for this job
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		// When no tasks exist, should keep the persisted state (Init), not overwrite to None
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState())
	})

	t.Run("job_not_found", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		_, err := manager.GetJobProgress(ctx, 999)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestExternalCollectionRefreshManager_ListJobs(t *testing.T) {
	ctx := context.Background()

	t.Run("has_jobs", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, StartTime: now - 2000},
			{JobId: 2, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: now - 1000},
			{JobId: 3, CollectionId: 100, State: indexpb.JobState_JobStateFinished, StartTime: now},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 2001, JobId: 2, State: indexpb.JobState_JobStateInProgress, Progress: 50},
			{TaskId: 3001, JobId: 3, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, tasks)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		// Should be sorted by StartTime descending
		assert.Equal(t, int64(3), result[0].GetJobId())
		assert.Equal(t, int64(2), result[1].GetJobId())
		assert.Equal(t, int64(1), result[2].GetJobId())
	})

	t.Run("jobs_without_tasks_keep_persisted_state", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, StartTime: now},
		}
		// No tasks
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		// When no tasks exist, should keep the persisted state (Init), not overwrite to None
		assert.Equal(t, indexpb.JobState_JobStateInit, result[0].GetState())
	})

	t.Run("empty_list", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})
}
