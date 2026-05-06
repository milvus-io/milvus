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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsMergesTaskResults(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		KeptSegments:    []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
	}))
	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:          1002,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		KeptSegments:    []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 20, CollectionID: 100, NumOfRows: 7}},
	}))

	segments := NewSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 100,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    5,
	}})
	segments.SetSegment(2, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           2,
		CollectionID: 100,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    6,
	}})
	mt := &meta{
		catalog:     catalog,
		segments:    segments,
		collections: newTestCollections(100),
	}
	mgr := &externalCollectionRefreshManager{
		mt:          mt,
		refreshMeta: refreshMeta,
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.NoError(t, err)

	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(1).GetState())
	assert.Equal(t, commonpb.SegmentState_Dropped, mt.segments.GetSegment(2).GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(10).GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(20).GetState())
	assert.Equal(t, int64(7), mt.segments.GetSegment(10).GetNumOfRows())
	assert.Equal(t, int64(7), mt.segments.GetSegment(20).GetNumOfRows())
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsNonFinishedTask(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
	}))
	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:       1002,
		JobId:        1,
		CollectionId: 100,
		State:        indexpb.JobState_JobStateInProgress,
	}))

	mt := &meta{
		catalog:     catalog,
		segments:    NewSegmentsInfo(),
		collections: newTestCollections(100),
	}
	updateCalls := 0
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).To(func(_ *meta, _ context.Context, _ ...UpdateOperator) error {
		updateCalls++
		return nil
	}).Build()
	defer mockUpdate.UnPatch()

	mgr := &externalCollectionRefreshManager{
		mt:          mt,
		refreshMeta: refreshMeta,
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-finished task")
	assert.Equal(t, 0, updateCalls)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsDuplicateUpdatedSegment(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
	}))
	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:          1002,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 8}},
	}))

	mt := &meta{
		catalog:     catalog,
		segments:    NewSegmentsInfo(),
		collections: newTestCollections(100),
	}
	updateCalls := 0
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).To(func(_ *meta, _ context.Context, _ ...UpdateOperator) error {
		updateCalls++
		return nil
	}).Build()
	defer mockUpdate.UnPatch()

	mgr := &externalCollectionRefreshManager{
		mt:          mt,
		refreshMeta: refreshMeta,
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate updated segment")
	assert.Equal(t, 0, updateCalls)
}

// ==================== Test Functions ====================

func TestExternalCollectionRefreshManager_NewManager(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)

	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)
	assert.NotNil(t, manager)
}

func TestExternalCollectionRefreshManager_StartStop(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)

	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

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

		// Create a mock meta with external collection. ExternalSpec must be
		// valid JSON now that createTasksForJob → exploreExternalFiles parses
		// it via externalspec.ParseExternalSpec (added in Part 8 cross-bucket).
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{collections: collections}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		// Mock exploreExternalFiles so the test does not need real S3 + parquet.
		// Returns one file so createTasksForJob produces a single task chunk.
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/file.parquet", NumRows: 100}}, "s3://bucket/path/manifest", nil).Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not an external collection")
	})

	t.Run("async_task_creation_failure_leaves_job_in_init", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}

		// Mock AllocID to return error — triggers createTasksForJob failure
		// in the async Phase B goroutine.
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

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), nil, nil)

		// Phase A persists the Init job and returns success. Phase B runs
		// in the background; its failure (AllocID error here) is logged
		// but does NOT unwind the job — that's the whole point of the
		// two-phase split. The job lingers in Init until the checker tick
		// retries or tryTimeoutJob marks it Failed. Stop() waits for the
		// background goroutine to complete so the assertion is stable.
		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job, "job should remain in Init state for retry")
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState())
		assert.Empty(t, job.GetTaskIds(), "no tasks should be persisted after async failure")
	})

	t.Run("ffi_explore_error_marks_job_failed_non_retriable", func(t *testing.T) {
		// Regression for #49233: any FFI failure during explore (NoSuchBucket,
		// AccessDenied, DNS NXDOMAIN, malformed URI, ...) is wrapped by the
		// loon FFI layer as ErrLoonTransient. Without classification this
		// looped forever as RefreshPending. Treat all FFI explore failures
		// as terminal so the job transitions to RefreshFailed and the user
		// gets a clear signal.
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket-does-not-exist/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{collections: collections}

		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		ffiErr := errors.Wrap(packed.ErrLoonTransient, "FFI operation failed: AWS Error NO_SUCH_BUCKET during ListObjectsV2")
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return(nil, "", ffiErr).Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState(),
			"FFI explore failure must transition job to Failed, not loop in Init")
		assert.Contains(t, job.GetFailReason(), "explore external files failed")
		assert.Contains(t, job.GetFailReason(), "NO_SUCH_BUCKET",
			"underlying error must be surfaced to operators")
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

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		// When no tasks exist, should keep the persisted state (Init), not overwrite to None
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState())
	})

	t.Run("finished_tasks_do_not_expose_finished_before_job_persisted", func(t *testing.T) {
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			Progress:     80,
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, tasks)

		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
		assert.Equal(t, int64(99), job.GetProgress())
	})

	t.Run("persisted_failed_job_not_overwritten_by_finished_tasks", func(t *testing.T) {
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFailed,
			Progress:     80,
			FailReason:   "apply failed",
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, tasks)

		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
		assert.Equal(t, int64(80), job.GetProgress())
		assert.Equal(t, "apply failed", job.GetFailReason())
	})

	t.Run("job_not_found", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

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

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		// When no tasks exist, should keep the persisted state (Init), not overwrite to None
		assert.Equal(t, indexpb.JobState_JobStateInit, result[0].GetState())
	})

	t.Run("finished_tasks_do_not_expose_finished_before_job_persisted", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, StartTime: now},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, tasks)

		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, result[0].GetState())
		assert.Equal(t, int64(99), result[0].GetProgress())
	})

	t.Run("empty_list", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})
}

func TestHandleJobFinished_SchemaChanged(t *testing.T) {
	ctx := context.Background()

	// Setup: collection with source="s3://old", job with source="s3://new"
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://old-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{collections: collections}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	// Track schemaUpdater calls
	updaterCalled := false
	var updatedCollID int64
	var updatedSource, updatedSpec string
	schemaUpdater := func(_ context.Context, collectionID int64, source, spec string) error {
		updaterCalled = true
		updatedCollID = collectionID
		updatedSource = source
		updatedSpec = spec
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet","version":2}`,
	}
	concreteManager.handleJobFinished(ctx, job)

	assert.True(t, updaterCalled, "schemaUpdater should be called when source/spec changed")
	assert.Equal(t, int64(100), updatedCollID)
	assert.Equal(t, "s3://new-bucket/path", updatedSource)
	assert.Equal(t, `{"format":"parquet","version":2}`, updatedSpec)
}

func TestHandleJobFinished_SchemaUnchanged(t *testing.T) {
	ctx := context.Background()

	// Setup: collection with same source/spec as job
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://same-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{collections: collections}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalled := false
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalled = true
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://same-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
	}
	concreteManager.handleJobFinished(ctx, job)

	assert.False(t, updaterCalled, "schemaUpdater should NOT be called when source/spec unchanged")
}

func TestHandleJobFinished_NilSchemaUpdater(t *testing.T) {
	ctx := context.Background()

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	// Create manager with nil schemaUpdater
	mgr := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
	}

	// Should not panic with nil schemaUpdater
	assert.NotPanics(t, func() {
		concreteManager.handleJobFinished(ctx, job)
	})
}

func TestHandleJobFinished_CollectionNotFound(t *testing.T) {
	ctx := context.Background()

	// Empty collections - collection not found
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	mt := &meta{collections: collections}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalled := false
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalled = true
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   999, // Does not exist
		ExternalSource: "s3://new-bucket/path",
	}

	// Should not panic, should return silently
	assert.NotPanics(t, func() {
		concreteManager.handleJobFinished(ctx, job)
	})
	assert.False(t, updaterCalled, "schemaUpdater should NOT be called when collection not found")
}

func TestHandleJobFinished_SchemaUpdaterError(t *testing.T) {
	ctx := context.Background()

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://old-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{collections: collections}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	// schemaUpdater returns error
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		return errors.New("WAL broadcast failed")
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
	}

	// Should not panic even if schemaUpdater returns error
	assert.NotPanics(t, func() {
		concreteManager.handleJobFinished(ctx, job)
	})
}

func TestHandleJobFinished_SourceChangedOnly(t *testing.T) {
	ctx := context.Background()

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://old-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{collections: collections}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalled := false
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalled = true
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(mt), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	// Only source changed, spec unchanged
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`, // Same as collection
	}
	concreteManager.handleJobFinished(ctx, job)

	assert.True(t, updaterCalled, "schemaUpdater should be called when only source changed")
}

// ==================== cleanupExploreTempForJob Tests ====================

// recordingChunkManager captures the prefixes passed to RemoveWithPrefix /
// Remove, and lets each call optionally return a configured error. Used by
// the cleanup-path unit tests without pulling in mockey for interface mocks
// (the manager holds a plain storage.ChunkManager field which this satisfies
// directly via method embedding).
type recordingChunkManager struct {
	storage.ChunkManager
	mu               sync.Mutex
	prefixCalls      []string
	removeCalls      []string
	prefixErr        error
	removeErr        error
	prefixBlockUntil <-chan struct{}
}

func (r *recordingChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	if r.prefixBlockUntil != nil {
		select {
		case <-r.prefixBlockUntil:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	r.prefixCalls = append(r.prefixCalls, prefix)
	r.mu.Unlock()
	return r.prefixErr
}

func (r *recordingChunkManager) Remove(ctx context.Context, key string) error {
	r.mu.Lock()
	r.removeCalls = append(r.removeCalls, key)
	r.mu.Unlock()
	return r.removeErr
}

func (r *recordingChunkManager) snapshot() (prefixes, removes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.prefixCalls...), append([]string(nil), r.removeCalls...)
}

func newManagerWithChunkManager(t *testing.T, cm storage.ChunkManager) *externalCollectionRefreshManager {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()
	mgr := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, cm)
	return mgr.(*externalCollectionRefreshManager)
}

func TestCleanupExploreTempForJob_Success(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	mgr.cleanupExploreTempForJob(42)

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_42"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_42"}, removes)
}

func TestCleanupExploreTempForJob_NilChunkManager(t *testing.T) {
	mgr := newManagerWithChunkManager(t, nil)

	// Nil chunkManager path must be safe and a no-op.
	assert.NotPanics(t, func() {
		mgr.cleanupExploreTempForJob(99)
	})
}

func TestCleanupExploreTempForJob_RemoveWithPrefixError(t *testing.T) {
	cm := &recordingChunkManager{prefixErr: errors.New("prefix walk failed")}
	mgr := newManagerWithChunkManager(t, cm)

	// Errors must be logged and swallowed; Remove should still be called as
	// the second pass (local-FS cleanup of the dir marker).
	assert.NotPanics(t, func() {
		mgr.cleanupExploreTempForJob(7)
	})
	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_7"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_7"}, removes)
}

func TestCleanupExploreTempForJob_RemoveError(t *testing.T) {
	cm := &recordingChunkManager{removeErr: errors.New("delete failed")}
	mgr := newManagerWithChunkManager(t, cm)

	assert.NotPanics(t, func() {
		mgr.cleanupExploreTempForJob(8)
	})
	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_8"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_8"}, removes)
}

func TestCleanupExploreTempForJob_RespectsManagerCtxCancel(t *testing.T) {
	// Build a chunkManager that blocks inside RemoveWithPrefix until the
	// manager ctx is canceled. If the cleanup derives its ctx from m.ctx
	// (as intended by P2-7), cancellation must abort the call within the
	// deadline instead of hanging for the 30s fallback timeout.
	unblock := make(chan struct{})
	cm := &recordingChunkManager{prefixBlockUntil: unblock}

	ctx, cancel := context.WithCancel(context.Background())
	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()
	mgr := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, cm).(*externalCollectionRefreshManager)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.cleanupExploreTempForJob(123)
	}()

	// Cancel the manager ctx; RemoveWithPrefix should unblock via ctx.Done().
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(unblock)
		t.Fatal("cleanupExploreTempForJob did not honor manager ctx cancellation")
	}
	close(unblock)
}

// ==================== handleJobFinished cleanup hook ====================

func TestHandleJobFinished_TriggersExploreTempCleanup(t *testing.T) {
	ctx := context.Background()

	// Build a collection whose schema will change, so schemaUpdater is
	// invoked and we exercise the full defer path.
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(200, &collectionInfo{
		ID: 200,
		Schema: &schemapb.CollectionSchema{
			Name:           "coll",
			ExternalSource: "s3://old",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{collections: collections}

	cm := &recordingChunkManager{}
	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error { return nil }

	mgr := NewExternalCollectionRefreshManager(
		ctx, mt, scheduler, alloc, refreshMeta, nil,
		testCollectionGetter(mt), schemaUpdater, cm,
	).(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          555,
		CollectionId:   200,
		ExternalSource: "s3://new",
		ExternalSpec:   `{"format":"parquet","v":2}`,
	}
	mgr.handleJobFinished(ctx, job)

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_555"}, prefixes, "Finished path must clean up the job-specific prefix")
	assert.Equal(t, []string{"__explore_temp__/coord_555"}, removes)

	// notifiedJobs must hold an entry so forgetJob later skips redundant cleanup.
	mgr.notifiedMu.Lock()
	_, present := mgr.notifiedJobs[555]
	mgr.notifiedMu.Unlock()
	assert.True(t, present, "handleJobFinished should mark jobID in notifiedJobs")
}

// ==================== handleJobFailed Tests ====================

func TestHandleJobFailed_TriggersCleanupAndDedups(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	mgr.handleJobFailed(777)
	mgr.handleJobFailed(777) // second call must no-op via notifiedJobs dedup

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_777"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_777"}, removes)

	mgr.notifiedMu.Lock()
	_, present := mgr.notifiedJobs[777]
	mgr.notifiedMu.Unlock()
	assert.True(t, present, "handleJobFailed should mark jobID in notifiedJobs")
}

// ==================== forgetJob Tests ====================

func TestForgetJob_SkipsCleanupWhenAlreadyHandled(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	// Simulate the Finished path having already cleaned the job.
	mgr.notifiedMu.Lock()
	mgr.notifiedJobs[321] = struct{}{}
	mgr.notifiedMu.Unlock()

	mgr.forgetJob(321)

	prefixes, _ := cm.snapshot()
	assert.Empty(t, prefixes, "forgetJob must skip cleanup when notifiedJobs entry is present")

	mgr.notifiedMu.Lock()
	_, stillPresent := mgr.notifiedJobs[321]
	mgr.notifiedMu.Unlock()
	assert.False(t, stillPresent, "forgetJob must still delete the dedup entry")
}

func TestForgetJob_CleansUpWhenNeverHandled(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	// Job never entered a terminal-state handler — forgetJob is the fallback
	// path (e.g. crash between Failed transition and callback firing).
	mgr.forgetJob(654)

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_654"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_654"}, removes)
}

func TestForgetJob_NilChunkManagerSafe(t *testing.T) {
	mgr := newManagerWithChunkManager(t, nil)

	assert.NotPanics(t, func() {
		mgr.forgetJob(1)
	})
}

// ==================== dedup path exhaustiveness ====================

func TestCleanup_DoubleHandleJobFailedDoesNotDouble(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	mgr.handleJobFailed(111)
	mgr.forgetJob(111) // checker GC path — must skip because handleJobFailed marked it

	prefixes, _ := cm.snapshot()
	if len(prefixes) != 1 {
		t.Fatalf("expected exactly 1 prefix cleanup for Failed+GC flow, got %d: %v", len(prefixes), prefixes)
	}
	assert.Equal(t, fmt.Sprintf("__explore_temp__/coord_%d", 111), prefixes[0])
}
