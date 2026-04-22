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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ==================== Stub Implementations ====================

// stubCatalog is a simple stub implementation of DataCoordCatalog for testing
type stubCatalog struct {
	metastore.DataCoordCatalog
	jobs            []*datapb.ExternalCollectionRefreshJob
	tasks           []*datapb.ExternalCollectionRefreshTask
	alterSegmentErr error
}

func (s *stubCatalog) ListExternalCollectionRefreshJobs(ctx context.Context) ([]*datapb.ExternalCollectionRefreshJob, error) {
	return s.jobs, nil
}

func (s *stubCatalog) ListExternalCollectionRefreshTasks(ctx context.Context) ([]*datapb.ExternalCollectionRefreshTask, error) {
	return s.tasks, nil
}

func (s *stubCatalog) SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	return nil
}

func (s *stubCatalog) SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask) error {
	return nil
}

func (s *stubCatalog) DropExternalCollectionRefreshJob(ctx context.Context, jobID typeutil.UniqueID) error {
	return nil
}

func (s *stubCatalog) DropExternalCollectionRefreshTask(ctx context.Context, taskID typeutil.UniqueID) error {
	return nil
}

func (s *stubCatalog) AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	return s.alterSegmentErr
}

// stubAllocator is a simple stub implementation of Allocator for testing
type stubAllocator struct {
	allocator.Allocator
	nextID int64
}

func (s *stubAllocator) AllocID(ctx context.Context) (typeutil.UniqueID, error) {
	s.nextID++
	return s.nextID, nil
}

func (s *stubAllocator) AllocTimestamp(ctx context.Context) (typeutil.Timestamp, error) {
	return uint64(time.Now().UnixNano()), nil
}

func (s *stubAllocator) AllocN(n int64) (typeutil.UniqueID, typeutil.UniqueID, error) {
	start := s.nextID + 1
	s.nextID += n
	return start, s.nextID + 1, nil
}

// stubCluster is a simple stub implementation of Cluster for testing
type stubCluster struct {
	session.Cluster
}

func (s *stubCluster) CreateRefreshExternalCollectionTask(nodeID int64, req *datapb.RefreshExternalCollectionTaskRequest) error {
	return nil
}

func (s *stubCluster) QueryRefreshExternalCollectionTask(nodeID int64, taskID int64) (*datapb.RefreshExternalCollectionTaskResponse, error) {
	return &datapb.RefreshExternalCollectionTaskResponse{
		State: indexpb.JobState_JobStateInProgress,
	}, nil
}

func (s *stubCluster) DropRefreshExternalCollectionTask(nodeID int64, taskID int64) error {
	return nil
}

// ==================== Helper Functions ====================

// newTestCollections creates a collections map with a single external collection
// that has one VChannel and one partition, as expected by SetJobInfo.
func newTestCollections(collectionID int64) *typeutil.ConcurrentMap[UniqueID, *collectionInfo] {
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collectionID, &collectionInfo{
		ID:            collectionID,
		VChannelNames: []string{"by-dev-rootcoord-dml_0_v1"},
		Partitions:    []int64{1},
	})
	return collections
}

func createTestRefreshTaskWithStubs(t *testing.T, taskID, jobID, collectionID int64) (*refreshExternalCollectionTask, *externalCollectionRefreshMeta) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         taskID,
		JobId:          jobID,
		CollectionId:   collectionID,
		State:          indexpb.JobState_JobStateInit,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)
	return task, refreshMeta
}

func createTestRefreshTaskWithMetaAndStubs(t *testing.T, taskID, jobID, collectionID int64, mt *meta, refreshMeta *externalCollectionRefreshMeta) *refreshExternalCollectionTask {
	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         taskID,
		JobId:          jobID,
		CollectionId:   collectionID,
		State:          indexpb.JobState_JobStateInit,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}

	alloc := &stubAllocator{nextID: 99999}
	return newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)
}

// ==================== Basic Interface Tests ====================

func TestRefreshExternalCollectionTask_NewTask(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.NotNil(t, task)
	assert.Equal(t, int64(1001), task.GetTaskId())
	assert.Equal(t, int64(1), task.GetJobId())
	assert.Equal(t, int64(100), task.GetCollectionId())
}

func TestRefreshExternalCollectionTask_GetTaskID(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.Equal(t, int64(1001), task.GetTaskID())
}

func TestRefreshExternalCollectionTask_GetTaskType(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.Equal(t, taskcommon.RefreshExternalCollection, task.GetTaskType())
}

func TestRefreshExternalCollectionTask_GetTaskState(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.Equal(t, indexpb.JobState_JobStateInit, task.GetTaskState())
}

func TestRefreshExternalCollectionTask_GetTaskSlot(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.Equal(t, int64(1), task.GetTaskSlot())
}

func TestRefreshExternalCollectionTask_GetTaskVersion(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.Equal(t, int64(0), task.GetTaskVersion())
}

func TestRefreshExternalCollectionTask_SetTaskTime(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)

	now := time.Now()
	task.SetTaskTime(taskcommon.TimeQueue, now)

	gotTime := task.GetTaskTime(taskcommon.TimeQueue)
	assert.Equal(t, now.Unix(), gotTime.Unix())
}

func TestRefreshExternalCollectionTask_SetState(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)

	task.SetState(indexpb.JobState_JobStateInProgress, "")
	assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())

	task.SetState(indexpb.JobState_JobStateFailed, "some error")
	assert.Equal(t, indexpb.JobState_JobStateFailed, task.GetState())
	assert.Equal(t, "some error", task.GetFailReason())
}

// ==================== ValidateSource Tests ====================

func TestRefreshExternalCollectionTask_ValidateSource(t *testing.T) {
	t.Run("skip_when_mt_is_nil", func(t *testing.T) {
		task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		// task.mt is nil by default
		err := task.validateSource()
		assert.NoError(t, err)
	})

	t.Run("job_not_found", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 999, 100)
		// Create a meta but don't add the job
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		mt := &meta{collections: collections}
		task.mt = mt

		// Job with ID 999 doesn't exist
		err := task.validateSource()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "job 999 not found")

		// Now add a different job
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		// Still should fail because task's jobID is 999
		err = task.validateSource()
		assert.Error(t, err)
	})

	t.Run("source_matches", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		mt := &meta{collections: collections}
		task.mt = mt

		// Add job with matching source
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err := refreshMeta.AddJob(job)
		assert.NoError(t, err)

		err = task.validateSource()
		assert.NoError(t, err)
	})

	t.Run("source_mismatch", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		mt := &meta{collections: collections}
		task.mt = mt

		// Add job with different source
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			ExternalSource: "s3://different/path",
			ExternalSpec:   "delta",
		}
		err := refreshMeta.AddJob(job)
		assert.NoError(t, err)

		err = task.validateSource()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task source mismatch")
	})
}

// ==================== UpdateStateWithMeta Tests ====================

func TestRefreshExternalCollectionTask_UpdateStateWithMeta(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 1, 100)

		// Add task to meta first
		err := refreshMeta.AddTask(task.ExternalCollectionRefreshTask)
		assert.NoError(t, err)

		err = task.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())

		// Verify meta was updated
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
	})

	t.Run("task_not_found", func(t *testing.T) {
		task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		// Don't add task to meta

		err := task.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
	})

	// Eager synchronous contract: when the task transitions to a terminal
	// state, processFinishedJob must fire BEFORE UpdateStateWithMeta returns,
	// with the correct jobID. This guarantees callers polling progress see a
	// consistent state (schema update has already been applied).
	t.Run("terminal_finished_fires_process_finished_job_synchronously", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 42, 100)
		err := refreshMeta.AddTask(task.ExternalCollectionRefreshTask)
		assert.NoError(t, err)

		var callbackJobID int64
		callbackCount := 0
		task.processFinishedJob = func(jobID int64) {
			callbackJobID = jobID
			callbackCount++
		}

		err = task.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "")
		assert.NoError(t, err)

		// Callback fired exactly once, with correct jobID, before return.
		assert.Equal(t, 1, callbackCount, "processFinishedJob must fire exactly once")
		assert.Equal(t, int64(42), callbackJobID, "processFinishedJob must receive the task's jobID")
	})

	t.Run("terminal_failed_fires_process_finished_job", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 7, 100)
		err := refreshMeta.AddTask(task.ExternalCollectionRefreshTask)
		assert.NoError(t, err)

		called := false
		task.processFinishedJob = func(jobID int64) { called = true }

		err = task.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "worker crashed")
		assert.NoError(t, err)
		assert.True(t, called, "processFinishedJob must also fire on Failed transitions")
	})

	t.Run("non_terminal_does_not_fire_process_finished_job", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		err := refreshMeta.AddTask(task.ExternalCollectionRefreshTask)
		assert.NoError(t, err)

		called := false
		task.processFinishedJob = func(jobID int64) { called = true }

		err = task.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)
		assert.False(t, called, "processFinishedJob must NOT fire on non-terminal transitions")
	})

	t.Run("nil_process_finished_job_no_panic", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		err := refreshMeta.AddTask(task.ExternalCollectionRefreshTask)
		assert.NoError(t, err)

		// processFinishedJob unset (nil) — test-fixture case.
		task.processFinishedJob = nil
		assert.NotPanics(t, func() {
			_ = task.UpdateStateWithMeta(indexpb.JobState_JobStateFinished, "")
		})
	})
}

// ==================== UpdateProgressWithMeta Tests ====================

func TestRefreshExternalCollectionTask_UpdateProgressWithMeta(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		task, refreshMeta := createTestRefreshTaskWithStubs(t, 1001, 1, 100)

		// Add task to meta first
		err := refreshMeta.AddTask(task.ExternalCollectionRefreshTask)
		assert.NoError(t, err)

		err = task.UpdateProgressWithMeta(50)
		assert.NoError(t, err)
		assert.Equal(t, int64(50), task.GetProgress())

		// Verify meta was updated
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, int64(50), metaTask.GetProgress())
	})

	t.Run("task_not_found", func(t *testing.T) {
		task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		// Don't add task to meta

		err := task.UpdateProgressWithMeta(50)
		assert.Error(t, err)
	})
}

// ==================== SetJobInfo Tests ====================

func TestRefreshExternalCollectionTask_SetJobInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("meta_is_nil", func(t *testing.T) {
		task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		// task.mt is nil

		resp := &datapb.RefreshExternalCollectionTaskResponse{
			KeptSegments:    []int64{1, 2},
			UpdatedSegments: []*datapb.SegmentInfo{},
		}

		err := task.SetJobInfo(ctx, resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "meta is nil")
	})

	t.Run("safety_check_drop_all_segments", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Create segments info
		segments := NewCachedSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			},
		}, 0)
		segments.SetSegment(2, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			},
		}, 0)

		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		task := createTestRefreshTaskWithMetaAndStubs(t, 1001, 1, 100, mt, refreshMeta)

		// Try to drop all segments without replacement
		resp := &datapb.RefreshExternalCollectionTaskResponse{
			KeptSegments:    []int64{},               // Keep none
			UpdatedSegments: []*datapb.SegmentInfo{}, // Add none
		}

		err = task.SetJobInfo(ctx, resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "safety check failed")
	})

	t.Run("success_drop_and_add_segments", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Create segments info with existing segments
		segments := NewCachedSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    500,
			},
		}, 0)
		segments.SetSegment(2, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    600,
			},
		}, 0)
		segments.SetSegment(3, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           3,
				CollectionID: 100,
				State:        commonpb.SegmentState_Dropped, // already dropped
				NumOfRows:    100,
			},
		}, 0)

		mt := &meta{
			catalog:     catalog,
			segments:    segments,
			collections: newTestCollections(100),
		}

		task := createTestRefreshTaskWithMetaAndStubs(t, 1001, 1, 100, mt, refreshMeta)

		// Keep segment 1, drop segment 2, add segment 10
		resp := &datapb.RefreshExternalCollectionTaskResponse{
			KeptSegments: []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{
				{ID: 10, CollectionID: 100, NumOfRows: 1000},
			},
		}

		err = task.SetJobInfo(ctx, resp)
		assert.NoError(t, err)
	})

	t.Run("high_drop_ratio_warning", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Create 10 segments
		segments := NewCachedSegmentsInfo()
		for i := int64(1); i <= 10; i++ {
			segments.SetSegment(i, &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:           i,
					CollectionID: 100,
					State:        commonpb.SegmentState_Flushed,
					NumOfRows:    100,
				},
			}, 0)
		}

		mt := &meta{
			catalog:     catalog,
			segments:    segments,
			collections: newTestCollections(100),
		}

		task := createTestRefreshTaskWithMetaAndStubs(t, 1001, 1, 100, mt, refreshMeta)

		// Keep only 1 segment (drop 9 out of 10 = 90% drop ratio, triggers warning)
		resp := &datapb.RefreshExternalCollectionTaskResponse{
			KeptSegments: []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{
				{ID: 20, CollectionID: 100, NumOfRows: 2000},
			},
		}

		err = task.SetJobInfo(ctx, resp)
		assert.NoError(t, err)
	})

	t.Run("update_segments_failed", func(t *testing.T) {
		catalog := &stubCatalog{
			alterSegmentErr: errors.New("alter segments failed"),
		}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Create segments info
		segments := NewCachedSegmentsInfo()
		mt := &meta{
			catalog:     catalog,
			segments:    segments,
			collections: newTestCollections(100),
		}

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}

		alloc := &stubAllocator{}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		resp := &datapb.RefreshExternalCollectionTaskResponse{
			KeptSegments: []int64{},
			UpdatedSegments: []*datapb.SegmentInfo{
				{ID: 1, CollectionID: 100, NumOfRows: 1000},
			},
		}

		err = task.SetJobInfo(ctx, resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "alter segments failed")
	})
}

// ==================== CreateTaskOnWorker Tests ====================

func TestRefreshExternalCollectionTask_CreateTaskOnWorker(t *testing.T) {
	t.Run("meta_is_nil", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc) // mt is nil

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "meta is nil")
	})

	t.Run("update_version_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		segments := NewCachedSegmentsInfo()
		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		// Mock SaveExternalCollectionRefreshTask to return error
		mockSave := mockey.Mock(mockey.GetMethod(catalog, "SaveExternalCollectionRefreshTask")).Return(errors.New("save failed")).Build()
		defer mockSave.UnPatch()

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// Task state should remain Init since version update failed
	})

	t.Run("alloc_segment_ids_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		segments := NewCachedSegmentsInfo()
		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		// Mock AllocN to return error
		mockAllocN := mockey.Mock(mockey.GetMethod(alloc, "AllocN")).Return(int64(0), int64(0), errors.New("alloc batch failed")).Build()
		defer mockAllocN.UnPatch()

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "alloc batch failed")
	})

	t.Run("collection_not_found", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		segments := NewCachedSegmentsInfo()
		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// Task should be marked as failed since collection is not in meta
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "collection 100 not found")
	})

	t.Run("create_task_on_worker_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		segments := NewCachedSegmentsInfo()
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "test_coll"}})
		mt := &meta{
			segments:    segments,
			collections: collections,
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		// Mock CreateRefreshExternalCollectionTask to return error
		mockCreate := mockey.Mock(mockey.GetMethod(cluster, "CreateRefreshExternalCollectionTask")).Return(errors.New("create task failed")).Build()
		defer mockCreate.UnPatch()

		task.CreateTaskOnWorker(1, cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "create task failed")
	})

	t.Run("success", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		segments := NewCachedSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			},
		}, 0)

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{Name: "test_coll"}})
		mt := &meta{
			segments:    segments,
			collections: collections,
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		task.CreateTaskOnWorker(1, cluster)

		// Task should be marked as in progress
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
	})
}

// ==================== QueryTaskOnWorker Tests ====================

func TestRefreshExternalCollectionTask_QueryTaskOnWorker(t *testing.T) {
	t.Run("job_not_found", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "job canceled")
	})

	t.Run("job_already_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Add a failed job
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFailed,
			FailReason:   "timeout",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "job canceled")
	})

	t.Run("query_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Add active job
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		// Mock QueryRefreshExternalCollectionTask to return error
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(nil, errors.New("query failed")).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "query task failed")
	})

	t.Run("task_in_progress", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Add active job
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		// stubCluster.QueryRefreshExternalCollectionTask returns InProgress by default

		task.QueryTaskOnWorker(cluster)

		// Task should remain in progress
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
	})

	t.Run("task_failed_on_worker", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Add active job
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		// Mock QueryRefreshExternalCollectionTask to return failed state
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State:      indexpb.JobState_JobStateFailed,
			FailReason: "worker error",
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as failed
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker error")
	})

	t.Run("task_finished_success", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Add active job with matching source
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		// Create segments and meta
		segments := NewCachedSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    500,
			},
		}, 0)

		mt := &meta{
			catalog:     catalog,
			segments:    segments,
			collections: newTestCollections(100),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		// Mock QueryRefreshExternalCollectionTask to return Finished with response
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State:        indexpb.JobState_JobStateFinished,
			KeptSegments: []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{
				{ID: 10, CollectionID: 100, NumOfRows: 1000},
			},
		}, nil).Build()
		defer mockQuery.UnPatch()

		// Mock UpdateSegmentsInfo to succeed
		mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).Return(nil).Build()
		defer mockUpdate.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as finished
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFinished, metaTask.GetState())
	})

	t.Run("task_finished_validate_source_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Add job with DIFFERENT source (to trigger validateSource failure)
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://different/path",
			ExternalSpec:   "delta",
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		mt := &meta{
			segments:    NewCachedSegmentsInfo(),
			collections: collections,
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		// Mock QueryRefreshExternalCollectionTask to return Finished
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateFinished,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as failed due to source mismatch
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "task source mismatch")
	})

	// Part 8 cross-bucket relaxed JobStateNone/JobStateInit to mean "not yet
	// picked up by the worker scheduler" (benign no-op) instead of "task not
	// found" (failure). JobStateRetry is the only state still treated as
	// unexpected and forces Failed.
	t.Run("task_state_none_no_op", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		// Worker reports JobStateNone — task hasn't been picked up yet.
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateNone,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task state should remain InProgress (no-op), not Failed.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
		assert.Empty(t, metaTask.GetFailReason())
	})

	// JobStateInit shares the no-op branch with JobStateNone/JobStateInProgress:
	// worker has the task but hasn't started execution yet. Task must stay
	// InProgress from DataCoord's view, not be marked Failed.
	t.Run("task_state_init_no_op", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		// Worker reports JobStateInit — task accepted but not yet running.
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateInit,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task state must remain InProgress (no-op), not Failed.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
		assert.Empty(t, metaTask.GetFailReason())
	})

	t.Run("task_state_retry_marks_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		}
		err = refreshMeta.AddJob(job)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		}
		err = refreshMeta.AddTask(protoTask)
		assert.NoError(t, err)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

		cluster := &stubCluster{}

		// JobStateRetry is the only state still considered unexpected.
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateRetry,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "unexpected state")
	})
}

// ==================== QueryTaskOnWorker Additional Tests ====================

func TestRefreshExternalCollectionTask_QueryTaskOnWorker_FinishedSuccess(t *testing.T) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	// Add active job with matching source
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddJob(job)
	assert.NoError(t, err)

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		NodeId:         1,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddTask(protoTask)
	assert.NoError(t, err)

	segments := NewCachedSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: newTestCollections(100),
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	cluster := &stubCluster{}

	// Mock QueryRefreshExternalCollectionTask to return Finished with response
	mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State:        indexpb.JobState_JobStateFinished,
		KeptSegments: []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: 100, NumOfRows: 1000},
		},
	}, nil).Build()
	defer mockQuery.UnPatch()

	// Mock UpdateSegmentsInfo to succeed
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).Return(nil).Build()
	defer mockUpdate.UnPatch()

	task.QueryTaskOnWorker(cluster)

	// Task should be marked as Finished
	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFinished, metaTask.GetState())
}

func TestRefreshExternalCollectionTask_QueryTaskOnWorker_FinishedValidateSourceFailed(t *testing.T) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	// Add job with DIFFERENT source than task
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://different/path",
		ExternalSpec:   "delta",
	}
	err = refreshMeta.AddJob(job)
	assert.NoError(t, err)

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		NodeId:         1,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddTask(protoTask)
	assert.NoError(t, err)

	segments := NewCachedSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	cluster := &stubCluster{}

	// Mock query to return Finished
	mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State: indexpb.JobState_JobStateFinished,
	}, nil).Build()
	defer mockQuery.UnPatch()

	task.QueryTaskOnWorker(cluster)

	// Task should be marked as failed due to source mismatch
	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
	assert.Contains(t, metaTask.GetFailReason(), "task source mismatch")
}

func TestRefreshExternalCollectionTask_QueryTaskOnWorker_FinishedSetJobInfoFailed(t *testing.T) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	// Add job with matching source
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddJob(job)
	assert.NoError(t, err)

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		NodeId:         1,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddTask(protoTask)
	assert.NoError(t, err)

	// Task has nil mt, so SetJobInfo will fail
	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc) // mt is nil

	cluster := &stubCluster{}

	// Mock query to return Finished
	mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryRefreshExternalCollectionTask")).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State: indexpb.JobState_JobStateFinished,
	}, nil).Build()
	defer mockQuery.UnPatch()

	task.QueryTaskOnWorker(cluster)

	// Task should fail because SetJobInfo fails (meta is nil)
	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
	assert.Contains(t, metaTask.GetFailReason(), "meta is nil")
}

func TestRefreshExternalCollectionTask_QueryTaskOnWorker_JobNotFoundNodeIdZero(t *testing.T) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		NodeId:         0, // Not assigned to any node
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddTask(protoTask)
	assert.NoError(t, err)

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)

	cluster := &stubCluster{}

	// Job doesn't exist, nodeId is 0 so DropRefreshExternalCollectionTask should NOT be called
	task.QueryTaskOnWorker(cluster)

	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
	assert.Contains(t, metaTask.GetFailReason(), "job canceled")
}

// ==================== SetJobInfo Additional Tests ====================

func TestRefreshExternalCollectionTask_SetJobInfo_SuccessWithSegments(t *testing.T) {
	ctx := context.Background()

	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	// Create existing segments
	segments := NewCachedSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    1000,
		},
	}, 0)
	segments.SetSegment(2, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: 100,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    2000,
		},
	}, 0)

	mt := &meta{
		segments:    segments,
		collections: newTestCollections(100),
	}

	alloc := &stubAllocator{nextID: 99999}
	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	// Keep segment 1, drop segment 2, add a new segment
	resp := &datapb.RefreshExternalCollectionTaskResponse{
		KeptSegments: []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{
			{ID: 999, CollectionID: 100, NumOfRows: 3000},
		},
	}

	// Mock UpdateSegmentsInfo to succeed
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).Return(nil).Build()
	defer mockUpdate.UnPatch()

	err = task.SetJobInfo(ctx, resp)
	assert.NoError(t, err)
}

func TestRefreshExternalCollectionTask_SetJobInfo_HighDropRatioWarning(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	// Create 10 existing segments
	segments := NewCachedSegmentsInfo()
	for i := int64(1); i <= 10; i++ {
		segments.SetSegment(i, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           i,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    1000,
			},
		}, 0)
	}

	mt := &meta{
		segments:    segments,
		collections: newTestCollections(100),
	}

	alloc := &stubAllocator{nextID: 99999}
	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	// Keep only 1 out of 10 segments (90% drop ratio) → triggers warning
	resp := &datapb.RefreshExternalCollectionTaskResponse{
		KeptSegments: []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{
			{ID: 999, CollectionID: 100, NumOfRows: 5000},
		},
	}

	// Mock UpdateSegmentsInfo to succeed
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).Return(nil).Build()
	defer mockUpdate.UnPatch()

	err = task.SetJobInfo(ctx, resp)
	assert.NoError(t, err)
}

func TestRefreshExternalCollectionTask_SetJobInfo_UpdateSegmentsInfoFailed(t *testing.T) {
	ctx := context.Background()

	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	segments := NewCachedSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: newTestCollections(100),
	}

	alloc := &stubAllocator{nextID: 99999}
	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	resp := &datapb.RefreshExternalCollectionTaskResponse{
		KeptSegments: []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: 100, NumOfRows: 1000},
		},
	}

	// Mock UpdateSegmentsInfo to fail
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).Return(errors.New("update segments failed")).Build()
	defer mockUpdate.UnPatch()

	err = task.SetJobInfo(ctx, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update segments failed")
}

// ==================== CreateTaskOnWorker Additional Tests ====================

func TestRefreshExternalCollectionTask_CreateTaskOnWorker_TaskNotFoundAfterVersionUpdate(t *testing.T) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInit,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	err = refreshMeta.AddTask(protoTask)
	assert.NoError(t, err)

	segments := NewCachedSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	cluster := &stubCluster{}

	// Mock GetTask to return nil (task disappears after version update)
	mockGetTask := mockey.Mock((*externalCollectionRefreshMeta).GetTask).Return(nil).Build()
	defer mockGetTask.UnPatch()

	task.CreateTaskOnWorker(1, cluster)

	// Task should be marked as failed
	// Note: since GetTask is mocked to return nil, we check the in-memory state
	assert.Equal(t, indexpb.JobState_JobStateFailed, task.GetState())
	assert.Contains(t, task.GetFailReason(), "not found after version update")
}

// ==================== DropTaskOnWorker Tests ====================

func TestRefreshExternalCollectionTask_DropTaskOnWorker(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		task.NodeId = 1

		cluster := &stubCluster{}

		task.DropTaskOnWorker(cluster)
		// No error expected
	})

	t.Run("drop_failed", func(t *testing.T) {
		task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
		task.NodeId = 1

		cluster := &stubCluster{}

		// Mock DropRefreshExternalCollectionTask to return error
		mockDrop := mockey.Mock(mockey.GetMethod(cluster, "DropRefreshExternalCollectionTask")).Return(errors.New("drop failed")).Build()
		defer mockDrop.UnPatch()

		task.DropTaskOnWorker(cluster)
		// Error is logged but not returned
	})
}
