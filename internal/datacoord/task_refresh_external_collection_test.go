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
	jobs  []*datapb.ExternalCollectionRefreshJob
	tasks []*datapb.ExternalCollectionRefreshTask
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

func (s *stubCluster) CreateExternalCollectionTask(nodeID int64, req *datapb.UpdateExternalCollectionRequest) error {
	return nil
}

func (s *stubCluster) QueryExternalCollectionTask(nodeID int64, taskID int64) (*datapb.UpdateExternalCollectionResponse, error) {
	return &datapb.UpdateExternalCollectionResponse{
		State: indexpb.JobState_JobStateInProgress,
	}, nil
}

func (s *stubCluster) DropExternalCollectionTask(nodeID int64, taskID int64) error {
	return nil
}

// ==================== Helper Functions ====================

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
	assert.Equal(t, taskcommon.Stats, task.GetTaskType())
}

func TestRefreshExternalCollectionTask_GetTaskState(t *testing.T) {
	task, _ := createTestRefreshTaskWithStubs(t, 1001, 1, 100)
	assert.Equal(t, taskcommon.State(indexpb.JobState_JobStateInit), task.GetTaskState())
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

		resp := &datapb.UpdateExternalCollectionResponse{
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
		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			},
		})
		segments.SetSegment(2, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			},
		})

		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		task := createTestRefreshTaskWithMetaAndStubs(t, 1001, 1, 100, mt, refreshMeta)

		// Try to drop all segments without replacement
		resp := &datapb.UpdateExternalCollectionResponse{
			KeptSegments:    []int64{},               // Keep none
			UpdatedSegments: []*datapb.SegmentInfo{}, // Add none
		}

		err = task.SetJobInfo(ctx, resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "safety check failed")
	})

	t.Run("alloc_segment_id_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Create segments info
		segments := NewSegmentsInfo()
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

		// Create a stub allocator and mock AllocID to fail
		alloc := &stubAllocator{}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		// Mock AllocID to return error
		mockAllocID := mockey.Mock(mockey.GetMethod(alloc, "AllocID")).Return(int64(0), errors.New("alloc failed")).Build()
		defer mockAllocID.UnPatch()

		resp := &datapb.UpdateExternalCollectionResponse{
			KeptSegments: []int64{},
			UpdatedSegments: []*datapb.SegmentInfo{
				{ID: 1, CollectionID: 100, NumOfRows: 1000},
			},
		}

		err = task.SetJobInfo(ctx, resp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "alloc failed")
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

		segments := NewSegmentsInfo()
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

		segments := NewSegmentsInfo()
		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		// Mock CreateExternalCollectionTask to return error
		mockCreate := mockey.Mock(mockey.GetMethod(cluster, "CreateExternalCollectionTask")).Return(errors.New("create task failed")).Build()
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

		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			},
		})

		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
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
		assert.Contains(t, metaTask.GetFailReason(), "job cancelled")
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
		assert.Contains(t, metaTask.GetFailReason(), "job cancelled")
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

		// Mock QueryExternalCollectionTask to return error
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryExternalCollectionTask")).Return(nil, errors.New("query failed")).Build()
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

		// stubCluster.QueryExternalCollectionTask returns InProgress by default

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

		// Mock QueryExternalCollectionTask to return failed state
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryExternalCollectionTask")).Return(&datapb.UpdateExternalCollectionResponse{
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

	t.Run("task_unexpected_state", func(t *testing.T) {
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

		// Mock QueryExternalCollectionTask to return unexpected state
		mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryExternalCollectionTask")).Return(&datapb.UpdateExternalCollectionResponse{
			State: indexpb.JobState_JobStateNone,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task should be marked as failed due to unexpected state
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

	segments := NewSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	cluster := &stubCluster{}

	// Mock QueryExternalCollectionTask to return Finished with response
	mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryExternalCollectionTask")).Return(&datapb.UpdateExternalCollectionResponse{
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

	segments := NewSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	cluster := &stubCluster{}

	// Mock query to return Finished
	mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryExternalCollectionTask")).Return(&datapb.UpdateExternalCollectionResponse{
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
	mockQuery := mockey.Mock(mockey.GetMethod(cluster, "QueryExternalCollectionTask")).Return(&datapb.UpdateExternalCollectionResponse{
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

	// Job doesn't exist, nodeId is 0 so DropExternalCollectionTask should NOT be called
	task.QueryTaskOnWorker(cluster)

	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
	assert.Contains(t, metaTask.GetFailReason(), "job cancelled")
}

// ==================== SetJobInfo Additional Tests ====================

func TestRefreshExternalCollectionTask_SetJobInfo_SuccessWithSegments(t *testing.T) {
	ctx := context.Background()

	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	// Create existing segments
	segments := NewSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    1000,
		},
	})
	segments.SetSegment(2, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: 100,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    2000,
		},
	})

	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
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
	resp := &datapb.UpdateExternalCollectionResponse{
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
	segments := NewSegmentsInfo()
	for i := int64(1); i <= 10; i++ {
		segments.SetSegment(i, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           i,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    1000,
			},
		})
	}

	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
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
	resp := &datapb.UpdateExternalCollectionResponse{
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

	segments := NewSegmentsInfo()
	mt := &meta{
		segments:    segments,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
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

	resp := &datapb.UpdateExternalCollectionResponse{
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

	segments := NewSegmentsInfo()
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

		// Mock DropExternalCollectionTask to return error
		mockDrop := mockey.Mock(mockey.GetMethod(cluster, "DropExternalCollectionTask")).Return(errors.New("drop failed")).Build()
		defer mockDrop.UnPatch()

		task.DropTaskOnWorker(cluster)
		// Error is logged but not returned
	})
}
