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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ==================== Stub Implementations ====================

// stubCatalog is a simple stub implementation of DataCoordCatalog for testing
type stubCatalog struct {
	metastore.DataCoordCatalog
	jobs            []*datapb.ExternalCollectionRefreshJob
	tasks           []*datapb.ExternalCollectionRefreshTask
	alterSegmentErr error
	alteredSegments []*datapb.SegmentInfo

	updateErr     error
	updateActions [][]metastore.UpdateAction
}

// Update records the actions passed to it (so tests can assert on the
// composite write a caller issued) and returns updateErr.
func (s *stubCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	s.updateActions = append(s.updateActions, actions)
	return s.updateErr
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

func (s *stubCatalog) AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	s.alteredSegments = append([]*datapb.SegmentInfo(nil), newSegments...)
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
	refreshReq    *datapb.RefreshExternalCollectionTaskRequest
	createErr     error
	droppedNodeID int64
	droppedTaskID int64
}

func (s *stubCluster) CreateRefreshExternalCollectionTask(nodeID int64, req *datapb.RefreshExternalCollectionTaskRequest) error {
	s.refreshReq = req
	return s.createErr
}

func (s *stubCluster) QueryRefreshExternalCollectionTask(nodeID int64, taskID int64) (*datapb.RefreshExternalCollectionTaskResponse, error) {
	return &datapb.RefreshExternalCollectionTaskResponse{
		State: indexpb.JobState_JobStateInProgress,
	}, nil
}

func (s *stubCluster) DropRefreshExternalCollectionTask(nodeID int64, taskID int64) error {
	s.droppedNodeID = nodeID
	s.droppedTaskID = taskID
	return nil
}

// ==================== Helper Functions ====================

// newTestCollections creates a collections map with a single external collection
// that has one VChannel and one partition, as required by segment apply.
func newTestCollections(collectionID int64) *typeutil.ConcurrentMap[UniqueID, *collectionInfo] {
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collectionID, &collectionInfo{
		ID:            collectionID,
		VChannelNames: []string{"by-dev-rootcoord-dml_0_v1"},
		Partitions:    []int64{1},
	})
	return collections
}

func newTestExternalRefreshSegment(segmentID, collectionID, numRows int64) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		NumOfRows:      numRows,
		StorageVersion: 3,
		ManifestPath:   `{"base_path":"new","ver":1}`,
		SchemaVersion:  1,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 0,
			Binlogs: []*datapb.Binlog{{
				LogID:      segmentID,
				EntriesNum: numRows,
				MemorySize: numRows,
				LogSize:    numRows,
			}},
		}},
	}
}

func addOwnershipTestRefreshTask(
	t *testing.T,
	refreshMeta *externalCollectionRefreshMeta,
	task *datapb.ExternalCollectionRefreshTask,
) {
	if refreshMeta.GetJob(task.GetJobId()) == nil {
		assert.NoError(t, refreshMeta.AddJob(&datapb.ExternalCollectionRefreshJob{
			JobId:          task.GetJobId(),
			CollectionId:   task.GetCollectionId(),
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: task.GetExternalSource(),
			ExternalSpec:   task.GetExternalSpec(),
		}))
	}
	task.OwnershipPlanVersion = externalRefreshOwnershipPlanVersion
	assert.NoError(t, refreshMeta.AddTask(task))
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

	task.SetState(indexpb.JobState_JobStateRetry, "retry")
	assert.Equal(t, taskcommon.Failed, task.GetTaskState(), "Retry must retire the old scheduler attempt before replacement")
	assert.Equal(t, indexpb.JobState_JobStateRetry, task.GetState(), "the durable task state remains Retry")
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

// ==================== CreateTaskOnWorker Tests ====================

func TestRefreshExternalCollectionTask_CreateTaskOnWorker(t *testing.T) {
	newOwnershipTask := func(
		t *testing.T,
		planVersion int32,
		ownedSegmentIDs []int64,
	) (*refreshExternalCollectionTask, *externalCollectionRefreshMeta, *stubCluster) {
		t.Helper()
		refreshMeta := createTestRefreshMeta(t)
		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateInit,
			ExternalSource:       "s3://bucket/path",
			ExternalSpec:         "iceberg",
			OwnershipPlanVersion: planVersion,
			OwnedSegmentIds:      ownedSegmentIDs,
		}
		assert.NoError(t, refreshMeta.AddJob(&datapb.ExternalCollectionRefreshJob{
			JobId:          protoTask.GetJobId(),
			CollectionId:   protoTask.GetCollectionId(),
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: protoTask.GetExternalSource(),
			ExternalSpec:   protoTask.GetExternalSpec(),
		}))
		assert.NoError(t, refreshMeta.AddTask(protoTask))

		segments := NewSegmentsInfo()
		for _, segmentID := range []int64{1, 2} {
			segments.SetSegment(segmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:           segmentID,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
			}})
		}
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID:         100,
			Schema:     &schemapb.CollectionSchema{Name: "test_coll"},
			Partitions: []int64{10},
		})
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, &meta{
			segments:    segments,
			collections: collections,
		}, &stubAllocator{nextID: 99999})
		return task, refreshMeta, &stubCluster{}
	}

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
		addOwnershipTestRefreshTask(t, refreshMeta, protoTask)

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
		addOwnershipTestRefreshTask(t, refreshMeta, protoTask)

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		// Mock SaveExternalCollectionRefreshTask to return error
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save failed")).Build()
		defer mockSave.UnPatch()

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// The failure happened before dispatch, so the same Init task is retried
		// with scheduler backoff and no worker-failure budget is consumed.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
		assert.Nil(t, cluster.refreshReq)
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, !ok || counter.Load() == 0)
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
		addOwnershipTestRefreshTask(t, refreshMeta, protoTask)

		segments := NewSegmentsInfo()
		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		// Mock AllocN to return error
		mockAllocN := mockey.Mock((*stubAllocator).AllocN).Return(int64(0), int64(0), errors.New("alloc batch failed")).Build()
		defer mockAllocN.UnPatch()

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// Allocation is pre-dispatch infrastructure work. Keep the same task Init
		// so it can recover without consuming a worker attempt.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
		assert.Empty(t, metaTask.GetFailReason())
		assert.Nil(t, cluster.refreshReq)
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, !ok || counter.Load() == 0)
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
		addOwnershipTestRefreshTask(t, refreshMeta, protoTask)

		segments := NewSegmentsInfo()
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
		addOwnershipTestRefreshTask(t, refreshMeta, protoTask)

		segments := NewSegmentsInfo()
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID:         100,
			Schema:     &schemapb.CollectionSchema{Name: "test_coll"},
			Partitions: []int64{10},
		})
		mt := &meta{
			segments:    segments,
			collections: collections,
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		mockCreate := mockey.Mock((*stubCluster).CreateRefreshExternalCollectionTask).
			Return(merr.WrapErrServiceInternalMsg("create task failed")).Build()
		defer mockCreate.UnPatch()

		task.CreateTaskOnWorker(1, cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "create task failed")
	})

	t.Run("accepted_create_then_in_progress_persist_failure_retries_once", func(t *testing.T) {
		task, refreshMeta, cluster := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		persistErr := errors.New("catalog unavailable after create")
		saveCalls := 0
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(context.Context, *datapb.ExternalCollectionRefreshTask) error {
				saveCalls++
				// 1: persist version/node before dispatch; 2: persist InProgress;
				// 3: persist the worker-failure Retry transition.
				if saveCalls == 2 {
					return persistErr
				}
				return nil
			}).Build()
		defer mockSave.UnPatch()

		task.CreateTaskOnWorker(1, cluster)

		assert.NotNil(t, cluster.refreshReq, "the worker accepted this attempt")
		assert.Equal(t, 3, saveCalls, "the failed InProgress write must be followed by one Retry write")
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Equal(t, indexpb.JobState_JobStateRetry, task.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker failure 1/10")
		assert.Contains(t, metaTask.GetFailReason(), persistErr.Error())
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, ok)
		assert.Equal(t, int64(1), counter.Load())
	})

	t.Run("accepted_create_then_in_progress_persist_failure_honors_limit", func(t *testing.T) {
		const maxRetryKey = "dataCoord.externalCollectionMaxRetryTimes"
		paramtable.Get().Save(maxRetryKey, "1")
		defer paramtable.Get().Reset(maxRetryKey)

		task, refreshMeta, cluster := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		persistErr := errors.New("catalog unavailable after create")
		saveCalls := 0
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(context.Context, *datapb.ExternalCollectionRefreshTask) error {
				saveCalls++
				if saveCalls == 2 {
					return persistErr
				}
				return nil
			}).Build()
		defer mockSave.UnPatch()
		finishedCalls := 0
		task.processFinishedJob = func(jobID int64) {
			assert.Equal(t, int64(1), jobID)
			finishedCalls++
		}

		task.CreateTaskOnWorker(1, cluster)

		assert.NotNil(t, cluster.refreshReq)
		assert.Equal(t, 3, saveCalls)
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker failure 1/1")
		assert.Equal(t, 1, finishedCalls)
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, ok)
		assert.Equal(t, int64(1), counter.Load())
	})

	t.Run("accepted_create_retry_persist_failure_does_not_consume_budget", func(t *testing.T) {
		task, refreshMeta, cluster := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		persistErr := errors.New("catalog unavailable after create")
		saveCalls := 0
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(context.Context, *datapb.ExternalCollectionRefreshTask) error {
				saveCalls++
				if saveCalls == 2 || saveCalls == 3 {
					return persistErr
				}
				return nil
			}).Build()
		defer mockSave.UnPatch()

		task.CreateTaskOnWorker(1, cluster)

		assert.NotNil(t, cluster.refreshReq)
		assert.Equal(t, 3, saveCalls)
		assert.Equal(t, indexpb.JobState_JobStateInit, refreshMeta.GetTask(1001).GetState())
		assert.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, ok)
		assert.Zero(t, counter.Load())
	})

	t.Run("success", func(t *testing.T) {
		const targetRowsPerSegmentKey = "dataNode.externalCollection.targetRowsPerSegment"
		paramtable.Get().Save(targetRowsPerSegmentKey, "12345")
		defer paramtable.Get().Reset(targetRowsPerSegmentKey)

		task, refreshMeta, cluster := newOwnershipTask(
			t,
			externalRefreshOwnershipPlanVersion,
			[]int64{1},
		)
		task.CreateTaskOnWorker(1, cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
		assert.NotNil(t, cluster.refreshReq)
		assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), cluster.refreshReq.GetClusterID())
		assert.Equal(t, int64(10), cluster.refreshReq.GetPartitionID())
		assert.Equal(t, int64(12345), cluster.refreshReq.GetTargetRowsPerSegment())
	})

	t.Run("legacy_task", func(t *testing.T) {
		task, refreshMeta, cluster := newOwnershipTask(t, 0, nil)
		task.CreateTaskOnWorker(1, cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "unsupported ownership plan version 0")
		assert.Nil(t, cluster.refreshReq)
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, !ok || counter.Load() == 0,
			"an immutable-plan validation failure is terminal, not a worker attempt")
	})

	t.Run("worker_pool_saturated_stays_retryable", func(t *testing.T) {
		task, refreshMeta, cluster := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		cluster.createErr = merr.WrapErrTooManyRequests(8, "external collection worker pool saturated")

		for range 12 {
			task.CreateTaskOnWorker(1, cluster)
		}

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
		assert.Empty(t, metaTask.GetFailReason())
		assert.Equal(t, taskcommon.Init, task.GetTaskState())
		counter, ok := refreshMeta.workerFailureCounts.Get(1001)
		assert.True(t, !ok || counter.Load() == 0,
			"capacity backoff must not consume the execution-failure budget")
	})

	t.Run("create_outcome_unknown_retries", func(t *testing.T) {
		task, refreshMeta, cluster := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		cluster.createErr = context.DeadlineExceeded

		task.CreateTaskOnWorker(1, cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), context.DeadlineExceeded.Error())
	})

	t.Run("create_errors_fail_at_configured_limit", func(t *testing.T) {
		const maxRetryKey = "dataCoord.externalCollectionMaxRetryTimes"
		paramtable.Get().Save(maxRetryKey, "2")
		defer paramtable.Get().Reset(maxRetryKey)

		task, refreshMeta, cluster := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		cluster.createErr = merr.WrapErrServiceInternalMsg("worker exploded")

		task.CreateTaskOnWorker(1, cluster)
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker failure 1/2")

		// Stand in for the inspector's fresh Init replacement. Inspector tests
		// separately prove that the counter transfers across the new task ID.
		assert.NoError(t, refreshMeta.UpdateTaskState(1001, indexpb.JobState_JobStateInit, ""))
		retryTask := newRefreshExternalCollectionTask(metaTask, refreshMeta, task.mt, task.allocator)
		retryTask.ExternalCollectionRefreshTask = refreshMeta.GetTask(1001)
		retryTask.CreateTaskOnWorker(2, cluster)
		metaTask = refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker failure 2/2")
		assert.Contains(t, metaTask.GetFailReason(), "worker exploded")
	})

	t.Run("catalog_failure_does_not_consume_worker_failure_budget", func(t *testing.T) {
		task, refreshMeta, _ := newOwnershipTask(t, externalRefreshOwnershipPlanVersion, []int64{1})
		saveFails := true
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(context.Context, *datapb.ExternalCollectionRefreshTask) error {
				if saveFails {
					return errors.New("catalog unavailable")
				}
				return nil
			}).Build()
		defer mockSave.UnPatch()

		assert.Error(t, task.retryWorkerFailure("worker failed"))
		assert.Error(t, task.retryWorkerFailure("worker failed"))
		counter, ok := refreshMeta.workerFailureCounts.Get(task.GetTaskId())
		assert.True(t, ok)
		assert.Zero(t, counter.Load())
		assert.Equal(t, indexpb.JobState_JobStateInit, refreshMeta.GetTask(task.GetTaskId()).GetState())

		saveFails = false
		assert.NoError(t, task.retryWorkerFailure("worker failed"))
		assert.Equal(t, int64(1), counter.Load())
		assert.Contains(t, refreshMeta.GetTask(task.GetTaskId()).GetFailReason(), "worker failure 1/10")
	})

	for _, test := range []struct {
		name            string
		ownedSegmentIDs []int64
		prepare         func(*refreshExternalCollectionTask)
		wantSegmentIDs  []int64
		wantError       string
	}{
		{name: "owned_segments_only", ownedSegmentIDs: []int64{1}, wantSegmentIDs: []int64{1}},
		{name: "empty_ownership", wantSegmentIDs: []int64{}},
		{name: "missing_owned_segment", ownedSegmentIDs: []int64{3}, wantError: "owned segment 3 not found"},
		{
			name:            "dropped_owned_segment",
			ownedSegmentIDs: []int64{1},
			prepare: func(task *refreshExternalCollectionTask) {
				task.mt.segments.GetSegment(1).State = commonpb.SegmentState_Dropped
			},
			wantError: "owned segment 1 is not active",
		},
		{
			name:            "foreign_owned_segment",
			ownedSegmentIDs: []int64{1},
			prepare: func(task *refreshExternalCollectionTask) {
				task.mt.segments.GetSegment(1).CollectionID = 200
			},
			wantError: "belongs to collection 200",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			task, refreshMeta, cluster := newOwnershipTask(
				t,
				externalRefreshOwnershipPlanVersion,
				test.ownedSegmentIDs,
			)
			if test.prepare != nil {
				test.prepare(task)
			}
			task.CreateTaskOnWorker(1, cluster)

			metaTask := refreshMeta.GetTask(1001)
			if test.wantError != "" {
				assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
				assert.Contains(t, metaTask.GetFailReason(), test.wantError)
				assert.Nil(t, cluster.refreshReq)
				return
			}
			assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
			if assert.NotNil(t, cluster.refreshReq) {
				assert.Equal(t, int64(10), cluster.refreshReq.GetPartitionID())
				gotSegmentIDs := make([]int64, 0, len(cluster.refreshReq.GetCurrentSegments()))
				for _, segment := range cluster.refreshReq.GetCurrentSegments() {
					gotSegmentIDs = append(gotSegmentIDs, segment.GetID())
				}
				assert.Equal(t, test.wantSegmentIDs, gotSegmentIDs)
			}
		})
	}
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
		assert.Equal(t, int64(1), cluster.droppedNodeID)
		assert.Equal(t, int64(1001), cluster.droppedTaskID)
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

		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).
			Return(nil, merr.WrapErrNodeNotFound(1, "query failed")).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "query task failed")
	})

	t.Run("query_outcome_unknown_retries_task", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		assert.NoError(t, refreshMeta.AddJob(&datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}))
		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1001,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		assert.NoError(t, refreshMeta.AddTask(protoTask))

		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, &stubAllocator{nextID: 99999})
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).
			Return(nil, context.DeadlineExceeded).Build()
		defer mockQuery.UnPatch()

		cluster := &stubCluster{}
		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), context.DeadlineExceeded.Error())
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

		// A worker reporting Failed has classified the failure as permanent --
		// the request is what it could not satisfy. Spending the retry budget on
		// it only delays the RefreshFailed the caller is waiting for, which is
		// what left permanent input errors sitting in RefreshInProgress.
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State:      indexpb.JobState_JobStateFailed,
			FailReason: "worker error",
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState(),
			"a permanent worker failure must end the task on the first report")
		assert.Contains(t, metaTask.GetFailReason(), "worker error")
	})

	t.Run("task_retry_requested_by_worker", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		job := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			TaskIds:      []int64{1002},
		}
		assert.NoError(t, refreshMeta.AddJob(job))

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:         1002,
			JobId:          1,
			CollectionId:   100,
			NodeId:         1,
			State:          indexpb.JobState_JobStateInProgress,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		assert.NoError(t, refreshMeta.AddTask(protoTask))

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc)
		cluster := &stubCluster{}

		// A transient fault on the worker: this is what the retry budget is for.
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State:      indexpb.JobState_JobStateRetry,
			FailReason: "SlowDown: please reduce your request rate",
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1002)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState(),
			"a retriable worker failure spends one attempt and stays retriable")
		assert.Contains(t, metaTask.GetFailReason(), "SlowDown")
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
		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    500,
			},
		})

		mt := &meta{
			catalog:     catalog,
			segments:    segments,
			collections: newTestCollections(100),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		// Mock QueryRefreshExternalCollectionTask to return Finished with response
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State:        indexpb.JobState_JobStateFinished,
			KeptSegments: []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{
				newTestExternalRefreshSegment(10, 100, 1000),
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
			segments:    NewSegmentsInfo(),
			collections: collections,
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}

		// Mock QueryRefreshExternalCollectionTask to return Finished
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
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
	// found" (failure).
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
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
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
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateInit,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// Task state must remain InProgress (no-op), not Failed.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
		assert.Empty(t, metaTask.GetFailReason())
	})

	t.Run("task_state_retry_honors_configured_limit", func(t *testing.T) {
		const maxRetryKey = "dataCoord.externalCollectionMaxRetryTimes"
		paramtable.Get().Save(maxRetryKey, "2")
		defer paramtable.Get().Reset(maxRetryKey)

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

		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State:      indexpb.JobState_JobStateRetry,
			FailReason: "worker requested retry",
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker failure 1/2")

		// A persisted Retry response is idempotent until the inspector publishes
		// its replacement; polling the same attempt again cannot spend budget.
		task.QueryTaskOnWorker(cluster)
		metaTask = refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker failure 1/2")
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
		collections: newTestCollections(100),
	}

	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

	cluster := &stubCluster{}

	// Mock QueryRefreshExternalCollectionTask to return Finished with response
	mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State:        indexpb.JobState_JobStateFinished,
		KeptSegments: []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{
			newTestExternalRefreshSegment(1, 100, 1000),
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

func TestRefreshExternalCollectionTask_QueryTaskOnWorker_DelaysSegmentUpdateUntilJobFinished(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInit,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	assert.NoError(t, refreshMeta.AddJob(job))

	task1 := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		NodeId:         1,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	task2 := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1002,
		JobId:          1,
		CollectionId:   100,
		NodeId:         1,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	assert.NoError(t, refreshMeta.AddTasksToJob(1, []*datapb.ExternalCollectionRefreshTask{task1, task2}))

	mt := &meta{
		catalog:     catalog,
		segments:    NewSegmentsInfo(),
		collections: newTestCollections(100),
	}

	cluster := &stubCluster{}
	mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State:        indexpb.JobState_JobStateFinished,
		KeptSegments: []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{
			newTestExternalRefreshSegment(10, 100, 7),
		},
	}, nil).Build()
	defer mockQuery.UnPatch()

	updateCalls := 0
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).To(func(_ *meta, _ context.Context, _ ...UpdateOperator) error {
		updateCalls++
		return nil
	}).Build()
	defer mockUpdate.UnPatch()

	task := newRefreshExternalCollectionTask(task1, refreshMeta, mt, &stubAllocator{nextID: 99999})
	task.QueryTaskOnWorker(cluster)

	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFinished, metaTask.GetState())
	assert.Equal(t, 0, updateCalls)
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_UpsertExistingSegment(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	partitionID := int64(1)
	segmentID := int64(10)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	oldSeg := &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  "by-dev-rootcoord-dml_0_v1",
		NumOfRows:      100,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: 3,
		Level:          datapb.SegmentLevel_L1,
		IsSorted:       true,
		ManifestPath:   `{"base_path":"old","ver":1}`,
		SchemaVersion:  3,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     0,
			ChildFields: []int64{100, 101, 102},
			Binlogs: []*datapb.Binlog{{
				LogID:      10,
				EntriesNum: 100,
				MemorySize: 1000,
				LogSize:    1000,
			}},
		}},
	}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(oldSeg))

	patched := proto.Clone(oldSeg).(*datapb.SegmentInfo)
	patched.ManifestPath = `{"base_path":"old","ver":2}`
	patched.SchemaVersion = 4
	patched.Level = datapb.SegmentLevel_L0
	patched.IsSorted = false
	patched.Binlogs = []*datapb.FieldBinlog{{
		FieldID:     0,
		ChildFields: []int64{100, 101, 102, 103},
		Binlogs: []*datapb.Binlog{{
			LogID:      10,
			EntriesNum: 100,
			MemorySize: 1400,
			LogSize:    1400,
		}},
	}}

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		[]int64{segmentID},
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.NoError(t, err)

	got := mt.segments.GetSegment(segmentID)
	assert.NotNil(t, got)
	assert.Equal(t, commonpb.SegmentState_Flushed, got.GetState())
	assert.Equal(t, int64(100), got.GetNumOfRows())
	assert.Equal(t, datapb.SegmentLevel_L1, got.GetLevel())
	assert.True(t, got.GetIsSorted())
	assert.Equal(t, `{"base_path":"old","ver":2}`, got.GetManifestPath())
	assert.Equal(t, int32(4), got.GetSchemaVersion())
	assert.ElementsMatch(t, []int64{100, 101, 102, 103}, got.GetBinlogs()[0].GetChildFields())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_ReplayNewSegment(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentID := int64(10)
	manifestBasePath := "files/insert_log/100/1/10"
	catalog := &stubCatalog{}
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     catalog,
	}
	incoming := newTestExternalRefreshSegment(segmentID, collectionID, 100)
	incoming.ManifestPath = packed.MarshalManifestPath(manifestBasePath, 1)

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{incoming},
	)
	assert.NoError(t, err)

	// Match the V3 catalog representation after a DataCoord restart: the
	// manifest and aggregate stats survive, while fake binlogs do not. Also
	// advance the manifest to model a later stats/index update.
	persisted := mt.segments.GetSegment(segmentID).Clone()
	persisted.Binlogs = nil
	persisted.ManifestPath = packed.MarshalManifestPath(manifestBasePath, 2)
	persisted.TextStatsLogs = map[int64]*datapb.TextIndexStats{1: {FieldID: 1}}
	persisted.JsonKeyStats = map[int64]*datapb.JsonKeyStats{2: {FieldID: 2}}
	mt.segments.SetSegment(segmentID, persisted)
	catalog.alteredSegments = nil

	err = applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{incoming},
	)
	assert.NoError(t, err)
	assert.Nil(t, catalog.alteredSegments)
	assert.Equal(t, packed.MarshalManifestPath(manifestBasePath, 2), mt.segments.GetSegment(segmentID).GetManifestPath())
	assert.Empty(t, mt.segments.GetSegment(segmentID).GetBinlogs())
	assert.Contains(t, mt.segments.GetSegment(segmentID).GetTextStatsLogs(), int64(1))
	assert.Contains(t, mt.segments.GetSegment(segmentID).GetJsonKeyStats(), int64(2))

	differentBaseReplay := proto.Clone(incoming).(*datapb.SegmentInfo)
	differentBaseReplay.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/1/999", 1)
	err = applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{differentBaseReplay},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collides with existing metadata")

	newerReplay := proto.Clone(incoming).(*datapb.SegmentInfo)
	newerReplay.ManifestPath = packed.MarshalManifestPath(manifestBasePath, 3)
	err = applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{newerReplay},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collides with existing metadata")
}

func TestApplyExternalRefreshPatchClearsStatsPlaceholders(t *testing.T) {
	oldManifest := packed.MarshalManifestPath("files/insert_log/100/200/300", 1)
	newManifest := packed.MarshalManifestPath("files/insert_log/100/200/300", 2)

	oldSeg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             300,
			CollectionID:   100,
			PartitionID:    200,
			NumOfRows:      1000,
			ManifestPath:   oldManifest,
			StorageVersion: storage.StorageV3,
			SchemaVersion:  1,
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				500: {
					FieldID: 500,
					Version: 1,
					BuildID: 10,
					Files:   []string{"files/insert_log/100/200/300/_stats/text_index.500/tokenizer.json"},
				},
			},
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				500: {
					FieldID:                500,
					Version:                1,
					BuildID:                10,
					Files:                  []string{"shared_key_index/.managed.json_0"},
					JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion,
				},
			},
		},
	}
	incoming := &datapb.SegmentInfo{
		ID:             300,
		CollectionID:   100,
		PartitionID:    200,
		NumOfRows:      1000,
		ManifestPath:   newManifest,
		StorageVersion: storage.StorageV3,
		SchemaVersion:  2,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     0,
			ChildFields: []int64{100, 500},
			Binlogs: []*datapb.Binlog{{
				LogID:      300,
				EntriesNum: 1000,
				MemorySize: 4096,
				LogSize:    4096,
			}},
		}},
	}

	patched := applyExternalRefreshPatch(oldSeg, incoming)
	assert.Equal(t, newManifest, patched.GetManifestPath())
	assert.Empty(t, patched.GetTextStatsLogs())
	assert.Empty(t, patched.GetJsonKeyStats())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectPatchRowCountChange(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentID := int64(10)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	oldSeg := &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    1,
		InsertChannel:  "by-dev-rootcoord-dml_0_v1",
		NumOfRows:      100,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: 3,
		ManifestPath:   `{"base_path":"old","ver":1}`,
		SchemaVersion:  3,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 0,
			Binlogs: []*datapb.Binlog{{
				LogID:      10,
				EntriesNum: 100,
				MemorySize: 1000,
				LogSize:    1000,
			}},
		}},
	}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(oldSeg))

	patched := proto.Clone(oldSeg).(*datapb.SegmentInfo)
	patched.NumOfRows = 101
	patched.ManifestPath = `{"base_path":"old","ver":2}`

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		[]int64{segmentID},
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "row count changed")
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectNewSegmentIDCollidingWithDroppedSegment(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentID := int64(10)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	oldSeg := &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    1,
		InsertChannel:  "by-dev-rootcoord-dml_0_v1",
		NumOfRows:      100,
		State:          commonpb.SegmentState_Dropped,
		StorageVersion: 3,
		ManifestPath:   `{"base_path":"old","ver":1}`,
		SchemaVersion:  3,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID:     0,
			ChildFields: []int64{100, 101, 102},
			Binlogs: []*datapb.Binlog{{
				LogID:      10,
				EntriesNum: 100,
				MemorySize: 1000,
				LogSize:    1000,
			}},
		}},
	}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(oldSeg))

	patched := proto.Clone(oldSeg).(*datapb.SegmentInfo)
	patched.ManifestPath = `{"base_path":"old","ver":2}`
	patched.SchemaVersion = 4

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collides with existing metadata")
	assert.Equal(t, commonpb.SegmentState_Dropped, mt.segments.GetSegment(segmentID).GetState())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectNewSegmentCollectionMismatch(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{{
			ID:             10,
			CollectionID:   collectionID + 1,
			NumOfRows:      100,
			StorageVersion: 3,
			ManifestPath:   `{"base_path":"new","ver":1}`,
			SchemaVersion:  1,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID: 0,
				Binlogs: []*datapb.Binlog{{
					LogID:      10,
					EntriesNum: 100,
					MemorySize: 1000,
					LogSize:    1000,
				}},
			}},
		}},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection mismatch")
	assert.Nil(t, mt.segments.GetSegment(10))
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectNewSegmentEmptyManifest(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, collectionID, 100)
	seg.ManifestPath = ""

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{seg},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty manifest path")
	assert.Nil(t, mt.segments.GetSegment(10))
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectNewSegmentIDCollidingWithOtherCollection(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentID := int64(10)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID + 1,
		PartitionID:    1,
		InsertChannel:  "by-dev-rootcoord-dml_1_v1",
		NumOfRows:      100,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: 3,
		ManifestPath:   `{"base_path":"other","ver":1}`,
		SchemaVersion:  3,
		Binlogs: []*datapb.FieldBinlog{{
			FieldID: 0,
			Binlogs: []*datapb.Binlog{{
				LogID:      10,
				EntriesNum: 100,
				MemorySize: 1000,
				LogSize:    1000,
			}},
		}},
	}))
	incoming := newTestExternalRefreshSegment(segmentID, collectionID, 100)

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{incoming},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collides with existing metadata")
	got := mt.segments.GetSegment(segmentID)
	assert.NotNil(t, got)
	assert.Equal(t, collectionID+1, got.GetCollectionID())
	assert.Equal(t, `{"base_path":"other","ver":1}`, got.GetManifestPath())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectKeptSegmentOutsideBaseline(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	mt.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           1,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    100,
	}))

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		[]int64{1},
		[]int64{999},
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kept segment 999 is outside the refresh baseline")
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(1).GetState())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectForeignKeptSegmentOutsideBaseline(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	mt.segments.SetSegment(10, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           10,
		CollectionID: collectionID + 1,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    100,
	}))

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		[]int64{10},
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kept segment 10 is outside the refresh baseline")
	assert.Equal(t, collectionID+1, mt.segments.GetSegment(10).GetCollectionID())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectDroppedKeptSegmentOutsideBaseline(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	mt.segments.SetSegment(10, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           10,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Dropped,
		NumOfRows:    100,
	}))

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		[]int64{10},
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kept segment 10 is outside the refresh baseline")
	assert.Equal(t, commonpb.SegmentState_Dropped, mt.segments.GetSegment(10).GetState())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_NormalizeNewSegmentCollection(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, 0, 100)

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{seg},
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), seg.GetCollectionID())
	assert.Equal(t, int64(0), seg.GetPartitionID())
	assert.Empty(t, seg.GetInsertChannel())

	got := mt.segments.GetSegment(10)
	assert.NotNil(t, got)
	assert.Equal(t, collectionID, got.GetCollectionID())
	assert.Equal(t, int64(1), got.GetPartitionID())
	assert.Equal(t, "by-dev-rootcoord-dml_0_v1", got.GetInsertChannel())
	assert.Equal(t, commonpb.SegmentState_Flushed, got.GetState())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectPatchBinlogRowCountMismatch(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentID := int64(10)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	oldSeg := newTestExternalRefreshSegment(segmentID, collectionID, 100)
	oldSeg.State = commonpb.SegmentState_Flushed
	oldSeg.PartitionID = 1
	oldSeg.InsertChannel = "by-dev-rootcoord-dml_0_v1"
	mt.segments.SetSegment(segmentID, NewSegmentInfo(oldSeg))

	patched := proto.Clone(oldSeg).(*datapb.SegmentInfo)
	patched.ManifestPath = `{"base_path":"old","ver":2}`
	patched.Binlogs[0].Binlogs[0].EntriesNum = 99

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		[]int64{segmentID},
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Equal(t, `{"base_path":"new","ver":1}`, mt.segments.GetSegment(segmentID).GetManifestPath())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectNewBinlogRowCountMismatch(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, collectionID, 100)
	seg.Binlogs[0].Binlogs[0].EntriesNum = 99

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{seg},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Nil(t, mt.segments.GetSegment(10))
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectPatchEmptyNestedBinlogs(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentID := int64(10)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	oldSeg := newTestExternalRefreshSegment(segmentID, collectionID, 100)
	oldSeg.State = commonpb.SegmentState_Flushed
	oldSeg.PartitionID = 1
	oldSeg.InsertChannel = "by-dev-rootcoord-dml_0_v1"
	mt.segments.SetSegment(segmentID, NewSegmentInfo(oldSeg))

	patched := proto.Clone(oldSeg).(*datapb.SegmentInfo)
	patched.ManifestPath = `{"base_path":"old","ver":2}`
	patched.Binlogs = []*datapb.FieldBinlog{{FieldID: 0}}

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		[]int64{segmentID},
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Equal(t, `{"base_path":"new","ver":1}`, mt.segments.GetSegment(segmentID).GetManifestPath())
}

func TestApplyExternalCollectionSegmentUpdateForBaseline_RejectNewEmptyNestedBinlogs(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, collectionID, 100)
	seg.Binlogs = []*datapb.FieldBinlog{{FieldID: 0}}

	err := applyExternalCollectionSegmentUpdateForBaseline(
		ctx,
		mt,
		collectionID,
		nil,
		nil,
		[]*datapb.SegmentInfo{seg},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Nil(t, mt.segments.GetSegment(10))
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
	mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State: indexpb.JobState_JobStateFinished,
	}, nil).Build()
	defer mockQuery.UnPatch()

	task.QueryTaskOnWorker(cluster)

	// Task should be marked as failed due to source mismatch
	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
	assert.Contains(t, metaTask.GetFailReason(), "task source mismatch")
}

func TestRefreshExternalCollectionTask_QueryTaskOnWorker_FinishedPersistsResultWithoutSegmentMeta(t *testing.T) {
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

	// Task has nil mt. Finished task handling must still succeed because
	// segment metadata is applied later at job level, not by this task.
	alloc := &stubAllocator{nextID: 99999}
	task := newRefreshExternalCollectionTask(protoTask, refreshMeta, nil, alloc) // mt is nil

	cluster := &stubCluster{}

	// Mock query to return Finished
	mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
		State:        indexpb.JobState_JobStateFinished,
		KeptSegments: []int64{10},
		UpdatedSegments: []*datapb.SegmentInfo{
			newTestExternalRefreshSegment(20, 100, 7),
		},
	}, nil).Build()
	defer mockQuery.UnPatch()

	task.QueryTaskOnWorker(cluster)

	metaTask := refreshMeta.GetTask(1001)
	assert.Equal(t, indexpb.JobState_JobStateFinished, metaTask.GetState())
	assert.Empty(t, metaTask.GetFailReason())
	assert.Equal(t, []int64{10}, metaTask.GetKeptSegments())
	assert.Len(t, metaTask.GetUpdatedSegments(), 1)
	assert.Equal(t, int64(20), metaTask.GetUpdatedSegments()[0].GetID())
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

// ==================== CreateTaskOnWorker Additional Tests ====================

func TestRefreshExternalCollectionTask_CreateTaskOnWorker_TaskNotFoundAfterVersionUpdate(t *testing.T) {
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	assert.NoError(t, refreshMeta.AddJob(&datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInProgress,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}))

	protoTask := &datapb.ExternalCollectionRefreshTask{
		TaskId:         1001,
		JobId:          1,
		CollectionId:   100,
		State:          indexpb.JobState_JobStateInit,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   "iceberg",
	}
	addOwnershipTestRefreshTask(t, refreshMeta, protoTask)

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

		// Mock DropRefreshExternalCollectionTask to return error
		mockDrop := mockey.Mock((*stubCluster).DropRefreshExternalCollectionTask).Return(errors.New("drop failed")).Build()
		defer mockDrop.UnPatch()

		task.DropTaskOnWorker(cluster)
		// Error is logged but not returned
	})
}
