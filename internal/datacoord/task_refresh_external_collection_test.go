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
	refreshReq *datapb.RefreshExternalCollectionTaskRequest
}

func (s *stubCluster) CreateRefreshExternalCollectionTask(nodeID int64, req *datapb.RefreshExternalCollectionTaskRequest) error {
	s.refreshReq = req
	return nil
}

func (s *stubCluster) QueryRefreshExternalCollectionTask(nodeID int64, taskID int64) (*datapb.RefreshExternalCollectionTaskResponse, error) {
	return &datapb.RefreshExternalCollectionTaskResponse{
		State: indexpb.JobState_JobStateInProgress,
	}, nil
}

func (s *stubCluster) DropRefreshExternalCollectionTask(nodeID int64, taskID int64, version int64) error {
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
		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    500,
			},
		})
		segments.SetSegment(2, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				CollectionID: 100,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    600,
			},
		})
		segments.SetSegment(3, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           3,
				CollectionID: 100,
				State:        commonpb.SegmentState_Dropped, // already dropped
				NumOfRows:    100,
			},
		})

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
				newTestExternalRefreshSegment(10, 100, 1000),
			},
		}

		err = task.SetJobInfo(ctx, resp)
		assert.NoError(t, err)
		newSegment := mt.segments.GetSegment(10)
		assert.NotNil(t, newSegment)
		assert.False(t, newSegment.GetIsSorted())
		assert.Equal(t, commonpb.SegmentState_Flushed, newSegment.GetState())
		assert.Equal(t, "by-dev-rootcoord-dml_0_v1", newSegment.GetInsertChannel())
		assert.Equal(t, int64(1), newSegment.GetPartitionID())
	})

	t.Run("success_update_existing_segment_manifest", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		segments := NewSegmentsInfo()
		segments.SetSegment(1, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				CollectionID:   100,
				PartitionID:    1,
				InsertChannel:  "by-dev-rootcoord-dml_0_v1",
				State:          commonpb.SegmentState_Flushed,
				NumOfRows:      500,
				ManifestPath:   `{"base_path":"seg1","ver":1}`,
				StorageVersion: 3,
			},
		})

		mt := &meta{
			catalog:     catalog,
			segments:    segments,
			collections: newTestCollections(100),
		}

		task := createTestRefreshTaskWithMetaAndStubs(t, 1001, 1, 100, mt, refreshMeta)
		updated := newTestExternalRefreshSegment(1, 100, 500)
		updated.ManifestPath = `{"base_path":"seg1","ver":2}`
		// Existing-segment patch must carry the current manifest as its base so the
		// fail-closed adoption CAS accepts it.
		updated.BaseManifest = `{"base_path":"seg1","ver":1}`
		resp := &datapb.RefreshExternalCollectionTaskResponse{
			UpdatedSegments: []*datapb.SegmentInfo{updated},
		}

		err = task.SetJobInfo(ctx, resp)
		assert.NoError(t, err)
		segment := mt.segments.GetSegment(1)
		assert.NotNil(t, segment)
		assert.Equal(t, commonpb.SegmentState_Flushed, segment.GetState())
		assert.Equal(t, `{"base_path":"seg1","ver":2}`, segment.GetManifestPath())
		assert.Equal(t, uint64(0), segment.GetDroppedAt())
		assert.Len(t, catalog.alteredSegments, 1)
		assert.Equal(t, int64(1), catalog.alteredSegments[0].GetID())
	})

	t.Run("high_drop_ratio_warning", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Create 10 segments
		segments := NewSegmentsInfo()
		for i := int64(1); i <= 10; i++ {
			segments.SetSegment(i, &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:           i,
					CollectionID: 100,
					State:        commonpb.SegmentState_Flushed,
					NumOfRows:    100,
				},
			})
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
				newTestExternalRefreshSegment(20, 100, 2000),
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
		segments := NewSegmentsInfo()
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
				newTestExternalRefreshSegment(1, 100, 1000),
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

		// A nil meta is internal/transient (not a data error), so the task is reset
		// to Init for re-dispatch rather than failing the job.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
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
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save failed")).Build()
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

		// An allocation failure is transient, so the task is reset to Init for
		// re-dispatch rather than failing the job.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
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

		segments := NewSegmentsInfo()
		mt := &meta{
			segments:    segments,
			collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)

		cluster := &stubCluster{}
		task.CreateTaskOnWorker(1, cluster)

		// Collection gone is a permanent (data) error, so the task is failed.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "collection")
	})

	t.Run("dispatch_carries_bumped_task_version_and_fences_prior_attempt", func(t *testing.T) {
		catalog := &stubCatalog{}
		refreshMeta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		protoTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:       1001,
			JobId:        1,
			CollectionId: 100,
			// A prior attempt ran on node 7: the re-dispatch must drop the
			// worker-side entry there before creating the new attempt.
			NodeId:         7,
			Version:        3,
			State:          indexpb.JobState_JobStateInit,
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}
		assert.NoError(t, refreshMeta.AddTask(protoTask))

		segments := NewSegmentsInfo()
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID:         100,
			Schema:     &schemapb.CollectionSchema{Name: "test_coll"},
			Partitions: []int64{10},
		})
		mt := &meta{segments: segments, collections: collections}

		alloc := &stubAllocator{nextID: 99999}
		task := newRefreshExternalCollectionTask(protoTask, refreshMeta, mt, alloc)
		cluster := &stubCluster{}

		var droppedNode, droppedTask, droppedVersion int64
		mockDrop := mockey.Mock((*stubCluster).DropRefreshExternalCollectionTask).To(
			func(_ *stubCluster, nodeID, taskID, version int64) error {
				droppedNode, droppedTask, droppedVersion = nodeID, taskID, version
				return nil
			}).Build()
		defer mockDrop.UnPatch()

		var sentReq *datapb.RefreshExternalCollectionTaskRequest
		mockCreate := mockey.Mock((*stubCluster).CreateRefreshExternalCollectionTask).To(
			func(_ *stubCluster, _ int64, req *datapb.RefreshExternalCollectionTaskRequest) error {
				sentReq = req
				return nil
			}).Build()
		defer mockCreate.UnPatch()

		task.CreateTaskOnWorker(8, cluster)

		// Prior attempt on node 7 was fenced by a defensive drop carrying the
		// prior attempt's version (3, before the dispatch bump).
		assert.Equal(t, int64(7), droppedNode)
		assert.Equal(t, int64(1001), droppedTask)
		assert.Equal(t, int64(3), droppedVersion)
		// The dispatch carries the bumped persisted version so the worker
		// registers this attempt under it and drops older attempts' writes.
		assert.NotNil(t, sentReq)
		assert.Equal(t, refreshMeta.GetTask(1001).GetVersion(), sentReq.GetTaskVersion())
		assert.Greater(t, sentReq.GetTaskVersion(), int64(3))
		assert.Equal(t, indexpb.JobState_JobStateInProgress, refreshMeta.GetTask(1001).GetState())
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

		// Mock CreateRefreshExternalCollectionTask to return error
		mockCreate := mockey.Mock((*stubCluster).CreateRefreshExternalCollectionTask).Return(errors.New("create task failed")).Build()
		defer mockCreate.UnPatch()

		task.CreateTaskOnWorker(1, cluster)

		// A dispatch RPC failure is transient, so the task is reset to Init for
		// re-dispatch rather than failing the job.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "create task failed")
	})

	t.Run("success", func(t *testing.T) {
		const targetRowsPerSegmentKey = "dataNode.externalCollection.targetRowsPerSegment"
		paramtable.Get().Save(targetRowsPerSegmentKey, "12345")
		defer paramtable.Get().Reset(targetRowsPerSegmentKey)

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

		task.CreateTaskOnWorker(1, cluster)

		// Task should be marked as in progress
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, metaTask.GetState())
		assert.NotNil(t, cluster.refreshReq)
		assert.Equal(t, int64(10), cluster.refreshReq.GetPartitionID())
		assert.Equal(t, int64(12345), cluster.refreshReq.GetTargetRowsPerSegment())
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
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(nil, errors.New("query failed")).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		// A query RPC failure is transient: the task is dropped on the worker and
		// reset to Init for re-dispatch, not failed.
		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
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
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
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

	t.Run("task_state_retry_redispatches", func(t *testing.T) {
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

		// A worker-requested retry is honored: the task is dropped on the worker
		// and reset to Init for re-dispatch, not failed.
		mockQuery := mockey.Mock((*stubCluster).QueryRefreshExternalCollectionTask).Return(&datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateRetry,
		}, nil).Build()
		defer mockQuery.UnPatch()

		task.QueryTaskOnWorker(cluster)

		metaTask := refreshMeta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInit, metaTask.GetState())
		assert.Contains(t, metaTask.GetFailReason(), "worker requested retry")
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
		State:          indexpb.JobState_JobStateInProgress,
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
	assert.NoError(t, refreshMeta.AddTask(task1))
	assert.NoError(t, refreshMeta.AddTask(task2))
	assert.NoError(t, refreshMeta.AddTaskIDToJob(1, 1001))
	assert.NoError(t, refreshMeta.AddTaskIDToJob(1, 1002))

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

func TestApplyExternalCollectionSegmentUpdate_UpsertExistingSegment(t *testing.T) {
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
	// Existing-segment patch must carry the current manifest as its base so the
	// fail-closed adoption CAS accepts it.
	patched.BaseManifest = oldSeg.GetManifestPath()
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
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

// TestApplyExternalCollectionSegmentUpdate_StalePatchAborts covers the adoption-time
// optimistic-concurrency CAS for an EXISTING segment: it fails closed. A patch whose
// base no longer matches the current manifest (a concurrent index commit advanced it)
// or whose base is empty (a pre-CAS / rolling-upgrade worker that cannot prove it) is
// NOT adopted — the whole apply aborts atomically with errExternalRefreshStaleManifest
// (nothing mutated, concurrent commit preserved) and the checker rebuilds on the
// current manifest. Only a base that equals the current manifest is patched.
func TestApplyExternalCollectionSegmentUpdate_StalePatchAborts(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	partitionID := int64(1)
	segmentID := int64(10)
	currentManifest := `{"base_path":"old","ver":2}`
	patchedManifest := `{"base_path":"old","ver":3}`

	newMeta := func() *meta {
		mt := &meta{
			collections: newTestCollections(collectionID),
			segments:    NewSegmentsInfo(),
			catalog:     &stubCatalog{},
		}
		mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    partitionID,
			InsertChannel:  "by-dev-rootcoord-dml_0_v1",
			NumOfRows:      100,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: 3,
			Level:          datapb.SegmentLevel_L1,
			ManifestPath:   currentManifest,
			SchemaVersion:  3,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs:     []*datapb.Binlog{{LogID: 10, EntriesNum: 100, MemorySize: 1000, LogSize: 1000}},
			}},
		}))
		return mt
	}
	patchedSeg := func(base string) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    partitionID,
			InsertChannel:  "by-dev-rootcoord-dml_0_v1",
			NumOfRows:      100,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: 3,
			ManifestPath:   patchedManifest,
			BaseManifest:   base,
			SchemaVersion:  4,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102, 103},
				Binlogs:     []*datapb.Binlog{{LogID: 10, EntriesNum: 100, MemorySize: 1400, LogSize: 1400}},
			}},
		}
	}

	t.Run("stale_base_aborts", func(t *testing.T) {
		mt := newMeta()
		err := applyExternalCollectionSegmentUpdate(ctx, mt, collectionID, nil,
			[]*datapb.SegmentInfo{patchedSeg(`{"base_path":"old","ver":1}`)})
		assert.ErrorIs(t, err, errExternalRefreshStaleManifest)
		// Nothing mutated: the segment keeps the concurrent commit's manifest.
		got := mt.segments.GetSegment(segmentID)
		assert.NotNil(t, got)
		assert.Equal(t, currentManifest, got.GetManifestPath())
		assert.NotEqual(t, commonpb.SegmentState_Dropped, got.GetState())
	})
	t.Run("matching_base_patched", func(t *testing.T) {
		mt := newMeta()
		err := applyExternalCollectionSegmentUpdate(ctx, mt, collectionID, nil,
			[]*datapb.SegmentInfo{patchedSeg(currentManifest)})
		assert.NoError(t, err)
		assert.Equal(t, patchedManifest, mt.segments.GetSegment(segmentID).GetManifestPath())
	})
	t.Run("empty_base_aborts", func(t *testing.T) {
		// Fail closed for an existing segment: an empty base (pre-CAS / rolling-upgrade
		// worker) cannot prove it built on the current manifest, so it is rejected
		// rather than blindly overwriting a possible concurrent commit.
		mt := newMeta()
		err := applyExternalCollectionSegmentUpdate(ctx, mt, collectionID, nil,
			[]*datapb.SegmentInfo{patchedSeg("")})
		assert.ErrorIs(t, err, errExternalRefreshStaleManifest)
		assert.Equal(t, currentManifest, mt.segments.GetSegment(segmentID).GetManifestPath())
	})
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

func TestIsRetryableRefreshFailure(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		retry bool
	}{
		{"nil", nil, false},
		{"permanent_marker", errExternalRefreshPermanent, false},
		{"parameter_invalid", merr.WrapErrParameterInvalidMsg("bad request"), false},
		{"parameter_missing", merr.WrapErrParameterMissingMsg("missing field"), false},
		{"collection_not_found", merr.WrapErrCollectionNotFound(int64(1)), false},
		{"data_integrity_permanent", merr.WrapErrDataIntegrityMsg("corrupt manifest"), false},
		{"storage_permanent", merr.WrapErrStorageMsg("hard storage error"), false},
		{"too_many_requests_retries", merr.WrapErrIoTooManyRequests("k", errors.New("throttled")), true},
		{"untyped_transient_retries", errors.New("s3: connection reset by peer"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.retry, isRetryableRefreshFailure(c.err))
		})
	}
}

func TestExternalRefreshPatchIsNoop(t *testing.T) {
	manifest := packed.MarshalManifestPath("files/insert_log/100/200/300", 5)
	fakeBinlogs := func(child []int64, mem int64) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{
			FieldID:     0,
			ChildFields: child,
			Binlogs:     []*datapb.Binlog{{LogID: 300, EntriesNum: 1000, MemorySize: mem, LogSize: mem}},
		}}
	}
	base := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 300, ManifestPath: manifest, SchemaVersion: 3, StorageVersion: storage.StorageV3,
		Binlogs: fakeBinlogs([]int64{100, 101}, 4096),
	}}
	same := func() *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID: 300, ManifestPath: manifest, SchemaVersion: 3, StorageVersion: storage.StorageV3,
			Binlogs: fakeBinlogs([]int64{100, 101}, 4096),
		}
	}

	assert.True(t, externalRefreshPatchIsNoop(base, same()), "identical patch must be a no-op")

	t.Run("different_manifest", func(t *testing.T) {
		in := same()
		in.ManifestPath = packed.MarshalManifestPath("files/insert_log/100/200/300", 6)
		assert.False(t, externalRefreshPatchIsNoop(base, in))
	})
	t.Run("different_schema_version", func(t *testing.T) {
		in := same()
		in.SchemaVersion = 4
		assert.False(t, externalRefreshPatchIsNoop(base, in))
	})
	t.Run("different_binlogs", func(t *testing.T) {
		in := same()
		in.Binlogs = fakeBinlogs([]int64{100, 101, 102}, 5000)
		assert.False(t, externalRefreshPatchIsNoop(base, in))
	})
	t.Run("different_storage_version", func(t *testing.T) {
		in := same()
		in.StorageVersion = storage.StorageV3 + 1
		assert.False(t, externalRefreshPatchIsNoop(base, in))
	})
	t.Run("zero_incoming_storage_version_ignored", func(t *testing.T) {
		// applyExternalRefreshPatch never overwrites the storage version when the
		// incoming one is zero, so a zero incoming version keeps the patch a no-op.
		in := same()
		in.StorageVersion = 0
		assert.True(t, externalRefreshPatchIsNoop(base, in))
	})
}

// TestApplyExternalCollectionSegmentUpdate_SameManifestReplay verifies that a
// result whose manifest already equals the segment's current manifest is NOT
// blindly skipped: only a *complete* no-op keeps the segment as-is, while a
// same-manifest result carrying a bumped schema / refreshed binlogs still lands.
func TestApplyExternalCollectionSegmentUpdate_SameManifestReplay(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	partitionID := int64(1)
	segmentID := int64(10)
	manifest := `{"base_path":"seg10","ver":5}`

	newMeta := func() *meta {
		mt := &meta{
			collections: newTestCollections(collectionID),
			segments:    NewSegmentsInfo(),
			catalog:     &stubCatalog{},
		}
		mt.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    partitionID,
			InsertChannel:  "by-dev-rootcoord-dml_0_v1",
			NumOfRows:      100,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: 3,
			Level:          datapb.SegmentLevel_L1,
			ManifestPath:   manifest,
			SchemaVersion:  3,
			TextStatsLogs:  map[int64]*datapb.TextIndexStats{500: {FieldID: 500, BuildID: 7}},
			Binlogs: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs:     []*datapb.Binlog{{LogID: 10, EntriesNum: 100, MemorySize: 1000, LogSize: 1000}},
			}},
		}))
		return mt
	}

	t.Run("same_manifest_metadata_applied", func(t *testing.T) {
		// The worker found the column already appended on the object store and
		// returned the unchanged manifest, but still bumped the schema and rebuilt
		// the fake binlogs for the new child field. The metadata must land.
		mt := newMeta()
		incoming := &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    partitionID,
			InsertChannel:  "by-dev-rootcoord-dml_0_v1",
			NumOfRows:      100,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: 3,
			ManifestPath:   manifest,
			BaseManifest:   manifest,
			SchemaVersion:  4,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102, 103},
				Binlogs:     []*datapb.Binlog{{LogID: 10, EntriesNum: 100, MemorySize: 1400, LogSize: 1400}},
			}},
		}
		err := applyExternalCollectionSegmentUpdate(ctx, mt, collectionID, nil, []*datapb.SegmentInfo{incoming})
		assert.NoError(t, err)
		got := mt.segments.GetSegment(segmentID)
		assert.Equal(t, manifest, got.GetManifestPath())
		assert.Equal(t, int32(4), got.GetSchemaVersion())
		assert.Equal(t, []int64{100, 101, 102, 103}, got.GetBinlogs()[0].GetChildFields())
		// Applying the patch clears the now-stale text stats so they rebuild.
		assert.Empty(t, got.GetTextStatsLogs())
	})

	t.Run("full_noop_preserves_stats", func(t *testing.T) {
		// Identical manifest, schema, binlogs and storage version: keep the segment
		// as-is and preserve its text stats (re-applying would clear them).
		mt := newMeta()
		incoming := &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    partitionID,
			InsertChannel:  "by-dev-rootcoord-dml_0_v1",
			NumOfRows:      100,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: 3,
			ManifestPath:   manifest,
			BaseManifest:   manifest,
			SchemaVersion:  3,
			Binlogs: []*datapb.FieldBinlog{{
				FieldID:     0,
				ChildFields: []int64{100, 101, 102},
				Binlogs:     []*datapb.Binlog{{LogID: 10, EntriesNum: 100, MemorySize: 1000, LogSize: 1000}},
			}},
		}
		err := applyExternalCollectionSegmentUpdate(ctx, mt, collectionID, nil, []*datapb.SegmentInfo{incoming})
		assert.NoError(t, err)
		got := mt.segments.GetSegment(segmentID)
		assert.Equal(t, int32(3), got.GetSchemaVersion())
		assert.Contains(t, got.GetTextStatsLogs(), int64(500))
	})
}

func TestApplyExternalCollectionSegmentUpdate_RejectPatchRowCountChange(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "row count changed")
}

func TestApplyExternalCollectionSegmentUpdate_RejectDroppedSegmentPatch(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot patch dropped segment")
	assert.Equal(t, commonpb.SegmentState_Dropped, mt.segments.GetSegment(segmentID).GetState())
}

func TestApplyExternalCollectionSegmentUpdate_RejectNewSegmentCollectionMismatch(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
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

func TestApplyExternalCollectionSegmentUpdate_RejectNewSegmentEmptyManifest(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, collectionID, 100)
	seg.ManifestPath = ""

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{seg},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty manifest path")
	assert.Nil(t, mt.segments.GetSegment(10))
}

func TestApplyExternalCollectionSegmentUpdate_RejectSegmentIDFromOtherCollection(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{incoming},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection mismatch")
	got := mt.segments.GetSegment(segmentID)
	assert.NotNil(t, got)
	assert.Equal(t, collectionID+1, got.GetCollectionID())
	assert.Equal(t, `{"base_path":"other","ver":1}`, got.GetManifestPath())
}

func TestApplyExternalCollectionSegmentUpdate_RejectInvalidKeptSegment(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		[]int64{999},
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kept segment 999 not found")
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(1).GetState())
}

func TestApplyExternalCollectionSegmentUpdate_RejectKeptSegmentFromOtherCollection(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		[]int64{10},
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection mismatch")
	assert.Equal(t, collectionID+1, mt.segments.GetSegment(10).GetCollectionID())
}

func TestApplyExternalCollectionSegmentUpdate_RejectDroppedKeptSegment(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		[]int64{10},
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot keep dropped segment")
	assert.Equal(t, commonpb.SegmentState_Dropped, mt.segments.GetSegment(10).GetState())
}

func TestApplyExternalCollectionSegmentUpdate_NormalizeNewSegmentCollection(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, 0, 100)

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
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

func TestApplyExternalCollectionSegmentUpdate_RejectPatchBinlogRowCountMismatch(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Equal(t, `{"base_path":"new","ver":1}`, mt.segments.GetSegment(segmentID).GetManifestPath())
}

func TestApplyExternalCollectionSegmentUpdate_RejectNewBinlogRowCountMismatch(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, collectionID, 100)
	seg.Binlogs[0].Binlogs[0].EntriesNum = 99

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{seg},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Nil(t, mt.segments.GetSegment(10))
}

func TestApplyExternalCollectionSegmentUpdate_RejectPatchEmptyNestedBinlogs(t *testing.T) {
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

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
		nil,
		[]*datapb.SegmentInfo{patched},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "binlog row count mismatch")
	assert.Equal(t, `{"base_path":"new","ver":1}`, mt.segments.GetSegment(segmentID).GetManifestPath())
}

func TestApplyExternalCollectionSegmentUpdate_RejectNewEmptyNestedBinlogs(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	mt := &meta{
		collections: newTestCollections(collectionID),
		segments:    NewSegmentsInfo(),
		catalog:     &stubCatalog{},
	}
	seg := newTestExternalRefreshSegment(10, collectionID, 100)
	seg.Binlogs = []*datapb.FieldBinlog{{FieldID: 0}}

	err := applyExternalCollectionSegmentUpdate(
		ctx,
		mt,
		collectionID,
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
			newTestExternalRefreshSegment(999, 100, 3000),
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
			newTestExternalRefreshSegment(999, 100, 5000),
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
			newTestExternalRefreshSegment(1, 100, 1000),
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

	// A missing task after version update is a transient race, so the task is
	// reset to Init for re-dispatch rather than failing.
	// Note: since GetTask is mocked to return nil, we check the in-memory state.
	assert.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
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
