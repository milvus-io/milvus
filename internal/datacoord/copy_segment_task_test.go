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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CopySegmentTaskSuite struct {
	suite.Suite
}

func TestCopySegmentTask(t *testing.T) {
	suite.Run(t, new(CopySegmentTaskSuite))
}

func (s *CopySegmentTaskSuite) TestCopySegmentTask_GettersAndSetters() {
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}

	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 2,
		NodeId:       5,
		TaskVersion:  1,
		TaskSlot:     1,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		Reason:       "test reason",
		IdMappings:   idMappings,
		CreatedTs:    12345,
		CompleteTs:   54321,
	})

	// Test all getters
	s.Equal(int64(1001), task.GetTaskId())
	s.Equal(int64(100), task.GetJobId())
	s.Equal(int64(2), task.GetCollectionId())
	s.Equal(int64(5), task.GetNodeId())
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
	s.Equal("test reason", task.GetReason())
	s.Equal(idMappings, task.GetIdMappings())
	s.NotNil(task.GetTR())

	// Test task.Task interface methods
	s.Equal(int64(1001), task.GetTaskID())
	s.Equal(taskcommon.CopySegment, task.GetTaskType())
	s.Equal(taskcommon.FromCopySegmentState(datapb.CopySegmentTaskState_CopySegmentTaskPending), task.GetTaskState())
	s.Equal(int64(1), task.GetTaskSlot())
	s.Equal(int64(1), task.GetTaskVersion())
}

func (s *CopySegmentTaskSuite) TestCopySegmentTask_Clone() {
	original := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}

	original.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 2,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		Reason:       "original reason",
	})

	// Clone the task
	cloned := original.Clone()

	// Verify cloned task has same values
	s.Equal(original.GetTaskId(), cloned.GetTaskId())
	s.Equal(original.GetJobId(), cloned.GetJobId())
	s.Equal(original.GetCollectionId(), cloned.GetCollectionId())
	s.Equal(original.GetState(), cloned.GetState())
	s.Equal(original.GetReason(), cloned.GetReason())

	// Verify time recorder is same reference
	s.Equal(original.GetTR(), cloned.GetTR())

	// Verify they share the same task pointer (not deep copy)
	clonedImpl := cloned.(*copySegmentTask)
	s.Equal(original.task.Load(), clonedImpl.task.Load())
}

func (s *CopySegmentTaskSuite) TestWithCopyTaskJob() {
	task1 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task1"),
		times: taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		JobId:  100,
	})

	task2 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task2"),
		times: taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		JobId:  200,
	})

	filter := WithCopyTaskJob(100)

	s.True(filter(task1))
	s.False(filter(task2))
}

func (s *CopySegmentTaskSuite) TestWithCopyTaskStates() {
	pendingTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("pending"),
		times: taskcommon.NewTimes(),
	}
	pendingTask.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	inProgressTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("inprogress"),
		times: taskcommon.NewTimes(),
	}
	inProgressTask.task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})

	completedTask := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("completed"),
		times: taskcommon.NewTimes(),
	}
	completedTask.task.Store(&datapb.CopySegmentTask{
		TaskId: 1003,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})

	// Test filter with single state
	filter1 := WithCopyTaskStates(datapb.CopySegmentTaskState_CopySegmentTaskPending)
	s.True(filter1(pendingTask))
	s.False(filter1(inProgressTask))
	s.False(filter1(completedTask))

	// Test filter with multiple states
	filter2 := WithCopyTaskStates(
		datapb.CopySegmentTaskState_CopySegmentTaskPending,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	)
	s.True(filter2(pendingTask))
	s.True(filter2(inProgressTask))
	s.False(filter2(completedTask))
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskState() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	// Apply update action
	action := UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress)
	action(task)

	// Verify state is updated
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskReason() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		Reason: "",
	})

	// Apply update action
	action := UpdateCopyTaskReason("task failed")
	action(task)

	// Verify reason is updated
	s.Equal("task failed", task.GetReason())
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskNodeID() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		NodeId: 0,
	})

	// Apply update action
	action := UpdateCopyTaskNodeID(5)
	action(task)

	// Verify node ID is updated
	s.Equal(int64(5), task.GetNodeId())
}

func (s *CopySegmentTaskSuite) TestUpdateCopyTaskCompleteTs() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		CompleteTs: 0,
	})

	now := uint64(time.Now().UnixNano())

	// Apply update action
	action := UpdateCopyTaskCompleteTs(now)
	action(task)

	// Verify complete timestamp is updated
	s.Equal(now, task.task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestMultipleUpdateActions() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		State:      datapb.CopySegmentTaskState_CopySegmentTaskPending,
		NodeId:     0,
		Reason:     "",
		CompleteTs: 0,
	})

	// Apply multiple update actions
	actions := []UpdateCopySegmentTaskAction{
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted),
		UpdateCopyTaskNodeID(5),
		UpdateCopyTaskReason("completed successfully"),
		UpdateCopyTaskCompleteTs(12345),
	}

	for _, action := range actions {
		action(task)
	}

	// Verify all updates are applied
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
	s.Equal(int64(5), task.GetNodeId())
	s.Equal("completed successfully", task.GetReason())
	s.Equal(uint64(12345), task.task.Load().GetCompleteTs())
}

func (s *CopySegmentTaskSuite) TestWrapCopySegmentTaskLog() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 2,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	// Test without extra fields
	fields := WrapCopySegmentTaskLog(task)
	s.Len(fields, 4)

	// Verify field names
	fieldNames := make(map[string]bool)
	for _, field := range fields {
		fieldNames[field.Key] = true
	}
	s.True(fieldNames["taskID"])
	s.True(fieldNames["jobID"])
	s.True(fieldNames["collectionID"])
	s.True(fieldNames["state"])

	// Test with extra fields
	extraFields := []zap.Field{
		zap.String("extra", "value"),
		zap.Int64("number", 123),
	}
	fieldsWithExtra := WrapCopySegmentTaskLog(task, extraFields...)
	s.Len(fieldsWithExtra, 6)
}

func (s *CopySegmentTaskSuite) TestCombinedFilters() {
	tasks := []CopySegmentTask{
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task1"),
			times: taskcommon.NewTimes(),
		},
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task2"),
			times: taskcommon.NewTimes(),
		},
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task3"),
			times: taskcommon.NewTimes(),
		},
		&copySegmentTask{
			tr:    timerecord.NewTimeRecorder("task4"),
			times: taskcommon.NewTimes(),
		},
	}

	tasks[0].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[1].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1002,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	tasks[2].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1003,
		JobId:  200,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	tasks[3].(*copySegmentTask).task.Store(&datapb.CopySegmentTask{
		TaskId: 1004,
		JobId:  100,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})

	// Filter: jobID = 100 AND state = Pending or InProgress
	jobFilter := WithCopyTaskJob(100)
	stateFilter := WithCopyTaskStates(
		datapb.CopySegmentTaskState_CopySegmentTaskPending,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	)

	var filtered []CopySegmentTask
	for _, task := range tasks {
		if jobFilter(task) && stateFilter(task) {
			filtered = append(filtered, task)
		}
	}

	// Should match tasks 1001 and 1002
	s.Len(filtered, 2)
	s.Equal(int64(1001), filtered[0].GetTaskId())
	s.Equal(int64(1002), filtered[1].GetTaskId())
}

func (s *CopySegmentTaskSuite) TestTaskTimeOperations() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
	})

	// Set and get task time
	now := time.Now()
	task.SetTaskTime(taskcommon.TimeStart, now)

	retrievedTime := task.GetTaskTime(taskcommon.TimeStart)
	s.True(retrievedTime.Equal(now))
}

func (s *CopySegmentTaskSuite) TestTaskWithEmptyIdMappings() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:     1001,
		IdMappings: nil,
	})

	// Should return nil, not panic
	mappings := task.GetIdMappings()
	s.Nil(mappings)
}

func (s *CopySegmentTaskSuite) TestStateConversion() {
	testCases := []struct {
		copySegmentState datapb.CopySegmentTaskState
		expectedState    taskcommon.State
	}{
		{datapb.CopySegmentTaskState_CopySegmentTaskPending, taskcommon.Init},
		{datapb.CopySegmentTaskState_CopySegmentTaskInProgress, taskcommon.InProgress},
		{datapb.CopySegmentTaskState_CopySegmentTaskCompleted, taskcommon.Finished},
		{datapb.CopySegmentTaskState_CopySegmentTaskFailed, taskcommon.Failed},
	}

	for _, tc := range testCases {
		task := &copySegmentTask{
			tr:    timerecord.NewTimeRecorder("test task"),
			times: taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId: 1001,
			State:  tc.copySegmentState,
		})

		s.Equal(tc.expectedState, task.GetTaskState(),
			"State conversion failed for %v", tc.copySegmentState)
	}
}

func (s *CopySegmentTaskSuite) TestTaskType() {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test task"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 1001,
	})

	s.Equal(taskcommon.CopySegment, task.GetTaskType())
}

// createTestIndexMeta creates an indexMeta with pre-registered index definitions for testing.
// If catalog is nil, a default mock catalog (CreateSegmentIndex returns nil) is used.
func createTestIndexMeta(t *testing.T, collectionID int64, indexes map[int64]*model.Index, catalog ...metastore.DataCoordCatalog) *indexMeta {
	var cat metastore.DataCoordCatalog
	if len(catalog) > 0 && catalog[0] != nil {
		cat = catalog[0]
	} else {
		mockCat := catalogmocks.NewDataCoordCatalog(t)
		mockCat.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
		cat = mockCat
	}

	im := &indexMeta{
		ctx:              context.Background(),
		catalog:          cat,
		keyLock:          lock.NewKeyLock[UniqueID](),
		indexes:          map[UniqueID]map[UniqueID]*model.Index{collectionID: indexes},
		segmentBuildInfo: newSegmentIndexBuildInfo(),
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
	}
	return im
}

// createTestCopyTask creates a minimal CopySegmentTask for testing syncVectorScalarIndexes.
func createTestCopyTask(collectionID int64, segmentID int64) CopySegmentTask {
	task := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("test"),
		times: taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: segmentID, PartitionId: 10},
		},
	})
	return task
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_EmptyResult() {
	result := &datapb.CopySegmentResult{
		SegmentId:  100,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{},
	}
	task := createTestCopyTask(1, 100)
	err := syncVectorScalarIndexes(context.Background(), result, task, &meta{}, nil)
	s.NoError(err)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_SingleIndex() {
	collectionID := int64(1)
	segmentID := int64(100)

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im}

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {
				FieldId:        101,
				IndexId:        200, // source indexID
				BuildId:        5001,
				IndexName:      "vec_idx",
				IndexFilePaths: []string{"HNSW"},
				IndexSize:      10000,
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	// Verify the segment index was added with target indexID and Finished state
	segIdx, ok := im.segmentBuildInfo.Get(5001)
	s.True(ok)
	s.Equal(int64(300), segIdx.IndexID) // target indexID, not source 200
	s.Equal(commonpb.IndexState_Finished, segIdx.IndexState)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_MultipleIndexesPerField() {
	collectionID := int64(1)
	segmentID := int64(100)

	// Two JSON path indexes on the same field (fieldID=101)
	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "idx_category"},
		301: {CollectionID: collectionID, FieldID: 101, IndexID: 301, IndexName: "idx_price"},
		302: {CollectionID: collectionID, FieldID: 102, IndexID: 302, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im}

	// Result keyed by buildID (not fieldID), with IndexName for matching
	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {
				FieldId:        101,
				IndexId:        200, // source indexID
				BuildId:        5001,
				IndexName:      "idx_category",
				IndexFilePaths: []string{"inverted1"},
				IndexSize:      3000,
			},
			5002: {
				FieldId:        101, // same field!
				IndexId:        201, // different source indexID
				BuildId:        5002,
				IndexName:      "idx_price",
				IndexFilePaths: []string{"inverted2"},
				IndexSize:      4000,
			},
			5003: {
				FieldId:        102,
				IndexId:        202,
				BuildId:        5003,
				IndexName:      "vec_idx",
				IndexFilePaths: []string{"HNSW"},
				IndexSize:      10000,
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	// All three indexes should be synced with correct target indexIDs
	for buildID, expectedIndexID := range map[int64]int64{5001: 300, 5002: 301, 5003: 302} {
		segIdx, ok := im.segmentBuildInfo.Get(buildID)
		s.True(ok, "buildID %d should exist", buildID)
		s.Equal(expectedIndexID, segIdx.IndexID, "buildID %d should map to indexID %d", buildID, expectedIndexID)
		s.Equal(commonpb.IndexState_Finished, segIdx.IndexState)
	}
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_IndexNameNotFound() {
	collectionID := int64(1)
	segmentID := int64(100)

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes)
	m := &meta{indexMeta: im}

	// Source has an index name that doesn't exist in target -> should skip
	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {
				FieldId:   101,
				BuildId:   5001,
				IndexName: "nonexistent_idx",
			},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	err := syncVectorScalarIndexes(context.Background(), result, task, m, nil)
	s.NoError(err)

	// Should not be added
	_, ok := im.segmentBuildInfo.Get(5001)
	s.False(ok)
}

func (s *CopySegmentTaskSuite) TestSyncVectorScalarIndexes_AddSegmentIndexError() {
	collectionID := int64(1)
	segmentID := int64(100)

	errCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	errCatalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).
		Return(errors.New("catalog error"))

	indexes := map[int64]*model.Index{
		300: {CollectionID: collectionID, FieldID: 101, IndexID: 300, IndexName: "vec_idx"},
	}
	im := createTestIndexMeta(s.T(), collectionID, indexes, errCatalog)
	m := &meta{indexMeta: im}

	result := &datapb.CopySegmentResult{
		SegmentId:    segmentID,
		ImportedRows: 1000,
		IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
			5001: {FieldId: 101, BuildId: 5001, IndexName: "vec_idx", IndexFilePaths: []string{"HNSW"}},
		},
	}
	task := createTestCopyTask(collectionID, segmentID)

	// Mock copyMeta for error path (UpdateTask and UpdateJobStateAndReleaseRef)
	copyCatalog := catalogmocks.NewDataCoordCatalog(s.T())
	copyCatalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
	copyCatalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyCatalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	copyMeta, cmErr := NewCopySegmentMeta(context.TODO(), copyCatalog, nil, nil, nil)
	s.NoError(cmErr)

	syncErr := syncVectorScalarIndexes(context.Background(), result, task, m, copyMeta)
	s.Error(syncErr)
	s.Contains(syncErr.Error(), "catalog error")
}

func TestAssembleCopySegmentRequest_SourceSegmentNotFound(t *testing.T) {
	// Arrange: snapshot data has segment 1, but task maps from segment 999 which doesn't exist
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	// Mock ReadSnapshotData to return our test data
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	task := &copySegmentTask{
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 999, TargetSegmentId: 2001, PartitionId: 10}, // 999 not in snapshot
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr: timerecord.NewTimeRecorder("test_job"),
	}

	// Act
	req, err := AssembleCopySegmentRequest(task, job)

	// Assert
	assert.Nil(t, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source segment 999 not found")
}

func TestAssembleCopySegmentRequest_AllocatesTextAndJsonBuildIDs(t *testing.T) {
	// Arrange: snapshot data with segment that has vector, text, and JSON key indexes
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					200: {FieldID: 200, BuildID: 4001},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					300: {FieldID: 300, BuildID: 5001},
				},
			},
		},
	}

	sm := &snapshotMeta{}
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	// Mock allocator to return sequential IDs starting from 9001
	nextID := int64(9001)
	alloc := &struct{ allocator.Allocator }{}
	mock2 := mockey.Mock((*struct{ allocator.Allocator }).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mock2.UnPatch()

	// Mock Params access
	mock3 := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mock3.UnPatch()

	task := &copySegmentTask{
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr: timerecord.NewTimeRecorder("test_job"),
	}

	// Act
	req, err := AssembleCopySegmentRequest(task, job)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Len(t, req.Targets, 1)

	newBuildIDs := req.Targets[0].GetNewBuildIds()
	// Should have 3 entries: one for vector index (3001), one for text (4001), one for JSON (5001)
	assert.Len(t, newBuildIDs, 3)
	assert.Contains(t, newBuildIDs, int64(3001))
	assert.Contains(t, newBuildIDs, int64(4001))
	assert.Contains(t, newBuildIDs, int64(5001))

	// All new IDs should be distinct and >= 9001
	seenIDs := make(map[int64]bool)
	for _, newID := range newBuildIDs {
		assert.True(t, newID >= 9001, "new build ID should be allocated by allocator")
		assert.False(t, seenIDs[newID], "new build IDs should be unique")
		seenIDs[newID] = true
	}
}
