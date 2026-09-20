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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestImportTask_TaskTime(t *testing.T) {
	task := &importTask{
		times: taskcommon.NewTimes(),
	}
	startTime := time.Now()
	endTime := time.Now()
	queueTime := time.Now()
	task.SetTaskTime(taskcommon.TimeStart, startTime)
	task.SetTaskTime(taskcommon.TimeEnd, endTime)
	task.SetTaskTime(taskcommon.TimeQueue, queueTime)

	assert.Equal(t, task.GetTaskTime(taskcommon.TimeStart), startTime)
	assert.Equal(t, task.GetTaskTime(taskcommon.TimeEnd), endTime)
	assert.Equal(t, task.GetTaskTime(taskcommon.TimeQueue), queueTime)
}

func TestImportRetryIsOwnedByBusinessInspector(t *testing.T) {
	task := &importTask{}
	task.task.Store(&datapb.ImportTaskV2{State: datapb.ImportTaskStateV2_Retry, TaskVersion: 3})
	assert.Equal(t, taskcommon.Retry, task.GetTaskState())
	assert.Equal(t, int64(3), task.GetTaskVersion())
}

func TestImportTask_GetTaskType(t *testing.T) {
	task := &importTask{}
	assert.Equal(t, task.GetTaskType(), taskcommon.Import)
}

func TestImportTask_GetNodeID(t *testing.T) {
	taskProto := &datapb.ImportTaskV2{
		NodeID: 1,
	}
	task := &importTask{}
	task.task.Store(taskProto)
	assert.Equal(t, task.GetNodeID(), int64(1))
}

func TestImportTask_CreateTaskOnWorker(t *testing.T) {
	t.Run("stale task removed by terminal GC", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		require.NoError(t, err)

		task := &importTask{importMeta: im, tr: timerecord.NewTimeRecorder("")}
		task.task.Store(&datapb.ImportTaskV2{
			JobID: 1, TaskID: 2, CollectionID: 3,
			State: datapb.ImportTaskStateV2_Pending,
		})

		task.CreateTaskOnWorker(1, session.NewMockCluster(t))

		assert.Equal(t, datapb.ImportTaskStateV2_None, task.GetState())
	})

	t.Run("AssembleImportRequest failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(1000, errors.New("mock err"))

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &importTask{
			alloc:      alloc,
			meta:       &meta{},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
	})

	t.Run("CreateImport rpc failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil)
		alloc.EXPECT().AllocN(mock.Anything).Return(10000, 20000, nil)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &importTask{
			alloc:      alloc,
			meta:       &meta{},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateImport(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))
		cluster.EXPECT().DropImport(int64(1), int64(2)).Return(nil).Once()
		task.CreateTaskOnWorker(1, cluster)
		// A Create error says nothing about whether the worker accepted the
		// request. Record retry debt for the business inspector; it replaces this
		// attempt with fresh task and segment IDs before dispatching again.
		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.EqualValues(t, 1, task.GetNodeID())
	})

	t.Run("UpdateTask failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil)
		alloc.EXPECT().AllocN(mock.Anything).Return(10000, 20000, nil)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &importTask{
			alloc:      alloc,
			meta:       &meta{},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		// CreateImport is deliberately not declared: a task whose assignment
		// cannot be persisted must not be sent at all, or an accepted request
		// would leave an attempt nobody can reclaim.
		cluster := session.NewMockCluster(t)

		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		im.(*importMeta).catalog = catalog

		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		assert.NotEqualValues(t, 1, task.GetNodeID(),
			"a task that was never sent must not name the node it would have gone to")
	})

	t.Run("normal", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID: 1,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil)
		alloc.EXPECT().AllocN(mock.Anything).Return(10000, 20000, nil)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			State:        datapb.ImportTaskStateV2_Pending,
		}
		task := &importTask{
			alloc:      alloc,
			meta:       &meta{},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	})
}

func TestImportTask_QueryTaskOnWorker(t *testing.T) {
	t.Run("worker no longer has the import task", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
		catalog.EXPECT().Update(mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		assert.NoError(t, im.AddJob(context.TODO(), newRetryTestJob()))

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
			TaskVersion:  4,
			FileStats:    newRetryTestFileStats(),
		}
		task := &importTask{
			alloc: newRetryTestAllocator(t, 9000),
			meta: &meta{
				catalog:  catalog,
				segments: NewSegmentsInfo(),
				// GetSegmentMaxSize consults index meta while building
				// replacement segments.
				indexMeta: &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)},
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)
		for _, segmentID := range taskProto.GetSegmentIDs() {
			task.meta.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
				ID: segmentID, CollectionID: task.GetCollectionID(), State: commonpb.SegmentState_Importing,
				IsImporting: true, IsInvisible: true,
			}))
		}

		// The task callback only records retry debt. The business inspector later
		// runs the metadata transaction that removes the old task before the
		// replacement can be dispatched.
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_None,
		}, nil).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(nil).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.Same(t, task, im.GetTask(context.TODO(), task.GetTaskID()))

		replacement, err := replaceImportTaskForRetry(context.TODO(), task,
			im.GetJob(context.TODO(), task.GetJobID()), task.alloc, task.meta, im)
		require.NoError(t, err)
		require.NotNil(t, replacement)
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		assert.Nil(t, im.GetTask(context.TODO(), task.GetTaskID()))
		tasks := im.GetTaskByJob(context.TODO(), task.GetJobID())
		if assert.Len(t, tasks, 1) {
			replacementMeta := tasks[0].(*importTask)
			assert.Equal(t, replacement.GetTaskID(), replacementMeta.GetTaskID())
			assert.NotEqual(t, task.GetTaskID(), replacementMeta.GetTaskID())
			assert.Equal(t, datapb.ImportTaskStateV2_Pending, replacementMeta.GetState())
			assert.Equal(t, int64(5), replacementMeta.GetTaskVersion())
			assert.NotSubset(t, []int64{5, 6}, replacementMeta.GetSegmentIDs())
			for _, segmentID := range replacementMeta.GetSegmentIDs() {
				assert.True(t, task.meta.segments.GetSegment(segmentID).GetIsInvisible(),
					"retry must preserve the persisted sort plan")
			}
		}
	})

	// A retry no longer reuses the abandoned attempt's segments, so there is no
	// stale row count to reset. The old segments are dropped and the replacement
	// gets a fresh set.
	t.Run("QueryImport rpc failed retires the old segments", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		assert.NoError(t, im.AddJob(context.TODO(), newRetryTestJob()))
		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
			TaskVersion:  4,
			FileStats:    newRetryTestFileStats(),
		}
		catalog.EXPECT().Update(mock.Anything,
			mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		task := &importTask{
			alloc: newRetryTestAllocator(t, 9100),
			meta: &meta{
				catalog:   catalog,
				segments:  NewSegmentsInfo(),
				indexMeta: &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)},
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(5, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           5,
				CollectionID: 3,
				State:        commonpb.SegmentState_Importing,
				IsImporting:  true,
				NumOfRows:    100,
				MaxRowNum:    100,
			},
		})
		task.meta.segments.SetSegment(6, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           6,
				CollectionID: 3,
				State:        commonpb.SegmentState_Importing,
				IsImporting:  true,
				NumOfRows:    50,
				MaxRowNum:    50,
			},
		})

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(7))
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(merr.WrapErrNodeNotFound(7)).Once()
		task.QueryTaskOnWorker(cluster)

		assert.Equal(t, datapb.ImportTaskStateV2_Retry, task.GetState())
		assert.Equal(t, commonpb.SegmentState_Importing, task.meta.segments.GetSegment(5).GetState())
		replacement, err := replaceImportTaskForRetry(context.TODO(), task,
			im.GetJob(context.TODO(), task.GetJobID()), task.alloc, task.meta, im)
		require.NoError(t, err)
		require.NotNil(t, replacement)
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		assert.Nil(t, im.GetTask(context.TODO(), task.GetTaskID()))
		tasks := im.GetTaskByJob(context.TODO(), task.GetJobID())
		require.Len(t, tasks, 1)
		replacementMeta := tasks[0].(*importTask)
		assert.Equal(t, replacement.GetTaskID(), replacementMeta.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, replacementMeta.GetState())
		assert.Equal(t, int64(5), replacementMeta.GetTaskVersion())
		// The old segments are retired, not reused -- so whatever rows the
		// abandoned attempt wrote into them go with them.
		assert.Equal(t, commonpb.SegmentState_Dropped, task.meta.segments.GetSegment(5).GetState())
		assert.Equal(t, commonpb.SegmentState_Dropped, task.meta.segments.GetSegment(6).GetState())
		assert.False(t, task.meta.segments.GetSegment(5).GetIsImporting())
		assert.False(t, task.meta.segments.GetSegment(6).GetIsImporting())
		assert.NotSubset(t, []int64{5, 6}, replacementMeta.GetSegmentIDs())
		assert.NotEmpty(t, replacementMeta.GetSegmentIDs(), "the retry has a fresh set to write into")
		for _, segmentID := range replacementMeta.GetSegmentIDs() {
			assert.False(t, task.meta.segments.GetSegment(segmentID).GetIsInvisible(),
				"retry must preserve the persisted no-sort plan")
		}
	})

	t.Run("late result for replaced task is discarded", func(t *testing.T) {
		im := &importMeta{jobs: make(map[int64]ImportJob), tasks: newImportTasks()}
		task := &importTask{ctx: context.Background(), importMeta: im}
		task.task.Store(&datapb.ImportTaskV2{
			JobID: 1, TaskID: 2, CollectionID: 3, NodeID: 7,
			State: datapb.ImportTaskStateV2_InProgress,
		})

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(int64(7), mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_Completed,
		}, nil).Once()

		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		assert.Nil(t, im.GetTask(context.Background(), task.GetTaskID()))
	})

	t.Run("import failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        1,
				CollectionID: 3,
				State:        internalpb.ImportJobState_Importing,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &importTask{
			alloc: nil,
			meta: &meta{
				segments: NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_Failed,
		}, nil)
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		job = im.GetJob(context.TODO(), 1)
		assert.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
	})

	t.Run("persist job failure before task failure", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)

		var (
			writeOrder       []string
			failTaskWrite    = true
			durableTaskState datapb.ImportTaskStateV2
		)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, job *datapb.ImportJob) error {
				if job.GetState() == internalpb.ImportJobState_Failed {
					writeOrder = append(writeOrder, "job")
				}
				return nil
			})
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, task *datapb.ImportTaskV2) error {
				if task.GetState() == datapb.ImportTaskStateV2_Failed {
					writeOrder = append(writeOrder, "task")
					if failTaskWrite {
						failTaskWrite = false
						return errors.New("mock task write failure")
					}
				}
				durableTaskState = task.GetState()
				return nil
			})

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		job := &importJob{ImportJob: &datapb.ImportJob{
			JobID: 1,
			State: internalpb.ImportJobState_Importing,
		}}
		assert.NoError(t, im.AddJob(context.TODO(), job))

		task := &importTask{
			ctx:        context.TODO(),
			meta:       &meta{},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(&datapb.ImportTaskV2{
			JobID: 1, TaskID: 2, CollectionID: 3, NodeID: 7,
			State: datapb.ImportTaskStateV2_InProgress,
		})
		assert.NoError(t, im.AddTask(context.TODO(), task))

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State:  datapb.ImportTaskStateV2_Failed,
			Reason: "bad input",
		}, nil)
		task.QueryTaskOnWorker(cluster)

		assert.Equal(t, []string{"job", "task"}, writeOrder)
		assert.Equal(t, internalpb.ImportJobState_Failed, im.GetJob(context.TODO(), 1).GetState())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, durableTaskState)

		// The durable Failed job owns convergence after the second write fails.
		checker := &importChecker{ctx: context.TODO(), importMeta: im}
		checker.tryFailingTasks(im.GetJob(context.TODO(), 1))
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, durableTaskState)
	})

	t.Run("normal, task in-progress", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
		require.NoError(t, im.AddJob(context.TODO(), &importJob{ImportJob: &datapb.ImportJob{
			JobID: 1, CollectionID: 3, State: internalpb.ImportJobState_Importing,
		}}))

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &importTask{
			alloc: nil,
			meta: &meta{
				segments: NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(5, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 5, CollectionID: 3, State: commonpb.SegmentState_Importing,
				IsImporting: true, NumOfRows: 100,
			},
		})
		task.meta.segments.SetSegment(6, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 6, CollectionID: 3, State: commonpb.SegmentState_Importing,
				IsImporting: true, NumOfRows: 100,
			},
		})
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_InProgress,
			ImportSegmentsInfo: []*datapb.ImportSegmentInfo{
				{
					SegmentID:    5,
					ImportedRows: 100, // imported rows not changed, no need to update
				},
				{
					SegmentID:    6,
					ImportedRows: 200,
				},
			},
		}, nil)

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.meta.catalog = catalog

		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
		assert.Equal(t, int64(100), task.meta.segments.GetSegment(5).GetNumOfRows())
		assert.Equal(t, int64(200), task.meta.segments.GetSegment(6).GetNumOfRows())
	})

	t.Run("normal, task completed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        1,
				CollectionID: 3,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6, 7},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &importTask{
			alloc: nil,
			meta: &meta{
				segments: NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(5, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 5, CollectionID: 3, State: commonpb.SegmentState_Importing,
				IsImporting: true, NumOfRows: 100,
			},
		})
		task.meta.segments.SetSegment(6, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 6, CollectionID: 3, State: commonpb.SegmentState_Importing,
				IsImporting: true, NumOfRows: 100,
			},
		})
		task.meta.segments.SetSegment(7, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 7, CollectionID: 3, State: commonpb.SegmentState_Importing, IsImporting: true,
		}})

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_Completed,
			ImportSegmentsInfo: []*datapb.ImportSegmentInfo{
				{
					SegmentID:    5,
					ImportedRows: 100, // imported rows not changed, no need to update
				},
				{
					SegmentID:    6,
					ImportedRows: 200,
					Stats:        &datapb.Statistics{InsertBinlogSize: 7777, StatsBinlogSize: 333},
				},
			},
		}, nil)

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.meta.catalog = catalog

		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
		assert.Equal(t, int64(100), task.meta.segments.GetSegment(5).GetNumOfRows())
		assert.Equal(t, int64(200), task.meta.segments.GetSegment(6).GetNumOfRows())
		seg6Stats := task.meta.segments.GetSegment(6).GetStats()
		assert.EqualValues(t, 7777, seg6Stats.GetInsertBinlogSize())
		assert.EqualValues(t, 333, seg6Stats.GetStatsBinlogSize())
		assert.Equal(t, commonpb.SegmentState_Dropped, task.meta.segments.GetSegment(7).GetState())
		assert.True(t, task.meta.segments.GetSegment(7).GetIsImporting(),
			"the existing importing flag remains the durable zero-output marker until job cleanup/commit")
	})

	// Replaces the removed TestExtractTimestampFromBinlogs coverage. The
	// helper extractTimestampFromBinlogs was inlined into the Completed
	// branch and now reuses storage.BuildStatsFromFieldBinlogs. Verify
	// end-to-end that:
	//   non-L0 import → StartPosition/DmlPosition derived from Binlogs
	//   L0 import     → StartPosition/DmlPosition derived from Deltalogs
	t.Run("completed non-L0 plumbs binlog timestamps into positions", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        100,
				CollectionID: 3,
				// No L0Import option → non-L0 path.
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.ImportTaskV2{
			JobID:        100,
			TaskID:       101,
			CollectionID: 3,
			SegmentIDs:   []int64{42},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &importTask{
			alloc: nil,
			meta: &meta{
				segments: NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(42, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 42, CollectionID: 3, State: commonpb.SegmentState_Importing,
				IsImporting: true, NumOfRows: 0,
			},
		})

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_Completed,
			ImportSegmentsInfo: []*datapb.ImportSegmentInfo{
				{
					SegmentID:    42,
					ImportedRows: 10,
					Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{LogID: 1, TimestampFrom: 1000, TimestampTo: 2000},
								{LogID: 2, TimestampFrom: 500, TimestampTo: 3000},
							},
						},
					},
				},
			},
		}, nil)

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.meta.catalog = catalog

		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())

		seg := task.meta.segments.GetSegment(42)
		require.NotNil(t, seg)
		// min(TimestampFrom) across binlogs → StartPosition.Timestamp.
		assert.EqualValues(t, 500, seg.GetStartPosition().GetTimestamp())
		// max(TimestampTo) across binlogs → DmlPosition.Timestamp.
		assert.EqualValues(t, 3000, seg.GetDmlPosition().GetTimestamp())
	})

	t.Run("completed L0 plumbs deltalog timestamps into positions", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        200,
				CollectionID: 3,
				Options: []*commonpb.KeyValuePair{
					{Key: importutilv2.L0Import, Value: "true"},
				},
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)

		taskProto := &datapb.ImportTaskV2{
			JobID:        200,
			TaskID:       201,
			CollectionID: 3,
			SegmentIDs:   []int64{43},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &importTask{
			alloc: nil,
			meta: &meta{
				segments: NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(43, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: 43, CollectionID: 3, State: commonpb.SegmentState_Importing,
				IsImporting: true, NumOfRows: 0, Level: datapb.SegmentLevel_L0,
			},
		})

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
			State: datapb.ImportTaskStateV2_Completed,
			ImportSegmentsInfo: []*datapb.ImportSegmentInfo{
				{
					SegmentID:    43,
					ImportedRows: 10,
					// Binlogs intentionally carry an outlier range that
					// should be ignored because IsL0Import == true.
					Binlogs: []*datapb.FieldBinlog{
						{
							FieldID: 100,
							Binlogs: []*datapb.Binlog{
								{LogID: 10, TimestampFrom: 99999, TimestampTo: 99999},
							},
						},
					},
					Deltalogs: []*datapb.FieldBinlog{
						{
							Binlogs: []*datapb.Binlog{
								{LogID: 1, EntriesNum: 5, TimestampFrom: 7000, TimestampTo: 8000},
								{LogID: 2, EntriesNum: 3, TimestampFrom: 5500, TimestampTo: 9500},
							},
						},
					},
				},
			},
		}, nil)

		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.meta.catalog = catalog

		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())

		seg := task.meta.segments.GetSegment(43)
		require.NotNil(t, seg)
		// L0: positions derived from deltalogs, NOT binlogs.
		assert.EqualValues(t, 5500, seg.GetStartPosition().GetTimestamp())
		assert.EqualValues(t, 9500, seg.GetDmlPosition().GetTimestamp())
	})
}

func TestImportTask_CompletedResultValidation(t *testing.T) {
	newValidationTask := func(t *testing.T, segmentIDs []int64, segments ...*datapb.SegmentInfo) *importTask {
		importMeta := NewMockImportMeta(t)
		importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(&importJob{ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 3,
			PartitionIDs: []int64{10},
			Vchannels:    []string{"ch"},
		}}).Once()
		segmentMeta := &meta{
			segments: NewSegmentsInfo(),
		}
		for _, segment := range segments {
			segmentMeta.segments.SetSegment(segment.GetID(), NewSegmentInfo(segment))
		}
		task := &importTask{meta: segmentMeta, importMeta: importMeta}
		task.task.Store(&datapb.ImportTaskV2{
			JobID: 1, TaskID: 2, CollectionID: 3, SegmentIDs: segmentIDs,
		})
		return task
	}

	newSegment := func(id, rows int64) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID: id, CollectionID: 3, PartitionID: 10, InsertChannel: "ch",
			State: commonpb.SegmentState_Importing, IsImporting: true, NumOfRows: rows,
		}
	}

	t.Run("duplicate", func(t *testing.T) {
		task := newValidationTask(t, []int64{10}, newSegment(10, 0))
		_, err := task.validateImportResponseSegments(context.Background(), []*datapb.ImportSegmentInfo{
			{SegmentID: 10}, {SegmentID: 10},
		}, true)
		require.ErrorContains(t, err, "returned segment 10 more than once")
	})

	t.Run("missing non-empty segment", func(t *testing.T) {
		task := newValidationTask(t, []int64{10}, newSegment(10, 1))
		_, err := task.validateImportResponseSegments(context.Background(), nil, true)
		require.ErrorContains(t, err, "omitted non-empty segment 10")
	})

	t.Run("missing zero-output segment", func(t *testing.T) {
		task := newValidationTask(t, []int64{10}, newSegment(10, 0))
		missing, err := task.validateImportResponseSegments(context.Background(), nil, true)
		require.NoError(t, err)
		require.Equal(t, []int64{10}, missing)
	})

	t.Run("partially persisted completed response can replay", func(t *testing.T) {
		flushed := newSegment(10, 1)
		flushed.State = commonpb.SegmentState_Flushed
		dropped := newSegment(11, 0)
		dropped.State = commonpb.SegmentState_Dropped
		task := newValidationTask(t, []int64{10, 11}, flushed, dropped)

		missing, err := task.validateImportResponseSegments(context.Background(), []*datapb.ImportSegmentInfo{
			{SegmentID: 10, ImportedRows: 1},
		}, true)
		require.NoError(t, err)
		require.Empty(t, missing)
	})

	t.Run("unexpected segment cannot be mutated", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

		importMeta, err := NewImportMeta(context.Background(), catalog, nil, nil)
		require.NoError(t, err)
		require.NoError(t, importMeta.AddJob(context.Background(), &importJob{ImportJob: &datapb.ImportJob{
			JobID: 1, CollectionID: 3, State: internalpb.ImportJobState_Importing,
		}}))
		segmentMeta := &meta{
			segments: NewSegmentsInfo(),
		}
		segmentMeta.segments.SetSegment(10, NewSegmentInfo(&datapb.SegmentInfo{
			ID: 10, CollectionID: 3, State: commonpb.SegmentState_Importing, IsImporting: true,
		}))
		segmentMeta.segments.SetSegment(99, NewSegmentInfo(&datapb.SegmentInfo{
			ID: 99, CollectionID: 9, State: commonpb.SegmentState_Flushed, NumOfRows: 7,
		}))
		task := &importTask{meta: segmentMeta, importMeta: importMeta, tr: timerecord.NewTimeRecorder("")}
		task.task.Store(&datapb.ImportTaskV2{
			JobID: 1, TaskID: 2, CollectionID: 3, SegmentIDs: []int64{10},
			NodeID: 7, State: datapb.ImportTaskStateV2_InProgress,
		})
		require.NoError(t, importMeta.AddTask(context.Background(), task))

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(int64(7), mock.Anything).Return(&datapb.QueryImportResponse{
			State:              datapb.ImportTaskStateV2_Completed,
			ImportSegmentsInfo: []*datapb.ImportSegmentInfo{{SegmentID: 99, ImportedRows: 100}},
		}, nil).Once()
		task.QueryTaskOnWorker(cluster)

		require.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())
		require.Equal(t, internalpb.ImportJobState_Failed, importMeta.GetJob(context.Background(), 1).GetState())
		require.EqualValues(t, 7, segmentMeta.GetSegment(context.Background(), 99).GetNumOfRows())
		require.Equal(t, commonpb.SegmentState_Flushed, segmentMeta.GetSegment(context.Background(), 99).GetState())
	})
}

func TestImportTask_DropTaskOnWorker(t *testing.T) {
	t.Run("DropImport rpc failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Completed,
		}
		task := &importTask{
			alloc:      nil,
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		task.DropTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
		assert.Equal(t, int64(7), task.GetNodeID())
	})

	t.Run("normal", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Completed,
		}
		task := &importTask{
			alloc:      nil,
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
		task.DropTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, task.GetState())
		assert.Equal(t, int64(NullNodeID), task.GetNodeID())
	})
}

// --- helpers for the fresh-identity retry path ---

// newRetryTestJob is the minimum job a retry needs to re-plan its output
// segments: replaceImportTaskForRetry reads the schema (for the primary field)
// and the job options.
func newRetryTestJob() ImportJob {
	return &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 3,
			State:        internalpb.ImportJobState_Importing,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			},
		},
	}
}

// newRetryTestFileStats gives assignSegments a hashed size to place, so the
// retry actually allocates segments instead of returning an empty set.
func newRetryTestFileStats() []*datapb.ImportFileStats {
	return []*datapb.ImportFileStats{{
		HashedStats: map[string]*datapb.PartitionImportStats{
			"ch-0": {PartitionDataSize: map[int64]int64{10: 1}},
		},
	}}
}

func newRetryTestAllocator(t *testing.T, next int64) allocator.Allocator {
	alloc := allocator.NewMockAllocator(t)
	counter := next
	alloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(context.Context) (int64, error) {
		counter++
		return counter, nil
	}).Maybe()
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		begin := counter + 1
		counter += n
		return begin, counter + 1, nil
	}).Maybe()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil).Maybe()
	return alloc
}
