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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
			meta:       &meta{collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()},
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
			meta:       &meta{collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateImport(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err"))
		task.CreateTaskOnWorker(1, cluster)
		// A Create error says nothing about whether the worker accepted the
		// request, so the durable assignment stays: the poll path reclaims the
		// attempt from that node before the task becomes dispatchable again.
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
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
			meta:       &meta{collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()},
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
			meta:       &meta{collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()},
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
	t.Run("QueryImport rpc failed", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)
		assert.NoError(t, im.AddJob(context.TODO(), newRotationTestJob()))

		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
			FileStats:    newRotationTestFileStats(),
		}
		task := &importTask{
			alloc: newRotationTestAllocator(t, 9000),
			meta: &meta{
				catalog:     catalog,
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
				// GetSegmentMaxSize consults index meta (DISKANN sizing) while
				// the rotation re-plans the replacement segments.
				indexMeta: &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)},
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		// A reclaimed attempt is not re-dispatched onto its own output: the drop
		// cancels the worker but does not wait for in-flight writes, so the task
		// gets a fresh set of output segments before it becomes dispatchable.
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(nil, errors.New("mock err")).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(nil).Once()
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		assert.EqualValues(t, NullNodeID, task.GetNodeID(),
			"a reclaimed attempt releases its assignment")
		assert.NotSubset(t, []int64{5, 6}, task.GetSegmentIDs(),
			"the retry must not inherit the abandoned attempt's output segments")

		// The reset is best effort: when persisting it fails the in-memory state
		// is left alone and the next round tries again.
		taskProto.State = datapb.ImportTaskStateV2_InProgress
		taskProto.NodeID = 7
		task.task.Store(taskProto)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).
			Return(nil, merr.WrapErrNodeNotFound(7)).Once()
		cluster.EXPECT().DropImport(int64(7), int64(2)).Return(nil).Once()
		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		im.(*importMeta).catalog = catalog
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	})

	// Superseded by segment rotation: a retry no longer reuses the abandoned
	// attempt's segments, so there is no stale row count to reset -- the old
	// segments are retired outright and the task gets a fresh set. This pins
	// that, plus the retirement, in place of the old row-reset assertion.
	t.Run("QueryImport rpc failed retires the old segments", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

		im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
		assert.NoError(t, err)

		assert.NoError(t, im.AddJob(context.TODO(), newRotationTestJob()))
		taskProto := &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_InProgress,
			FileStats:    newRotationTestFileStats(),
		}
		segCatalog := mocks.NewDataCoordCatalog(t)
		segCatalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
		segCatalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
		task := &importTask{
			alloc: newRotationTestAllocator(t, 9100),
			meta: &meta{
				catalog:     segCatalog,
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
				indexMeta:   &indexMeta{indexes: make(map[UniqueID]map[UniqueID]*model.Index)},
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(5, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        5,
				State:     commonpb.SegmentState_Importing,
				NumOfRows: 100,
				MaxRowNum: 100,
			},
		})
		task.meta.segments.SetSegment(6, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        6,
				State:     commonpb.SegmentState_Importing,
				NumOfRows: 50,
				MaxRowNum: 50,
			},
		})

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(7))
		// A node that is gone took the task with it, so the reclaim succeeds and
		// the task becomes dispatchable again.
		cluster.EXPECT().DropImport(int64(7), mock.Anything).Return(merr.WrapErrNodeNotFound(7)).Once()
		task.QueryTaskOnWorker(cluster)

		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		// The old segments are retired, not reused -- so whatever rows the
		// abandoned attempt wrote into them go with them.
		assert.Equal(t, commonpb.SegmentState_Dropped, task.meta.segments.GetSegment(5).GetState())
		assert.Equal(t, commonpb.SegmentState_Dropped, task.meta.segments.GetSegment(6).GetState())
		assert.NotSubset(t, []int64{5, 6}, task.GetSegmentIDs())
		assert.NotEmpty(t, task.GetSegmentIDs(), "the retry has a fresh set to write into")
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
				JobID: 1,
				State: internalpb.ImportJobState_Importing,
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
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
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
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
		job = im.GetJob(context.TODO(), 1)
		assert.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
	})

	t.Run("normal, task in-progress", func(t *testing.T) {
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
			State:        datapb.ImportTaskStateV2_InProgress,
		}
		task := &importTask{
			alloc: nil,
			meta: &meta{
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(5, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        5,
				NumOfRows: 100,
			},
		})
		task.meta.segments.SetSegment(6, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        6,
				NumOfRows: 100,
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
				JobID: 1,
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
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(5, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        5,
				NumOfRows: 100,
			},
		})
		task.meta.segments.SetSegment(6, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        6,
				NumOfRows: 100,
			},
		})

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
				JobID: 100,
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
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(42, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        42,
				NumOfRows: 0,
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
				JobID: 200,
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
				collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
				segments:    NewSegmentsInfo(),
			},
			importMeta: im,
			tr:         timerecord.NewTimeRecorder(""),
		}
		task.task.Store(taskProto)
		err = im.AddTask(context.TODO(), task)
		assert.NoError(t, err)

		task.meta.segments.SetSegment(43, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        43,
				NumOfRows: 0,
				Level:     datapb.SegmentLevel_L0,
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

// --- helpers for the segment-rotation retry path ---

// newRotationTestJob is the minimum job a retry needs to re-plan its output
// segments: rotateImportTaskSegments re-runs assignSegments, which reads the
// schema (for the primary field) and the job options.
func newRotationTestJob() ImportJob {
	return &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 3,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			},
		},
	}
}

// newRotationTestFileStats gives assignSegments a hashed size to place, so the
// rotation actually allocates segments instead of returning an empty set.
func newRotationTestFileStats() []*datapb.ImportFileStats {
	return []*datapb.ImportFileStats{{
		HashedStats: map[string]*datapb.PartitionImportStats{
			"ch-0": {PartitionDataSize: map[int64]int64{10: 1}},
		},
	}}
}

func newRotationTestAllocator(t *testing.T, next int64) allocator.Allocator {
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
