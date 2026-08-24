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
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
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

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().CreateImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		im.(*importMeta).catalog = catalog

		task.CreateTaskOnWorker(1, cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
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

		cluster := session.NewMockCluster(t)
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))
		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
		im.(*importMeta).catalog = catalog
		task.QueryTaskOnWorker(cluster)
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	})

	t.Run("QueryImport rpc failed resets NumOfRows", func(t *testing.T) {
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
		segCatalog := mocks.NewDataCoordCatalog(t)
		segCatalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
		task := &importTask{
			alloc: nil,
			meta: &meta{
				catalog:     segCatalog,
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
		cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))
		task.QueryTaskOnWorker(cluster)

		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		assert.EqualValues(t, 0, task.meta.segments.GetSegment(5).GetNumOfRows())
		assert.EqualValues(t, 0, task.meta.segments.GetSegment(6).GetNumOfRows())
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
