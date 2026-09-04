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
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type ImportInspectorSuite struct {
	suite.Suite

	collectionID int64

	catalog    *mocks.DataCoordCatalog
	alloc      *allocator.MockAllocator
	meta       *meta
	importMeta ImportMeta
	inspector  *importInspector
}

func (s *ImportInspectorSuite) SetupTest() {
	var err error

	s.collectionID = 1

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	s.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	s.alloc = allocator.NewMockAllocator(s.T())
	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	s.meta, err = newMeta(context.TODO(), s.catalog, nil, broker)
	s.NoError(err)
	s.importMeta, err = NewImportMeta(context.TODO(), s.catalog, s.alloc, s.meta)
	s.NoError(err)
	scheduler := task2.NewMockGlobalScheduler(s.T())
	s.inspector = NewImportInspector(context.TODO(), s.meta, s.importMeta, scheduler).(*importInspector)
}

func (s *ImportInspectorSuite) TestProcessPreImport() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.PreImportTask{
		JobID:        0,
		TaskID:       1,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_Pending,
	}

	var task ImportTask = &preImportTask{
		importMeta: s.importMeta,
		tr:         timerecord.NewTimeRecorder("preimport task"),
	}
	task.(*preImportTask).task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			TimeoutTs:    math.MaxUint64,
			Schema:       &schemapb.CollectionSchema{},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// pending -> inProgress
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.inspector.scheduler.(*task2.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(task task2.Task) {
		task.CreateTaskOnWorker(1, cluster)
	})
	s.inspector.inspect()
	task = s.importMeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_InProgress, task.GetState())

	// inProgress -> completed
	cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)
	task.QueryTaskOnWorker(cluster)
	task = s.importMeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Completed, task.GetState())
}

func (s *ImportInspectorSuite) TestProcessImport() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       1,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_Pending,
		FileStats: []*datapb.ImportFileStats{
			{
				HashedStats: map[string]*datapb.PartitionImportStats{
					"channel1": {
						PartitionRows: map[int64]int64{
							int64(2): 100,
						},
						PartitionDataSize: map[int64]int64{
							int64(2): 100,
						},
					},
				},
			},
		},
	}

	var task ImportTask = &importTask{
		alloc:      s.alloc,
		meta:       s.meta,
		importMeta: s.importMeta,
		tr:         timerecord.NewTimeRecorder("import task"),
	}
	task.(*importTask).task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"channel1"},
			Schema:       &schemapb.CollectionSchema{},
			TimeoutTs:    math.MaxUint64,
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// pending -> inProgress
	const nodeID = 10
	s.alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	s.alloc.EXPECT().AllocTimestamp(mock.Anything).Return(300, nil)
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.inspector.scheduler.(*task2.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(task task2.Task) {
		task.CreateTaskOnWorker(nodeID, cluster)
	})
	s.inspector.inspect()
	task = s.importMeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_InProgress, task.GetState())

	// inProgress -> completed
	cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)
	task.QueryTaskOnWorker(cluster)
	task = s.importMeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Completed, task.GetState())
}

func (s *ImportInspectorSuite) TestProcessFailed() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:            0,
		TaskID:           1,
		CollectionID:     s.collectionID,
		NodeID:           6,
		SegmentIDs:       []int64{2, 3},
		SortedSegmentIDs: []int64{4, 5},
		State:            datapb.ImportTaskStateV2_Failed,
	}

	var task ImportTask = &importTask{
		tr: timerecord.NewTimeRecorder("import task"),
	}
	task.(*importTask).task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"channel1"},
			Schema:       &schemapb.CollectionSchema{},
			TimeoutTs:    math.MaxUint64,
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	originSegmentIDs := append([]int64(nil), task.(*importTask).GetSegmentIDs()...)
	for _, id := range originSegmentIDs {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{ID: id, State: commonpb.SegmentState_Importing, IsImporting: true},
		}
		err = s.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
	}
	for _, id := range originSegmentIDs {
		segment := s.meta.GetSegment(context.TODO(), id)
		s.NotNil(segment)
	}
	const sortedOutputID = int64(6)
	err = s.meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             sortedOutputID,
		State:          commonpb.SegmentState_Flushed,
		IsImporting:    true,
		CompactionFrom: []int64{originSegmentIDs[0]},
	}))
	s.NoError(err)

	// Cleanup must read the compaction edge only after the origin is dropped.
	// At that point a concurrent sort either already published its output, or
	// its completion will reject the unhealthy origin.
	checkedOrigins := make(map[int64]bool)
	var getCompactionTo func(*meta, int64) ([]*SegmentInfo, bool)
	mockGetCompactionTo := mockey.Mock((*meta).GetCompactionTo).To(
		func(meta *meta, originID int64) ([]*SegmentInfo, bool) {
			origin := meta.GetSegment(context.TODO(), originID)
			s.Require().NotNil(origin)
			s.Equal(commonpb.SegmentState_Dropped, origin.GetState())
			checkedOrigins[originID] = true
			return getCompactionTo(meta, originID)
		}).Origin(&getCompactionTo).Build()
	defer mockGetCompactionTo.UnPatch()

	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	s.inspector.inspect()
	s.Len(checkedOrigins, len(originSegmentIDs))
	for _, id := range append(originSegmentIDs, sortedOutputID) {
		segment := s.meta.GetSegment(context.TODO(), id)
		s.Equal(commonpb.SegmentState_Dropped, segment.GetState())
		s.False(segment.GetIsImporting())
	}
	task = s.importMeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Failed, task.GetState())
	s.Empty(task.(*importTask).GetSegmentIDs())
	s.Empty(task.(*importTask).GetSortedSegmentIDs())
}

func (s *ImportInspectorSuite) TestProcessPreImportRetryPublishesFreshID() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.alloc.EXPECT().AllocID(mock.Anything).Return(int64(12), nil).Once()

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 10, State: internalpb.ImportJobState_PreImporting,
	}}
	s.NoError(s.importMeta.AddJob(context.TODO(), job))
	oldTask := &preImportTask{alloc: s.alloc, importMeta: s.importMeta, tr: timerecord.NewTimeRecorder("preimport retry")}
	oldTask.task.Store(&datapb.PreImportTask{
		JobID: 10, TaskID: 11, CollectionID: s.collectionID,
		State: datapb.ImportTaskStateV2_Retry, NodeID: NullNodeID, TaskVersion: 2,
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), oldTask))
	s.inspector.scheduler.(*task2.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).
		Run(func(task task2.Task) {
			s.Equal(int64(12), task.GetTaskID())
		}).Once()

	s.inspector.inspect()
	s.Nil(s.importMeta.GetTask(context.TODO(), int64(11)))
	updated := s.importMeta.GetTask(context.TODO(), int64(12))
	s.Require().NotNil(updated)
	s.Equal(datapb.ImportTaskStateV2_Pending, updated.GetState())
	s.Equal(int64(3), updated.GetTaskVersion())
	s.Equal(datapb.ImportTaskStateV2_Retry, oldTask.GetState())
}

func (s *ImportInspectorSuite) TestRetryAttemptCapFailsJob() {
	key := Params.DataCoordCfg.ImportMaxAttempts.Key
	Params.Save(key, "2")
	defer Params.Reset(key)

	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 20}}
	s.NoError(s.importMeta.AddJob(context.TODO(), job))
	task := &preImportTask{importMeta: s.importMeta, tr: timerecord.NewTimeRecorder("spent retry")}
	task.task.Store(&datapb.PreImportTask{
		JobID: 20, TaskID: 21, CollectionID: s.collectionID,
		State: datapb.ImportTaskStateV2_Retry, TaskVersion: 1, Reason: "worker unavailable",
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	s.inspector.inspect()
	updatedJob := s.importMeta.GetJob(context.TODO(), job.GetJobID())
	s.Equal(internalpb.ImportJobState_Failed, updatedJob.GetState())
	s.Contains(updatedJob.GetReason(), "attempt limit (2)")
	// The checker owns task settlement and cleanup after the job decision.
	s.Equal(datapb.ImportTaskStateV2_Retry, s.importMeta.GetTask(context.TODO(), task.GetTaskID()).GetState())
}

func (s *ImportInspectorSuite) TestRetryAttemptCapDecisionFailsStopAfterCatalogFailure() {
	key := Params.DataCoordCfg.ImportMaxAttempts.Key
	Params.Save(key, "1")
	defer Params.Reset(key)

	// AddJob succeeds, then the terminal decision gets an ambiguous response.
	// Fail-stop leaves the Retry task as durable debt for restart recovery.
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(errors.New("mock save job err")).Once()
	s.catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 30}}
	s.NoError(s.importMeta.AddJob(context.TODO(), job))
	task := &preImportTask{importMeta: s.importMeta, tr: timerecord.NewTimeRecorder("spent retry")}
	task.task.Store(&datapb.PreImportTask{
		JobID: 30, TaskID: 31, CollectionID: s.collectionID,
		State: datapb.ImportTaskStateV2_Retry,
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	s.inspector.inspect()
	s.True(fatalCalled)
	s.NotEqual(internalpb.ImportJobState_Failed, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.Equal(datapb.ImportTaskStateV2_Retry, s.importMeta.GetTask(context.TODO(), task.GetTaskID()).GetState())
}

func (s *ImportInspectorSuite) TestReloadFromMeta() {
	// Test case 1: No jobs and tasks
	s.catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	s.inspector.reloadFromMeta()

	// Test case 2: Jobs with in-progress tasks
	jobProto := &datapb.ImportJob{
		JobID:        1,
		CollectionID: s.collectionID,
		TimeoutTs:    math.MaxUint64,
		Schema:       &schemapb.CollectionSchema{},
	}
	job := &importJob{
		ImportJob: jobProto,
		tr:        timerecord.NewTimeRecorder("import job"),
	}
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	err := s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Add an in-progress pre-import task
	inprogressPreImportTask := &preImportTask{
		importMeta: s.importMeta,
		tr:         timerecord.NewTimeRecorder("preimport task"),
	}
	inprogressPreImportTask.task.Store(&datapb.PreImportTask{
		JobID:        1,
		TaskID:       1,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_InProgress,
	})
	s.catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	err = s.importMeta.AddTask(context.TODO(), inprogressPreImportTask)
	s.NoError(err)

	// Add an in-progress import task
	inprogressImportTask := &importTask{
		importMeta: s.importMeta,
		tr:         timerecord.NewTimeRecorder("import task"),
	}
	inprogressImportTask.task.Store(&datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       2,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_InProgress,
	})
	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	err = s.importMeta.AddTask(context.TODO(), inprogressImportTask)
	s.NoError(err)

	// Add an pending import task
	pendingImportTask := &importTask{
		importMeta: s.importMeta,
		tr:         timerecord.NewTimeRecorder("import task"),
	}
	pendingImportTask.task.Store(&datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       3,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_Pending,
	})
	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	_ = s.importMeta.AddTask(context.TODO(), pendingImportTask)

	// Mock scheduler expectations
	s.inspector.scheduler.(*task2.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Times(3)
	s.inspector.reloadFromMeta()
}

func (s *ImportInspectorSuite) TestIgnoreOrphanTasks() {
	newTask := func(taskID int64, state datapb.ImportTaskStateV2, segmentIDs ...int64) ImportTask {
		task := &importTask{
			importMeta: s.importMeta,
			tr:         timerecord.NewTimeRecorder("import task"),
		}
		task.task.Store(&datapb.ImportTaskV2{
			JobID:        100,
			TaskID:       taskID,
			CollectionID: s.collectionID,
			State:        state,
			SegmentIDs:   segmentIDs,
		})
		return task
	}

	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Times(3)
	s.NoError(s.importMeta.AddTask(context.TODO(), newTask(1, datapb.ImportTaskStateV2_InProgress)))
	s.NoError(s.importMeta.AddTask(context.TODO(), newTask(2, datapb.ImportTaskStateV2_Pending)))
	s.NoError(s.importMeta.AddTask(context.TODO(), newTask(3, datapb.ImportTaskStateV2_Failed, 10)))

	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.NoError(s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:          10,
			State:       commonpb.SegmentState_Importing,
			IsImporting: true,
		},
	}))

	// Orphan tasks are skipped just as they were when the inspector iterated jobs first.
	s.inspector.reloadFromMeta()
	s.inspector.inspect()
	s.Equal(commonpb.SegmentState_Importing, s.meta.GetSegment(context.TODO(), 10).GetState())
}

func (s *ImportInspectorSuite) TestSortImportTasks() {
	newTask := func(jobID, taskID int64) ImportTask {
		task := &importTask{}
		task.task.Store(&datapb.ImportTaskV2{JobID: jobID, TaskID: taskID})
		return task
	}
	tasks := []ImportTask{
		newTask(2, 4),
		newTask(1, 3),
		newTask(2, 2),
		newTask(1, 1),
	}

	sortImportTasks(tasks)
	s.Equal([]int64{1, 1, 2, 2}, lo.Map(tasks, func(task ImportTask, _ int) int64 {
		return task.GetJobID()
	}))
	s.Equal([]int64{1, 3, 2, 4}, lo.Map(tasks, func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))
}

func TestImportInspector(t *testing.T) {
	suite.Run(t, new(ImportInspectorSuite))
}
