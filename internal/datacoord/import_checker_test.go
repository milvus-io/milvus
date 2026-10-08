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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	datacoordtask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type ImportCheckerSuite struct {
	suite.Suite

	jobID      int64
	importMeta ImportMeta
	checker    *importChecker
	alloc      *allocator.MockAllocator
	cluster    *session.MockCluster
}

func completedPreImportFileStats(task ImportTask, totalRows, totalMemorySize int64) []*datapb.ImportFileStats {
	stats := make([]*datapb.ImportFileStats, 0, len(task.GetFileStats()))
	for _, pendingStat := range task.GetFileStats() {
		stats = append(stats, &datapb.ImportFileStats{
			ImportFile:      pendingStat.GetImportFile(),
			TotalRows:       totalRows,
			TotalMemorySize: totalMemorySize,
		})
	}
	return stats
}

func (s *ImportCheckerSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	s.alloc = allocator.NewMockAllocator(s.T())

	broker := broker2.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	importMeta, err := NewImportMeta(context.TODO(), catalog, s.alloc, meta)
	s.NoError(err)
	s.importMeta = importMeta

	ci := NewMockCompactionInspector(s.T())

	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collID int64) (*collectionInfo, error) {
		return &collectionInfo{
			ID: collID,
		}, nil
	}).Maybe()

	s.cluster = session.NewMockCluster(s.T())
	checker := NewImportChecker(context.TODO(), meta, broker, s.alloc, importMeta, ci, handler, s.cluster, nil, importCheckerHooks{}).(*importChecker)
	s.checker = checker

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"ch0"},
			State:        internalpb.ImportJobState_Pending,
			TimeoutTs:    1000,
			CleanupTs:    tsoutil.ComposeTSByTime(time.Now()),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
				},
			},
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
				{
					Id:    2,
					Paths: []string{"b.json"},
				},
				{
					Id:    3,
					Paths: []string{"c.json"},
				},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}

	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	err = s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)
	s.jobID = job.GetJobID()
}

func (s *ImportCheckerSuite) TestLogStats() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:  s.jobID,
		TaskID: 1,
		State:  datapb.ImportTaskStateV2_Failed,
	}
	pit1 := &preImportTask{
		tr: timerecord.NewTimeRecorder("preimport task"),
	}
	pit1.task.Store(preImportTaskProto)
	err := s.importMeta.AddTask(context.TODO(), pit1)
	s.NoError(err)

	importTaskProto := &datapb.ImportTaskV2{
		JobID:      s.jobID,
		TaskID:     2,
		SegmentIDs: []int64{10, 11, 12},
		State:      datapb.ImportTaskStateV2_Pending,
	}
	it1 := &importTask{
		tr: timerecord.NewTimeRecorder("import task"),
	}
	it1.task.Store(importTaskProto)
	err = s.importMeta.AddTask(context.TODO(), it1)
	s.NoError(err)

	s.checker.LogTaskStats()
}

func (s *ImportCheckerSuite) TestCheckJob() {
	job := s.importMeta.GetJob(context.TODO(), s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	s.checker.checkPendingJob(job)
	preimportTasks := s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.checkPendingJob(job) // no lack
	preimportTasks = s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkPreImportingJob
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil).Once()
	for _, t := range preimportTasks {
		err := s.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats(completedPreImportFileStats(t, 100, 0)))
		s.NoError(err)
	}

	job = s.restampSuiteJobRanges()
	s.checker.checkPreImportingJob(job)
	importTasks := s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.checkPreImportingJob(job) // no lack
	importTasks = s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkImportingJob
	s.checker.checkImportingJob(job)
	s.Equal(internalpb.ImportJobState_Importing, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	for _, t := range importTasks {
		task := s.importMeta.GetTask(context.TODO(), t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(context.TODO(), id)
			s.Equal(true, segment.GetIsImporting())
		}
	}
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	// AlterSegments is no longer called from checkIndexBuildingJob (unsetSegmentImporting removed);
	// the upstream checkImportingJob path may still invoke it. Loosen to .Maybe().
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	targetSegmentIDs := make([]int64, 0)
	for _, t := range importTasks {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            rand.Int63(),
				CollectionID:  job.GetCollectionID(),
				PartitionID:   job.GetPartitionIDs()[0],
				State:         commonpb.SegmentState_Flushed,
				IsImporting:   true,
				InsertChannel: "ch0",
				NumOfRows:     1000,
			},
		}
		err := s.checker.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
		targetSegmentID := rand.Int63()
		err = s.importMeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}), UpdateStatsSegmentIDs([]int64{targetSegmentID}))
		s.NoError(err)
		targetSegmentIDs = append(targetSegmentIDs, targetSegmentID)
		err = s.checker.meta.UpdateChannelCheckpoint(context.TODO(), segment.GetInsertChannel(), &msgpb.MsgPosition{MsgID: []byte{0}})
		s.NoError(err)
	}
	s.checker.checkImportingJob(job)
	s.Equal(internalpb.ImportJobState_Sorting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test check stats job
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil).Maybe()
	cim := s.checker.ci.(*MockCompactionInspector)
	cim.EXPECT().enqueueCompaction(mock.Anything).Return(nil)

	s.checker.checkSortingJob(job)
	s.Equal(internalpb.ImportJobState_Sorting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	for i, segmentID := range targetSegmentIDs {
		originSegmentID := importTasks[i].(*importTask).GetSegmentIDs()[0]
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             segmentID,
				CollectionID:   job.GetCollectionID(),
				PartitionID:    job.GetPartitionIDs()[0],
				State:          commonpb.SegmentState_Flushed,
				IsImporting:    true,
				InsertChannel:  "ch0",
				IsSorted:       true,
				CompactionFrom: []int64{originSegmentID},
			},
		}
		err := s.checker.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
	}

	s.checker.checkSortingJob(job)
	s.Equal(internalpb.ImportJobState_IndexBuilding, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test check IndexBuilding job — transitions to Uncommitted, segments keep is_importing=true
	// until HandleCommitVchannel runs after the WAL commit fence.
	s.checker.checkIndexBuildingJob(job)
	for _, t := range importTasks {
		task := s.importMeta.GetTask(context.TODO(), t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(context.TODO(), id)
			s.Equal(true, segment.GetIsImporting(), "is_importing must stay true until HandleCommitVchannel")
		}
	}
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckPendingJobRetriesStateAfterTasksPersisted() {
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.ExpectedCalls = nil

	s.alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 100, 100 + n, nil
	}).Once()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(errors.New("save job failed")).Once()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()

	// The task writes land before the Pending -> PreImporting write fails.
	s.checker.checkPendingJob(job)
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	preimports := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(PreImportTaskType))
	s.NotEmpty(preimports)

	// No files are missing now. The next pass must retry only the job-state write.
	s.checker.checkPendingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(PreImportTaskType)), len(preimports))
}

func (s *ImportCheckerSuite) TestCheckPendingJobTaskPublicationFailureFailsStopWhileCheckerAlive() {
	job := s.importMeta.GetJob(context.Background(), s.jobID)
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.ExpectedCalls = nil
	s.alloc.ExpectedCalls = nil

	s.alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 100, 100 + n, nil
	}).Once()
	writeErr := errors.New("ambiguous catalog response")
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(writeErr).Once()

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	s.checker.ctx = context.Background()
	s.checker.checkPendingJob(job)

	s.True(fatalCalled)
	s.Empty(s.importMeta.GetTaskByJob(context.Background(), s.jobID, WithType(PreImportTaskType)))
	s.Equal(internalpb.ImportJobState_Pending,
		s.importMeta.GetJob(context.Background(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckPendingJobTaskPublicationFailureDuringShutdownReturns() {
	job := s.importMeta.GetJob(context.Background(), s.jobID)
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.ExpectedCalls = nil
	s.alloc.ExpectedCalls = nil

	s.alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 100, 100 + n, nil
	}).Once()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).
		Return(errors.New("checker is shutting down")).
		Once()

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	checkerCtx, cancel := context.WithCancel(context.Background())
	cancel()
	s.checker.ctx = checkerCtx
	s.checker.checkPendingJob(job)

	s.False(fatalCalled)
	s.Empty(s.importMeta.GetTaskByJob(context.Background(), s.jobID, WithType(PreImportTaskType)))
	s.Equal(internalpb.ImportJobState_Pending,
		s.importMeta.GetJob(context.Background(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckPreImportingRecoversCompleteTaskSetAfterRestart() {
	oldDiskProtection := Params.QuotaConfig.DiskProtectionEnabled.SwapTempValue("true")
	s.T().Cleanup(func() {
		Params.QuotaConfig.DiskProtectionEnabled.SwapTempValue(oldDiskProtection)
	})

	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_PreImporting))
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	stats := make([]*datapb.ImportFileStats, 0, len(job.GetFiles()))
	var expectedRequestSize int64
	for i, file := range job.GetFiles() {
		memorySize := int64((i + 1) * 100)
		expectedRequestSize += memorySize
		stats = append(stats, &datapb.ImportFileStats{
			ImportFile:      file,
			TotalRows:       10,
			TotalMemorySize: memorySize,
			HashedStats: map[string]*datapb.PartitionImportStats{
				"ch0": {
					PartitionDataSize: map[int64]int64{2: 1},
				},
			},
		})
	}

	preimportProto := &datapb.PreImportTask{
		JobID:        s.jobID,
		TaskID:       100,
		CollectionID: job.GetCollectionID(),
		State:        datapb.ImportTaskStateV2_Completed,
		FileStats:    stats,
	}
	preimport := &preImportTask{tr: timerecord.NewTimeRecorder("preimport task")}
	preimport.task.Store(preimportProto)

	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.importMeta.AddTask(context.TODO(), preimport))
	// Replace SetupTest's broad SaveImportJob expectation with the one-shot
	// failure that models the publication boundary under test.
	catalog.ExpectedCalls = nil

	s.alloc.EXPECT().AllocN(int64(1)).Return(int64(200), int64(201), nil).Once()
	s.alloc.EXPECT().AllocID(mock.Anything).Return(int64(300), nil).Once()
	s.alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(400), nil).Once()
	var persistedImportTask *datapb.ImportTaskV2
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, actions ...metastore.UpdateAction) {
			for _, action := range actions {
				if entry, ok := action.Entry.(metastore.ImportTaskEntry); ok {
					persistedImportTask = proto.Clone(entry.Task).(*datapb.ImportTaskV2)
				}
			}
		}).
		Return(nil).
		Once()
	persistErr := errors.New("failed to persist Importing state")
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).
		Run(func(_ context.Context, updated *datapb.ImportJob) {
			s.Equal(internalpb.ImportJobState_Importing, updated.GetState())
			s.Equal(expectedRequestSize, updated.GetRequestedDiskSize())
		}).
		Return(persistErr).
		Once()

	// Every import task is durable, but the final job write fails. The in-memory
	// job must remain PreImporting, matching what a restarted process reloads.
	s.checker.checkPreImportingJob(job)
	s.NotNil(persistedImportTask)
	s.Equal(internalpb.ImportJobState_PreImporting,
		s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Zero(s.importMeta.GetJob(context.TODO(), s.jobID).GetRequestedDiskSize())
	s.Len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType)), 1)

	persistedJob := proto.Clone(s.importMeta.GetJob(context.TODO(), s.jobID).(*importJob).ImportJob).(*datapb.ImportJob)
	restartCatalog := mocks.NewDataCoordCatalog(s.T())
	restartCatalog.EXPECT().ListPreImportTasks(mock.Anything).
		Return([]*datapb.PreImportTask{proto.Clone(preimportProto).(*datapb.PreImportTask)}, nil).
		Once()
	restartCatalog.EXPECT().ListImportTasks(mock.Anything).
		Return([]*datapb.ImportTaskV2{persistedImportTask}, nil).
		Once()
	restartCatalog.EXPECT().ListImportJobs(mock.Anything).
		Return([]*datapb.ImportJob{persistedJob}, nil).
		Once()

	restartedMeta, err := NewImportMeta(context.TODO(), restartCatalog, s.alloc, s.checker.meta)
	s.NoError(err)
	restartCatalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).
		Run(func(_ context.Context, updated *datapb.ImportJob) {
			s.Equal(internalpb.ImportJobState_Importing, updated.GetState())
			s.Equal(expectedRequestSize, updated.GetRequestedDiskSize())
		}).
		Return(nil).
		Once()

	originalImportMeta := s.checker.importMeta
	s.checker.importMeta = restartedMeta
	defer func() { s.checker.importMeta = originalImportMeta }()
	s.checker.checkPreImportingJob(restartedMeta.GetJob(context.TODO(), s.jobID))

	recovered := restartedMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Importing, recovered.GetState())
	s.Equal(expectedRequestSize, recovered.GetRequestedDiskSize())
	s.Len(restartedMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType)), 1,
		"recovery must reuse the complete persisted task set")
}

func (s *ImportCheckerSuite) TestGetLackFilesForImportsAcceptsDuplicateCoverage() {
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	stats := make([]*datapb.ImportFileStats, 0, len(job.GetFiles()))
	for _, file := range job.GetFiles() {
		stats = append(stats, &datapb.ImportFileStats{ImportFile: file, TotalRows: 1})
	}
	preimport := &preImportTask{}
	preimport.task.Store(&datapb.PreImportTask{
		JobID:        job.GetJobID(),
		TaskID:       100,
		CollectionID: job.GetCollectionID(),
		State:        datapb.ImportTaskStateV2_Completed,
		FileStats:    stats,
	})
	newImportTask := func(taskID int64, taskStats ...*datapb.ImportFileStats) ImportTask {
		task := &importTask{}
		task.task.Store(&datapb.ImportTaskV2{
			JobID:        job.GetJobID(),
			TaskID:       taskID,
			CollectionID: job.GetCollectionID(),
			FileStats:    taskStats,
		})
		return task
	}

	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	s.NoError(s.importMeta.AddTask(context.TODO(), preimport))
	s.NoError(s.importMeta.AddTask(context.TODO(), newImportTask(200, stats[0], stats[1])))
	s.NoError(s.importMeta.AddTask(context.TODO(), newImportTask(201, stats[0], stats[2])))

	// An ambiguous catalog response can leave duplicate work. Every file is
	// still covered, so the job may advance; normal task/job GC removes the
	// duplicate later.
	s.Empty(s.checker.getLackFilesForImports(job))
}

func (s *ImportCheckerSuite) assertIndexBuildingRejectsTarget(target *datapb.SegmentInfo, reason string) {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &importTask{tr: timerecord.NewTimeRecorder("import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:        s.jobID,
		TaskID:       10,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Completed,
		SegmentIDs:   []int64{100},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	// The origin is the sort-planned shape: invisible, so its sorted output
	// must be discovered through the compactionTo edge.
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch0",
		State:         commonpb.SegmentState_Flushed,
		IsImporting:   true,
		IsInvisible:   true,
	})))
	if target != nil {
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(target)))
	}

	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_IndexBuilding))
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.checker.checkIndexBuildingJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), reason)
}

func (s *ImportCheckerSuite) TestCheckIndexBuildingJobRejectsMissingTarget() {
	s.assertIndexBuildingRejectsTarget(nil, "origin segment 100 has no sorted output")
}

// A job planned without sort by an older binary (dataCoord.enableCompaction
// off at planning time) has healthy, visible, importing origins and no sorted
// output: rolling upgrade must treat the origin as the final imported segment
// instead of failing the job and dropping the data.
func (s *ImportCheckerSuite) TestIndexBuildingAcceptsLegacyUnsortedOrigins() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &importTask{tr: timerecord.NewTimeRecorder("legacy unsorted import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:        s.jobID,
		TaskID:       10,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Completed,
		SegmentIDs:   []int64{100},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch0",
		State:         commonpb.SegmentState_Flushed,
		IsImporting:   true,
		IsInvisible:   false,
	})))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	tasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))
	targets, err := s.checker.getValidatedImportTargets(job, tasks, true)
	s.NoError(err)
	s.Equal([]int64{100}, targets)
}

func (s *ImportCheckerSuite) TestCheckIndexBuildingJobAllowsExplicitZeroRowSortSkip() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &importTask{tr: timerecord.NewTimeRecorder("zero-row sorted import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:            s.jobID,
		TaskID:           10,
		CollectionID:     1,
		State:            datapb.ImportTaskStateV2_Completed,
		SegmentIDs:       []int64{100},
		SortedSegmentIDs: []int64{200},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	// Start with the exact zero-row origin produced by import. Sorting must
	// persist its Dropped marker and intentionally create no target 200.
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch0",
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     0,
		IsImporting:   true,
	})))

	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_Sorting))
	catalog.ExpectedCalls = nil
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.checker.checkSortingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(commonpb.SegmentState_Dropped, s.checker.meta.GetSegment(context.TODO(), 100).GetState())
	s.Equal(internalpb.ImportJobState_IndexBuilding, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.checker.checkIndexBuildingJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Uncommitted, job.GetState())
}

func (s *ImportCheckerSuite) TestCheckIndexBuildingJobRejectsForeignTarget() {
	s.assertIndexBuildingRejectsTarget(&datapb.SegmentInfo{
		ID:             200,
		CollectionID:   2,
		PartitionID:    2,
		InsertChannel:  "ch0",
		State:          commonpb.SegmentState_Flushed,
		IsImporting:    true,
		IsSorted:       true,
		CompactionFrom: []int64{100},
	}, "belongs to collection 2, expected 1")
}

// A zero-row sorted output is published Dropped (all rows expired or deleted
// before the sort): the branch is a completed empty result, not corruption --
// the job must skip it rather than fail and drop the other origins' data.
func (s *ImportCheckerSuite) TestCheckIndexBuildingJobSkipsDroppedSortedOutput() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &importTask{tr: timerecord.NewTimeRecorder("zero-row sorted output task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:        s.jobID,
		TaskID:       10,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Completed,
		SegmentIDs:   []int64{100},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch0",
		State:         commonpb.SegmentState_Flushed,
		IsImporting:   true,
		IsInvisible:   true,
	})))
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             200,
		CollectionID:   1,
		PartitionID:    2,
		InsertChannel:  "ch0",
		State:          commonpb.SegmentState_Dropped,
		NumOfRows:      0,
		IsImporting:    true,
		IsSorted:       true,
		CompactionFrom: []int64{100},
	})))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	tasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))
	targets, err := s.checker.getValidatedImportTargets(job, tasks, true)
	s.NoError(err)
	s.Empty(targets, "a dropped zero-row output is a completed empty branch, not a target or a failure")
}

func (s *ImportCheckerSuite) TestCheckIndexBuildingJobRejectsUnflushedTarget() {
	s.assertIndexBuildingRejectsTarget(&datapb.SegmentInfo{
		ID:             200,
		CollectionID:   1,
		PartitionID:    2,
		InsertChannel:  "ch0",
		State:          commonpb.SegmentState_Importing,
		IsImporting:    true,
		IsSorted:       true,
		CompactionFrom: []int64{100},
	}, "must be Flushed")
}

func (s *ImportCheckerSuite) TestIndexBuildingSkipsZeroRowUnsortedOrigin() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &importTask{tr: timerecord.NewTimeRecorder("zero-row unsorted import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID: s.jobID, TaskID: 10, CollectionID: 1,
		State: datapb.ImportTaskStateV2_Completed, SegmentIDs: []int64{100, 101},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Twice()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID: 100, CollectionID: 1, PartitionID: 2, InsertChannel: "ch0",
		State: commonpb.SegmentState_Flushed, IsImporting: true,
	})))
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID: 101, CollectionID: 1, PartitionID: 2, InsertChannel: "ch0",
		State: commonpb.SegmentState_Dropped, NumOfRows: 0, IsImporting: true,
	})))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	targets, err := s.checker.getValidatedImportTargets(job, []ImportTask{task}, false)
	s.NoError(err)
	s.Equal([]int64{100}, targets)
}

func TestExplicitZeroRowSortedOutputSkip(t *testing.T) {
	job := &importJob{ImportJob: &datapb.ImportJob{
		CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"ch0"},
	}}
	valid := &datapb.SegmentInfo{
		ID: 200, CollectionID: 1, PartitionID: 2, InsertChannel: "ch0",
		State: commonpb.SegmentState_Dropped, NumOfRows: 0, IsImporting: true,
		CompactionFrom: []int64{100},
	}
	assert.True(t, isExplicitZeroRowSortedOutputSkip(job, 100, NewSegmentInfo(proto.Clone(valid).(*datapb.SegmentInfo))))

	tests := map[string]func(*datapb.SegmentInfo){
		"non-zero rows":         func(segment *datapb.SegmentInfo) { segment.NumOfRows = 1 },
		"published":             func(segment *datapb.SegmentInfo) { segment.IsImporting = false },
		"foreign collection":    func(segment *datapb.SegmentInfo) { segment.CollectionID = 9 },
		"foreign partition":     func(segment *datapb.SegmentInfo) { segment.PartitionID = 9 },
		"foreign channel":       func(segment *datapb.SegmentInfo) { segment.InsertChannel = "other" },
		"wrong compaction edge": func(segment *datapb.SegmentInfo) { segment.CompactionFrom = []int64{999} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			segment := proto.Clone(valid).(*datapb.SegmentInfo)
			mutate(segment)
			assert.False(t, isExplicitZeroRowSortedOutputSkip(job, 100, NewSegmentInfo(segment)))
		})
	}
}

// Namespace-enabled collections mark their sorted output IsSortedByNamespace
// instead of IsSorted; validation must accept either flag.
func (s *ImportCheckerSuite) TestIndexBuildingAcceptsNamespaceSortedTarget() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	task := &importTask{tr: timerecord.NewTimeRecorder("namespace sorted import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:        s.jobID,
		TaskID:       10,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Completed,
		SegmentIDs:   []int64{100},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch0",
		State:         commonpb.SegmentState_Flushed,
		IsImporting:   true,
		IsInvisible:   true,
	})))
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:                  200,
		CollectionID:        1,
		PartitionID:         2,
		InsertChannel:       "ch0",
		State:               commonpb.SegmentState_Flushed,
		IsImporting:         true,
		IsSorted:            false,
		IsSortedByNamespace: true,
		CompactionFrom:      []int64{100},
	})))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	tasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))
	targets, err := s.checker.getValidatedImportTargets(job, tasks, true)
	s.NoError(err)
	s.Equal([]int64{200}, targets)
}

func (s *ImportCheckerSuite) TestCheckIndexBuildingJobRejectsPublishedTarget() {
	s.assertIndexBuildingRejectsTarget(&datapb.SegmentInfo{
		ID:             200,
		CollectionID:   1,
		PartitionID:    2,
		InsertChannel:  "ch0",
		State:          commonpb.SegmentState_Flushed,
		IsImporting:    false,
		IsSorted:       true,
		CompactionFrom: []int64{100},
	}, "segment 200 is already published")
}

func (s *ImportCheckerSuite) TestLegacyL0SortMetadataUsesOriginPlan() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	task := &importTask{tr: timerecord.NewTimeRecorder("legacy l0 import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:            s.jobID,
		TaskID:           10,
		CollectionID:     1,
		State:            datapb.ImportTaskStateV2_Completed,
		SegmentIDs:       []int64{100},
		SortedSegmentIDs: []int64{200},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))
	s.NoError(s.checker.meta.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:            100,
		CollectionID:  1,
		PartitionID:   2,
		InsertChannel: "ch0",
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L0,
		IsImporting:   true,
	})))
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		legacyL0Job := job.(*importJob)
		legacyL0Job.Options = []*commonpb.KeyValuePair{
			{Key: importutilv2.L0Import, Value: "true"},
		}
		legacyL0Job.State = internalpb.ImportJobState_Sorting
	})
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Twice()

	s.checker.checkSortingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_IndexBuilding,
		s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"L0 must skip sorting even when a legacy task contains sorted IDs")

	s.checker.checkIndexBuildingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_Uncommitted,
		s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"L0 index completion must use origin segments and ignore legacy sorted targets")
}

func (s *ImportCheckerSuite) TestCorruptL0TaskFailsClosedWithoutPanic() {
	job := s.importMeta.GetJob(context.TODO(), s.jobID).Clone().(*importJob)
	job.Options = []*commonpb.KeyValuePair{
		{Key: importutilv2.L0Import, Value: "true"},
	}
	job.State = internalpb.ImportJobState_Sorting

	corruptMeta := NewMockImportMeta(s.T())
	corruptMeta.EXPECT().GetTaskByJob(mock.Anything, job.GetJobID(), mock.Anything).
		Return([]ImportTask{nil})
	corruptMeta.EXPECT().UpdateJob(mock.Anything, job.GetJobID(), mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, actions ...UpdateJobAction) error {
			for _, action := range actions {
				action(job)
			}
			return nil
		})
	originalImportMeta := s.checker.importMeta
	s.checker.importMeta = corruptMeta
	defer func() { s.checker.importMeta = originalImportMeta }()

	s.NotPanics(func() {
		s.checker.checkSortingJob(job)
	})
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "invalid concrete type")
}

func (s *ImportCheckerSuite) newSortingJob(jobID, collectionID, firstSegmentID int64, rows ...int64) ImportJob {
	s.T().Helper()
	ctx := context.Background()
	job := s.importMeta.GetJob(ctx, s.jobID).Clone().(*importJob)
	job.JobID = jobID
	job.CollectionID = collectionID
	job.State = internalpb.ImportJobState_Sorting
	s.Require().NoError(s.importMeta.AddJob(ctx, job))
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	for i, numRows := range rows {
		segmentID := firstSegmentID + int64(i)
		catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
		s.Require().NoError(s.checker.meta.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID: segmentID, CollectionID: collectionID, PartitionID: job.GetPartitionIDs()[0],
			InsertChannel: job.GetVchannels()[0], State: commonpb.SegmentState_Flushed,
			NumOfRows: numRows, IsImporting: true, IsInvisible: true,
		})))
		catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
		importTask := &importTask{tr: timerecord.NewTimeRecorder("sorting import task")}
		importTask.task.Store(&datapb.ImportTaskV2{
			JobID: jobID, TaskID: segmentID, CollectionID: collectionID,
			State: datapb.ImportTaskStateV2_Completed, SegmentIDs: []int64{segmentID},
		})
		s.Require().NoError(s.importMeta.AddTask(ctx, importTask))
	}
	return job
}

func (s *ImportCheckerSuite) TestCheckSortingJobSharesCollectionWithinRound() {
	job := s.newSortingJob(10, 1, 100, 100, 200, 300)
	collection := &collectionInfo{ID: job.GetCollectionID(), Schema: job.GetSchema()}
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, job.GetCollectionID()).Return(collection, nil).Once()
	s.checker.handler = handler
	nextID := int64(1000)
	s.alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		start := nextID
		nextID += n
		return start, nextID, nil
	}).Times(6)
	origins := make([]int64, 0, 3)
	ci := s.checker.ci.(*MockCompactionInspector)
	ci.EXPECT().enqueueCompaction(mock.Anything).RunAndReturn(func(plan *datapb.CompactionTask) error {
		s.Same(collection.Schema, plan.GetSchema())
		s.Equal(job.GetCollectionID(), plan.GetCollectionID())
		origins = append(origins, plan.GetInputSegments()...)
		return nil
	}).Times(3)

	s.checker.checkSortingJob(job)

	s.ElementsMatch([]int64{100, 101, 102}, origins)
	handler.AssertNumberOfCalls(s.T(), "GetCollection", 1)
	s.Equal(internalpb.ImportJobState_Sorting, s.importMeta.GetJob(context.Background(), job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckSortingJobDroppedCollectionDoesNotBlockNextJob() {
	dropped := s.newSortingJob(10, 1, 100, 100, 200, 300)
	healthy := s.newSortingJob(20, 2, 200, 100)
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, dropped.GetCollectionID()).Return(nil,
		merr.Wrap(merr.WrapErrCollectionNotFound(dropped.GetCollectionID()), "load sorting collection")).Once()
	handler.EXPECT().GetCollection(mock.Anything, healthy.GetCollectionID()).Return(&collectionInfo{
		ID: healthy.GetCollectionID(), Schema: healthy.GetSchema(),
	}, nil).Once()
	s.checker.handler = handler
	s.alloc.EXPECT().AllocN(int64(2)).Return(int64(1000), int64(1002), nil).Once()
	s.alloc.EXPECT().AllocN(int64(1)).Return(int64(1002), int64(1003), nil).Once()
	ci := s.checker.ci.(*MockCompactionInspector)
	ci.EXPECT().enqueueCompaction(mock.MatchedBy(func(plan *datapb.CompactionTask) bool {
		return plan.GetCollectionID() == healthy.GetCollectionID() && len(plan.GetInputSegments()) == 1 && plan.GetInputSegments()[0] == 200
	})).Return(nil).Once()

	// The checker processes jobs serially. A missing collection must return
	// after its first lookup so this next healthy job can be scheduled.
	s.checker.checkSortingJob(dropped)
	failedJob := s.importMeta.GetJob(context.Background(), dropped.GetJobID())
	s.Equal(internalpb.ImportJobState_Failed, failedJob.GetState())
	s.Equal("collection 1 dropped", failedJob.GetReason())
	handler.AssertNumberOfCalls(s.T(), "GetCollection", 1)
	ci.AssertNotCalled(s.T(), "enqueueCompaction", mock.Anything)
	s.alloc.AssertNotCalled(s.T(), "AllocN", mock.Anything)

	s.checker.checkSortingJob(healthy)

	ci.AssertNumberOfCalls(s.T(), "enqueueCompaction", 1)
	handler.AssertNumberOfCalls(s.T(), "GetCollection", 2)
	s.Equal(internalpb.ImportJobState_Sorting, s.importMeta.GetJob(context.Background(), healthy.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckSortingJobRetriesCollectionLookupNextRound() {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "transient error", err: merr.Wrap(merr.ErrServiceNotReady, "RootCoord not ready")},
		{name: "missing metadata without error"},
	} {
		s.Run(tc.name, func() {
			s.SetupTest()
			job := s.newSortingJob(10, 1, 100, 100, 200)
			handler := NewNMockHandler(s.T())
			handler.EXPECT().GetCollection(mock.Anything, job.GetCollectionID()).Return(nil, tc.err).Once()
			s.checker.handler = handler

			s.NotPanics(func() { s.checker.checkSortingJob(job) })

			current := s.importMeta.GetJob(context.Background(), job.GetJobID())
			s.Equal(internalpb.ImportJobState_Sorting, current.GetState())
			s.Empty(current.GetReason())
			handler.AssertNumberOfCalls(s.T(), "GetCollection", 1)
			ci := s.checker.ci.(*MockCompactionInspector)
			ci.AssertNotCalled(s.T(), "enqueueCompaction", mock.Anything)
			s.alloc.AssertNotCalled(s.T(), "AllocN", mock.Anything)

			handler.EXPECT().GetCollection(mock.Anything, job.GetCollectionID()).Return(&collectionInfo{
				ID: job.GetCollectionID(), Schema: job.GetSchema(),
			}, nil).Once()
			s.alloc.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil).Times(4)
			ci.EXPECT().enqueueCompaction(mock.Anything).Return(nil).Twice()

			s.checker.checkSortingJob(current)

			handler.AssertNumberOfCalls(s.T(), "GetCollection", 2)
			ci.AssertNumberOfCalls(s.T(), "enqueueCompaction", 2)
			s.Equal(internalpb.ImportJobState_Sorting, s.importMeta.GetJob(context.Background(), job.GetJobID()).GetState())
		})
	}
}

func (s *ImportCheckerSuite) TestCheckSortingJobSkipsUnneededCollectionLookup() {
	for _, tc := range []struct {
		name          string
		rows          int64
		sorted        bool
		compacting    bool
		expectedState internalpb.ImportJobState
	}{
		{name: "zero row origin", expectedState: internalpb.ImportJobState_IndexBuilding},
		{name: "already sorted", rows: 100, sorted: true, expectedState: internalpb.ImportJobState_IndexBuilding},
		{name: "already compacting", rows: 100, compacting: true, expectedState: internalpb.ImportJobState_Sorting},
	} {
		s.Run(tc.name, func() {
			s.SetupTest()
			job := s.newSortingJob(10, 1, 100, tc.rows)
			handler := NewNMockHandler(s.T())
			s.checker.handler = handler
			catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
			if tc.rows == 0 {
				catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
			}
			if tc.sorted {
				catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
				s.Require().NoError(s.checker.meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
					ID: 200, CollectionID: job.GetCollectionID(), PartitionID: job.GetPartitionIDs()[0],
					InsertChannel: job.GetVchannels()[0], State: commonpb.SegmentState_Flushed,
					NumOfRows: tc.rows, IsImporting: true, IsSorted: true, CompactionFrom: []int64{100},
				})))
			}
			if tc.compacting {
				s.checker.meta.SetSegmentsCompacting(context.Background(), []int64{100}, true)
			}

			s.checker.checkSortingJob(job)

			handler.AssertNotCalled(s.T(), "GetCollection", mock.Anything, mock.Anything)
			s.checker.ci.(*MockCompactionInspector).AssertNotCalled(s.T(), "enqueueCompaction", mock.Anything)
			s.alloc.AssertNotCalled(s.T(), "AllocN", mock.Anything)
			s.Equal(tc.expectedState, s.importMeta.GetJob(context.Background(), job.GetJobID()).GetState())
			if tc.rows == 0 {
				s.Equal(commonpb.SegmentState_Dropped, s.checker.meta.GetSegment(context.Background(), 100).GetState())
			}
		})
	}
}

func (s *ImportCheckerSuite) manuallyUpdateJob(jobID int64, actions ...UpdateJobAction) {
	meta := s.importMeta.(*importMeta)
	meta.mu.Lock()
	defer meta.mu.Unlock()
	current := meta.jobs[jobID].(*importJob)
	cloned := current.Clone().(*importJob)
	for _, action := range actions {
		action(cloned)
	}
	meta.jobs[jobID] = cloned
}

// restampSuiteJobRanges reserves each file's completed preimport row count.
// Legacy fixtures without file identities retain a placeholder range; their
// anonymous stats do not participate in the per-file divergence check.
func (s *ImportCheckerSuite) restampSuiteJobRanges() ImportJob {
	rowsByFile := make(map[int64]int64)
	for _, task := range s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(PreImportTaskType)) {
		for _, stat := range task.GetFileStats() {
			if stat.GetImportFile() != nil {
				rowsByFile[stat.GetImportFile().GetId()] = stat.GetTotalRows()
			}
		}
	}
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		begin := int64(1000)
		for i, f := range job.GetFiles() {
			rows, ok := rowsByFile[f.GetId()]
			if !ok {
				rows = 1000
			}
			job.GetFiles()[i].IdRange = &commonpb.IDRange{Begin: begin, End: begin + rows}
			begin += rows
		}
	})
	return s.importMeta.GetJob(context.TODO(), s.jobID)
}

func (s *ImportCheckerSuite) TestCheckJob_Failed() {
	mockErr := errors.New("mock err")
	job := s.importMeta.GetJob(context.TODO(), s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(mockErr)
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	cancelShutdown()
	s.checker.ctx = shutdownCtx

	s.checker.checkPendingJob(job)
	preimportTasks := s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	s.checker.checkPendingJob(job)
	preimportTasks = s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.ctx = context.Background()

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPendingJob(job)
	preimportTasks = s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkPreImportingJob
	for _, t := range preimportTasks {
		err := s.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats(completedPreImportFileStats(t, 100, 0)))
		s.NoError(err)
	}

	catalog.ExpectedCalls = nil
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(mockErr)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	job = s.restampSuiteJobRanges()
	checkerCtx, cancelChecker := context.WithCancel(context.Background())
	cancelChecker()
	s.checker.ctx = checkerCtx
	s.checker.checkPreImportingJob(job)
	importTasks := s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_Failed, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	s.manuallyUpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	s.checker.checkPreImportingJob(job)
	importTasks = s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil)
	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	s.checker.checkPreImportingJob(job)
	importTasks = s.importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckTimeout() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.PreImportTask{
		TaskID: 1,
		State:  datapb.ImportTaskStateV2_InProgress,
	}
	task := &preImportTask{
		tr: timerecord.NewTimeRecorder("preimport task"),
	}
	task.task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), task)
	s.NoError(err)
	s.checker.tryTimeoutJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Equal("import timeout", job.GetReason())
}

func (s *ImportCheckerSuite) TestTimeoutCatalogFailureFailsStopWhileCheckerAlive() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(errors.New("ambiguous catalog response")).Once()

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	s.checker.ctx = context.Background()
	s.checker.tryTimeoutJob(s.importMeta.GetJob(context.Background(), s.jobID))
	s.True(fatalCalled)
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.Background(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCommittingJobDoesNotTimeout() {
	// Keep the Pending snapshot the checker could have loaded before a commit
	// acknowledgement moved the authoritative job to Committing.
	staleJob := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_Committing))

	s.checker.tryTimeoutJob(staleJob)

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	// Only the state is guarded: UpdateJobState refuses to fail a committed
	// job, while UpdateJobReason in the same UpdateJob call still applies.
	s.Equal(internalpb.ImportJobState_Committing, job.GetState())
}

// runGCLoop evaluates a snapshot taken at the start of the tick. A job that
// entered Committing after the snapshot must still not be failed.
func (s *ImportCheckerSuite) TestCheckTimeoutStaleSnapshotCommittingJob() {
	err := s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobState(internalpb.ImportJobState_Uncommitted))
	s.NoError(err)
	staleSnapshot := s.importMeta.GetJob(context.TODO(), s.jobID)

	err = s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobState(internalpb.ImportJobState_Committing))
	s.NoError(err)

	s.checker.tryTimeoutJob(staleSnapshot)

	// Only the state is asserted: UpdateJobReason in the same UpdateJob call still
	// applies, and reason is surfaced to clients only for Failed jobs.
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Committing, job.GetState())
}

func (s *ImportCheckerSuite) TestUpdateJobStateRefusesFailingCommittedJob() {
	for _, state := range []internalpb.ImportJobState{
		internalpb.ImportJobState_Committing,
		internalpb.ImportJobState_Completed,
	} {
		job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: state}}
		UpdateJobState(internalpb.ImportJobState_Failed)(job)
		s.Equal(state, job.GetState())
	}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Uncommitted}}
	UpdateJobState(internalpb.ImportJobState_Failed)(job)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
}

func (s *ImportCheckerSuite) TestCheckFailure() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:            s.jobID,
		TaskID:           1,
		State:            datapb.ImportTaskStateV2_Pending,
		SegmentIDs:       []int64{2},
		SortedSegmentIDs: []int64{3},
	}
	it := &importTask{
		tr: timerecord.NewTimeRecorder("import task"),
	}
	it.task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), it)
	s.NoError(err)

	s.checker.checkFailedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	tasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
	s.checker.checkFailedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	tasks = s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkFailedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	tasks = s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))
}

func (s *ImportCheckerSuite) TestFailingTaskWaitsForInFlightQuery() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	var (
		savedMu     sync.Mutex
		savedStates []datapb.ImportTaskStateV2
	)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *datapb.ImportTaskV2) {
			savedMu.Lock()
			defer savedMu.Unlock()
			savedStates = append(savedStates, task.GetState())
		}).
		Return(nil).
		Times(3)

	s.manuallyUpdateJob(s.jobID,
		UpdateJobState(internalpb.ImportJobState_Failed),
		UpdateJobReason("import timeout"))
	job := s.importMeta.GetJob(context.Background(), s.jobID)

	const (
		taskID = int64(101)
		nodeID = int64(7)
	)
	importTask := &importTask{
		ctx:        context.Background(),
		meta:       s.checker.meta,
		importMeta: s.importMeta,
		tr:         timerecord.NewTimeRecorder("import task"),
	}
	importTask.task.Store(&datapb.ImportTaskV2{
		JobID:        s.jobID,
		TaskID:       taskID,
		CollectionID: job.GetCollectionID(),
		NodeID:       nodeID,
		State:        datapb.ImportTaskStateV2_InProgress,
	})
	s.NoError(s.importMeta.AddTask(context.Background(), importTask))

	cluster := session.NewMockCluster(s.T())
	queryEntered := make(chan struct{})
	releaseQuery := make(chan struct{})
	cluster.EXPECT().QueryImport(nodeID, mock.Anything).
		RunAndReturn(func(int64, *datapb.QueryImportRequest) (*datapb.QueryImportResponse, error) {
			close(queryEntered)
			<-releaseQuery
			return &datapb.QueryImportResponse{State: datapb.ImportTaskStateV2_Completed}, nil
		}).Once()

	scheduler := datacoordtask.NewGlobalTaskScheduler(context.Background(), cluster)
	defer scheduler.Stop()
	s.checker.scheduler = scheduler

	queryDone := make(chan struct{})
	go func() {
		scheduler.Update(taskID, func() {
			importTask.QueryTaskOnWorker(cluster)
		})
		close(queryDone)
	}()
	select {
	case <-queryEntered:
	case <-time.After(time.Second):
		s.FailNow("query callback did not start")
	}

	failureStarted := make(chan struct{})
	failureDone := make(chan struct{})
	go func() {
		close(failureStarted)
		s.checker.tryFailingTasks(job)
		close(failureDone)
	}()
	<-failureStarted
	interleaved := false
	select {
	case <-failureDone:
		interleaved = true
	case <-time.After(100 * time.Millisecond):
	}
	s.Equal(datapb.ImportTaskStateV2_InProgress, importTask.GetState())

	close(releaseQuery)
	select {
	case <-queryDone:
	case <-time.After(time.Second):
		s.FailNow("query callback did not finish")
	}
	select {
	case <-failureDone:
	case <-time.After(time.Second):
		s.FailNow("failure update did not run after the query callback drained")
	}

	s.False(interleaved, "failure update interleaved with the in-flight query callback")
	s.Equal(datapb.ImportTaskStateV2_Failed, importTask.GetState())
	savedMu.Lock()
	defer savedMu.Unlock()
	s.Equal([]datapb.ImportTaskStateV2{
		datapb.ImportTaskStateV2_InProgress,
		datapb.ImportTaskStateV2_Completed,
		datapb.ImportTaskStateV2_Failed,
	}, savedStates)
}

func (s *ImportCheckerSuite) TestCheckGC() {
	mockErr := errors.New("mock err")

	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:            s.jobID,
		TaskID:           1,
		State:            datapb.ImportTaskStateV2_InProgress,
		SegmentIDs:       []int64{2},
		SortedSegmentIDs: []int64{3},
	}

	task := &importTask{
		tr: timerecord.NewTimeRecorder("import task"),
	}
	task.task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// not failed or completed
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	err = s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobState(internalpb.ImportJobState_Failed))
	s.NoError(err)

	// not reach cleanup ts
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	job.(*importJob).CleanupTs = tsoutil.AddPhysicalDurationOnTs(job.GetCleanupTs(), GCRetention*-2)
	err = s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// origin segment not dropped
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	err = s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateSegmentIDs([]int64{}))
	s.NoError(err)

	// stats segment not dropped
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	err = s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateStatsSegmentIDs([]int64{}))
	s.NoError(err)

	// task is not dropped: it still names a worker, so GC retries the drop the
	// scheduler could not land instead of waiting for someone else to do it.
	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).
		Return(errors.New("connection refused")).Once()
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	err = s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(NullNodeID))
	s.NoError(err)

	// remove task failed
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))

	// remove job failed
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))

	// normal case
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID)))
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

func (s *ImportCheckerSuite) TestCheckGC_FinalizeReloadsAssignedTasks() {
	const (
		preImportTaskID int64 = 1
		importTaskID    int64 = 2
	)
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().DropPreImportTask(mock.Anything, preImportTaskID).Return(nil).Once()
	catalog.EXPECT().DropImportTask(mock.Anything, importTaskID).Return(nil).Once()
	catalog.EXPECT().DropImportJob(mock.Anything, s.jobID).Return(nil).Once()

	preImport := &preImportTask{tr: timerecord.NewTimeRecorder("preimport task")}
	preImport.task.Store(&datapb.PreImportTask{
		JobID:  s.jobID,
		TaskID: preImportTaskID,
		State:  datapb.ImportTaskStateV2_Pending,
		NodeID: NullNodeID,
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), preImport))

	importing := &importTask{tr: timerecord.NewTimeRecorder("import task")}
	importing.task.Store(&datapb.ImportTaskV2{
		JobID:  s.jobID,
		TaskID: importTaskID,
		State:  datapb.ImportTaskStateV2_Pending,
		NodeID: NullNodeID,
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), importing))

	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID,
		UpdateJobState(internalpb.ImportJobState_Completed)))
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	job.(*importJob).CleanupTs = tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour))
	s.NoError(s.importMeta.AddJob(context.TODO(), job))

	scheduler := datacoordtask.NewMockGlobalScheduler(s.T())
	s.checker.scheduler = scheduler
	scheduler.EXPECT().Finalize(preImportTaskID, mock.Anything).Run(func(_ int64, fn func()) {
		// Simulate a Create callback publishing its assignment while Finalize waits.
		s.NoError(s.importMeta.UpdateTask(context.TODO(), preImportTaskID,
			UpdateNodeID(11), UpdateState(datapb.ImportTaskStateV2_InProgress)))
		fn()
	}).Once()
	scheduler.EXPECT().Finalize(importTaskID, mock.Anything).Run(func(_ int64, fn func()) {
		s.NoError(s.importMeta.UpdateTask(context.TODO(), importTaskID,
			UpdateNodeID(12), UpdateState(datapb.ImportTaskStateV2_InProgress)))
		fn()
	}).Once()
	s.cluster.EXPECT().DropImport(int64(11), preImportTaskID).Return(nil).Once()
	s.cluster.EXPECT().DropImport(int64(12), importTaskID).Return(nil).Once()

	s.checker.checkGC(job)
	s.Nil(s.importMeta.GetTask(context.TODO(), preImportTaskID))
	s.Nil(s.importMeta.GetTask(context.TODO(), importTaskID))
	s.Nil(s.importMeta.GetJob(context.TODO(), s.jobID))
}

// setupGCReadyFailedJob puts the suite's job into a Failed, past-cleanup-ts state with
// a single import task that has no live segments and is unassigned, so checkGC is one
// step away from removing the job (the only remaining gate is the replicate rollback).
func (s *ImportCheckerSuite) setupGCReadyFailedJob() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:  s.jobID,
		TaskID: 1,
		State:  datapb.ImportTaskStateV2_Failed,
		NodeID: NullNodeID,
	}
	task := &importTask{tr: timerecord.NewTimeRecorder("import task")}
	task.task.Store(taskProto)
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobState(internalpb.ImportJobState_Failed)))
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	job.(*importJob).CleanupTs = tsoutil.AddPhysicalDurationOnTs(job.GetCleanupTs(), GCRetention*-2)
	s.NoError(s.importMeta.AddJob(context.TODO(), job))
}

// A failed source in a replicating cluster must broadcast RollbackImport before its job
// is GC'd. A transient broadcast error keeps the job alive to retry; once it succeeds the
// job is removed.
func (s *ImportCheckerSuite) TestCheckGCReplicateSourceBroadcastsRollback() {
	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	rollbackCalls := 0
	rollbackErr := errors.New("broadcast failed")
	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		rollbackCalls++
		return rollbackErr
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	// First tick: broadcast fails → job retained (tasks already removed), rollback attempted.
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, rollbackCalls)
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))

	// Next tick: broadcast succeeds → job removed.
	rollbackErr = nil
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(2, rollbackCalls)
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

// A standby (not the replication primary) gets ErrNotPrimary from the broadcast; that is
// treated as success so its own failed job is still GC'd.
func (s *ImportCheckerSuite) TestCheckGCReplicateNotPrimaryProceeds() {
	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error { return broadcaster.ErrNotPrimary }
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

// A non-replicating cluster must not broadcast any rollback; the failed job is GC'd as before.
func (s *ImportCheckerSuite) TestCheckGCNonReplicatingSkipsRollback() {
	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	rollbackCalls := 0
	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		rollbackCalls++
		return nil
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, false, nil
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, rollbackCalls)
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

// The replication check reaches the streaming balancer future, which blocks until the
// balancer is registered; checkGC must pass a deadline-bounded ctx so an unregistered
// balancer (datacoord ready before streamingcoord) cannot park the whole checker loop.
func (s *ImportCheckerSuite) TestCheckGCReplicateCheckCtxIsBounded() {
	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error { return nil }
	hasDeadline := false
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		_, hasDeadline = ctx.Deadline()
		return replicateutil.RolePrimary, false, nil
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.True(hasDeadline)
}

// When the replication status cannot be determined (e.g. a transient balancer error during
// shutdown), GC must NOT drop the job: a false "not replicating" would strand a replicating
// peer with no recovery path. The job is retained and no rollback is broadcast.
func (s *ImportCheckerSuite) TestCheckGCReplicateIndeterminateRetainsJob() {
	s.setupGCReadyFailedJob()
	// The task-cleanup loop runs before the replication gate, so the task is removed
	// even though the job itself is retained; DropImportJob must NOT be called.
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)

	rollbackCalls := 0
	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		rollbackCalls++
		return nil
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, false, errors.New("balancer not ready")
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, rollbackCalls)
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
}

// A permanent rollback error (e.g. the collection was dropped → ErrCollectionNotFound) must
// NOT be retried forever, which would leak the job's metadata. The job is GC'd instead.
func (s *ImportCheckerSuite) TestCheckGCReplicatePermanentRollbackErrRemovesJob() {
	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	rollbackCalls := 0
	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		rollbackCalls++
		return merr.WrapErrCollectionNotFound(job.GetCollectionID())
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, rollbackCalls)
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

// The rollback broadcast can block until every vchannel append succeeds (or forever on
// an unavailable streamingnode) under the server-lifetime c.ctx; checkGC must pass a
// deadline-bounded ctx so a stuck broadcast cannot park the whole checker loop.
func (s *ImportCheckerSuite) TestCheckGCReplicateRollbackCtxIsBounded() {
	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	hasDeadline := false
	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		_, hasDeadline = ctx.Deadline()
		return nil
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.True(hasDeadline)
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

// A job without vchannels can never deliver its rollback (Vchannels are fixed at
// creation), so the error must be classified permanent and the job GC'd — while a
// generic ImportSysFailed error must stay transient.
func (s *ImportCheckerSuite) TestCheckGCReplicateNoVchannelsRollbackErrRemovesJob() {
	server := &Server{}
	err := server.broadcastRollbackImportMessage(context.TODO(), &importJob{ImportJob: &datapb.ImportJob{JobID: 1}})
	s.Error(err)
	s.True(isPermanentRollbackErr(err))
	s.False(isPermanentRollbackErr(merr.WrapErrImportSysFailedMsg("some transient failure")))

	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		return server.broadcastRollbackImportMessage(ctx, &importJob{ImportJob: &datapb.ImportJob{JobID: job.GetJobID()}})
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
}

// The GC loop and the state-machine loop must run on separate goroutines: a
// rollback broadcast parked on the ctx-insensitive resource-key lock (or any
// other stall inside checkGC) must delay only GC, never the import state
// machine. This test parks checkGC's rollback forever and asserts the state
// machine still processes another job meanwhile.
func (s *ImportCheckerSuite) TestStateMachineProgressesWhileGCRollbackParked() {
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ImportCheckIntervalHigh.Key, "0.05")
	params.Save(params.DataCoordCfg.ImportCheckIntervalLow.Key, "0.05")
	defer params.Reset(params.DataCoordCfg.ImportCheckIntervalHigh.Key)
	defer params.Reset(params.DataCoordCfg.ImportCheckIntervalLow.Key)

	s.setupGCReadyFailedJob()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	// Teardown releases the parked rollback, letting the in-flight checkGC run to
	// completion (RemoveJob, then checkCollection) while the loops shut down.
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.checker.broker.(*broker2.MockBroker).EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil).Maybe()

	rollbackEntered := make(chan struct{}, 1)
	rollbackRelease := make(chan struct{})
	s.checker.hooks.rollbackImport = func(ctx context.Context, job ImportJob) error {
		select {
		case rollbackEntered <- struct{}{}:
		default:
		}
		// Park forever, ignoring ctx — simulates the ctx-insensitive lock.
		<-rollbackRelease
		return nil
	}
	s.checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

	go s.checker.Start()
	defer s.checker.Close()
	defer close(rollbackRelease)

	// Wait until the GC loop is parked inside the rollback broadcast.
	select {
	case <-rollbackEntered:
	case <-time.After(10 * time.Second):
		s.FailNow("GC loop never reached the rollback broadcast")
	}

	// Feed the state machine a Failed job with a live task; only the
	// state-machine loop (checkFailedJob → tryFailingTasks) can fail the task.
	jobB := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          s.jobID + 100,
			CollectionID:   1,
			Vchannels:      []string{"ch1"},
			ReadyVchannels: []string{"ch1"},
			State:          internalpb.ImportJobState_Failed,
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(24 * time.Hour)), // never GC-ready
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	s.NoError(s.importMeta.AddJob(context.TODO(), jobB))
	taskProto := &datapb.ImportTaskV2{
		JobID:  jobB.GetJobID(),
		TaskID: 999,
		State:  datapb.ImportTaskStateV2_Pending,
		NodeID: NullNodeID,
	}
	task := &importTask{tr: timerecord.NewTimeRecorder("import task")}
	task.task.Store(taskProto)
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	s.Eventually(func() bool {
		tasks := s.importMeta.GetTaskByJob(context.TODO(), jobB.GetJobID())
		return len(tasks) == 1 && tasks[0].GetState() == datapb.ImportTaskStateV2_Failed
	}, 10*time.Second, 20*time.Millisecond)
}

func (s *ImportCheckerSuite) TestCheckCollection() {
	mockErr := errors.New("mock err")

	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.PreImportTask{
		JobID:  s.jobID,
		TaskID: 1,
		State:  datapb.ImportTaskStateV2_Pending,
	}
	task := &preImportTask{
		tr: timerecord.NewTimeRecorder("preimport task"),
	}
	task.task.Store(taskProto)
	err := s.importMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// no jobs
	s.checker.checkCollection(1, []ImportJob{})
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// collection exist
	broker := s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil)
	s.checker.checkCollection(1, []ImportJob{s.importMeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// HasCollection failed
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, mockErr)
	s.checker.checkCollection(1, []ImportJob{s.importMeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// SaveImportJob failed
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(mockErr)
	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	s.checker.checkCollection(1, []ImportJob{s.importMeta.GetJob(context.TODO(), s.jobID)})
	mockFatal.UnPatch()
	s.True(fatalCalled)
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// collection dropped
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkCollection(1, []ImportJob{s.importMeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Failed, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func TestImportChecker(t *testing.T) {
	suite.Run(t, new(ImportCheckerSuite))
}

func TestImportCheckerCompaction(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.DataCoordCfg.ImportCheckIntervalHigh.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.ImportCheckIntervalHigh.Key)
	Params.Save(Params.DataCoordCfg.ImportCheckIntervalLow.Key, "10000")
	defer Params.Reset(Params.DataCoordCfg.ImportCheckIntervalLow.Key)

	// prepare objects
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	alloc := allocator.NewMockAllocator(t)

	broker := broker2.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	importMeta, err := NewImportMeta(context.TODO(), catalog, alloc, meta)
	assert.NoError(t, err)

	cim := NewMockCompactionInspector(t)
	handler := NewNMockHandler(t)

	checker := NewImportChecker(context.TODO(), meta, broker, alloc, importMeta, cim, handler, session.NewMockCluster(t), nil, importCheckerHooks{
		getReplicationRole: func(context.Context) (replicateutil.Role, bool, error) {
			return replicateutil.RolePrimary, false, nil
		},
		assignImportIDRange: func(ctx context.Context, job ImportJob, fileRows []int64) error {
			// Mirror the production wiring: the primary allocates one exact range per file
			// and applies it to the job, which is what lets the range gate proceed.
			ranges := make([]*commonpb.IDRange, len(fileRows))
			begin := int64(100000)
			for i, rows := range fileRows {
				ranges[i] = &commonpb.IDRange{Begin: begin, End: begin + rows}
				begin += rows
			}
			return importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobIDRanges(ranges))
		},
	}).(*importChecker)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          1001,
			CollectionID:   1,
			PartitionIDs:   []int64{2},
			ReadyVchannels: []string{"ch0"},
			Vchannels:      []string{"ch0", "ch1"},
			State:          internalpb.ImportJobState_Pending,
			TimeoutTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
				},
			},
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
				{
					Id:    2,
					Paths: []string{"b.json"},
				},
				{
					Id:    3,
					Paths: []string{"c.json"},
				},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	err = importMeta.AddJob(context.TODO(), job)
	assert.NoError(t, err)
	jobID := job.GetJobID()

	// start check
	go checker.Start()

	// sleep 1.5s and ready the job, go to pending stats
	time.Sleep(1500 * time.Millisecond)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	job2 := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          1001,
			CollectionID:   1,
			PartitionIDs:   []int64{2},
			ReadyVchannels: []string{"ch1"},
			Vchannels:      []string{"ch0", "ch1"},
			State:          internalpb.ImportJobState_Pending,
			TimeoutTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
				},
			},
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
				{
					Id:    2,
					Paths: []string{"b.json"},
				},
				{
					Id:    3,
					Paths: []string{"c.json"},
				},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = importMeta.AddJob(context.TODO(), job2)
	assert.NoError(t, err)
	mlog.Info(context.TODO(), "job ready")

	// check pending
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	}).Maybe()
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil).Maybe()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		preimportTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
		taskLen := len(preimportTasks)
		mlog.Info(context.TODO(), "job pre-importing", mlog.Any("taskLen", taskLen), mlog.Any("jobState", job.GetState()))
		return taskLen == 2 && job.GetState() == internalpb.ImportJobState_PreImporting
	}, 5*time.Second, 500*time.Millisecond)
	mlog.Info(context.TODO(), "job pre-importing")

	// check pre-importing
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	// The gate writes the job three times here: the AssigningIDRange transition, the ranges
	// the assign hook applies, and the Importing transition.
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	preimportTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	// Complete every preimport task with one exact count per file, carrying the file id like
	// the datanode report does: the range gate needs a row count per job file.
	for _, pt := range preimportTasks {
		err := importMeta.UpdateTask(context.TODO(), pt.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats(completedPreImportFileStats(pt, 100, 0)))
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		importTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
		return len(importTasks) == 1 && job.GetState() == internalpb.ImportJobState_Importing
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job importing")

	// check importing
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	// AlterSegments was previously driven by unsetSegmentImporting (removed in 2PC);
	// the remaining segment writes in this flow may or may not hit it.
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	// Maybe, not Once: the open-ended SaveImportJob expectation set for the pre-importing
	// gate window matches every later call first, so a later Once() would never be consumed.
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	importTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	targetSegmentIDs := make([]int64, 0)
	for _, it := range importTasks {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            rand.Int63(),
				CollectionID:  job.GetCollectionID(),
				PartitionID:   job.GetPartitionIDs()[0],
				State:         commonpb.SegmentState_Flushed,
				IsImporting:   true,
				InsertChannel: "ch0",
			},
		}
		err := checker.meta.AddSegment(context.Background(), segment)
		assert.NoError(t, err)
		targetSegmentID := rand.Int63()
		err = importMeta.UpdateTask(context.TODO(), it.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}), UpdateStatsSegmentIDs([]int64{targetSegmentID}))
		assert.NoError(t, err)
		targetSegmentIDs = append(targetSegmentIDs, targetSegmentID)
		err = checker.meta.UpdateChannelCheckpoint(context.TODO(), segment.GetInsertChannel(), &msgpb.MsgPosition{MsgID: []byte{0}})
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Sorting
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job stats")

	// check stats
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	for i, targetSegmentID := range targetSegmentIDs {
		originSegmentID := importTasks[i].(*importTask).GetSegmentIDs()[0]
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             targetSegmentID,
				CollectionID:   job.GetCollectionID(),
				PartitionID:    job.GetPartitionIDs()[0],
				State:          commonpb.SegmentState_Flushed,
				IsImporting:    true,
				InsertChannel:  "ch0",
				IsSorted:       true,
				CompactionFrom: []int64{originSegmentID},
			},
		}
		err := checker.meta.AddSegment(context.Background(), segment)
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_IndexBuilding
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job index building")

	// check index building → Uncommitted (2PC: no longer transitions directly to Completed;
	// the test does not wire up a CommitImport broadcaster, so the job stops at Uncommitted).
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Uncommitted
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job uncommitted (awaiting CommitImport WAL fence)")
}

// ---------------------------------------------------------------------------
// Tests for checkUncommittedJob
// ---------------------------------------------------------------------------

func (s *ImportCheckerSuite) TestCheckUncommittedJob_AutoCommitTrue() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)

	// Put the job into Uncommitted state with auto_commit=true (default).
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = true
	})

	commitCalled := false
	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error {
		commitCalled = true
		s.Equal(internalpb.ImportJobState_Uncommitted, job.GetState())
		return s.importMeta.UpdateJob(ctx, job.GetJobID(),
			UpdateJobState(internalpb.ImportJobState_Committing))
	}

	s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.True(commitCalled, "commit hook should be called when auto_commit=true")
	s.Equal(internalpb.ImportJobState_Committing, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckUncommittedJob_AutoCommitFalse() {
	// Put the job into Uncommitted state with auto_commit=false.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = false
	})

	commitCalled := false
	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error {
		commitCalled = true
		return nil
	}

	s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.False(commitCalled, "commit hook must NOT be called when auto_commit=false")
	// Job state must remain Uncommitted.
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckUncommittedJob_NilFn_AutoCommitTrue() {
	// commit hook=nil with auto_commit=true is a programming error; the checker
	// must log an error and return without crashing (no panic in the ticker goroutine).
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = true
	})
	s.checker.hooks.commitImport = nil

	s.NotPanics(func() {
		s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	})
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckUncommittedJob_RepeatedTicks_Safe() {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = true
	})
	callCount := 0
	s.checker.hooks.commitImport = func(context.Context, ImportJob) error {
		callCount++
		return nil
	}
	for i := 0; i < 3; i++ {
		s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	}
	s.Equal(3, callCount, "each tick retries until the commit hook persists its intent")
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckUncommittedJob_NoVchannels() {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = true
		job.(*importJob).Vchannels = nil
	})

	commitCalled := false
	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error {
		commitCalled = true
		return nil
	}

	s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.False(commitCalled)
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"an undeliverable commit must not create a durable commit intent")
}

// TestCheckCommittingJob_ReplaysBroadcast verifies that a durable commit intent
// closes the crash/failure window before broadcaster task creation: subsequent
// checker ticks replay the broadcast until every vchannel is committed.
func (s *ImportCheckerSuite) TestCheckCommittingJob_ReplaysBroadcast() {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = true
		job.(*importJob).Vchannels = []string{"ch0"}
		job.(*importJob).CommittedVchannels = nil
	})

	callCount := 0
	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error {
		if job.GetState() == internalpb.ImportJobState_Uncommitted {
			s.NoError(s.importMeta.UpdateJob(ctx, job.GetJobID(),
				UpdateJobState(internalpb.ImportJobState_Committing)))
		}
		callCount++
		return nil
	}

	s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	for i := 0; i < 2; i++ {
		s.checker.checkCommittingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	}
	s.Equal(3, callCount, "each Committing tick must replay the commit broadcast")
	s.Equal(internalpb.ImportJobState_Committing, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

// ---------------------------------------------------------------------------
// Tests for checkCommittingJob
// ---------------------------------------------------------------------------

func (s *ImportCheckerSuite) TestCheckCommittingJob_CoordinatorOwnedReplaysWithoutCompleting() {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Committing
		job.(*importJob).CommitByCoordinator = true
		job.(*importJob).Vchannels = []string{"ch0"}
		job.(*importJob).CommittedVchannels = []string{"ch0"}
	})
	calls := 0
	s.checker.hooks.commitImport = func(context.Context, ImportJob) error {
		calls++
		return errors.New("broadcast not persisted yet")
	}
	s.checker.checkCommittingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, calls, "a persisted intent must recover a crash before broadcast persistence")
	s.Equal(internalpb.ImportJobState_Committing, s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"coordinator-owned jobs complete only in the broadcast callback")
}

func (s *ImportCheckerSuite) TestCheckCommittingJob_AllVchannelsDone() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	// All vchannels committed → expect transition to Completed.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Committing
		job.(*importJob).Vchannels = []string{"ch0"}
		job.(*importJob).CommittedVchannels = []string{"ch0"}
	})

	s.checker.checkCommittingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_Completed, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckCommittingJob_NoVchannelsDoesNotComplete() {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Committing
		job.(*importJob).Vchannels = nil
		job.(*importJob).CommittedVchannels = nil
	})

	s.checker.checkCommittingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_Committing, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckCommittingJob_Partial() {
	// Only some vchannels committed → job should stay Committing.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Committing
		job.(*importJob).Vchannels = []string{"ch0", "ch1"}
		job.(*importJob).CommittedVchannels = []string{"ch0"}
	})

	replayed := false
	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error {
		replayed = true
		return nil
	}
	s.checker.checkCommittingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.True(replayed)
	s.Equal(internalpb.ImportJobState_Committing, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckCommittingJob_WrongVchannelWithSameCount() {
	// Equal slice lengths are insufficient: an unexpected vchannel must not
	// stand in for an expected acknowledgement.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Committing
		job.(*importJob).Vchannels = []string{"ch0", "ch1"}
		job.(*importJob).CommittedVchannels = []string{"ch0", "ch2"}
	})

	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error { return nil }
	s.checker.checkCommittingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_Committing, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

// ---------------------------------------------------------------------------
// Tests for checkPreImportingJob — empty-import fast path
// ---------------------------------------------------------------------------

func (s *ImportCheckerSuite) TestCheckPreImporting_EmptyImport_AutoCommitFalse() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)

	// First, advance job to PreImporting by creating pre-import tasks.
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPendingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// Mark all pre-import tasks completed with totalRows == 0 (empty import).
	preimportTasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(PreImportTaskType))
	for _, t := range preimportTasks {
		err := s.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats([]*datapb.ImportFileStats{{TotalRows: 0}}))
		s.NoError(err)
	}

	// Set auto_commit=false on the job.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).AutoCommit = false
	})

	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	// The range gate runs before the zero-rows exit: a locally empty ranged job still
	// has to match the peer's ranges, so the first tick parks it in AssigningIDRange.
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// Apply the zero-width ranges the ack callback would deliver, then run the gate tail.
	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 0, End: 0}, {Begin: 0, End: 0}, {Begin: 0, End: 0},
	})))
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	// With auto_commit=false, empty import should land in Uncommitted, not Completed.
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

func (s *ImportCheckerSuite) TestCheckPreImporting_EmptyImport_AutoCommitTrue() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)

	// First, advance job to PreImporting.
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPendingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// Mark all pre-import tasks completed with totalRows == 0 (empty import).
	preimportTasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(PreImportTaskType))
	for _, t := range preimportTasks {
		err := s.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats([]*datapb.ImportFileStats{{TotalRows: 0}}))
		s.NoError(err)
	}

	// auto_commit=true (the default), so job should go directly to Completed.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).AutoCommit = true
	})

	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	// Same gate-before-zero-rows order as the auto_commit=false case.
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 0, End: 0}, {Begin: 0, End: 0}, {Begin: 0, End: 0},
	})))
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	s.Equal(internalpb.ImportJobState_Completed, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}

// ---------------------------------------------------------------------------
// Helpers (newCapturingImportMeta is shared with ddl_callbacks_import_test.go)
// ---------------------------------------------------------------------------

// newCapturingImportMeta builds an importMeta whose catalog records every proto
// handed to SaveImportJob, so tests can assert what was actually persisted.
func newCapturingImportMeta(t *testing.T, saved *[]*datapb.ImportJob) ImportMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, job *datapb.ImportJob) error {
			*saved = append(*saved, job)
			return nil
		}).Maybe()
	alloc := allocator.NewMockAllocator(t)
	importMeta, err := NewImportMeta(context.Background(), catalog, alloc, nil)
	require.NoError(t, err)
	return importMeta
}

// enableGateMocks sets the recurring catalog/allocator expectations the gate
// tests need: preimport task saves (pending -> completed) and open-ended AllocN.
func (s *ImportCheckerSuite) enableGateMocks() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	}).Maybe() // tests that add tasks manually never reach the allocator
}

// driveToPreImportDone runs checkPendingJob for the given job (creating one
// preimport task per file group) and completes every task with exact per-file
// row counts, reproducing the state the gate observes in production: all
// PreImport tasks completed, TotalRows present in each ImportFileStats.
func (s *ImportCheckerSuite) driveToPreImportDone(jobID int64, rowsByFile map[int64]int64) {
	ctx := context.TODO()
	s.checker.checkPendingJob(s.importMeta.GetJob(ctx, jobID))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(ctx, jobID).GetState())

	tasks := s.importMeta.GetTaskByJob(ctx, jobID, WithType(PreImportTaskType))
	s.NotEmpty(tasks)
	for _, t := range tasks {
		// Replace the task's fileStats with fresh protos carrying the exact counts,
		// like the datanode report does (also un-aliasing them from job.GetFiles()).
		stats := make([]*datapb.ImportFileStats, 0, len(t.GetFileStats()))
		for _, st := range t.GetFileStats() {
			f := st.GetImportFile()
			stats = append(stats, &datapb.ImportFileStats{
				ImportFile: &internalpb.ImportFile{Id: f.GetId(), Paths: f.GetPaths()},
				TotalRows:  rowsByFile[f.GetId()],
			})
		}
		s.NoError(s.importMeta.UpdateTask(ctx, t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed), UpdateFileStats(stats)))
	}
}

// setupAutoIDPreImportDone flips the suite job's PK to autoID and drives it to
// "all preimport tasks completed" with the given exact per-file row counts
// (keyed by the suite job's fileIDs 1, 2, 3).
func (s *ImportCheckerSuite) setupAutoIDPreImportDone(rowsByFile map[int64]int64) {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).Schema.Fields[0].AutoID = true
	})
	s.driveToPreImportDone(s.jobID, rowsByFile)
}

// gateTestSchema builds a PK-only schema; autoID toggles the gate predicate.
func gateTestSchema(autoID bool) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
				AutoID:       autoID,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Gate: predicate helpers
// ---------------------------------------------------------------------------

func (s *ImportCheckerSuite) TestNeedsIDRanges() {
	cases := []struct {
		name    string
		schema  *schemapb.CollectionSchema
		options []*commonpb.KeyValuePair
		want    bool
	}{
		{"autoID", gateTestSchema(true), nil, true},
		{"non-autoID", gateTestSchema(false), nil, true},
		{
			"autoID backup", gateTestSchema(true),
			[]*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}},
			false,
		},
		{
			"non-autoID backup", gateTestSchema(false),
			[]*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}},
			false,
		},
		{
			"autoID backup=false", gateTestSchema(true),
			[]*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "false"}},
			true,
		},
		{
			"autoID L0", gateTestSchema(true),
			[]*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
			false,
		},
		{
			"non-autoID L0", gateTestSchema(false),
			[]*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
			false,
		},
		{"no primary key", &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
		}}, nil, false},
	}
	for _, tc := range cases {
		job := &importJob{ImportJob: &datapb.ImportJob{Schema: tc.schema, Options: tc.options}}
		s.Equal(tc.want, needsIDRanges(job), tc.name)
	}
}

// jobIDRangesSet is a nil check, not End>Begin: a zero-row file legitimately
// carries an empty (Begin==End) range that still counts as set.
// A locally empty side must still compare its counts against the peer's ranges. It enters
// the gate like any other ranged job, and once the peer's ranges arrive with rows it fails
// instead of taking the zero-rows exit and committing an empty import.
func (s *ImportCheckerSuite) TestIDRangeGate_ZeroRowsSideFailsOnPeerRanges() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 0, 2: 0, 3: 0})

	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"a zero-row ranged job waits for the ranges like any other")

	// The peer counted 100 rows for file 1 and none for the rest.
	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 5000, End: 5100},
		{Begin: 5100, End: 5100},
		{Begin: 5100, End: 5100},
	})))

	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "local row count 0")
	s.Contains(job.GetReason(), "cross-cluster file divergence")
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))),
		"the job must fail before any Import task is created")
}

// Both sides empty: the ranges are zero-width, the counts match, and the job keeps the
// zero-rows exit (Uncommitted for a 2PC import) instead of creating Import tasks.
func (s *ImportCheckerSuite) TestIDRangeGate_ZeroRowsOnBothSidesProceeds() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 0, 2: 0, 3: 0})

	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 5000, End: 5000},
		{Begin: 5000, End: 5000},
		{Begin: 5000, End: 5000},
	})))

	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Uncommitted, job.GetState(),
		"zero rows on both sides keep the zero-rows exit, now reached through the gate")
	s.Empty(job.GetReason())
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))))
}

func TestJobIDRangesSet_NilCheckNotEmptyCheck(t *testing.T) {
	job := &importJob{ImportJob: &datapb.ImportJob{Files: []*internalpb.ImportFile{
		{Id: 1, IdRange: &commonpb.IDRange{Begin: 5, End: 5}}, // empty range counts as set
		{Id: 2},
	}}}
	assert.False(t, jobIDRangesSet(job))
	job.GetFiles()[1].IdRange = &commonpb.IDRange{Begin: 5, End: 9}
	assert.True(t, jobIDRangesSet(job))
}

// ---------------------------------------------------------------------------
// Gate: primary path (Test Plan item 1)
// ---------------------------------------------------------------------------

// Preimport done, no ranges, primary role (or non-replicating, or role hook
// disabled): the first tick moves the job PreImporting → AssigningIDRange without
// broadcasting; later ticks in AssigningIDRange invoke assignImportIDRange with the
// exact per-file row counts aligned to job.GetFiles() order -- including a zero-row
// file -- under a bounded ctx, and the job stays in AssigningIDRange until the ranges
// are applied.
func (s *ImportCheckerSuite) TestIDRangeGate_PrimaryAssignsWithAlignedFileRows() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	calls := 0
	var gotJobID int64
	var gotFileRows []int64
	var deadlineOK bool
	s.checker.hooks.assignImportIDRange = func(ctx context.Context, job ImportJob, fileRows []int64) error {
		calls++
		gotJobID = job.GetJobID()
		gotFileRows = fileRows
		deadline, hasDeadline := ctx.Deadline()
		deadlineOK = hasDeadline && time.Until(deadline) > 0 && time.Until(deadline) <= 10*time.Second
		return nil
	}

	// Tick 1: the transition itself never consults the role nor broadcasts.
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		s.Fail("PreImporting → AssigningIDRange must not consult the replication role")
		return replicateutil.RolePrimary, false, nil
	}
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, calls, "the transition tick must not broadcast")
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Empty(s.importMeta.GetJob(context.TODO(), s.jobID).GetReason())

	// A nil getReplicationRole hook means "not replicating -> primary" (the
	// feature-disabled default); an explicit primary role -- replicating or not
	// -- takes the same path.
	roles := []struct {
		name string
		set  func()
	}{
		{"nil role hook", func() { s.checker.hooks.getReplicationRole = nil }},
		{"not replicating", func() {
			s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
				return replicateutil.RolePrimary, false, nil
			}
		}},
		{"replicating primary", func() {
			s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
				return replicateutil.RolePrimary, true, nil
			}
		}},
	}
	for i, r := range roles {
		r.set()
		s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
		s.Equal(i+1, calls, r.name+": each tick without applied ranges retries the broadcast")
		s.Equal(s.jobID, gotJobID)
		// Aligned with job.GetFiles() order (fileIDs 1,2,3), zero-row file included.
		s.Equal([]int64{100, 0, 200}, gotFileRows, r.name)
		s.True(deadlineOK, r.name+": hook ctx must be bounded (~10s)")

		job := s.importMeta.GetJob(context.TODO(), s.jobID)
		s.Equal(internalpb.ImportJobState_AssigningIDRange, job.GetState(), r.name)
		s.Empty(job.GetReason(), r.name)
		s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))),
			r.name+": no Import task may be created before the ranges are applied")
	}
}

// ---------------------------------------------------------------------------
// Gate: secondary path (Test Plan item 2)
// ---------------------------------------------------------------------------

// A secondary never allocates: it waits for the replicated UpdateImport. Once
// the ack callback (simulated here via UpdateJobIDRanges) applies the primary's
// ranges, the next tick passes the gate: Import tasks are created and the
// ranges are stamped onto the task fileStats (ImportTaskV2 meta carries
// IdRange).
func (s *ImportCheckerSuite) TestIDRangeGate_SecondaryWaitsThenProceeds() {
	s.enableGateMocks()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil)
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	roleCalls, assignCalls := 0, 0
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		roleCalls++
		return replicateutil.RoleSecondary, true, nil
	}
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	// Tick 1: PreImporting → AssigningIDRange, without consulting the role.
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, roleCalls, "the transition tick must not consult the replication role")
	s.Equal(0, assignCalls)
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// Tick 2: secondary waits -- no allocation, no broadcast, stays AssigningIDRange.
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, roleCalls)
	s.Equal(0, assignCalls, "a secondary must never allocate")
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// Tick 3: still waiting; nothing changes (each tick only logs at Debug).
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, assignCalls)
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))))

	// The replicated UpdateImport ack applies the primary's ranges (what
	// updateImportAckCallback does via UpdateJobIDRanges). Each range is sized to its
	// file's exact row count.
	ranges := []*commonpb.IDRange{
		{Begin: 5000, End: 5100}, // file 1: 100 rows
		{Begin: 5100, End: 5100}, // file 2: zero-row file, empty range
		{Begin: 5100, End: 5300}, // file 3: 200 rows
	}
	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges(ranges)))

	// Tick 3: gate passes -> Import tasks created, ranges stamped onto task fileStats.
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, assignCalls, "ranges arrived via replication; no local allocation ever")
	s.Equal(2, roleCalls, "gate satisfied -> role is no longer consulted")

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Importing, job.GetState())

	importTasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))
	s.NotEmpty(importTasks)
	wantByFileID := map[int64]*commonpb.IDRange{1: ranges[0], 2: ranges[1], 3: ranges[2]}
	seen := make(map[int64]bool)
	for _, t := range importTasks {
		for _, stat := range t.GetFileStats() {
			f := stat.GetImportFile()
			want := wantByFileID[f.GetId()]
			s.NotNil(want, "task file %d must belong to the job", f.GetId())
			got := f.GetIdRange()
			s.NotNil(got, "range must be stamped onto ImportTaskV2 meta for file %d", f.GetId())
			s.Equal(want.GetBegin(), got.GetBegin(), "file %d", f.GetId())
			s.Equal(want.GetEnd(), got.GetEnd(), "file %d", f.GetId())
			seen[f.GetId()] = true
		}
	}
	for fileID := range wantByFileID {
		s.True(seen[fileID], "every job file must appear in some Import task")
	}
}

// ---------------------------------------------------------------------------
// Gate: indeterminate role (Test Plan item 3)
// ---------------------------------------------------------------------------

// An indeterminate replication role must NOT allocate -- a secondary that
// allocated would diverge from the primary's authoritative range. No broadcast,
// job waits in AssigningIDRange (reached from PreImporting without consulting the
// role at all).
func (s *ImportCheckerSuite) TestIDRangeGate_IndeterminateRoleWaits() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	assignCalls := 0
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, false, errors.New("balancer not ready")
	}
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, assignCalls, "an indeterminate role must never reach allocation")
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_AssigningIDRange, job.GetState())
	s.Empty(job.GetReason())
}

// ---------------------------------------------------------------------------
// Gate: assign hook errors are transient (Test Plan item 4)
// ---------------------------------------------------------------------------

// ErrNotPrimary (stale role during switchover) is tolerated as "wait"; any
// other error is logged and retried next tick. Neither fails the job nor
// creates Import tasks.
func (s *ImportCheckerSuite) TestIDRangeGate_AssignErrorsAreTransient() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	calls := 0
	hookErr := error(nil)
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		calls++
		return hookErr
	}

	// Reach the waiting state first; the errors below all happen on AssigningIDRange ticks.
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	errCases := []struct {
		name string
		err  error
	}{
		{"ErrNotPrimary tolerated as wait", broadcaster.ErrNotPrimary},
		{"wrapped ErrNotPrimary", errors.Wrap(broadcaster.ErrNotPrimary, "append rejected")},
		{"generic error retried", errors.New("wal unavailable")},
	}
	for _, tc := range errCases {
		hookErr = tc.err
		s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
		job := s.importMeta.GetJob(context.TODO(), s.jobID)
		s.Equal(internalpb.ImportJobState_AssigningIDRange, job.GetState(), tc.name)
		s.Empty(job.GetReason(), tc.name+": a transient broadcast error must not fail the job")
		s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))), tc.name)
	}
	s.Equal(len(errCases), calls, "every tick must retry the broadcast")
}

// An input-class error from the assign hook is permanent: the request content itself
// cannot be ranged (a single file above the per-allocation ceiling), so no retry fixes it.
// The job must fail immediately with the precise reason, instead of retrying every tick
// until the timeout replaces it with a generic message.
func (s *ImportCheckerSuite) TestIDRangeGate_PermanentAssignErrorFailsJob() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	calls := 0
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		calls++
		return merr.WrapErrParameterInvalidMsg(
			"import file 0 holds 5000000000 rows, more than one allocation batch can reserve (max 4294967295); split the file")
	}

	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "split the file", "the precise reason must survive")
	s.Equal(1, calls, "a permanent error must not be retried")
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))),
		"no Import task may be created for a failed job")
}

// ---------------------------------------------------------------------------
// Gate: excluded jobs proceed without the hook (Test Plan item 5)
// ---------------------------------------------------------------------------

// Backup and L0 jobs do not need per-file ranges: each proceeds straight to
// Importing without the role or assign hooks ever firing.
func (s *ImportCheckerSuite) TestIDRangeGate_ExcludedJobsProceedWithoutHook() {
	s.enableGateMocks()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil)

	roleCalls, assignCalls := 0, 0
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		roleCalls++
		return replicateutil.RolePrimary, false, nil
	}
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	variants := []struct {
		name    string
		autoID  bool
		options []*commonpb.KeyValuePair
	}{
		{"backup", true, []*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}}},
		{"L0", true, []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}}},
	}
	for i, v := range variants {
		jobID := int64(900 + i)
		job := &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:        jobID,
				CollectionID: 1,
				PartitionIDs: []int64{2},
				Vchannels:    []string{"ch0"},
				State:        internalpb.ImportJobState_Pending,
				TimeoutTs:    1000,
				Schema:       gateTestSchema(v.autoID),
				Options:      v.options,
				Files: []*internalpb.ImportFile{
					{Id: 1, Paths: []string{"a.json"}},
					{Id: 2, Paths: []string{"b.json"}},
				},
			},
			tr: timerecord.NewTimeRecorder("import job"),
		}
		s.NoError(s.importMeta.AddJob(context.TODO(), job))
		s.driveToPreImportDone(jobID, map[int64]int64{1: 10, 2: 20})

		s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), jobID))
		s.Equal(internalpb.ImportJobState_Importing,
			s.importMeta.GetJob(context.TODO(), jobID).GetState(), v.name)
	}
	s.Equal(0, assignCalls, "excluded jobs must never allocate/broadcast ranges")
	s.Equal(0, roleCalls, "excluded jobs must never consult the replication role")
}

// While the version gate is not yet satisfied (ImportEnableIDRangeMsg resolves to
// "false": the default "auto" until the MixCoord confirmator flips it after every node
// reaches the gate version), a ranged job must NOT enter AssigningIDRange or broadcast
// the UpdateImport V2 message. It proceeds straight to Importing with no ranges, so the
// datanode falls back to the legacy local allocator -- the behavior every older streaming
// node understands. This is what prevents the new-type panic during a rolling upgrade.
func (s *ImportCheckerSuite) TestIDRangeGate_VersionGateOffUsesLegacyPath() {
	item := &Params.DataCoordCfg.ImportEnableIDRangeMsg
	old := item.SwapTempValue("false")
	defer item.SwapTempValue(old)

	s.enableGateMocks()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil)

	roleCalls, assignCalls := 0, 0
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		roleCalls++
		return replicateutil.RolePrimary, false, nil
	}
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	s.setupAutoIDPreImportDone(map[int64]int64{1: 10, 2: 20, 3: 30})
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Importing, job.GetState(),
		"gate off must skip AssigningIDRange and go straight to Importing")
	s.False(jobIDRangesSet(job), "the legacy path must leave every file range nil")
	s.Equal(0, assignCalls, "gate off must never allocate/broadcast ranges")
	s.Equal(0, roleCalls, "gate off must never consult the replication role")
}

// ---------------------------------------------------------------------------
// Gate: totalRows == 0 (Test Plan item 6)
// ---------------------------------------------------------------------------

// A zero-row autoID job takes the existing empty-import branch before the gate:
// no range is needed and nothing is broadcast, regardless of auto_commit.
// A zero-row ranged job still has to obtain the ranges: they are the only signal of what
// the peer counted, so an empty side that skipped the gate could commit against the peer's
// rows. It takes the zero-rows exit (Uncommitted / Completed) only after the ranges are
// applied and its counts match.
func (s *ImportCheckerSuite) TestIDRangeGate_ZeroRowsStillTakesTheRangeGate() {
	s.enableGateMocks()
	assignCalls := 0
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, false, nil
	}
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	// auto_commit=false -> Uncommitted (2PC surface) once the ranges are in.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).Schema.Fields[0].AutoID = true
		job.(*importJob).AutoCommit = false
	})
	s.driveToPreImportDone(s.jobID, map[int64]int64{1: 0, 2: 0, 3: 0})
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"an empty ranged job waits for the ranges like any other")
	// The broadcast is the AssigningIDRange tick, not the transition tick; the job stays
	// parked until the ranges are applied.
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Equal(1, assignCalls, "the primary broadcasts even for an empty job")
	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 5000, End: 5000},
		{Begin: 5000, End: 5000},
		{Begin: 5000, End: 5000},
	})))
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// auto_commit=true -> Completed once the ranges are in.
	jobID := int64(950)
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        jobID,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"ch0"},
			State:        internalpb.ImportJobState_Pending,
			TimeoutTs:    1000,
			AutoCommit:   true,
			Schema:       gateTestSchema(true),
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"a.json"}},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	s.NoError(s.importMeta.AddJob(context.TODO(), job))
	s.driveToPreImportDone(jobID, map[int64]int64{1: 0})
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), jobID).GetState())
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), jobID))
	s.NoError(s.importMeta.UpdateJob(context.TODO(), jobID, UpdateJobIDRanges([]*commonpb.IDRange{{Begin: 5000, End: 5000}})))
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), jobID))
	s.Equal(internalpb.ImportJobState_Completed, s.importMeta.GetJob(context.TODO(), jobID).GetState())
	s.Equal(2, assignCalls)
}

// ---------------------------------------------------------------------------
// Gate: post-gate divergence check (Test Plan item 7)
// ---------------------------------------------------------------------------

// A ranged job whose local preimport row count differs from the size of the
// primary's reserved range (cross-cluster file divergence) fails loudly before any
// Import task exists, with both numbers in the reason. The range size IS the
// primary's exact row count, so comparing against it is the whole check. The
// validation fires from checkAssigningIDRangeJob: the job first waits in
// AssigningIDRange, then fails once the divergent ranges are applied.
func (s *ImportCheckerSuite) TestIDRangeGate_RowCountDivergenceFailsJob() {
	s.enableGateMocks()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	// The job must fail before any Import task exists, so this must NOT fire.
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	// Tick 1: preimport done, ranges unset → wait in AssigningIDRange.
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	// The primary reserved 150 ids for file 3; this cluster's preimport counted 200.
	// The gate compares the local count against the range size, so this is a divergence.
	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 5000, End: 5100}, // file 1: 100 rows
		{Begin: 5100, End: 5100}, // file 2: zero-row
		{Begin: 5100, End: 5250}, // file 3: 150 ids reserved, 200 local rows
	})))

	assignCalls := 0
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	// Tick 2: the divergence check fires from the AssigningIDRange state.
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "import file 3")
	s.Contains(job.GetReason(), "local row count 200")
	s.Contains(job.GetReason(), "reserved ID range size 150")
	s.Equal(0, assignCalls, "ranges were applied; the divergence is terminal, not a re-broadcast")
	s.Equal(0, len(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType))),
		"the job must fail before any Import task is created")
}

// ---------------------------------------------------------------------------
// Gate: missing preimport stat (Test Plan item 8)
// ---------------------------------------------------------------------------

// A job file without a completed preimport stat at ensureIDRanges time is an
// internal inconsistency: warn and retry next tick -- no panic, no allocation.
// The transition itself only checks task states, so the gap surfaces on the
// AssigningIDRange tick that builds the file-row picture.
func (s *ImportCheckerSuite) TestIDRangeGate_MissingStatWaits() {
	s.enableGateMocks()
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).Schema.Fields[0].AutoID = true
		job.(*importJob).State = internalpb.ImportJobState_PreImporting
	})

	// One completed preimport task covering only files 1 and 2; the job has file 3 too.
	task := &preImportTask{tr: timerecord.NewTimeRecorder("preimport task")}
	task.task.Store(&datapb.PreImportTask{
		JobID:  s.jobID,
		TaskID: 555,
		State:  datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportFileStats{
			{ImportFile: &internalpb.ImportFile{Id: 1, Paths: []string{"a.json"}}, TotalRows: 100},
			{ImportFile: &internalpb.ImportFile{Id: 2, Paths: []string{"b.json"}}, TotalRows: 50},
		},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))

	assignCalls := 0
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	// The lone task is completed, so the transition fires despite the coverage gap.
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Equal(0, assignCalls, "the transition tick must not allocate")

	s.NotPanics(func() {
		s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	})
	s.Equal(0, assignCalls, "no allocation with an incomplete file-row picture")
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_AssigningIDRange, job.GetState())
	s.Empty(job.GetReason())
}

// ---------------------------------------------------------------------------
// Gate: full primary transition PreImporting → AssigningIDRange → Importing
// ---------------------------------------------------------------------------

// End-to-end on the primary side: the transition tick broadcasts nothing, the
// waiting tick broadcasts once, and once the ack callback's ranges are applied
// (simulated via UpdateJobIDRanges) the job reaches Importing with stamped tasks.
func (s *ImportCheckerSuite) TestIDRangeGate_PrimaryFullTransition() {
	s.enableGateMocks()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil)
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})

	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, false, nil
	}
	assignCalls := 0
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}

	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
	s.Equal(0, assignCalls, "the transition tick must not broadcast")

	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, assignCalls, "the waiting tick broadcasts")
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 5000, End: 5100},
		{Begin: 5100, End: 5100},
		{Begin: 5100, End: 5300},
	})))
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, assignCalls, "ranges applied; no further broadcast")

	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Importing, job.GetState())
	s.NotEmpty(s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(ImportTaskType)))
}

// ---------------------------------------------------------------------------
// Gate: restart recovery from the persisted AssigningIDRange state
// ---------------------------------------------------------------------------

// importMeta reload is state-agnostic, so a job persisted in AssigningIDRange is
// picked up by the new state-machine case: it proceeds when ranges are set and
// waits (secondary) when they are not. Constructing the job directly in
// AssigningIDRange reproduces the post-restart snapshot.
func (s *ImportCheckerSuite) TestIDRangeGate_RestartRecovery() {
	s.enableGateMocks()
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Recovered with ranges already applied → proceeds to Importing.
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})
	s.NoError(s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobIDRanges([]*commonpb.IDRange{
		{Begin: 5000, End: 5100},
		{Begin: 5100, End: 5100},
		{Begin: 5100, End: 5300},
	})))
	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_AssigningIDRange))
	s.Equal(internalpb.ImportJobState_AssigningIDRange,
		s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_Importing,
		s.importMeta.GetJob(context.TODO(), s.jobID).GetState(), "a recovered ranged job must proceed")

	// Recovered without ranges (secondary) → keeps waiting, never allocates.
	jobID := int64(960)
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        jobID,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"ch0"},
			State:        internalpb.ImportJobState_Pending,
			TimeoutTs:    1000,
			Schema:       gateTestSchema(true),
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"a.json"}},
				{Id: 2, Paths: []string{"b.json"}},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	s.NoError(s.importMeta.AddJob(context.TODO(), job))
	s.driveToPreImportDone(jobID, map[int64]int64{1: 10, 2: 20})
	s.manuallyUpdateJob(jobID, UpdateJobState(internalpb.ImportJobState_AssigningIDRange))

	assignCalls := 0
	s.checker.hooks.getReplicationRole = func(context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RoleSecondary, true, nil
	}
	s.checker.hooks.assignImportIDRange = func(context.Context, ImportJob, []int64) error {
		assignCalls++
		return nil
	}
	s.checker.checkAssigningIDRangeJob(s.importMeta.GetJob(context.TODO(), jobID))
	s.Equal(0, assignCalls, "a recovered secondary must never allocate")
	s.Equal(internalpb.ImportJobState_AssigningIDRange,
		s.importMeta.GetJob(context.TODO(), jobID).GetState())
}

// ---------------------------------------------------------------------------
// Gate: progress bucket for AssigningIDRange
// ---------------------------------------------------------------------------

// AssigningIDRange reports the end-of-preimport bucket (40), coalesced to the
// Importing display state like PreImporting, so GetImportProgress distinguishes
// "preimport running" (< 40) from "waiting for ID range broadcast/replication".
func (s *ImportCheckerSuite) TestIDRangeGate_AssigningIDRangeProgress() {
	s.enableGateMocks()
	s.setupAutoIDPreImportDone(map[int64]int64{1: 100, 2: 0, 3: 200})
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(internalpb.ImportJobState_AssigningIDRange, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())

	progress, state, _, _, reason := GetJobProgress(context.TODO(), s.jobID, s.importMeta, s.checker.meta)
	s.Equal(int64(40), progress)
	s.Equal(internalpb.ImportJobState_Importing, state)
	s.Empty(reason)
}

// ---------------------------------------------------------------------------
// UpdateJobIDRanges (Test Plan item 15)
// ---------------------------------------------------------------------------

func TestUpdateJobIDRanges_SetsRangesAndPersists(t *testing.T) {
	ctx := context.Background()
	var saved []*datapb.ImportJob
	importMeta := newCapturingImportMeta(t, &saved)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 1,
			State: internalpb.ImportJobState_PreImporting,
			Files: []*internalpb.ImportFile{{Id: 10}, {Id: 11}},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	require.NoError(t, importMeta.AddJob(ctx, job))

	ranges := []*commonpb.IDRange{
		{Begin: 100, End: 110},
		{Begin: 110, End: 110}, // zero-row file keeps its empty range
	}
	require.NoError(t, importMeta.UpdateJob(ctx, 1, UpdateJobIDRanges(ranges)))

	// Survives re-GetJob: ranges by position.
	got := importMeta.GetJob(ctx, 1)
	require.NotNil(t, got)
	assert.EqualValues(t, 100, got.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, got.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 110, got.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, got.GetFiles()[1].GetIdRange().GetEnd())

	// Persisted: the proto handed to the catalog carries the ranges.
	require.NotEmpty(t, saved)
	last := saved[len(saved)-1]
	assert.EqualValues(t, 100, last.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, last.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 110, last.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, last.GetFiles()[1].GetIdRange().GetEnd())
}

func (s *ImportCheckerSuite) TestCheckPreImporting_EmptyImportCompletionFailureFailsStop() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)

	s.alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPendingJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	preimportTasks := s.importMeta.GetTaskByJob(context.TODO(), s.jobID, WithType(PreImportTaskType))
	for _, task := range preimportTasks {
		err := s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats([]*datapb.ImportFileStats{{TotalRows: 0}}))
		s.NoError(err)
	}
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).AutoCommit = true
	})
	s.restampSuiteJobRanges()

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).
		Return(errors.New("ambiguous catalog response")).
		Once()
	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	s.checker.ctx = context.Background()
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.Background(), s.jobID))

	s.True(fatalCalled)
	s.Equal(internalpb.ImportJobState_PreImporting,
		s.importMeta.GetJob(context.Background(), s.jobID).GetState())
}
