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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	checker := NewImportChecker(context.TODO(), meta, broker, s.alloc, importMeta, ci, handler, s.cluster, importCheckerHooks{}).(*importChecker)
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
	preimportTasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.checkPendingJob(job) // no lack
	preimportTasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkPreImportingJob
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	for _, t := range preimportTasks {
		err := s.importMeta.UpdateTask(context.TODO(), t.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats(completedPreImportFileStats(t, 100, 0)))
		s.NoError(err)
	}

	s.checker.checkPreImportingJob(job)
	importTasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.checkPreImportingJob(job) // no lack
	importTasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
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
	// SetupTest's broad SaveImportJob expectation was used to add the job. Drop
	// it before installing the one-shot failure that models the publication
	// boundary under test.
	catalog.ExpectedCalls = nil

	s.alloc.EXPECT().AllocN(int64(1)).Return(int64(200), int64(201), nil).Once()
	s.alloc.EXPECT().AllocID(mock.Anything).Return(int64(300), nil).Once()
	s.alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(400), nil).Once()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()

	var persistedImportTask *datapb.ImportTaskV2
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *datapb.ImportTaskV2) {
			persistedImportTask = proto.Clone(task).(*datapb.ImportTaskV2)
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
	s.Len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithType(ImportTaskType)), 1)

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

	restartedChecker := *s.checker
	restartedChecker.importMeta = restartedMeta
	restartedChecker.checkPreImportingJob(restartedMeta.GetJob(context.TODO(), s.jobID))

	recovered := restartedMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Importing, recovered.GetState())
	s.Equal(expectedRequestSize, recovered.GetRequestedDiskSize())
	s.Len(restartedMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithType(ImportTaskType)), 1,
		"recovery must reuse the complete persisted task set")
}

func (s *ImportCheckerSuite) TestValidateImportTaskSetRejectsFalseCompleteCoverage() {
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

	// Every requested file appears at least once, so the former delete-from-map
	// check reported lacks == 0. The duplicate means this is not a legal task
	// partition and must not be used to commit the job state.
	lacks, err := validateImportTaskSet(job, []ImportTask{preimport}, []ImportTask{
		newImportTask(200, stats[0], stats[1]),
		newImportTask(201, stats[0], stats[2]),
	})
	s.Error(err)
	s.Nil(lacks)
	s.Contains(err.Error(), "appears in multiple import tasks")
}

func (s *ImportCheckerSuite) TestCheckSortingJobRejectsInvalidSortPlan() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	task := &importTask{tr: timerecord.NewTimeRecorder("import task")}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:            s.jobID,
		TaskID:           10,
		State:            datapb.ImportTaskStateV2_Completed,
		SegmentIDs:       []int64{100, 101},
		SortedSegmentIDs: []int64{200},
	})
	s.NoError(s.importMeta.AddTask(context.TODO(), task))
	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_Sorting))
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	s.NotPanics(func() {
		s.checker.checkSortingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	})
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "2 origin segments and 1 sorted segments")
}

func (s *ImportCheckerSuite) TestCheckIndexBuildingJobRejectsMixedSortPlan() {
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Twice()

	newTask := func(taskID, originID int64, sortedIDs []int64) *importTask {
		task := &importTask{tr: timerecord.NewTimeRecorder("import task")}
		task.task.Store(&datapb.ImportTaskV2{
			JobID:            s.jobID,
			TaskID:           taskID,
			State:            datapb.ImportTaskStateV2_Completed,
			SegmentIDs:       []int64{originID},
			SortedSegmentIDs: sortedIDs,
		})
		return task
	}
	s.NoError(s.importMeta.AddTask(context.TODO(), newTask(10, 100, nil)))
	s.NoError(s.importMeta.AddTask(context.TODO(), newTask(11, 101, []int64{201})))
	s.manuallyUpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_IndexBuilding))
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	s.NotPanics(func() {
		s.checker.checkIndexBuildingJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	})
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "both sorted and unsorted tasks")
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
	tasks := s.importMeta.GetTaskBy(context.TODO(), WithType(ImportTaskType), WithJob(s.jobID))
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
	tasks := s.importMeta.GetTaskBy(context.TODO(), WithType(ImportTaskType), WithJob(s.jobID))
	targets, err := s.checker.getValidatedImportTargets(job, tasks, true)
	s.NoError(err)
	s.Empty(targets, "a dropped zero-row output is a completed empty branch, not a target or a failure")
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
	tasks := s.importMeta.GetTaskBy(context.TODO(), WithType(ImportTaskType), WithJob(s.jobID))
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
	corruptMeta.EXPECT().GetTaskBy(mock.Anything, mock.Anything, mock.Anything).
		Return([]ImportTask{nil})
	corruptMeta.EXPECT().UpdateJob(mock.Anything, job.GetJobID(), mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, actions ...UpdateJobAction) error {
			for _, action := range actions {
				action(job)
			}
			return nil
		})
	checker := *s.checker
	checker.importMeta = corruptMeta

	s.NotPanics(func() {
		checker.checkSortingJob(job)
	})
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Contains(job.GetReason(), "invalid concrete type")
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

func (s *ImportCheckerSuite) TestCheckJob_Failed() {
	mockErr := errors.New("mock err")
	job := s.importMeta.GetJob(context.TODO(), s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog := s.importMeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(mockErr)

	s.checker.checkPendingJob(job)
	preimportTasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	s.checker.checkPendingJob(job)
	preimportTasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPendingJob(job)
	preimportTasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
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
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(mockErr)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPreImportingJob(job)
	importTasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_Failed, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	s.manuallyUpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	s.checker.checkPreImportingJob(job)
	importTasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.importMeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	s.checker.checkPreImportingJob(job)
	importTasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
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
	tasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
	s.checker.checkFailedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	tasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkFailedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	tasks = s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))
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
	s.Equal(1, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	err = s.importMeta.UpdateJob(context.TODO(), s.jobID, UpdateJobState(internalpb.ImportJobState_Failed))
	s.NoError(err)

	// not reach cleanup ts
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	job := s.importMeta.GetJob(context.TODO(), s.jobID)
	job.(*importJob).CleanupTs = tsoutil.AddPhysicalDurationOnTs(job.GetCleanupTs(), GCRetention*-2)
	err = s.importMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// origin segment not dropped
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	err = s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateSegmentIDs([]int64{}))
	s.NoError(err)

	// stats segment not dropped
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	err = s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateStatsSegmentIDs([]int64{}))
	s.NoError(err)

	// task is not dropped: it still names a worker, so GC retries the drop the
	// scheduler could not land instead of waiting for someone else to do it.
	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).
		Return(errors.New("connection refused")).Once()
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))
	err = s.importMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(NullNodeID))
	s.NoError(err)

	// remove task failed
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))

	// remove job failed
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.importMeta.GetJobBy(context.TODO())))

	// normal case
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkGC(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(0, len(s.importMeta.GetJobBy(context.TODO())))
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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return false, nil }

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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) {
		_, hasDeadline = ctx.Deadline()
		return false, nil
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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) {
		return false, errors.New("balancer not ready")
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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

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
	s.checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

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
		tasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(jobB.GetJobID()))
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
	s.checker.checkCollection(1, []ImportJob{s.importMeta.GetJob(context.TODO(), s.jobID)})
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

	checker := NewImportChecker(context.TODO(), meta, broker, alloc, importMeta, cim, handler, session.NewMockCluster(t), importCheckerHooks{}).(*importChecker)

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
		preimportTasks := importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
		taskLen := len(preimportTasks)
		mlog.Info(context.TODO(), "job pre-importing", mlog.Any("taskLen", taskLen), mlog.Any("jobState", job.GetState()))
		return taskLen == 2 && job.GetState() == internalpb.ImportJobState_PreImporting
	}, 2*time.Second, 500*time.Millisecond)
	mlog.Info(context.TODO(), "job pre-importing")

	// check pre-importing
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	preimportTasks := importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	for _, pt := range preimportTasks {
		err := importMeta.UpdateTask(context.TODO(), pt.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateFileStats(completedPreImportFileStats(pt, 100, 0)))
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		importTasks := importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
		return len(importTasks) == 1 && job.GetState() == internalpb.ImportJobState_Importing
	}, 2*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job importing")

	// check importing
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	// AlterSegments was previously driven by unsetSegmentImporting (removed in 2PC);
	// the remaining segment writes in this flow may or may not hit it.
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	importTasks := importMeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
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
	}, 2*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job stats")

	// check stats
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
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
	}, 2*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job index building")

	// check index building → Uncommitted (2PC: no longer transitions directly to Completed;
	// the test does not wire up a CommitImport broadcaster, so the job stops at Uncommitted).
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Uncommitted
	}, 2*time.Second, 100*time.Millisecond)
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
		return nil
	}

	s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	s.True(commitCalled, "commit hook should be called when auto_commit=true")
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

// TestCheckUncommittedJob_RepeatedTicks_Safe verifies that ticker re-entry into
// checkUncommittedJob before the ack callback transitions the job state is safe.
// the commit hook is invoked once per tick; correctness against the resulting
// duplicate broadcasts is guaranteed by the broadcaster's resource-key lock,
// the ack callback's state guard, and HandleCommitVchannel's idempotency.
func (s *ImportCheckerSuite) TestCheckUncommittedJob_RepeatedTicks_Safe() {
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Uncommitted
		job.(*importJob).AutoCommit = true
	})

	callCount := 0
	s.checker.hooks.commitImport = func(ctx context.Context, job ImportJob) error {
		callCount++
		return nil
	}

	for i := 0; i < 3; i++ {
		s.checker.checkUncommittedJob(s.importMeta.GetJob(context.TODO(), s.jobID))
	}
	s.Equal(3, callCount, "each tick must call the commit hook; broadcaster handles dedup")
	s.Equal(internalpb.ImportJobState_Uncommitted, s.importMeta.GetJob(context.TODO(), s.jobID).GetState(),
		"state must remain Uncommitted until the ack callback fires")
}

// ---------------------------------------------------------------------------
// Tests for checkCommittingJob
// ---------------------------------------------------------------------------

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

func (s *ImportCheckerSuite) TestCheckCommittingJob_Partial() {
	// Only some vchannels committed → job should stay Committing.
	s.manuallyUpdateJob(s.jobID, func(job ImportJob) {
		job.(*importJob).State = internalpb.ImportJobState_Committing
		job.(*importJob).Vchannels = []string{"ch0", "ch1"}
		job.(*importJob).CommittedVchannels = []string{"ch0"}
	})

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
	preimportTasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithType(PreImportTaskType))
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
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))

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
	preimportTasks := s.importMeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithType(PreImportTaskType))
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
	s.checker.checkPreImportingJob(s.importMeta.GetJob(context.TODO(), s.jobID))

	s.Equal(internalpb.ImportJobState_Completed, s.importMeta.GetJob(context.TODO(), s.jobID).GetState())
}
