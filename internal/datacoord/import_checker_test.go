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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type ImportCheckerSuite struct {
	suite.Suite

	jobID   int64
	imeta   ImportMeta
	checker *importChecker
	alloc   *allocator.MockAllocator
}

func (s *ImportCheckerSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

	cluster := NewMockCluster(s.T())
	s.alloc = allocator.NewMockAllocator(s.T())

	imeta, err := NewImportMeta(context.TODO(), catalog)
	s.NoError(err)
	s.imeta = imeta

	broker := broker2.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	sjm := NewMockStatsJobManager(s.T())
	l0CompactionTrigger := NewMockTriggerManager(s.T())
	compactionChan := make(chan struct{}, 1)
	close(compactionChan)
	l0CompactionTrigger.EXPECT().GetPauseCompactionChan(mock.Anything, mock.Anything).Return(compactionChan).Maybe()
	l0CompactionTrigger.EXPECT().GetResumeCompactionChan(mock.Anything, mock.Anything).Return(compactionChan).Maybe()

	checker := NewImportChecker(meta, broker, cluster, s.alloc, imeta, sjm, l0CompactionTrigger).(*importChecker)
	s.checker = checker

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"ch0"},
			State:        internalpb.ImportJobState_Pending,
			TimeoutTs:    1000,
			CleanupTs:    tsoutil.GetCurrentTime(),
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
	err = s.imeta.AddJob(context.TODO(), job)
	s.NoError(err)
	s.jobID = job.GetJobID()
}

func (s *ImportCheckerSuite) TestLogStats() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Failed,
		},
		tr: timerecord.NewTimeRecorder("preimport task"),
	}
	err := s.imeta.AddTask(context.TODO(), pit1)
	s.NoError(err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:      s.jobID,
			TaskID:     2,
			SegmentIDs: []int64{10, 11, 12},
			State:      datapb.ImportTaskStateV2_Pending,
		},
		tr: timerecord.NewTimeRecorder("import task"),
	}
	err = s.imeta.AddTask(context.TODO(), it1)
	s.NoError(err)

	s.checker.LogTaskStats()
}

func (s *ImportCheckerSuite) TestCheckJob() {
	job := s.imeta.GetJob(context.TODO(), s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	s.checker.checkPendingJob(job)
	preimportTasks := s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.checkPendingJob(job) // no lack
	preimportTasks = s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkPreImportingJob
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	for _, t := range preimportTasks {
		err := s.imeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		s.NoError(err)
	}

	s.checker.checkPreImportingJob(job)
	importTasks := s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	s.checker.checkPreImportingJob(job) // no lack
	importTasks = s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkImportingJob
	s.checker.checkImportingJob(job)
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	for _, t := range importTasks {
		task := s.imeta.GetTask(context.TODO(), t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(context.TODO(), id)
			s.Equal(true, segment.GetIsImporting())
		}
	}
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	for _, t := range importTasks {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            rand.Int63(),
				State:         commonpb.SegmentState_Flushed,
				IsImporting:   true,
				InsertChannel: "ch0",
			},
		}
		err := s.checker.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
		err = s.imeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}), UpdateStatsSegmentIDs([]int64{rand.Int63()}))
		s.NoError(err)
		err = s.checker.meta.UpdateChannelCheckpoint(context.TODO(), segment.GetInsertChannel(), &msgpb.MsgPosition{MsgID: []byte{0}})
		s.NoError(err)
	}
	s.checker.checkImportingJob(job)
	s.Equal(internalpb.ImportJobState_Stats, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test check stats job
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil).Maybe()
	sjm := s.checker.sjm.(*MockStatsJobManager)
	sjm.EXPECT().SubmitStatsTask(mock.Anything, mock.Anything, mock.Anything, false).Return(nil)
	sjm.EXPECT().GetStatsTask(mock.Anything, mock.Anything).Return(&indexpb.StatsTask{
		State: indexpb.JobState_JobStateNone,
	})
	s.checker.checkStatsJob(job)
	s.Equal(internalpb.ImportJobState_Stats, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	sjm = NewMockStatsJobManager(s.T())
	sjm.EXPECT().GetStatsTask(mock.Anything, mock.Anything).Return(&indexpb.StatsTask{
		State: indexpb.JobState_JobStateInProgress,
	})
	s.checker.sjm = sjm
	s.checker.checkStatsJob(job)
	s.Equal(internalpb.ImportJobState_Stats, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
	sjm = NewMockStatsJobManager(s.T())
	sjm.EXPECT().GetStatsTask(mock.Anything, mock.Anything).Return(&indexpb.StatsTask{
		State: indexpb.JobState_JobStateFinished,
	})
	s.checker.sjm = sjm
	s.checker.checkStatsJob(job)
	s.Equal(internalpb.ImportJobState_IndexBuilding, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test check IndexBuilding job
	s.checker.checkIndexBuildingJob(job)
	for _, t := range importTasks {
		task := s.imeta.GetTask(context.TODO(), t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(context.TODO(), id)
			s.Equal(false, segment.GetIsImporting())
		}
	}
	s.Equal(internalpb.ImportJobState_Completed, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckJob_Failed() {
	mockErr := errors.New("mock err")
	job := s.imeta.GetJob(context.TODO(), s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(mockErr)

	s.checker.checkPendingJob(job)
	preimportTasks := s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	s.checker.checkPendingJob(job)
	preimportTasks = s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPendingJob(job)
	preimportTasks = s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	// test checkPreImportingJob
	for _, t := range preimportTasks {
		err := s.imeta.UpdateTask(context.TODO(), t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		s.NoError(err)
	}

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(mockErr)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkPreImportingJob(job)
	importTasks := s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_Failed, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	err := s.imeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	s.NoError(err)
	s.checker.checkPreImportingJob(job)
	importTasks = s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	s.checker.checkPreImportingJob(job)
	importTasks = s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckTimeout() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_InProgress,
		},
		tr: timerecord.NewTimeRecorder("preimport task"),
	}
	err := s.imeta.AddTask(context.TODO(), task)
	s.NoError(err)
	s.checker.tryTimeoutJob(s.imeta.GetJob(context.TODO(), s.jobID))

	job := s.imeta.GetJob(context.TODO(), s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Equal("import timeout", job.GetReason())
}

func (s *ImportCheckerSuite) TestCheckFailure() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	it := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:           s.jobID,
			TaskID:          1,
			State:           datapb.ImportTaskStateV2_Pending,
			SegmentIDs:      []int64{2},
			StatsSegmentIDs: []int64{3},
		},
		tr: timerecord.NewTimeRecorder("import task"),
	}
	err := s.imeta.AddTask(context.TODO(), it)
	s.NoError(err)

	sjm := NewMockStatsJobManager(s.T())
	sjm.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock err"))
	s.checker.sjm = sjm
	s.checker.checkFailedJob(s.imeta.GetJob(context.TODO(), s.jobID))
	tasks := s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(0, len(tasks))
	sjm.ExpectedCalls = nil
	sjm.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil)

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
	s.checker.checkFailedJob(s.imeta.GetJob(context.TODO(), s.jobID))
	tasks = s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(0, len(tasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkFailedJob(s.imeta.GetJob(context.TODO(), s.jobID))
	tasks = s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(1, len(tasks))
}

func (s *ImportCheckerSuite) TestCheckGC() {
	mockErr := errors.New("mock err")

	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:           s.jobID,
			TaskID:          1,
			State:           datapb.ImportTaskStateV2_Failed,
			SegmentIDs:      []int64{2},
			StatsSegmentIDs: []int64{3},
		},
		tr: timerecord.NewTimeRecorder("import task"),
	}
	err := s.imeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// not failed or completed
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	err = s.imeta.UpdateJob(context.TODO(), s.jobID, UpdateJobState(internalpb.ImportJobState_Failed))
	s.NoError(err)

	// not reach cleanup ts
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	job := s.imeta.GetJob(context.TODO(), s.jobID)
	job.(*importJob).CleanupTs = tsoutil.AddPhysicalDurationOnTs(job.GetCleanupTs(), GCRetention*-2)
	err = s.imeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// origin segment not dropped
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))
	err = s.imeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateSegmentIDs([]int64{}))
	s.NoError(err)

	// stats segment not dropped
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))
	err = s.imeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateStatsSegmentIDs([]int64{}))
	s.NoError(err)

	// task is not dropped
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))
	err = s.imeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(NullNodeID))
	s.NoError(err)

	// remove task failed
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))

	// remove job failed
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy(context.TODO())))

	// normal case
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkGC(s.imeta.GetJob(context.TODO(), s.jobID))
	s.Equal(0, len(s.imeta.GetTaskBy(context.TODO(), WithJob(s.jobID))))
	s.Equal(0, len(s.imeta.GetJobBy(context.TODO())))
}

func (s *ImportCheckerSuite) TestCheckCollection() {
	mockErr := errors.New("mock err")

	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Pending,
		},
		tr: timerecord.NewTimeRecorder("preimport task"),
	}
	err := s.imeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// no jobs
	s.checker.checkCollection(1, []ImportJob{})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(context.TODO(), s.jobID).GetState())

	// collection exist
	broker := s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(context.TODO(), s.jobID).GetState())

	// HasCollection failed
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, mockErr)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(context.TODO(), s.jobID).GetState())

	// SaveImportJob failed
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(mockErr)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(context.TODO(), s.jobID).GetState())

	// collection dropped
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(context.TODO(), s.jobID)})
	s.Equal(internalpb.ImportJobState_Failed, s.imeta.GetJob(context.TODO(), s.jobID).GetState())
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
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

	cluster := NewMockCluster(t)
	alloc := allocator.NewMockAllocator(t)

	imeta, err := NewImportMeta(context.TODO(), catalog)
	assert.NoError(t, err)

	broker := broker2.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	sjm := NewMockStatsJobManager(t)
	l0CompactionTrigger := NewMockTriggerManager(t)
	compactionChan := make(chan struct{}, 1)
	close(compactionChan)
	l0CompactionTrigger.EXPECT().GetPauseCompactionChan(mock.Anything, mock.Anything).Return(compactionChan).Maybe()
	l0CompactionTrigger.EXPECT().GetResumeCompactionChan(mock.Anything, mock.Anything).Return(compactionChan).Maybe()

	checker := NewImportChecker(meta, broker, cluster, alloc, imeta, sjm, l0CompactionTrigger).(*importChecker)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          1001,
			CollectionID:   1,
			PartitionIDs:   []int64{2},
			ReadyVchannels: []string{"ch0"},
			Vchannels:      []string{"ch0", "ch1"},
			State:          internalpb.ImportJobState_Pending,
			TimeoutTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour), 0),
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour), 0),
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
	err = imeta.AddJob(context.TODO(), job)
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
			TimeoutTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour), 0),
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour), 0),
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
	err = imeta.AddJob(context.TODO(), job2)
	assert.NoError(t, err)
	log.Info("job ready")

	// check pending
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	}).Maybe()
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil).Maybe()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	assert.Eventually(t, func() bool {
		job := imeta.GetJob(context.TODO(), jobID)
		preimportTasks := imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
		taskLen := len(preimportTasks)
		log.Info("job pre-importing", zap.Any("taskLen", taskLen), zap.Any("jobState", job.GetState()))
		return taskLen == 2 && job.GetState() == internalpb.ImportJobState_PreImporting
	}, 2*time.Second, 500*time.Millisecond)
	log.Info("job pre-importing")

	// check pre-importing
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	preimportTasks := imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
	for _, pt := range preimportTasks {
		err := imeta.UpdateTask(context.TODO(), pt.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := imeta.GetJob(context.TODO(), jobID)
		importTasks := imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
		return len(importTasks) == 1 && job.GetState() == internalpb.ImportJobState_Importing
	}, 2*time.Second, 100*time.Millisecond)
	log.Info("job importing")

	// check importing
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	importTasks := imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	for _, it := range importTasks {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            rand.Int63(),
				State:         commonpb.SegmentState_Flushed,
				IsImporting:   true,
				InsertChannel: "ch0",
			},
		}
		err := checker.meta.AddSegment(context.Background(), segment)
		assert.NoError(t, err)
		err = imeta.UpdateTask(context.TODO(), it.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}), UpdateStatsSegmentIDs([]int64{rand.Int63()}))
		assert.NoError(t, err)
		err = checker.meta.UpdateChannelCheckpoint(context.TODO(), segment.GetInsertChannel(), &msgpb.MsgPosition{MsgID: []byte{0}})
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := imeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Stats
	}, 2*time.Second, 100*time.Millisecond)
	log.Info("job stats")

	// check stats
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	sjm.EXPECT().GetStatsTask(mock.Anything, mock.Anything).Return(&indexpb.StatsTask{
		State: indexpb.JobState_JobStateFinished,
	}).Once()
	assert.Eventually(t, func() bool {
		job := imeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_IndexBuilding
	}, 2*time.Second, 100*time.Millisecond)
	log.Info("job index building")

	// wait l0 import task
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	imeta.AddTask(context.TODO(), &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:  jobID,
			TaskID: 100000,
			Source: datapb.ImportTaskSourceV2_L0Compaction,
			State:  datapb.ImportTaskStateV2_InProgress,
		},
	})
	time.Sleep(1200 * time.Millisecond)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	imeta.UpdateTask(context.TODO(), 100000, UpdateState(datapb.ImportTaskStateV2_Completed))
	log.Info("job l0 compaction")

	// check index building
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	assert.Eventually(t, func() bool {
		job := imeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Completed
	}, 2*time.Second, 100*time.Millisecond)
	log.Info("job completed")
}
