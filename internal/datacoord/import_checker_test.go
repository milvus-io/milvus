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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
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
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
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

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)

	broker := broker2.NewMockBroker(s.T())

	sjm := NewMockStatsJobManager(s.T())

	checker := NewImportChecker(meta, broker, cluster, s.alloc, imeta, sjm).(*importChecker)
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

	s.checker.LogStats()
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
	s.checker.checkPreImportingJob(job)
	importTasks := s.imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(context.TODO(), job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
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
