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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
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

	imeta, err := NewImportMeta(catalog)
	s.NoError(err)
	s.imeta = imeta

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)

	broker := broker2.NewMockBroker(s.T())
	sm := NewMockManager(s.T())

	checker := NewImportChecker(meta, broker, cluster, s.alloc, sm, imeta).(*importChecker)
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
	}

	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	err = s.imeta.AddJob(job)
	s.NoError(err)
	s.jobID = job.GetJobID()
}

func (s *ImportCheckerSuite) TestLogStats() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Failed,
		},
	}
	err := s.imeta.AddTask(pit1)
	s.NoError(err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:      s.jobID,
			TaskID:     2,
			SegmentIDs: []int64{10, 11, 12},
			State:      datapb.ImportTaskStateV2_Pending,
		},
	}
	err = s.imeta.AddTask(it1)
	s.NoError(err)

	s.checker.LogStats()
}

func (s *ImportCheckerSuite) TestCheckJob() {
	job := s.imeta.GetJob(s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)

	s.checker.checkPendingJob(job)
	preimportTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(job.GetJobID()).GetState())
	s.checker.checkPendingJob(job) // no lack
	preimportTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(job.GetJobID()).GetState())

	// test checkPreImportingJob
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	for _, t := range preimportTasks {
		err := s.imeta.UpdateTask(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		s.NoError(err)
	}

	s.checker.checkPreImportingJob(job)
	importTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(job.GetJobID()).GetState())
	s.checker.checkPreImportingJob(job) // no lack
	importTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(job.GetJobID()).GetState())

	// test checkImportingJob
	s.checker.checkImportingJob(job) // not completed
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(job.GetJobID()).GetState())
	for _, t := range importTasks {
		task := s.imeta.GetTask(t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(id)
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
		err = s.imeta.UpdateTask(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}))
		s.NoError(err)
		err = s.checker.meta.UpdateChannelCheckpoint(segment.GetInsertChannel(), &msgpb.MsgPosition{MsgID: []byte{0}})
		s.NoError(err)
	}
	s.checker.checkImportingJob(job)
	for _, t := range importTasks {
		task := s.imeta.GetTask(t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(id)
			s.Equal(false, segment.GetIsImporting())
		}
	}
	s.Equal(internalpb.ImportJobState_Completed, s.imeta.GetJob(job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckJob_Failed() {
	mockErr := errors.New("mock err")
	job := s.imeta.GetJob(s.jobID)

	// test checkPendingJob
	alloc := s.alloc
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(mockErr)

	s.checker.checkPendingJob(job)
	preimportTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	s.checker.checkPendingJob(job)
	preimportTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	s.checker.checkPendingJob(job)
	preimportTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(job.GetJobID()).GetState())

	// test checkPreImportingJob
	for _, t := range preimportTasks {
		err := s.imeta.UpdateTask(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		s.NoError(err)
	}

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(mockErr)
	s.checker.checkPreImportingJob(job)
	importTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(job.GetJobID()).GetState())

	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, mockErr)
	importTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))
	s.Equal(internalpb.ImportJobState_PreImporting, s.imeta.GetJob(job.GetJobID()).GetState())

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	alloc.ExpectedCalls = nil
	alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, nil)
	s.checker.checkPreImportingJob(job)
	importTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.Equal(internalpb.ImportJobState_Importing, s.imeta.GetJob(job.GetJobID()).GetState())
}

func (s *ImportCheckerSuite) TestCheckTimeout() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)

	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_InProgress,
		},
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)
	s.checker.tryTimeoutJob(s.imeta.GetJob(s.jobID))

	job := s.imeta.GetJob(s.jobID)
	s.Equal(internalpb.ImportJobState_Failed, job.GetState())
	s.Equal("import timeout", job.GetReason())
}

func (s *ImportCheckerSuite) TestCheckFailure() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Pending,
		},
	}
	err := s.imeta.AddTask(pit1)
	s.NoError(err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 2,
			State:  datapb.ImportTaskStateV2_Completed,
		},
	}
	err = s.imeta.AddTask(pit2)
	s.NoError(err)

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(errors.New("mock error"))
	s.checker.tryFailingTasks(s.imeta.GetJob(s.jobID))
	tasks := s.imeta.GetTaskBy(WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(0, len(tasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	s.checker.tryFailingTasks(s.imeta.GetJob(s.jobID))
	tasks = s.imeta.GetTaskBy(WithJob(s.jobID), WithStates(datapb.ImportTaskStateV2_Failed))
	s.Equal(2, len(tasks))
}

func (s *ImportCheckerSuite) TestCheckGC() {
	mockErr := errors.New("mock err")

	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:      s.jobID,
			TaskID:     1,
			State:      datapb.ImportTaskStateV2_Failed,
			SegmentIDs: []int64{2},
		},
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)

	// not failed or completed
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy()))
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	err = s.imeta.UpdateJob(s.jobID, UpdateJobState(internalpb.ImportJobState_Failed))
	s.NoError(err)

	// not reach cleanup ts
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy()))
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	job := s.imeta.GetJob(s.jobID)
	job.(*importJob).CleanupTs = tsoutil.AddPhysicalDurationOnTs(job.GetCleanupTs(), GCRetention*-2)
	err = s.imeta.AddJob(job)
	s.NoError(err)

	// segment not dropped
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy()))
	err = s.imeta.UpdateTask(task.GetTaskID(), UpdateSegmentIDs([]int64{}))
	s.NoError(err)

	// task is not dropped
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy()))
	err = s.imeta.UpdateTask(task.GetTaskID(), UpdateNodeID(NullNodeID))
	s.NoError(err)

	// remove task failed
	catalog.EXPECT().DropImportTask(mock.Anything).Return(mockErr)
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(1, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy()))

	// remove job failed
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything).Return(mockErr)
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(0, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(1, len(s.imeta.GetJobBy()))

	// normal case
	catalog.ExpectedCalls = nil
	catalog.EXPECT().DropImportJob(mock.Anything).Return(nil)
	s.checker.checkGC(s.imeta.GetJob(s.jobID))
	s.Equal(0, len(s.imeta.GetTaskBy(WithJob(s.jobID))))
	s.Equal(0, len(s.imeta.GetJobBy()))
}

func (s *ImportCheckerSuite) TestCheckCollection() {
	mockErr := errors.New("mock err")

	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Pending,
		},
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)

	// no jobs
	s.checker.checkCollection(1, []ImportJob{})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(s.jobID).GetState())

	// collection exist
	broker := s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(s.jobID).GetState())

	// HasCollection failed
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, mockErr)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(s.jobID).GetState())

	// SaveImportJob failed
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(mockErr)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(s.jobID)})
	s.Equal(internalpb.ImportJobState_Pending, s.imeta.GetJob(s.jobID).GetState())

	// collection dropped
	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	s.checker.checkCollection(1, []ImportJob{s.imeta.GetJob(s.jobID)})
	s.Equal(internalpb.ImportJobState_Failed, s.imeta.GetJob(s.jobID).GetState())
}

func TestImportChecker(t *testing.T) {
	suite.Run(t, new(ImportCheckerSuite))
}
