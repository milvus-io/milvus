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

	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type ImportCheckerSuite struct {
	suite.Suite

	jobID   int64
	imeta   ImportMeta
	checker *importChecker
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

	cluster := NewMockCluster(s.T())
	alloc := NewNMockAllocator(s.T())

	imeta, err := NewImportMeta(catalog)
	s.NoError(err)
	s.imeta = imeta

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)

	broker := broker2.NewMockBroker(s.T())
	sm := NewMockManager(s.T())
	buildIndexCh := make(chan UniqueID, 1024)

	checker := NewImportChecker(meta, broker, cluster, alloc, sm, imeta, buildIndexCh).(*importChecker)
	s.checker = checker

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"ch0"},
			TimeoutTs:    1000,
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
			State:  internalpb.ImportState_Failed,
		},
	}
	err := s.imeta.AddTask(pit1)
	s.NoError(err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:      s.jobID,
			TaskID:     2,
			SegmentIDs: []int64{10, 11, 12},
			State:      internalpb.ImportState_Pending,
		},
	}
	err = s.imeta.AddTask(it1)
	s.NoError(err)

	s.checker.LogStats()
}

func (s *ImportCheckerSuite) TestCheckState() {
	job := s.imeta.GetJob(s.jobID)

	// test checkLackPreImport
	alloc := s.checker.alloc.(*NMockAllocator)
	alloc.EXPECT().allocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)

	s.checker.checkLackPreImport(job)
	preimportTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))
	s.checker.checkLackPreImport(job) // no lack
	preimportTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))

	// test checkLackImports
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	for _, t := range preimportTasks {
		err := s.imeta.UpdateTask(t.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
		s.NoError(err)
	}

	s.checker.checkLackImports(job)
	importTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
	s.checker.checkLackImports(job) // no lack
	importTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))

	// test checkImportState
	sm := s.checker.sm.(*MockManager)
	sm.EXPECT().FlushImportSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	cluster := s.checker.cluster.(*MockCluster)
	cluster.EXPECT().AddImportSegment(mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.checker.checkImportState(job) // not completed
	for _, t := range importTasks {
		task := s.imeta.GetTask(t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(id)
			s.Equal(true, segment.GetIsImporting())
		}
	}
	for _, t := range importTasks {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{ID: rand.Int63(), IsImporting: true},
		}
		err := s.checker.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
		err = s.imeta.UpdateTask(t.GetTaskID(), UpdateState(internalpb.ImportState_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}))
		s.NoError(err)
	}
	s.checker.checkImportState(job)
	for _, t := range importTasks {
		task := s.imeta.GetTask(t.GetTaskID())
		for _, id := range task.(*importTask).GetSegmentIDs() {
			segment := s.checker.meta.GetSegment(id)
			s.Equal(false, segment.GetIsImporting())
		}
	}
}

func (s *ImportCheckerSuite) TestCheckState_Failed() {
	mockErr := errors.New("mock err")
	job := s.imeta.GetJob(s.jobID)

	// test checkLackPreImport
	alloc := s.checker.alloc.(*NMockAllocator)
	alloc.EXPECT().allocN(mock.Anything).Return(0, 0, nil)
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(mockErr)
	s.checker.checkLackPreImport(job)
	preimportTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))

	alloc.ExpectedCalls = nil
	alloc.EXPECT().allocN(mock.Anything).Return(0, 0, mockErr)
	s.checker.checkLackPreImport(job)
	preimportTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(0, len(preimportTasks))

	alloc.ExpectedCalls = nil
	alloc.EXPECT().allocN(mock.Anything).Return(0, 0, nil)
	catalog.ExpectedCalls = nil
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	s.checker.checkLackPreImport(job)
	preimportTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(PreImportTaskType))
	s.Equal(2, len(preimportTasks))

	// test checkLackImports
	for _, t := range preimportTasks {
		err := s.imeta.UpdateTask(t.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
		s.NoError(err)
	}

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(mockErr)
	s.checker.checkLackImports(job)
	importTasks := s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))

	alloc.ExpectedCalls = nil
	alloc.EXPECT().allocN(mock.Anything).Return(0, 0, mockErr)
	importTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(0, len(importTasks))

	catalog.ExpectedCalls = nil
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	alloc.ExpectedCalls = nil
	alloc.EXPECT().allocN(mock.Anything).Return(0, 0, nil)
	s.checker.checkLackImports(job)
	importTasks = s.imeta.GetTaskBy(WithJob(job.GetJobID()), WithType(ImportTaskType))
	s.Equal(1, len(importTasks))
}

func (s *ImportCheckerSuite) TestCheckTimeout() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  internalpb.ImportState_InProgress,
		},
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)
	s.checker.checkTimeout(s.jobID)

	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(internalpb.ImportState_Failed, task.GetState())
	s.Equal("import timeout", task.GetReason())
}

func (s *ImportCheckerSuite) TestCheckFailure() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  internalpb.ImportState_Pending,
		},
	}
	err := s.imeta.AddTask(pit1)
	s.NoError(err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 2,
			State:  internalpb.ImportState_Completed,
		},
	}
	err = s.imeta.AddTask(pit2)
	s.NoError(err)

	s.checker.checkFailure(s.jobID)
	tasks := s.imeta.GetTaskBy(WithJob(s.jobID), WithStates(internalpb.ImportState_Failed))
	s.Equal(0, len(tasks))

	err = s.imeta.UpdateTask(pit1.GetTaskID(), UpdateState(internalpb.ImportState_Failed))
	s.NoError(err)
	s.checker.checkFailure(s.jobID)
	tasks = s.imeta.GetTaskBy(WithJob(s.jobID), WithStates(internalpb.ImportState_Failed))
	s.Equal(2, len(tasks))
}

func (s *ImportCheckerSuite) TestCheckGC() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  internalpb.ImportState_Completed,
		},
		lastActiveTime: time.Now(),
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)
	s.checker.checkGC(s.jobID)
	tasks := s.imeta.GetTaskBy(WithJob(s.jobID))
	s.Equal(1, len(tasks))

	catalog.EXPECT().DropImportJob(mock.Anything).Return(nil)
	catalog.EXPECT().DropPreImportTask(mock.Anything).Return(nil)
	GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
	task.(*preImportTask).lastActiveTime = time.Now().Add(GCRetention * -2)
	err = s.imeta.AddTask(task)
	s.NoError(err)
	s.checker.checkGC(s.jobID)
	tasks = s.imeta.GetTaskBy(WithJob(s.jobID))
	s.Equal(0, len(tasks))
}

func (s *ImportCheckerSuite) TestCheckCollection() {
	catalog := s.imeta.(*importMeta).catalog.(*mocks.DataCoordCatalog)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  s.jobID,
			TaskID: 1,
			State:  internalpb.ImportState_Pending,
		},
		lastActiveTime: time.Now(),
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)

	broker := s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(true, nil)
	s.checker.checkCollection(s.jobID)
	tasks := s.imeta.GetTaskBy(WithJob(s.jobID), WithStates(internalpb.ImportState_Failed))
	s.Equal(0, len(tasks))

	s.checker.broker = broker2.NewMockBroker(s.T())
	broker = s.checker.broker.(*broker2.MockBroker)
	broker.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(false, nil)
	s.checker.checkCollection(s.jobID)
	tasks = s.imeta.GetTaskBy(WithJob(s.jobID), WithStates(internalpb.ImportState_Failed))
	s.Equal(1, len(tasks))
}

func TestImportChecker(t *testing.T) {
	suite.Run(t, new(ImportCheckerSuite))
}
