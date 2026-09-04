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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type CopySegmentMetaSuite struct {
	suite.Suite

	collectionID int64

	catalog  *mocks.DataCoordCatalog
	broker   *broker.MockBroker
	meta     *meta
	copyMeta CopySegmentMeta
}

func (s *CopySegmentMetaSuite) SetupTest() {
	var err error

	s.collectionID = 1

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
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

	s.broker = broker.NewMockBroker(s.T())
	s.broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	s.meta, err = newMeta(context.TODO(), s.catalog, nil, s.broker)
	s.NoError(err)
	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, nil, nil)
	s.NoError(err)
}

func TestCopySegmentMeta(t *testing.T) {
	suite.Run(t, new(CopySegmentMetaSuite))
}

func (s *CopySegmentMetaSuite) TestCompleteJob_CatalogErrorFailsStop() {
	const (
		jobID     = int64(100)
		segmentID = int64(101)
	)
	writeErr := errors.New("ambiguous catalog response")
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(writeErr).Once()

	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  s.collectionID,
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
		InsertChannel: "ch1",
	})
	s.Require().NoError(s.meta.AddSegment(context.Background(), segment))
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.Require().NoError(s.copyMeta.AddJob(context.Background(), job))

	fatalCalled := false
	fatalHeldPublicationLocks := false
	concreteCopyMeta := s.copyMeta.(*copySegmentMeta)
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) {
			fatalCalled = true
			jobLockAcquired := concreteCopyMeta.mu.TryLock()
			if jobLockAcquired {
				concreteCopyMeta.mu.Unlock()
			}
			segmentLockAcquired := s.meta.segMu.TryLock()
			if segmentLockAcquired {
				s.meta.segMu.Unlock()
			}
			fatalHeldPublicationLocks = !jobLockAcquired && !segmentLockAcquired
		}).
		Build()
	defer mockFatal.UnPatch()

	applied, err := s.copyMeta.CompleteJob(context.Background(), jobID, []int64{segmentID},
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	s.False(applied)
	s.ErrorIs(err, writeErr)
	s.True(fatalCalled)
	s.True(fatalHeldPublicationLocks)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		s.copyMeta.GetJob(context.Background(), jobID).GetState())
	s.Equal(commonpb.SegmentState_Importing,
		s.meta.GetSegment(context.Background(), segmentID).GetState())
}

func (s *CopySegmentMetaSuite) TestCompleteTask_CatalogErrorFailsStop() {
	ctx := context.Background()
	writeErr := errors.New("ambiguous catalog response")
	task := createTestCopyTask(s.collectionID, 101)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.Require().NoError(s.copyMeta.AddTask(ctx, task))
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.MatchedBy(func(saved *datapb.CopySegmentTask) bool {
		return saved.GetTaskId() == task.GetTaskId() &&
			saved.GetState() == datapb.CopySegmentTaskState_CopySegmentTaskCompleted
	})).Return(writeErr).Once()

	fatalCalled := false
	fatalHeldTaskLock := false
	concreteCopyMeta := s.copyMeta.(*copySegmentMeta)
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) {
			fatalCalled = true
			lockAcquired := concreteCopyMeta.mu.TryLock()
			if lockAcquired {
				concreteCopyMeta.mu.Unlock()
			}
			fatalHeldTaskLock = !lockAcquired
		}).
		Build()
	defer mockFatal.UnPatch()

	applied, err := s.copyMeta.UpdateTaskInState(ctx, task.GetTaskId(),
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted))

	s.NoError(err, "the unreachable post-Fatal path must not enter ordinary task failure handling")
	s.False(applied)
	s.True(fatalCalled)
	s.True(fatalHeldTaskLock)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		s.copyMeta.GetTask(ctx, task.GetTaskId()).GetState())
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_Success() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
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

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil, nil)
	s.NoError(err)
	s.NotNil(copyMeta)
}

func (s *CopySegmentMetaSuite) TestAddJobWithSegmentsPublishesTogether() {
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("copy segment job"),
	}
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            200,
		CollectionID:  s.collectionID,
		State:         commonpb.SegmentState_Importing,
		Level:         datapb.SegmentLevel_L1,
		IsImporting:   true,
		InsertChannel: "ch0",
	})

	err := s.copyMeta.AddJobWithSegments(context.Background(), job, []*SegmentInfo{segment})
	s.NoError(err)
	s.Same(job, s.copyMeta.GetJob(context.Background(), job.GetJobId()))
	s.Same(segment, s.meta.GetSegment(context.Background(), segment.GetID()))
}

func (s *CopySegmentMetaSuite) TestAddJobWithSegments_CanceledRequestDoesNotSuppressFailStop() {
	writeErr := errors.New("ambiguous catalog response")
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(writeErr).Once()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("copy segment job"),
	}
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID:            200,
		CollectionID:  s.collectionID,
		State:         commonpb.SegmentState_Importing,
		IsImporting:   true,
		InsertChannel: "ch0",
	})

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(ctx context.Context, _ string, _ ...mlog.Field) {
			fatalCalled = true
			s.NoError(ctx.Err(), "fail-stop must use the live DataCoord context")
		}).
		Build()
	defer mockFatal.UnPatch()

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.copyMeta.AddJobWithSegments(requestCtx, job, []*SegmentInfo{segment})
	s.ErrorIs(err, writeErr)
	s.True(fatalCalled, "request cancellation must not suppress fail-stop while DataCoord is alive")
	s.Nil(s.copyMeta.GetJob(context.Background(), job.GetJobId()))
	s.Nil(s.meta.GetSegment(context.Background(), segment.GetID()))
}

func (s *CopySegmentMetaSuite) TestReplaceRetryTaskSkipsTerminalJob() {
	ctx := context.Background()
	const (
		jobID     = int64(300)
		oldTaskID = int64(301)
		newTaskID = int64(302)
		oldTarget = int64(303)
		newTarget = int64(304)
	)

	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		tr: timerecord.NewTimeRecorder("terminal copy job"),
	}
	s.Require().NoError(s.copyMeta.AddJob(ctx, job))

	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	oldTask := &copySegmentTask{tr: timerecord.NewTimeRecorder("old task"), times: taskcommon.NewTimes()}
	oldTask.task.Store(&datapb.CopySegmentTask{
		TaskId: oldTaskID,
		JobId:  jobID,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskRetry,
		IdMappings: []*datapb.CopySegmentIDMapping{{
			SourceSegmentId: 1,
			TargetSegmentId: oldTarget,
		}},
	})
	s.Require().NoError(s.copyMeta.AddTask(ctx, oldTask))
	s.meta.segments.SetSegment(oldTarget, NewSegmentInfo(&datapb.SegmentInfo{
		ID: oldTarget, CollectionID: s.collectionID,
		State: commonpb.SegmentState_Importing, IsImporting: true,
	}))

	replacement := &copySegmentTask{tr: timerecord.NewTimeRecorder("new task"), times: taskcommon.NewTimes()}
	replacement.task.Store(&datapb.CopySegmentTask{
		TaskId: newTaskID,
		JobId:  jobID,
		State:  datapb.CopySegmentTaskState_CopySegmentTaskPending,
		IdMappings: []*datapb.CopySegmentIDMapping{{
			SourceSegmentId: 1,
			TargetSegmentId: newTarget,
		}},
	})

	replaced, err := s.copyMeta.ReplaceRetryTask(ctx, oldTaskID, replacement)
	s.NoError(err)
	s.False(replaced)
	s.Same(oldTask, s.copyMeta.GetTask(ctx, oldTaskID))
	s.Nil(s.copyMeta.GetTask(ctx, newTaskID))
	s.Nil(s.meta.GetSegment(ctx, newTarget))
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_ListJobsError() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, errors.New("list jobs error"))

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, nil, nil, nil)
	s.Error(err)
	s.Nil(copyMeta)
	s.Contains(err.Error(), "list jobs error")
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_ListTasksError() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, errors.New("list tasks error"))

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, nil, nil, nil)
	s.Error(err)
	s.Nil(copyMeta)
	s.Contains(err.Error(), "list tasks error")
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_RestoreJobs() {
	catalog := mocks.NewDataCoordCatalog(s.T())

	restoredJobs := []*datapb.CopySegmentJob{
		{
			JobId:        100,
			CollectionId: 1,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		{
			JobId:        200,
			CollectionId: 1,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
	}

	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(restoredJobs, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(nil, nil)
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

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil, nil)
	s.NoError(err)

	// Verify jobs are restored
	job1 := copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(job1)
	s.Equal(int64(100), job1.GetJobId())

	job2 := copyMeta.GetJob(context.TODO(), 200)
	s.NotNil(job2)
	s.Equal(int64(200), job2.GetJobId())
}

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_RestoreTasks() {
	catalog := mocks.NewDataCoordCatalog(s.T())

	restoredTasks := []*datapb.CopySegmentTask{
		{
			TaskId:       1001,
			JobId:        100,
			CollectionId: 1,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		},
		{
			TaskId:       1002,
			JobId:        100,
			CollectionId: 1,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		},
	}

	catalog.EXPECT().ListCopySegmentJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCopySegmentTasks(mock.Anything).Return(restoredTasks, nil)
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

	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	s.NoError(err)

	copyMeta, err := NewCopySegmentMeta(context.TODO(), catalog, meta, nil, nil)
	s.NoError(err)

	// Verify tasks are restored
	task1 := copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(task1)
	s.Equal(int64(1001), task1.GetTaskId())

	task2 := copyMeta.GetTask(context.TODO(), 1002)
	s.NotNil(task2)
	s.Equal(int64(1002), task2.GetTaskId())
}

func (s *CopySegmentMetaSuite) TestAddJob_Success() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Verify job is added
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(retrievedJob)
	s.Equal(int64(100), retrievedJob.GetJobId())
}

func (s *CopySegmentMetaSuite) TestAddJob_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(errors.New("catalog error"))

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	err := s.copyMeta.AddJob(context.TODO(), job)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Verify job is not added
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.Nil(retrievedJob)
}

func (s *CopySegmentMetaSuite) TestUpdateJob_Success() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Add a job first
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Update the job
	err = s.copyMeta.UpdateJob(context.TODO(), 100,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting),
		UpdateCopyJobReason("executing"))
	s.NoError(err)

	// Verify job is updated
	updatedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, updatedJob.GetState())
	s.Equal("executing", updatedJob.GetReason())
}

func (s *CopySegmentMetaSuite) TestUpdateJobInState_AppliesOnlyWhenStateMatches() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobFailed,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	// Expected state mismatch (job is Failed, expected Pending): the update
	// must be skipped without touching the catalog, so a stale caller cannot
	// resurrect a terminal job.
	updated, err := s.copyMeta.UpdateJobInState(context.TODO(), 100,
		datapb.CopySegmentJobState_CopySegmentJobPending,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)
	s.False(updated)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed,
		s.copyMeta.GetJob(context.TODO(), 100).GetState())

	// Missing job: skipped, no error.
	updated, err = s.copyMeta.UpdateJobInState(context.TODO(), 999,
		datapb.CopySegmentJobState_CopySegmentJobPending,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)
	s.False(updated)

	// Matching expected state: the update applies.
	updated, err = s.copyMeta.UpdateJobInState(context.TODO(), 100,
		datapb.CopySegmentJobState_CopySegmentJobFailed,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)
	s.True(updated)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		s.copyMeta.GetJob(context.TODO(), 100).GetState())
}

func (s *CopySegmentMetaSuite) TestUpdateJob_NotFound() {
	// Try to update non-existent job (should not error, just no-op)
	err := s.copyMeta.UpdateJob(context.TODO(), 999,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestUpdateJob_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(errors.New("catalog error")).Once()

	// Add a job first
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	err := s.copyMeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// Update should fail
	err = s.copyMeta.UpdateJob(context.TODO(), 100,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.Error(err)
	s.Contains(err.Error(), "catalog error")
}

func (s *CopySegmentMetaSuite) TestGetJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Get existing job
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(retrievedJob)
	s.Equal(int64(100), retrievedJob.GetJobId())

	// Get non-existent job
	nonExistent := s.copyMeta.GetJob(context.TODO(), 999)
	s.Nil(nonExistent)
}

func (s *CopySegmentMetaSuite) TestGetJobBy() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Add multiple jobs
	jobs := []*copySegmentJob{
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        100,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job1"),
		},
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        200,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			},
			tr: timerecord.NewTimeRecorder("job2"),
		},
		{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        300,
				CollectionId: 2,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job3"),
		},
	}

	for _, job := range jobs {
		s.copyMeta.AddJob(context.TODO(), job)
	}

	// Filter by collection ID
	filtered := s.copyMeta.GetJobBy(context.TODO(), WithCopyJobCollectionID(1))
	s.Len(filtered, 2)

	// Filter by state
	filtered = s.copyMeta.GetJobBy(context.TODO(),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending))
	s.Len(filtered, 2)

	// Filter by collection ID and state
	filtered = s.copyMeta.GetJobBy(context.TODO(),
		WithCopyJobCollectionID(1),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending))
	s.Len(filtered, 1)
	s.Equal(int64(100), filtered[0].GetJobId())
}

func (s *CopySegmentMetaSuite) TestCountJobBy() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Add multiple jobs
	for i := 0; i < 3; i++ {
		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        int64(100 + i),
				CollectionId: s.collectionID,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job"),
		}
		s.copyMeta.AddJob(context.TODO(), job)
	}

	count := s.copyMeta.CountJobBy(context.TODO(),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending))
	s.Equal(3, count)

	count = s.copyMeta.CountJobBy(context.TODO(),
		WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobExecuting))
	s.Equal(0, count)
}

func (s *CopySegmentMetaSuite) TestRemoveJob_Success() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, int64(100)).Return(nil)

	// Add a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Remove the job
	err := s.copyMeta.RemoveJob(context.TODO(), 100)
	s.NoError(err)

	// Verify job is removed
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.Nil(retrievedJob)
}

func (s *CopySegmentMetaSuite) TestRemoveJob_NotFound() {
	// Remove non-existent job (should not error)
	err := s.copyMeta.RemoveJob(context.TODO(), 999)
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestRemoveJob_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentJob(mock.Anything, int64(100)).Return(errors.New("catalog error"))

	// Add a job
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: s.collectionID,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	// Remove should fail
	err := s.copyMeta.RemoveJob(context.TODO(), 100)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Job should still exist in memory
	retrievedJob := s.copyMeta.GetJob(context.TODO(), 100)
	s.NotNil(retrievedJob)
}

func (s *CopySegmentMetaSuite) TestAddTask_Success() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})

	err := s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Verify task is added
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
	s.Equal(int64(1001), retrievedTask.GetTaskId())
}

func (s *CopySegmentMetaSuite) TestAddTask_CatalogErrorDuringShutdown() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(errors.New("catalog error"))

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.copyMeta.AddTask(ctx, task)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Verify task is not added
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Nil(retrievedTask)
}

func (s *CopySegmentMetaSuite) TestUpdateTask_Success() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Add a task first
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err := s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Update the task
	err = s.copyMeta.UpdateTask(context.TODO(), 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress),
		UpdateCopyTaskReason("executing"))
	s.NoError(err)

	// Verify task is updated
	updatedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, updatedTask.GetState())
	s.Equal("executing", updatedTask.GetReason())
}

func (s *CopySegmentMetaSuite) TestUpdateTask_SaveFailureLeavesCacheUnchanged() {
	// AddTask persists fine; the subsequent UpdateTask save fails.
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(errors.New("etcd unavailable")).Once()

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
	})
	err := s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	err = s.copyMeta.UpdateTask(context.TODO(), 1001,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed),
		UpdateCopyTaskReason("should not stick"))
	s.Error(err)

	// The in-memory cache must still reflect the persisted (old) state.
	cachedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, cachedTask.GetState())
	s.Empty(cachedTask.GetReason())
}

func (s *CopySegmentMetaSuite) TestUpdateTask_NotFound() {
	// Try to update non-existent task (should not error, just no-op)
	err := s.copyMeta.UpdateTask(context.TODO(), 9999,
		UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskInProgress))
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestGetTask() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})
	s.copyMeta.AddTask(context.TODO(), task)

	// Get existing task
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
	s.Equal(int64(1001), retrievedTask.GetTaskId())

	// Get non-existent task
	nonExistent := s.copyMeta.GetTask(context.TODO(), 9999)
	s.Nil(nonExistent)
}

func (s *CopySegmentMetaSuite) TestGetTaskBy() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Add multiple tasks
	tasks := []struct {
		taskID int64
		jobID  int64
		state  datapb.CopySegmentTaskState
	}{
		{1001, 100, datapb.CopySegmentTaskState_CopySegmentTaskPending},
		{1002, 100, datapb.CopySegmentTaskState_CopySegmentTaskInProgress},
		{1003, 200, datapb.CopySegmentTaskState_CopySegmentTaskPending},
	}

	for _, t := range tasks {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       t.taskID,
			JobId:        t.jobID,
			CollectionId: s.collectionID,
			State:        t.state,
		})
		s.copyMeta.AddTask(context.TODO(), task)
	}

	// Filter by job ID
	filtered := s.copyMeta.GetTaskBy(context.TODO(), WithCopyTaskJob(100))
	s.Len(filtered, 2)

	// Filter by state
	filtered = s.copyMeta.GetTaskBy(context.TODO(),
		WithCopyTaskStates(datapb.CopySegmentTaskState_CopySegmentTaskPending))
	s.Len(filtered, 2)

	// Filter by job ID and state
	filtered = s.copyMeta.GetTaskBy(context.TODO(),
		WithCopyTaskJob(100),
		WithCopyTaskStates(datapb.CopySegmentTaskState_CopySegmentTaskPending))
	s.Len(filtered, 1)
	s.Equal(int64(1001), filtered[0].GetTaskId())
}

func (s *CopySegmentMetaSuite) TestGetTasksByJobID() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Create tasks for different jobs
	tasks := []struct {
		taskID       int64
		jobID        int64
		collectionID int64
	}{
		{taskID: 1001, jobID: 100, collectionID: 1},
		{taskID: 1002, jobID: 100, collectionID: 1},
		{taskID: 1003, jobID: 200, collectionID: 2},
	}

	for _, t := range tasks {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       t.taskID,
			JobId:        t.jobID,
			CollectionId: t.collectionID,
		})
		s.copyMeta.AddTask(context.TODO(), task)
	}

	// GetTasksByJobID should return tasks for job 100
	result := s.copyMeta.GetTasksByJobID(context.TODO(), 100)
	s.Len(result, 2)
	taskIDs := make([]int64, 0)
	for _, t := range result {
		taskIDs = append(taskIDs, t.GetTaskId())
	}
	s.ElementsMatch([]int64{1001, 1002}, taskIDs)

	// GetTasksByJobID should return tasks for job 200
	result = s.copyMeta.GetTasksByJobID(context.TODO(), 200)
	s.Len(result, 1)
	s.Equal(int64(1003), result[0].GetTaskId())

	// GetTasksByJobID should return empty for non-existent job
	result = s.copyMeta.GetTasksByJobID(context.TODO(), 999)
	s.Len(result, 0)
}

func (s *CopySegmentMetaSuite) TestGetTasksByCollectionID() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	// Create tasks for different collections
	tasks := []struct {
		taskID       int64
		jobID        int64
		collectionID int64
	}{
		{taskID: 1001, jobID: 100, collectionID: 1},
		{taskID: 1002, jobID: 100, collectionID: 1},
		{taskID: 1003, jobID: 200, collectionID: 2},
	}

	for _, t := range tasks {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       t.taskID,
			JobId:        t.jobID,
			CollectionId: t.collectionID,
		})
		s.copyMeta.AddTask(context.TODO(), task)
	}

	// GetTasksByCollectionID should return tasks for collection 1
	result := s.copyMeta.GetTasksByCollectionID(context.TODO(), 1)
	s.Len(result, 2)
	taskIDs := make([]int64, 0)
	for _, t := range result {
		taskIDs = append(taskIDs, t.GetTaskId())
	}
	s.ElementsMatch([]int64{1001, 1002}, taskIDs)

	// GetTasksByCollectionID should return tasks for collection 2
	result = s.copyMeta.GetTasksByCollectionID(context.TODO(), 2)
	s.Len(result, 1)
	s.Equal(int64(1003), result[0].GetTaskId())

	// GetTasksByCollectionID should return empty for non-existent collection
	result = s.copyMeta.GetTasksByCollectionID(context.TODO(), 999)
	s.Len(result, 0)
}

func (s *CopySegmentMetaSuite) TestSecondaryIndexCleanup() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	// Add tasks
	task1 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 1,
	})

	task2 := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{
		TaskId:       1002,
		JobId:        100,
		CollectionId: 1,
	})

	s.copyMeta.AddTask(context.TODO(), task1)
	s.copyMeta.AddTask(context.TODO(), task2)

	// Both tasks exist in indexes
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), 100), 2)
	s.Len(s.copyMeta.GetTasksByCollectionID(context.TODO(), 1), 2)

	// Remove first task
	err := s.copyMeta.RemoveTask(context.TODO(), 1001)
	s.NoError(err)

	// Index should be updated
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), 100), 1)
	s.Len(s.copyMeta.GetTasksByCollectionID(context.TODO(), 1), 1)

	// Remove second task
	err = s.copyMeta.RemoveTask(context.TODO(), 1002)
	s.NoError(err)

	// Index should be empty
	s.Len(s.copyMeta.GetTasksByJobID(context.TODO(), 100), 0)
	s.Len(s.copyMeta.GetTasksByCollectionID(context.TODO(), 1), 0)
}

func (s *CopySegmentMetaSuite) TestRemoveTask_Success() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, int64(1001)).Return(nil)

	// Add a task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})
	s.copyMeta.AddTask(context.TODO(), task)

	// Remove the task
	err := s.copyMeta.RemoveTask(context.TODO(), 1001)
	s.NoError(err)

	// Verify task is removed
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.Nil(retrievedTask)
}

func (s *CopySegmentMetaSuite) TestRemoveTask_NotFound() {
	// Remove non-existent task (should not error)
	err := s.copyMeta.RemoveTask(context.TODO(), 9999)
	s.NoError(err)
}

func (s *CopySegmentMetaSuite) TestRemoveTask_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().DropCopySegmentTask(mock.Anything, int64(1001)).Return(errors.New("catalog error"))

	// Add a task
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: s.collectionID,
	})
	s.copyMeta.AddTask(context.TODO(), task)

	// Remove should fail
	err := s.copyMeta.RemoveTask(context.TODO(), 1001)
	s.Error(err)
	s.Contains(err.Error(), "catalog error")

	// Task should still exist in memory
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
}

func (s *CopySegmentMetaSuite) TestCopySegmentTasks_Operations() {
	tasks := newCopySegmentTasks()

	// Test empty tasks
	s.Nil(tasks.get(1001))
	s.Empty(tasks.listTasks())

	// Test add
	task1 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task1"),
		times: taskcommon.NewTimes(),
	}
	task1.task.Store(&datapb.CopySegmentTask{TaskId: 1001})
	tasks.add(task1)

	task2 := &copySegmentTask{
		tr:    timerecord.NewTimeRecorder("task2"),
		times: taskcommon.NewTimes(),
	}
	task2.task.Store(&datapb.CopySegmentTask{TaskId: 1002})
	tasks.add(task2)

	// Test get
	s.NotNil(tasks.get(1001))
	s.NotNil(tasks.get(1002))
	s.Nil(tasks.get(9999))

	// Test listTasks
	allTasks := tasks.listTasks()
	s.Len(allTasks, 2)

	// Test remove
	tasks.remove(1001)
	s.Nil(tasks.get(1001))
	s.NotNil(tasks.get(1002))
	s.Len(tasks.listTasks(), 1)
}

// TestUpdateJobStateAndReleasePin_UnpinsOnTerminal verifies that transitioning a
// job to Completed with PinId>0 unpins the source snapshot exactly once.
func TestUpdateJobStateAndReleasePin_UnpinsOnTerminal(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	snapMeta := &snapshotMeta{}
	unpinCalls := 0
	var unpinPinID int64
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, pinID int64) (int64, string, int, error) {
			unpinCalls++
			unpinPinID = pinID
			return 0, "", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()

	copyMeta := &copySegmentMeta{
		catalog:      catalog,
		snapshotMeta: snapMeta,
		jobs:         map[int64]CopySegmentJob{},
		tasks:        newCopySegmentTasks(),
	}

	jobID := int64(777)
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       1,
			SourceCollectionId: 1,
			SnapshotName:       "snap_pin",
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			PinId:              42,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	copyMeta.jobs[jobID] = job

	applied, err := copyMeta.UpdateJobStateAndReleasePin(context.TODO(), jobID,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, 1, unpinCalls, "UnpinSnapshot must be called exactly once on terminal transition")
	assert.Equal(t, int64(42), unpinPinID, "Unpin must be called with job.PinId")
	assert.Zero(t, copyMeta.jobs[jobID].GetPinId(), "successful release must durably clear job.PinId")

	// The stale Executing snapshot no longer matches after the first transition.
	applied, err = copyMeta.UpdateJobStateAndReleasePin(context.TODO(), jobID,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	assert.NoError(t, err)
	assert.False(t, applied)
	assert.Equal(t, 1, unpinCalls, "Double terminal transition must not double-unpin")
}

// TestUpdateJobStateAndReleasePin_SkipsUnpinForLegacyJob verifies jobs persisted
// before the pin refactor (PinId=0) skip Unpin and do not panic on nil snapshotMeta.
func TestUpdateJobStateAndReleasePin_SkipsUnpinForLegacyJob(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	unpinCalled := false
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, _ int64) (int64, string, int, error) {
			unpinCalled = true
			return 0, "", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()

	// snapshotMeta=nil is allowed because PinId==0 short-circuits before Unpin.
	copyMeta := &copySegmentMeta{
		catalog: catalog,
		jobs:    map[int64]CopySegmentJob{},
		tasks:   newCopySegmentTasks(),
	}

	jobID := int64(888)
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       1,
			SourceCollectionId: 1,
			SnapshotName:       "snap_legacy",
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			// PinId intentionally zero (pre-refactor job).
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	copyMeta.jobs[jobID] = job

	applied, err := copyMeta.UpdateJobStateAndReleasePin(context.TODO(), jobID,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.False(t, unpinCalled, "UnpinSnapshot must not be called for legacy job (PinId=0)")
}

// TestUpdateJobStateAndReleasePin_UnpinErrorSwallowed verifies that when UnpinSnapshot
// returns an error the state transition is still persisted, and the caller receives nil.
// PinId remains on the terminal job so the checker can retry the release.
func TestUpdateJobStateAndReleasePin_UnpinErrorSwallowed(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	unpinCalls := 0
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, _ int64) (int64, string, int, error) {
			unpinCalls++
			return 0, "", 0, errors.New("etcd unavailable")
		}).Build()
	defer mockUnpin.UnPatch()

	copyMeta := &copySegmentMeta{
		catalog:      catalog,
		snapshotMeta: &snapshotMeta{},
		jobs:         map[int64]CopySegmentJob{},
		tasks:        newCopySegmentTasks(),
	}

	jobID := int64(999)
	copyMeta.jobs[jobID] = &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       1,
			SourceCollectionId: 1,
			SnapshotName:       "snap_err",
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			PinId:              101,
		},
		tr: timerecord.NewTimeRecorder("test"),
	}

	applied, err := copyMeta.UpdateJobStateAndReleasePin(context.TODO(), jobID,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))

	assert.NoError(t, err, "unpin error must be swallowed — state transition already persisted")
	assert.True(t, applied)
	assert.Equal(t, 1, unpinCalls)
	// State transition happened despite unpin failure.
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, copyMeta.jobs[jobID].GetState())
	assert.Equal(t, int64(101), copyMeta.jobs[jobID].GetPinId(), "failed release must retain durable ownership")
}

func TestReleaseJobPin_RetriesAndClearsOwnership(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()

	unpinCalls := 0
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, pinID int64) (int64, string, int, error) {
			unpinCalls++
			assert.Equal(t, int64(202), pinID)
			return 1, "snap_retry", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()

	copyMeta := &copySegmentMeta{
		catalog:      catalog,
		snapshotMeta: &snapshotMeta{},
		jobs: map[int64]CopySegmentJob{
			1001: &copySegmentJob{
				CopySegmentJob: &datapb.CopySegmentJob{
					JobId: 1001, State: datapb.CopySegmentJobState_CopySegmentJobFailed,
					PinId: 202, SourceCollectionId: 1, SnapshotName: "snap_retry",
				},
				tr: timerecord.NewTimeRecorder("test"),
			},
		},
		tasks: newCopySegmentTasks(),
	}

	assert.NoError(t, copyMeta.ReleaseJobPin(context.Background(), 1001))
	assert.Equal(t, 1, unpinCalls)
	assert.Zero(t, copyMeta.jobs[1001].GetPinId())

	// Once ownership is cleared, later checker rounds are no-ops.
	assert.NoError(t, copyMeta.ReleaseJobPin(context.Background(), 1001))
	assert.Equal(t, 1, unpinCalls)
}

// TestUpdateJobStateAndReleasePin_NotFound verifies that updating a non-existent
// job is a no-op and does not error.
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleasePin_NotFound() {
	applied, err := s.copyMeta.UpdateJobStateAndReleasePin(context.TODO(), 999,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.NoError(err)
	s.False(applied)
}

func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleasePin_TerminalCatalogErrorFailsStop() {
	writeErr := errors.New("ambiguous catalog response")
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(writeErr).Once()

	snapshotName := "snap_err"
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              600,
			CollectionId:       s.collectionID,
			SourceCollectionId: s.collectionID,
			SnapshotName:       snapshotName,
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.copyMeta.AddJob(context.TODO(), job)

	fatalCalled := false
	fatalHeldJobLock := false
	concreteCopyMeta := s.copyMeta.(*copySegmentMeta)
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) {
			fatalCalled = true
			lockAcquired := concreteCopyMeta.mu.TryLock()
			if lockAcquired {
				concreteCopyMeta.mu.Unlock()
			}
			fatalHeldJobLock = !lockAcquired
		}).
		Build()
	defer mockFatal.UnPatch()

	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	applied, err := s.copyMeta.UpdateJobStateAndReleasePin(requestCtx, 600,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.NoError(err, "the unreachable post-Fatal path must not enter ordinary job failure handling")
	s.False(applied)
	s.True(fatalCalled, "request cancellation must not suppress fail-stop while DataCoord is alive")
	s.True(fatalHeldJobLock)

	savedJob := s.copyMeta.GetJob(context.TODO(), 600)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, savedJob.GetState())
}

func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleasePin_CatalogErrorDuringShutdown() {
	writeErr := errors.New("catalog error during shutdown")
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(writeErr).Once()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        601,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.Require().NoError(s.copyMeta.AddJob(context.Background(), job))

	componentCtx, cancel := context.WithCancel(context.Background())
	cancel()
	s.copyMeta.(*copySegmentMeta).ctx = componentCtx

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) { fatalCalled = true }).
		Build()
	defer mockFatal.UnPatch()

	applied, err := s.copyMeta.UpdateJobStateAndReleasePin(context.Background(), job.GetJobId(),
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.False(applied)
	s.ErrorIs(err, writeErr)
	s.False(fatalCalled)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting,
		s.copyMeta.GetJob(context.Background(), job.GetJobId()).GetState())
}
