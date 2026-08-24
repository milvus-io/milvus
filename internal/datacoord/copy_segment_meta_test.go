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
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
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
	s.meta.AddCollection(&collectionInfo{
		ID:     s.collectionID,
		Schema: newTestSchema(),
	})

	s.copyMeta, err = NewCopySegmentMeta(context.TODO(), s.catalog, s.meta, nil, nil)
	s.NoError(err)
}

func TestCopySegmentMeta(t *testing.T) {
	suite.Run(t, new(CopySegmentMetaSuite))
}

func TestNewCopySegmentMeta_ReconcilesPublishingTargets(t *testing.T) {
	ctx := context.Background()
	catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 901, CollectionID: 1, PartitionID: 2,
		State: commonpb.SegmentState_Flushed, IsImporting: false,
		InsertChannel: "ch",
	})
	assert.NoError(t, catalog.AddSegment(ctx, segment.SegmentInfo))
	m := &meta{catalog: catalog, segments: NewSegmentsInfo()}
	m.segments.SetSegment(901, segment)
	assert.NoError(t, catalog.SaveCopySegmentJob(ctx, &datapb.CopySegmentJob{
		JobId:      77,
		State:      datapb.CopySegmentJobState_CopySegmentJobPublishing,
		IdMappings: []*datapb.CopySegmentIDMapping{{TargetSegmentId: 901}},
	}))

	_, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	assert.NoError(t, err)
	reconciled := m.GetSegment(ctx, 901)
	assert.Equal(t, commonpb.SegmentState_Importing, reconciled.GetState())
	assert.True(t, reconciled.GetIsImporting())
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

func (s *CopySegmentMetaSuite) TestNewCopySegmentMeta_ReconcilesPartialPublication() {
	ctx := context.Background()
	catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	m := &meta{catalog: catalog, segments: NewSegmentsInfo()}

	segments := []*datapb.SegmentInfo{
		{ID: 101, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Flushed, IsImporting: false},
		{ID: 102, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Importing, IsImporting: true},
		{ID: 201, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Flushed, IsImporting: false},
		{ID: 301, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Flushed, IsImporting: false},
	}
	for _, segment := range segments {
		m.segments.SetSegment(segment.GetID(), NewSegmentInfo(segment))
	}

	jobs := []*datapb.CopySegmentJob{
		{
			JobId: 1,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			IdMappings: []*datapb.CopySegmentIDMapping{
				{TargetSegmentId: 101},
				{TargetSegmentId: 102},
			},
		},
		{
			JobId:      2,
			State:      datapb.CopySegmentJobState_CopySegmentJobFailed,
			IdMappings: []*datapb.CopySegmentIDMapping{{TargetSegmentId: 201}},
		},
		{
			JobId:      3,
			State:      datapb.CopySegmentJobState_CopySegmentJobCompleted,
			IdMappings: []*datapb.CopySegmentIDMapping{{TargetSegmentId: 301}},
		},
	}
	for _, job := range jobs {
		s.NoError(catalog.SaveCopySegmentJob(ctx, job))
	}

	copyMeta, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
	s.Require().NoError(err)
	s.NotNil(copyMeta)

	activePublished := m.GetSegment(ctx, 101)
	s.Equal(commonpb.SegmentState_Importing, activePublished.GetState())
	s.True(activePublished.GetIsImporting())
	activeHidden := m.GetSegment(ctx, 102)
	s.Equal(commonpb.SegmentState_Importing, activeHidden.GetState())
	s.True(activeHidden.GetIsImporting())
	s.Equal(commonpb.SegmentState_Dropped, m.GetSegment(ctx, 201).GetState())
	s.Equal(commonpb.SegmentState_Flushed, m.GetSegment(ctx, 301).GetState())
}

func (s *CopySegmentMetaSuite) TestCommitTaskDispatchOutcomes() {
	testCases := []struct {
		name           string
		jobState       datapb.CopySegmentJobState
		taskState      datapb.CopySegmentTaskState
		existingNodeID int64
		dispatchNodeID int64
		wantResolution taskDispatchResolution
		wantState      datapb.CopySegmentTaskState
		wantNodeID     int64
	}{
		{
			name: "active pending is applied", jobState: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskPending, existingNodeID: NullNodeID,
			dispatchNodeID: 10, wantResolution: taskDispatchApplied,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress, wantNodeID: 10,
		},
		{
			name: "same active assignment is idempotent", jobState: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress, existingNodeID: 10,
			dispatchNodeID: 10, wantResolution: taskDispatchAlreadyTracked,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress, wantNodeID: 10,
		},
		{
			name: "terminal task stays terminal and tracks cleanup", jobState: datapb.CopySegmentJobState_CopySegmentJobFailed,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskFailed, existingNodeID: NullNodeID,
			dispatchNodeID: 10, wantResolution: taskDispatchCleanupTracked,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskFailed, wantNodeID: 10,
		},
		{
			name: "different assignment is not overwritten", jobState: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress, existingNodeID: 11,
			dispatchNodeID: 10, wantResolution: taskDispatchCleanupUntracked,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress, wantNodeID: 11,
		},
		// The regression: a dispatch absorbed by the authoritative task on the
		// SAME node must reach the tracked cleanup path, never the untracked one.
		// The untracked drop is an unconditional abort at this dispatch's epoch,
		// which the worker accepts because CreateCopySegment adopted that epoch
		// onto the runtime shared with the task already registered there — so it
		// would delete the completed task's output, whose binlog paths are
		// already in segment metadata and about to be published as Flushed.
		{
			name: "completed task on same node keeps tracked cleanup", jobState: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, existingNodeID: 10,
			dispatchNodeID: 10, wantResolution: taskDispatchCleanupTracked,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, wantNodeID: 10,
		},
		{
			name: "failed task on same node keeps tracked cleanup", jobState: datapb.CopySegmentJobState_CopySegmentJobFailed,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskFailed, existingNodeID: 10,
			dispatchNodeID: 10, wantResolution: taskDispatchCleanupTracked,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskFailed, wantNodeID: 10,
		},
		{
			name: "completed task on a different node stays untracked", jobState: datapb.CopySegmentJobState_CopySegmentJobExecuting,
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, existingNodeID: 11,
			dispatchNodeID: 10, wantResolution: taskDispatchCleanupUntracked,
			wantState: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, wantNodeID: 11,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			ctx := context.Background()
			catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
			copyMeta, err := NewCopySegmentMeta(ctx, catalog, nil, nil, nil)
			s.Require().NoError(err)
			s.NoError(copyMeta.AddJob(ctx, &copySegmentJob{
				CopySegmentJob: &datapb.CopySegmentJob{JobId: 100, State: tc.jobState},
				tr:             timerecord.NewTimeRecorder("job"),
			}))
			task := &copySegmentTask{tr: timerecord.NewTimeRecorder("task"), times: taskcommon.NewTimes()}
			task.task.Store(&datapb.CopySegmentTask{
				TaskId: 1001, JobId: 100, State: tc.taskState, NodeId: tc.existingNodeID,
			})
			s.NoError(copyMeta.AddTask(ctx, task))

			resolution, err := copyMeta.CommitTaskDispatch(ctx, 1001, tc.dispatchNodeID, "job inactive")
			s.NoError(err)
			s.Equal(tc.wantResolution, resolution)
			updated := copyMeta.GetTask(ctx, 1001)
			s.Equal(tc.wantState, updated.GetState())
			s.Equal(tc.wantNodeID, updated.GetNodeId())
		})
	}
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
		NodeId:       NullNodeID,
	})

	err := s.copyMeta.AddTask(context.TODO(), task)
	s.NoError(err)

	// Verify task is added
	retrievedTask := s.copyMeta.GetTask(context.TODO(), 1001)
	s.NotNil(retrievedTask)
	s.Equal(int64(1001), retrievedTask.GetTaskId())
}

func (s *CopySegmentMetaSuite) TestAddTask_CatalogError() {
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

	err := s.copyMeta.AddTask(context.TODO(), task)
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
		NodeId:       NullNodeID,
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
		NodeId:       NullNodeID,
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

// addActiveCopyJob registers the parent job that a dispatch claim requires.
// ClaimTaskDispatch re-validates the job state under its own lock, so a task
// whose job is unknown to meta is deliberately not dispatchable.
func (s *CopySegmentMetaSuite) addActiveCopyJob(jobID int64) {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.NoError(s.copyMeta.AddJob(context.TODO(), newTestCopyJob(jobID,
		datapb.CopySegmentJobState_CopySegmentJobExecuting)))
}

func (s *CopySegmentMetaSuite) TestClaimTaskDispatch() {
	s.addActiveCopyJob(100)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(3)

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
		NodeId:       NullNodeID,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))

	// Each dispatch claims a distinct epoch, and it is persisted before the
	// worker can accept the task so it stays monotonic across a restart.
	first, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.NoError(err)
	s.EqualValues(1, first)
	s.EqualValues(1, s.copyMeta.GetTask(context.TODO(), 1001).GetTaskVersion())
	s.copyMeta.ReleaseTaskDispatch(1001)

	second, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.NoError(err)
	s.EqualValues(2, second)
	s.EqualValues(2, s.copyMeta.GetTask(context.TODO(), 1001).GetTaskVersion())
}

// TestClaimTaskDispatch_RefusesSecondClaimWhileInFlight is the regression for the
// cross-node double-dispatch hole: the scheduler pops a task out of pendingTasks
// and only inserts it into runningTasks after CommitTaskDispatch, so while the
// first dispatch is still in flight the task is invisible to Enqueue's dedup and
// gets dispatched a second time — possibly to a different node. The epoch fence
// lives on the worker runtime and so only covers a re-dispatch to the SAME node,
// and the untracked-drop gate only covers a dispatch that starts after the drop
// is queued. Neither closes this window, so the claim has to.
func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_RefusesSecondClaimWhileInFlight() {
	s.addActiveCopyJob(100)
	// Two saves only: AddTask and the single successful claim. A second claim
	// that reached the catalog would fail this expectation.
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

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
		NodeId:       NullNodeID,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))

	first, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.NoError(err)
	s.EqualValues(1, first)

	// The second dispatcher must be turned away before it can reach the worker,
	// and it must not burn an epoch either: the task stays exactly as the first
	// dispatch left it, so the scheduler simply re-queues it.
	second, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.ErrorIs(err, errCopySegmentDispatchInFlight)
	s.EqualValues(0, second)
	s.EqualValues(1, s.copyMeta.GetTask(context.TODO(), 1001).GetTaskVersion())
}

// TestClaimTaskDispatch_ReleaseIsPerTask keeps one task's in-flight dispatch from
// blocking every other task's.
func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_ReleaseIsPerTask() {
	s.addActiveCopyJob(100)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(4)

	for _, taskID := range []int64{1001, 1002} {
		task := &copySegmentTask{
			copyMeta: s.copyMeta,
			tr:       timerecord.NewTimeRecorder("task"),
			times:    taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       taskID,
			JobId:        100,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
			NodeId:       NullNodeID,
		})
		s.NoError(s.copyMeta.AddTask(context.TODO(), task))
	}

	_, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.NoError(err)

	// A different task is unaffected by 1001's in-flight dispatch.
	_, err = s.copyMeta.ClaimTaskDispatch(context.TODO(), 1002)
	s.NoError(err)

	// Releasing one task must not release the other.
	s.copyMeta.ReleaseTaskDispatch(1002)
	_, err = s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.ErrorIs(err, errCopySegmentDispatchInFlight)
}

func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_SaveFailureKeepsEpoch() {
	s.addActiveCopyJob(100)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(errors.New("etcd unavailable")).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()

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
		NodeId:       NullNodeID,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))

	// An unpersisted epoch must not be handed out: the dispatch is abandoned
	// instead, so no worker task can exist under an epoch metadata forgot.
	version, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.Error(err)
	s.EqualValues(0, version)
	s.EqualValues(0, s.copyMeta.GetTask(context.TODO(), 1001).GetTaskVersion())

	// A claim that never reached the worker must not be left holding the task:
	// nothing else releases it, so a leak here would stall the task forever.
	retried, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.NoError(err)
	s.EqualValues(1, retried)
}

func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_TaskNotFound() {
	version, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 9999)
	s.Error(err)
	s.NotErrorIs(err, errCopySegmentDispatchInFlight)
	s.EqualValues(0, version)

	// The failed claim left nothing behind for a task that may yet be created.
	s.False(s.copyMeta.(*copySegmentMeta).hasInFlightDispatch(9999))
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

// TestUpdateJobStateAndReleaseRef_UnpinsOnTerminal verifies that transitioning a
// job to Completed with PinId>0 unpins the source snapshot exactly once.
func TestUpdateJobStateAndReleaseRef_UnpinsOnTerminal(t *testing.T) {
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

	applied, err := copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, 1, unpinCalls, "UnpinSnapshot must be called exactly once on terminal transition")
	assert.Equal(t, int64(42), unpinPinID, "Unpin must be called with job.PinId")

	// Second terminal call: already terminal → must not Unpin again, and the
	// caller must be able to see that its transition was NOT applied.
	applied, err = copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	assert.NoError(t, err)
	assert.False(t, applied, "skipped transition must be distinguishable from an applied one")
	assert.Equal(t, 1, unpinCalls, "Double terminal transition must not double-unpin")
}

// TestUpdateJobStateAndReleaseRef_SkipsUnpinForLegacyJob verifies jobs persisted
// before the pin refactor (PinId=0) skip Unpin and do not panic on nil snapshotMeta.
func TestUpdateJobStateAndReleaseRef_SkipsUnpinForLegacyJob(t *testing.T) {
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

	applied, err := copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	assert.NoError(t, err)
	assert.True(t, applied, "transition applies even when no unpin is needed")
	assert.False(t, unpinCalled, "UnpinSnapshot must not be called for legacy job (PinId=0)")
}

// TestUpdateJobStateAndReleaseRef_UnpinErrorSwallowed verifies that when UnpinSnapshot
// returns an error the state transition is still persisted, and the caller receives nil.
// The pin is expected to self-expire via TTL — failing the state machine would double-drive it.
func TestUpdateJobStateAndReleaseRef_UnpinErrorSwallowed(t *testing.T) {
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

	applied, err := copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))

	assert.NoError(t, err, "unpin error must be swallowed — state transition already persisted")
	assert.True(t, applied, "the transition itself was applied; only the unpin failed")
	assert.Equal(t, 1, unpinCalls)
	// State transition happened despite unpin failure.
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, copyMeta.jobs[jobID].GetState())
}

// TestUpdateJobStateAndReleaseRef_NotFound verifies that updating a non-existent
// job is a no-op and does not error.
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_NotFound() {
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 999,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.NoError(err)
	s.False(applied)
}

// TestUpdateJobStateAndReleaseRef_CatalogError verifies that if the catalog save fails,
// job state is preserved (no partial transition).
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_CatalogError() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(errors.New("catalog error")).Once()

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

	// Update fails at catalog layer
	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 600,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	s.Error(err)
	s.False(applied)

	// Job should still be in Executing state (catalog write failed, in-memory unchanged)
	savedJob := s.copyMeta.GetJob(context.TODO(), 600)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, savedJob.GetState())
}

// TestUpdateJobStateAndReleaseRef_SkipsTerminalJob covers the terminal-state
// guard: a caller holding a stale non-terminal snapshot must not rewrite the
// outcome a concurrent path already persisted. This is the review-reported race
// where tryTimeoutJob runs after checkCopyingJob completed the job in the same
// checker round and would flip Completed -> Failed.
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_SkipsTerminalJob() {
	// Only the AddJob write is expected; the guarded update must not reach the catalog.
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(1)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        700,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			TotalRows:    42,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 700,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("timeout"))
	s.NoError(err)
	s.False(applied, "guarded update must report it was skipped")

	saved := s.copyMeta.GetJob(context.TODO(), 700)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, saved.GetState())
	s.Empty(saved.GetReason())
	s.Equal(int64(42), saved.GetTotalRows())
}

func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_SkipsPublishingJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        702,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 702,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("late failure"))
	s.NoError(err)
	s.False(applied)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobPublishing,
		s.copyMeta.GetJob(context.TODO(), 702).GetState())
}

// TestTimeoutJob_TakesPublishingJob: the deadline is the only exit from
// Publishing other than success. UpdateJobStateAndReleaseRef fences Publishing
// against every failure path (they act on a stale view of a job that claimed
// success); TimeoutJob is the one transition allowed through, so a persistent
// publication failure converges instead of retrying forever.
func (s *CopySegmentMetaSuite) TestTimeoutJob_TakesPublishingJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        703,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	applied, err := s.copyMeta.TimeoutJob(context.TODO(), 703, "timeout while publishing")
	s.NoError(err)
	s.True(applied)
	saved := s.copyMeta.GetJob(context.TODO(), 703)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, saved.GetState())
	s.Equal("timeout while publishing", saved.GetReason())

	// Failed is terminal: FinalizeJobPublication must now be a no-op.
	applied, err = s.copyMeta.FinalizeJobPublication(context.TODO(), 703, 0, 1)
	s.NoError(err)
	s.False(applied)
}

// TestTimeoutJob_SkipsTerminalJob: the terminal fence still holds — a job that
// completed (or failed) concurrently is never overwritten by a late timeout.
func (s *CopySegmentMetaSuite) TestTimeoutJob_SkipsTerminalJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        704,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			TotalRows:    9,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	applied, err := s.copyMeta.TimeoutJob(context.TODO(), 704, "timeout")
	s.NoError(err)
	s.False(applied)
	saved := s.copyMeta.GetJob(context.TODO(), 704)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, saved.GetState())
	s.Empty(saved.GetReason())
	s.EqualValues(9, saved.GetTotalRows())

	// Missing job: nothing applied, no error.
	applied, err = s.copyMeta.TimeoutJob(context.TODO(), 9999, "timeout")
	s.NoError(err)
	s.False(applied)
}

// TestFinalizeJobPublication_MirrorsUpdateSegmentsInfoEncoding: the publication
// hand-builds its catalog actions instead of going through meta.UpdateSegmentsInfo,
// so it pins the two ways it could silently diverge from it: binlog increments
// are refused (they would be dropped on the floor here), and segment records
// are written with the AlterSegments encoding, which keeps the V3 guard on
// binlog-based row-count reconciliation that the record-only encoding bypasses.
func (s *CopySegmentMetaSuite) TestFinalizeJobPublication_MirrorsUpdateSegmentsInfoEncoding() {
	ctx := context.TODO()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        705,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
			IdMappings:   []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 7051, PartitionId: 10}},
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(ctx, job))
	s.NoError(s.meta.AddSegment(ctx, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 7051, CollectionID: s.collectionID, PartitionID: 10, InsertChannel: "ch1",
		State: commonpb.SegmentState_Importing, IsImporting: true, NumOfRows: 3,
		// Every restore target looks V3 from pre-registration onward.
		StorageVersion: storage.StorageV3, ManifestPath: `{"ver":1,"base_path":"files/insert_log/1/10/7051"}`,
	}}))

	// An operator that produces a binlog increment is refused before any write.
	applied, err := s.copyMeta.FinalizeJobPublication(ctx, 705, 3, 1,
		[]UpdateOperator{
			UpdateStatusOperator(7051, commonpb.SegmentState_Flushed),
			AddBinlogsOperator(7051, []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 9, EntriesNum: 3}}}}, nil, nil, nil),
		})
	s.Error(err)
	s.False(applied)
	s.Contains(err.Error(), "must not carry binlog increments")
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobPublishing, s.copyMeta.GetJob(ctx, 705).GetState())
	s.Equal(commonpb.SegmentState_Importing, s.meta.GetSegment(ctx, 7051).GetState())

	// The visibility-only publication goes through: the segment chunk with the
	// AlterSegments encoding first, then the Completed job record as its own
	// final write — the marker that publication finished.
	var updateCalls int
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, actions ...metastore.UpdateAction) error {
			updateCalls++
			s.Require().Len(actions, 1)
			switch updateCalls {
			case 1:
				entry, ok := actions[0].Entry.(metastore.SegmentEntry)
				s.Require().True(ok)
				s.Equal(metastore.ActionUpdate, actions[0].Type)
				s.True(entry.AlterEncoding, "publication must use the AlterSegments encoding, not the record-only one")
				s.EqualValues(7051, entry.Segment.GetID())
				s.Equal(commonpb.SegmentState_Flushed, entry.Segment.GetState())
				s.False(entry.Segment.GetIsImporting())
			case 2:
				_, ok := actions[0].Entry.(metastore.CopySegmentJobEntry)
				s.True(ok, "the Completed job record is the commit marker and comes last")
			}
			return nil
		}).Times(2)
	applied, err = s.copyMeta.FinalizeJobPublication(ctx, 705, 3, 1,
		[]UpdateOperator{
			UpdateStatusOperator(7051, commonpb.SegmentState_Flushed),
			UpdateIsImporting(7051, false),
		})
	s.NoError(err)
	s.True(applied)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, s.copyMeta.GetJob(ctx, 705).GetState())
	s.Equal(commonpb.SegmentState_Flushed, s.meta.GetSegment(ctx, 7051).GetState())
}

// TestFinalizeJobPublication_ChunksSegmentWrites pins the bounded-lock-window
// property: a restore larger than copySegmentPublicationChunk must be
// published in several bounded catalog writes (each under its own
// m.mu + segMu window) instead of one composite write whose etcd fallback
// would hold the global segment lock across sequential transactions, and the
// Completed job record must still come last.
func (s *CopySegmentMetaSuite) TestFinalizeJobPublication_ChunksSegmentWrites() {
	ctx := context.TODO()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        708,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(ctx, job))

	total := copySegmentPublicationChunk + 1
	segmentOperators := make([][]UpdateOperator, 0, total)
	for i := 0; i < total; i++ {
		segID := int64(80000 + i)
		s.NoError(s.meta.AddSegment(ctx, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: segID, CollectionID: s.collectionID, PartitionID: 10, InsertChannel: "ch1",
			State: commonpb.SegmentState_Importing, IsImporting: true,
		}}))
		segmentOperators = append(segmentOperators, []UpdateOperator{
			UpdateStatusOperator(segID, commonpb.SegmentState_Flushed),
			UpdateIsImporting(segID, false),
		})
	}

	// The generated expecter matches variadic actions positionally, so the
	// full-chunk write needs one matcher per action.
	fullChunkArgs := make([]interface{}, 0, copySegmentPublicationChunk+1)
	for i := 0; i <= copySegmentPublicationChunk; i++ {
		fullChunkArgs = append(fullChunkArgs, mock.Anything)
	}
	var sequence []string
	record := func(args mock.Arguments) {
		n := len(args) - 1
		if n == 1 {
			if action, ok := args.Get(1).(metastore.UpdateAction); ok {
				if _, isJob := action.Entry.(metastore.CopySegmentJobEntry); isJob {
					sequence = append(sequence, "job")
					return
				}
			}
		}
		sequence = append(sequence, fmt.Sprintf("segments:%d", n))
	}
	s.catalog.Mock.On("Update", fullChunkArgs...).Run(record).Return(nil).Once()
	s.catalog.Mock.On("Update", mock.Anything, mock.Anything).Run(record).Return(nil).Times(2)

	applied, err := s.copyMeta.FinalizeJobPublication(ctx, 708, 9, 1, segmentOperators...)
	s.NoError(err)
	s.True(applied)
	// Full chunk, remainder chunk, then the job marker alone and last.
	s.Equal([]string{fmt.Sprintf("segments:%d", copySegmentPublicationChunk), "segments:1", "job"}, sequence)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, s.copyMeta.GetJob(ctx, 708).GetState())
	for i := 0; i < total; i++ {
		s.Equal(commonpb.SegmentState_Flushed, s.meta.GetSegment(ctx, int64(80000+i)).GetState())
	}
}

// TestFinalizeJobPublication_StopsWhenOutcomeClaimedBetweenChunks pins the
// per-chunk outcome fence: a terminal transition landing between chunks (the
// TimeoutJob deadline serializes with each chunk on m.mu) must stop the
// publication at the next chunk boundary — no further segment writes and no
// Completed record — leaving the already-published chunks to the failed-job
// cleanup exactly like a crash would.
func (s *CopySegmentMetaSuite) TestFinalizeJobPublication_StopsWhenOutcomeClaimedBetweenChunks() {
	ctx := context.TODO()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        709,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(ctx, job))

	total := copySegmentPublicationChunk + 1
	segmentOperators := make([][]UpdateOperator, 0, total)
	for i := 0; i < total; i++ {
		segID := int64(90000 + i)
		s.NoError(s.meta.AddSegment(ctx, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: segID, CollectionID: s.collectionID, PartitionID: 10, InsertChannel: "ch1",
			State: commonpb.SegmentState_Importing, IsImporting: true,
		}}))
		segmentOperators = append(segmentOperators, []UpdateOperator{
			UpdateStatusOperator(segID, commonpb.SegmentState_Flushed),
		})
	}

	// The first chunk lands; a concurrent timeout claims the outcome before the
	// second. The catalog callback runs on the goroutine that holds m.mu, so
	// mutating the cached job here is exactly a transition serialized between
	// this chunk and the next one's re-check. Only this one write is expected:
	// a second chunk write or a Completed record would be an unexpected call
	// and fail the test.
	fullChunkArgs := make([]interface{}, 0, copySegmentPublicationChunk+1)
	for i := 0; i <= copySegmentPublicationChunk; i++ {
		fullChunkArgs = append(fullChunkArgs, mock.Anything)
	}
	s.catalog.Mock.On("Update", fullChunkArgs...).Run(func(mock.Arguments) {
		cm := s.copyMeta.(*copySegmentMeta)
		failed := cm.jobs[709].Clone()
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed)(failed)
		cm.jobs[709] = failed
	}).Return(nil).Once()

	applied, err := s.copyMeta.FinalizeJobPublication(ctx, 709, 9, 1, segmentOperators...)
	s.NoError(err)
	s.False(applied)
	// Only one catalog write happened (the mock allows exactly one), the job
	// was never marked Completed, and the second chunk's segment stayed hidden.
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, s.copyMeta.GetJob(ctx, 709).GetState())
	s.Equal(commonpb.SegmentState_Importing,
		s.meta.GetSegment(ctx, int64(90000+copySegmentPublicationChunk)).GetState())
}

// TestUpdateJobStateAndReleaseRef_AppliesOnNonTerminalJob is the positive
// counterpart: the guard must not block the legitimate Executing -> Failed
// transition that the timeout and fail-fast paths rely on.
func (s *CopySegmentMetaSuite) TestUpdateJobStateAndReleaseRef_AppliesOnNonTerminalJob() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Times(2)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        701,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	applied, err := s.copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), 701,
		UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed),
		UpdateCopyJobReason("timeout"))
	s.NoError(err)
	s.True(applied)

	saved := s.copyMeta.GetJob(context.TODO(), 701)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, saved.GetState())
	s.Equal("timeout", saved.GetReason())
}

// addJobWithTask is a helper for the ResolveTaskOnWorkerLoss cases.
func (s *CopySegmentMetaSuite) addJobWithTask(jobID, taskID int64,
	jobState datapb.CopySegmentJobState, taskState datapb.CopySegmentTaskState,
) {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        jobID,
			CollectionId: s.collectionID,
			State:        jobState,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}
	s.NoError(s.copyMeta.AddJob(context.TODO(), job))

	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       taskID,
		JobId:        jobID,
		CollectionId: s.collectionID,
		State:        taskState,
		NodeId:       7,
		TaskVersion:  3,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))
}

// TestResolveTaskOnWorkerLoss_RedispatchesWhenJobActive: while the parent job
// is still Executing, a lost task must be reset to Pending with NullNodeID so
// the scheduler re-dispatches it to a live node.
func (s *CopySegmentMetaSuite) TestResolveTaskOnWorkerLoss_RedispatchesWhenJobActive() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	s.addJobWithTask(710, 1710,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress)

	outcome, err := s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 1710, "worker lost")
	s.NoError(err)
	s.Equal(workerLossRedispatched, outcome.resolution)
	// The outcome names the exact dispatch that was cleared so the caller can
	// queue its worker-side cleanup: the node it was on and its epoch.
	s.EqualValues(7, outcome.nodeID)
	s.EqualValues(3, outcome.taskVersion)

	saved := s.copyMeta.GetTask(context.TODO(), 1710)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskPending, saved.GetState())
	s.EqualValues(NullNodeID, saved.GetNodeId())
	s.Empty(saved.GetReason())
}

// TestResolveTaskOnWorkerLoss_ConvergesToFailedWhenJobTerminal is the
// review-reported case: a delayed worker-loss response for a task whose parent
// job already failed must NOT revive it to Pending (one extra dispatch for a
// dead job), but it also must not stay InProgress on the dead node — that
// would block checkGC forever, since GC skips tasks with a node assignment and
// nothing else clears it. The task converges to Failed with NullNodeID.
func (s *CopySegmentMetaSuite) TestResolveTaskOnWorkerLoss_ConvergesToFailedWhenJobTerminal() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)

	s.addJobWithTask(711, 1711,
		datapb.CopySegmentJobState_CopySegmentJobFailed,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress)

	outcome, err := s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 1711, "worker lost after job failed")
	s.NoError(err)
	s.Equal(workerLossFailed, outcome.resolution)
	s.EqualValues(7, outcome.nodeID)
	s.EqualValues(3, outcome.taskVersion)

	saved := s.copyMeta.GetTask(context.TODO(), 1711)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, saved.GetState())
	s.EqualValues(NullNodeID, saved.GetNodeId(), "assignment must be cleared so checkGC can reclaim the task")
	s.Equal("worker lost after job failed", saved.GetReason())
}

// TestResolveTaskOnWorkerLoss_SkipsWhenTaskStateMismatch: a task that has
// already left InProgress (e.g. checkFailedJob marked it Failed) must be left
// untouched by a late worker-loss response.
func (s *CopySegmentMetaSuite) TestResolveTaskOnWorkerLoss_SkipsWhenTaskStateMismatch() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	// Only the AddTask write; the skipped resolution must not reach the catalog.
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(1)

	s.addJobWithTask(712, 1712,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		datapb.CopySegmentTaskState_CopySegmentTaskFailed)

	outcome, err := s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 1712, "worker lost")
	s.NoError(err)
	s.Equal(workerLossSkipped, outcome.resolution)
	s.EqualValues(NullNodeID, outcome.nodeID, "a skipped resolution cleared nothing and names no dispatch")

	saved := s.copyMeta.GetTask(context.TODO(), 1712)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, saved.GetState())
}

// TestResolveTaskOnWorkerLoss_MissingTaskOrJob covers the lookup misses: a
// missing task is skipped; a task whose parent job is absent converges to
// Failed (it can never progress and must not block GC).
func (s *CopySegmentMetaSuite) TestResolveTaskOnWorkerLoss_MissingTaskOrJob() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Maybe()

	// Task does not exist at all.
	outcome, err := s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 9999, "worker lost")
	s.NoError(err)
	s.Equal(workerLossSkipped, outcome.resolution)

	// Task exists but its parent job is absent from meta: same as a terminal
	// job — the task is unrecoverable and must converge to Failed.
	orphan := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	orphan.task.Store(&datapb.CopySegmentTask{
		TaskId:       1713,
		JobId:        713, // no such job
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
		NodeId:       7,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), orphan))

	outcome, err = s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 1713, "worker lost")
	s.NoError(err)
	s.Equal(workerLossFailed, outcome.resolution)
	saved := s.copyMeta.GetTask(context.TODO(), 1713)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskFailed, saved.GetState())
	s.EqualValues(NullNodeID, saved.GetNodeId())
}

// TestResolveTaskOnWorkerLoss_CatalogErrorLeavesTaskUnchanged ensures a failed
// persist does not mutate the in-memory task, on both resolution branches.
func (s *CopySegmentMetaSuite) TestResolveTaskOnWorkerLoss_CatalogErrorLeavesTaskUnchanged() {
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).
		Return(errors.New("catalog down")).Once()

	s.addJobWithTask(714, 1714,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
		datapb.CopySegmentTaskState_CopySegmentTaskInProgress)

	outcome, err := s.copyMeta.ResolveTaskOnWorkerLoss(context.TODO(), 1714, "worker lost")
	s.Error(err)
	s.Equal(workerLossSkipped, outcome.resolution)

	saved := s.copyMeta.GetTask(context.TODO(), 1714)
	s.Equal(datapb.CopySegmentTaskState_CopySegmentTaskInProgress, saved.GetState())
	s.Equal(int64(7), saved.GetNodeId())
}

// newClaimTestTask registers a Pending, unassigned task ready to be claimed.
func (s *CopySegmentMetaSuite) newClaimTestTask(taskID, jobID int64) {
	task := &copySegmentTask{
		copyMeta: s.copyMeta,
		tr:       timerecord.NewTimeRecorder("task"),
		times:    taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       taskID,
		JobId:        jobID,
		CollectionId: s.collectionID,
		State:        datapb.CopySegmentTaskState_CopySegmentTaskPending,
		NodeId:       NullNodeID,
	})
	s.NoError(s.copyMeta.AddTask(context.TODO(), task))
}

// TestClaimTaskDispatch_RevalidatesPreconditions is the regression for promoting
// a stale observation into a second dispatch. The caller samples the task state,
// its assignment, the job state and the pending-cleanup gate BEFORE
// AssembleCopySegmentRequest, whose remote snapshot read can outlast several
// inspector rounds while the task is invisible to the scheduler's dedup. The
// claim is the only point that serializes a dispatch against its predecessor's
// committed outcome, so every precondition has to be re-checked here — otherwise
// a second dispatch reaches another node and its own cleanup deletes the
// winner's output from the deterministic target keys the two share.
func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_RevalidatesPreconditions() {
	testCases := []struct {
		name      string
		taskState datapb.CopySegmentTaskState
		nodeID    int64
	}{
		{
			name:      "already dispatched elsewhere",
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
			nodeID:    7,
		},
		{
			name:      "already completed",
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			nodeID:    NullNodeID,
		},
		{
			name:      "already failed",
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskFailed,
			nodeID:    NullNodeID,
		},
		{
			name:      "still pending but assigned",
			taskState: datapb.CopySegmentTaskState_CopySegmentTaskPending,
			nodeID:    7,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.SetupTest()
			s.addActiveCopyJob(100)
			// One save for AddTask, one for the state the case sets up. A claim
			// that persisted an epoch would exceed this expectation.
			s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Times(2)
			s.newClaimTestTask(1001, 100)
			s.NoError(s.copyMeta.UpdateTask(context.TODO(), 1001,
				UpdateCopyTaskState(tc.taskState),
				UpdateCopyTaskNodeID(tc.nodeID)))

			version, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
			s.ErrorIs(err, errCopySegmentDispatchStale)
			s.EqualValues(0, version)

			// No epoch burned and no claim held: the refused dispatch leaves the
			// task exactly as it found it.
			s.EqualValues(0, s.copyMeta.GetTask(context.TODO(), 1001).GetTaskVersion())
			s.False(s.copyMeta.(*copySegmentMeta).hasInFlightDispatch(1001))
		})
	}
}

// TestClaimTaskDispatch_RefusesInactiveJob covers the job-level half of the same
// revalidation: a job that ended while the snapshot was being read must not get
// another worker task written against a snapshot whose pin has been released.
func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_RefusesInactiveJob() {
	for _, jobState := range []datapb.CopySegmentJobState{
		datapb.CopySegmentJobState_CopySegmentJobFailed,
		datapb.CopySegmentJobState_CopySegmentJobCompleted,
	} {
		s.Run(jobState.String(), func() {
			s.SetupTest()
			s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
			s.NoError(s.copyMeta.AddJob(context.TODO(), newTestCopyJob(100, jobState)))
			s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
			s.newClaimTestTask(1001, 100)

			version, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
			s.ErrorIs(err, errCopySegmentDispatchStale)
			s.EqualValues(0, version)
			s.False(s.copyMeta.(*copySegmentMeta).hasInFlightDispatch(1001))
		})
	}
}

// TestClaimTaskDispatch_RefusesUnknownJob keeps a task whose job meta has already
// forgotten from being dispatched: CommitTaskDispatch treats a missing job as
// inactive, so a dispatch granted here could only ever be cleaned up again.
func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_RefusesUnknownJob() {
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.newClaimTestTask(1001, 100)

	version, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.ErrorIs(err, errCopySegmentDispatchStale)
	s.EqualValues(0, version)
	s.False(s.copyMeta.(*copySegmentMeta).hasInFlightDispatch(1001))
}

// TestClaimTaskDispatch_RefusesWhilePendingUntrackedDrop moves the
// pending-cleanup gate under the serializing lock. Checked only at the caller,
// it is sampled before the snapshot read, so a queued abort for an earlier
// dispatch can be registered while this dispatch is still assembling — and that
// abort deletes from the exact target keys this dispatch would write.
func (s *CopySegmentMetaSuite) TestClaimTaskDispatch_RefusesWhilePendingUntrackedDrop() {
	s.addActiveCopyJob(100)
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.newClaimTestTask(1001, 100)

	handler := &fakeUntrackedDropHandler{pending: map[int64]bool{1001: true}}
	s.copyMeta.(*copySegmentMeta).setUntrackedDropHandler(handler)

	version, err := s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.ErrorIs(err, errCopySegmentDispatchStale)
	s.EqualValues(0, version)
	s.False(s.copyMeta.(*copySegmentMeta).hasInFlightDispatch(1001))

	// Once the earlier dispatch is provably gone the claim succeeds.
	handler.pending[1001] = false
	s.catalog.EXPECT().SaveCopySegmentTask(mock.Anything, mock.Anything).Return(nil).Once()
	version, err = s.copyMeta.ClaimTaskDispatch(context.TODO(), 1001)
	s.NoError(err)
	s.EqualValues(1, version)
}

// TestFinalizeJobPublication_ReleasesSnapshotCache pins the memory contract of
// the successful-restore completion path. Clone() shares snapshotCache by
// pointer, and that cache holds the whole SnapshotData (every segment
// description, MB-scale for a large restore). Completed is terminal, so the
// cache can never be read again; the sibling terminal path
// (UpdateJobStateAndReleaseRef) already drops it. Without the same release
// here, the SnapshotData stays resident on the Completed job until checkGC
// collects it CopySegmentTaskRetention (3h) later, so back-to-back large
// restores accumulate in DataCoord.
func (s *CopySegmentMetaSuite) TestFinalizeJobPublication_ReleasesSnapshotCache() {
	ctx := context.TODO()
	s.catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Once()
	s.catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(nil).Once()

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        707,
			CollectionId: s.collectionID,
			State:        datapb.CopySegmentJobState_CopySegmentJobPublishing,
		},
		tr:            timerecord.NewTimeRecorder("test job"),
		snapshotCache: &copySegmentSnapshotCache{data: &snapshotstorage.SnapshotData{}},
	}
	s.NoError(s.copyMeta.AddJob(ctx, job))
	s.NotNil(s.copyMeta.GetJob(ctx, 707).(*copySegmentJob).snapshotCache)

	applied, err := s.copyMeta.FinalizeJobPublication(ctx, 707, 0, 1)
	s.NoError(err)
	s.True(applied)
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, s.copyMeta.GetJob(ctx, 707).GetState())
	s.Nil(s.copyMeta.GetJob(ctx, 707).(*copySegmentJob).snapshotCache,
		"a completed job must not pin the snapshot cache")
}
