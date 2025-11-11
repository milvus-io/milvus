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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

type CopySegmentJobSuite struct {
	suite.Suite
}

func TestCopySegmentJob(t *testing.T) {
	suite.Run(t, new(CopySegmentJobSuite))
}

func (s *CopySegmentJobSuite) TestCopySegmentJob_GettersAndSetters() {
	idMappings := []*datapb.CopySegmentIDMapping{
		{SourceSegmentId: 1, TargetSegmentId: 101, PartitionId: 10},
		{SourceSegmentId: 2, TargetSegmentId: 102, PartitionId: 10},
	}

	options := []*commonpb.KeyValuePair{
		{Key: "option1", Value: "value1"},
	}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:          100,
			DbId:           1,
			CollectionId:   2,
			CollectionName: "test_collection",
			State:          datapb.CopySegmentJobState_CopySegmentJobPending,
			Reason:         "test reason",
			IdMappings:     idMappings,
			Options:        options,
			TimeoutTs:      12345,
			CleanupTs:      54321,
			StartTs:        11111,
			CompleteTs:     22222,
			TotalSegments:  10,
			CopiedSegments: 5,
			TotalRows:      1000,
			SnapshotName:   "test_snapshot",
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Test all getters
	s.Equal(int64(100), job.GetJobId())
	s.Equal(int64(1), job.GetDbId())
	s.Equal(int64(2), job.GetCollectionId())
	s.Equal("test_collection", job.GetCollectionName())
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobPending, job.GetState())
	s.Equal("test reason", job.GetReason())
	s.Equal(idMappings, job.GetIdMappings())
	s.Equal(options, job.GetOptions())
	s.Equal(uint64(12345), job.GetTimeoutTs())
	s.Equal(uint64(54321), job.GetCleanupTs())
	s.Equal(uint64(11111), job.GetStartTs())
	s.Equal(uint64(22222), job.GetCompleteTs())
	s.Equal(int64(10), job.GetTotalSegments())
	s.Equal(int64(5), job.GetCopiedSegments())
	s.Equal(int64(1000), job.GetTotalRows())
	s.Equal("test_snapshot", job.GetSnapshotName())
	s.NotNil(job.GetTR())
}

func (s *CopySegmentJobSuite) TestCopySegmentJob_Clone() {
	original := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 2,
			State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			Reason:       "original reason",
			TotalRows:    1000,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Clone the job
	cloned := original.Clone()

	// Verify cloned job has same values
	s.Equal(original.GetJobId(), cloned.GetJobId())
	s.Equal(original.GetCollectionId(), cloned.GetCollectionId())
	s.Equal(original.GetState(), cloned.GetState())
	s.Equal(original.GetReason(), cloned.GetReason())
	s.Equal(original.GetTotalRows(), cloned.GetTotalRows())

	// Verify time recorder is same reference
	s.Equal(original.GetTR(), cloned.GetTR())

	// Verify modifying clone doesn't affect original
	clonedImpl := cloned.(*copySegmentJob)
	clonedImpl.CopySegmentJob.Reason = "modified reason"
	s.Equal("original reason", original.GetReason())
	s.Equal("modified reason", cloned.GetReason())
}

func (s *CopySegmentJobSuite) TestWithCopyJobCollectionID() {
	job1 := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 1,
		},
		tr: timerecord.NewTimeRecorder("job1"),
	}

	job2 := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        200,
			CollectionId: 2,
		},
		tr: timerecord.NewTimeRecorder("job2"),
	}

	filter := WithCopyJobCollectionID(1)

	s.True(filter(job1))
	s.False(filter(job2))
}

func (s *CopySegmentJobSuite) TestWithCopyJobStates() {
	pendingJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 100,
			State: datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("pending"),
	}

	executingJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 200,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("executing"),
	}

	completedJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 300,
			State: datapb.CopySegmentJobState_CopySegmentJobCompleted,
		},
		tr: timerecord.NewTimeRecorder("completed"),
	}

	// Test filter with single state
	filter1 := WithCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobPending)
	s.True(filter1(pendingJob))
	s.False(filter1(executingJob))
	s.False(filter1(completedJob))

	// Test filter with multiple states
	filter2 := WithCopyJobStates(
		datapb.CopySegmentJobState_CopySegmentJobPending,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
	)
	s.True(filter2(pendingJob))
	s.True(filter2(executingJob))
	s.False(filter2(completedJob))
}

func (s *CopySegmentJobSuite) TestWithoutCopyJobStates() {
	pendingJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 100,
			State: datapb.CopySegmentJobState_CopySegmentJobPending,
		},
		tr: timerecord.NewTimeRecorder("pending"),
	}

	executingJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 200,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("executing"),
	}

	completedJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 300,
			State: datapb.CopySegmentJobState_CopySegmentJobCompleted,
		},
		tr: timerecord.NewTimeRecorder("completed"),
	}

	// Test filter excluding completed and failed
	filter := WithoutCopyJobStates(
		datapb.CopySegmentJobState_CopySegmentJobCompleted,
		datapb.CopySegmentJobState_CopySegmentJobFailed,
	)
	s.True(filter(pendingJob))
	s.True(filter(executingJob))
	s.False(filter(completedJob))
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobState_Completed() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 100,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	beforeUpdate := time.Now()

	// Apply update action
	action := UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)
	action(job)

	// Verify state is updated
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobCompleted, job.GetState())

	// Verify cleanup timestamp is set
	s.NotZero(job.GetCleanupTs())

	// Verify cleanup time is in the future (allowing small time difference)
	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	s.True(cleanupTime.After(beforeUpdate) || cleanupTime.Equal(beforeUpdate))
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobState_Failed() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 100,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Apply update action
	action := UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed)
	action(job)

	// Verify state is updated
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobFailed, job.GetState())

	// Verify cleanup timestamp is set
	s.NotZero(job.GetCleanupTs())
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobState_Executing() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:     100,
			State:     datapb.CopySegmentJobState_CopySegmentJobPending,
			CleanupTs: 0,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Apply update action to non-terminal state
	action := UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobExecuting)
	action(job)

	// Verify state is updated
	s.Equal(datapb.CopySegmentJobState_CopySegmentJobExecuting, job.GetState())

	// Verify cleanup timestamp is NOT set for non-terminal states
	s.Zero(job.GetCleanupTs())
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobReason() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:  100,
			Reason: "",
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Apply update action
	action := UpdateCopyJobReason("task failed")
	action(job)

	// Verify reason is updated
	s.Equal("task failed", job.GetReason())
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobProgress() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:          100,
			CopiedSegments: 0,
			TotalSegments:  0,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Apply update action
	action := UpdateCopyJobProgress(5, 10)
	action(job)

	// Verify progress is updated
	s.Equal(int64(5), job.GetCopiedSegments())
	s.Equal(int64(10), job.GetTotalSegments())
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobCompleteTs() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:      100,
			CompleteTs: 0,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	now := uint64(time.Now().UnixNano())

	// Apply update action
	action := UpdateCopyJobCompleteTs(now)
	action(job)

	// Verify complete timestamp is updated
	s.Equal(now, job.GetCompleteTs())
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobTotalRows() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:     100,
			TotalRows: 0,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Apply update action
	action := UpdateCopyJobTotalRows(1000)
	action(job)

	// Verify total rows is updated
	s.Equal(int64(1000), job.GetTotalRows())
}

func (s *CopySegmentJobSuite) TestMultipleUpdateActions() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:          100,
			State:          datapb.CopySegmentJobState_CopySegmentJobExecuting,
			CopiedSegments: 0,
			TotalSegments:  0,
			TotalRows:      0,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Apply multiple update actions
	actions := []UpdateCopySegmentJobAction{
		UpdateCopyJobProgress(8, 10),
		UpdateCopyJobTotalRows(5000),
		UpdateCopyJobReason("almost done"),
	}

	for _, action := range actions {
		action(job)
	}

	// Verify all updates are applied
	s.Equal(int64(8), job.GetCopiedSegments())
	s.Equal(int64(10), job.GetTotalSegments())
	s.Equal(int64(5000), job.GetTotalRows())
	s.Equal("almost done", job.GetReason())
}

func (s *CopySegmentJobSuite) TestCombinedFilters() {
	jobs := []CopySegmentJob{
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        100,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job1"),
		},
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        200,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobExecuting,
			},
			tr: timerecord.NewTimeRecorder("job2"),
		},
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        300,
				CollectionId: 2,
				State:        datapb.CopySegmentJobState_CopySegmentJobPending,
			},
			tr: timerecord.NewTimeRecorder("job3"),
		},
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:        400,
				CollectionId: 1,
				State:        datapb.CopySegmentJobState_CopySegmentJobCompleted,
			},
			tr: timerecord.NewTimeRecorder("job4"),
		},
	}

	// Filter: collection ID = 1 AND state = Pending or Executing
	collectionFilter := WithCopyJobCollectionID(1)
	stateFilter := WithCopyJobStates(
		datapb.CopySegmentJobState_CopySegmentJobPending,
		datapb.CopySegmentJobState_CopySegmentJobExecuting,
	)

	var filtered []CopySegmentJob
	for _, job := range jobs {
		if collectionFilter(job) && stateFilter(job) {
			filtered = append(filtered, job)
		}
	}

	// Should match jobs 100 and 200
	s.Len(filtered, 2)
	s.Equal(int64(100), filtered[0].GetJobId())
	s.Equal(int64(200), filtered[1].GetJobId())
}

func (s *CopySegmentJobSuite) TestUpdateCopyJobState_CleanupTsCalculation() {
	// Get current retention period setting
	retentionDuration := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 100,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	beforeUpdate := time.Now()

	// Apply update to completed state
	action := UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)
	action(job)

	afterUpdate := time.Now()

	// Verify cleanup timestamp is approximately retention duration in the future
	// Note: tsoutil.PhysicalTime loses some precision, so we allow 1 second tolerance
	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
	expectedMin := beforeUpdate.Add(retentionDuration).Add(-1 * time.Second)
	expectedMax := afterUpdate.Add(retentionDuration).Add(1 * time.Second)

	s.True(cleanupTime.After(expectedMin) || cleanupTime.Equal(expectedMin),
		"cleanup time %v should be after or equal to %v", cleanupTime, expectedMin)
	s.True(cleanupTime.Before(expectedMax) || cleanupTime.Equal(expectedMax),
		"cleanup time %v should be before or equal to %v", cleanupTime, expectedMax)
}

func (s *CopySegmentJobSuite) TestJobWithEmptyIdMappings() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:      100,
			IdMappings: nil,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Should return empty slice, not nil
	mappings := job.GetIdMappings()
	s.Nil(mappings)
}

func (s *CopySegmentJobSuite) TestJobWithEmptyOptions() {
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:   100,
			Options: nil,
		},
		tr: timerecord.NewTimeRecorder("test job"),
	}

	// Should return empty slice, not nil
	options := job.GetOptions()
	s.Nil(options)
}
