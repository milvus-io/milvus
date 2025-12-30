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

package external

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type UpdateExternalTaskSuite struct {
	suite.Suite
	collectionID int64
	taskID       int64
}

func (s *UpdateExternalTaskSuite) SetupSuite() {
	s.collectionID = 1000
	s.taskID = 1
}

func (s *UpdateExternalTaskSuite) TestNewUpdateExternalTask() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   "test_spec",
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	s.NotNil(task)
	s.Equal(s.collectionID, task.req.GetCollectionID())
	s.Equal(s.taskID, task.req.GetTaskID())
	s.Equal(indexpb.JobState_JobStateInit, task.GetState())
	s.Contains(task.Name(), "UpdateExternalTask")
}

func (s *UpdateExternalTaskSuite) TestTaskLifecycle() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Test OnEnqueue
	err := task.OnEnqueue(ctx)
	s.NoError(err)

	// Test PreExecute
	err = task.PreExecute(ctx)
	s.NoError(err)

	// Test Execute
	err = task.Execute(ctx)
	s.NoError(err)

	// Test PostExecute
	err = task.PostExecute(ctx)
	s.NoError(err)

	// Test GetSlot
	s.Equal(int64(1), task.GetSlot())
}

func (s *UpdateExternalTaskSuite) TestPreExecuteWithNilRequest() {
	ctx, cancel := context.WithCancel(context.Background())
	task := &UpdateExternalTask{
		ctx:    ctx,
		cancel: cancel,
		req:    nil,
	}

	err := task.PreExecute(ctx)
	s.Error(err)
}

func (s *UpdateExternalTaskSuite) TestSetAndGetState() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	task.SetState(indexpb.JobState_JobStateInProgress, "")
	s.Equal(indexpb.JobState_JobStateInProgress, task.GetState())

	task.SetState(indexpb.JobState_JobStateFailed, "test failure")
	s.Equal(indexpb.JobState_JobStateFailed, task.GetState())
	s.Equal("test failure", task.failReason)
}

func (s *UpdateExternalTaskSuite) TestReset() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	task.Reset()

	s.Nil(task.ctx)
	s.Nil(task.cancel)
	s.Nil(task.req)
	s.Nil(task.tr)
	s.Nil(task.updatedSegments)
}

func (s *UpdateExternalTaskSuite) TestBalanceFragmentsToSegments_Empty() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	result, err := task.balanceFragmentsToSegments(context.Background(), []Fragment{})
	s.NoError(err)
	s.Nil(result)
}

func (s *UpdateExternalTaskSuite) TestBalanceFragmentsToSegments_SingleFragment() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	fragments := []Fragment{
		{FragmentID: 1, RowCount: 500},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)
	s.Len(result, 1)
	s.Equal(int64(500), result[0].GetNumOfRows())
}

func (s *UpdateExternalTaskSuite) TestBalanceFragmentsToSegments_MultipleFragments() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	fragments := []Fragment{
		{FragmentID: 1, RowCount: 300000},
		{FragmentID: 2, RowCount: 400000},
		{FragmentID: 3, RowCount: 500000},
		{FragmentID: 4, RowCount: 600000},
		{FragmentID: 5, RowCount: 200000},
	}

	result, err := task.balanceFragmentsToSegments(context.Background(), fragments)
	s.NoError(err)

	// Verify total rows are preserved
	var totalRows int64
	for _, seg := range result {
		totalRows += seg.GetNumOfRows()
	}
	s.Equal(int64(2000000), totalRows)

	// Verify segments are reasonably balanced
	if len(result) > 1 {
		var minRows, maxRows int64 = result[0].GetNumOfRows(), result[0].GetNumOfRows()
		for _, seg := range result {
			if seg.GetNumOfRows() < minRows {
				minRows = seg.GetNumOfRows()
			}
			if seg.GetNumOfRows() > maxRows {
				maxRows = seg.GetNumOfRows()
			}
		}
		// The difference between max and min should be reasonable
		// (less than 2x the average fragment size)
		avgFragmentSize := int64(2000000 / 5)
		s.Less(maxRows-minRows, avgFragmentSize*2)
	}
}

func (s *UpdateExternalTaskSuite) TestPreExecuteContextCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	cancel()

	err := task.PreExecute(ctx)
	s.ErrorIs(err, context.Canceled)
}

func (s *UpdateExternalTaskSuite) TestExecuteContextCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	cancel()

	err := task.Execute(ctx)
	s.ErrorIs(err, context.Canceled)
}

func (s *UpdateExternalTaskSuite) TestBalanceFragmentsToSegmentsContextCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
	}

	task := NewUpdateExternalTask(ctx, cancel, req)
	cancel()

	result, err := task.balanceFragmentsToSegments(ctx, []Fragment{{FragmentID: 1, RowCount: 10}})
	s.ErrorIs(err, context.Canceled)
	s.Nil(result)
}

func (s *UpdateExternalTaskSuite) TestOrganizeSegments_AllFragmentsExist() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Simulate current segment fragments mapping
	currentSegmentFragments := SegmentFragments{
		1: []Fragment{{FragmentID: 101, RowCount: 1000}},
		2: []Fragment{{FragmentID: 102, RowCount: 2000}},
	}

	// New fragments contain all existing fragments
	newFragments := []Fragment{
		{FragmentID: 101, RowCount: 1000},
		{FragmentID: 102, RowCount: 2000},
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// Both segments should be kept
	s.Len(result, 2)
}

func (s *UpdateExternalTaskSuite) TestOrganizeSegments_FragmentRemoved() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Segment 1 has fragment 101, Segment 2 has fragments 102 and 103
	currentSegmentFragments := SegmentFragments{
		1: []Fragment{{FragmentID: 101, RowCount: 1000}},
		2: []Fragment{{FragmentID: 102, RowCount: 1000}, {FragmentID: 103, RowCount: 1000}},
	}

	// Fragment 103 is removed - segment 2 should be invalidated
	newFragments := []Fragment{
		{FragmentID: 101, RowCount: 1000},
		{FragmentID: 102, RowCount: 1000},
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// Segment 1 should be kept, segment 2 invalidated, fragment 102 becomes orphan
	// Result should have segment 1 kept + new segment for orphan fragment 102
	s.GreaterOrEqual(len(result), 1)

	// Verify segment 1 is in the result
	hasSegment1 := false
	for _, seg := range result {
		if seg.GetID() == 1 {
			hasSegment1 = true
			break
		}
	}
	s.True(hasSegment1, "Segment 1 should be kept")
}

func (s *UpdateExternalTaskSuite) TestOrganizeSegments_NewFragmentsAdded() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	currentSegmentFragments := SegmentFragments{
		1: []Fragment{{FragmentID: 101, RowCount: 1000}},
	}

	// New fragments include existing + new ones
	newFragments := []Fragment{
		{FragmentID: 101, RowCount: 1000},
		{FragmentID: 102, RowCount: 2000}, // new
		{FragmentID: 103, RowCount: 3000}, // new
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// Should have segment 1 kept + new segments for orphan fragments
	s.GreaterOrEqual(len(result), 2)

	// Verify total rows
	var totalRows int64
	for _, seg := range result {
		totalRows += seg.GetNumOfRows()
	}
	s.Equal(int64(6000), totalRows)
}

func (s *UpdateExternalTaskSuite) TestNewSegmentRowMapping() {
	fragments := []Fragment{
		{FragmentID: 1, RowCount: 100},
		{FragmentID: 2, RowCount: 200},
		{FragmentID: 3, RowCount: 150},
	}

	mapping := NewSegmentRowMapping(1001, fragments)

	s.Equal(int64(1001), mapping.SegmentID)
	s.Equal(int64(450), mapping.TotalRows)
	s.Len(mapping.Ranges, 3)

	// Check ranges
	s.Equal(int64(1), mapping.Ranges[0].FragmentID)
	s.Equal(int64(0), mapping.Ranges[0].StartRow)
	s.Equal(int64(100), mapping.Ranges[0].EndRow)

	s.Equal(int64(2), mapping.Ranges[1].FragmentID)
	s.Equal(int64(100), mapping.Ranges[1].StartRow)
	s.Equal(int64(300), mapping.Ranges[1].EndRow)

	s.Equal(int64(3), mapping.Ranges[2].FragmentID)
	s.Equal(int64(300), mapping.Ranges[2].StartRow)
	s.Equal(int64(450), mapping.Ranges[2].EndRow)
}

func (s *UpdateExternalTaskSuite) TestGetFragmentByRowIndex() {
	fragments := []Fragment{
		{FragmentID: 1, RowCount: 100},
		{FragmentID: 2, RowCount: 200},
		{FragmentID: 3, RowCount: 150},
	}
	mapping := NewSegmentRowMapping(1001, fragments)

	// Test first fragment
	r := mapping.GetFragmentByRowIndex(0)
	s.NotNil(r)
	s.Equal(int64(1), r.FragmentID)

	r = mapping.GetFragmentByRowIndex(99)
	s.NotNil(r)
	s.Equal(int64(1), r.FragmentID)

	// Test second fragment
	r = mapping.GetFragmentByRowIndex(100)
	s.NotNil(r)
	s.Equal(int64(2), r.FragmentID)

	r = mapping.GetFragmentByRowIndex(299)
	s.NotNil(r)
	s.Equal(int64(2), r.FragmentID)

	// Test third fragment
	r = mapping.GetFragmentByRowIndex(300)
	s.NotNil(r)
	s.Equal(int64(3), r.FragmentID)

	r = mapping.GetFragmentByRowIndex(449)
	s.NotNil(r)
	s.Equal(int64(3), r.FragmentID)

	// Test out of range
	r = mapping.GetFragmentByRowIndex(-1)
	s.Nil(r)

	r = mapping.GetFragmentByRowIndex(450)
	s.Nil(r)

	r = mapping.GetFragmentByRowIndex(1000)
	s.Nil(r)
}

func (s *UpdateExternalTaskSuite) TestGetFragmentByRowIndex_LocalIndex() {
	fragments := []Fragment{
		{FragmentID: 1, RowCount: 100},
		{FragmentID: 2, RowCount: 200},
	}
	mapping := NewSegmentRowMapping(1001, fragments)

	// Row 0 -> fragment 1, local index 0
	r := mapping.GetFragmentByRowIndex(0)
	s.NotNil(r)
	s.Equal(int64(1), r.FragmentID)
	s.Equal(int64(0), 0-r.StartRow) // local index

	// Row 50 -> fragment 1, local index 50
	r = mapping.GetFragmentByRowIndex(50)
	s.NotNil(r)
	s.Equal(int64(1), r.FragmentID)
	s.Equal(int64(50), 50-r.StartRow)

	// Row 100 -> fragment 2, local index 0
	r = mapping.GetFragmentByRowIndex(100)
	s.NotNil(r)
	s.Equal(int64(2), r.FragmentID)
	s.Equal(int64(0), 100-r.StartRow)

	// Row 150 -> fragment 2, local index 50
	r = mapping.GetFragmentByRowIndex(150)
	s.NotNil(r)
	s.Equal(int64(2), r.FragmentID)
	s.Equal(int64(50), 150-r.StartRow)

	// Row 299 -> fragment 2, local index 199
	r = mapping.GetFragmentByRowIndex(299)
	s.NotNil(r)
	s.Equal(int64(2), r.FragmentID)
	s.Equal(int64(199), 299-r.StartRow)
}

func (s *UpdateExternalTaskSuite) TestSegmentRowMapping_EmptyFragments() {
	mapping := NewSegmentRowMapping(1001, []Fragment{})

	s.Equal(int64(0), mapping.TotalRows)
	s.Len(mapping.Ranges, 0)

	r := mapping.GetFragmentByRowIndex(0)
	s.Nil(r)
}

func (s *UpdateExternalTaskSuite) TestMappingsComputedDuringOrganize() {
	ctx, cancel := context.WithCancel(context.Background())
	// Use segment ID 100 to avoid collision with placeholder ID (1)
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 100, CollectionID: s.collectionID, NumOfRows: 1000},
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Simulate current segment has fragment 101
	currentSegmentFragments := SegmentFragments{
		100: []Fragment{{FragmentID: 101, RowCount: 1000}},
	}

	// New fragments include existing + new ones
	newFragments := []Fragment{
		{FragmentID: 101, RowCount: 1000},
		{FragmentID: 102, RowCount: 500},
	}

	_, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	mappings := task.GetSegmentMappings()
	s.Len(mappings, 2)

	// Check mapping for kept segment (ID=100)
	mapping100 := mappings[100]
	s.NotNil(mapping100)
	s.Equal(int64(1000), mapping100.TotalRows)
	s.Len(mapping100.Ranges, 1)
	s.Equal(int64(101), mapping100.Ranges[0].FragmentID)

	// Check mapping for new segment (ID=1, placeholder)
	mapping1 := mappings[1]
	s.NotNil(mapping1)
	s.Equal(int64(500), mapping1.TotalRows)
	s.Len(mapping1.Ranges, 1)
	s.Equal(int64(102), mapping1.Ranges[0].FragmentID)
}

func TestUpdateExternalTaskSuite(t *testing.T) {
	suite.Run(t, new(UpdateExternalTaskSuite))
}
