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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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
		ExternalSpec:   `{"format":"parquet"}`,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "id"},
			},
		},
		StorageConfig: &indexpb.StorageConfig{
			StorageType: "local",
		},
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
		},
		PreAllocatedSegmentIds: &datapb.IDRange{
			Begin: 1000,
			End:   2000,
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Test OnEnqueue
	err := task.OnEnqueue(ctx)
	s.NoError(err)

	// Test PreExecute - validates schema, storage config, external source
	err = task.PreExecute(ctx)
	s.NoError(err)
	s.NotNil(task.parsedSpec)
	s.Equal("parquet", task.parsedSpec.Format)
	s.Equal([]string{"id"}, task.columns)

	// Test PostExecute (Execute skipped as it requires CGO FFI calls)
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
	result, err := task.balanceFragmentsToSegments(context.Background(), []packed.Fragment{})
	s.NoError(err)
	s.Nil(result)
}

func (s *UpdateExternalTaskSuite) TestBalanceFragmentsToSegments_SingleFragment() {
	s.T().Skip("Skip test that requires CGO FFI calls")
}

func (s *UpdateExternalTaskSuite) TestBalanceFragmentsToSegments_MultipleFragments() {
	s.T().Skip("Skip test that requires CGO FFI calls")
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

	result, err := task.balanceFragmentsToSegments(ctx, []packed.Fragment{{FragmentID: 1, RowCount: 10}})
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

	// Simulate current segment fragments mapping (use FilePath as identifier)
	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", RowCount: 1000}},
		2: []packed.Fragment{{FragmentID: 102, FilePath: "/data/file2.parquet", RowCount: 2000}},
	}

	// New fragments contain all existing fragments
	newFragments := []packed.Fragment{
		{FragmentID: 101, FilePath: "/data/file1.parquet", RowCount: 1000},
		{FragmentID: 102, FilePath: "/data/file2.parquet", RowCount: 2000},
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// Both segments should be kept
	s.Len(result, 2)

	// Verify kept/new tracking
	s.Equal([]int64{1, 2}, task.GetKeptSegmentIDs())
	s.Empty(task.GetNewSegments())
}

func (s *UpdateExternalTaskSuite) TestOrganizeSegments_FragmentRemoved() {
	s.T().Skip("Skip test that requires CGO FFI calls for manifest creation")
}

func (s *UpdateExternalTaskSuite) TestOrganizeSegments_PartialFragmentRemoved() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID: s.collectionID,
		TaskID:       s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: s.collectionID, NumOfRows: 1000},
			{ID: 2, CollectionID: s.collectionID, NumOfRows: 2000},
			{ID: 3, CollectionID: s.collectionID, NumOfRows: 1500},
		},
		PreAllocatedSegmentIds: &datapb.IDRange{Begin: 100, End: 200},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// S1 has file1, S2 has file2, S3 has file3
	currentSegmentFragments := packed.SegmentFragments{
		1: []packed.Fragment{{FragmentID: 101, FilePath: "/data/file1.parquet", RowCount: 1000}},
		2: []packed.Fragment{{FragmentID: 102, FilePath: "/data/file2.parquet", RowCount: 2000}},
		3: []packed.Fragment{{FragmentID: 103, FilePath: "/data/file3.parquet", RowCount: 1500}},
	}

	// New fragments: file1 and file3 still exist, file2 removed
	newFragments := []packed.Fragment{
		{FragmentID: 201, FilePath: "/data/file1.parquet", RowCount: 1000},
		{FragmentID: 203, FilePath: "/data/file3.parquet", RowCount: 1500},
	}

	result, err := task.organizeSegments(context.Background(), currentSegmentFragments, newFragments)
	s.NoError(err)

	// S1 and S3 should be kept, S2 should be invalidated
	// No new segments because there are no orphan fragments (all new fragments match kept segments)
	s.Len(result, 2)
	s.Equal([]int64{1, 3}, task.GetKeptSegmentIDs())
	s.Empty(task.GetNewSegments(), "No orphan fragments to create new segments from")
}

func (s *UpdateExternalTaskSuite) TestOrganizeSegments_NewFragmentsAdded() {
	s.T().Skip("Skip test that requires CGO FFI calls for manifest creation")
}

func (s *UpdateExternalTaskSuite) TestNewSegmentRowMapping() {
	fragments := []packed.Fragment{
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
	fragments := []packed.Fragment{
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
	fragments := []packed.Fragment{
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
	mapping := NewSegmentRowMapping(1001, []packed.Fragment{})

	s.Equal(int64(0), mapping.TotalRows)
	s.Len(mapping.Ranges, 0)

	r := mapping.GetFragmentByRowIndex(0)
	s.Nil(r)
}

func (s *UpdateExternalTaskSuite) TestMappingsComputedDuringOrganize() {
	s.T().Skip("Skip test that requires CGO FFI calls for manifest creation")
}

func (s *UpdateExternalTaskSuite) TestSplitFileToFragments_SmallFile() {
	// File smaller than limit - should return single fragment
	fragments := packed.SplitFileToFragments("/data/small.parquet", 500000, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 1)
	s.Equal(int64(0), fragments[0].FragmentID)
	s.Equal("/data/small.parquet", fragments[0].FilePath)
	s.Equal(int64(0), fragments[0].StartRow)
	s.Equal(int64(500000), fragments[0].EndRow)
	s.Equal(int64(500000), fragments[0].RowCount)
}

func (s *UpdateExternalTaskSuite) TestSplitFileToFragments_ExactLimit() {
	// File exactly at limit - should return single fragment
	fragments := packed.SplitFileToFragments("/data/exact.parquet", packed.DefaultFragmentRowLimit, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 1)
	s.Equal(int64(0), fragments[0].FragmentID)
	s.Equal(int64(0), fragments[0].StartRow)
	s.Equal(int64(packed.DefaultFragmentRowLimit), fragments[0].EndRow)
	s.Equal(int64(packed.DefaultFragmentRowLimit), fragments[0].RowCount)
}

func (s *UpdateExternalTaskSuite) TestSplitFileToFragments_LargeFile() {
	// File with 2.5 million rows - should split into 3 fragments
	totalRows := int64(2500000)
	fragments := packed.SplitFileToFragments("/data/large.parquet", totalRows, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 3)

	// First fragment: 0-1000000
	s.Equal(int64(0), fragments[0].FragmentID)
	s.Equal("/data/large.parquet", fragments[0].FilePath)
	s.Equal(int64(0), fragments[0].StartRow)
	s.Equal(int64(1000000), fragments[0].EndRow)
	s.Equal(int64(1000000), fragments[0].RowCount)

	// Second fragment: 1000000-2000000
	s.Equal(int64(1), fragments[1].FragmentID)
	s.Equal("/data/large.parquet", fragments[1].FilePath)
	s.Equal(int64(1000000), fragments[1].StartRow)
	s.Equal(int64(2000000), fragments[1].EndRow)
	s.Equal(int64(1000000), fragments[1].RowCount)

	// Third fragment: 2000000-2500000
	s.Equal(int64(2), fragments[2].FragmentID)
	s.Equal("/data/large.parquet", fragments[2].FilePath)
	s.Equal(int64(2000000), fragments[2].StartRow)
	s.Equal(int64(2500000), fragments[2].EndRow)
	s.Equal(int64(500000), fragments[2].RowCount)
}

func (s *UpdateExternalTaskSuite) TestSplitFileToFragments_BaseFragmentID() {
	// Test with non-zero base fragment ID
	fragments := packed.SplitFileToFragments("/data/test.parquet", 2500000, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(100))

	s.Len(fragments, 3)
	s.Equal(int64(100), fragments[0].FragmentID)
	s.Equal(int64(101), fragments[1].FragmentID)
	s.Equal(int64(102), fragments[2].FragmentID)
}

func (s *UpdateExternalTaskSuite) TestSplitFileToFragments_ZeroRows() {
	// Empty file - should return single fragment with zero rows
	fragments := packed.SplitFileToFragments("/data/empty.parquet", 0, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 1)
	s.Equal(int64(0), fragments[0].RowCount)
}

func (s *UpdateExternalTaskSuite) TestSplitFileToFragments_TenMillionRows() {
	// 10 million rows - should split into 10 fragments
	totalRows := int64(10000000)
	fragments := packed.SplitFileToFragments("/data/huge.parquet", totalRows, packed.DefaultFragmentRowLimit, packed.NewFragmentIDGenerator(0))

	s.Len(fragments, 10)

	// Verify total rows across all fragments
	var totalFragmentRows int64
	for _, f := range fragments {
		totalFragmentRows += f.RowCount
	}
	s.Equal(totalRows, totalFragmentRows)

	// Verify fragment IDs are sequential
	for i, f := range fragments {
		s.Equal(int64(i), f.FragmentID)
	}

	// Verify ranges are continuous
	for i := 1; i < len(fragments); i++ {
		s.Equal(fragments[i-1].EndRow, fragments[i].StartRow)
	}
}

func (s *UpdateExternalTaskSuite) TestCreateManifestForSegment() {
	// Skip: createManifestForSegment calls packed.CreateSegmentManifestWithBasePath which requires CGO FFI
	s.T().Skip("Skip test that requires CGO FFI calls for manifest creation")
}

func (s *UpdateExternalTaskSuite) TestPreAllocatedSegmentIDs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request with pre-allocated IDs
	idRange := &datapb.IDRange{
		Begin: 1000,
		End:   2000,
	}

	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: idRange,
		NumSegmentsExpected:    1000,
		CurrentSegments:        []*datapb.SegmentInfo{},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Before Execute(), pre-allocated fields should not be initialized
	s.Nil(task.preallocatedIDRange)
	s.Equal(int64(0), task.nextAllocID)

	// After testing with pre-allocated IDs manually set (simulating Execute)
	task.preallocatedIDRange = idRange
	task.nextAllocID = idRange.Begin

	// Verify pre-allocated IDs are properly initialized
	s.NotNil(task.preallocatedIDRange)
	s.Equal(int64(1000), task.preallocatedIDRange.Begin)
	s.Equal(int64(2000), task.preallocatedIDRange.End)
	s.Equal(int64(1000), task.nextAllocID)
}

func (s *UpdateExternalTaskSuite) TestPreAllocatedIDAllocation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request with pre-allocated IDs
	idRange := &datapb.IDRange{
		Begin: 5000,
		End:   5010, // Only 10 IDs available
	}

	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: idRange,
		NumSegmentsExpected:    10,
		CurrentSegments:        []*datapb.SegmentInfo{},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Manually initialize (simulating Execute)
	task.preallocatedIDRange = idRange
	task.nextAllocID = idRange.Begin

	// Simulate creating multiple segments by advancing nextAllocID
	// This should work fine up to the limit
	for i := 0; i < 10; i++ {
		s.True(task.nextAllocID < task.preallocatedIDRange.End, "Should have IDs available")
		task.nextAllocID++
	}

	// Now we should be out of IDs
	s.Equal(task.preallocatedIDRange.End, task.nextAllocID)
}

func (s *UpdateExternalTaskSuite) TestMissingPreAllocatedIDs() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request WITHOUT pre-allocated IDs
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:    s.collectionID,
		TaskID:          s.taskID,
		CurrentSegments: []*datapb.SegmentInfo{},
		// PreAllocatedSegmentIds is nil
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Execute should fail because pre-allocated IDs are missing
	err := task.Execute(ctx)
	s.NotNil(err)
	s.Contains(err.Error(), "pre-allocated segment IDs not provided")
}

func (s *UpdateExternalTaskSuite) TestGetSegmentResults() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create request with pre-allocated IDs
	idRange := &datapb.IDRange{
		Begin: 1000,
		End:   2000,
	}

	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:           s.collectionID,
		TaskID:                 s.taskID,
		PreAllocatedSegmentIds: idRange,
		CurrentSegments: []*datapb.SegmentInfo{
			{ID: 100, CollectionID: s.collectionID, NumOfRows: 1000},
		},
	}

	task := NewUpdateExternalTask(ctx, cancel, req)

	// Simulate task execution result with real pre-allocated IDs
	task.keptSegmentIDs = []int64{100}
	task.newSegments = []*datapb.SegmentInfo{
		{ID: 1000, NumOfRows: 2000},
		{ID: 1001, NumOfRows: 1500},
	}
	task.updatedSegments = []*datapb.SegmentInfo{
		{ID: 100, NumOfRows: 1000},  // Kept segment (from currentSegments)
		{ID: 1000, NumOfRows: 2000}, // New segment (from pre-allocated range)
		{ID: 1001, NumOfRows: 1500}, // New segment (from pre-allocated range)
	}

	task.segmentMappings = map[int64]*SegmentRowMapping{
		100:  {SegmentID: 100, TotalRows: 1000},
		1000: {SegmentID: 1000, TotalRows: 2000},
		1001: {SegmentID: 1001, TotalRows: 1500},
	}

	// Get results
	results := task.GetSegmentResults()

	// Verify
	s.Equal(3, len(results))

	// Check kept segment
	s.Equal(int64(100), results[0].Segment.GetID())
	s.False(results[0].IsNew, "Segment 100 should not be new (it's in currentSegments)")

	// Check new segments
	s.Equal(int64(1000), results[1].Segment.GetID())
	s.True(results[1].IsNew, "Segment 1000 should be new (not in currentSegments)")

	s.Equal(int64(1001), results[2].Segment.GetID())
	s.True(results[2].IsNew, "Segment 1001 should be new (not in currentSegments)")

	// Verify kept/new getters
	s.Equal([]int64{100}, task.GetKeptSegmentIDs())
	s.Len(task.GetNewSegments(), 2)
	s.Equal(int64(1000), task.GetNewSegments()[0].GetID())
	s.Equal(int64(1001), task.GetNewSegments()[1].GetID())
}

func (s *UpdateExternalTaskSuite) TestPreExecute_NilSchema() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
		// Schema is nil
	}
	task := NewUpdateExternalTask(ctx, cancel, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "schema is nil")
}

func (s *UpdateExternalTaskSuite) TestPreExecute_NilStorageConfig() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		Schema:         &schemapb.CollectionSchema{},
		// StorageConfig is nil
	}
	task := NewUpdateExternalTask(ctx, cancel, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "storage config is nil")
}

func (s *UpdateExternalTaskSuite) TestPreExecute_EmptyExternalSource() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:  s.collectionID,
		TaskID:        s.taskID,
		Schema:        &schemapb.CollectionSchema{},
		StorageConfig: &indexpb.StorageConfig{StorageType: "local"},
		// ExternalSource is empty
	}
	task := NewUpdateExternalTask(ctx, cancel, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "external source is empty")
}

func (s *UpdateExternalTaskSuite) TestPreExecute_InvalidExternalSpec() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   "not-valid-json",
		Schema:         &schemapb.CollectionSchema{},
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
	}
	task := NewUpdateExternalTask(ctx, cancel, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "failed to parse external spec")
}

func (s *UpdateExternalTaskSuite) TestPreExecute_UnsupportedFormat() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &datapb.UpdateExternalCollectionRequest{
		CollectionID:   s.collectionID,
		TaskID:         s.taskID,
		ExternalSource: "test_source",
		ExternalSpec:   `{"format":"avro"}`,
		Schema:         &schemapb.CollectionSchema{},
		StorageConfig:  &indexpb.StorageConfig{StorageType: "local"},
	}
	task := NewUpdateExternalTask(ctx, cancel, req)
	err := task.PreExecute(ctx)
	s.Error(err)
	s.Contains(err.Error(), "unsupported format")
}

func (s *UpdateExternalTaskSuite) TestParseExternalSpec() {
	// Empty spec defaults to parquet
	spec, err := ParseExternalSpec("")
	s.NoError(err)
	s.Equal("parquet", spec.Format)

	// Explicit format
	spec, err = ParseExternalSpec(`{"format":"csv"}`)
	s.NoError(err)
	s.Equal("csv", spec.Format)

	// JSON format
	spec, err = ParseExternalSpec(`{"format":"json"}`)
	s.NoError(err)
	s.Equal("json", spec.Format)

	// Missing format defaults to parquet
	spec, err = ParseExternalSpec(`{}`)
	s.NoError(err)
	s.Equal("parquet", spec.Format)

	// Invalid JSON
	_, err = ParseExternalSpec("not-json")
	s.Error(err)

	// Unsupported format
	_, err = ParseExternalSpec(`{"format":"avro"}`)
	s.Error(err)
	s.Contains(err.Error(), "unsupported format")
}

func (s *UpdateExternalTaskSuite) TestFragmentKey() {
	f1 := packed.Fragment{FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000}
	f2 := packed.Fragment{FilePath: "/data/file1.parquet", StartRow: 1000, EndRow: 2000}
	f3 := packed.Fragment{FilePath: "/data/file2.parquet", StartRow: 0, EndRow: 1000}

	// Same file, different ranges should produce different keys
	s.NotEqual(fragmentKey(f1), fragmentKey(f2))

	// Different files, same ranges should produce different keys
	s.NotEqual(fragmentKey(f1), fragmentKey(f3))

	// Identical fragments should produce the same key
	f4 := packed.Fragment{FilePath: "/data/file1.parquet", StartRow: 0, EndRow: 1000}
	s.Equal(fragmentKey(f1), fragmentKey(f4))
}

func (s *UpdateExternalTaskSuite) TestGetColumnNamesFromSchema() {
	// Nil schema
	columns := packed.GetColumnNamesFromSchema(nil)
	s.Nil(columns)

	// Schema with ExternalField set
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "vector", ExternalField: "embedding"},
		},
	}
	columns = packed.GetColumnNamesFromSchema(schema)
	s.Equal([]string{"external_id", "embedding"}, columns)

	// Schema without ExternalField (fallback to field name)
	schema2 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id"},
			{Name: "vector"},
		},
	}
	columns = packed.GetColumnNamesFromSchema(schema2)
	s.Equal([]string{"id", "vector"}, columns)

	// Mixed: some with ExternalField, some without
	schema3 := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "vector"},
		},
	}
	columns = packed.GetColumnNamesFromSchema(schema3)
	s.Equal([]string{"external_id", "vector"}, columns)
}

func TestUpdateExternalTaskSuite(t *testing.T) {
	suite.Run(t, new(UpdateExternalTaskSuite))
}
