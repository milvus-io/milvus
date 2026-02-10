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
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// --- Test CreateSnapshot ---

func TestSnapshotManager_CreateSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	// Mock allocator to return snapshot ID
	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	// Mock handler to generate snapshot data
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			CollectionId: 100,
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, NumOfRows: 100},
		},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	// Mock snapshotMeta methods using mockey
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found") // Name doesn't exist
	}).Build()
	defer mockGetSnapshot.UnPatch()

	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *SnapshotData) error {
		// Verify snapshot data was set correctly
		assert.Equal(t, int64(1001), data.SnapshotInfo.Id)
		assert.Equal(t, "test_snapshot", data.SnapshotInfo.Name)
		assert.Equal(t, "test description", data.SnapshotInfo.Description)
		return nil
	}).Build()
	defer mockSaveSnapshot.UnPatch()

	// Create snapshot manager
	sm := NewSnapshotManager(
		nil,             // meta
		&snapshotMeta{}, // snapshotMeta
		nil,             // copySegmentMeta
		mockAllocator,
		mockHandler,
		nil, // broker
		nil, // getChannelsFunc
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "test description")

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, int64(1001), snapshotID)
}

func TestSnapshotManager_CreateSnapshot_DuplicateName(t *testing.T) {
	ctx := context.Background()

	// Mock snapshotMeta.GetSnapshot to return existing snapshot
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return &datapb.SnapshotInfo{Id: 1, Name: name}, nil // Name already exists
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "existing_snapshot", "description")

	// Verify
	assert.Error(t, err)
	assert.Equal(t, int64(0), snapshotID)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
	assert.Contains(t, err.Error(), "already exists")
}

func TestSnapshotManager_CreateSnapshot_AllocatorError(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	expectedErr := errors.New("allocator error")
	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(0), expectedErr).Once()

	// Mock snapshotMeta.GetSnapshot to return not found
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		mockAllocator,
		nil,
		nil,
		nil,
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "description")

	// Verify
	assert.Error(t, err)
	assert.Equal(t, int64(0), snapshotID)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotManager_CreateSnapshot_GenSnapshotError(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	expectedErr := errors.New("gen snapshot error")
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(nil, expectedErr).Once()

	// Mock snapshotMeta.GetSnapshot to return not found
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "description")

	// Verify
	assert.Error(t, err)
	assert.Equal(t, int64(0), snapshotID)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotManager_CreateSnapshot_SaveError(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	// Mock snapshotMeta methods
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	expectedErr := errors.New("save error")
	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *SnapshotData) error {
		return expectedErr
	}).Build()
	defer mockSaveSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "description")

	// Verify
	assert.Error(t, err)
	assert.Equal(t, int64(0), snapshotID)
	assert.Equal(t, expectedErr, err)
}

// --- Test DropSnapshot ---

func TestSnapshotManager_DropSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	// Mock GetSnapshot to return existing snapshot
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return &datapb.SnapshotInfo{Id: 1, Name: name}, nil
	}).Build()
	defer mockGetSnapshot.UnPatch()

	mockDropSnapshot := mockey.Mock((*snapshotMeta).DropSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) error {
		assert.Equal(t, "test_snapshot", name)
		return nil
	}).Build()
	defer mockDropSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	err := sm.DropSnapshot(ctx, "test_snapshot")

	// Verify
	assert.NoError(t, err)
}

func TestSnapshotManager_DropSnapshot_NotFound_Idempotent(t *testing.T) {
	ctx := context.Background()

	// Mock GetSnapshot to return not found (snapshot doesn't exist)
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute - should succeed even if snapshot doesn't exist (idempotent)
	err := sm.DropSnapshot(ctx, "nonexistent_snapshot")

	// Verify
	assert.NoError(t, err)
}

func TestSnapshotManager_DropSnapshot_Error(t *testing.T) {
	ctx := context.Background()

	// Mock GetSnapshot to return existing snapshot
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return &datapb.SnapshotInfo{Id: 1, Name: name}, nil
	}).Build()
	defer mockGetSnapshot.UnPatch()

	expectedErr := errors.New("drop error")
	mockDropSnapshot := mockey.Mock((*snapshotMeta).DropSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) error {
		return expectedErr
	}).Build()
	defer mockDropSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	err := sm.DropSnapshot(ctx, "test_snapshot")

	// Verify
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// --- Test GetSnapshot ---

func TestSnapshotManager_GetSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	expectedInfo := &datapb.SnapshotInfo{
		Id:           1001,
		Name:         "test_snapshot",
		CollectionId: 100,
	}

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		assert.Equal(t, "test_snapshot", name)
		return expectedInfo, nil
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	info, err := sm.GetSnapshot(ctx, "test_snapshot")

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedInfo, info)
}

func TestSnapshotManager_GetSnapshot_NotFound(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("snapshot not found")
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, name string) (*datapb.SnapshotInfo, error) {
		return nil, expectedErr
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	info, err := sm.GetSnapshot(ctx, "nonexistent")

	// Verify
	assert.Error(t, err)
	assert.Nil(t, info)
	assert.Equal(t, expectedErr, err)
}

// --- Test DescribeSnapshot ---

func TestSnapshotManager_DescribeSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	expectedData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1001,
			Name:         "test_snapshot",
			CollectionId: 100,
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, NumOfRows: 100},
		},
	}

	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, name string, includeSegments bool) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		assert.False(t, includeSegments)
		return expectedData, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	data, err := sm.DescribeSnapshot(ctx, "test_snapshot")

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedData, data)
}

func TestSnapshotManager_DescribeSnapshot_NotFound(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("snapshot not found")
	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, name string, includeSegments bool) (*SnapshotData, error) {
		return nil, expectedErr
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	data, err := sm.DescribeSnapshot(ctx, "nonexistent")

	// Verify
	assert.Error(t, err)
	assert.Nil(t, data)
	assert.Equal(t, expectedErr, err)
}

// --- Test ListSnapshots ---

func TestSnapshotManager_ListSnapshots_Success(t *testing.T) {
	ctx := context.Background()

	expectedSnapshots := []string{"snapshot1", "snapshot2", "snapshot3"}

	mockListSnapshots := mockey.Mock((*snapshotMeta).ListSnapshots).To(func(sm *snapshotMeta, ctx context.Context, collectionID, partitionID int64) ([]string, error) {
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, int64(0), partitionID)
		return expectedSnapshots, nil
	}).Build()
	defer mockListSnapshots.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	snapshots, err := sm.ListSnapshots(ctx, 100, 0)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedSnapshots, snapshots)
}

func TestSnapshotManager_ListSnapshots_Error(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("list error")
	mockListSnapshots := mockey.Mock((*snapshotMeta).ListSnapshots).To(func(sm *snapshotMeta, ctx context.Context, collectionID, partitionID int64) ([]string, error) {
		return nil, expectedErr
	}).Build()
	defer mockListSnapshots.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	snapshots, err := sm.ListSnapshots(ctx, 100, 0)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, snapshots)
	assert.Equal(t, expectedErr, err)
}

// --- Test GetRestoreState ---

func TestSnapshotManager_GetRestoreState_Success(t *testing.T) {
	ctx := context.Background()

	// Create a real copy segment job for testing
	testJob := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:          2001,
			SnapshotName:   "test_snapshot",
			CollectionId:   100,
			State:          datapb.CopySegmentJobState_CopySegmentJobExecuting,
			TotalSegments:  10,
			CopiedSegments: 5,
			StartTs:        1000000000,
			CompleteTs:     0,
		},
	}

	// Mock copySegmentMeta.GetJob using mockey
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(csm *copySegmentMeta, ctx context.Context, jobID int64) CopySegmentJob {
		if jobID == 2001 {
			return testJob
		}
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	sm := NewSnapshotManager(
		nil,
		nil,
		&copySegmentMeta{},
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	info, err := sm.GetRestoreState(ctx, 2001)

	// Verify
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, int64(2001), info.GetJobId())
	assert.Equal(t, "test_snapshot", info.GetSnapshotName())
	assert.Equal(t, datapb.RestoreSnapshotState_RestoreSnapshotExecuting, info.GetState())
	assert.Equal(t, int32(50), info.GetProgress()) // 5/10 * 100 = 50%
}

func TestSnapshotManager_GetRestoreState_NotFound(t *testing.T) {
	ctx := context.Background()

	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(csm *copySegmentMeta, ctx context.Context, jobID int64) CopySegmentJob {
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	sm := NewSnapshotManager(
		nil,
		nil,
		&copySegmentMeta{},
		nil,
		nil,
		nil,
		nil,
	)

	// Execute
	info, err := sm.GetRestoreState(ctx, 9999)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, info)
}

// --- Test ListRestoreJobs ---

func TestSnapshotManager_ListRestoreJobs_Success(t *testing.T) {
	ctx := context.Background()

	testJobs := []CopySegmentJob{
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          1,
				SnapshotName:   "snapshot1",
				CollectionId:   100,
				State:          datapb.CopySegmentJobState_CopySegmentJobCompleted,
				TotalSegments:  10,
				CopiedSegments: 10,
			},
		},
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          2,
				SnapshotName:   "snapshot2",
				CollectionId:   200,
				State:          datapb.CopySegmentJobState_CopySegmentJobPending,
				TotalSegments:  5,
				CopiedSegments: 0,
			},
		},
	}

	mockGetJobBy := mockey.Mock((*copySegmentMeta).GetJobBy).To(func(csm *copySegmentMeta, ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob {
		return testJobs
	}).Build()
	defer mockGetJobBy.UnPatch()

	sm := NewSnapshotManager(
		&meta{},
		nil,
		&copySegmentMeta{},
		nil,
		nil,
		nil,
		nil,
	)

	// Execute - no filter
	jobs, err := sm.ListRestoreJobs(ctx, 0)

	// Verify
	assert.NoError(t, err)
	assert.Len(t, jobs, 2)
}

func TestSnapshotManager_ListRestoreJobs_FilterByCollectionID(t *testing.T) {
	ctx := context.Background()

	testJobs := []CopySegmentJob{
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          1,
				SnapshotName:   "snapshot1",
				CollectionId:   100,
				State:          datapb.CopySegmentJobState_CopySegmentJobCompleted,
				TotalSegments:  10,
				CopiedSegments: 10,
			},
		},
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          2,
				SnapshotName:   "snapshot2",
				CollectionId:   200,
				State:          datapb.CopySegmentJobState_CopySegmentJobPending,
				TotalSegments:  5,
				CopiedSegments: 0,
			},
		},
		&copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:          3,
				SnapshotName:   "snapshot3",
				CollectionId:   100,
				State:          datapb.CopySegmentJobState_CopySegmentJobExecuting,
				TotalSegments:  8,
				CopiedSegments: 4,
			},
		},
	}

	mockGetJobBy := mockey.Mock((*copySegmentMeta).GetJobBy).To(func(csm *copySegmentMeta, ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob {
		return testJobs
	}).Build()
	defer mockGetJobBy.UnPatch()

	sm := NewSnapshotManager(
		&meta{},
		nil,
		&copySegmentMeta{},
		nil,
		nil,
		nil,
		nil,
	)

	// Execute - filter by collection ID 100
	jobs, err := sm.ListRestoreJobs(ctx, 100)

	// Verify - should return 2 jobs for collection 100
	assert.NoError(t, err)
	assert.Len(t, jobs, 2)
	for _, job := range jobs {
		assert.Equal(t, int64(100), job.GetCollectionId())
	}

	// Execute - filter by collection ID 200
	jobs, err = sm.ListRestoreJobs(ctx, 200)

	// Verify - should return 1 job for collection 200
	assert.NoError(t, err)
	assert.Len(t, jobs, 1)
	assert.Equal(t, int64(200), jobs[0].GetCollectionId())
	assert.Equal(t, int64(2), jobs[0].GetJobId())

	// Execute - filter by non-existent collection ID
	jobs, err = sm.ListRestoreJobs(ctx, 999)

	// Verify - should return 0 jobs
	assert.NoError(t, err)
	assert.Len(t, jobs, 0)
}

// --- Test Helper Functions ---

func TestSnapshotManager_ConvertJobState(t *testing.T) {
	sm := &snapshotManager{}

	tests := []struct {
		input    datapb.CopySegmentJobState
		expected datapb.RestoreSnapshotState
	}{
		{datapb.CopySegmentJobState_CopySegmentJobPending, datapb.RestoreSnapshotState_RestoreSnapshotPending},
		{datapb.CopySegmentJobState_CopySegmentJobExecuting, datapb.RestoreSnapshotState_RestoreSnapshotExecuting},
		{datapb.CopySegmentJobState_CopySegmentJobCompleted, datapb.RestoreSnapshotState_RestoreSnapshotCompleted},
		{datapb.CopySegmentJobState_CopySegmentJobFailed, datapb.RestoreSnapshotState_RestoreSnapshotFailed},
		{datapb.CopySegmentJobState(999), datapb.RestoreSnapshotState_RestoreSnapshotNone}, // Unknown state
	}

	for _, tt := range tests {
		result := sm.convertJobState(tt.input)
		assert.Equal(t, tt.expected, result)
	}
}

func TestSnapshotManager_CalculateProgress(t *testing.T) {
	sm := &snapshotManager{}

	tests := []struct {
		name           string
		totalSegments  int64
		copiedSegments int64
		expected       int32
	}{
		{"0% progress", 10, 0, 0},
		{"50% progress", 10, 5, 50},
		{"100% progress", 10, 10, 100},
		{"zero total", 0, 0, 100}, // No segments to copy means 100% complete
		{"partial progress", 3, 1, 33},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &copySegmentJob{
				CopySegmentJob: &datapb.CopySegmentJob{
					TotalSegments:  tt.totalSegments,
					CopiedSegments: tt.copiedSegments,
				},
			}
			result := sm.calculateProgress(job)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSnapshotManager_CalculateTimeCost(t *testing.T) {
	sm := &snapshotManager{}

	tests := []struct {
		name       string
		startTs    uint64
		completeTs uint64
		expected   uint64
	}{
		{"completed job", 1000000000, 2000000000, 1000}, // 1 second = 1000 ms
		{"not started", 0, 0, 0},
		{"in progress", 1000000000, 0, 0},
		{"5 seconds", 1000000000, 6000000000, 5000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &copySegmentJob{
				CopySegmentJob: &datapb.CopySegmentJob{
					StartTs:    tt.startTs,
					CompleteTs: tt.completeTs,
				},
			}
			result := sm.calculateTimeCost(job)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSnapshotManager_BuildRestoreInfo(t *testing.T) {
	sm := &snapshotManager{}

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:          1001,
			SnapshotName:   "snapshot1",
			CollectionId:   100,
			State:          datapb.CopySegmentJobState_CopySegmentJobCompleted,
			TotalSegments:  10,
			CopiedSegments: 10,
			StartTs:        1000000000,
			CompleteTs:     3000000000,
		},
	}

	result := sm.buildRestoreInfo(job)

	assert.Equal(t, int64(1001), result.GetJobId())
	assert.Equal(t, "snapshot1", result.GetSnapshotName())
	assert.Equal(t, int64(100), result.GetCollectionId())
	assert.Equal(t, datapb.RestoreSnapshotState_RestoreSnapshotCompleted, result.GetState())
	assert.Equal(t, int32(100), result.GetProgress())
	assert.Equal(t, uint64(2000), result.GetTimeCost()) // 2 seconds = 2000 ms
}

// --- Test BuildChannelMapping ---

func TestSnapshotManager_BuildChannelMapping_Success(t *testing.T) {
	ctx := context.Background()

	// Test pchannel-based mapping with VirtualChannelNames
	// Snapshot vchannels: dml_0_100v0, dml_1_100v1 (collectionID=100)
	// Target vchannels: dml_0_200v0, dml_1_200v1 (collectionID=200)
	// Mapping should be based on pchannel: dml_0 -> dml_0, dml_1 -> dml_1
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			VirtualChannelNames: []string{"dml_0_100v0", "dml_1_100v1"},
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, ChannelName: "dml_0_100v0"},
			{SegmentId: 2, ChannelName: "dml_1_100v1"},
		},
	}

	// Mock getChannelsByCollectionID - target collection has same pchannels
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return []RWChannel{
			&channelMeta{Name: "dml_0_200v0"},
			&channelMeta{Name: "dml_1_200v1"},
		}, nil
	}

	sm := &snapshotManager{
		getChannelsByCollectionID: getChannelsFunc,
	}

	// Execute
	mapping, err := sm.buildChannelMapping(ctx, snapshotData, 200)

	// Verify
	assert.NoError(t, err)
	assert.Len(t, mapping, 2)
	// Mapping by pchannel: dml_0_100v0 -> dml_0_200v0, dml_1_100v1 -> dml_1_200v1
	assert.Equal(t, "dml_0_200v0", mapping["dml_0_100v0"])
	assert.Equal(t, "dml_1_200v1", mapping["dml_1_100v1"])
}

func TestSnapshotManager_BuildChannelMapping_EmptySegments(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		Segments: []*datapb.SegmentDescription{},
	}

	sm := &snapshotManager{}

	// Execute
	mapping, err := sm.buildChannelMapping(ctx, snapshotData, 100)

	// Verify
	assert.NoError(t, err)
	assert.Empty(t, mapping)
}

func TestSnapshotManager_BuildChannelMapping_CountMismatch(t *testing.T) {
	ctx := context.Background()

	// Snapshot has 2 vchannels but target only has 1
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			VirtualChannelNames: []string{"dml_0_100v0", "dml_1_100v1"},
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, ChannelName: "dml_0_100v0"},
			{SegmentId: 2, ChannelName: "dml_1_100v1"},
		},
	}

	// Mock getChannelsByCollectionID - returns different count
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return []RWChannel{
			&channelMeta{Name: "dml_0_200v0"},
		}, nil
	}

	sm := &snapshotManager{
		getChannelsByCollectionID: getChannelsFunc,
	}

	// Execute
	mapping, err := sm.buildChannelMapping(ctx, snapshotData, 200)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, mapping)
	assert.Contains(t, err.Error(), "channel count mismatch")
}

func TestSnapshotManager_BuildChannelMapping_GetChannelsError(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			VirtualChannelNames: []string{"dml_0_100v0"},
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, ChannelName: "dml_0_100v0"},
		},
	}

	expectedErr := errors.New("get channels error")
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return nil, expectedErr
	}

	sm := &snapshotManager{
		getChannelsByCollectionID: getChannelsFunc,
	}

	// Execute
	mapping, err := sm.buildChannelMapping(ctx, snapshotData, 200)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, mapping)
	assert.Equal(t, expectedErr, err)
}

// --- Test RestoreSnapshot ---
// TODO: Add tests for new DataCoord-driven RestoreSnapshot flow
// The new flow requires mocking: CreateCollection, CreatePartition, ShowCollections, etc.

// --- Test NewSnapshotManager ---

func TestNewSnapshotManager(t *testing.T) {
	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)
	mockBroker := broker.NewMockBroker(t)
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return nil, nil
	}

	sm := NewSnapshotManager(
		nil,
		nil,
		nil,
		mockAllocator,
		mockHandler,
		mockBroker,
		getChannelsFunc,
	)

	assert.NotNil(t, sm)
}

// --- Test ReadSnapshotData ---

func TestSnapshotManager_ReadSnapshotData_Success(t *testing.T) {
	ctx := context.Background()

	expectedData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1001,
			Name: "test_snapshot",
		},
	}

	// Mock snapshotMeta.ReadSnapshotData
	mockRead := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(
		sm *snapshotMeta,
		ctx context.Context,
		snapshotName string,
		includeSegments bool,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", snapshotName)
		assert.True(t, includeSegments)
		return expectedData, nil
	}).Build()
	defer mockRead.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	result, err := sm.ReadSnapshotData(ctx, "test_snapshot")

	assert.NoError(t, err)
	assert.Equal(t, expectedData, result)
}

func TestSnapshotManager_ReadSnapshotData_NotFound(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("snapshot not found")

	// Mock snapshotMeta.ReadSnapshotData to return error
	mockRead := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(
		sm *snapshotMeta,
		ctx context.Context,
		snapshotName string,
		includeSegments bool,
	) (*SnapshotData, error) {
		return nil, expectedErr
	}).Build()
	defer mockRead.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	result, err := sm.ReadSnapshotData(ctx, "nonexistent")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedErr, err)
}

// --- Test RestoreData ---

func TestSnapshotManager_RestoreData_Success(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1001,
			Name: "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{"_default": 1},
		},
		Segments: []*datapb.SegmentDescription{},
	}

	// Mock ReadSnapshotData to return snapshot data
	mockReadSnapshotData := mockey.Mock((*snapshotManager).ReadSnapshotData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		return snapshotData, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	// Mock copySegmentMeta.GetJob to return nil (job doesn't exist)
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(
		cm *copySegmentMeta,
		ctx context.Context,
		jobID int64,
	) CopySegmentJob {
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	// Mock buildPartitionMapping
	mockBuildPartition := mockey.Mock((*snapshotManager).buildPartitionMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return map[int64]int64{1: 10}, nil
	}).Build()
	defer mockBuildPartition.UnPatch()

	// Mock buildChannelMapping
	mockBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		targetCollectionID int64,
	) (map[string]string, error) {
		return map[string]string{"ch1": "ch2"}, nil
	}).Build()
	defer mockBuildChannel.UnPatch()

	// Mock createRestoreJob
	mockCreateJob := mockey.Mock((*snapshotManager).createRestoreJob).To(func(
		sm *snapshotManager,
		ctx context.Context,
		collectionID int64,
		channelMapping map[string]string,
		partitionMapping map[int64]int64,
		snapshotData *SnapshotData,
		jobID int64,
	) error {
		assert.Equal(t, int64(200), collectionID)
		assert.Equal(t, int64(12345), jobID)
		return nil
	}).Build()
	defer mockCreateJob.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, snapshotData.SnapshotInfo.GetName(), 200, 12345)

	assert.NoError(t, err)
	assert.Equal(t, int64(12345), jobID)
}

func TestSnapshotManager_RestoreData_Idempotent(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1001,
			Name: "test_snapshot",
		},
	}

	// Mock copySegmentMeta.GetJob to return existing job (idempotency case)
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(
		cm *copySegmentMeta,
		ctx context.Context,
		jobID int64,
	) CopySegmentJob {
		// Return a non-nil job to indicate it already exists
		return &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId: jobID,
			},
		}
	}).Build()
	defer mockGetJob.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	// Should return immediately without creating a new job
	jobID, err := sm.RestoreData(ctx, snapshotData.SnapshotInfo.GetName(), 200, 12345)

	assert.NoError(t, err)
	assert.Equal(t, int64(12345), jobID)
}

func TestSnapshotManager_RestoreData_PartitionMappingError(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1001,
			Name: "test_snapshot",
		},
	}

	expectedErr := errors.New("partition mapping error")

	// Mock ReadSnapshotData to return snapshot data
	mockReadSnapshotData := mockey.Mock((*snapshotManager).ReadSnapshotData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		return snapshotData, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	// Mock copySegmentMeta.GetJob to return nil
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(
		cm *copySegmentMeta,
		ctx context.Context,
		jobID int64,
	) CopySegmentJob {
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	// Mock buildPartitionMapping to return error
	mockBuildPartition := mockey.Mock((*snapshotManager).buildPartitionMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return nil, expectedErr
	}).Build()
	defer mockBuildPartition.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, snapshotData.SnapshotInfo.GetName(), 200, 12345)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "partition mapping failed")
}

func TestSnapshotManager_RestoreData_ChannelMappingError(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1001,
			Name: "test_snapshot",
		},
	}

	expectedErr := errors.New("channel mapping error")

	// Mock ReadSnapshotData to return snapshot data
	mockReadSnapshotData := mockey.Mock((*snapshotManager).ReadSnapshotData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		return snapshotData, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	// Mock copySegmentMeta.GetJob to return nil
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(
		cm *copySegmentMeta,
		ctx context.Context,
		jobID int64,
	) CopySegmentJob {
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	// Mock buildPartitionMapping
	mockBuildPartition := mockey.Mock((*snapshotManager).buildPartitionMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return map[int64]int64{1: 10}, nil
	}).Build()
	defer mockBuildPartition.UnPatch()

	// Mock buildChannelMapping to return error
	mockBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		targetCollectionID int64,
	) (map[string]string, error) {
		return nil, expectedErr
	}).Build()
	defer mockBuildChannel.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, snapshotData.SnapshotInfo.GetName(), 200, 12345)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "channel mapping failed")
}

func TestSnapshotManager_RestoreData_CreateJobError(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:   1001,
			Name: "test_snapshot",
		},
	}

	expectedErr := errors.New("create job error")

	// Mock ReadSnapshotData to return snapshot data
	mockReadSnapshotData := mockey.Mock((*snapshotManager).ReadSnapshotData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		return snapshotData, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	// Mock copySegmentMeta.GetJob to return nil
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(
		cm *copySegmentMeta,
		ctx context.Context,
		jobID int64,
	) CopySegmentJob {
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	// Mock buildPartitionMapping
	mockBuildPartition := mockey.Mock((*snapshotManager).buildPartitionMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return map[int64]int64{1: 10}, nil
	}).Build()
	defer mockBuildPartition.UnPatch()

	// Mock buildChannelMapping
	mockBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *SnapshotData,
		targetCollectionID int64,
	) (map[string]string, error) {
		return map[string]string{"ch1": "ch2"}, nil
	}).Build()
	defer mockBuildChannel.UnPatch()

	// Mock createRestoreJob to return error
	mockCreateJob := mockey.Mock((*snapshotManager).createRestoreJob).To(func(
		sm *snapshotManager,
		ctx context.Context,
		collectionID int64,
		channelMapping map[string]string,
		partitionMapping map[int64]int64,
		snapshotData *SnapshotData,
		jobID int64,
	) error {
		return expectedErr
	}).Build()
	defer mockCreateJob.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, snapshotData.SnapshotInfo.GetName(), 200, 12345)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "restore job creation failed")
}

func TestSnapshotManager_RestoreData_ReadSnapshotDataError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("snapshot read error")

	// Mock copySegmentMeta.GetJob to return nil
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).To(func(
		cm *copySegmentMeta,
		ctx context.Context,
		jobID int64,
	) CopySegmentJob {
		return nil
	}).Build()
	defer mockGetJob.UnPatch()

	// Mock ReadSnapshotData to return error
	mockReadSnapshotData := mockey.Mock((*snapshotManager).ReadSnapshotData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		return nil, expectedErr
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, "test_snapshot", 200, 12345)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "failed to read snapshot data")
}

// --- Test buildPartitionMapping ---

func TestSnapshotManager_BuildPartitionMapping_Success(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{
				"_default": 1,
				"part1":    2,
				"part2":    3,
			},
		},
	}

	// Mock broker.ShowPartitions
	mockShowPartitions := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "ShowPartitions")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		collectionID int64,
	) (*milvuspb.ShowPartitionsResponse, error) {
		return &milvuspb.ShowPartitionsResponse{
			PartitionNames: []string{"_default", "part1", "part2"},
			PartitionIDs:   []int64{10, 20, 30},
		}, nil
	}).Build()
	defer mockShowPartitions.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	result, err := sm.buildPartitionMapping(ctx, snapshotData, 200)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(10), result[1]) // _default: 1 -> 10
	assert.Equal(t, int64(20), result[2]) // part1: 2 -> 20
	assert.Equal(t, int64(30), result[3]) // part2: 3 -> 30
}

func TestSnapshotManager_BuildPartitionMapping_ShowPartitionsError(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{"_default": 1},
		},
	}

	expectedErr := errors.New("show partitions error")

	// Mock broker.ShowPartitions to return error
	mockShowPartitions := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "ShowPartitions")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		collectionID int64,
	) (*milvuspb.ShowPartitionsResponse, error) {
		return nil, expectedErr
	}).Build()
	defer mockShowPartitions.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	result, err := sm.buildPartitionMapping(ctx, snapshotData, 200)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedErr, err)
}

// --- Test validateCMEKCompatibility ---

func TestSnapshotManager_ValidateCMEKCompatibility_NonEncryptedSnapshot(t *testing.T) {
	ctx := context.Background()

	// Non-encrypted snapshot (no cipher.ezID in properties)
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Properties: []*commonpb.KeyValuePair{
					{Key: "other_key", Value: "other_value"},
				},
			},
		},
	}

	// Mock DescribeDatabase to return non-encrypted database
	mockDescribeDB := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeDatabase")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		dbName string,
	) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return &rootcoordpb.DescribeDatabaseResponse{
			DbName:     dbName,
			Properties: []*commonpb.KeyValuePair{},
		}, nil
	}).Build()
	defer mockDescribeDB.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	// Should pass - non-encrypted snapshot to non-encrypted database
	err := sm.validateCMEKCompatibility(ctx, snapshotData, "target_db")

	assert.NoError(t, err)
}

func TestSnapshotManager_ValidateCMEKCompatibility_SameEZDatabase(t *testing.T) {
	ctx := context.Background()

	// Encrypted snapshot with ezID = 12345
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Properties: []*commonpb.KeyValuePair{
					{Key: "cipher.ezID", Value: "12345"},
				},
			},
		},
	}

	// Mock DescribeDatabase to return same ezID
	mockDescribeDB := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeDatabase")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		dbName string,
	) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return &rootcoordpb.DescribeDatabaseResponse{
			DbName: dbName,
			Properties: []*commonpb.KeyValuePair{
				{Key: "cipher.ezID", Value: "12345"},
				{Key: "cipher.key", Value: "encrypted_root_key"},
			},
		}, nil
	}).Build()
	defer mockDescribeDB.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	// Should pass - same encryption zone
	err := sm.validateCMEKCompatibility(ctx, snapshotData, "target_db")

	assert.NoError(t, err)
}

func TestSnapshotManager_ValidateCMEKCompatibility_NonEncryptedDatabase(t *testing.T) {
	ctx := context.Background()

	// Encrypted snapshot with ezID = 12345
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Properties: []*commonpb.KeyValuePair{
					{Key: "cipher.ezID", Value: "12345"},
				},
			},
		},
	}

	// Mock DescribeDatabase to return non-encrypted database
	mockDescribeDB := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeDatabase")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		dbName string,
	) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return &rootcoordpb.DescribeDatabaseResponse{
			DbName:     dbName,
			Properties: []*commonpb.KeyValuePair{
				// No cipher.enabled property or set to false
			},
		}, nil
	}).Build()
	defer mockDescribeDB.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	// Should fail - cannot restore encrypted snapshot to non-encrypted database
	err := sm.validateCMEKCompatibility(ctx, snapshotData, "target_db")

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
	assert.Contains(t, err.Error(), "non-encrypted database")
}

func TestSnapshotManager_ValidateCMEKCompatibility_DifferentEZDatabase(t *testing.T) {
	ctx := context.Background()

	// Encrypted snapshot with ezID = 12345
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Properties: []*commonpb.KeyValuePair{
					{Key: "cipher.ezID", Value: "12345"},
				},
			},
		},
	}

	// Mock DescribeDatabase to return different ezID (67890)
	mockDescribeDB := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeDatabase")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		dbName string,
	) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return &rootcoordpb.DescribeDatabaseResponse{
			DbName: dbName,
			Properties: []*commonpb.KeyValuePair{
				{Key: "cipher.enabled", Value: "true"},
				{Key: "cipher.ezID", Value: "67890"},
				{Key: "cipher.key", Value: "test-root-key"},
			},
		}, nil
	}).Build()
	defer mockDescribeDB.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	// Should fail - different encryption zone
	err := sm.validateCMEKCompatibility(ctx, snapshotData, "target_db")

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
	assert.Contains(t, err.Error(), "different encryption zone")
	assert.Contains(t, err.Error(), "12345")
	assert.Contains(t, err.Error(), "67890")
}

func TestSnapshotManager_ValidateCMEKCompatibility_DescribeDatabaseError(t *testing.T) {
	ctx := context.Background()

	// Encrypted snapshot with ezID = 12345
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Properties: []*commonpb.KeyValuePair{
					{Key: "cipher.ezID", Value: "12345"},
				},
			},
		},
	}

	expectedErr := errors.New("describe database error")

	// Mock DescribeDatabase to return error
	mockDescribeDB := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeDatabase")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		dbName string,
	) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return nil, expectedErr
	}).Build()
	defer mockDescribeDB.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	// Should return error from DescribeDatabase
	err := sm.validateCMEKCompatibility(ctx, snapshotData, "target_db")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to describe target database")
}

func TestSnapshotManager_ValidateCMEKCompatibility_NonEncryptedToEncrypted(t *testing.T) {
	ctx := context.Background()

	// Non-encrypted snapshot (no cipher.ezID in properties)
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Properties: []*commonpb.KeyValuePair{
					{Key: "other_key", Value: "other_value"},
				},
			},
		},
	}

	// Mock DescribeDatabase to return encrypted database
	mockDescribeDB := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeDatabase")).To(func(
		b *broker.MockBroker,
		ctx context.Context,
		dbName string,
	) (*rootcoordpb.DescribeDatabaseResponse, error) {
		return &rootcoordpb.DescribeDatabaseResponse{
			DbName: dbName,
			Properties: []*commonpb.KeyValuePair{
				{Key: "cipher.enabled", Value: "true"},
				{Key: "cipher.ezID", Value: "12345"},
				{Key: "cipher.key", Value: "test-root-key"},
			},
		}, nil
	}).Build()
	defer mockDescribeDB.UnPatch()

	sm := &snapshotManager{
		broker: broker.NewMockBroker(t),
	}

	// Should fail - cannot restore non-encrypted collection to CMEK-encrypted database
	err := sm.validateCMEKCompatibility(ctx, snapshotData, "target_db")

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
	assert.Contains(t, err.Error(), "cannot restore non-encrypted collection to CMEK-encrypted database")
}

// --- Test RestoreCollection ---

func TestSnapshotManager_RestoreCollection_SchemaNameAndDbName(t *testing.T) {
	ctx := context.Background()

	// Snapshot data with original collection name and db name
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name:   "original_collection",
				DbName: "original_db",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				},
			},
			NumShards:        2,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			Partitions:       map[string]int64{"_default": 1},
		},
	}

	targetCollectionName := "target_collection"
	targetDbName := "target_db"

	// Capture the CreateCollectionRequest to verify schema modifications
	var capturedReq *milvuspb.CreateCollectionRequest

	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().CreateCollection(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *milvuspb.CreateCollectionRequest) {
		capturedReq = req
	}).Return(nil)

	mockBroker.EXPECT().DescribeCollectionByName(mock.Anything, targetDbName, targetCollectionName).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 12345,
	}, nil)

	sm := &snapshotManager{
		broker: mockBroker,
	}

	collectionID, err := sm.RestoreCollection(ctx, snapshotData, targetCollectionName, targetDbName)

	assert.NoError(t, err)
	assert.Equal(t, int64(12345), collectionID)

	// Verify the schema in the request has updated Name and DbName
	assert.NotNil(t, capturedReq)
	assert.Equal(t, targetDbName, capturedReq.DbName)
	assert.Equal(t, targetCollectionName, capturedReq.CollectionName)

	// Unmarshal and verify the schema bytes
	var schema schemapb.CollectionSchema
	err = proto.Unmarshal(capturedReq.Schema, &schema)
	assert.NoError(t, err)
	assert.Equal(t, targetCollectionName, schema.Name, "schema.Name should be updated to target collection name")
	assert.Equal(t, targetDbName, schema.DbName, "schema.DbName should be updated to target database name")
}

func TestSnapshotManager_GetSnapshotRestoreRefCount(t *testing.T) {
	mockGetRef := mockey.Mock((*copySegmentMeta).GetRestoreRefCount).Return(int32(5)).Build()
	defer mockGetRef.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	count := sm.GetSnapshotRestoreRefCount("test_snapshot")
	assert.Equal(t, int32(5), count)
}
