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

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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
		{"zero total", 0, 0, 0},
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

func TestSnapshotManager_BuildChannelMapping_PChannelNotFoundInTarget(t *testing.T) {
	ctx := context.Background()

	// Snapshot has vchannels on pchannel dml_0 and dml_1
	// But target only has pchannel dml_0 (dml_1 is missing)
	snapshotData := &SnapshotData{
		Collection: &datapb.CollectionDescription{
			VirtualChannelNames: []string{"dml_0_100v0", "dml_1_100v1"},
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, ChannelName: "dml_0_100v0"},
			{SegmentId: 2, ChannelName: "dml_1_100v1"},
		},
	}

	// Target has 2 vchannels but on different pchannels (dml_0 and dml_2)
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return []RWChannel{
			&channelMeta{Name: "dml_0_200v0"},
			&channelMeta{Name: "dml_2_200v1"}, // Different pchannel - dml_2 instead of dml_1
		}, nil
	}

	sm := &snapshotManager{
		getChannelsByCollectionID: getChannelsFunc,
	}

	// Execute
	mapping, err := sm.buildChannelMapping(ctx, snapshotData, 200)

	// Verify - should fail because dml_1 pchannel not found in target
	assert.Error(t, err)
	assert.Nil(t, mapping)
	assert.Contains(t, err.Error(), "pchannel not found in target collection")
}

// --- Test BuildPartitionMapping ---

func TestSnapshotManager_BuildPartitionMapping_Success(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			PartitionIds: []int64{1, 2, 3},
		},
	}

	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, int64(100)).Return([]int64{101, 102, 103}, nil).Once()

	sm := &snapshotManager{
		broker: mockBroker,
	}

	// Execute
	mapping, err := sm.buildPartitionMapping(ctx, snapshotData, 100)

	// Verify
	assert.NoError(t, err)
	assert.Len(t, mapping, 3)
	// Sorted mapping: 1->101, 2->102, 3->103
	assert.Equal(t, int64(101), mapping[1])
	assert.Equal(t, int64(102), mapping[2])
	assert.Equal(t, int64(103), mapping[3])
}

func TestSnapshotManager_BuildPartitionMapping_CountMismatch(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			PartitionIds: []int64{1, 2, 3},
		},
	}

	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, int64(100)).Return([]int64{101, 102}, nil).Once()

	sm := &snapshotManager{
		broker: mockBroker,
	}

	// Execute
	mapping, err := sm.buildPartitionMapping(ctx, snapshotData, 100)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, mapping)
	assert.Contains(t, err.Error(), "partition count mismatch")
}

func TestSnapshotManager_BuildPartitionMapping_BrokerError(t *testing.T) {
	ctx := context.Background()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			PartitionIds: []int64{1, 2, 3},
		},
	}

	expectedErr := errors.New("broker error")
	mockBroker := broker.NewMockBroker(t)
	mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, int64(100)).Return(nil, expectedErr).Once()

	sm := &snapshotManager{
		broker: mockBroker,
	}

	// Execute
	mapping, err := sm.buildPartitionMapping(ctx, snapshotData, 100)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, mapping)
	assert.Equal(t, expectedErr, err)
}

// --- Test RestoreSnapshot ---

func TestSnapshotManager_RestoreSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	mockBroker := broker.NewMockBroker(t)

	jobID := int64(5001)

	// Mock ReadSnapshotData for restore
	// Use vchannel names that map to pchannels: ch1, ch2
	// Format: {pchannel}_{collectionID}v{index}
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1001,
			Name:         "test_snapshot",
			CollectionId: 100,
			PartitionIds: []int64{1, 2},
		},
		Collection: &datapb.CollectionDescription{
			VirtualChannelNames: []string{"ch1_100v0", "ch2_100v1"},
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, ChannelName: "ch1_100v0", PartitionId: 1},
			{SegmentId: 2, ChannelName: "ch2_100v1", PartitionId: 2},
		},
	}

	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, name string, includeSegments bool) (*SnapshotData, error) {
		assert.True(t, includeSegments)
		return snapshotData, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	// Mock getChannelsByCollectionID
	// Target vchannels use same pchannels (ch1, ch2) but different collectionID (200)
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return []RWChannel{
			&channelMeta{Name: "ch1_200v0"},
			&channelMeta{Name: "ch2_200v1"},
		}, nil
	}

	// Mock broker for partition mapping
	mockBroker.EXPECT().ShowPartitionsInternal(mock.Anything, int64(200)).Return([]int64{101, 102}, nil).Once()

	// Mock copySegmentMeta.AddJob - should use provided jobID
	mockAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(func(csm *copySegmentMeta, ctx context.Context, job CopySegmentJob) error {
		assert.Equal(t, jobID, job.GetJobId())
		return nil
	}).Build()
	defer mockAddJob.UnPatch()

	// Mock meta.GetSegment - return valid segment info for source segments
	mockGetSegment := mockey.Mock((*meta).GetSegment).To(func(m *meta, ctx context.Context, segmentID int64) *SegmentInfo {
		return &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID: segmentID,
			},
		}
	}).Build()
	defer mockGetSegment.UnPatch()

	// Mock meta.AddSegment - for pre-registering target segments
	mockAddSegment := mockey.Mock((*meta).AddSegment).To(func(m *meta, ctx context.Context, segment *SegmentInfo) error {
		return nil
	}).Build()
	defer mockAddSegment.UnPatch()

	// Mock allocator for segment IDs
	mockAllocator.EXPECT().AllocN(mock.Anything).Return(int64(10000), int64(10002), nil).Maybe()

	sm := &snapshotManager{
		meta:                      &meta{},
		snapshotMeta:              &snapshotMeta{},
		copySegmentMeta:           &copySegmentMeta{},
		allocator:                 mockAllocator,
		broker:                    mockBroker,
		getChannelsByCollectionID: getChannelsFunc,
	}

	// Execute - provide a pre-allocated jobID
	err := sm.RestoreSnapshot(ctx, "test_snapshot", 200, jobID)

	// Verify
	assert.NoError(t, err)
}

func TestSnapshotManager_RestoreSnapshot_ReadSnapshotDataError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("read snapshot data error")
	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, name string, includeSegments bool) (*SnapshotData, error) {
		return nil, expectedErr
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	// Execute
	err := sm.RestoreSnapshot(ctx, "test_snapshot", 200, 5001)

	// Verify
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotManager_RestoreSnapshot_InvalidJobID_Zero(t *testing.T) {
	ctx := context.Background()

	sm := &snapshotManager{}

	// Execute with jobID = 0
	err := sm.RestoreSnapshot(ctx, "test_snapshot", 200, 0)

	// Verify
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

func TestSnapshotManager_RestoreSnapshot_InvalidJobID_Negative(t *testing.T) {
	ctx := context.Background()

	sm := &snapshotManager{}

	// Execute with jobID = -1
	err := sm.RestoreSnapshot(ctx, "test_snapshot", 200, -1)

	// Verify
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

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
