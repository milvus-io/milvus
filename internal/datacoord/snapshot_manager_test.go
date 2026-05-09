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
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
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

	// Create snapshot manager. We need a properly-initialized snapshotMeta so that
	// the unconditional SetSnapshotPending / ClearSnapshotPending calls (required for
	// GenSnapshot → SaveSnapshot atomicity) don't panic on uninitialized maps.
	sm := NewSnapshotManager(
		nil,                             // meta
		createTestSnapshotMetaLoaded(t), // snapshotMeta
		nil,                             // copySegmentMeta
		mockAllocator,
		mockHandler,
		nil, // broker
		nil, // getChannelsFunc
		nil, // indexEngineVersionManager
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "test description", 0)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, int64(1001), snapshotID)
}

func TestSnapshotManager_CreateSnapshot_WithCompactionProtection(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(2001), nil).Once()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			CollectionId: 100,
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, NumOfRows: 100},
		},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *SnapshotData) error {
		// Verify compaction expire time is set
		assert.True(t, data.SnapshotInfo.CompactionExpireTime > 0)
		return nil
	}).Build()
	defer mockSaveSnapshot.UnPatch()

	snapshotMetaInstance := createTestSnapshotMetaLoaded(t)

	sm := NewSnapshotManager(
		nil,
		snapshotMetaInstance,
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	snapshotID, err := sm.CreateSnapshot(ctx, 100, "protected_snap", "with protection", 3600)

	// Verify snapshot pending intent is cleared after CreateSnapshot completes
	assert.False(t, snapshotMetaInstance.IsCollectionCompactionBlocked(100))
	assert.NoError(t, err)
	assert.Equal(t, int64(2001), snapshotID)
}

func TestSnapshotManager_CreateSnapshot_DuplicateName(t *testing.T) {
	ctx := context.Background()

	// Mock snapshotMeta.GetSnapshot to return existing snapshot
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "existing_snapshot", "description", 0)

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
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		createTestSnapshotMetaLoaded(t),
		nil,
		mockAllocator,
		nil,
		nil,
		nil,
		nil, /* indexEngineVersionManager */
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "description", 0)

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
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		createTestSnapshotMetaLoaded(t),
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "description", 0)

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
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
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
		createTestSnapshotMetaLoaded(t),
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	// Execute
	snapshotID, err := sm.CreateSnapshot(ctx, 100, "test_snapshot", "description", 0)

	// Verify
	assert.Error(t, err)
	assert.Equal(t, int64(0), snapshotID)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotManager_CreateSnapshot_ClearsSnapshotPendingOnGenSnapshotError(t *testing.T) {
	ctx := context.Background()

	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	expectedErr := errors.New("gen snapshot error")
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(nil, expectedErr).Once()

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	snapshotMetaInstance := createTestSnapshotMetaLoaded(t)

	sm := NewSnapshotManager(
		nil,
		snapshotMetaInstance,
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	_, err := sm.CreateSnapshot(ctx, 100, "test_snap", "desc", 3600)
	assert.Error(t, err)

	// Verify snapshot pending intent is cleared even on error
	assert.False(t, snapshotMetaInstance.IsCollectionCompactionBlocked(100))
}

func TestSnapshotManager_CreateSnapshot_ClearsSnapshotPendingOnSaveError(t *testing.T) {
	ctx := context.Background()

	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	expectedErr := errors.New("save error")
	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *SnapshotData) error {
		// While inside SaveSnapshot, snapshot pending should be active
		assert.True(t, sm.IsCollectionCompactionBlocked(data.SnapshotInfo.GetCollectionId()))
		return expectedErr
	}).Build()
	defer mockSaveSnapshot.UnPatch()

	snapshotMetaInstance := createTestSnapshotMetaLoaded(t)

	sm := NewSnapshotManager(
		nil,
		snapshotMetaInstance,
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	_, err := sm.CreateSnapshot(ctx, 100, "test_snap", "desc", 3600)
	assert.Error(t, err)

	// Verify snapshot pending intent is cleared after save failure
	assert.False(t, snapshotMetaInstance.IsCollectionCompactionBlocked(100))
}

// Regression for PR #48227 review comment #4: even when the user requests zero
// long-term compaction protection, CreateSnapshot must hold SetSnapshotPending
// across the GenSnapshot → SaveSnapshot window. Otherwise concurrent compaction
// could drop segments that the in-flight snapshot is about to reference, leaving
// the snapshot immediately broken. Before the fix the SetSnapshotPending call was
// gated on compactionProtectionSeconds > 0.
func TestSnapshotManager_CreateSnapshot_PendingHeldEvenWithoutLongTermProtection(t *testing.T) {
	ctx := context.Background()

	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100},
		Segments:     []*datapb.SegmentDescription{{SegmentId: 1}},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	snapshotMetaInstance := createTestSnapshotMetaLoaded(t)

	// While SaveSnapshot is in flight, the collection MUST be marked as blocked so
	// concurrent compaction commits see the TOCTOU guard and back off. We observe
	// this by intercepting SaveSnapshot and asserting the block is visible at that point.
	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *SnapshotData) error {
		assert.True(t, sm.IsCollectionCompactionBlocked(data.SnapshotInfo.GetCollectionId()),
			"collection must be blocked during SaveSnapshot even with protection=0")
		return nil
	}).Build()
	defer mockSaveSnapshot.UnPatch()

	sm := NewSnapshotManager(
		nil,
		snapshotMetaInstance,
		nil,
		mockAllocator,
		mockHandler,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	_, err := sm.CreateSnapshot(ctx, 100, "test_snap", "desc", 0) // compactionProtectionSeconds = 0
	assert.NoError(t, err)

	// After CreateSnapshot returns, the deferred ClearSnapshotPending must have run.
	assert.False(t, snapshotMetaInstance.IsCollectionCompactionBlocked(100),
		"block must be released once CreateSnapshot finishes")
}

func TestSnapshotManager_CreateSnapshot_ClearsSnapshotPendingOnAllocError(t *testing.T) {
	ctx := context.Background()

	mockAllocator := allocator.NewMockAllocator(t)
	expectedErr := errors.New("alloc error")
	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(0), expectedErr).Once()

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	snapshotMetaInstance := createTestSnapshotMetaLoaded(t)

	sm := NewSnapshotManager(
		nil,
		snapshotMetaInstance,
		nil,
		mockAllocator,
		nil,
		nil,
		nil,
		nil, // indexEngineVersionManager
	)

	_, err := sm.CreateSnapshot(ctx, 100, "test_snap", "desc", 3600)
	assert.Error(t, err)

	// Verify snapshot pending intent is cleared after alloc failure
	assert.False(t, snapshotMetaInstance.IsCollectionCompactionBlocked(100))
}

// --- Test DropSnapshot ---

func TestSnapshotManager_DropSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	// Mock GetSnapshot to return existing snapshot
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return &datapb.SnapshotInfo{Id: 1, Name: name}, nil
	}).Build()
	defer mockGetSnapshot.UnPatch()

	mockDropSnapshot := mockey.Mock((*snapshotMeta).DropSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) error {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	err := sm.DropSnapshot(ctx, int64(100), "test_snapshot")

	// Verify
	assert.NoError(t, err)
}

func TestSnapshotManager_DropSnapshot_NotFound_Idempotent(t *testing.T) {
	ctx := context.Background()

	// Mock GetSnapshot to return not found (snapshot doesn't exist)
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, merr.WrapErrSnapshotNotFound(name, fmt.Sprintf("collection %d", collectionID))
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
		nil, /* indexEngineVersionManager */
	)

	// Execute - should succeed even if snapshot doesn't exist (idempotent)
	err := sm.DropSnapshot(ctx, int64(100), "nonexistent_snapshot")

	// Verify
	assert.NoError(t, err)
}

func TestSnapshotManager_DropSnapshot_Error(t *testing.T) {
	ctx := context.Background()

	// Mock GetSnapshot to return existing snapshot
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return &datapb.SnapshotInfo{Id: 1, Name: name}, nil
	}).Build()
	defer mockGetSnapshot.UnPatch()

	expectedErr := errors.New("drop error")
	mockDropSnapshot := mockey.Mock((*snapshotMeta).DropSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) error {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	err := sm.DropSnapshot(ctx, int64(100), "test_snapshot")

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

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	info, err := sm.GetSnapshot(ctx, int64(100), "test_snapshot")

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedInfo, info)
}

func TestSnapshotManager_GetSnapshot_NotFound(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("snapshot not found")
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	info, err := sm.GetSnapshot(ctx, int64(100), "nonexistent")

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

	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string, includeSegments bool) (*SnapshotData, error) {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	data, err := sm.DescribeSnapshot(ctx, int64(100), "test_snapshot")

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, expectedData, data)
}

func TestSnapshotManager_DescribeSnapshot_NotFound(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("snapshot not found")
	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string, includeSegments bool) (*SnapshotData, error) {
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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	data, err := sm.DescribeSnapshot(ctx, int64(100), "nonexistent")

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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	snapshots, err := sm.ListSnapshots(ctx, 100, 0, 0)

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
		nil, /* indexEngineVersionManager */
	)

	// Execute
	snapshots, err := sm.ListSnapshots(ctx, 100, 0, 0)

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
		nil, // indexEngineVersionManager
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
		nil, // indexEngineVersionManager
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
		nil, // indexEngineVersionManager
	)

	// Execute - no filter
	jobs, err := sm.ListRestoreJobs(ctx, 0, 0)

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
		nil, // indexEngineVersionManager
	)

	// Execute - filter by collection ID 100
	jobs, err := sm.ListRestoreJobs(ctx, 100, 0)

	// Verify - should return 2 jobs for collection 100
	assert.NoError(t, err)
	assert.Len(t, jobs, 2)
	for _, job := range jobs {
		assert.Equal(t, int64(100), job.GetCollectionId())
	}

	// Execute - filter by collection ID 200
	jobs, err = sm.ListRestoreJobs(ctx, 200, 0)

	// Verify - should return 1 job for collection 200
	assert.NoError(t, err)
	assert.Len(t, jobs, 1)
	assert.Equal(t, int64(200), jobs[0].GetCollectionId())
	assert.Equal(t, int64(2), jobs[0].GetJobId())

	// Execute - filter by non-existent collection ID
	jobs, err = sm.ListRestoreJobs(ctx, 999, 0)

	// Verify - should return 0 jobs
	assert.NoError(t, err)
	assert.Len(t, jobs, 0)
}

// --- Test ListRestoreJobs with dbID filtering ---

func TestSnapshotManager_ListRestoreJobs_FilterByDbID(t *testing.T) {
	ctx := context.Background()

	testJobs := []CopySegmentJob{
		&copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 1, SnapshotName: "snap1", CollectionId: 100,
			State: datapb.CopySegmentJobState_CopySegmentJobCompleted, TotalSegments: 10, CopiedSegments: 10,
		}},
		&copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 2, SnapshotName: "snap2", CollectionId: 200,
			State: datapb.CopySegmentJobState_CopySegmentJobPending, TotalSegments: 5, CopiedSegments: 0,
		}},
		&copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{
			JobId: 3, SnapshotName: "snap3", CollectionId: 300,
			State: datapb.CopySegmentJobState_CopySegmentJobExecuting, TotalSegments: 8, CopiedSegments: 4,
		}},
	}

	mockGetJobBy := mockey.Mock((*copySegmentMeta).GetJobBy).To(func(csm *copySegmentMeta, ctx context.Context, filters ...CopySegmentJobFilter) []CopySegmentJob {
		return testJobs
	}).Build()
	defer mockGetJobBy.UnPatch()

	// Build meta with collections in different databases
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	m.collections.Insert(100, &collectionInfo{ID: 100, DatabaseID: 1})
	m.collections.Insert(200, &collectionInfo{ID: 200, DatabaseID: 1})
	m.collections.Insert(300, &collectionInfo{ID: 300, DatabaseID: 2})

	sm := NewSnapshotManager(m, nil, &copySegmentMeta{}, nil, nil, nil, nil, nil)

	t.Run("dbID_filter", func(t *testing.T) {
		// dbID=1 should return jobs for collections 100 and 200
		jobs, err := sm.ListRestoreJobs(ctx, 0, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 2)
		assert.Equal(t, int64(1), jobs[0].GetJobId())
		assert.Equal(t, int64(2), jobs[1].GetJobId())
	})

	t.Run("dbID_filter_different_db", func(t *testing.T) {
		// dbID=2 should return job for collection 300
		jobs, err := sm.ListRestoreJobs(ctx, 0, 2)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, int64(3), jobs[0].GetJobId())
	})

	t.Run("dbID_filter_no_match", func(t *testing.T) {
		// dbID=999 should return empty
		jobs, err := sm.ListRestoreJobs(ctx, 0, 999)
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)
	})

	t.Run("collectionID_takes_priority", func(t *testing.T) {
		// When collectionID is set, dbID is ignored
		jobs, err := sm.ListRestoreJobs(ctx, 100, 1)
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, int64(1), jobs[0].GetJobId())
	})
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

func TestRestoreSnapshot_ValidationFailsCloseBroadcasterBeforeRollback(t *testing.T) {
	ctx := context.Background()

	// Track call order
	var callOrder []string

	// Mock snapshotMeta.GetSnapshot (Phase 0 TOCTOU re-check) to succeed.
	mGet := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(42), 1, nil).Build()
	defer mGet.UnPatch()
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
	defer mUnpin.UnPatch()

	// Mock ReadSnapshotData
	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1"},
		Segments:     []*datapb.SegmentDescription{},
		Indexes:      nil,
	}, nil).Build()
	defer m1.UnPatch()

	// Mock validateCMEKCompatibility
	m2 := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer m2.UnPatch()

	// Mock RestoreCollection
	m3 := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer m3.UnPatch()

	// Mock RestoreIndexes
	m4 := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer m4.UnPatch()

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(999), nil)

	sm := &snapshotManager{
		allocator:       mockAlloc,
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}

	// Phase 0 lock holder (no-op; only tracks acquire/release).
	phase0Lock := &mockBroadcastAPI{closeFn: func() { callOrder = append(callOrder, "phase0_close") }}
	startRestoreLock := func(ctx context.Context, sourceCollectionID int64, snapshotName, targetDbName, targetCollectionName string) (broadcaster.BroadcastAPI, error) {
		callOrder = append(callOrder, "phase0_lock")
		return phase0Lock, nil
	}

	// Mock broadcaster that tracks Close calls
	closeCalled := 0
	mockBroadcaster := &mockBroadcastAPI{
		closeFn: func() {
			closeCalled++
			callOrder = append(callOrder, "close")
		},
	}

	startBroadcaster := func(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error) {
		callOrder = append(callOrder, "start_broadcaster")
		return mockBroadcaster, nil
	}

	rollbackCalled := false
	rollback := func(ctx context.Context, dbName, collName string) error {
		rollbackCalled = true
		callOrder = append(callOrder, "rollback")
		return nil
	}

	validateResources := func(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error {
		callOrder = append(callOrder, "validate")
		return errors.New("partition missing")
	}

	// Execute
	jobID, err := sm.RestoreSnapshot(ctx, int64(100), "snap1", "target_coll", "default",
		startRestoreLock, startBroadcaster, rollback, validateResources)

	// Verify
	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "resource validation failed")
	assert.True(t, rollbackCalled)

	// Key assertion: Close must happen BEFORE rollback (Phase 4 broadcaster).
	// Phase 0 lock is acquired first and released before Phase 1.
	assert.Equal(t, []string{"phase0_lock", "phase0_close", "start_broadcaster", "validate", "close", "rollback"}, callOrder)

	// Phase 4 broadcaster closed exactly once (not double-closed by defer)
	assert.Equal(t, 1, closeCalled)

	// Ref count was claimed and released on the failure path.
}

func TestRestoreSnapshot_ValidationFailsRollbackAlsoFails(t *testing.T) {
	ctx := context.Background()

	mGet := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(42), 1, nil).Build()
	defer mGet.UnPatch()
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
	defer mUnpin.UnPatch()

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1"},
		Segments:     []*datapb.SegmentDescription{},
	}, nil).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer m2.UnPatch()

	m3 := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer m3.UnPatch()

	m4 := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer m4.UnPatch()

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(999), nil)

	sm := &snapshotManager{
		allocator:       mockAlloc,
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}

	phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
	startRestoreLock := func(ctx context.Context, sourceCollectionID int64, snapshotName, targetDbName, targetCollectionName string) (broadcaster.BroadcastAPI, error) {
		return phase0Lock, nil
	}

	closeCalled := 0
	mockBcast := &mockBroadcastAPI{closeFn: func() { closeCalled++ }}

	startBroadcaster := func(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error) {
		return mockBcast, nil
	}
	rollback := func(ctx context.Context, dbName, collName string) error {
		return errors.New("rollback failed too")
	}
	validateResources := func(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error {
		return errors.New("validation error")
	}

	jobID, err := sm.RestoreSnapshot(ctx, int64(100), "snap1", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validateResources)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	assert.Contains(t, err.Error(), "resource validation failed")
	// Phase 4 broadcaster closed once despite rollback also failing
	assert.Equal(t, 1, closeCalled)
	// Ref count released on failure
}

func TestRestoreSnapshot_ValidationPassesThenBroadcastSucceeds(t *testing.T) {
	ctx := context.Background()

	mGet := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(42), 1, nil).Build()
	defer mGet.UnPatch()
	unpinCalls := 0
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, _ int64) (int64, string, int, error) {
			unpinCalls++
			return 0, "", 0, nil
		}).Build()
	defer mUnpin.UnPatch()

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1"},
		Segments:     []*datapb.SegmentDescription{},
	}, nil).Build()
	defer m1.UnPatch()

	m2 := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer m2.UnPatch()

	m3 := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer m3.UnPatch()

	m4 := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer m4.UnPatch()

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(999), nil)

	sm := &snapshotManager{
		allocator:       mockAlloc,
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}

	phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
	startRestoreLock := func(ctx context.Context, sourceCollectionID int64, snapshotName, targetDbName, targetCollectionName string) (broadcaster.BroadcastAPI, error) {
		return phase0Lock, nil
	}

	closeCalled := 0
	broadcastCalled := false
	mockBcast := &mockBroadcastAPI{
		closeFn:     func() { closeCalled++ },
		broadcastFn: func() { broadcastCalled = true },
	}

	startBroadcaster := func(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error) {
		return mockBcast, nil
	}
	rollback := func(ctx context.Context, dbName, collName string) error {
		t.Fatal("rollback should not be called on success")
		return nil
	}
	validateResources := func(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error {
		return nil // validation passes
	}

	// Mock streaming.WAL().ControlChannel() since Broadcast builds a message using it
	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().ControlChannel().Return("control_channel")
	streaming.SetWALForTest(mockWAL)

	jobID, err := sm.RestoreSnapshot(ctx, int64(100), "snap1", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validateResources)

	assert.NoError(t, err)
	assert.Equal(t, int64(999), jobID)
	assert.True(t, broadcastCalled)
	// Close called once by defer (normal cleanup)
	assert.Equal(t, 1, closeCalled)
	// On success path, the pin ownership is transferred to the copy-segment
	// job; the defer must NOT unpin. The job's terminal-transition hook will
	// release the pin via UpdateJobStateAndReleaseRef.
	assert.Equal(t, 0, unpinCalls, "success path must not unpin — ownership transferred to job")
}

// TestRestoreSnapshot_PinTTLReadFromParamtable verifies that the restore pin TTL is
// sourced from Params.DataCoordCfg.SnapshotRestorePinTTLSeconds, guarding against a
// future regression where the TTL is hardcoded to 0 (which would disable the orphan-pin
// safety net on crash-between-Pin-and-Broadcast).
func TestRestoreSnapshot_PinTTLReadFromParamtable(t *testing.T) {
	ctx := context.Background()

	var capturedTTL int64 = -1
	mPin := mockey.Mock((*snapshotMeta).PinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, _ int64, _ string, ttl int64) (int64, int, error) {
			capturedTTL = ttl
			return int64(42), 1, nil
		}).Build()
	defer mPin.UnPatch()
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
	defer mUnpin.UnPatch()

	// Fail early so we only exercise Phase 0.
	mRead := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(nil, errors.New("stop here")).Build()
	defer mRead.UnPatch()

	sm := &snapshotManager{
		allocator:       allocator.NewMockAllocator(t),
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}
	phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
	startRestoreLock := func(ctx context.Context, _ int64, _, _, _ string) (broadcaster.BroadcastAPI, error) {
		return phase0Lock, nil
	}
	startBroadcaster := func(ctx context.Context, _ int64, _ string) (broadcaster.BroadcastAPI, error) {
		t.Fatal("not reached")
		return nil, nil
	}
	rollback := func(ctx context.Context, _, _ string) error { return nil }
	validate := func(ctx context.Context, _ int64, _ *SnapshotData) error { return nil }

	_, err := sm.RestoreSnapshot(ctx, int64(100), "snap", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validate)
	assert.Error(t, err)

	expected := Params.DataCoordCfg.SnapshotRestorePinTTLSeconds.GetAsInt64()
	assert.Equal(t, expected, capturedTTL, "PinSnapshot must be invoked with TTL from paramtable")
	assert.Greater(t, capturedTTL, int64(0), "default TTL must be > 0 to enable orphan-pin self-heal")
}

// TestRestoreSnapshot_FailurePathUnpinsWithCorrectPinID verifies that when restore
// Phase 0 successfully pins the source snapshot but a later phase fails, the
// deferred Unpin is invoked exactly once with the same pinID returned by PinSnapshot.
// This guards the pin/unpin linkage that replaces the previous ref-count mechanism.
func TestRestoreSnapshot_FailurePathUnpinsWithCorrectPinID(t *testing.T) {
	ctx := context.Background()

	const expectedPinID int64 = 7777

	mPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(expectedPinID, 1, nil).Build()
	defer mPin.UnPatch()

	var unpinCalls []int64
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(func(_ *snapshotMeta, _ context.Context, pinID int64) (int64, string, int, error) {
		unpinCalls = append(unpinCalls, pinID)
		return 0, "", 0, nil
	}).Build()
	defer mUnpin.UnPatch()

	// Fail in Phase 1 (ReadSnapshotData) so the defer executes the pin release.
	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(nil, errors.New("read failed")).Build()
	defer m1.UnPatch()

	sm := &snapshotManager{
		allocator:       allocator.NewMockAllocator(t),
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}

	phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
	startRestoreLock := func(ctx context.Context, sourceCollectionID int64, snapshotName, targetDbName, targetCollectionName string) (broadcaster.BroadcastAPI, error) {
		return phase0Lock, nil
	}
	startBroadcaster := func(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error) {
		t.Fatal("startBroadcaster should not be reached")
		return nil, nil
	}
	rollback := func(ctx context.Context, dbName, collName string) error {
		t.Fatal("rollback should not be reached (pre-Phase 2 failure)")
		return nil
	}
	validateResources := func(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error {
		return nil
	}

	jobID, err := sm.RestoreSnapshot(ctx, int64(100), "snap1", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validateResources)

	assert.Error(t, err)
	assert.Equal(t, int64(0), jobID)
	// Defer unpinned exactly once with the exact pinID that PinSnapshot returned.
	assert.Equal(t, []int64{expectedPinID}, unpinCalls, "failure path must unpin with the pinID from PinSnapshot")
}

// TestRestoreSnapshot_PostPhase2FailurePathsUnpinAndRollback drives each phase
// past Phase 0 pin success and then fails it, asserting: (a) the deferred Unpin
// is invoked once with the correct pinID, (b) if a target collection was created,
// rollback is invoked. This tightens RestoreSnapshot failure-path coverage.
func TestRestoreSnapshot_PostPhase2FailurePathsUnpinAndRollback(t *testing.T) {
	cases := []struct {
		name            string
		setup           func() []*mockey.Mocker
		expectRollback  bool
		expectErrString string
	}{
		{
			name: "restore_collection_fails",
			setup: func() []*mockey.Mocker {
				m := []*mockey.Mocker{
					mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Name: "s"}}, nil).Build(),
					mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build(),
					mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(0), errors.New("rc fail")).Build(),
				}
				return m
			},
			expectRollback:  false, // collection not yet created
			expectErrString: "failed to restore collection",
		},
		{
			name: "restore_indexes_fails",
			setup: func() []*mockey.Mocker {
				return []*mockey.Mocker{
					mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Name: "s"}}, nil).Build(),
					mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build(),
					mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build(),
					mockey.Mock((*snapshotManager).RestoreIndexes).Return(errors.New("idx fail")).Build(),
				}
			},
			expectRollback:  true, // collection created, must roll back
			expectErrString: "failed to restore indexes",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const pinID int64 = 9000

			mPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(pinID, 1, nil).Build()
			defer mPin.UnPatch()

			unpinCalls := []int64{}
			mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
				func(_ *snapshotMeta, _ context.Context, p int64) (int64, string, int, error) {
					unpinCalls = append(unpinCalls, p)
					return 0, "", 0, nil
				}).Build()
			defer mUnpin.UnPatch()

			mockers := tc.setup()
			defer func() {
				for _, m := range mockers {
					m.UnPatch()
				}
			}()

			sm := &snapshotManager{
				allocator:       allocator.NewMockAllocator(t),
				snapshotMeta:    &snapshotMeta{},
				copySegmentMeta: &copySegmentMeta{},
			}
			phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
			startRestoreLock := func(ctx context.Context, _ int64, _, _, _ string) (broadcaster.BroadcastAPI, error) {
				return phase0Lock, nil
			}
			startBroadcaster := func(ctx context.Context, _ int64, _ string) (broadcaster.BroadcastAPI, error) {
				return &mockBroadcastAPI{closeFn: func() {}}, nil
			}
			rollbackCalled := 0
			rollback := func(ctx context.Context, _, _ string) error {
				rollbackCalled++
				return nil
			}
			validate := func(ctx context.Context, _ int64, _ *SnapshotData) error { return nil }

			_, err := sm.RestoreSnapshot(ctx, int64(100), "s", "target", "default",
				startRestoreLock, startBroadcaster, rollback, validate)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectErrString)
			assert.Equal(t, []int64{pinID}, unpinCalls, "failure path must unpin once with correct pinID")
			if tc.expectRollback {
				assert.Equal(t, 1, rollbackCalled, "rollback must run when target collection was already created")
			} else {
				assert.Equal(t, 0, rollbackCalled, "rollback must not run before collection is created")
			}
		})
	}
}

// TestRestoreSnapshot_AllocIDFailureUnpinsAndRollsBack verifies that if jobID
// allocation fails AFTER indexes are restored, the deferred Unpin fires and the
// target collection is rolled back.
func TestRestoreSnapshot_AllocIDFailureUnpinsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	const pinID int64 = 1234

	mPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(pinID, 1, nil).Build()
	defer mPin.UnPatch()
	unpinCalls := []int64{}
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, p int64) (int64, string, int, error) {
			unpinCalls = append(unpinCalls, p)
			return 0, "", 0, nil
		}).Build()
	defer mUnpin.UnPatch()

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "s"},
	}, nil).Build()
	defer m1.UnPatch()
	m2 := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer m2.UnPatch()
	m3 := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer m3.UnPatch()
	m4 := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer m4.UnPatch()

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(0), errors.New("alloc fail"))

	sm := &snapshotManager{
		allocator:       mockAlloc,
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}
	phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
	startRestoreLock := func(ctx context.Context, _ int64, _, _, _ string) (broadcaster.BroadcastAPI, error) {
		return phase0Lock, nil
	}
	startBroadcaster := func(ctx context.Context, _ int64, _ string) (broadcaster.BroadcastAPI, error) {
		t.Fatal("startBroadcaster must not be called if AllocID fails first")
		return nil, nil
	}
	rollbackCalled := 0
	rollback := func(ctx context.Context, _, _ string) error {
		rollbackCalled++
		return nil
	}
	validate := func(ctx context.Context, _ int64, _ *SnapshotData) error { return nil }

	_, err := sm.RestoreSnapshot(ctx, int64(100), "s", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validate)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to allocate job ID")
	assert.Equal(t, []int64{pinID}, unpinCalls)
	assert.Equal(t, 1, rollbackCalled)
}

// TestCreateRestoreJob_PropagatesPinID is a direct unit test for createRestoreJob
// (previously only exercised indirectly via mocked RestoreData paths). Verifies
// that the pinID parameter is persisted into CopySegmentJob.PinId — critical for
// the terminal-transition Unpin wiring in UpdateJobStateAndReleaseRef.
func TestCreateRestoreJob_PropagatesPinID(t *testing.T) {
	ctx := context.Background()
	const expectedPinID int64 = 314159

	// Use empty validSegments via empty SnapshotData.Segments so we skip the
	// per-segment heavy path (GetSegment / AddSegment / channel checkpoint).
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1", CollectionId: 100},
		Segments:     []*datapb.SegmentDescription{},
	}

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocN(int64(0)).Return(int64(0), int64(0), nil)

	mockHandler := NewNMockHandler(t)
	mockHandler.EXPECT().GetCollection(mock.Anything, int64(200)).Return(&collectionInfo{
		StartPositions: nil,
	}, nil)

	var captured *datapb.CopySegmentJob
	mAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
			captured = job.(*copySegmentJob).CopySegmentJob
			return nil
		}).Build()
	defer mAddJob.UnPatch()

	sm := &snapshotManager{
		meta:            &meta{},
		allocator:       mockAlloc,
		handler:         mockHandler,
		copySegmentMeta: &copySegmentMeta{},
	}

	err := sm.createRestoreJob(ctx, int64(200), map[string]string{}, map[int64]int64{}, snapshotData, int64(42), expectedPinID)
	assert.NoError(t, err)
	require.NotNil(t, captured, "AddJob must be invoked")
	assert.Equal(t, expectedPinID, captured.GetPinId(), "PinId must be propagated verbatim to the persisted job")
	assert.Equal(t, int64(42), captured.GetJobId())
	assert.Equal(t, int64(200), captured.GetCollectionId())
	assert.Equal(t, "snap1", captured.GetSnapshotName())
	assert.Equal(t, int64(100), captured.GetSourceCollectionId())
}

// TestSnapshotManager_HasActivePins_Delegation verifies the manager-layer wrapper
// delegates to snapshotMeta.HasActivePins and propagates both result and error.
func TestSnapshotManager_HasActivePins_Delegation(t *testing.T) {
	ctx := context.Background()

	// Case 1: delegation returns (true, nil)
	mTrue := mockey.Mock((*snapshotMeta).HasActivePins).Return(true, nil).Build()
	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}}
	active, err := sm.HasActivePins(ctx, 100, "snap")
	assert.NoError(t, err)
	assert.True(t, active)
	mTrue.UnPatch()

	// Case 2: delegation returns (false, err)
	mErr := mockey.Mock((*snapshotMeta).HasActivePins).Return(false, errors.New("not found")).Build()
	active, err = sm.HasActivePins(ctx, 100, "snap")
	assert.Error(t, err)
	assert.False(t, active)
	mErr.UnPatch()
}

// TestCreateRestoreJob_AllocNFailurePropagates verifies that segment-ID
// allocation failures are propagated and no job is persisted.
func TestCreateRestoreJob_AllocNFailurePropagates(t *testing.T) {
	ctx := context.Background()
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1", CollectionId: 100},
		Segments:     []*datapb.SegmentDescription{},
	}

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocN(int64(0)).Return(int64(0), int64(0), errors.New("alloc segment IDs failed"))

	addJobCalled := false
	mAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, _ CopySegmentJob) error {
			addJobCalled = true
			return nil
		}).Build()
	defer mAddJob.UnPatch()

	sm := &snapshotManager{
		meta:            &meta{},
		allocator:       mockAlloc,
		copySegmentMeta: &copySegmentMeta{},
	}

	err := sm.createRestoreJob(ctx, int64(200), nil, nil, snapshotData, int64(42), int64(7))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "alloc segment IDs failed")
	assert.False(t, addJobCalled, "AddJob must not be called when segment-ID allocation fails")
}

// TestRestoreSnapshot_StartBroadcasterFailureUnpinsAndRollsBack verifies
// failure at the startBroadcaster step (Phase 4) still triggers defer-unpin
// and rollback of the target collection.
func TestRestoreSnapshot_StartBroadcasterFailureUnpinsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	const pinID int64 = 555

	mPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(pinID, 1, nil).Build()
	defer mPin.UnPatch()
	unpinCalls := []int64{}
	mUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(_ *snapshotMeta, _ context.Context, p int64) (int64, string, int, error) {
			unpinCalls = append(unpinCalls, p)
			return 0, "", 0, nil
		}).Build()
	defer mUnpin.UnPatch()

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "s"},
	}, nil).Build()
	defer m1.UnPatch()
	m2 := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer m2.UnPatch()
	m3 := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer m3.UnPatch()
	m4 := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer m4.UnPatch()

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(77), nil)

	sm := &snapshotManager{
		allocator:       mockAlloc,
		snapshotMeta:    &snapshotMeta{},
		copySegmentMeta: &copySegmentMeta{},
	}
	phase0Lock := &mockBroadcastAPI{closeFn: func() {}}
	startRestoreLock := func(ctx context.Context, _ int64, _, _, _ string) (broadcaster.BroadcastAPI, error) {
		return phase0Lock, nil
	}
	startBroadcaster := func(ctx context.Context, _ int64, _ string) (broadcaster.BroadcastAPI, error) {
		return nil, errors.New("broadcaster init fail")
	}
	rollbackCalled := 0
	rollback := func(ctx context.Context, _, _ string) error {
		rollbackCalled++
		return nil
	}
	validate := func(ctx context.Context, _ int64, _ *SnapshotData) error { return nil }

	_, err := sm.RestoreSnapshot(ctx, int64(100), "s", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validate)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start broadcaster")
	assert.Equal(t, []int64{pinID}, unpinCalls)
	assert.Equal(t, 1, rollbackCalled)
}

// mockBroadcastAPI implements broadcaster.BroadcastAPI for testing.
type mockBroadcastAPI struct {
	closeFn     func()
	broadcastFn func()
}

func (m *mockBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	if m.broadcastFn != nil {
		m.broadcastFn()
	}
	return &types.BroadcastAppendResult{}, nil
}

func (m *mockBroadcastAPI) Close() {
	if m.closeFn != nil {
		m.closeFn()
	}
}

// Ensure mockBroadcastAPI satisfies the broadcaster.BroadcastAPI interface.
var _ broadcaster.BroadcastAPI = (*mockBroadcastAPI)(nil)

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
		nil, // indexEngineVersionManager
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
		collectionID int64,
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

	result, err := sm.ReadSnapshotData(ctx, int64(100), "test_snapshot")

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
		collectionID int64,
		snapshotName string,
		includeSegments bool,
	) (*SnapshotData, error) {
		return nil, expectedErr
	}).Build()
	defer mockRead.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	result, err := sm.ReadSnapshotData(ctx, int64(100), "nonexistent")

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
		collectionID int64,
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
		pinID int64,
	) error {
		assert.Equal(t, int64(200), collectionID)
		assert.Equal(t, int64(12345), jobID)
		return nil
	}).Build()
	defer mockCreateJob.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, int64(100), snapshotData.SnapshotInfo.GetName(), 200, 12345, int64(0))

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
	jobID, err := sm.RestoreData(ctx, int64(100), snapshotData.SnapshotInfo.GetName(), 200, 12345, int64(0))

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
		collectionID int64,
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

	jobID, err := sm.RestoreData(ctx, int64(100), snapshotData.SnapshotInfo.GetName(), 200, 12345, int64(0))

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
		collectionID int64,
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

	jobID, err := sm.RestoreData(ctx, int64(100), snapshotData.SnapshotInfo.GetName(), 200, 12345, int64(0))

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
		collectionID int64,
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
		pinID int64,
	) error {
		return expectedErr
	}).Build()
	defer mockCreateJob.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, int64(100), snapshotData.SnapshotInfo.GetName(), 200, 12345, int64(0))

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
		collectionID int64,
		name string,
	) (*SnapshotData, error) {
		assert.Equal(t, "test_snapshot", name)
		return nil, expectedErr
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	sm := &snapshotManager{
		copySegmentMeta: &copySegmentMeta{},
	}

	jobID, err := sm.RestoreData(ctx, int64(100), "test_snapshot", 200, 12345, int64(0))

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

// --- Test DropSnapshotsByCollection ---

func TestSnapshotManager_DropSnapshotsByCollection_Success(t *testing.T) {
	ctx := context.Background()

	mockDrop := mockey.Mock((*snapshotMeta).DropSnapshotsByCollection).To(
		func(sm *snapshotMeta, ctx context.Context, collectionID int64) ([]string, error) {
			assert.Equal(t, int64(100), collectionID)
			return nil, nil
		},
	).Build()
	defer mockDrop.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	err := sm.DropSnapshotsByCollection(ctx, 100)
	assert.NoError(t, err)
}

func TestSnapshotManager_DropSnapshotsByCollection_Error(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("drop failed")
	mockDrop := mockey.Mock((*snapshotMeta).DropSnapshotsByCollection).Return([]string(nil), expectedErr).Build()
	defer mockDrop.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	err := sm.DropSnapshotsByCollection(ctx, 200)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotManager_DropSnapshotsByCollection_NoSnapshots(t *testing.T) {
	ctx := context.Background()

	// When no snapshots exist for the collection, snapshotMeta returns nil
	mockDrop := mockey.Mock((*snapshotMeta).DropSnapshotsByCollection).Return([]string(nil), nil).Build()
	defer mockDrop.UnPatch()

	sm := NewSnapshotManager(
		nil,
		&snapshotMeta{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	err := sm.DropSnapshotsByCollection(ctx, 999)
	assert.NoError(t, err)
}

// --- Test getDBCollectionIDs ---

func TestSnapshotManager_getDBCollectionIDs(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	m.collections.Insert(1, &collectionInfo{ID: 1, DatabaseID: 10})
	m.collections.Insert(2, &collectionInfo{ID: 2, DatabaseID: 10})
	m.collections.Insert(3, &collectionInfo{ID: 3, DatabaseID: 20})
	m.collections.Insert(4, &collectionInfo{ID: 4, DatabaseID: 10})
	m.collections.Insert(5, &collectionInfo{ID: 5, DatabaseID: 30})

	sm := &snapshotManager{
		meta: m,
	}

	// Filter for dbID=10, should get collections 1, 2, 4
	result := sm.getDBCollectionIDs(10)
	assert.Len(t, result, 3)
	assert.Contains(t, result, int64(1))
	assert.Contains(t, result, int64(2))
	assert.Contains(t, result, int64(4))

	// Filter for dbID=20, should get collection 3 only
	result = sm.getDBCollectionIDs(20)
	assert.Len(t, result, 1)
	assert.Contains(t, result, int64(3))

	// Filter for dbID=30, should get collection 5 only
	result = sm.getDBCollectionIDs(30)
	assert.Len(t, result, 1)
	assert.Contains(t, result, int64(5))
}

func TestSnapshotManager_getDBCollectionIDs_EmptyResult(t *testing.T) {
	m := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	m.collections.Insert(1, &collectionInfo{ID: 1, DatabaseID: 10})
	m.collections.Insert(2, &collectionInfo{ID: 2, DatabaseID: 20})

	sm := &snapshotManager{
		meta: m,
	}

	// No collections for dbID=999
	result := sm.getDBCollectionIDs(999)
	assert.Empty(t, result)
	assert.Len(t, result, 0)
}

// --- Test PinSnapshotData ---

func TestSnapshotManager_PinSnapshotData_Success(t *testing.T) {
	ctx := context.Background()

	mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(5001), 1, nil).Build()
	defer mockPin.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	pinID, err := sm.PinSnapshotData(ctx, 100, "test_snap", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(5001), pinID)
}

func TestSnapshotManager_PinSnapshotData_Error(t *testing.T) {
	ctx := context.Background()

	mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(0), 0, errors.New("snapshot not found")).Build()
	defer mockPin.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	_, err := sm.PinSnapshotData(ctx, 100, "nonexistent", 0)
	assert.Error(t, err)
}

// --- Test UnpinSnapshotData ---

func TestSnapshotManager_UnpinSnapshotData_Success(t *testing.T) {
	ctx := context.Background()

	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
	defer mockUnpin.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	err := sm.UnpinSnapshotData(ctx, 5001)
	assert.NoError(t, err)
}

func TestSnapshotManager_UnpinSnapshotData_Error(t *testing.T) {
	ctx := context.Background()

	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, errors.New("not pinned")).Build()
	defer mockUnpin.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
	}

	err := sm.UnpinSnapshotData(ctx, 99999)
	assert.Error(t, err)
}
