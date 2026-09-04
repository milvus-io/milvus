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
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type restoreAllocatorTarget struct {
	allocator.Allocator
}

type restoreBrokerTarget struct {
	broker.Broker
}

type restoreWALAccesserTarget struct {
	streaming.WALAccesser
}

// --- Test CreateSnapshot ---

func TestSnapshotManager_CreateSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	mockAllocator := allocator.NewMockAllocator(t)
	mockHandler := NewNMockHandler(t)

	// Mock allocator to return snapshot ID
	mockAllocator.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()

	// Mock handler to generate snapshot data
	snapshotData := &snapshotstorage.SnapshotData{
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

	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *snapshotstorage.SnapshotData) error {
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

	snapshotData := &snapshotstorage.SnapshotData{
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

	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *snapshotstorage.SnapshotData) error {
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

	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	// Mock snapshotMeta methods
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	expectedErr := errors.New("save error")
	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *snapshotstorage.SnapshotData) error {
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

	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100},
	}
	mockHandler.EXPECT().GenSnapshot(mock.Anything, int64(100)).Return(snapshotData, nil).Once()

	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string) (*datapb.SnapshotInfo, error) {
		return nil, errors.New("not found")
	}).Build()
	defer mockGetSnapshot.UnPatch()

	expectedErr := errors.New("save error")
	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *snapshotstorage.SnapshotData) error {
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	mockSaveSnapshot := mockey.Mock((*snapshotMeta).SaveSnapshot).To(func(sm *snapshotMeta, ctx context.Context, data *snapshotstorage.SnapshotData) error {
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

	expectedData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1001,
			Name:         "test_snapshot",
			CollectionId: 100,
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, NumOfRows: 100},
		},
	}

	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
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
	mockReadSnapshotData := mockey.Mock((*snapshotMeta).ReadSnapshotData).To(func(sm *snapshotMeta, ctx context.Context, collectionID int64, name string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
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
	snapshotData := &snapshotstorage.SnapshotData{
		Collection: &datapb.CollectionDescription{
			VirtualChannelNames: []string{"dml_1_100v1", "dml_0_100v0"},
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, ChannelName: "dml_0_100v0"},
			{SegmentId: 2, ChannelName: "dml_1_100v1"},
		},
	}

	// Mock getChannelsByCollectionID - target collection has same pchannels
	getChannelsFunc := func(ctx context.Context, collectionID int64) ([]RWChannel, error) {
		return []RWChannel{
			&channelMeta{Name: "dml_1_200v1"},
			&channelMeta{Name: "dml_0_200v0"},
		}, nil
	}

	sm := &snapshotManager{
		getChannelsByCollectionID: getChannelsFunc,
	}

	originalChannels := append([]string(nil), snapshotData.Collection.GetVirtualChannelNames()...)

	// Execute
	mapping, err := sm.buildChannelMapping(ctx, snapshotData, 200)

	// Verify
	assert.NoError(t, err)
	assert.Len(t, mapping, 2)
	// Mapping by pchannel: dml_0_100v0 -> dml_0_200v0, dml_1_100v1 -> dml_1_200v1
	assert.Equal(t, "dml_0_200v0", mapping["dml_0_100v0"])
	assert.Equal(t, "dml_1_200v1", mapping["dml_1_100v1"])
	assert.Equal(t, originalChannels, snapshotData.Collection.GetVirtualChannelNames())
}

func TestSnapshotManager_BuildChannelMapping_EmptySegments(t *testing.T) {
	ctx := context.Background()

	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{
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

	validateResources := func(ctx context.Context, collectionID int64, snapshotData *snapshotstorage.SnapshotData) error {
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

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{
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
	validateResources := func(ctx context.Context, collectionID int64, snapshotData *snapshotstorage.SnapshotData) error {
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

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{
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
	validateResources := func(ctx context.Context, collectionID int64, snapshotData *snapshotstorage.SnapshotData) error {
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
	validate := func(ctx context.Context, _ int64, _ *snapshotstorage.SnapshotData) error { return nil }

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
	validateResources := func(ctx context.Context, collectionID int64, snapshotData *snapshotstorage.SnapshotData) error {
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
					mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Name: "s"}}, nil).Build(),
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
					mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Name: "s"}}, nil).Build(),
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
			validate := func(ctx context.Context, _ int64, _ *snapshotstorage.SnapshotData) error { return nil }

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

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{
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
	validate := func(ctx context.Context, _ int64, _ *snapshotstorage.SnapshotData) error { return nil }

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

	// Use empty validSegments via empty snapshotstorage.SnapshotData.Segments so we skip the
	// per-segment heavy path (GetSegment / AddSegment / channel checkpoint).
	snapshotData := &snapshotstorage.SnapshotData{
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

	err := sm.createRestoreJob(ctx, int64(200), map[string]string{}, map[int64]int64{}, snapshotData, int64(42), expectedPinID, false, "", "", "")
	assert.NoError(t, err)
	require.NotNil(t, captured, "AddJob must be invoked")
	assert.Equal(t, expectedPinID, captured.GetPinId(), "PinId must be propagated verbatim to the persisted job")
	assert.Equal(t, int64(42), captured.GetJobId())
	assert.Equal(t, int64(200), captured.GetCollectionId())
	assert.Equal(t, "snap1", captured.GetSnapshotName())
	assert.Equal(t, int64(100), captured.GetSourceCollectionId())
}

func TestCreateRestoreJob_PreRegistersTargetSegmentsAsImporting(t *testing.T) {
	ctx := context.Background()

	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1", CollectionId: 100},
		Collection:   &datapb.CollectionDescription{},
		Segments: []*datapb.SegmentDescription{{
			SegmentId:      11,
			PartitionId:    10,
			SegmentLevel:   datapb.SegmentLevel_L1,
			ChannelName:    "src-ch",
			NumOfRows:      123,
			StorageVersion: 3,
			IsSorted:       true,
		}},
	}

	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, seg *datapb.SegmentInfo) error {
		assert.Equal(t, int64(2001), seg.GetID())
		assert.Equal(t, int64(200), seg.GetCollectionID())
		assert.Equal(t, int64(20), seg.GetPartitionID())
		assert.Equal(t, "dst-ch", seg.GetInsertChannel())
		assert.Equal(t, commonpb.SegmentState_Importing, seg.GetState())
		assert.True(t, seg.GetIsImporting())
		assert.Equal(t, int64(3), seg.GetStorageVersion())
		return nil
	}).Once()
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, "dst-ch", mock.Anything).Return(nil).Once()

	mt := &meta{ctx: ctx, catalog: catalog, segments: NewSegmentsInfo(), channelCPs: newChannelCps()}
	mt.segments.SetSegment(11, NewSegmentInfo(&datapb.SegmentInfo{ID: 11}))

	var err error

	mockAlloc := allocator.NewMockAllocator(t)
	mockAlloc.EXPECT().AllocN(int64(1)).Return(int64(2001), int64(2002), nil)

	mockHandler := NewNMockHandler(t)
	mockHandler.EXPECT().GetCollection(mock.Anything, int64(200)).Return(&collectionInfo{StartPositions: []*commonpb.KeyDataPair{{Key: "dst-ch", Data: []byte{1}}}}, nil)

	addJobCalled := false
	mAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, _ CopySegmentJob) error {
			addJobCalled = true
			return nil
		}).Build()
	defer mAddJob.UnPatch()

	sm := &snapshotManager{
		meta:            mt,
		allocator:       mockAlloc,
		handler:         mockHandler,
		copySegmentMeta: &copySegmentMeta{},
	}

	err = sm.createRestoreJob(ctx, int64(200), map[string]string{"src-ch": "dst-ch"}, map[int64]int64{10: 20}, snapshotData, int64(42), int64(7), false, "", "", "")
	require.NoError(t, err)
	assert.True(t, addJobCalled)
}

func TestCreateRestoreJob_ExternalPersistsSourceLocationAndSkipsLocalSegmentLookup(t *testing.T) {
	ctx := context.Background()
	const snapshotLocation = "s3://bucket/files/snapshots/meta.json"

	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1", CollectionId: 100},
		Collection:   &datapb.CollectionDescription{},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 10, PartitionId: 20, ChannelName: "source-channel", NumOfRows: 99},
		},
	}

	getSegmentCalled := false
	mGetSegment := mockey.Mock((*meta).GetSegment).To(func(_ *meta, _ context.Context, _ int64) *SegmentInfo {
		getSegmentCalled = true
		return nil
	}).Build()
	defer mGetSegment.UnPatch()
	mAddSegment := mockey.Mock((*meta).AddSegment).Return(nil).Build()
	defer mAddSegment.UnPatch()
	mUpdateCheckpoint := mockey.Mock((*meta).UpdateChannelCheckpoint).Return(nil).Build()
	defer mUpdateCheckpoint.UnPatch()

	var captured *datapb.CopySegmentJob
	mAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
			captured = job.(*copySegmentJob).CopySegmentJob
			return nil
		}).Build()
	defer mAddJob.UnPatch()
	alloc := &restoreAllocatorTarget{}
	mockAllocN := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(typeutil.UniqueID(1000), typeutil.UniqueID(1001), nil).Build()
	defer mockAllocN.UnPatch()

	sm := &snapshotManager{
		meta:            &meta{},
		allocator:       alloc,
		handler:         newMockHandler(),
		copySegmentMeta: &copySegmentMeta{},
	}

	expectedFingerprint, err := snapshotstorage.SnapshotFingerprint(snapshotData)
	require.NoError(t, err)
	err = sm.createRestoreJob(ctx, int64(200), map[string]string{"source-channel": "target-channel"}, map[int64]int64{20: 30}, snapshotData, int64(42), int64(7), true, snapshotLocation, `{"extfs":{"region":"us-west-2"}}`, expectedFingerprint)
	assert.NoError(t, err)
	assert.False(t, getSegmentCalled, "external restore must not require source segments in local meta")
	require.NotNil(t, captured, "AddJob must be invoked")
	assert.True(t, captured.GetExternal())
	assert.Equal(t, snapshotLocation, captured.GetSnapshotS3Location())
	assert.Equal(t, `{"extfs":{"region":"us-west-2"}}`, captured.GetExternalSpec())
	assert.Equal(t, expectedFingerprint, captured.GetSnapshotFingerprint())
	assert.Equal(t, int64(1), captured.GetTotalSegments())
	assert.Len(t, captured.GetIdMappings(), 1)
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
	snapshotData := &snapshotstorage.SnapshotData{
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

	err := sm.createRestoreJob(ctx, int64(200), nil, nil, snapshotData, int64(42), int64(7), false, "", "", "")
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

	m1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(&snapshotstorage.SnapshotData{
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
	validate := func(ctx context.Context, _ int64, _ *snapshotstorage.SnapshotData) error { return nil }

	_, err := sm.RestoreSnapshot(ctx, int64(100), "s", "target", "default",
		startRestoreLock, startBroadcaster, rollback, validate)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start broadcaster")
	assert.Equal(t, []int64{pinID}, unpinCalls)
	assert.Equal(t, 1, rollbackCalled)
}

func TestFinishRestoreSnapshot_BroadcastFailureRollbackBoundary(t *testing.T) {
	oldWAL := streaming.WAL()
	fakeWAL := &restoreWALAccesserTarget{}
	mockControlChannel := mockey.Mock((*restoreWALAccesserTarget).ControlChannel).
		Return("control_channel").Build()
	defer mockControlChannel.UnPatch()
	streaming.SetWALForTest(fakeWAL)
	defer streaming.SetWALForTest(oldWAL)

	mockRestoreCollection := mockey.Mock((*snapshotManager).RestoreCollection).Return(int64(200), nil).Build()
	defer mockRestoreCollection.UnPatch()
	mockRestoreIndexes := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer mockRestoreIndexes.UnPatch()
	alloc := &embeddedAllocator{}
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocID).Return(typeutil.UniqueID(999), nil).Build()
	defer mockAlloc.UnPatch()

	tests := []struct {
		name          string
		broadcastErr  error
		wantJobID     int64
		wantRollback  bool
		wantErr       bool
		wantErrorText string
	}{
		{
			name:          "task not created rolls back",
			broadcastErr:  errors.Mark(errors.New("broadcast rejected"), broadcaster.ErrBroadcastTaskNotCreated),
			wantRollback:  true,
			wantErr:       true,
			wantErrorText: "failed to broadcast restore message",
		},
		{
			name:         "registered task is accepted",
			broadcastErr: errors.New("ack wait canceled"),
			wantJobID:    999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rollbackCalls := 0
			api := &mockBroadcastAPI{broadcastErr: tt.broadcastErr}
			sm := &snapshotManager{allocator: alloc}

			jobID, err := sm.finishRestoreSnapshot(
				context.Background(),
				mlog.With(),
				&snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot"}},
				"snapshot",
				100,
				"target",
				"default",
				0,
				false,
				"",
				"",
				func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) { return api, nil },
				func(context.Context, string, string) error {
					rollbackCalls++
					return nil
				},
				func(context.Context, int64, *snapshotstorage.SnapshotData) error { return nil },
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrorText)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantJobID, jobID)
			if tt.wantRollback {
				assert.Equal(t, 1, rollbackCalls)
			} else {
				assert.Zero(t, rollbackCalls)
			}
		})
	}
}

// mockBroadcastAPI implements broadcaster.BroadcastAPI for testing.
type mockBroadcastAPI struct {
	closeFn      func()
	broadcastFn  func()
	broadcastErr error
}

func (m *mockBroadcastAPI) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	if m.broadcastFn != nil {
		m.broadcastFn()
	}
	return &types.BroadcastAppendResult{}, m.broadcastErr
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

	expectedData := &snapshotstorage.SnapshotData{
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
	) (*snapshotstorage.SnapshotData, error) {
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
	) (*snapshotstorage.SnapshotData, error) {
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	) (*snapshotstorage.SnapshotData, error) {
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
		snapshotData *snapshotstorage.SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return map[int64]int64{1: 10}, nil
	}).Build()
	defer mockBuildPartition.UnPatch()

	// Mock buildChannelMapping
	mockBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *snapshotstorage.SnapshotData,
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
		snapshotData *snapshotstorage.SnapshotData,
		jobID int64,
		pinID int64,
		external bool,
		snapshotS3Location string,
		externalSpec string,
		snapshotFingerprint string,
	) error {
		assert.Equal(t, int64(200), collectionID)
		assert.Equal(t, int64(12345), jobID)
		assert.False(t, external)
		assert.Empty(t, snapshotS3Location)
		assert.Empty(t, externalSpec)
		assert.Empty(t, snapshotFingerprint)
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

	snapshotData := &snapshotstorage.SnapshotData{
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	) (*snapshotstorage.SnapshotData, error) {
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
		snapshotData *snapshotstorage.SnapshotData,
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	) (*snapshotstorage.SnapshotData, error) {
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
		snapshotData *snapshotstorage.SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return map[int64]int64{1: 10}, nil
	}).Build()
	defer mockBuildPartition.UnPatch()

	// Mock buildChannelMapping to return error
	mockBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *snapshotstorage.SnapshotData,
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	) (*snapshotstorage.SnapshotData, error) {
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
		snapshotData *snapshotstorage.SnapshotData,
		collectionID int64,
	) (map[int64]int64, error) {
		return map[int64]int64{1: 10}, nil
	}).Build()
	defer mockBuildPartition.UnPatch()

	// Mock buildChannelMapping
	mockBuildChannel := mockey.Mock((*snapshotManager).buildChannelMapping).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotData *snapshotstorage.SnapshotData,
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
		snapshotData *snapshotstorage.SnapshotData,
		jobID int64,
		pinID int64,
		external bool,
		snapshotS3Location string,
		externalSpec string,
		snapshotFingerprint string,
	) error {
		assert.False(t, external)
		assert.Empty(t, snapshotS3Location)
		assert.Empty(t, externalSpec)
		assert.Empty(t, snapshotFingerprint)
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
	) (*snapshotstorage.SnapshotData, error) {
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

	snapshotData := &snapshotstorage.SnapshotData{
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

	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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
	snapshotData := &snapshotstorage.SnapshotData{
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

func TestSnapshotManager_RestoreCollection_PreservesMetadata(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "current snapshot"
		if legacy {
			name = "snapshot created before metadata fix"
		}
		t.Run(name, func(t *testing.T) {
			sourceProperties := []*commonpb.KeyValuePair{
				{Key: common.CollectionTTLConfigKey, Value: "360"},
				{Key: common.CollectionAutoCompactionKey, Value: "false"},
				{Key: common.MmapEnabledKey, Value: "false"},
				{Key: common.AllowInsertAutoIDKey, Value: "false"},
			}
			schemaProperties := common.CloneKeyValuePairs(sourceProperties)
			schemaProperties = append(schemaProperties, &commonpb.KeyValuePair{
				Key:   common.ConsistencyLevel,
				Value: strconv.Itoa(int(commonpb.ConsistencyLevel_Bounded)),
			})

			collection := &datapb.CollectionDescription{
				Schema: &schemapb.CollectionSchema{
					Name:       "original_collection",
					DbName:     "original_db",
					Properties: schemaProperties,
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					},
				},
				NumShards: 1,
			}
			if !legacy {
				collection.ConsistencyLevel = commonpb.ConsistencyLevel_Bounded
				collection.Properties = common.CloneKeyValuePairs(sourceProperties)
			}

			originalCollectionProperties := common.CloneKeyValuePairs(collection.GetProperties())
			originalSchemaProperties := common.CloneKeyValuePairs(collection.GetSchema().GetProperties())

			var capturedReq *milvuspb.CreateCollectionRequest
			fakeBroker := &embeddedBroker{}
			mockCreateCollection := mockey.Mock((*embeddedBroker).CreateCollection).To(
				func(_ *embeddedBroker, _ context.Context, req *milvuspb.CreateCollectionRequest) error {
					capturedReq = proto.Clone(req).(*milvuspb.CreateCollectionRequest)
					return nil
				}).Build()
			defer mockCreateCollection.UnPatch()

			mockDescribeCollection := mockey.Mock((*embeddedBroker).DescribeCollectionByName).
				Return(&milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 12345,
				}, nil).
				Build()
			defer mockDescribeCollection.UnPatch()

			sm := &snapshotManager{broker: fakeBroker}
			collectionID, err := sm.RestoreCollection(
				context.Background(),
				&snapshotstorage.SnapshotData{Collection: collection},
				"target_collection",
				"target_db",
			)

			require.NoError(t, err)
			assert.Equal(t, int64(12345), collectionID)
			require.NotNil(t, capturedReq)

			requestProperties := common.KeyValuePairs(capturedReq.GetProperties()).ToMap()
			for _, property := range sourceProperties {
				assert.Equal(t, property.GetValue(), requestProperties[property.GetKey()])
			}
			assert.Equal(t, "true", requestProperties[util.PreserveFieldIdsKey])

			if legacy {
				assert.Equal(t, strconv.Itoa(int(commonpb.ConsistencyLevel_Bounded)), requestProperties[common.ConsistencyLevel])
			} else {
				assert.Equal(t, commonpb.ConsistencyLevel_Bounded, capturedReq.GetConsistencyLevel())
			}

			assert.Equal(t, originalCollectionProperties, common.KeyValuePairs(collection.GetProperties()))
			assert.Equal(t, originalSchemaProperties, common.KeyValuePairs(collection.GetSchema().GetProperties()))
		})
	}
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

func TestSnapshotManager_ExportSnapshot_SubmitsJob(t *testing.T) {
	ctx := context.Background()
	manager := &snapshotExportManager{}
	mockSubmit := mockey.Mock((*snapshotExportManager).Submit).To(
		func(
			_ *snapshotExportManager,
			gotCtx context.Context,
			collectionID int64,
			snapshotName string,
			dbName string,
			collectionName string,
			targetPath string,
			externalSpec string,
		) (int64, error) {
			assert.Equal(t, ctx, gotCtx)
			assert.Equal(t, int64(100), collectionID)
			assert.Equal(t, "snapshot-1", snapshotName)
			assert.Equal(t, "default", dbName)
			assert.Equal(t, "collection-1", collectionName)
			assert.Equal(t, "s3://foreign-bucket/export-root", targetPath)
			assert.Equal(t, `{"extfs":{"region":"us-west-2"}}`, externalSpec)
			return 9001, nil
		}).Build()
	defer mockSubmit.UnPatch()

	sm := &snapshotManager{exportManager: manager}
	jobID, err := sm.ExportSnapshot(
		ctx,
		100,
		"snapshot-1",
		"default",
		"collection-1",
		"s3://foreign-bucket/export-root",
		`{"extfs":{"region":"us-west-2"}}`,
	)

	require.NoError(t, err)
	assert.Equal(t, int64(9001), jobID)
}

func TestSnapshotManager_ExportSnapshot_RequiresManager(t *testing.T) {
	_, err := (&snapshotManager{}).ExportSnapshot(
		context.Background(),
		100,
		"snapshot-1",
		"default",
		"collection-1",
		"s3://foreign-bucket/export-root",
		"",
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot export manager is not initialized")
}

func TestSnapshotExportManager_LockTargetSerializesEquivalentRoots(t *testing.T) {
	manager := newSnapshotExportManager(context.Background(), nil, nil)
	target := snapshotExportTarget{bucket: "shared-bucket", root: "export-root"}
	unlockFirst, err := manager.lockTarget(context.Background(), target)
	require.NoError(t, err)

	acquiredSecond := make(chan struct{})
	releaseSecond := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		unlockSecond, lockErr := manager.lockTarget(context.Background(), target)
		if lockErr != nil {
			return
		}
		close(acquiredSecond)
		<-releaseSecond
		unlockSecond()
	}()

	select {
	case <-acquiredSecond:
		require.FailNow(t, "equivalent export roots must be serialized")
	case <-time.After(50 * time.Millisecond):
	}

	unlockFirst()
	select {
	case <-acquiredSecond:
	case <-time.After(time.Second):
		require.FailNow(t, "second export did not acquire the released target lock")
	}
	close(releaseSecond)
	<-secondDone
}

func TestNamespacedSnapshotExportTarget(t *testing.T) {
	const namespace = "550e8400-e29b-41d4-a716-446655440000"
	tests := []struct {
		name       string
		targetPath string
		expected   string
	}{
		{
			name:       "object key",
			targetPath: "backup/root/",
			expected:   "backup/root/exports/" + namespace,
		},
		{
			name:       "S3 URI",
			targetPath: "s3://bucket/backup/root",
			expected:   "s3://bucket/backup/root/exports/" + namespace,
		},
		{
			name:       "endpoint URI",
			targetPath: "https://minio.example.com/bucket/backup/root/",
			expected:   "https://minio.example.com/bucket/backup/root/exports/" + namespace,
		},
		{
			name:       "GCS URI",
			targetPath: "gs://bucket/backup/root",
			expected:   "gs://bucket/backup/root/exports/" + namespace,
		},
		{
			name:       "Azure URI",
			targetPath: "azure://account.blob.core.windows.net/container/backup/root",
			expected:   "azure://account.blob.core.windows.net/container/backup/root/exports/" + namespace,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, namespacedSnapshotExportTarget(test.targetPath, namespace))
		})
	}
}

func TestSnapshotExportManager_SubmitDurableJob(t *testing.T) {
	t.Run("accepted job outlives submission context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events := make([]string, 0, 5)

		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).To(
			func(*objectstorage.Config, snapshotstorage.Direction, string, string) error {
				events = append(events, "validate")
				return nil
			}).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).To(
			func(*snapshotMeta, context.Context, int64, string) (*datapb.SnapshotInfo, error) {
				events = append(events, "lookup")
				return &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot-1"}, nil
			}).Build()
		defer mockGetSnapshot.UnPatch()
		mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).To(
			func(*snapshotMeta, context.Context, int64, string, int64) (int64, int, error) {
				events = append(events, "pin")
				return 7001, 1, nil
			}).Build()
		defer mockPin.UnPatch()
		allocatorTarget := &restoreAllocatorTarget{}
		mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).To(
			func(*restoreAllocatorTarget, context.Context) (typeutil.UniqueID, error) {
				events = append(events, "allocate")
				return 9001, nil
			}).Build()
		defer mockAlloc.UnPatch()

		catalog := newSnapshotExportCatalogFake()
		meta, err := newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		catalog.beforeSave = func(*datapb.ExportSnapshotJob) {
			events = append(events, "persist")
			cancel()
		}
		snapshotMeta := &snapshotMeta{}
		snapshotManager := &snapshotManager{snapshotMeta: snapshotMeta, allocator: allocatorTarget}
		manager := newSnapshotExportManager(context.Background(), meta, snapshotManager)

		jobID, err := manager.Submit(
			ctx,
			100,
			"snapshot-1",
			"default",
			"collection-1",
			"s3://target-bucket/export-root",
			`{"extfs":{"access_key_id":"AK","access_key_value":"SK"}}`,
		)

		require.NoError(t, err)
		assert.Equal(t, int64(9001), jobID)
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.Equal(t, []string{"validate", "lookup", "allocate", "pin", "persist"}, events)
		job, ok := meta.GetJob(jobID)
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPending, job.GetState())
		assert.Equal(t, int64(7001), job.GetPinId())
		assert.NotEmpty(t, job.GetExternalSpec())
		const targetPrefix = "s3://target-bucket/export-root/exports/"
		require.True(t, strings.HasPrefix(job.GetTargetS3Path(), targetPrefix))
		_, parseErr := uuid.Parse(strings.TrimPrefix(job.GetTargetS3Path(), targetPrefix))
		require.NoError(t, parseErr)

		reloadedMeta, err := newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		reloadedJob, ok := reloadedMeta.GetJob(jobID)
		require.True(t, ok)
		assert.Equal(t, job.GetTargetS3Path(), reloadedJob.GetTargetS3Path())
	})

	t.Run("persistence failure releases pin with live context", func(t *testing.T) {
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(
			&datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot-1"}, nil).Build()
		defer mockGetSnapshot.UnPatch()
		mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(7001), 1, nil).Build()
		defer mockPin.UnPatch()
		cleanupCalled := false
		mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
			func(_ *snapshotMeta, cleanupCtx context.Context, pinID int64) (int64, string, int, error) {
				cleanupCalled = true
				assert.NoError(t, cleanupCtx.Err())
				assert.Equal(t, int64(7001), pinID)
				return 100, "snapshot-1", 0, nil
			}).Build()
		defer mockUnpin.UnPatch()
		allocatorTarget := &restoreAllocatorTarget{}
		mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
		defer mockAlloc.UnPatch()

		catalog := newSnapshotExportCatalogFake()
		catalog.saveErr = errors.New("etcd unavailable")
		meta, err := newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		snapshotManager := &snapshotManager{snapshotMeta: &snapshotMeta{}, allocator: allocatorTarget}
		manager := newSnapshotExportManager(context.Background(), meta, snapshotManager)

		jobID, err := manager.Submit(
			context.Background(),
			100,
			"snapshot-1",
			"default",
			"collection-1",
			"s3://target-bucket/export-root",
			"",
		)

		require.Error(t, err)
		assert.Zero(t, jobID)
		assert.True(t, cleanupCalled)
		_, ok := meta.GetJob(9001)
		assert.False(t, ok)
	})
}

func TestSnapshotExportManager_SubmitSeparatesMatchingClusterLocalIDs(t *testing.T) {
	mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
	defer mockValidate.UnPatch()
	mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(
		&datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot-1"}, nil).Build()
	defer mockGetSnapshot.UnPatch()
	mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(7001), 1, nil).Build()
	defer mockPin.UnPatch()
	mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
	defer mockAlloc.UnPatch()

	newManager := func() (*snapshotExportManager, *snapshotExportMeta) {
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake())
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, &snapshotManager{
			snapshotMeta: &snapshotMeta{},
			allocator:    &restoreAllocatorTarget{},
		})
		return manager, meta
	}
	managerA, metaA := newManager()
	managerB, metaB := newManager()

	jobIDA, err := managerA.Submit(
		context.Background(), 100, "snapshot-1", "default", "collection-1", "s3://bucket/export-root", "")
	require.NoError(t, err)
	jobIDB, err := managerB.Submit(
		context.Background(), 100, "snapshot-1", "default", "collection-1", "s3://bucket/export-root", "")
	require.NoError(t, err)
	assert.Equal(t, jobIDA, jobIDB)

	jobA, ok := metaA.GetJob(jobIDA)
	require.True(t, ok)
	jobB, ok := metaB.GetJob(jobIDB)
	require.True(t, ok)
	assert.NotEqual(t, jobA.GetTargetS3Path(), jobB.GetTargetS3Path())
	assert.True(t, strings.HasPrefix(jobA.GetTargetS3Path(), "s3://bucket/export-root/exports/"))
	assert.True(t, strings.HasPrefix(jobB.GetTargetS3Path(), "s3://bucket/export-root/exports/"))
}

func waitForSnapshotExportJobState(
	t *testing.T,
	meta *snapshotExportMeta,
	jobID int64,
	state datapb.ExportSnapshotJobState,
) *datapb.ExportSnapshotJob {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, ok := meta.GetJob(jobID)
		if ok && job.GetState() == state {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, _ := meta.GetJob(jobID)
	require.Equal(t, state, job.GetState())
	return job
}

func preparePublishingSnapshotExportJob(
	t *testing.T,
	targetCM storage.ChunkManager,
	targetRoot string,
	jobID int64,
) *datapb.ExportSnapshotJob {
	t.Helper()
	snapshot := createTestSnapshotDataForMeta()
	snapshot.Segments = nil
	snapshot.SegmentIDs = nil
	snapshot.Indexes = nil
	snapshot.BuildIDs = nil
	_, metadataPath := snapshotstorage.GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	plan := &snapshotExportPlan{
		targetRoot:  targetRoot,
		metadataURI: metadataPath,
		mappings:    map[string]string{},
	}
	totalBytes, err := prepareSnapshotExportPlanWithSize(context.Background(), targetCM, snapshot, plan)
	require.NoError(t, err)
	return &datapb.ExportSnapshotJob{
		JobId:               jobID,
		SnapshotName:        snapshot.SnapshotInfo.GetName(),
		CollectionId:        snapshot.SnapshotInfo.GetCollectionId(),
		DbName:              "default",
		CollectionName:      snapshot.Collection.GetSchema().GetName(),
		TargetS3Path:        targetRoot,
		State:               datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
		Progress:            99,
		StartTime:           uint64(time.Now().Add(-time.Second).UnixMilli()),
		DeadlineTime:        uint64(time.Now().Add(-time.Millisecond).UnixMilli()),
		SnapshotMetadataUri: metadataPath,
		TotalBytes:          totalBytes,
	}
}

func TestSnapshotExportManager_ExecutesAndPublishesDurableResult(t *testing.T) {
	ctx := context.Background()
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	targetRoot := path.Join(t.TempDir(), "export-root")
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
	sourcePath := path.Join(sourceCM.RootPath(), "files/insert_log/100/1/1001/1/1")
	require.NoError(t, sourceCM.Write(ctx, sourcePath, []byte("binlog")))

	snapshot := createTestSnapshotDataForMeta()
	snapshot.SnapshotInfo.S3Location = path.Join(sourceCM.RootPath(), "snapshots/100/metadata/1.json")
	snapshot.SegmentIDs = []int64{1001}
	snapshot.Indexes = nil
	snapshot.Segments[0].Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: sourcePath}},
	}}
	clearSegmentNonInsertFiles(snapshot.Segments[0])

	copier := newSnapshotExporterCopierMock(t, func(copyCtx context.Context, _, src, _, dst string) error {
		data, err := sourceCM.Read(copyCtx, src)
		if err != nil {
			return err
		}
		return targetCM.Write(copyCtx, dst, data)
	})
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket:        "target-bucket",
		ForeignCM:            targetCM,
		ForeignStorageConfig: &indexpb.StorageConfig{},
		Copier:               copier,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
	defer mockRead.UnPatch()
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(100), "snapshot-1", 0, nil).Build()
	defer mockUnpin.UnPatch()

	job := &datapb.ExportSnapshotJob{
		JobId:          9001,
		SnapshotName:   "snapshot-1",
		CollectionId:   100,
		DbName:         "default",
		CollectionName: "collection-1",
		TargetS3Path:   targetRoot,
		ExternalSpec:   `{"extfs":{"access_key_id":"AK","access_key_value":"SK"}}`,
		State:          datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
		StartTime:      uint64(time.Now().UnixMilli()),
		DeadlineTime:   uint64(time.Now().Add(time.Minute).UnixMilli()),
		PinId:          7001,
	}
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(ctx, catalog)
	require.NoError(t, err)
	snapshotManager := &snapshotManager{snapshotMeta: &snapshotMeta{chunkManager: sourceCM}}
	manager := newSnapshotExportManager(ctx, meta, snapshotManager)
	manager.Start()
	defer manager.Close()

	completed := waitForSnapshotExportJobState(t, meta, job.GetJobId(), datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted)
	assert.Equal(t, int32(100), completed.GetProgress())
	assert.Equal(t, int64(1), completed.GetTotalFiles())
	assert.Equal(t, int64(1), completed.GetCopiedFiles())
	assert.Empty(t, completed.GetExternalSpec())
	assert.NotEmpty(t, completed.GetSnapshotMetadataUri())
	exists, err := targetCM.Exist(ctx, completed.GetSnapshotMetadataUri())
	require.NoError(t, err)
	assert.True(t, exists)
	manifestDir, metadataPath := snapshotstorage.GetSnapshotPaths(
		targetRoot,
		snapshot.SnapshotInfo.GetCollectionId(),
		snapshot.SnapshotInfo.GetId(),
	)
	exportedPaths := []string{
		path.Join(targetRoot, snapshotstorage.ExportedSnapshotFilesPath, "files/insert_log/100/1/1001/1/1"),
		snapshotstorage.GetSegmentManifestPath(manifestDir, snapshot.Segments[0].GetSegmentId()),
		metadataPath,
	}
	var expectedTotalBytes int64
	for _, exportedPath := range exportedPaths {
		size, err := targetCM.Size(ctx, exportedPath)
		require.NoError(t, err)
		expectedTotalBytes += size
	}
	assert.Equal(t, expectedTotalBytes, completed.GetTotalBytes())

	info, err := manager.GetJobInfo(job.GetJobId())
	require.NoError(t, err)
	assert.Equal(t, int32(100), info.GetProgress())
	assert.Equal(t, completed.GetSnapshotMetadataUri(), info.GetSnapshotMetadataUri())
	assert.Equal(t, completed.GetTotalBytes(), info.GetTotalBytes())
}

func TestSnapshotExportManager_ReplaysUncheckpointedBatchAfterRestart(t *testing.T) {
	copyConcurrencyKey := Params.DataCoordCfg.SnapshotExportCopyConcurrency.Key
	Params.Save(copyConcurrencyKey, "1")
	defer Params.Reset(copyConcurrencyKey)

	ctx := context.Background()
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	targetRoot := path.Join(t.TempDir(), "export-root")
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
	sourcePaths := []string{
		path.Join(sourceCM.RootPath(), "files/insert_log/100/1/1001/1/1"),
		path.Join(sourceCM.RootPath(), "files/insert_log/100/1/1001/1/2"),
	}
	for index, sourcePath := range sourcePaths {
		require.NoError(t, sourceCM.Write(ctx, sourcePath, []byte(fmt.Sprintf("binlog-%d", index))))
	}

	snapshot := createTestSnapshotDataForMeta()
	snapshot.SnapshotInfo.S3Location = path.Join(sourceCM.RootPath(), "snapshots/100/metadata/1.json")
	snapshot.SegmentIDs = []int64{1001}
	snapshot.Indexes = nil
	snapshot.Segments[0].Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{
			{LogID: 1, LogPath: sourcePaths[0]},
			{LogID: 2, LogPath: sourcePaths[1]},
		},
	}}
	clearSegmentNonInsertFiles(snapshot.Segments[0])

	firstCopyDone := make(chan struct{})
	var phase atomic.Int32
	var firstPhaseCalls atomic.Int32
	var recoveryCalls atomic.Int32
	copier := newSnapshotExporterCopierMock(t, func(copyCtx context.Context, _, src, _, dst string) error {
		if phase.Load() == 0 {
			if firstPhaseCalls.Add(1) == 1 {
				data, err := sourceCM.Read(copyCtx, src)
				if err != nil {
					return err
				}
				if err := targetCM.Write(copyCtx, dst, data); err != nil {
					return err
				}
				close(firstCopyDone)
				return nil
			}
			<-copyCtx.Done()
			return copyCtx.Err()
		}

		recoveryCalls.Add(1)
		data, err := sourceCM.Read(copyCtx, src)
		if err != nil {
			return err
		}
		return targetCM.Write(copyCtx, dst, data)
	})
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket:        "target-bucket",
		ForeignCM:            targetCM,
		ForeignStorageConfig: &indexpb.StorageConfig{},
		Copier:               copier,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
	defer mockRead.UnPatch()

	job := &datapb.ExportSnapshotJob{
		JobId:          9001,
		SnapshotName:   "snapshot-1",
		CollectionId:   100,
		DbName:         "default",
		CollectionName: "collection-1",
		TargetS3Path:   targetRoot,
		State:          datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
		StartTime:      uint64(time.Now().UnixMilli()),
		DeadlineTime:   uint64(time.Now().Add(time.Minute).UnixMilli()),
	}
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(ctx, catalog)
	require.NoError(t, err)
	snapshotManager := &snapshotManager{snapshotMeta: &snapshotMeta{chunkManager: sourceCM}}

	firstManager := newSnapshotExportManager(ctx, meta, snapshotManager)
	firstManager.Start()
	select {
	case <-firstCopyDone:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "first export copy did not complete")
	}
	firstManager.Close()

	interrupted, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting, interrupted.GetState())
	assert.Equal(t, int64(0), interrupted.GetCopyCursor())
	assert.Equal(t, int64(0), interrupted.GetCopiedFiles())
	assert.Equal(t, int32(5), interrupted.GetProgress())

	phase.Store(1)
	recoveredManager := newSnapshotExportManager(ctx, meta, snapshotManager)
	recoveredManager.Start()
	defer recoveredManager.Close()
	completed := waitForSnapshotExportJobState(
		t,
		meta,
		job.GetJobId(),
		datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
	)
	assert.Equal(t, int64(2), completed.GetTotalFiles())
	assert.Equal(t, int64(2), completed.GetCopiedFiles())
	assert.Equal(t, int32(100), completed.GetProgress())
	assert.Equal(t, int32(2), recoveryCalls.Load())
}

func TestSnapshotExportManager_PlanPersistenceAndMismatch(t *testing.T) {
	ctx := context.Background()
	job := &datapb.ExportSnapshotJob{
		JobId: 9001,
		State: datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting,
	}
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(ctx, catalog)
	require.NoError(t, err)
	manager := newSnapshotExportManager(ctx, meta, nil)
	plan := &snapshotExportPlan{
		version:             snapshotExportPlanVersion,
		fingerprint:         "plan-a",
		snapshotFingerprint: "snapshot-a",
		items: []snapshotExportPlanItem{
			{sourcePath: "source-a", destinationPath: "target-a", fileType: snapshotstorage.SnapshotFileTypeInsertBinlog},
		},
	}

	catalog.saveErr = errors.New("etcd unavailable")
	_, err = manager.persistOrValidatePlan(ctx, job.GetJobId(), plan)
	require.Error(t, err)
	assert.ErrorIs(t, err, errSnapshotExportJobPersistence)
	unchanged, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Empty(t, unchanged.GetPlanFingerprint())

	catalog.saveErr = nil
	persisted, err := manager.persistOrValidatePlan(ctx, job.GetJobId(), plan)
	require.NoError(t, err)
	assert.Equal(t, int32(5), persisted.GetProgress())
	assert.Equal(t, int64(1), persisted.GetTotalFiles())

	changedPlan := *plan
	changedPlan.fingerprint = "plan-b"
	_, err = manager.persistOrValidatePlan(ctx, job.GetJobId(), &changedPlan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan changed")
}

func TestSnapshotExportManager_GetJobInfoHidesUnpublishedMetadata(t *testing.T) {
	job := &datapb.ExportSnapshotJob{
		JobId:               9001,
		SnapshotName:        "snapshot-1",
		State:               datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
		Progress:            99,
		SnapshotMetadataUri: "s3://bucket/root/snapshots/100/metadata/1.json",
		TotalBytes:          128,
		StartTime:           uint64(time.Now().Add(-time.Second).UnixMilli()),
	}
	meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
	require.NoError(t, err)
	manager := newSnapshotExportManager(context.Background(), meta, nil)

	info, err := manager.GetJobInfo(job.GetJobId())
	require.NoError(t, err)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing, info.GetState())
	assert.Empty(t, info.GetSnapshotMetadataUri())
	assert.Zero(t, info.GetTotalBytes())
	assert.GreaterOrEqual(t, info.GetTimeCost(), uint64(1000))
}

func TestSnapshotExportManager_TimeoutDoesNotScheduleJob(t *testing.T) {
	job := &datapb.ExportSnapshotJob{
		JobId:        9001,
		ExternalSpec: `{"extfs":{"access_key_value":"secret"}}`,
		State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
		StartTime:    uint64(time.Now().Add(-time.Hour).UnixMilli()),
		DeadlineTime: uint64(time.Now().Add(-time.Second).UnixMilli()),
	}
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(context.Background(), catalog)
	require.NoError(t, err)
	manager := newSnapshotExportManager(context.Background(), meta, nil)

	manager.reconcile()

	failed, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobFailed, failed.GetState())
	assert.Equal(t, "snapshot export job timed out", failed.GetReason())
	assert.Empty(t, failed.GetExternalSpec())
	manager.runningMu.Lock()
	assert.Empty(t, manager.running)
	manager.runningMu.Unlock()
}

func TestSnapshotExportManager_DoesNotPublishAfterDeadline(t *testing.T) {
	job := &datapb.ExportSnapshotJob{
		JobId:        9001,
		State:        datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting,
		DeadlineTime: uint64(time.Now().Add(-time.Second).UnixMilli()),
	}
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(context.Background(), catalog)
	require.NoError(t, err)

	manager := newSnapshotExportManager(context.Background(), meta, nil)
	err = manager.executeJob(context.Background(), job.GetJobId())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, manager.failJob(job.GetJobId(), "snapshot export job timed out"))

	failed, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobFailed, failed.GetState())
}

func TestSnapshotExportManager_CompletesPublishingAfterOriginalDeadline(t *testing.T) {
	targetRoot := path.Join(t.TempDir(), "export-root")
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
	job := preparePublishingSnapshotExportJob(t, targetCM, targetRoot, 9001)
	meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
	require.NoError(t, err)
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "target-bucket",
		ForeignCM:     targetCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).To(
		func(*snapshotManager, context.Context, int64, string) (*snapshotstorage.SnapshotData, error) {
			require.FailNow(t, "Publishing must not read the source snapshot")
			return nil, nil
		}).Build()
	defer mockRead.UnPatch()
	manager := newSnapshotExportManager(context.Background(), meta, nil)

	manager.wg.Add(1)
	manager.runJob(context.Background(), job.GetJobId())

	completed, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted, completed.GetState())
	assert.Equal(t, int32(100), completed.GetProgress())
	assert.Equal(t, job.GetTotalBytes(), completed.GetTotalBytes())
	assert.Equal(t, job.GetSnapshotMetadataUri(), completed.GetSnapshotMetadataUri())
}

func TestSnapshotExportManager_ReplaysPublishingAfterCompletionPersistenceFailure(t *testing.T) {
	targetRoot := path.Join(t.TempDir(), "export-root")
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
	job := preparePublishingSnapshotExportJob(t, targetCM, targetRoot, 9001)
	job.ExternalSpec = `{"extfs":{"access_key_id":"AK","access_key_value":"SK"}}`
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(context.Background(), catalog)
	require.NoError(t, err)
	manager := newSnapshotExportManager(context.Background(), meta, nil)
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "target-bucket",
		ForeignCM:     targetCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).To(
		func(*snapshotManager, context.Context, int64, string) (*snapshotstorage.SnapshotData, error) {
			require.FailNow(t, "Publishing recovery must not read the source snapshot")
			return nil, nil
		}).Build()
	defer mockRead.UnPatch()
	catalog.saveErr = errors.New("catalog unavailable")
	manager.wg.Add(1)
	manager.runJob(context.Background(), job.GetJobId())

	publishing, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing, publishing.GetState())
	assert.Equal(t, int32(99), publishing.GetProgress())
	assert.Equal(t, job.GetSnapshotMetadataUri(), publishing.GetSnapshotMetadataUri())
	assert.Equal(t, job.GetTotalBytes(), publishing.GetTotalBytes())
	assert.NotEmpty(t, publishing.GetExternalSpec())
	finalData, err := targetCM.Read(context.Background(), job.GetSnapshotMetadataUri())
	require.NoError(t, err)
	assert.NotEmpty(t, finalData)
	catalog.mu.Lock()
	catalog.saveErr = nil
	catalog.mu.Unlock()
	manager.wg.Add(1)
	manager.runJob(context.Background(), job.GetJobId())

	completed, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted, completed.GetState())
	assert.Equal(t, int32(100), completed.GetProgress())
	assert.Equal(t, job.GetTotalBytes(), completed.GetTotalBytes())
	assert.Empty(t, completed.GetExternalSpec())
}

func TestSnapshotExportManager_PublishingStagingFailures(t *testing.T) {
	t.Run("missing staging fails the job", func(t *testing.T) {
		targetRoot := path.Join(t.TempDir(), "export-root")
		targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
		job := preparePublishingSnapshotExportJob(t, targetCM, targetRoot, 9001)
		require.NoError(t, targetCM.Remove(
			context.Background(),
			snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot),
		))
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "target-bucket",
			ForeignCM:     targetCM,
		}, nil).Build()
		defer mockResolve.UnPatch()

		manager.wg.Add(1)
		manager.runJob(context.Background(), job.GetJobId())

		failed, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobFailed, failed.GetState())
		assert.Contains(t, failed.GetReason(), "staged snapshot metadata object is missing")
	})

	t.Run("transient staging read remains publishing", func(t *testing.T) {
		targetRoot := path.Join(t.TempDir(), "export-root")
		targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
		job := preparePublishingSnapshotExportJob(t, targetCM, targetRoot, 9002)
		stagingPath := snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot)
		var origin func(*storage.LocalChunkManager, context.Context, string) ([]byte, error)
		mockRead := mockey.Mock((*storage.LocalChunkManager).Read).To(
			func(manager *storage.LocalChunkManager, readCtx context.Context, objectPath string) ([]byte, error) {
				if objectPath == stagingPath {
					return nil, merr.WrapErrIoTooManyRequests(objectPath, errors.New("object store throttled"))
				}
				return origin(manager, readCtx, objectPath)
			}).Origin(&origin).Build()
		defer mockRead.UnPatch()
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "target-bucket",
			ForeignCM:     targetCM,
		}, nil).Build()
		defer mockResolve.UnPatch()

		manager.wg.Add(1)
		manager.runJob(context.Background(), job.GetJobId())

		current, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing, current.GetState())
		assert.Empty(t, current.GetReason())
	})

	t.Run("corrupt staging fails the job", func(t *testing.T) {
		targetRoot := path.Join(t.TempDir(), "export-root")
		targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
		job := preparePublishingSnapshotExportJob(t, targetCM, targetRoot, 9003)
		require.NoError(t, targetCM.Write(
			context.Background(),
			snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot),
			[]byte("{"),
		))
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "target-bucket",
			ForeignCM:     targetCM,
		}, nil).Build()
		defer mockResolve.UnPatch()

		manager.wg.Add(1)
		manager.runJob(context.Background(), job.GetJobId())

		failed, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobFailed, failed.GetState())
		assert.Contains(t, failed.GetReason(), "invalid staged snapshot metadata")
	})
}

func TestSnapshotExportManager_TimeoutStopsLateCopyCheckpoint(t *testing.T) {
	ctx := context.Background()
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	targetRoot := path.Join(t.TempDir(), "export-root")
	targetCM := storage.NewLocalChunkManager(objectstorage.RootPath(path.Dir(targetRoot)))
	sourcePath := path.Join(sourceCM.RootPath(), "files/insert_log/100/1/1001/1/1")
	require.NoError(t, sourceCM.Write(ctx, sourcePath, []byte("binlog")))

	snapshot := createTestSnapshotDataForMeta()
	snapshot.SnapshotInfo.S3Location = path.Join(sourceCM.RootPath(), "snapshots/100/metadata/1.json")
	snapshot.SegmentIDs = []int64{1001}
	snapshot.Indexes = nil
	snapshot.Segments[0].Binlogs = []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: sourcePath}},
	}}
	clearSegmentNonInsertFiles(snapshot.Segments[0])

	copyStarted := make(chan struct{})
	releaseCopy := make(chan struct{})
	copier := newSnapshotExporterCopierMock(t, func(_ context.Context, _, src, _, dst string) error {
		close(copyStarted)
		<-releaseCopy
		data, err := sourceCM.Read(context.Background(), src)
		if err != nil {
			return err
		}
		return targetCM.Write(context.Background(), dst, data)
	})
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket:        "target-bucket",
		ForeignCM:            targetCM,
		ForeignStorageConfig: &indexpb.StorageConfig{},
		Copier:               copier,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
	defer mockRead.UnPatch()

	job := &datapb.ExportSnapshotJob{
		JobId:          9001,
		SnapshotName:   "snapshot-1",
		CollectionId:   100,
		DbName:         "default",
		CollectionName: "collection-1",
		TargetS3Path:   targetRoot,
		State:          datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
		StartTime:      uint64(time.Now().UnixMilli()),
		DeadlineTime:   uint64(time.Now().Add(100 * time.Millisecond).UnixMilli()),
	}
	meta, err := newSnapshotExportMeta(ctx, newSnapshotExportCatalogFake(job))
	require.NoError(t, err)
	manager := newSnapshotExportManager(
		ctx,
		meta,
		&snapshotManager{snapshotMeta: &snapshotMeta{chunkManager: sourceCM}},
	)
	manager.Start()
	defer manager.Close()
	releaseBlockedCopy := func() {
		select {
		case <-releaseCopy:
		default:
			close(releaseCopy)
		}
	}
	defer releaseBlockedCopy()

	select {
	case <-copyStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "snapshot export copy did not start")
	}
	failed := waitForSnapshotExportJobState(
		t,
		meta,
		job.GetJobId(),
		datapb.ExportSnapshotJobState_ExportSnapshotJobFailed,
	)
	assert.Equal(t, int64(1), failed.GetTotalFiles())
	assert.Zero(t, failed.GetCopyCursor())
	assert.Zero(t, failed.GetCopiedFiles())
	assert.Equal(t, int32(5), failed.GetProgress())
	assert.Empty(t, failed.GetSnapshotMetadataUri())

	releaseBlockedCopy()
	manager.Close()
	latest, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobFailed, latest.GetState())
	assert.Zero(t, latest.GetCopyCursor())
	_, metadataPath := snapshotstorage.GetSnapshotPaths(targetRoot, 100, 1)
	metadataExists, err := targetCM.Exist(ctx, metadataPath)
	require.NoError(t, err)
	assert.False(t, metadataExists)
}

func TestSnapshotExportManager_PersistenceFailureWaitsForReconcileTick(t *testing.T) {
	job := &datapb.ExportSnapshotJob{
		JobId:        9001,
		State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
		StartTime:    uint64(time.Now().UnixMilli()),
		DeadlineTime: uint64(time.Now().Add(time.Minute).UnixMilli()),
	}
	meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
	require.NoError(t, err)

	var attempts atomic.Int32
	firstAttempt := make(chan struct{})
	mockExecute := mockey.Mock((*snapshotExportManager).executeJob).To(
		func(*snapshotExportManager, context.Context, int64) error {
			if attempts.Add(1) == 1 {
				close(firstAttempt)
			}
			return errSnapshotExportJobPersistence
		}).Build()
	defer mockExecute.UnPatch()

	manager := newSnapshotExportManager(context.Background(), meta, nil)
	manager.Start()
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		require.FailNow(t, "snapshot export worker did not start")
	}
	time.Sleep(100 * time.Millisecond)
	manager.Close()

	assert.Equal(t, int32(1), attempts.Load())
	current, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting, current.GetState())
}

func TestSnapshotExportManager_ConcurrencyAndShutdownRecovery(t *testing.T) {
	key := Params.DataCoordCfg.SnapshotExportMaxConcurrentJobs.Key
	Params.Save(key, "1")
	defer Params.Reset(key)
	now := time.Now()
	catalog := newSnapshotExportCatalogFake(
		&datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			StartTime:    uint64(now.UnixMilli()),
			DeadlineTime: uint64(now.Add(time.Minute).UnixMilli()),
		},
		&datapb.ExportSnapshotJob{
			JobId:        9002,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			StartTime:    uint64(now.Add(time.Millisecond).UnixMilli()),
			DeadlineTime: uint64(now.Add(time.Minute).UnixMilli()),
		},
	)
	meta, err := newSnapshotExportMeta(context.Background(), catalog)
	require.NoError(t, err)
	started := make(chan int64, 2)
	mockExecute := mockey.Mock((*snapshotExportManager).executeJob).To(
		func(_ *snapshotExportManager, workerCtx context.Context, jobID int64) error {
			started <- jobID
			<-workerCtx.Done()
			return workerCtx.Err()
		}).Build()
	defer mockExecute.UnPatch()
	manager := newSnapshotExportManager(context.Background(), meta, nil)
	manager.Start()

	select {
	case jobID := <-started:
		assert.Equal(t, int64(9001), jobID)
	case <-time.After(time.Second):
		require.FailNow(t, "snapshot export worker did not start")
	}
	select {
	case <-started:
		require.FailNow(t, "job concurrency limit was exceeded")
	case <-time.After(100 * time.Millisecond):
	}

	manager.Close()
	first, ok := meta.GetJob(9001)
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting, first.GetState())
	second, ok := meta.GetJob(9002)
	require.True(t, ok)
	assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPending, second.GetState())
}

func TestSnapshotExportManager_TerminalCleanupAndRetention(t *testing.T) {
	key := Params.DataCoordCfg.SnapshotExportJobRetention.Key
	Params.Save(key, "0")
	defer Params.Reset(key)
	job := &datapb.ExportSnapshotJob{
		JobId:     9001,
		State:     datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
		StartTime: uint64(time.Now().Add(-time.Minute).UnixMilli()),
		EndTime:   uint64(time.Now().Add(-time.Second).UnixMilli()),
		PinId:     7001,
	}
	catalog := newSnapshotExportCatalogFake(job)
	meta, err := newSnapshotExportMeta(context.Background(), catalog)
	require.NoError(t, err)
	var attempts atomic.Int32
	mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).To(
		func(*snapshotMeta, context.Context, int64) (int64, string, int, error) {
			if attempts.Add(1) == 1 {
				return 100, "snapshot-1", 0, errors.New("etcd unavailable")
			}
			return 100, "snapshot-1", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()
	manager := newSnapshotExportManager(
		context.Background(),
		meta,
		&snapshotManager{snapshotMeta: &snapshotMeta{}},
	)

	manager.cleanupTerminalJob(job, uint64(time.Now().UnixMilli()))
	stillPinned, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Equal(t, int64(7001), stillPinned.GetPinId())

	manager.cleanupTerminalJob(stillPinned, uint64(time.Now().UnixMilli()))
	cleaned, ok := meta.GetJob(job.GetJobId())
	require.True(t, ok)
	assert.Zero(t, cleaned.GetPinId())

	manager.cleanupTerminalJob(cleaned, uint64(time.Now().UnixMilli()))
	_, ok = meta.GetJob(job.GetJobId())
	assert.False(t, ok)
}

func TestSnapshotExportManager_Observability(t *testing.T) {
	t.Run("metrics follow lifecycle transitions", func(t *testing.T) {
		activeBefore := testutil.ToFloat64(metrics.DataCoordSnapshotExportActiveJobs)
		completedCounter := metrics.DataCoordSnapshotExportTerminalJobs.WithLabelValues("completed")
		failedCounter := metrics.DataCoordSnapshotExportTerminalJobs.WithLabelValues("failed")
		completedBefore := testutil.ToFloat64(completedCounter)
		failedBefore := testutil.ToFloat64(failedCounter)

		now := time.Now()
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			StartTime:    uint64(now.UnixMilli()),
			DeadlineTime: uint64(now.Add(time.Minute).UnixMilli()),
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		started := make(chan struct{})
		mockExecute := mockey.Mock((*snapshotExportManager).executeJob).To(
			func(_ *snapshotExportManager, ctx context.Context, _ int64) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}).Build()
		defer mockExecute.UnPatch()

		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.Start()
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "snapshot export worker did not start")
		}
		assert.Equal(t, activeBefore+1, testutil.ToFloat64(metrics.DataCoordSnapshotExportActiveJobs))
		manager.Close()
		assert.Equal(t, activeBefore, testutil.ToFloat64(metrics.DataCoordSnapshotExportActiveJobs))

		observeSnapshotExportTerminal(&datapb.ExportSnapshotJob{
			State:     datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
			StartTime: 100,
			EndTime:   200,
		})
		observeSnapshotExportTerminal(&datapb.ExportSnapshotJob{
			State:     datapb.ExportSnapshotJobState_ExportSnapshotJobFailed,
			StartTime: 100,
			EndTime:   200,
		})
		assert.Equal(t, completedBefore+1, testutil.ToFloat64(completedCounter))
		assert.Equal(t, failedBefore+1, testutil.ToFloat64(failedCounter))
	})

	t.Run("worker span derives from lifecycle context", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithSyncer(exporter),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)
		previousProvider := otel.GetTracerProvider()
		otel.SetTracerProvider(provider)
		defer func() {
			otel.SetTracerProvider(previousProvider)
			require.NoError(t, provider.Shutdown(context.Background()))
		}()

		lifecycleCtx, lifecycleSpan := otel.Tracer("snapshot-export-test").Start(
			context.Background(),
			"snapshot-export-lifecycle",
		)
		parentSpanID := lifecycleSpan.SpanContext().SpanID()
		job := &datapb.ExportSnapshotJob{
			JobId: 9002,
			State: datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
		}
		meta, err := newSnapshotExportMeta(lifecycleCtx, newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(lifecycleCtx, meta, nil)
		manager.wg.Add(1)
		manager.runJob(manager.ctx, job.GetJobId())
		lifecycleSpan.End()

		var workerSpan tracetest.SpanStub
		for _, span := range exporter.GetSpans() {
			if span.Name == "DataCoord-ExportSnapshotJob" {
				workerSpan = span
				break
			}
		}
		require.Equal(t, "DataCoord-ExportSnapshotJob", workerSpan.Name)
		assert.Equal(t, parentSpanID, workerSpan.Parent.SpanID())
		var observedJobID int64
		for _, attr := range workerSpan.Attributes {
			if string(attr.Key) == "jobID" {
				observedJobID = attr.Value.AsInt64()
			}
		}
		assert.Equal(t, job.GetJobId(), observedJobID)
	})

	t.Run("failure log uses context and redacts reason", func(t *testing.T) {
		var logs syncBuffer
		oldLogger := mlog.L()
		oldLevel := mlog.GetAtomicLevel()
		logger, props, err := mlog.InitLoggerWithWriteSyncer(&mlog.Config{
			Level:             "warn",
			Format:            "text",
			DisableCaller:     true,
			DisableTimestamp:  true,
			DisableStacktrace: true,
		}, &logs)
		require.NoError(t, err)
		mlog.ReplaceGlobals(logger, props)
		defer mlog.ReplaceGlobals(oldLogger, &mlog.ZapProperties{Level: oldLevel})

		const secret = "SNAPSHOT_EXPORT_SECRET"
		externalSpec := `{"extfs":{"access_key_value":"` + secret + `"}}`
		job := &datapb.ExportSnapshotJob{
			JobId:        9003,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			ExternalSpec: externalSpec,
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		reason := sanitizeSnapshotExportReason(errors.New("copy failed with "+secret), externalSpec)

		require.True(t, manager.failJob(job.GetJobId(), reason))
		output := logs.String()
		assert.Contains(t, output, "snapshot export job failed")
		assert.Contains(t, output, "<redacted>")
		assert.NotContains(t, output, secret)
		assert.NotContains(t, output, externalSpec)
		assert.NotContains(t, output, "_ctx_nil")
	})
}

func TestSanitizeSnapshotExportReason(t *testing.T) {
	secret := "SUPERSECRET"
	sas := "sv=2024-08-04&sig=SECRETSAS&sp=r"
	externalSpec := `{"extfs":{"access_key_id":"AKIAEXAMPLE","access_key_value":"` + secret + `","source_sas_token":"` + sas + `"}}`
	err := errors.New("copy failed for AKIAEXAMPLE using " + secret +
		" from https://src.blob.core.windows.net/c/o?" + sas + strings.Repeat("x", snapshotExportFailureReasonLimit))

	reason := sanitizeSnapshotExportReason(err, externalSpec)

	assert.NotContains(t, reason, "AKIAEXAMPLE")
	assert.NotContains(t, reason, secret)
	assert.NotContains(t, reason, "SECRETSAS")
	assert.LessOrEqual(t, len(reason), snapshotExportFailureReasonLimit)

	truncated := sanitizeSnapshotExportReason(
		errors.New(strings.Repeat("x", snapshotExportFailureReasonLimit+1)),
		"",
	)
	assert.Len(t, truncated, snapshotExportFailureReasonLimit)
}

func TestSnapshotExportManager_SubmitFailurePaths(t *testing.T) {
	t.Run("missing target path", func(t *testing.T) {
		manager := newSnapshotExportManager(context.Background(), nil, nil)
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "", "")
		require.Error(t, err)
		assert.Zero(t, jobID)
	})

	t.Run("storage validation fails", func(t *testing.T) {
		expected := errors.New("invalid target")
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(expected).Build()
		defer mockValidate.UnPatch()
		manager := newSnapshotExportManager(context.Background(), nil, nil)
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "target", "")
		require.ErrorIs(t, err, expected)
		assert.Zero(t, jobID)
	})

	t.Run("snapshot lookup fails", func(t *testing.T) {
		expected := errors.New("snapshot missing")
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return((*datapb.SnapshotInfo)(nil), expected).Build()
		defer mockGetSnapshot.UnPatch()
		manager := newSnapshotExportManager(context.Background(), nil, &snapshotManager{snapshotMeta: &snapshotMeta{}})
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "target", "")
		require.ErrorIs(t, err, expected)
		assert.Zero(t, jobID)
	})

	t.Run("job allocation fails", func(t *testing.T) {
		expected := errors.New("allocator unavailable")
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(&datapb.SnapshotInfo{Id: 1}, nil).Build()
		defer mockGetSnapshot.UnPatch()
		allocatorTarget := &restoreAllocatorTarget{}
		mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(0), expected).Build()
		defer mockAlloc.UnPatch()
		manager := newSnapshotExportManager(context.Background(), nil, &snapshotManager{
			snapshotMeta: &snapshotMeta{},
			allocator:    allocatorTarget,
		})
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "target", "")
		require.ErrorIs(t, err, expected)
		assert.Zero(t, jobID)
	})

	t.Run("source pin fails", func(t *testing.T) {
		expected := errors.New("pin unavailable")
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(&datapb.SnapshotInfo{Id: 1}, nil).Build()
		defer mockGetSnapshot.UnPatch()
		allocatorTarget := &restoreAllocatorTarget{}
		mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
		defer mockAlloc.UnPatch()
		mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(0), 0, expected).Build()
		defer mockPin.UnPatch()
		manager := newSnapshotExportManager(context.Background(), nil, &snapshotManager{
			snapshotMeta: &snapshotMeta{},
			allocator:    allocatorTarget,
		})
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "target", "")
		require.ErrorIs(t, err, expected)
		assert.Zero(t, jobID)
	})

	t.Run("persistence failure reports cleanup failure", func(t *testing.T) {
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(&datapb.SnapshotInfo{Id: 1}, nil).Build()
		defer mockGetSnapshot.UnPatch()
		allocatorTarget := &restoreAllocatorTarget{}
		mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
		defer mockAlloc.UnPatch()
		mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).Return(int64(7001), 1, nil).Build()
		defer mockPin.UnPatch()
		cleanupErr := errors.New("unpin unavailable")
		mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, cleanupErr).Build()
		defer mockUnpin.UnPatch()
		catalog := newSnapshotExportCatalogFake()
		catalog.saveErr = errors.New("catalog unavailable")
		meta, err := newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, &snapshotManager{
			snapshotMeta: &snapshotMeta{},
			allocator:    allocatorTarget,
		})
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "target", "")
		require.Error(t, err)
		assert.Zero(t, jobID)
	})

	t.Run("export deadline extends source pin ttl", func(t *testing.T) {
		restoreTTLKey := Params.DataCoordCfg.SnapshotRestorePinTTLSeconds.Key
		timeoutKey := Params.DataCoordCfg.SnapshotExportJobTimeout.Key
		Params.Save(restoreTTLKey, "300")
		defer Params.Reset(restoreTTLKey)
		Params.Save(timeoutKey, "2")
		defer Params.Reset(timeoutKey)
		mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
		defer mockValidate.UnPatch()
		mockGetSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(&datapb.SnapshotInfo{Id: 1}, nil).Build()
		defer mockGetSnapshot.UnPatch()
		allocatorTarget := &restoreAllocatorTarget{}
		mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
		defer mockAlloc.UnPatch()
		var pinTTL int64
		mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).To(
			func(_ *snapshotMeta, _ context.Context, _ int64, _ string, ttl int64) (int64, int, error) {
				pinTTL = ttl
				return 7001, 1, nil
			}).Build()
		defer mockPin.UnPatch()
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake())
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, &snapshotManager{
			snapshotMeta: &snapshotMeta{},
			allocator:    allocatorTarget,
		})
		jobID, err := manager.Submit(context.Background(), 100, "snapshot-1", "default", "collection-1", "target", "")
		require.NoError(t, err)
		assert.Equal(t, int64(9001), jobID)
		assert.Equal(t, int64(302), pinTTL)
	})
}

func TestSnapshotExportManager_GetJobInfoVariants(t *testing.T) {
	completed := &datapb.ExportSnapshotJob{
		JobId:               9001,
		State:               datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
		StartTime:           200,
		EndTime:             100,
		SnapshotMetadataUri: "s3://bucket/root/snapshots/100/metadata/1.json",
	}
	failed := &datapb.ExportSnapshotJob{
		JobId:               9002,
		State:               datapb.ExportSnapshotJobState_ExportSnapshotJobFailed,
		StartTime:           uint64(time.Now().Add(-time.Second).UnixMilli()),
		SnapshotMetadataUri: "must-not-be-visible",
	}
	meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(completed, failed))
	require.NoError(t, err)
	manager := newSnapshotExportManager(context.Background(), meta, nil)

	completedInfo, err := manager.GetJobInfo(completed.GetJobId())
	require.NoError(t, err)
	assert.Zero(t, completedInfo.GetTimeCost())
	assert.Equal(t, completed.GetSnapshotMetadataUri(), completedInfo.GetSnapshotMetadataUri())

	failedInfo, err := manager.GetJobInfo(failed.GetJobId())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, failedInfo.GetTimeCost(), uint64(1000))
	assert.Empty(t, failedInfo.GetSnapshotMetadataUri())

	_, err = manager.GetJobInfo(9999)
	require.Error(t, err)
}

func TestSnapshotExportManager_RunJobFailurePaths(t *testing.T) {
	t.Run("execution failure becomes sanitized terminal job", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			ExternalSpec: `{"extfs":{"access_key_value":"SECRET"}}`,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			DeadlineTime: uint64(time.Now().Add(time.Minute).UnixMilli()),
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		mockExecute := mockey.Mock((*snapshotExportManager).executeJob).Return(errors.New("copy failed with SECRET")).Build()
		defer mockExecute.UnPatch()
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.wg.Add(1)
		manager.runJob(context.Background(), job.GetJobId())

		failed, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobFailed, failed.GetState())
		assert.NotContains(t, failed.GetReason(), "SECRET")
		assert.Empty(t, failed.GetExternalSpec())
	})

	t.Run("transition persistence failure leaves job pending", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			DeadlineTime: uint64(time.Now().Add(time.Minute).UnixMilli()),
		}
		catalog := newSnapshotExportCatalogFake(job)
		meta, err := newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		catalog.saveErr = errors.New("catalog unavailable")
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.wg.Add(1)
		manager.runJob(context.Background(), job.GetJobId())

		pending, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPending, pending.GetState())
	})

	t.Run("terminal job is not executed again", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{JobId: 9001, State: datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.wg.Add(1)
		manager.runJob(context.Background(), job.GetJobId())
	})

	t.Run("canceled worker does not mutate pending job", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{JobId: 9001, State: datapb.ExportSnapshotJobState_ExportSnapshotJobPending}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		workerCtx, cancel := context.WithCancel(context.Background())
		cancel()
		manager.wg.Add(1)
		manager.runJob(workerCtx, job.GetJobId())
		pending, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPending, pending.GetState())
	})
}

func TestSnapshotExportManager_HelperBranches(t *testing.T) {
	assert.Equal(t, int32(5), snapshotExportCopyProgress(0, 0))
	assert.Equal(t, int32(95), snapshotExportCopyProgress(2, 1))

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, snapshotExportAdvanceError(canceledCtx, &datapb.ExportSnapshotJob{}), context.Canceled)
	assert.ErrorIs(t, ensureSnapshotExportCanAdvance(context.Background(), &datapb.ExportSnapshotJob{}), errSnapshotExportJobStopped)
	assert.ErrorIs(t, ensureSnapshotExportCanPublish(context.Background(), &datapb.ExportSnapshotJob{}), errSnapshotExportJobStopped)
	assert.NoError(t, ensureSnapshotExportCanPublish(context.Background(), &datapb.ExportSnapshotJob{
		State: datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
	}))
	assert.ErrorIs(t, ensureSnapshotExportCanPublish(canceledCtx, &datapb.ExportSnapshotJob{
		State: datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
	}), context.Canceled)

	meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake())
	require.NoError(t, err)
	manager := newSnapshotExportManager(context.Background(), meta, nil)
	lockCtx, cancelLock := context.WithCancel(context.Background())
	cancelLock()
	_, err = manager.lockTarget(lockCtx, snapshotExportTarget{bucket: "bucket", root: "root"})
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, manager.targetLocks)

	target := snapshotExportTarget{bucket: "bucket", root: "root"}
	current := &snapshotExportTargetLock{semaphore: make(chan struct{}, 1), refs: 1}
	manager.targetLocks[target] = current
	manager.releaseTargetLockRef(target, &snapshotExportTargetLock{})
	assert.Same(t, current, manager.targetLocks[target])

	noDeadlineCtx, noDeadlineCancel := manager.withJobDeadline(context.Background(), 9999)
	defer noDeadlineCancel()
	_, hasDeadline := noDeadlineCtx.Deadline()
	assert.False(t, hasDeadline)

	assert.Equal(t, "", sanitizeSnapshotExportReason(nil, ""))
	assert.Nil(t, snapshotExportSecretValues(""))
	assert.Equal(t, []string{"{"}, snapshotExportSecretValues("{"))
	observeSnapshotExportTerminal(nil)
	observeSnapshotExportTerminal(&datapb.ExportSnapshotJob{State: datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting})

	timedOut := &datapb.ExportSnapshotJob{DeadlineTime: uint64(time.Now().Add(-time.Second).UnixMilli())}
	assert.Equal(t, "snapshot export job timed out", manager.snapshotExportFailureReason(timedOut, errors.New("other"), ""))
	publishing := &datapb.ExportSnapshotJob{
		State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
		DeadlineTime: timedOut.GetDeadlineTime(),
	}
	assert.Equal(t, "other", manager.snapshotExportFailureReason(publishing, errors.New("other"), ""))
}

func TestSnapshotExportManager_ReconcileAndTargetLockBranches(t *testing.T) {
	t.Run("wake triggers reconciliation", func(t *testing.T) {
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake())
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.Wake()
		manager.Wake()
		manager.Start()
		manager.Wake()
		time.Sleep(20 * time.Millisecond)
		manager.Close()
	})

	t.Run("terminal jobs are reconciled", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
			ExternalSpec: `{"extfs":{"access_key_value":"SECRET"}}`,
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)

		manager.reconcile()

		updated, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Empty(t, updated.GetExternalSpec())
	})

	t.Run("expired locked job is not scheduled", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPending,
			DeadlineTime: uint64(time.Now().Add(-time.Second).UnixMilli()),
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		meta.locks.Lock(job.GetJobId())
		manager.reconcile()
		meta.locks.Unlock(job.GetJobId())
		assert.Empty(t, manager.running)
	})

	t.Run("publishing job remains schedulable after deadline", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
			DeadlineTime: uint64(time.Now().Add(-time.Second).UnixMilli()),
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		started := make(chan struct{})
		mockExecute := mockey.Mock((*snapshotExportManager).executeJob).To(
			func(*snapshotExportManager, context.Context, int64) error {
				close(started)
				return errSnapshotExportJobPersistence
			}).Build()
		defer mockExecute.UnPatch()
		manager := newSnapshotExportManager(context.Background(), meta, nil)

		manager.reconcile()
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "publishing snapshot export job was not scheduled")
		}
		manager.Close()
		current, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing, current.GetState())
	})

	t.Run("waiting target lock observes cancellation", func(t *testing.T) {
		manager := newSnapshotExportManager(context.Background(), nil, nil)
		target := snapshotExportTarget{bucket: "bucket", root: "root"}
		unlock, err := manager.lockTarget(context.Background(), target)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, lockErr := manager.lockTarget(ctx, target)
			result <- lockErr
		}()
		time.Sleep(20 * time.Millisecond)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		unlock()
		assert.Empty(t, manager.targetLocks)
	})
}

func TestSnapshotExportManager_ExecuteJobErrorBranches(t *testing.T) {
	newManager := func(t *testing.T, job *datapb.ExportSnapshotJob) (*snapshotExportManager, *snapshotExportMeta, *snapshotExportCatalogFake) {
		t.Helper()
		catalog := newSnapshotExportCatalogFake(job)
		meta, err := newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, &snapshotManager{snapshotMeta: &snapshotMeta{}})
		return manager, meta, catalog
	}
	newJob := func() *datapb.ExportSnapshotJob {
		return &datapb.ExportSnapshotJob{
			JobId:        9001,
			CollectionId: 100,
			SnapshotName: "snapshot-1",
			TargetS3Path: "s3://bucket/export-root",
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting,
			DeadlineTime: uint64(time.Now().Add(time.Minute).UnixMilli()),
		}
	}
	resolved := &snapshotstorage.ResolvedForeignStorage{ForeignBucket: "bucket"}
	snapshot := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot-1"},
	}
	newPlan := func(items ...snapshotExportPlanItem) *snapshotExportPlan {
		return &snapshotExportPlan{
			version:             snapshotExportPlanVersion,
			fingerprint:         "plan-fingerprint",
			snapshotFingerprint: "snapshot-fingerprint",
			metadataURI:         "s3://bucket/export-root/snapshots/100/metadata/1.json",
			items:               items,
		}
	}

	t.Run("missing job", func(t *testing.T) {
		manager, _, _ := newManager(t, nil)
		err := manager.executeJob(context.Background(), 9001)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("non-executing job", func(t *testing.T) {
		job := newJob()
		job.State = datapb.ExportSnapshotJobState_ExportSnapshotJobPending
		manager, _, _ := newManager(t, job)
		require.ErrorIs(t, manager.executeJob(context.Background(), job.GetJobId()), errSnapshotExportJobStopped)
	})

	t.Run("storage resolution failure", func(t *testing.T) {
		expected := errors.New("storage unavailable")
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return((*snapshotstorage.ResolvedForeignStorage)(nil), expected).Build()
		defer mockResolve.UnPatch()
		job := newJob()
		manager, _, _ := newManager(t, job)
		require.ErrorIs(t, manager.executeJob(context.Background(), job.GetJobId()), expected)
	})

	t.Run("snapshot read failure", func(t *testing.T) {
		expected := errors.New("snapshot unavailable")
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return((*snapshotstorage.SnapshotData)(nil), expected).Build()
		defer mockRead.UnPatch()
		job := newJob()
		manager, _, _ := newManager(t, job)
		require.ErrorIs(t, manager.executeJob(context.Background(), job.GetJobId()), expected)
	})

	t.Run("plan build failure", func(t *testing.T) {
		expected := errors.New("plan unavailable")
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
		defer mockRead.UnPatch()
		mockBuild := mockey.Mock(buildSnapshotExportPlan).Return((*snapshotExportPlan)(nil), expected).Build()
		defer mockBuild.UnPatch()
		job := newJob()
		manager, _, _ := newManager(t, job)
		require.ErrorIs(t, manager.executeJob(context.Background(), job.GetJobId()), expected)
	})

	t.Run("copy cursor conflict", func(t *testing.T) {
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
		defer mockRead.UnPatch()
		plan := newPlan(snapshotExportPlanItem{sourcePath: "source", destinationPath: "target"})
		mockBuild := mockey.Mock(buildSnapshotExportPlan).Return(plan, nil).Build()
		defer mockBuild.UnPatch()
		job := newJob()
		manager, meta, _ := newManager(t, job)
		mockCopy := mockey.Mock(copySnapshotExportPlan).To(
			func(context.Context, storage.CrossBucketCopier, string, string, []snapshotExportPlanItem, int) error {
				_, _, err := meta.UpdateJob(context.Background(), job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
					latest.CopyCursor = 1
					latest.CopiedFiles = 1
					return false, nil
				})
				return err
			}).Build()
		defer mockCopy.UnPatch()

		err := manager.executeJob(context.Background(), job.GetJobId())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "copy cursor changed")
	})

	t.Run("terminal transition blocks checkpoint", func(t *testing.T) {
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
		defer mockRead.UnPatch()
		plan := newPlan(snapshotExportPlanItem{sourcePath: "source", destinationPath: "target"})
		mockBuild := mockey.Mock(buildSnapshotExportPlan).Return(plan, nil).Build()
		defer mockBuild.UnPatch()
		job := newJob()
		manager, meta, _ := newManager(t, job)
		mockCopy := mockey.Mock(copySnapshotExportPlan).To(
			func(context.Context, storage.CrossBucketCopier, string, string, []snapshotExportPlanItem, int) error {
				_, _, err := meta.UpdateJob(context.Background(), job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
					latest.State = datapb.ExportSnapshotJobState_ExportSnapshotJobFailed
					return false, nil
				})
				return err
			}).Build()
		defer mockCopy.UnPatch()

		require.ErrorIs(t, manager.executeJob(context.Background(), job.GetJobId()), errSnapshotExportJobStopped)
	})

	t.Run("finalization progress persistence failure", func(t *testing.T) {
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
		defer mockRead.UnPatch()
		mockBuild := mockey.Mock(buildSnapshotExportPlan).Return(newPlan(), nil).Build()
		defer mockBuild.UnPatch()
		mockPrepare := mockey.Mock(prepareSnapshotExportPlanWithSize).Return(int64(128), nil).Build()
		defer mockPrepare.UnPatch()
		job := newJob()
		manager, _, catalog := newManager(t, job)
		expected := errors.New("catalog unavailable")
		catalog.beforeSave = func(saved *datapb.ExportSnapshotJob) {
			if saved.GetPlanFingerprint() != "" && saved.GetProgress() == 5 {
				catalog.mu.Lock()
				catalog.saveErr = expected
				catalog.mu.Unlock()
			}
		}

		err := manager.executeJob(context.Background(), job.GetJobId())
		require.Error(t, err)
		require.ErrorIs(t, err, errSnapshotExportJobPersistence)
	})

	t.Run("metadata URI change blocks completion", func(t *testing.T) {
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
		defer mockRead.UnPatch()
		plan := newPlan()
		mockBuild := mockey.Mock(buildSnapshotExportPlan).Return(plan, nil).Build()
		defer mockBuild.UnPatch()
		job := newJob()
		manager, meta, _ := newManager(t, job)
		mockPrepare := mockey.Mock(prepareSnapshotExportPlanWithSize).Return(int64(128), nil).Build()
		defer mockPrepare.UnPatch()
		mockCommit := mockey.Mock(commitSnapshotExportMetadata).To(
			func(context.Context, storage.ChunkManager, string, string) error {
				_, _, err := meta.UpdateJob(context.Background(), job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
					latest.SnapshotMetadataUri = "other-metadata"
					return false, nil
				})
				return err
			}).Build()
		defer mockCommit.UnPatch()

		err := manager.executeJob(context.Background(), job.GetJobId())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "metadata URI changed")
	})

	t.Run("terminal transition blocks metadata publication", func(t *testing.T) {
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(resolved, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotManager).ReadSnapshotData).Return(snapshot, nil).Build()
		defer mockRead.UnPatch()
		job := newJob()
		manager, meta, _ := newManager(t, job)
		mockBuild := mockey.Mock(buildSnapshotExportPlan).To(
			func(
				context.Context,
				storage.ChunkManager,
				storage.ChunkManager,
				string,
				string,
				*snapshotstorage.SnapshotData,
				string,
				*indexpb.StorageConfig,
			) (*snapshotExportPlan, error) {
				_, _, err := meta.UpdateJob(context.Background(), job.GetJobId(), func(latest *datapb.ExportSnapshotJob) (bool, error) {
					latest.State = datapb.ExportSnapshotJobState_ExportSnapshotJobFailed
					return false, nil
				})
				return newPlan(), err
			}).Build()
		defer mockBuild.UnPatch()
		prepareCalls := 0
		mockPrepare := mockey.Mock(prepareSnapshotExportPlanWithSize).To(
			func(context.Context, storage.ChunkManager, *snapshotstorage.SnapshotData, *snapshotExportPlan) (int64, error) {
				prepareCalls++
				return 0, nil
			}).Build()
		defer mockPrepare.UnPatch()

		require.ErrorIs(t, manager.executeJob(context.Background(), job.GetJobId()), errSnapshotExportJobStopped)
		assert.Zero(t, prepareCalls)
	})
}

func TestSnapshotExportManager_PersistenceAndCleanupBranches(t *testing.T) {
	t.Run("persist plan rejects stopped and invalid checkpoint jobs", func(t *testing.T) {
		plan := &snapshotExportPlan{
			version:             snapshotExportPlanVersion,
			fingerprint:         "plan",
			snapshotFingerprint: "snapshot",
			items:               []snapshotExportPlanItem{{sourcePath: "source", destinationPath: "target"}},
		}
		stopped := &datapb.ExportSnapshotJob{JobId: 9001, State: datapb.ExportSnapshotJobState_ExportSnapshotJobFailed}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(stopped))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		_, err = manager.persistOrValidatePlan(context.Background(), stopped.GetJobId(), plan)
		require.ErrorIs(t, err, errSnapshotExportJobStopped)

		invalid := &datapb.ExportSnapshotJob{
			JobId:               9002,
			State:               datapb.ExportSnapshotJobState_ExportSnapshotJobExecuting,
			PlanVersion:         plan.version,
			PlanFingerprint:     plan.fingerprint,
			SnapshotFingerprint: plan.snapshotFingerprint,
			TotalFiles:          1,
			CopyCursor:          2,
			CopiedFiles:         2,
		}
		meta, err = newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(invalid))
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		_, err = manager.persistOrValidatePlan(context.Background(), invalid.GetJobId(), plan)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "checkpoint is invalid")

		publishing := &datapb.ExportSnapshotJob{
			JobId:               9003,
			State:               datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
			TargetS3Path:        "target",
			TotalFiles:          1,
			CopyCursor:          1,
			CopiedFiles:         1,
			SnapshotMetadataUri: "target/snapshots/100/metadata/1.json",
			TotalBytes:          128,
		}
		meta, err = newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(publishing))
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		_, err = manager.persistOrValidatePlan(context.Background(), publishing.GetJobId(), plan)
		require.ErrorIs(t, err, errSnapshotExportJobStopped)
		require.NoError(t, validateSnapshotExportPublishingJob(context.Background(), publishing))

		incomplete := proto.Clone(publishing).(*datapb.ExportSnapshotJob)
		incomplete.CopyCursor = 0
		incomplete.CopiedFiles = 0
		require.ErrorContains(t, validateSnapshotExportPublishingJob(context.Background(), incomplete), "incomplete copy plan")

		missingPaths := proto.Clone(publishing).(*datapb.ExportSnapshotJob)
		missingPaths.SnapshotMetadataUri = ""
		require.ErrorContains(t, validateSnapshotExportPublishingJob(context.Background(), missingPaths), "missing its target paths")

		missingSize := proto.Clone(publishing).(*datapb.ExportSnapshotJob)
		missingSize.TotalBytes = 0
		require.ErrorContains(t, validateSnapshotExportPublishingJob(context.Background(), missingSize), "no prepared bundle size")
	})

	t.Run("failure update handles terminal lock and persistence cases", func(t *testing.T) {
		terminal := &datapb.ExportSnapshotJob{JobId: 9001, State: datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(terminal))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		assert.False(t, manager.failJob(terminal.GetJobId(), "ignored"))

		pending := &datapb.ExportSnapshotJob{JobId: 9002, State: datapb.ExportSnapshotJobState_ExportSnapshotJobPending}
		meta, err = newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(pending))
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		meta.locks.Lock(pending.GetJobId())
		assert.False(t, manager.tryFailJob(pending.GetJobId(), "busy"))
		meta.locks.Unlock(pending.GetJobId())

		publishing := &datapb.ExportSnapshotJob{
			JobId: 9003,
			State: datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing,
		}
		meta, err = newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(publishing))
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		assert.False(t, manager.tryFailJob(publishing.GetJobId(), "snapshot export job timed out"))
		current, ok := meta.GetJob(publishing.GetJobId())
		require.True(t, ok)
		assert.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPublishing, current.GetState())

		catalog := newSnapshotExportCatalogFake(pending)
		catalog.saveErr = errors.New("catalog unavailable")
		meta, err = newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		assert.False(t, manager.failJob(pending.GetJobId(), "failed"))
	})

	t.Run("terminal cleanup clears credentials and handles stale state", func(t *testing.T) {
		job := &datapb.ExportSnapshotJob{
			JobId:        9001,
			State:        datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
			ExternalSpec: `{"extfs":{"access_key_value":"SECRET"}}`,
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(job))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.cleanupTerminalJob(job, uint64(time.Now().UnixMilli()))
		updated, ok := meta.GetJob(job.GetJobId())
		require.True(t, ok)
		assert.Empty(t, updated.GetExternalSpec())

		stale := proto.Clone(job).(*datapb.ExportSnapshotJob)
		manager.cleanupTerminalJob(stale, uint64(time.Now().UnixMilli()))

		failingJob := proto.Clone(job).(*datapb.ExportSnapshotJob)
		failingJob.JobId = 9002
		catalog := newSnapshotExportCatalogFake(failingJob)
		catalog.saveErr = errors.New("catalog unavailable")
		meta, err = newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		manager.cleanupTerminalJob(failingJob, uint64(time.Now().UnixMilli()))
		updated, ok = meta.GetJob(failingJob.GetJobId())
		require.True(t, ok)
		assert.NotEmpty(t, updated.GetExternalSpec())
	})

	t.Run("terminal cleanup handles stale and failed pin persistence", func(t *testing.T) {
		mockUnpin := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(100), "snapshot-1", 0, nil).Build()
		defer mockUnpin.UnPatch()
		current := &datapb.ExportSnapshotJob{JobId: 9001, State: datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(current))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, &snapshotManager{snapshotMeta: &snapshotMeta{}})
		stale := proto.Clone(current).(*datapb.ExportSnapshotJob)
		stale.PinId = 7001
		manager.cleanupTerminalJob(stale, uint64(time.Now().UnixMilli()))

		pinned := &datapb.ExportSnapshotJob{JobId: 9002, State: datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted, PinId: 7002}
		catalog := newSnapshotExportCatalogFake(pinned)
		catalog.saveErr = errors.New("catalog unavailable")
		meta, err = newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, &snapshotManager{snapshotMeta: &snapshotMeta{}})
		manager.cleanupTerminalJob(pinned, uint64(time.Now().UnixMilli()))
		updated, ok := meta.GetJob(pinned.GetJobId())
		require.True(t, ok)
		assert.Equal(t, int64(7002), updated.GetPinId())
	})

	t.Run("terminal retention keeps recent jobs and preserves drop failures", func(t *testing.T) {
		now := uint64(time.Now().UnixMilli())
		recent := &datapb.ExportSnapshotJob{
			JobId:   9001,
			State:   datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
			EndTime: now,
		}
		meta, err := newSnapshotExportMeta(context.Background(), newSnapshotExportCatalogFake(recent))
		require.NoError(t, err)
		manager := newSnapshotExportManager(context.Background(), meta, nil)
		manager.cleanupTerminalJob(recent, now)
		_, ok := meta.GetJob(recent.GetJobId())
		assert.True(t, ok)

		expired := &datapb.ExportSnapshotJob{
			JobId:   9002,
			State:   datapb.ExportSnapshotJobState_ExportSnapshotJobCompleted,
			EndTime: 1,
		}
		catalog := newSnapshotExportCatalogFake(expired)
		catalog.dropErr = errors.New("catalog unavailable")
		meta, err = newSnapshotExportMeta(context.Background(), catalog)
		require.NoError(t, err)
		manager = newSnapshotExportManager(context.Background(), meta, nil)
		manager.cleanupTerminalJob(expired, now)
		_, ok = meta.GetJob(expired.GetJobId())
		assert.True(t, ok)
	})
}

func TestSnapshotManager_RestoreExternalData_PermanentReadErrorCreatesFailedJob(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignCM:            cm,
		ForeignStorageConfig: &indexpb.StorageConfig{},
	}, nil).Build()
	defer mockResolve.UnPatch()
	copyMeta := &copySegmentMeta{}
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(nil).Build()
	defer mockGetJob.UnPatch()
	var persisted CopySegmentJob
	mockAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
			persisted = job
			return nil
		}).Build()
	defer mockAddJob.UnPatch()

	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}, copySegmentMeta: copyMeta}
	jobID, err := sm.RestoreExternalData(
		context.Background(),
		100,
		"snapshot-1",
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		200,
		300,
		"",
		"",
	)

	require.NoError(t, err)
	assert.Equal(t, int64(300), jobID)
	require.NotNil(t, persisted)
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, persisted.GetState())
	assert.Equal(t, int64(200), persisted.GetCollectionId())
	assert.Equal(t, "snapshot-1", persisted.GetSnapshotName())
	assert.Contains(t, persisted.GetReason(), "key not found")
	assert.Contains(t, persisted.GetReason(), "root/snapshots/100/metadata/1.json")
	assert.Greater(t, persisted.GetCleanupTs(), uint64(0))
}

func TestSnapshotManager_RestoreExternalData_TransientReadErrorRemainsRetryable(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignCM:            cm,
		ForeignStorageConfig: &indexpb.StorageConfig{},
	}, nil).Build()
	defer mockResolve.UnPatch()
	readErr := merr.WrapErrIoTooManyRequests("snapshot metadata", errors.New("throttled"))
	mockRead := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).Return(nil, readErr).Build()
	defer mockRead.UnPatch()
	copyMeta := &copySegmentMeta{}
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(nil).Build()
	defer mockGetJob.UnPatch()
	addJobCalled := false
	mockAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, _ CopySegmentJob) error {
			addJobCalled = true
			return nil
		}).Build()
	defer mockAddJob.UnPatch()

	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}, copySegmentMeta: copyMeta}
	jobID, err := sm.RestoreExternalData(
		context.Background(),
		100,
		"snapshot-1",
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		200,
		300,
		"",
		"",
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
	assert.Zero(t, jobID)
	assert.False(t, addJobCalled)
}

func TestSnapshotManager_RestoreExternalData_DataIntegrityCreatesFailedJob(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignCM:            cm,
		ForeignStorageConfig: &indexpb.StorageConfig{},
	}, nil).Build()
	defer mockResolve.UnPatch()
	readErr := merr.WrapErrDataIntegrityMsg("invalid snapshot metadata")
	mockRead := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).Return(nil, readErr).Build()
	defer mockRead.UnPatch()
	copyMeta := &copySegmentMeta{}
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(nil).Build()
	defer mockGetJob.UnPatch()
	var persisted CopySegmentJob
	mockAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
			persisted = job
			return nil
		}).Build()
	defer mockAddJob.UnPatch()

	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}, copySegmentMeta: copyMeta}
	jobID, err := sm.RestoreExternalData(
		context.Background(),
		100,
		"snapshot-1",
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		200,
		300,
		"",
		"",
	)

	require.NoError(t, err)
	assert.Equal(t, int64(300), jobID)
	require.NotNil(t, persisted)
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, persisted.GetState())
	assert.Contains(t, persisted.GetReason(), "invalid snapshot metadata")
}

func TestSnapshotManager_RestoreExternalData_FingerprintMismatchCreatesFailedJob(t *testing.T) {
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = "snapshot-1"
	snapshotData.SnapshotInfo.CollectionId = 100
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignCM:            cm,
		ForeignStorageConfig: &indexpb.StorageConfig{},
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockRead := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).
		Return(snapshotData, nil).Build()
	defer mockRead.UnPatch()
	copyMeta := &copySegmentMeta{}
	mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(nil).Build()
	defer mockGetJob.UnPatch()
	var persisted CopySegmentJob
	mockAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
		func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
			persisted = job
			return nil
		}).Build()
	defer mockAddJob.UnPatch()

	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}, copySegmentMeta: copyMeta}
	jobID, err := sm.RestoreExternalData(
		context.Background(),
		100,
		"snapshot-1",
		"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
		200,
		300,
		"",
		"preflight-fingerprint",
	)

	require.NoError(t, err)
	assert.Equal(t, int64(300), jobID)
	require.NotNil(t, persisted)
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, persisted.GetState())
	assert.Contains(t, persisted.GetReason(), "fingerprint mismatch")
}

func TestSnapshotManager_RestoreExternalData_DeterministicMappingErrorsCreateFailedJob(t *testing.T) {
	tests := []struct {
		name             string
		snapshotData     *snapshotstorage.SnapshotData
		targetPartitions *milvuspb.ShowPartitionsResponse
		targetChannels   []RWChannel
		reason           string
	}{
		{
			name: "missing target partition",
			snapshotData: &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot-1", CollectionId: 100},
				Collection: &datapb.CollectionDescription{
					Partitions:          map[string]int64{"source-partition": 10},
					VirtualChannelNames: []string{"source-channel"},
				},
				Segments: []*datapb.SegmentDescription{{
					SegmentId:   1,
					PartitionId: 10,
					ChannelName: "source-channel",
				}},
			},
			targetPartitions: &milvuspb.ShowPartitionsResponse{
				PartitionNames: []string{"other-partition"},
				PartitionIDs:   []int64{20},
			},
			targetChannels: []RWChannel{&channelMeta{Name: "target-channel"}},
			reason:         "partition mapping failed",
		},
		{
			name: "channel count mismatch",
			snapshotData: &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot-1", CollectionId: 100},
				Collection: &datapb.CollectionDescription{
					Partitions:          map[string]int64{"source-partition": 10},
					VirtualChannelNames: []string{"source-channel-1", "source-channel-2"},
				},
				Segments: []*datapb.SegmentDescription{{
					SegmentId:   1,
					PartitionId: 10,
					ChannelName: "source-channel-1",
				}},
			},
			targetPartitions: &milvuspb.ShowPartitionsResponse{
				PartitionNames: []string{"source-partition"},
				PartitionIDs:   []int64{20},
			},
			targetChannels: []RWChannel{&channelMeta{Name: "target-channel"}},
			reason:         "channel mapping failed",
		},
		{
			name: "segment channel missing from mapping",
			snapshotData: &snapshotstorage.SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Name: "snapshot-1", CollectionId: 100},
				Collection: &datapb.CollectionDescription{
					Partitions:          map[string]int64{"source-partition": 10},
					VirtualChannelNames: []string{"declared-channel"},
				},
				Segments: []*datapb.SegmentDescription{{
					SegmentId:   1,
					PartitionId: 10,
					ChannelName: "missing-channel",
				}},
			},
			targetPartitions: &milvuspb.ShowPartitionsResponse{
				PartitionNames: []string{"source-partition"},
				PartitionIDs:   []int64{20},
			},
			targetChannels: []RWChannel{&channelMeta{Name: "target-channel"}},
			reason:         "restore job creation failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
			mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
				ForeignCM:            cm,
				ForeignStorageConfig: &indexpb.StorageConfig{},
			}, nil).Build()
			defer mockResolve.UnPatch()
			mockRead := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).
				Return(test.snapshotData, nil).
				Build()
			defer mockRead.UnPatch()

			fakeBroker := &restoreBrokerTarget{}
			mockShowPartitions := mockey.Mock((*restoreBrokerTarget).ShowPartitions).
				Return(test.targetPartitions, nil).
				Build()
			defer mockShowPartitions.UnPatch()

			alloc := &restoreAllocatorTarget{}
			mockAllocN := mockey.Mock((*restoreAllocatorTarget).AllocN).
				Return(typeutil.UniqueID(1000), typeutil.UniqueID(1001), nil).
				Build()
			defer mockAllocN.UnPatch()

			copyMeta := &copySegmentMeta{}
			mockGetJob := mockey.Mock((*copySegmentMeta).GetJob).Return(nil).Build()
			defer mockGetJob.UnPatch()
			var persisted CopySegmentJob
			mockAddJob := mockey.Mock((*copySegmentMeta).AddJob).To(
				func(_ *copySegmentMeta, _ context.Context, job CopySegmentJob) error {
					persisted = job
					return nil
				}).Build()
			defer mockAddJob.UnPatch()

			sm := &snapshotManager{
				snapshotMeta:    &snapshotMeta{},
				copySegmentMeta: copyMeta,
				allocator:       alloc,
				broker:          fakeBroker,
				getChannelsByCollectionID: func(context.Context, int64) ([]RWChannel, error) {
					return test.targetChannels, nil
				},
			}
			jobID, err := sm.RestoreExternalData(
				context.Background(),
				100,
				"snapshot-1",
				"s3://foreign-bucket/root/snapshots/100/metadata/1.json",
				200,
				300,
				"",
				"",
			)

			require.NoError(t, err)
			assert.Equal(t, int64(300), jobID)
			require.NotNil(t, persisted)
			assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, persisted.GetState())
			assert.Contains(t, persisted.GetReason(), test.reason)
		})
	}
}

func TestRestoreExternalSnapshot_RejectsUnsupportedExternalSpecBeforeBroadcast(t *testing.T) {
	ctx := context.Background()
	unsupportedSpec := `{"extfs":{"cloud_provider":"aws","role_arn":"arn:aws:iam::1:role/snapshot"}}`
	snapshotURI := "s3://foreign-bucket/root/snapshots/100/metadata/1.json"

	broadcastCalled := false
	sm := &snapshotManager{}
	_, err := sm.RestoreExternalSnapshot(
		ctx,
		snapshotURI,
		"target_collection",
		"target_db",
		unsupportedSpec,
		func(context.Context, string, string) (broadcaster.BroadcastAPI, error) {
			return newMockBroadcastAPIImpl(), nil
		},
		func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) {
			broadcastCalled = true
			return newMockBroadcastAPIImpl(), nil
		},
		func(context.Context, string, string) error {
			return nil
		},
		func(context.Context, int64, *snapshotstorage.SnapshotData) error {
			return nil
		},
	)

	require.Error(t, err)
	assert.False(t, broadcastCalled)
}

func TestRestoreExternalSnapshot_RejectsObjectKeyBeforeLock(t *testing.T) {
	ctx := context.Background()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).
		Return(nil, errors.New("object key reached metadata read")).Build()
	defer mockReadExternal.UnPatch()

	lockCalled := false
	broadcastCalled := false
	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}}

	_, err := sm.RestoreExternalSnapshot(
		ctx,
		"export-root/snapshots/100/metadata/1.json",
		"target_collection",
		"target_db",
		"",
		func(context.Context, string, string) (broadcaster.BroadcastAPI, error) {
			lockCalled = true
			return newMockBroadcastAPIImpl(), nil
		},
		func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) {
			broadcastCalled = true
			return newMockBroadcastAPIImpl(), nil
		},
		func(context.Context, string, string) error {
			return nil
		},
		func(context.Context, int64, *snapshotstorage.SnapshotData) error {
			return nil
		},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "complete URI")
	assert.False(t, lockCalled)
	assert.False(t, broadcastCalled)
}

func TestRestoreExternalSnapshot_BroadcastCarriesExternalSpec(t *testing.T) {
	ctx := context.Background()
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	foreignSpec := `{"extfs":{"region":"us-west-2"}}`
	snapshotURI := "s3://foreign-bucket/root/snapshots/s1/metadata/1.json"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = "snapshot-1"
	snapshotData.SnapshotInfo.CollectionId = 100
	foreignStorageConfig := &indexpb.StorageConfig{
		BucketName: "foreign-bucket",
		RootPath:   "root",
	}

	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).To(
		func(
			_ context.Context,
			_ *objectstorage.Config,
			direction snapshotstorage.Direction,
			foreignURI string,
			externalSpec string,
		) (*snapshotstorage.ResolvedForeignStorage, error) {
			assert.Equal(t, snapshotstorage.DirectionRestore, direction)
			assert.Equal(t, snapshotURI, foreignURI)
			assert.Equal(t, foreignSpec, externalSpec)
			return &snapshotstorage.ResolvedForeignStorage{
				ForeignBucket:        "foreign-bucket",
				ForeignCM:            sourceCM,
				ForeignStorageConfig: foreignStorageConfig,
			}, nil
		}).Build()
	defer mockResolve.UnPatch()

	mockReadExternal := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).To(
		func(
			_ *snapshotMeta,
			_ context.Context,
			gotCM storage.ChunkManager,
			gotURI string,
			includeSegments bool,
			gotStorageConfig *indexpb.StorageConfig,
		) (*snapshotstorage.SnapshotData, error) {
			assert.Same(t, sourceCM, gotCM)
			assert.Equal(t, snapshotURI, gotURI)
			assert.True(t, includeSegments)
			assert.Same(t, foreignStorageConfig, gotStorageConfig)
			return snapshotData, nil
		}).Build()
	defer mockReadExternal.UnPatch()

	mockValidateCMEK := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer mockValidateCMEK.UnPatch()
	phase0Lock := newMockBroadcastAPIImpl()
	mockRestoreCollection := mockey.Mock((*snapshotManager).RestoreCollection).To(
		func(
			_ *snapshotManager,
			_ context.Context,
			_ *snapshotstorage.SnapshotData,
			_, _ string,
		) (int64, error) {
			assert.True(t, phase0Lock.closeCalled.Load())
			return 200, nil
		}).Build()
	defer mockRestoreCollection.UnPatch()
	mockRestoreIndexes := mockey.Mock((*snapshotManager).RestoreIndexes).Return(nil).Build()
	defer mockRestoreIndexes.UnPatch()
	alloc := &restoreAllocatorTarget{}
	mockAllocID := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(77), nil).Build()
	defer mockAllocID.UnPatch()

	capture := &captureBroadcastAPI{}
	targetBroker := &restoreBrokerTarget{}
	mockDescribeTarget := mockey.Mock((*restoreBrokerTarget).DescribeCollectionByName).
		Return(nil, merr.WrapErrCollectionNotFound("target_collection")).Build()
	defer mockDescribeTarget.UnPatch()
	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
		allocator:    alloc,
		broker:       targetBroker,
	}
	streaming.SetupNoopWALForTest()

	jobID, err := sm.RestoreExternalSnapshot(
		ctx,
		snapshotURI,
		"target_collection",
		"target_db",
		foreignSpec,
		func(context.Context, string, string) (broadcaster.BroadcastAPI, error) {
			return phase0Lock, nil
		},
		func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) {
			return capture, nil
		},
		func(context.Context, string, string) error {
			return nil
		},
		func(context.Context, int64, *snapshotstorage.SnapshotData) error {
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(77), jobID)
	assert.True(t, phase0Lock.closeCalled.Load())
	require.NotNil(t, capture.captured)

	restoreMsg := message.MustAsBroadcastRestoreSnapshotMessageV2(capture.captured)
	header := restoreMsg.Header()
	assert.Equal(t, "snapshot-1", header.GetSnapshotName())
	assert.Equal(t, int64(200), header.GetCollectionId())
	assert.Equal(t, int64(77), header.GetJobId())
	assert.Equal(t, int64(100), header.GetSourceCollectionId())
	assert.True(t, header.GetExternal())
	assert.Equal(t, snapshotURI, header.GetSnapshotS3Location())
	assert.Equal(t, foreignSpec, header.GetExternalSpec())
	expectedFingerprint, err := snapshotstorage.SnapshotFingerprint(snapshotData)
	require.NoError(t, err)
	assert.Equal(t, expectedFingerprint, header.GetSnapshotFingerprint())
	formattedHeader := protojson.Format(header)
	assert.NotContains(t, formattedHeader, "secret_access_key")
	assert.NotContains(t, formattedHeader, "credential_json")
	assert.NotContains(t, formattedHeader, "sas")
	assert.NotContains(t, formattedHeader, "role_arn")
}

func TestRestoreExternalSnapshot_RejectsExistingTargetUnderNameLock(t *testing.T) {
	ctx := context.Background()
	snapshotURI := "s3://foreign-bucket/root/snapshots/s1/metadata/1.json"
	phase0Lock := newMockBroadcastAPIImpl()

	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(
		&snapshotstorage.ResolvedForeignStorage{
			ForeignCM:            storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())),
			ForeignStorageConfig: &indexpb.StorageConfig{BucketName: "foreign-bucket", RootPath: "root"},
		},
		nil,
	).Build()
	defer mockResolve.UnPatch()

	targetBroker := &restoreBrokerTarget{}
	mockDescribeTarget := mockey.Mock((*restoreBrokerTarget).DescribeCollectionByName).To(
		func(_ *restoreBrokerTarget, _ context.Context, dbName, collectionName string) (*milvuspb.DescribeCollectionResponse, error) {
			assert.Equal(t, "target_db", dbName)
			assert.Equal(t, "target_collection", collectionName)
			assert.False(t, phase0Lock.closeCalled.Load())
			return &milvuspb.DescribeCollectionResponse{CollectionID: 200}, nil
		}).Build()
	defer mockDescribeTarget.UnPatch()

	readCalled := false
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).To(
		func(
			_ *snapshotMeta,
			_ context.Context,
			_ storage.ChunkManager,
			_ string,
			_ bool,
			_ *indexpb.StorageConfig,
		) (*snapshotstorage.SnapshotData, error) {
			readCalled = true
			return nil, merr.WrapErrDataIntegrityMsg("external metadata should not be read")
		}).Build()
	defer mockReadExternal.UnPatch()

	sm := &snapshotManager{
		snapshotMeta: &snapshotMeta{},
		broker:       targetBroker,
	}
	_, err := sm.RestoreExternalSnapshot(
		ctx,
		snapshotURI,
		"target_collection",
		"target_db",
		"",
		func(context.Context, string, string) (broadcaster.BroadcastAPI, error) {
			return phase0Lock, nil
		},
		nil,
		nil,
		nil,
	)

	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
	assert.Contains(t, err.Error(), "already exists")
	assert.False(t, readCalled)
	assert.True(t, phase0Lock.closeCalled.Load())
}

func TestRestoreExternalSnapshot_SerializesSameTarget(t *testing.T) {
	ctx := context.Background()
	snapshotURI := "s3://foreign-bucket/root/snapshots/s1/metadata/1.json"
	snapshotData := createTestSnapshotDataForMeta()
	foreignCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	firstResolveEntered := make(chan struct{})
	releaseFirstResolve := make(chan struct{})
	secondResolveEntered := make(chan struct{}, 1)
	var resolveCalls atomic.Int32

	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).To(
		func(
			_ context.Context,
			_ *objectstorage.Config,
			_ snapshotstorage.Direction,
			_ string,
			_ string,
		) (*snapshotstorage.ResolvedForeignStorage, error) {
			if resolveCalls.Add(1) == 1 {
				close(firstResolveEntered)
				<-releaseFirstResolve
			} else {
				secondResolveEntered <- struct{}{}
			}
			return &snapshotstorage.ResolvedForeignStorage{
				ForeignCM:            foreignCM,
				ForeignStorageConfig: &indexpb.StorageConfig{BucketName: "foreign-bucket", RootPath: "root"},
			}, nil
		}).Build()
	defer mockResolve.UnPatch()

	mockValidateTarget := mockey.Mock((*snapshotManager).validateRestoreTargetAbsent).Return(nil).Build()
	defer mockValidateTarget.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadAndValidateExternalSnapshotDataWithChunkManager).
		Return(snapshotData, nil).Build()
	defer mockReadExternal.UnPatch()
	mockValidateCMEK := mockey.Mock((*snapshotManager).validateCMEKCompatibility).Return(nil).Build()
	defer mockValidateCMEK.UnPatch()
	mockFinishRestore := mockey.Mock((*snapshotManager).finishRestoreSnapshot).Return(int64(77), nil).Build()
	defer mockFinishRestore.UnPatch()

	sm := &snapshotManager{snapshotMeta: &snapshotMeta{}}
	callRestore := func() error {
		_, err := sm.RestoreExternalSnapshot(
			ctx,
			snapshotURI,
			"target_collection",
			"target_db",
			"",
			func(context.Context, string, string) (broadcaster.BroadcastAPI, error) {
				return newMockBroadcastAPIImpl(), nil
			},
			nil,
			nil,
			nil,
		)
		return err
	}

	firstResult := make(chan error, 1)
	go func() { firstResult <- callRestore() }()
	<-firstResolveEntered

	secondResult := make(chan error, 1)
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		secondResult <- callRestore()
	}()
	<-secondStarted
	select {
	case <-secondResolveEntered:
		require.FailNow(t, "second restore reached storage resolution before the first restore completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirstResolve)
	require.NoError(t, <-firstResult)
	require.NoError(t, <-secondResult)
	assert.Equal(t, int32(2), resolveCalls.Load())
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
