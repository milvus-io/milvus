// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	kv_datacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// waitForRefIndexLoaded waits for a specific RefIndex to be loaded with timeout.
func waitForRefIndexLoaded(sm *snapshotMeta, snapshotID int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotID)
		if exists && refIndex.IsLoaded() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitForRefIndexFailed waits for a specific RefIndex to be marked as failed with timeout.
func waitForRefIndexFailed(sm *snapshotMeta, snapshotID int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotID)
		if exists && refIndex.IsFailed() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// --- Helper functions for test data creation ---

func createTestSnapshotInfoForMeta() *datapb.SnapshotInfo {
	return &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		CreateTs:     1234567890,
		Name:         "test_snapshot",
		Description:  "test description",
		S3Location:   "s3://test-bucket/snapshot",
		PartitionIds: []int64{1, 2},
	}
}

func createTestSnapshotDataForMeta() *SnapshotData {
	return &SnapshotData{
		SnapshotInfo: createTestSnapshotInfoForMeta(),
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1001,
				PartitionId: 1,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001},
					{BuildID: 3002},
				},
			},
		},
		Indexes: []*indexpb.IndexInfo{
			{
				IndexID: 2001,
			},
		},
	}
}

func createTestSnapshotMeta(t *testing.T) *snapshotMeta {
	// Create empty Catalog for mockey to mock
	catalog := &kv_datacoord.Catalog{}

	// Use temporary directory to avoid polluting project directory
	tempDir := t.TempDir()
	tempChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	loaderCtx, loaderCancel := context.WithCancel(context.Background())
	return &snapshotMeta{
		catalog:                catalog,
		snapshotID2Info:        typeutil.NewConcurrentMap[typeutil.UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[typeutil.UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[typeutil.UniqueID, *typeutil.ConcurrentMap[string, typeutil.UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[typeutil.UniqueID, typeutil.UniqueSet](),
		loaderCtx:              loaderCtx,
		loaderCancel:           loaderCancel,
		reader:                 NewSnapshotReader(tempChunkManager),
		writer:                 NewSnapshotWriter(tempChunkManager),
	}
}

// createTestSnapshotMetaLoaded creates a snapshotMeta for tests that don't call reload().
// Same as createTestSnapshotMeta since RefIndex state is now per-snapshot.
func createTestSnapshotMetaLoaded(t *testing.T) *snapshotMeta {
	return createTestSnapshotMeta(t)
}

// insertTestSnapshot inserts snapshot data into snapshotMeta for testing.
// Use this for setting up test data when you don't need to go through SaveSnapshot.
func insertTestSnapshot(sm *snapshotMeta, info *datapb.SnapshotInfo, segmentIDs, indexIDs []int64) {
	sm.snapshotID2Info.Insert(info.GetId(), info)
	sm.snapshotID2RefIndex.Insert(info.GetId(), NewLoadedSnapshotRefIndex(segmentIDs, indexIDs, nil))
	sm.addToSecondaryIndexes(info)
}

// saveTestSnapshots saves multiple snapshots to snapshotMeta using mocked catalog and writer.
// This is the preferred way to set up test data as it exercises the real SaveSnapshot logic.
// Returns cleanup function that must be deferred by the caller.
func saveTestSnapshots(t *testing.T, sm *snapshotMeta, snapshots ...*SnapshotData) func() {
	// Mock SnapshotWriter.Save
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, s *SnapshotData) (string, error) {
		return fmt.Sprintf("s3://bucket/snapshots/%d/metadata.json", s.SnapshotInfo.GetId()), nil
	}).Build()

	// Mock catalog.SaveSnapshot
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()

	// Save all snapshots
	for _, snapshot := range snapshots {
		err := sm.SaveSnapshot(context.Background(), snapshot)
		assert.NoError(t, err)
	}

	// Return cleanup function
	return func() {
		mock1.UnPatch()
		mock2.UnPatch()
	}
}

// --- ListSnapshots Tests ---

func TestSnapshotMeta_ListSnapshots_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := int64(100)
	partitionID := int64(1)

	snapshot1 := createTestSnapshotDataForMeta()
	snapshot1.SnapshotInfo.Name = "snapshot1"
	snapshot1.SnapshotInfo.Id = 1
	snapshot1.SnapshotInfo.CollectionId = 100
	snapshot1.SnapshotInfo.PartitionIds = []int64{1, 2}

	snapshot2 := createTestSnapshotDataForMeta()
	snapshot2.SnapshotInfo.Name = "snapshot2"
	snapshot2.SnapshotInfo.Id = 2
	snapshot2.SnapshotInfo.CollectionId = 100
	snapshot2.SnapshotInfo.PartitionIds = []int64{3, 4}

	sm := createTestSnapshotMetaLoaded(t)
	cleanup := saveTestSnapshots(t, sm, snapshot1, snapshot2)
	defer cleanup()

	// Act
	snapshots, err := sm.ListSnapshots(ctx, collectionID, partitionID)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 1) // Only snapshot1 matches collection 100 and partition 1
	assert.Contains(t, snapshots, "snapshot1")
}

func TestSnapshotMeta_ListSnapshots_AllCollections(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := int64(0) // 0 means all collections
	partitionID := int64(0)  // 0 means all partitions

	snapshot1 := createTestSnapshotDataForMeta()
	snapshot1.SnapshotInfo.Name = "snapshot1"
	snapshot1.SnapshotInfo.Id = 1

	snapshot2 := createTestSnapshotDataForMeta()
	snapshot2.SnapshotInfo.Name = "snapshot2"
	snapshot2.SnapshotInfo.Id = 2

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshot1.SnapshotInfo, nil, nil)
	insertTestSnapshot(sm, snapshot2.SnapshotInfo, nil, nil)

	// Act
	snapshots, err := sm.ListSnapshots(ctx, collectionID, partitionID)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 2)
	assert.Contains(t, snapshots, "snapshot1")
	assert.Contains(t, snapshots, "snapshot2")
}

func TestSnapshotMeta_ListSnapshots_EmptyResult(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := int64(999) // Non-existent collection
	partitionID := int64(0)

	sm := createTestSnapshotMetaLoaded(t)

	// Act
	snapshots, err := sm.ListSnapshots(ctx, collectionID, partitionID)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 0)
}

// --- GetSnapshot Tests ---

func TestSnapshotMeta_GetSnapshot_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()

	sm := createTestSnapshotMetaLoaded(t)
	cleanup := saveTestSnapshots(t, sm, snapshotData)
	defer cleanup()

	// Act
	result, err := sm.GetSnapshot(ctx, int64(100), snapshotName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshotName, result.GetName())
}

func TestSnapshotMeta_GetSnapshot_NotFound(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "nonexistent_snapshot"

	sm := createTestSnapshotMetaLoaded(t)

	// Act
	result, err := sm.GetSnapshot(ctx, int64(100), snapshotName)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "not found")
}

// --- getSnapshotByName Tests ---

func TestSnapshotMeta_GetSnapshotByName_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()

	sm := createTestSnapshotMetaLoaded(t)
	cleanup := saveTestSnapshots(t, sm, snapshotData)
	defer cleanup()

	// Act
	result, err := sm.getSnapshotByName(ctx, int64(100), snapshotName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshotName, result.GetName())
}

func TestSnapshotMeta_GetSnapshotByName_NotFound(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "nonexistent_snapshot"

	sm := createTestSnapshotMetaLoaded(t)

	// Act
	result, err := sm.getSnapshotByName(ctx, int64(100), snapshotName)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "not found")
}

func TestSnapshotMeta_GetSnapshotByName_MultipleSnapshots(t *testing.T) {
	// Arrange
	ctx := context.Background()
	targetName := "target_snapshot"

	snapshot1 := createTestSnapshotDataForMeta()
	snapshot1.SnapshotInfo.Name = "snapshot1"
	snapshot1.SnapshotInfo.Id = 1

	snapshot2 := createTestSnapshotDataForMeta()
	snapshot2.SnapshotInfo.Name = targetName
	snapshot2.SnapshotInfo.Id = 2

	snapshot3 := createTestSnapshotDataForMeta()
	snapshot3.SnapshotInfo.Name = "snapshot3"
	snapshot3.SnapshotInfo.Id = 3

	sm := createTestSnapshotMetaLoaded(t)
	cleanup := saveTestSnapshots(t, sm, snapshot1, snapshot2, snapshot3)
	defer cleanup()

	// Act
	result, err := sm.getSnapshotByName(ctx, int64(100), targetName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshot2.SnapshotInfo, result)
}

// --- GetSnapshotBySegment Tests ---

func TestSnapshotMeta_GetSnapshotBySegment_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := typeutil.UniqueID(100)
	segmentID := typeutil.UniqueID(1001)

	snapshotInfo1 := createTestSnapshotInfoForMeta()
	snapshotInfo1.CollectionId = 100
	snapshotInfo1.Id = 1

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2

	snapshotInfo3 := createTestSnapshotInfoForMeta()
	snapshotInfo3.CollectionId = 200 // Different collection
	snapshotInfo3.Id = 3

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshotInfo1, []int64{1001, 1002}, []int64{2001})
	insertTestSnapshot(sm, snapshotInfo2, []int64{1003, 1004}, []int64{2002})
	insertTestSnapshot(sm, snapshotInfo3, []int64{1001}, []int64{2003})

	// Act
	snapshotIDs := sm.GetSnapshotBySegment(ctx, collectionID, segmentID)

	// Assert
	assert.Len(t, snapshotIDs, 1)
	assert.Contains(t, snapshotIDs, UniqueID(1)) // Only snapshot1 matches
}

func TestSnapshotMeta_GetSnapshotBySegment_EmptyResult(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	segmentID := UniqueID(9999) // Non-existent segment

	snapshotInfo := createTestSnapshotInfoForMeta()
	snapshotInfo.CollectionId = 100
	snapshotInfo.Id = 1

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshotInfo, []int64{1001, 1002}, []int64{2001})

	// Act
	snapshotIDs := sm.GetSnapshotBySegment(ctx, collectionID, segmentID)

	// Assert
	assert.Len(t, snapshotIDs, 0)
}

func TestSnapshotMeta_GetSnapshotBySegment_MultipleMatches(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	segmentID := UniqueID(1001)

	snapshotInfo1 := createTestSnapshotInfoForMeta()
	snapshotInfo1.CollectionId = 100
	snapshotInfo1.Id = 1

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshotInfo1, []int64{1001, 1002}, []int64{2001})
	insertTestSnapshot(sm, snapshotInfo2, []int64{1001, 1003}, []int64{2002})

	// Act
	snapshotIDs := sm.GetSnapshotBySegment(ctx, collectionID, segmentID)

	// Assert
	assert.Len(t, snapshotIDs, 2)
	assert.Contains(t, snapshotIDs, UniqueID(1))
	assert.Contains(t, snapshotIDs, UniqueID(2))
}

// --- GetSnapshotByIndex Tests ---

func TestSnapshotMeta_GetSnapshotByIndex_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	indexID := UniqueID(2001)

	snapshotInfo1 := createTestSnapshotInfoForMeta()
	snapshotInfo1.CollectionId = 100
	snapshotInfo1.Id = 1

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2

	snapshotInfo3 := createTestSnapshotInfoForMeta()
	snapshotInfo3.CollectionId = 200 // Different collection
	snapshotInfo3.Id = 3

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshotInfo1, []int64{1001}, []int64{2001, 2002})
	insertTestSnapshot(sm, snapshotInfo2, []int64{1002}, []int64{2003, 2004})
	insertTestSnapshot(sm, snapshotInfo3, []int64{1003}, []int64{2001})

	// Act
	snapshotIDs := sm.GetSnapshotByIndex(ctx, collectionID, indexID)

	// Assert
	assert.Len(t, snapshotIDs, 1)
	assert.Contains(t, snapshotIDs, UniqueID(1)) // Only snapshot1 matches
}

func TestSnapshotMeta_GetSnapshotByIndex_EmptyResult(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	indexID := UniqueID(9999) // Non-existent index

	snapshotInfo := createTestSnapshotInfoForMeta()
	snapshotInfo.CollectionId = 100
	snapshotInfo.Id = 1

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshotInfo, []int64{1001}, []int64{2001, 2002})

	// Act
	snapshotIDs := sm.GetSnapshotByIndex(ctx, collectionID, indexID)

	// Assert
	assert.Len(t, snapshotIDs, 0)
}

func TestSnapshotMeta_GetSnapshotByIndex_MultipleMatches(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	indexID := UniqueID(2001)

	snapshotInfo1 := createTestSnapshotInfoForMeta()
	snapshotInfo1.CollectionId = 100
	snapshotInfo1.Id = 1

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2

	sm := createTestSnapshotMetaLoaded(t)
	insertTestSnapshot(sm, snapshotInfo1, []int64{1001}, []int64{2001, 2002})
	insertTestSnapshot(sm, snapshotInfo2, []int64{1002}, []int64{2001, 2003})

	// Act
	snapshotIDs := sm.GetSnapshotByIndex(ctx, collectionID, indexID)

	// Assert
	assert.Len(t, snapshotIDs, 2)
	assert.Contains(t, snapshotIDs, UniqueID(1))
	assert.Contains(t, snapshotIDs, UniqueID(2))
}

// --- newSnapshotMeta Tests (Mockey-based) ---

func TestNewSnapshotMeta_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	mockCatalog := &kv_datacoord.Catalog{}
	tempDir := t.TempDir()
	mockChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	// Mock catalog.ListSnapshots to return empty list
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{}, nil
	}).Build()
	defer mock1.UnPatch()

	// Act
	sm, err := newSnapshotMeta(ctx, mockCatalog, mockChunkManager)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, sm)
	defer sm.Close()
	assert.Equal(t, mockCatalog, sm.catalog)
	assert.NotNil(t, sm.snapshotID2Info)
	assert.NotNil(t, sm.snapshotID2RefIndex)
	assert.NotNil(t, sm.reader)
	assert.NotNil(t, sm.writer)
}

func TestNewSnapshotMeta_CatalogListError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	mockCatalog := &kv_datacoord.Catalog{}
	tempDir := t.TempDir()
	mockChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	expectedErr := errors.New("catalog list snapshots failed")

	// Mock catalog.ListSnapshots to return error
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return nil, expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	sm, err := newSnapshotMeta(ctx, mockCatalog, mockChunkManager)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, sm)
	assert.Contains(t, err.Error(), "catalog list snapshots failed")
}

func TestNewSnapshotMeta_ReaderError_AsyncLoading_WithMockey(t *testing.T) {
	// With async loading, reader errors don't fail newSnapshotMeta.
	// The snapshot is still loaded, but with empty segment/index sets.
	// Arrange
	ctx := context.Background()
	mockCatalog := &kv_datacoord.Catalog{}
	tempDir := t.TempDir()
	mockChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	snapshotInfo := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "test_snapshot",
		S3Location:   "s3://bucket/snapshot",
	}

	// Mock catalog.ListSnapshots to return snapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{snapshotInfo}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot to return error
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, path string, includeSegments bool) (*SnapshotData, error) {
		return nil, errors.New("reader failed")
	}).Build()
	defer mock2.UnPatch()

	// Act
	sm, err := newSnapshotMeta(ctx, mockCatalog, mockChunkManager)

	// Assert - newSnapshotMeta succeeds even with reader error (async loading)
	assert.NoError(t, err)
	assert.NotNil(t, sm)

	// Snapshot info is still available
	info, exists := sm.snapshotID2Info.Get(snapshotInfo.Id)
	assert.True(t, exists)
	assert.Equal(t, snapshotInfo.Name, info.Name)

	// Wait for background loader to process (will fail and mark as Failed)
	assert.True(t, waitForRefIndexFailed(sm, snapshotInfo.Id, 2*time.Second))

	// RefIndex should be in Failed state (due to reader error)
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotInfo.Id)
	assert.True(t, exists)
	assert.True(t, refIndex.IsFailed())
	// Should return false since loading failed
	assert.False(t, refIndex.ContainsSegment(1001))

	// Cleanup
	sm.Close()
}

// --- reload Tests (Mockey-based) ---

func TestSnapshotMeta_Reload_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
	snapshotData := createTestSnapshotDataForMeta()
	// Add pre-computed IDs to test fast path
	snapshotData.SegmentIDs = []int64{1001}
	snapshotData.IndexIDs = []int64{2001}
	// Use the SnapshotInfo from snapshotData for catalog.ListSnapshots
	snapshotInfo := snapshotData.SnapshotInfo

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{snapshotInfo}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot - now called with metadataFilePath instead of collection/snapshot IDs
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		assert.Equal(t, snapshotInfo.S3Location, metadataFilePath)
		assert.False(t, includeSegments) // Fast path: includeSegments=false
		return snapshotData, nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)
	// Verify snapshot info was inserted into map immediately
	info, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.Id)
	assert.True(t, exists)
	assert.Equal(t, snapshotData.SnapshotInfo, info)

	// Verify refIndex exists (in Pending state)
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.Id)
	assert.True(t, exists)
	assert.False(t, refIndex.IsLoaded()) // Not loaded yet

	// Manually trigger loading (simulates background goroutine)
	sm.loadUnloadedRefIndexes()

	// Verify segment and index IDs are correctly loaded
	assert.True(t, refIndex.IsLoaded())
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsIndex(2001))
}

func TestSnapshotMeta_Reload_CatalogError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
	expectedErr := errors.New("catalog error")

	// Mock catalog.ListSnapshots to return error
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return nil, expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotMeta_Reload_Concurrent_MultipleSnapshots(t *testing.T) {
	// Test concurrent loading of multiple snapshots using fast path (pre-computed IDs)
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Create multiple test snapshots with pre-computed IDs (new format)
	numSnapshots := 10
	snapshotInfos := make([]*datapb.SnapshotInfo, numSnapshots)
	// Use sync.Map to avoid data race in concurrent mock calls
	var snapshotDataMap sync.Map

	for i := 0; i < numSnapshots; i++ {
		snapshotID := int64(1000 + i)
		collectionID := int64(100)
		snapshotData := &SnapshotData{
			SnapshotInfo: &datapb.SnapshotInfo{
				Id:           snapshotID,
				Name:         fmt.Sprintf("test_snapshot_%d", i),
				CollectionId: collectionID,
				S3Location:   fmt.Sprintf("s3://bucket/snapshots/%d/metadata.json", snapshotID),
			},
			Segments: []*datapb.SegmentDescription{
				{SegmentId: snapshotID * 10},
				{SegmentId: snapshotID*10 + 1},
			},
			Indexes: []*indexpb.IndexInfo{
				{IndexID: snapshotID * 100},
			},
			// Pre-computed IDs for fast path (new format)
			SegmentIDs: []int64{snapshotID * 10, snapshotID*10 + 1},
			IndexIDs:   []int64{snapshotID * 100},
		}
		snapshotInfos[i] = snapshotData.SnapshotInfo
		snapshotDataMap.Store(snapshotID, snapshotData)
	}

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return snapshotInfos, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot - will be called with metadataFilePath (fast path)
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		// Find snapshot by S3Location - iterate over a copy to avoid data race
		for _, info := range snapshotInfos {
			if info.S3Location == metadataFilePath {
				data, exists := snapshotDataMap.Load(info.Id)
				if !exists {
					return nil, errors.New("snapshot not found in map")
				}
				// Return a deep copy to avoid data race when reload modifies SnapshotInfo.S3Location
				original := data.(*SnapshotData)
				return &SnapshotData{
					SnapshotInfo: &datapb.SnapshotInfo{
						Id:           original.SnapshotInfo.Id,
						Name:         original.SnapshotInfo.Name,
						CollectionId: original.SnapshotInfo.CollectionId,
						S3Location:   original.SnapshotInfo.S3Location,
					},
					Segments:   original.Segments,
					Indexes:    original.Indexes,
					SegmentIDs: original.SegmentIDs,
					IndexIDs:   original.IndexIDs,
				}, nil
			}
		}
		return nil, errors.New("snapshot not found")
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)

	// Verify all snapshots were inserted immediately
	for _, snapshotInfo := range snapshotInfos {
		info, exists := sm.snapshotID2Info.Get(snapshotInfo.Id)
		assert.True(t, exists, "snapshot %d should be loaded", snapshotInfo.Id)
		assert.Equal(t, snapshotInfo, info)
	}

	// Manually trigger loading (simulates background goroutine)
	sm.loadUnloadedRefIndexes()

	// Verify segment and index IDs are correctly loaded
	for _, snapshotInfo := range snapshotInfos {
		refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotInfo.Id)
		assert.True(t, exists)

		expectedData, _ := snapshotDataMap.Load(snapshotInfo.Id)
		for _, segID := range expectedData.(*SnapshotData).SegmentIDs {
			assert.True(t, refIndex.ContainsSegment(segID))
		}
		for _, idxID := range expectedData.(*SnapshotData).IndexIDs {
			assert.True(t, refIndex.ContainsIndex(idxID))
		}
	}
}

func TestSnapshotMeta_Reload_Concurrent_PartialFailure(t *testing.T) {
	// Test that if one snapshot fails to load from S3, other snapshots still load successfully
	// With async loading, reload returns immediately and failures are logged as warnings
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Create multiple test snapshots
	snapshotInfos := []*datapb.SnapshotInfo{
		{Id: 1001, Name: "snapshot_1", CollectionId: 100, S3Location: "s3://bucket/1001"},
		{Id: 1002, Name: "snapshot_2", CollectionId: 100, S3Location: "s3://bucket/1002"},
		{Id: 1003, Name: "snapshot_3", CollectionId: 100, S3Location: "s3://bucket/1003"},
	}

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return snapshotInfos, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot - fail on second snapshot (s3://bucket/1002)
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		if metadataFilePath == "s3://bucket/1002" {
			return nil, errors.New("s3 read error")
		}
		// Find snapshot by S3Location
		for _, info := range snapshotInfos {
			if info.S3Location == metadataFilePath {
				return &SnapshotData{
					SnapshotInfo: &datapb.SnapshotInfo{Id: info.Id, CollectionId: info.CollectionId, S3Location: info.S3Location},
					Segments:     []*datapb.SegmentDescription{{SegmentId: info.Id * 10}},
					Indexes:      []*indexpb.IndexInfo{{IndexID: info.Id * 100}},
					SegmentIDs:   []int64{info.Id * 10},
					IndexIDs:     []int64{info.Id * 100},
				}, nil
			}
		}
		return nil, errors.New("snapshot not found")
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert - reload returns immediately without error (async loading)
	assert.NoError(t, err)

	// All snapshots should be inserted
	for _, info := range snapshotInfos {
		infoVal, exists := sm.snapshotID2Info.Get(info.Id)
		assert.True(t, exists, "snapshot %d should be inserted", info.Id)
		assert.Equal(t, info, infoVal)
	}

	// Manually trigger loading (simulates background goroutine)
	sm.loadUnloadedRefIndexes()

	// Verify successful snapshots are loaded
	refIndex1, _ := sm.snapshotID2RefIndex.Get(1001)
	assert.True(t, refIndex1.ContainsSegment(10010))

	refIndex3, _ := sm.snapshotID2RefIndex.Get(1003)
	assert.True(t, refIndex3.ContainsSegment(10030))

	// Verify failed snapshot is in Failed state
	refIndex2, _ := sm.snapshotID2RefIndex.Get(1002)
	assert.True(t, refIndex2.IsFailed())
	assert.False(t, refIndex2.ContainsSegment(10020))
}

func TestSnapshotMeta_Reload_EmptyList_WithMockey(t *testing.T) {
	// Test reload with empty snapshot list
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Mock catalog.ListSnapshots to return empty list
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{}, nil
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)
	// Verify maps are empty
	assert.Equal(t, 0, sm.snapshotID2Info.Len())
	assert.Equal(t, 0, sm.snapshotID2RefIndex.Len())
}

// --- Test Helper Functions ---

// setupSnapshotViaSaveSnapshot creates a snapshot using SaveSnapshot with mocked dependencies.
// This function mocks catalog.SaveSnapshot and SnapshotWriter.Save, executes SaveSnapshot,
// then unpatches the mocks so the actual test can set up its own mocks.
func setupSnapshotViaSaveSnapshot(t *testing.T, sm *snapshotMeta, snapshotData *SnapshotData) {
	ctx := context.Background()
	metadataFilePath := "s3://bucket/snapshots/test/metadata.json"

	// Mock catalog.SaveSnapshot to succeed
	mock1 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
		return nil
	}).Build()

	// Mock SnapshotWriter.Save to succeed
	mock2 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return metadataFilePath, nil
	}).Build()

	// Execute SaveSnapshot
	err := sm.SaveSnapshot(ctx, snapshotData)
	require.NoError(t, err)

	// Unpatch immediately so the actual test can set up its own mocks
	mock1.UnPatch()
	mock2.UnPatch()
}

// --- SaveSnapshot Tests (Mockey-based) ---

func TestSnapshotMeta_SaveSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotData := createTestSnapshotDataForMeta()
	metadataFilePath := "s3://bucket/snapshots/100/metadata/test-uuid.json"

	// Track catalog save calls for 2PC verification
	catalogSaveCalls := 0

	// Mock SnapshotWriter.Save (2PC uses snapshot ID for path computation)
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return metadataFilePath, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock catalog.SaveSnapshot - called twice in 2PC (PENDING then COMMITTED)
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		catalogSaveCalls++
		if catalogSaveCalls == 1 {
			// Phase 1: PENDING state
			assert.Equal(t, datapb.SnapshotState_SnapshotStatePending, snapshotInfo.State)
		} else {
			// Phase 2: COMMITTED state
			assert.Equal(t, datapb.SnapshotState_SnapshotStateCommitted, snapshotInfo.State)
			assert.Equal(t, metadataFilePath, snapshotInfo.S3Location)
		}
		return nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 2, catalogSaveCalls, "2PC should call catalog.SaveSnapshot twice")
	// Verify snapshot info was inserted
	info, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)
	assert.Equal(t, int64(100), info.CollectionId)
	// Verify refIndex was inserted
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsIndex(2001))
	// Verify S3Location was set
	assert.Equal(t, metadataFilePath, snapshotData.SnapshotInfo.S3Location)
}

func TestSnapshotMeta_SaveSnapshot_WriterError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotData := createTestSnapshotDataForMeta()
	expectedErr := errors.New("writer failed")

	// Mock catalog.SaveSnapshot for Phase 1 (PENDING)
	mock1 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Save to return error (S3 write fails after Phase 1)
	mock2 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return "", expectedErr
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert - error is wrapped with context
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "writer failed")
}

func TestSnapshotMeta_SaveSnapshot_CatalogPhase1Error_WithMockey(t *testing.T) {
	// Test: Phase 1 (PENDING) catalog save fails
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotData := createTestSnapshotDataForMeta()
	expectedErr := errors.New("catalog failed")

	// Mock catalog.SaveSnapshot to return error on Phase 1
	mock1 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		return expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert - error is wrapped
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "catalog failed")
}

func TestSnapshotMeta_SaveSnapshot_CatalogPhase2Error_WithMockey(t *testing.T) {
	// Test: Phase 2 (COMMITTED) catalog save fails after S3 write succeeds
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotData := createTestSnapshotDataForMeta()
	metadataFilePath := "s3://bucket/snapshots/100/metadata/test-uuid.json"
	expectedErr := errors.New("catalog phase 2 failed")
	catalogSaveCalls := 0

	// Mock catalog.SaveSnapshot - succeed on Phase 1, fail on Phase 2
	mock1 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		catalogSaveCalls++
		if catalogSaveCalls == 1 {
			return nil // Phase 1 succeeds
		}
		return expectedErr // Phase 2 fails
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Save to succeed
	mock2 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return metadataFilePath, nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert - error is wrapped, and snapshot is NOT in memory (rolled back)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "catalog phase 2 failed")
	_, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists, "snapshot should not be in memory after Phase 2 failure")
	_, exists = sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists, "refIndex should not be in memory after Phase 2 failure")
}

// --- DropSnapshot Tests (Mockey-based) ---

func TestSnapshotMeta_DropSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName

	// Setup snapshot via SaveSnapshot with mocked dependencies
	setupSnapshotViaSaveSnapshot(t, sm, snapshotData)

	// Mock catalog.SaveSnapshot (for marking as Deleting - two-phase delete)
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
		assert.Equal(t, datapb.SnapshotState_SnapshotStateDeleting, snapshot.GetState())
		return nil
	}).Build()
	defer mock0.UnPatch()

	// Mock catalog.DropSnapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, int64(1), snapshotID)
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Drop - now takes metadataFilePath instead of collectionID/snapshotID
	mock2 := mockey.Mock((*SnapshotWriter).Drop).To(func(ctx context.Context, metadataFilePath string) error {
		assert.Equal(t, snapshotData.SnapshotInfo.GetS3Location(), metadataFilePath)
		return nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.DropSnapshot(ctx, int64(100), snapshotName)

	// Assert
	assert.NoError(t, err)
	// Verify snapshot was removed from both maps and secondary indexes
	_, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
	_, exists = sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
	nameMap, nameMapExists := sm.snapshotName2ID.Get(int64(100))
	if nameMapExists {
		_, exists = nameMap.Get(snapshotName)
		assert.False(t, exists)
	}
}

func TestSnapshotMeta_DropSnapshot_NotFound_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotName := "nonexistent_snapshot"

	// Act
	err := sm.DropSnapshot(ctx, int64(100), snapshotName)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSnapshotMeta_DropSnapshot_CatalogDropError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName

	// Setup snapshot via SaveSnapshot with mocked dependencies
	setupSnapshotViaSaveSnapshot(t, sm, snapshotData)

	// Mock catalog.SaveSnapshot (for marking as Deleting - two-phase delete)
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
		return nil
	}).Build()
	defer mock0.UnPatch()

	// Mock SnapshotWriter.Drop to succeed (S3 deletion succeeds)
	mock2 := mockey.Mock((*SnapshotWriter).Drop).To(func(ctx context.Context, metadataFilePath string) error {
		return nil
	}).Build()
	defer mock2.UnPatch()

	// Mock catalog.DropSnapshot to return error (catalog cleanup fails after S3 deletion)
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		return errors.New("catalog drop failed")
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.DropSnapshot(ctx, int64(100), snapshotName)

	// Assert - Two-phase delete: if S3 deletion succeeds, operation returns success
	// even if catalog cleanup fails. GC will clean up the catalog record later.
	assert.NoError(t, err)
	// Verify snapshot was removed from memory (user sees deletion immediately)
	_, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
	_, exists = sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
}

func TestSnapshotMeta_DropSnapshot_WriterError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName
	writerErr := errors.New("writer drop failed")

	// Setup snapshot via SaveSnapshot with mocked dependencies
	setupSnapshotViaSaveSnapshot(t, sm, snapshotData)

	// Mock catalog.SaveSnapshot (for marking as Deleting - two-phase delete)
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
		assert.Equal(t, datapb.SnapshotState_SnapshotStateDeleting, snapshot.GetState())
		return nil
	}).Build()
	defer mock0.UnPatch()

	// Mock SnapshotWriter.Drop to return error - now takes metadataFilePath
	// Two-phase delete: S3 error should NOT fail the operation, GC will retry
	mock2 := mockey.Mock((*SnapshotWriter).Drop).To(func(ctx context.Context, metadataFilePath string) error {
		return writerErr
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.DropSnapshot(ctx, int64(100), snapshotName)

	// Assert - Two-phase delete: S3 failure returns success, GC will clean up
	assert.NoError(t, err)
	// Verify snapshot was removed from memory (user sees deletion immediately)
	_, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
	_, exists = sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
}

func TestSnapshotMeta_DropSnapshot_MarkDeletingError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName
	expectedErr := errors.New("catalog save failed")

	// Setup snapshot via SaveSnapshot with mocked dependencies
	setupSnapshotViaSaveSnapshot(t, sm, snapshotData)

	// Mock catalog.SaveSnapshot to return error (marking as Deleting fails)
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
		return expectedErr
	}).Build()
	defer mock0.UnPatch()

	// Act
	err := sm.DropSnapshot(ctx, int64(100), snapshotName)

	// Assert - If marking as Deleting fails, operation should fail
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	// Verify snapshot is still in memory (operation failed before removal)
	_, exists := sm.snapshotID2Info.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)
	_, exists = sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)
}

// --- SnapshotRefIndex Tests ---

func TestSnapshotRefIndex_NewLoaded(t *testing.T) {
	// Test NewLoadedSnapshotRefIndex creates a pre-loaded refIndex
	refIndex := NewLoadedSnapshotRefIndex([]int64{1001, 1002}, []int64{2001, 2002}, []int64{3001, 3002})

	// Should not block (already loaded)
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsSegment(1002))
	assert.False(t, refIndex.ContainsSegment(1003))
	assert.True(t, refIndex.ContainsIndex(2001))
	assert.True(t, refIndex.ContainsIndex(2002))
	assert.False(t, refIndex.ContainsIndex(2003))
	assert.True(t, refIndex.ContainsBuildID(3001))
	assert.True(t, refIndex.ContainsBuildID(3002))
	assert.False(t, refIndex.ContainsBuildID(3003))
}

func TestSnapshotRefIndex_SetLoaded(t *testing.T) {
	// Test SetLoaded method
	refIndex := NewSnapshotRefIndex()

	// Initially empty
	assert.False(t, refIndex.ContainsSegment(1001))
	assert.False(t, refIndex.ContainsIndex(2001))

	// Set loaded data
	refIndex.SetLoaded([]int64{1001, 1002}, []int64{2001}, []int64{3001})

	// Should contain the loaded IDs
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsSegment(1002))
	assert.False(t, refIndex.ContainsSegment(1003))
	assert.True(t, refIndex.ContainsIndex(2001))
	assert.False(t, refIndex.ContainsIndex(2002))
	assert.True(t, refIndex.ContainsBuildID(3001))
	assert.False(t, refIndex.ContainsBuildID(3002))
}

func TestSnapshotRefIndex_EmptySets(t *testing.T) {
	// Test refIndex with empty/nil sets (e.g., after load failure)
	refIndex := NewLoadedSnapshotRefIndex(nil, nil, nil)

	// Should not block and should return false for all queries
	assert.False(t, refIndex.ContainsSegment(1001))
	assert.False(t, refIndex.ContainsIndex(2001))
	assert.False(t, refIndex.ContainsBuildID(3001))
}

// --- GetSnapshotByBuildID Tests ---

func TestSnapshotMeta_GetSnapshotByBuildID(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// Insert snapshot with buildIDs
	info1 := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "snap1",
	}
	sm.snapshotID2Info.Insert(info1.GetId(), info1)
	sm.snapshotID2RefIndex.Insert(info1.GetId(), NewLoadedSnapshotRefIndex(
		[]int64{1001}, []int64{2001}, []int64{3001, 3002}))
	sm.addToSecondaryIndexes(info1)

	// Insert another snapshot with different buildIDs
	info2 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 200,
		Name:         "snap2",
	}
	sm.snapshotID2Info.Insert(info2.GetId(), info2)
	sm.snapshotID2RefIndex.Insert(info2.GetId(), NewLoadedSnapshotRefIndex(
		[]int64{1002}, []int64{2002}, []int64{3003}))
	sm.addToSecondaryIndexes(info2)

	// Found in first snapshot
	assert.Equal(t, []UniqueID{1}, sm.GetSnapshotByBuildID(3001))
	assert.Equal(t, []UniqueID{1}, sm.GetSnapshotByBuildID(3002))
	// Found in second snapshot
	assert.Equal(t, []UniqueID{2}, sm.GetSnapshotByBuildID(3003))
	// Not found in any snapshot
	assert.Empty(t, sm.GetSnapshotByBuildID(9999))
}

func TestSnapshotMeta_GetSnapshotByBuildID_EmptySnapshots(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)
	// No snapshots at all
	assert.Empty(t, sm.GetSnapshotByBuildID(3001))
}

func TestSnapshotMeta_GetSnapshotByBuildID_NilBuildIDs(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// Snapshot with nil buildIDs (backward compatibility)
	info := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "old_snap",
	}
	sm.snapshotID2Info.Insert(info.GetId(), info)
	sm.snapshotID2RefIndex.Insert(info.GetId(), NewLoadedSnapshotRefIndex(
		[]int64{1001}, []int64{2001}, nil))
	sm.addToSecondaryIndexes(info)

	assert.Empty(t, sm.GetSnapshotByBuildID(3001))
}

// --- Concurrent Operations Tests ---

func TestSnapshotMeta_ConcurrentOperations(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Test concurrent map operations don't panic
	snapshotData := createTestSnapshotDataForMeta()

	// Act - simulate concurrent operations
	insertTestSnapshot(sm, snapshotData.SnapshotInfo, []int64{1001}, []int64{2001})

	// These operations should work concurrently
	snapshots, err := sm.ListSnapshots(ctx, 0, 0)
	segmentSnapshots := sm.GetSnapshotBySegment(ctx, 100, 1001)
	indexSnapshots := sm.GetSnapshotByIndex(ctx, 100, 2001)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 1)
	assert.Len(t, segmentSnapshots, 1)
	assert.Len(t, indexSnapshots, 1)
}

func TestSnapshotMeta_EmptyMaps(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Act
	snapshots, err := sm.ListSnapshots(ctx, 100, 1)
	segmentSnapshots := sm.GetSnapshotBySegment(ctx, 100, 1001)
	indexSnapshots := sm.GetSnapshotByIndex(ctx, 100, 2001)
	snapshot, getErr := sm.GetSnapshot(ctx, int64(100), "nonexistent")

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 0)
	assert.Len(t, segmentSnapshots, 0)
	assert.Len(t, indexSnapshots, 0)
	assert.Error(t, getErr)
	assert.Nil(t, snapshot)
}

// --- Edge Case Tests for Filtering Logic ---

func TestSnapshotMeta_ListSnapshots_FilterByPartition(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := int64(100)
	partitionID := int64(2)

	snapshot1 := createTestSnapshotDataForMeta()
	snapshot1.SnapshotInfo.Name = "snapshot1"
	snapshot1.SnapshotInfo.Id = 1
	snapshot1.SnapshotInfo.CollectionId = 100
	snapshot1.SnapshotInfo.PartitionIds = []int64{1, 3} // No partition 2

	snapshot2 := createTestSnapshotDataForMeta()
	snapshot2.SnapshotInfo.Name = "snapshot2"
	snapshot2.SnapshotInfo.Id = 2
	snapshot2.SnapshotInfo.CollectionId = 100
	snapshot2.SnapshotInfo.PartitionIds = []int64{2, 4} // Has partition 2

	sm := createTestSnapshotMetaLoaded(t)
	cleanup := saveTestSnapshots(t, sm, snapshot1, snapshot2)
	defer cleanup()

	// Act
	snapshots, err := sm.ListSnapshots(ctx, collectionID, partitionID)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 1)
	assert.Contains(t, snapshots, "snapshot2")
}

func TestSnapshotMeta_ListSnapshots_FilterByCollection(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := int64(200)
	partitionID := int64(0) // All partitions

	snapshot1 := createTestSnapshotDataForMeta()
	snapshot1.SnapshotInfo.Name = "snapshot1"
	snapshot1.SnapshotInfo.Id = 1
	snapshot1.SnapshotInfo.CollectionId = 100

	snapshot2 := createTestSnapshotDataForMeta()
	snapshot2.SnapshotInfo.Name = "snapshot2"
	snapshot2.SnapshotInfo.Id = 2
	snapshot2.SnapshotInfo.CollectionId = 200

	sm := createTestSnapshotMetaLoaded(t)
	cleanup := saveTestSnapshots(t, sm, snapshot1, snapshot2)
	defer cleanup()

	// Act
	snapshots, err := sm.ListSnapshots(ctx, collectionID, partitionID)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 1)
	assert.Contains(t, snapshots, "snapshot2")
}

// --- Reload Legacy Format Tests ---

func TestSnapshotMeta_Reload_LegacyFormat_NoPrecomputedIDs(t *testing.T) {
	// Test reload with legacy snapshots that don't have pre-computed IDs
	// In current implementation, SegmentIDs/IndexIDs will be empty for legacy snapshots
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Create snapshot without pre-computed IDs (legacy format)
	snapshotInfo := &datapb.SnapshotInfo{
		Id:           1001,
		Name:         "legacy_snapshot",
		CollectionId: 100,
		S3Location:   "s3://bucket/snapshots/1001/metadata.json",
	}

	// Legacy snapshot data without pre-computed IDs
	legacySnapshotData := &SnapshotData{
		SnapshotInfo: snapshotInfo,
		Segments:     nil, // Not populated when includeSegments=false
		Indexes:      nil,
		SegmentIDs:   nil, // Empty - legacy format
		IndexIDs:     nil, // Empty - legacy format
	}

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{snapshotInfo}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot - only called once with metadataFilePath
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		assert.Equal(t, snapshotInfo.S3Location, metadataFilePath)
		assert.False(t, includeSegments) // Should only read metadata, not full segments
		return legacySnapshotData, nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)

	// Verify snapshot info was inserted immediately
	info, exists := sm.snapshotID2Info.Get(snapshotInfo.Id)
	assert.True(t, exists)
	assert.Equal(t, snapshotInfo, info)

	// Verify refIndex exists (in Pending state)
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotInfo.Id)
	assert.True(t, exists)

	// Manually trigger loading (simulates background goroutine)
	sm.loadUnloadedRefIndexes()

	// Legacy snapshots will have empty ID sets
	assert.False(t, refIndex.ContainsSegment(1001))
	assert.False(t, refIndex.ContainsIndex(2001))
}

func TestSnapshotMeta_Reload_MixedFormats(t *testing.T) {
	// Test reload with mixed snapshots: some with pre-computed IDs, some without
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Snapshot 1: new format with pre-computed IDs
	newFormatInfo := &datapb.SnapshotInfo{
		Id:           1001,
		Name:         "new_format_snapshot",
		CollectionId: 100,
		S3Location:   "s3://bucket/snapshots/1001/metadata.json",
	}

	// Snapshot 2: legacy format without pre-computed IDs
	legacyFormatInfo := &datapb.SnapshotInfo{
		Id:           1002,
		Name:         "legacy_format_snapshot",
		CollectionId: 100,
		S3Location:   "s3://bucket/snapshots/1002/metadata.json",
	}

	snapshotInfos := []*datapb.SnapshotInfo{newFormatInfo, legacyFormatInfo}

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return snapshotInfos, nil
	}).Build()
	defer mock1.UnPatch()

	// Track calls for each snapshot by S3Location using sync.Map for concurrent safety
	var callCounts sync.Map
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		// Increment call count atomically
		val, _ := callCounts.LoadOrStore(metadataFilePath, new(int32))
		count := val.(*int32)
		*count++

		if metadataFilePath == newFormatInfo.S3Location {
			// New format: return pre-computed IDs with deep copy
			return &SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{
					Id:           newFormatInfo.Id,
					Name:         newFormatInfo.Name,
					CollectionId: newFormatInfo.CollectionId,
					S3Location:   newFormatInfo.S3Location,
				},
				SegmentIDs: []int64{10010, 10011},
				IndexIDs:   []int64{100100},
			}, nil
		}

		// Legacy format: no pre-computed IDs with deep copy
		return &SnapshotData{
			SnapshotInfo: &datapb.SnapshotInfo{
				Id:           legacyFormatInfo.Id,
				Name:         legacyFormatInfo.Name,
				CollectionId: legacyFormatInfo.CollectionId,
				S3Location:   legacyFormatInfo.S3Location,
			},
			SegmentIDs: nil, // Empty - legacy format
			IndexIDs:   nil, // Empty - legacy format
		}, nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)

	// Verify snapshots are inserted immediately
	_, exists := sm.snapshotID2Info.Get(1001)
	assert.True(t, exists)
	_, exists = sm.snapshotID2Info.Get(1002)
	assert.True(t, exists)

	// Manually trigger loading (simulates background goroutine)
	sm.loadUnloadedRefIndexes()

	// Verify each snapshot was read exactly once (no slow path fallback)
	newRefIndex, _ := sm.snapshotID2RefIndex.Get(1001)
	legacyRefIndex, _ := sm.snapshotID2RefIndex.Get(1002)

	// Verify new format snapshot has pre-computed IDs
	assert.True(t, newRefIndex.ContainsSegment(10010))
	assert.True(t, newRefIndex.ContainsSegment(10011))
	assert.True(t, newRefIndex.ContainsIndex(100100))

	// Verify legacy format snapshot has empty ID sets
	assert.False(t, legacyRefIndex.ContainsSegment(10020))
	assert.False(t, legacyRefIndex.ContainsIndex(100200))

	// Verify call counts
	newCount, _ := callCounts.Load(newFormatInfo.S3Location)
	legacyCount, _ := callCounts.Load(legacyFormatInfo.S3Location)
	assert.Equal(t, int32(1), *newCount.(*int32))
	assert.Equal(t, int32(1), *legacyCount.(*int32))
}

// --- Two-Phase Delete Tests ---

func TestSnapshotMeta_Reload_SkipPendingAndDeletingState_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Create snapshots with different states
	committedSnapshot := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "committed_snapshot",
		S3Location:   "s3://bucket/committed",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
	}
	pendingSnapshot := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 100,
		Name:         "pending_snapshot",
		S3Location:   "s3://bucket/pending",
		State:        datapb.SnapshotState_SnapshotStatePending,
	}
	deletingSnapshot := &datapb.SnapshotInfo{
		Id:           3,
		CollectionId: 100,
		Name:         "deleting_snapshot",
		S3Location:   "s3://bucket/deleting",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
	}

	// Mock catalog.ListSnapshots - return all three snapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{committedSnapshot, pendingSnapshot, deletingSnapshot}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot - should only be called for committed snapshot
	readCalled := false
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		assert.Equal(t, committedSnapshot.S3Location, metadataFilePath)
		readCalled = true
		return &SnapshotData{
			SnapshotInfo: committedSnapshot,
			SegmentIDs:   []int64{1001},
			IndexIDs:     []int64{2001},
		}, nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)

	// Verify only committed snapshot was inserted (pending/deleting skipped)
	_, exists := sm.snapshotID2Info.Get(committedSnapshot.Id)
	assert.True(t, exists, "Committed snapshot should be loaded")

	_, exists = sm.snapshotID2Info.Get(pendingSnapshot.Id)
	assert.False(t, exists, "Pending snapshot should be skipped")

	_, exists = sm.snapshotID2Info.Get(deletingSnapshot.Id)
	assert.False(t, exists, "Deleting snapshot should be skipped")

	// Manually trigger loading (simulates background goroutine)
	sm.loadUnloadedRefIndexes()

	// Verify refIndex for committed snapshot exists and can be accessed
	refIndex, exists := sm.snapshotID2RefIndex.Get(committedSnapshot.Id)
	assert.True(t, exists)
	// Verify data is loaded
	assert.True(t, refIndex.ContainsSegment(1001))

	// Verify ReadSnapshot was called for committed snapshot only
	assert.True(t, readCalled, "ReadSnapshot should be called for committed snapshot")
}

func TestSnapshotMeta_GetDeletingSnapshots_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Create snapshots with different states
	committedSnapshot := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "committed_snapshot",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
	}
	deletingSnapshot1 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 100,
		Name:         "deleting_snapshot_1",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
	}
	deletingSnapshot2 := &datapb.SnapshotInfo{
		Id:           3,
		CollectionId: 200,
		Name:         "deleting_snapshot_2",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
	}
	pendingSnapshot := &datapb.SnapshotInfo{
		Id:           4,
		CollectionId: 100,
		Name:         "pending_snapshot",
		State:        datapb.SnapshotState_SnapshotStatePending,
	}

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{committedSnapshot, deletingSnapshot1, deletingSnapshot2, pendingSnapshot}, nil
	}).Build()
	defer mock1.UnPatch()

	// Act
	deletingSnapshots, err := sm.GetDeletingSnapshots(ctx)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, deletingSnapshots, 2)

	// Verify only Deleting state snapshots are returned
	ids := make([]int64, len(deletingSnapshots))
	for i, s := range deletingSnapshots {
		ids[i] = s.Id
	}
	assert.Contains(t, ids, deletingSnapshot1.Id)
	assert.Contains(t, ids, deletingSnapshot2.Id)
}

func TestSnapshotMeta_GetDeletingSnapshots_CatalogError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	expectedErr := errors.New("catalog error")

	// Mock catalog.ListSnapshots to return error
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return nil, expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	deletingSnapshots, err := sm.GetDeletingSnapshots(ctx)

	// Assert
	assert.Error(t, err)
	assert.ErrorIs(t, err, expectedErr) // Use ErrorIs to check wrapped error
	assert.Nil(t, deletingSnapshots)
}

func TestSnapshotMeta_CleanupDeletingSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshot := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "deleting_snapshot",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
	}

	dropCalled := false
	// Mock catalog.DropSnapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		assert.Equal(t, snapshot.CollectionId, collectionID)
		assert.Equal(t, snapshot.Id, snapshotID)
		dropCalled = true
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.CleanupDeletingSnapshot(ctx, snapshot)

	// Assert
	assert.NoError(t, err)
	assert.True(t, dropCalled, "catalog.DropSnapshot should be called")
}

func TestSnapshotMeta_CleanupDeletingSnapshot_CatalogError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	snapshot := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "deleting_snapshot",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
	}
	expectedErr := errors.New("catalog drop failed")

	// Mock catalog.DropSnapshot to return error
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		return expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.CleanupDeletingSnapshot(ctx, snapshot)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotRefIndex_SetFailed(t *testing.T) {
	// Test SetFailed marks RefIndex as failed
	refIndex := NewSnapshotRefIndex()

	// Initially not failed
	assert.False(t, refIndex.IsFailed())

	// Mark as failed
	refIndex.SetFailed()

	// Should now be failed
	assert.True(t, refIndex.IsFailed())
	assert.False(t, refIndex.IsLoaded())

	// ContainsSegment/Index should return false for failed state
	assert.False(t, refIndex.ContainsSegment(1001))
	assert.False(t, refIndex.ContainsIndex(2001))
}

func TestSnapshotRefIndex_TransitionFromFailedToLoaded(t *testing.T) {
	// Test that SetLoaded can recover from failed state
	refIndex := NewSnapshotRefIndex()

	// Set as failed first
	refIndex.SetFailed()
	assert.True(t, refIndex.IsFailed())

	// Now set as loaded with data
	refIndex.SetLoaded([]int64{1001, 1002}, []int64{2001}, []int64{3001})

	// Should now be loaded, not failed
	assert.False(t, refIndex.IsFailed())
	assert.True(t, refIndex.IsLoaded())
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsIndex(2001))
	assert.True(t, refIndex.ContainsBuildID(3001))
}

func TestSnapshotMeta_Reload_PartialFailure_SetsFailed(t *testing.T) {
	// Test that loadUnloadedRefIndexes sets RefIndex to failed state (not nil) when S3 read fails
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	snapshotInfo := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "test_snapshot",
		S3Location:   "s3://bucket/test",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
	}

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{snapshotInfo}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot to always fail
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(sr *SnapshotReader, ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		return nil, errors.New("S3 read failed")
	}).Build()
	defer mock2.UnPatch()

	// Trigger reload
	err := sm.reload(ctx)
	assert.NoError(t, err)

	// Manually trigger loading (simulates background goroutine - should fail)
	sm.loadUnloadedRefIndexes()

	// Verify RefIndex is in failed state
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotInfo.Id)
	assert.True(t, exists)
	assert.True(t, refIndex.IsFailed())
	assert.False(t, refIndex.IsLoaded())
}

func TestSnapshotMeta_IsRefIndexLoadedForCollection_NoSnapshots(t *testing.T) {
	// Test that IsRefIndexLoadedForCollection returns true when collection has no snapshots
	sm := createTestSnapshotMetaLoaded(t)

	// Collection 999 has no snapshots
	assert.True(t, sm.IsRefIndexLoadedForCollection(999))
}

func TestSnapshotMeta_IsRefIndexLoadedForCollection_AllLoaded(t *testing.T) {
	// Test that IsRefIndexLoadedForCollection returns true when all RefIndexes for collection are loaded
	sm := createTestSnapshotMetaLoaded(t)

	collectionID := int64(100)

	// Add snapshots with loaded RefIndexes for collection 100
	for i := int64(1); i <= 3; i++ {
		snapshotInfo := &datapb.SnapshotInfo{
			Id:           i,
			CollectionId: collectionID,
			Name:         fmt.Sprintf("test_snapshot_%d", i),
		}
		sm.snapshotID2Info.Insert(snapshotInfo.Id, snapshotInfo)
		sm.snapshotID2RefIndex.Insert(snapshotInfo.Id, NewLoadedSnapshotRefIndex([]int64{i * 1000}, []int64{i * 2000}, nil))
		sm.addToSecondaryIndexes(snapshotInfo)
	}

	// IsRefIndexLoadedForCollection should return true
	assert.True(t, sm.IsRefIndexLoadedForCollection(collectionID))
}

func TestSnapshotMeta_IsRefIndexLoadedForCollection_HasFailed(t *testing.T) {
	// Test that IsRefIndexLoadedForCollection returns false when collection has failed RefIndex
	sm := createTestSnapshotMetaLoaded(t)

	collectionID := int64(100)

	// Add a loaded RefIndex
	snapshot1 := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: collectionID,
		Name:         "snapshot1",
	}
	sm.snapshotID2Info.Insert(snapshot1.Id, snapshot1)
	sm.snapshotID2RefIndex.Insert(snapshot1.Id, NewLoadedSnapshotRefIndex([]int64{1001}, nil, nil))
	sm.addToSecondaryIndexes(snapshot1)

	// Add a failed RefIndex for same collection
	snapshot2 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: collectionID,
		Name:         "snapshot2",
	}
	failedRefIndex := NewSnapshotRefIndex()
	failedRefIndex.SetFailed()
	sm.snapshotID2Info.Insert(snapshot2.Id, snapshot2)
	sm.snapshotID2RefIndex.Insert(snapshot2.Id, failedRefIndex)
	sm.addToSecondaryIndexes(snapshot2)

	// IsRefIndexLoadedForCollection should return false
	assert.False(t, sm.IsRefIndexLoadedForCollection(collectionID))
}

func TestSnapshotMeta_IsRefIndexLoadedForCollection_OtherCollectionFailed(t *testing.T) {
	// Test that IsRefIndexLoadedForCollection returns true even if other collection has failed RefIndex
	sm := createTestSnapshotMetaLoaded(t)

	// Add loaded RefIndex for collection 100
	snapshot1 := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "snapshot1",
	}
	sm.snapshotID2Info.Insert(snapshot1.Id, snapshot1)
	sm.snapshotID2RefIndex.Insert(snapshot1.Id, NewLoadedSnapshotRefIndex([]int64{1001}, nil, nil))
	sm.addToSecondaryIndexes(snapshot1)

	// Add failed RefIndex for collection 200
	snapshot2 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 200,
		Name:         "snapshot2",
	}
	failedRefIndex := NewSnapshotRefIndex()
	failedRefIndex.SetFailed()
	sm.snapshotID2Info.Insert(snapshot2.Id, snapshot2)
	sm.snapshotID2RefIndex.Insert(snapshot2.Id, failedRefIndex)
	sm.addToSecondaryIndexes(snapshot2)

	// Collection 100 should return true (its RefIndex is loaded)
	assert.True(t, sm.IsRefIndexLoadedForCollection(100))

	// Collection 200 should return false (its RefIndex is failed)
	assert.False(t, sm.IsRefIndexLoadedForCollection(200))
}

func TestSnapshotMeta_IsRefIndexLoadedForCollection_MissingRefIndexWithSnapshotInfo(t *testing.T) {
	// Safety: if snapshotInfo exists but refIndex is missing, treat as not loaded to avoid unsafe GC.
	sm := createTestSnapshotMetaLoaded(t)

	collectionID := int64(100)
	snapshot := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: collectionID,
		Name:         "snapshot_missing_refindex",
	}
	sm.snapshotID2Info.Insert(snapshot.Id, snapshot)
	// Intentionally do NOT insert snapshotID2RefIndex.
	sm.addToSecondaryIndexes(snapshot)

	assert.False(t, sm.IsRefIndexLoadedForCollection(collectionID))
}

func TestSnapshotMeta_IsRefIndexLoadedForCollection_MissingRefIndexAndSnapshotInfo(t *testing.T) {
	// If snapshotInfo is already deleted/cleaned, a stale snapshotID should not block GC forever.
	sm := createTestSnapshotMetaLoaded(t)

	collectionID := int64(100)
	staleSnapshotID := int64(1)
	set := typeutil.NewUniqueSet(staleSnapshotID)
	sm.collectionID2Snapshots.Insert(collectionID, set)
	// No snapshotID2Info and no snapshotID2RefIndex for staleSnapshotID.

	assert.True(t, sm.IsRefIndexLoadedForCollection(collectionID))
}

func TestSnapshotMeta_GetSnapshotBySegment_AllCollections_Found(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	info1 := createTestSnapshotInfoForMeta()
	info1.CollectionId = 100
	info1.Id = 1
	insertTestSnapshot(sm, info1, []int64{1001, 1002}, []int64{2001})

	info2 := createTestSnapshotInfoForMeta()
	info2.CollectionId = 200
	info2.Id = 2
	insertTestSnapshot(sm, info2, []int64{1003, 1004}, []int64{2002})

	snapshotIDs := sm.GetSnapshotBySegment(ctx, -1, 1001)
	assert.Len(t, snapshotIDs, 1)
	assert.Contains(t, snapshotIDs, UniqueID(1))
}

func TestSnapshotMeta_GetSnapshotBySegment_AllCollections_NotFound(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	info1 := createTestSnapshotInfoForMeta()
	info1.CollectionId = 100
	info1.Id = 1
	insertTestSnapshot(sm, info1, []int64{1001, 1002}, []int64{2001})

	snapshotIDs := sm.GetSnapshotBySegment(ctx, -1, 9999)
	assert.Len(t, snapshotIDs, 0)
}

func TestSnapshotMeta_GetSnapshotBySegment_AllCollections_NoSnapshots(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snapshotIDs := sm.GetSnapshotBySegment(ctx, -1, 1001)
	assert.Len(t, snapshotIDs, 0)
}

func TestSnapshotMeta_GetSnapshotBySegment_AllCollections_CrossCollection(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Segment 1001 is in collection 200, not collection 100
	info1 := createTestSnapshotInfoForMeta()
	info1.CollectionId = 100
	info1.Id = 1
	insertTestSnapshot(sm, info1, []int64{2001, 2002}, []int64{3001})

	info2 := createTestSnapshotInfoForMeta()
	info2.CollectionId = 200
	info2.Id = 2
	insertTestSnapshot(sm, info2, []int64{1001, 1002}, []int64{3002})

	// Should find segment 1001 even though it's in collection 200
	snapshotIDs := sm.GetSnapshotBySegment(ctx, -1, 1001)
	assert.Len(t, snapshotIDs, 1)
	assert.Contains(t, snapshotIDs, UniqueID(2))
}

func TestSnapshotMeta_IsAllRefIndexLoaded(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// No snapshots — should be considered loaded
	assert.True(t, sm.IsAllRefIndexLoaded())

	// Add a loaded snapshot
	info1 := createTestSnapshotInfoForMeta()
	info1.CollectionId = 100
	info1.Id = 1
	insertTestSnapshot(sm, info1, []int64{1001}, []int64{2001})
	assert.True(t, sm.IsAllRefIndexLoaded())

	// Add a not-loaded snapshot
	info2 := createTestSnapshotInfoForMeta()
	info2.CollectionId = 200
	info2.Id = 2
	sm.snapshotID2Info.Insert(info2.GetId(), info2)
	sm.snapshotID2RefIndex.Insert(info2.GetId(), NewSnapshotRefIndex())
	assert.False(t, sm.IsAllRefIndexLoaded())
}

// --- DropSnapshotsByCollection Tests ---

func TestSnapshotMeta_DropSnapshotsByCollection_DeletesAllForCollection(t *testing.T) {
	// Should delete all snapshots for a collection while leaving other collections untouched.
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Create snapshots for collection 100
	snap1 := createTestSnapshotDataForMeta()
	snap1.SnapshotInfo.Id = 1
	snap1.SnapshotInfo.Name = "snap_a"
	snap1.SnapshotInfo.CollectionId = 100

	snap2 := createTestSnapshotDataForMeta()
	snap2.SnapshotInfo.Id = 2
	snap2.SnapshotInfo.Name = "snap_b"
	snap2.SnapshotInfo.CollectionId = 100

	// Create snapshot for collection 200
	snap3 := createTestSnapshotDataForMeta()
	snap3.SnapshotInfo.Id = 3
	snap3.SnapshotInfo.Name = "snap_c"
	snap3.SnapshotInfo.CollectionId = 200

	// Save all snapshots
	cleanup := saveTestSnapshots(t, sm, snap1, snap2, snap3)
	// Must cleanup before re-mocking SaveSnapshot below
	cleanup()

	// Mock catalog.SaveSnapshot (for marking as Deleting)
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mock0.UnPatch()

	// Mock catalog.DropSnapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Drop
	mock2 := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.DropSnapshotsByCollection(ctx, int64(100))

	// Assert
	assert.NoError(t, err)

	// Verify collection 100 snapshots are gone
	_, exists := sm.snapshotID2Info.Get(int64(1))
	assert.False(t, exists, "snapshot 1 should be deleted")
	_, exists = sm.snapshotID2Info.Get(int64(2))
	assert.False(t, exists, "snapshot 2 should be deleted")

	// Verify collection 100 is gone from secondary indexes
	names100, err := sm.ListSnapshots(ctx, int64(100), int64(0))
	assert.NoError(t, err)
	assert.Len(t, names100, 0, "collection 100 should have no snapshots")

	// Verify collection 200 snapshot remains
	info3, exists := sm.snapshotID2Info.Get(int64(3))
	assert.True(t, exists, "snapshot 3 should remain")
	assert.Equal(t, "snap_c", info3.GetName())

	names200, err := sm.ListSnapshots(ctx, int64(200), int64(0))
	assert.NoError(t, err)
	assert.Len(t, names200, 1)
	assert.Contains(t, names200, "snap_c")
}

func TestSnapshotMeta_DropSnapshotsByCollection_NoopForNonexistentCollection(t *testing.T) {
	// Should be no-op for collection with no snapshots.
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Act - drop snapshots for collection that has no snapshots
	err := sm.DropSnapshotsByCollection(ctx, int64(999))

	// Assert
	assert.NoError(t, err)
}

// --- Per-Collection Name Uniqueness Tests ---

func TestSnapshotMeta_SameNameDifferentCollections_BothSucceed(t *testing.T) {
	// Same snapshot name in different collections should both succeed.
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snap1 := createTestSnapshotDataForMeta()
	snap1.SnapshotInfo.Id = 1
	snap1.SnapshotInfo.Name = "my_snapshot"
	snap1.SnapshotInfo.CollectionId = 100

	snap2 := createTestSnapshotDataForMeta()
	snap2.SnapshotInfo.Id = 2
	snap2.SnapshotInfo.Name = "my_snapshot"
	snap2.SnapshotInfo.CollectionId = 200

	cleanup := saveTestSnapshots(t, sm, snap1, snap2)
	defer cleanup()

	// Assert - both snapshots exist
	info1, err := sm.GetSnapshot(ctx, int64(100), "my_snapshot")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info1.GetId())
	assert.Equal(t, int64(100), info1.GetCollectionId())

	info2, err := sm.GetSnapshot(ctx, int64(200), "my_snapshot")
	assert.NoError(t, err)
	assert.Equal(t, int64(2), info2.GetId())
	assert.Equal(t, int64(200), info2.GetCollectionId())

	// Both collections should list the snapshot
	names100, err := sm.ListSnapshots(ctx, int64(100), int64(0))
	assert.NoError(t, err)
	assert.Len(t, names100, 1)
	assert.Contains(t, names100, "my_snapshot")

	names200, err := sm.ListSnapshots(ctx, int64(200), int64(0))
	assert.NoError(t, err)
	assert.Len(t, names200, 1)
	assert.Contains(t, names200, "my_snapshot")
}

func TestSnapshotMeta_SameNameSameCollection_SecondFails(t *testing.T) {
	// Same snapshot name in same collection should fail on the second one.
	// The uniqueness check happens in GetSnapshot - if it returns non-nil, the name already exists.
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snap1 := createTestSnapshotDataForMeta()
	snap1.SnapshotInfo.Id = 1
	snap1.SnapshotInfo.Name = "duplicate_name"
	snap1.SnapshotInfo.CollectionId = 100

	cleanup := saveTestSnapshots(t, sm, snap1)
	defer cleanup()

	// Act - check name uniqueness using GetSnapshot (as the manager does)
	existingInfo, err := sm.GetSnapshot(ctx, int64(100), "duplicate_name")

	// Assert - name already exists in collection 100
	assert.NoError(t, err)
	assert.NotNil(t, existingInfo)
	assert.Equal(t, int64(1), existingInfo.GetId())

	// Verify the name does NOT exist in a different collection
	_, err = sm.GetSnapshot(ctx, int64(200), "duplicate_name")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSnapshotMeta_DropSnapshotsByCollection_PartialFailureReturnsError(t *testing.T) {
	// DropSnapshotsByCollection tries to delete all snapshots for a collection.
	// This test verifies that:
	// 1. Successfully deleted snapshots are cleaned up
	// 2. Failed snapshots remain in the index
	// 3. Return value is an aggregated error for partial failures
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snap1 := createTestSnapshotDataForMeta()
	snap1.SnapshotInfo.Id = 1
	snap1.SnapshotInfo.Name = "snap_ok"
	snap1.SnapshotInfo.CollectionId = 100

	snap2 := createTestSnapshotDataForMeta()
	snap2.SnapshotInfo.Id = 2
	snap2.SnapshotInfo.Name = "snap_fail"
	snap2.SnapshotInfo.CollectionId = 100

	cleanup := saveTestSnapshots(t, sm, snap1, snap2)
	cleanup()

	// Mock catalog.SaveSnapshot to succeed (for marking as Deleting)
	callCount := 0
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(_ *kv_datacoord.Catalog, _ context.Context, info *datapb.SnapshotInfo) error {
		callCount++
		if info.GetName() == "snap_fail" {
			return fmt.Errorf("catalog save error")
		}
		return nil
	}).Build()
	defer mockSave.UnPatch()

	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()

	mockWriter := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriter.UnPatch()

	// Act
	err := sm.DropSnapshotsByCollection(ctx, int64(100))

	// Assert: returns aggregated error for partial failure
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to drop 1/2 snapshots for collection 100")

	// snap_ok should be deleted
	_, exists := sm.snapshotID2Info.Get(int64(1))
	assert.False(t, exists, "snap_ok should be deleted")

	// snap_fail should remain (catalog save failed, DropSnapshot returned error)
	_, exists = sm.snapshotID2Info.Get(int64(2))
	assert.True(t, exists, "snap_fail should remain after partial failure")
}

// --- Per-Collection Snapshot Isolation Tests ---

func TestSnapshotMeta_SameNameDifferentCollections(t *testing.T) {
	// Arrange: two collections (100, 200) each have a snapshot named "backup"
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snap1 := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "backup",
		CreateTs:     1000,
		S3Location:   "s3://bucket/100/1",
		PartitionIds: []int64{10},
	}
	snap2 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 200,
		Name:         "backup",
		CreateTs:     2000,
		S3Location:   "s3://bucket/200/2",
		PartitionIds: []int64{20},
	}
	// Also add a second snapshot for collection 100
	snap3 := &datapb.SnapshotInfo{
		Id:           3,
		CollectionId: 100,
		Name:         "daily",
		CreateTs:     3000,
		S3Location:   "s3://bucket/100/3",
		PartitionIds: []int64{10},
	}

	insertTestSnapshot(sm, snap1, []int64{1001}, []int64{2001})
	insertTestSnapshot(sm, snap2, []int64{1002}, []int64{2002})
	insertTestSnapshot(sm, snap3, []int64{1003}, []int64{2003})

	// Verify GetSnapshot for collection 100 returns collection 100's snapshot
	info1, err := sm.GetSnapshot(ctx, 100, "backup")
	require.NoError(t, err)
	assert.Equal(t, int64(1), info1.GetId())
	assert.Equal(t, int64(100), info1.GetCollectionId())

	// Verify GetSnapshot for collection 200 returns collection 200's snapshot
	info2, err := sm.GetSnapshot(ctx, 200, "backup")
	require.NoError(t, err)
	assert.Equal(t, int64(2), info2.GetId())
	assert.Equal(t, int64(200), info2.GetCollectionId())

	// Drop snapshot "backup" from collection 100
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()
	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()
	mockWriter := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriter.UnPatch()

	err = sm.DropSnapshot(ctx, 100, "backup")
	require.NoError(t, err)

	// Verify collection 100's "backup" is gone
	_, err = sm.GetSnapshot(ctx, 100, "backup")
	assert.Error(t, err)

	// Verify collection 200's "backup" is NOT affected
	info2After, err := sm.GetSnapshot(ctx, 200, "backup")
	require.NoError(t, err)
	assert.Equal(t, int64(2), info2After.GetId())

	// Verify ListSnapshots for collection 100 only returns collection 100's remaining snapshot
	names, err := sm.ListSnapshots(ctx, 100, 0)
	require.NoError(t, err)
	assert.Len(t, names, 1)
	assert.Contains(t, names, "daily")
	assert.NotContains(t, names, "backup")

	// Verify ListSnapshots for collection 200 still has "backup"
	names2, err := sm.ListSnapshots(ctx, 200, 0)
	require.NoError(t, err)
	assert.Len(t, names2, 1)
	assert.Contains(t, names2, "backup")
}

func TestSnapshotMeta_DropSnapshot_CollectionScoped(t *testing.T) {
	// Arrange: two collections with same-named snapshot
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snapA := &datapb.SnapshotInfo{
		Id:           10,
		CollectionId: 300,
		Name:         "weekly",
		CreateTs:     1000,
		S3Location:   "s3://bucket/300/10",
		PartitionIds: []int64{30},
	}
	snapB := &datapb.SnapshotInfo{
		Id:           20,
		CollectionId: 400,
		Name:         "weekly",
		CreateTs:     2000,
		S3Location:   "s3://bucket/400/20",
		PartitionIds: []int64{40},
	}

	insertTestSnapshot(sm, snapA, []int64{3001}, []int64{4001})
	insertTestSnapshot(sm, snapB, []int64{3002}, []int64{4002})

	// Mock catalog and writer for drop
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()
	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()
	mockWriter := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriter.UnPatch()

	// Act: Drop "weekly" from collection 300
	err := sm.DropSnapshot(ctx, 300, "weekly")
	require.NoError(t, err)

	// Assert: collection 300's snapshot is gone
	_, err = sm.GetSnapshot(ctx, 300, "weekly")
	assert.Error(t, err)

	// Assert: collection 300 has no snapshots in memory
	_, exists := sm.snapshotID2Info.Get(int64(10))
	assert.False(t, exists, "snapshot 10 should be removed from snapshotID2Info")
	_, exists = sm.snapshotID2RefIndex.Get(int64(10))
	assert.False(t, exists, "snapshot 10 should be removed from snapshotID2RefIndex")

	// Assert: collection 400's "weekly" is completely unaffected
	infoB, err := sm.GetSnapshot(ctx, 400, "weekly")
	require.NoError(t, err)
	assert.Equal(t, int64(20), infoB.GetId())
	assert.Equal(t, int64(400), infoB.GetCollectionId())

	// Assert: collection 400's snapshot data is intact in memory
	_, exists = sm.snapshotID2Info.Get(int64(20))
	assert.True(t, exists, "snapshot 20 should still exist in snapshotID2Info")
	refIndex, exists := sm.snapshotID2RefIndex.Get(int64(20))
	assert.True(t, exists, "snapshot 20 should still exist in snapshotID2RefIndex")
	assert.True(t, refIndex.ContainsSegment(3002), "snapshot 20 should still reference segment 3002")
}

func TestSnapshotMeta_ListSnapshots_AllCollections_MultipleCollections(t *testing.T) {
	// Arrange: snapshots across multiple collections
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snap1 := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "snap_c100_1",
		CreateTs:     1000,
		S3Location:   "s3://bucket/100/1",
		PartitionIds: []int64{10},
	}
	snap2 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 100,
		Name:         "snap_c100_2",
		CreateTs:     2000,
		S3Location:   "s3://bucket/100/2",
		PartitionIds: []int64{10},
	}
	snap3 := &datapb.SnapshotInfo{
		Id:           3,
		CollectionId: 200,
		Name:         "snap_c200_1",
		CreateTs:     3000,
		S3Location:   "s3://bucket/200/3",
		PartitionIds: []int64{20},
	}
	snap4 := &datapb.SnapshotInfo{
		Id:           4,
		CollectionId: 300,
		Name:         "snap_c300_1",
		CreateTs:     4000,
		S3Location:   "s3://bucket/300/4",
		PartitionIds: []int64{30},
	}

	insertTestSnapshot(sm, snap1, nil, nil)
	insertTestSnapshot(sm, snap2, nil, nil)
	insertTestSnapshot(sm, snap3, nil, nil)
	insertTestSnapshot(sm, snap4, nil, nil)

	// Act: ListSnapshots with collectionID=0 (all collections)
	names, err := sm.ListSnapshots(ctx, 0, 0)
	require.NoError(t, err)

	// Assert: all 4 snapshots from all 3 collections are returned
	assert.Len(t, names, 4)
	assert.Contains(t, names, "snap_c100_1")
	assert.Contains(t, names, "snap_c100_2")
	assert.Contains(t, names, "snap_c200_1")
	assert.Contains(t, names, "snap_c300_1")

	// Act: ListSnapshots with specific collectionID only returns that collection
	names100, err := sm.ListSnapshots(ctx, 100, 0)
	require.NoError(t, err)
	assert.Len(t, names100, 2)
	assert.Contains(t, names100, "snap_c100_1")
	assert.Contains(t, names100, "snap_c100_2")

	names200, err := sm.ListSnapshots(ctx, 200, 0)
	require.NoError(t, err)
	assert.Len(t, names200, 1)
	assert.Contains(t, names200, "snap_c200_1")

	names300, err := sm.ListSnapshots(ctx, 300, 0)
	require.NoError(t, err)
	assert.Len(t, names300, 1)
	assert.Contains(t, names300, "snap_c300_1")

	// Act: ListSnapshots with partition filter across all collections
	namesP10, err := sm.ListSnapshots(ctx, 0, 10)
	require.NoError(t, err)
	assert.Len(t, namesP10, 2)
	assert.Contains(t, namesP10, "snap_c100_1")
	assert.Contains(t, namesP10, "snap_c100_2")
}

// --- Orphan Snapshot Detection Tests ---

func TestSnapshotMeta_OrphanDetection_SkipsDeletingState(t *testing.T) {
	// The orphan GC logic in gcSnapshotCleanup ranges over snapshotID2Info and
	// collects collectionIDs only for snapshots NOT in SnapshotStateDeleting.
	// This test verifies that ranging+filtering logic produces the correct set
	// of collection IDs, ensuring DELETING snapshots are excluded.
	sm := createTestSnapshotMetaLoaded(t)

	// Collection 100: one COMMITTED snapshot (should appear in orphan set)
	snapCommitted := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "snap_committed",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/100/1",
	}
	insertTestSnapshot(sm, snapCommitted, nil, nil)

	// Collection 200: one DELETING snapshot (should NOT appear in orphan set)
	snapDeleting := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: 200,
		Name:         "snap_deleting",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
		S3Location:   "s3://bucket/200/2",
	}
	insertTestSnapshot(sm, snapDeleting, nil, nil)

	// Collection 300: two snapshots - one COMMITTED, one DELETING
	// Collection 300 should still appear because it has a non-DELETING snapshot
	snapCommitted2 := &datapb.SnapshotInfo{
		Id:           3,
		CollectionId: 300,
		Name:         "snap_committed2",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/300/3",
	}
	insertTestSnapshot(sm, snapCommitted2, nil, nil)

	snapDeleting2 := &datapb.SnapshotInfo{
		Id:           4,
		CollectionId: 300,
		Name:         "snap_deleting2",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
		S3Location:   "s3://bucket/300/4",
	}
	insertTestSnapshot(sm, snapDeleting2, nil, nil)

	// Collection 400: only DELETING snapshot (should NOT appear)
	snapDeleting3 := &datapb.SnapshotInfo{
		Id:           5,
		CollectionId: 400,
		Name:         "snap_deleting3",
		State:        datapb.SnapshotState_SnapshotStateDeleting,
		S3Location:   "s3://bucket/400/5",
	}
	insertTestSnapshot(sm, snapDeleting3, nil, nil)

	// Replicate the orphan detection logic from gcSnapshotCleanup (lines 1757-1765)
	orphanCollections := typeutil.NewSet[int64]()
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		if info.GetState() != datapb.SnapshotState_SnapshotStateDeleting {
			orphanCollections.Insert(info.GetCollectionId())
		}
		return true
	})

	// Assert: only collections with non-DELETING snapshots are detected
	assert.Equal(t, 2, orphanCollections.Len(), "should detect 2 collections with non-DELETING snapshots")
	assert.True(t, orphanCollections.Contain(int64(100)), "collection 100 (COMMITTED) should be in orphan set")
	assert.False(t, orphanCollections.Contain(int64(200)), "collection 200 (only DELETING) should NOT be in orphan set")
	assert.True(t, orphanCollections.Contain(int64(300)), "collection 300 (has COMMITTED) should be in orphan set")
	assert.False(t, orphanCollections.Contain(int64(400)), "collection 400 (only DELETING) should NOT be in orphan set")
}

func TestSnapshotMeta_DropSnapshotsByCollection_WithMixedStates(t *testing.T) {
	// When DropSnapshotsByCollection is called, it should clean up all snapshots
	// tracked in the collectionID2Snapshots index. Snapshots in DELETING state
	// that are already tracked in the index (e.g., inserted via insertTestSnapshot
	// which simulates reload) should also be cleaned up.
	//
	// In production, DELETING snapshots are NOT reloaded into snapshotID2Info
	// (see reload(), line 268). But COMMITTED snapshots that are then dropped
	// individually transition to DELETING during DropSnapshot flow. This test
	// verifies that DropSnapshotsByCollection handles collections where some
	// snapshots have already been individually dropped (and are no longer in
	// the in-memory index).
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Insert 3 COMMITTED snapshots for collection 100
	snapA := &datapb.SnapshotInfo{
		Id:           10,
		CollectionId: 100,
		Name:         "snap_a",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/100/10",
	}
	insertTestSnapshot(sm, snapA, []int64{1001}, nil)

	snapB := &datapb.SnapshotInfo{
		Id:           11,
		CollectionId: 100,
		Name:         "snap_b",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/100/11",
	}
	insertTestSnapshot(sm, snapB, []int64{1002}, nil)

	snapC := &datapb.SnapshotInfo{
		Id:           12,
		CollectionId: 100,
		Name:         "snap_c",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/100/12",
	}
	insertTestSnapshot(sm, snapC, []int64{1003}, nil)

	// Also insert a snapshot for collection 200 to verify isolation
	snapOther := &datapb.SnapshotInfo{
		Id:           20,
		CollectionId: 200,
		Name:         "snap_other",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/200/20",
	}
	insertTestSnapshot(sm, snapOther, []int64{2001}, nil)

	// Verify initial state: collection 100 has 3 snapshots
	names100, err := sm.ListSnapshots(ctx, int64(100), int64(0))
	require.NoError(t, err)
	assert.Len(t, names100, 3)

	// Mock all dependencies for DropSnapshot
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()
	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()
	mockWriter := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriter.UnPatch()

	// Act: drop all snapshots for collection 100
	err = sm.DropSnapshotsByCollection(ctx, int64(100))

	// Assert: no error
	require.NoError(t, err)

	// All 3 snapshots for collection 100 should be removed from memory
	_, exists := sm.snapshotID2Info.Get(int64(10))
	assert.False(t, exists, "snap_a should be removed")
	_, exists = sm.snapshotID2Info.Get(int64(11))
	assert.False(t, exists, "snap_b should be removed")
	_, exists = sm.snapshotID2Info.Get(int64(12))
	assert.False(t, exists, "snap_c should be removed")

	// RefIndex entries should also be removed
	_, exists = sm.snapshotID2RefIndex.Get(int64(10))
	assert.False(t, exists, "snap_a RefIndex should be removed")
	_, exists = sm.snapshotID2RefIndex.Get(int64(11))
	assert.False(t, exists, "snap_b RefIndex should be removed")
	_, exists = sm.snapshotID2RefIndex.Get(int64(12))
	assert.False(t, exists, "snap_c RefIndex should be removed")

	// Collection 100 should have no snapshots listed
	names100After, err := sm.ListSnapshots(ctx, int64(100), int64(0))
	require.NoError(t, err)
	assert.Len(t, names100After, 0, "collection 100 should have no snapshots")

	// Collection 200 should be unaffected
	info200, exists := sm.snapshotID2Info.Get(int64(20))
	assert.True(t, exists, "collection 200 snapshot should remain")
	assert.Equal(t, "snap_other", info200.GetName())

	names200, err := sm.ListSnapshots(ctx, int64(200), int64(0))
	require.NoError(t, err)
	assert.Len(t, names200, 1)
	assert.Contains(t, names200, "snap_other")
}

func TestSnapshotMeta_OrphanDetection_MultipleCollections(t *testing.T) {
	// Test orphan detection with multiple collections having various snapshot states.
	// This tests the scenario where the GC needs to identify which collections
	// are candidates for orphan cleanup across many collections.
	sm := createTestSnapshotMetaLoaded(t)

	// Setup: 5 collections with different snapshot state combinations
	testCases := []struct {
		collectionID int64
		snapshots    []*datapb.SnapshotInfo
		expectOrphan bool
		description  string
	}{
		{
			collectionID: 100,
			snapshots: []*datapb.SnapshotInfo{
				{Id: 1, CollectionId: 100, Name: "s1", State: datapb.SnapshotState_SnapshotStateCommitted, S3Location: "s3://b/1"},
				{Id: 2, CollectionId: 100, Name: "s2", State: datapb.SnapshotState_SnapshotStateCommitted, S3Location: "s3://b/2"},
			},
			expectOrphan: true,
			description:  "all COMMITTED",
		},
		{
			collectionID: 200,
			snapshots: []*datapb.SnapshotInfo{
				{Id: 3, CollectionId: 200, Name: "s3", State: datapb.SnapshotState_SnapshotStateDeleting, S3Location: "s3://b/3"},
			},
			expectOrphan: false,
			description:  "all DELETING",
		},
		{
			collectionID: 300,
			snapshots: []*datapb.SnapshotInfo{
				{Id: 4, CollectionId: 300, Name: "s4", State: datapb.SnapshotState_SnapshotStateCommitted, S3Location: "s3://b/4"},
				{Id: 5, CollectionId: 300, Name: "s5", State: datapb.SnapshotState_SnapshotStateDeleting, S3Location: "s3://b/5"},
				{Id: 6, CollectionId: 300, Name: "s6", State: datapb.SnapshotState_SnapshotStateDeleting, S3Location: "s3://b/6"},
			},
			expectOrphan: true,
			description:  "mixed: 1 COMMITTED + 2 DELETING",
		},
		{
			collectionID: 400,
			snapshots: []*datapb.SnapshotInfo{
				{Id: 7, CollectionId: 400, Name: "s7", State: datapb.SnapshotState_SnapshotStateDeleting, S3Location: "s3://b/7"},
				{Id: 8, CollectionId: 400, Name: "s8", State: datapb.SnapshotState_SnapshotStateDeleting, S3Location: "s3://b/8"},
			},
			expectOrphan: false,
			description:  "multiple DELETING",
		},
		{
			collectionID: 500,
			snapshots: []*datapb.SnapshotInfo{
				{Id: 9, CollectionId: 500, Name: "s9", State: datapb.SnapshotState_SnapshotStatePending, S3Location: "s3://b/9"},
			},
			expectOrphan: true,
			description:  "PENDING (not DELETING, so included)",
		},
	}

	// Insert all snapshots
	for _, tc := range testCases {
		for _, snap := range tc.snapshots {
			insertTestSnapshot(sm, snap, nil, nil)
		}
	}

	// Replicate orphan detection logic
	orphanCollections := typeutil.NewSet[int64]()
	sm.snapshotID2Info.Range(func(id UniqueID, info *datapb.SnapshotInfo) bool {
		if info.GetState() != datapb.SnapshotState_SnapshotStateDeleting {
			orphanCollections.Insert(info.GetCollectionId())
		}
		return true
	})

	// Verify each collection
	for _, tc := range testCases {
		assert.Equal(t, tc.expectOrphan, orphanCollections.Contain(tc.collectionID),
			"collection %d (%s): expected orphan=%v", tc.collectionID, tc.description, tc.expectOrphan)
	}

	// Total orphan candidates: 100 (COMMITTED), 300 (mixed), 500 (PENDING)
	assert.Equal(t, 3, orphanCollections.Len(), "should have 3 orphan candidate collections")
}

func TestSnapshotMeta_DropSnapshotsByCollection_SnapshotAlreadyRemovedFromMemory(t *testing.T) {
	// Edge case: collection has snapshot IDs in the collectionID2Snapshots index
	// but the snapshot was already removed from snapshotID2Info (e.g., by concurrent
	// DropSnapshot). DropSnapshotsByCollection should handle this gracefully by
	// skipping snapshots that no longer exist in memory.
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Insert two snapshots for collection 100
	snapA := &datapb.SnapshotInfo{
		Id:           10,
		CollectionId: 100,
		Name:         "snap_a",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/100/10",
	}
	insertTestSnapshot(sm, snapA, nil, nil)

	snapB := &datapb.SnapshotInfo{
		Id:           11,
		CollectionId: 100,
		Name:         "snap_b",
		State:        datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:   "s3://bucket/100/11",
	}
	insertTestSnapshot(sm, snapB, nil, nil)

	// Simulate snap_a already removed from snapshotID2Info (concurrent DropSnapshot)
	// but still in the collectionID2Snapshots index
	sm.snapshotID2Info.Remove(int64(10))
	sm.snapshotID2RefIndex.Remove(int64(10))
	// Name index is also cleaned up
	if nameMap, ok := sm.snapshotName2ID.Get(int64(100)); ok {
		nameMap.Remove("snap_a")
	}

	// Mock dependencies
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()
	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()
	mockWriter := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriter.UnPatch()

	// Act: DropSnapshotsByCollection should handle the missing snapshot gracefully
	err := sm.DropSnapshotsByCollection(ctx, int64(100))

	// Assert: no error - snap_a is already gone, snap_b is successfully deleted
	require.NoError(t, err)

	// snap_b should be removed
	_, exists := sm.snapshotID2Info.Get(int64(11))
	assert.False(t, exists, "snap_b should be removed")

	// Collection 100 should have no snapshots
	names, err := sm.ListSnapshots(ctx, int64(100), int64(0))
	require.NoError(t, err)
	assert.Len(t, names, 0)
}

// --- getSnapshotByName index inconsistency test ---

func TestSnapshotMeta_GetSnapshotByName_IndexInconsistency(t *testing.T) {
	// Test: when snapshotName2ID has an entry but snapshotID2Info doesn't,
	// getSnapshotByName should return an error AND clean up the orphan name index entry.

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	collectionID := int64(100)
	snapshotName := "orphan_snapshot"
	snapshotID := int64(999)

	// Set up inconsistent state: name2ID has an entry, but snapshotID2Info does NOT
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert(snapshotName, snapshotID)
	sm.snapshotName2ID.Insert(collectionID, nameMap)

	// Verify the orphan entry exists before the call
	_, exists := nameMap.Get(snapshotName)
	require.True(t, exists, "orphan name entry should exist before getSnapshotByName")

	// Act
	ctx := context.Background()
	info, err := sm.getSnapshotByName(ctx, collectionID, snapshotName)

	// Assert: error returned
	assert.Nil(t, info)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Assert: orphan entry was cleaned up from nameMap
	_, exists = nameMap.Get(snapshotName)
	assert.False(t, exists, "orphan name entry should be removed after getSnapshotByName detects inconsistency")
}

// --- addToSecondaryIndexes tests ---

func TestSnapshotMeta_AddToSecondaryIndexes_MultipleSnapshots(t *testing.T) {
	// Test: adding two snapshots to the same collection puts both in collectionID2Snapshots

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	collectionID := int64(100)

	snap1 := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: collectionID,
		Name:         "snapshot_a",
	}
	snap2 := &datapb.SnapshotInfo{
		Id:           2,
		CollectionId: collectionID,
		Name:         "snapshot_b",
	}

	// Act
	sm.addToSecondaryIndexes(snap1)
	sm.addToSecondaryIndexes(snap2)

	// Assert: collectionID2Snapshots has both snapshot IDs
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	require.True(t, ok, "collectionID2Snapshots should have an entry for collectionID")
	assert.True(t, snapshotIDs.Contain(int64(1)), "should contain snapshot 1")
	assert.True(t, snapshotIDs.Contain(int64(2)), "should contain snapshot 2")
	assert.Equal(t, 2, snapshotIDs.Len(), "should have exactly 2 snapshots")

	// Assert: snapshotName2ID has both entries under the same collection
	nameMap, ok := sm.snapshotName2ID.Get(collectionID)
	require.True(t, ok, "snapshotName2ID should have an entry for collectionID")
	id1, ok := nameMap.Get("snapshot_a")
	assert.True(t, ok)
	assert.Equal(t, int64(1), id1)
	id2, ok := nameMap.Get("snapshot_b")
	assert.True(t, ok)
	assert.Equal(t, int64(2), id2)
}

// --- removeFromSecondaryIndexes tests ---

func TestSnapshotMeta_RemoveFromSecondaryIndexes_LastSnapshot(t *testing.T) {
	// Test: removing the last snapshot from a collection removes the collection entry entirely

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	collectionID := int64(100)
	snap := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: collectionID,
		Name:         "only_snapshot",
	}

	// Setup: add one snapshot
	sm.addToSecondaryIndexes(snap)

	// Verify setup
	_, ok := sm.collectionID2Snapshots.Get(collectionID)
	require.True(t, ok, "collection entry should exist after add")

	// Act: remove the only snapshot
	sm.removeFromSecondaryIndexes(snap)

	// Assert: collection entry is removed from collectionID2Snapshots
	_, ok = sm.collectionID2Snapshots.Get(collectionID)
	assert.False(t, ok, "collection entry should be removed when last snapshot is deleted")

	// Assert: name entry is removed from snapshotName2ID
	nameMap, ok := sm.snapshotName2ID.Get(collectionID)
	if ok {
		_, nameExists := nameMap.Get("only_snapshot")
		assert.False(t, nameExists, "snapshot name should be removed from name index")
	}
}

func TestSnapshotMeta_RemoveFromSecondaryIndexes_OneOfTwo(t *testing.T) {
	// Test: removing one of two snapshots leaves the remaining one intact

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	collectionID := int64(200)
	snap1 := &datapb.SnapshotInfo{
		Id:           10,
		CollectionId: collectionID,
		Name:         "snap_keep",
	}
	snap2 := &datapb.SnapshotInfo{
		Id:           20,
		CollectionId: collectionID,
		Name:         "snap_remove",
	}

	// Setup: add two snapshots
	sm.addToSecondaryIndexes(snap1)
	sm.addToSecondaryIndexes(snap2)

	// Verify setup
	snapshotIDs, ok := sm.collectionID2Snapshots.Get(collectionID)
	require.True(t, ok)
	require.Equal(t, 2, snapshotIDs.Len())

	// Act: remove snap2
	sm.removeFromSecondaryIndexes(snap2)

	// Assert: collection entry still exists with snap1
	snapshotIDs, ok = sm.collectionID2Snapshots.Get(collectionID)
	require.True(t, ok, "collection entry should still exist")
	assert.Equal(t, 1, snapshotIDs.Len(), "should have 1 snapshot remaining")
	assert.True(t, snapshotIDs.Contain(int64(10)), "snap1 should remain")
	assert.False(t, snapshotIDs.Contain(int64(20)), "snap2 should be removed")

	// Assert: name index reflects the removal
	nameMap, ok := sm.snapshotName2ID.Get(collectionID)
	require.True(t, ok)
	_, ok = nameMap.Get("snap_keep")
	assert.True(t, ok, "snap_keep should still be in name index")
	_, ok = nameMap.Get("snap_remove")
	assert.False(t, ok, "snap_remove should be removed from name index")
}

func TestSnapshotMeta_RemoveFromSecondaryIndexes_NonExistent(t *testing.T) {
	// Test: removing a snapshot from a non-existent collection should not panic

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	snap := &datapb.SnapshotInfo{
		Id:           42,
		CollectionId: 999, // Non-existent collection
		Name:         "ghost_snapshot",
	}

	// Act: should not panic
	assert.NotPanics(t, func() {
		sm.removeFromSecondaryIndexes(snap)
	}, "removeFromSecondaryIndexes should not panic for non-existent collection")

	// Assert: maps are still empty / unchanged
	_, ok := sm.collectionID2Snapshots.Get(int64(999))
	assert.False(t, ok, "no collection entry should be created")
}
