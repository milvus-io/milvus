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
	"github.com/cockroachdb/errors"
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
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		catalog:                      catalog,
		snapshotID2Info:              typeutil.NewConcurrentMap[typeutil.UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:          typeutil.NewConcurrentMap[typeutil.UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:              typeutil.NewConcurrentMap[typeutil.UniqueID, *typeutil.ConcurrentMap[string, typeutil.UniqueID]](),
		collectionID2Snapshots:       typeutil.NewConcurrentMap[typeutil.UniqueID, typeutil.UniqueSet](),
		segmentProtectionUntil:       make(map[int64]uint64),
		compactionBlockedCollections: typeutil.NewUniqueSet(),
		gcBlockedCollections:         typeutil.NewUniqueSet(),
		segmentReferencedByGC:        typeutil.NewUniqueSet(),
		buildIDReferencedByGC:        typeutil.NewUniqueSet(),
		snapshotPendingCollections:   typeutil.NewUniqueSet(),
		loaderCtx:                    loaderCtx,
		loaderCancel:                 loaderCancel,
		reader:                       NewSnapshotReader(tempChunkManager),
		writer:                       NewSnapshotWriter(tempChunkManager),
	}
}

// createTestSnapshotMetaLoaded creates a snapshotMeta for tests that don't call reload().
// Same as createTestSnapshotMeta since RefIndex state is now per-snapshot.
func createTestSnapshotMetaLoaded(t *testing.T) *snapshotMeta {
	return createTestSnapshotMeta(t)
}

// insertTestSnapshot is a LIGHTWEIGHT helper that only primes the three lookup maps
// (snapshotID2Info / snapshotID2RefIndex / secondary indexes). It is meant for tests
// that exercise pure lookup paths (ListSnapshots, name→ID resolution, etc.) where the
// snapshot protection state is irrelevant.
//
// It does NOT maintain any of the protection sets — segmentProtectionUntil,
// segmentReferencedByGC, buildIDReferencedByGC, compactionBlockedCollections, or
// gcBlockedCollections are left untouched. If your test needs protection state to
// reflect the inserted snapshot, one of the following is required:
//   - call sm.rebuildAllSegmentProtection() after insertion to derive all 5 sets, OR
//   - call sm.registerSnapshotProtection(info, segmentIDs, buildIDs) for an incremental
//     update, OR
//   - use saveTestSnapshots() which goes through the real SaveSnapshot code path.
//
// Parameters:
//   - sm: the snapshotMeta to populate
//   - info: the snapshot metadata (with Id, Name, CollectionId, etc.)
//   - segmentIDs: segment IDs the snapshot references (passed to RefIndex)
//   - buildIDs: index build IDs the snapshot references (passed to RefIndex)
func insertTestSnapshot(sm *snapshotMeta, info *datapb.SnapshotInfo, segmentIDs, buildIDs []int64) {
	sm.snapshotID2Info.Insert(info.GetId(), info)
	sm.snapshotID2RefIndex.Insert(info.GetId(), NewLoadedSnapshotRefIndex(segmentIDs, buildIDs))
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
	refIndex := NewLoadedSnapshotRefIndex([]int64{1001, 1002}, []int64{3001, 3002})

	// Should not block (already loaded)
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsSegment(1002))
	assert.False(t, refIndex.ContainsSegment(1003))
	assert.True(t, refIndex.ContainsBuildID(3001))
	assert.True(t, refIndex.ContainsBuildID(3002))
	assert.False(t, refIndex.ContainsBuildID(3003))
}

func TestSnapshotRefIndex_SetLoaded(t *testing.T) {
	// Test SetLoaded method
	refIndex := NewSnapshotRefIndex()

	// Initially empty
	assert.False(t, refIndex.ContainsSegment(1001))
	assert.False(t, refIndex.ContainsBuildID(3001))

	// Set loaded data
	refIndex.SetLoaded([]int64{1001, 1002}, []int64{3001})

	// Should contain the loaded IDs
	assert.True(t, refIndex.ContainsSegment(1001))
	assert.True(t, refIndex.ContainsSegment(1002))
	assert.False(t, refIndex.ContainsSegment(1003))
	assert.True(t, refIndex.ContainsBuildID(3001))
	assert.False(t, refIndex.ContainsBuildID(3002))
}

func TestSnapshotRefIndex_EmptySets(t *testing.T) {
	// Test refIndex with empty/nil sets (e.g., after load failure)
	refIndex := NewLoadedSnapshotRefIndex(nil, nil)

	// Should not block and should return false for all queries
	assert.False(t, refIndex.ContainsSegment(1001))
	assert.False(t, refIndex.ContainsBuildID(3001))
}

// --- Concurrent Operations Tests ---

func TestSnapshotMeta_ConcurrentOperations(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Test concurrent map operations don't panic
	snapshotData := createTestSnapshotDataForMeta()

	// Act - simulate concurrent operations
	insertTestSnapshot(sm, snapshotData.SnapshotInfo, []int64{1001}, nil)
	sm.rebuildAllSegmentProtection()

	// These operations should work concurrently
	snapshots, err := sm.ListSnapshots(ctx, 0, 0)
	blocked := sm.IsSegmentGCBlocked(100, 1001)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 1)
	assert.True(t, blocked)
}

func TestSnapshotMeta_EmptyMaps(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Act
	snapshots, err := sm.ListSnapshots(ctx, 100, 1)
	blocked := sm.IsSegmentGCBlocked(100, 1001)
	snapshot, getErr := sm.GetSnapshot(ctx, int64(100), "nonexistent")

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 0)
	assert.False(t, blocked)
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
	// In current implementation, SegmentIDs will be empty for legacy snapshots
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

	// Verify legacy format snapshot has empty ID sets
	assert.False(t, legacyRefIndex.ContainsSegment(10020))

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

	// ContainsSegment/ContainsBuildID should return false for failed state
	assert.False(t, refIndex.ContainsSegment(1001))
}

func TestSnapshotRefIndex_TransitionFromFailedToLoaded(t *testing.T) {
	// Test that SetLoaded can recover from failed state
	refIndex := NewSnapshotRefIndex()

	// Set as failed first
	refIndex.SetFailed()
	assert.True(t, refIndex.IsFailed())

	// Now set as loaded with data
	refIndex.SetLoaded([]int64{1001, 1002}, []int64{3001})

	// Should now be loaded, not failed
	assert.False(t, refIndex.IsFailed())
	assert.True(t, refIndex.IsLoaded())
	assert.True(t, refIndex.ContainsSegment(1001))
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

// TestSnapshotMeta_LoadUnloadedRefIndexes_HungReadIsBoundedByTimeout asserts that
// a hung S3 ReadSnapshot is bounded by the per-call timeout
// (dataCoord.snapshot.refIndexLoadTimeout) and does not block the loader Range.
//
// Regression for the third-round review finding: without per-call timeout, a single
// hung S3 read would freeze the entire loader, no other RefIndex would ever be loaded,
// rebuildAllSegmentProtection would never be triggered, and every collection with a
// snapshot would stay in fail-closed coarse block — leaking storage indefinitely.
func TestSnapshotMeta_LoadUnloadedRefIndexes_HungReadIsBoundedByTimeout(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Configure a tiny timeout so the test runs in milliseconds, not seconds.
	pt := paramtable.Get()
	pt.Save(pt.DataCoordCfg.SnapshotRefIndexLoadTimeout.Key, "100ms")
	defer pt.Reset(pt.DataCoordCfg.SnapshotRefIndexLoadTimeout.Key)

	infoHung := &datapb.SnapshotInfo{
		Id: 1, CollectionId: 100, Name: "hung", S3Location: "s3://bucket/hung",
		State: datapb.SnapshotState_SnapshotStateCommitted,
	}
	infoFast := &datapb.SnapshotInfo{
		Id: 2, CollectionId: 200, Name: "fast", S3Location: "s3://bucket/fast",
		State: datapb.SnapshotState_SnapshotStateCommitted,
	}

	mockList := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).Return(
		[]*datapb.SnapshotInfo{infoHung, infoFast}, nil).Build()
	defer mockList.UnPatch()

	// Mock ReadSnapshot to BLOCK indefinitely on hung's path until ctx is canceled,
	// and return a successful (empty) result on fast's path.
	mockRead := mockey.Mock((*SnapshotReader).ReadSnapshot).To(
		func(sr *SnapshotReader, ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
			if metadataFilePath == "s3://bucket/hung" {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &SnapshotData{
				SnapshotInfo: &datapb.SnapshotInfo{Id: 2},
				SegmentIDs:   []int64{2001},
				BuildIDs:     []int64{3001},
			}, nil
		}).Build()
	defer mockRead.UnPatch()

	require.NoError(t, sm.reload(ctx))

	// loadUnloadedRefIndexes must return within a reasonable time bounded by the
	// per-call timeout (100ms × 2 calls = ~200ms expected, allow generous slack).
	done := make(chan bool)
	go func() {
		sm.loadUnloadedRefIndexes()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("loadUnloadedRefIndexes did not return within 5s — per-call timeout is missing")
	}

	// hung snapshot's RefIndex must be Failed (not Loaded) due to timeout.
	hungRef, ok := sm.snapshotID2RefIndex.Get(infoHung.Id)
	require.True(t, ok)
	assert.True(t, hungRef.IsFailed(),
		"hung snapshot's RefIndex must transition to Failed after timeout")
	assert.False(t, hungRef.IsLoaded())

	// fast snapshot's RefIndex must be Loaded — proving the loader did not get
	// stuck on the hung read and continued to the next snapshot.
	fastRef, ok := sm.snapshotID2RefIndex.Get(infoFast.Id)
	require.True(t, ok)
	assert.True(t, fastRef.IsLoaded(),
		"fast snapshot must be loaded — hung read did not block the Range")
	assert.True(t, fastRef.ContainsSegment(2001))
	assert.True(t, fastRef.ContainsBuildID(3001))
}

func TestSnapshotMeta_SaveSnapshot_CollectsBuildIDs_AllIndexTypes(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	metadataFilePath := "s3://bucket/snapshots/1/metadata/test-uuid.json"

	snapshotData := &SnapshotData{
		SnapshotInfo: createTestSnapshotInfoForMeta(),
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1001,
				PartitionId: 1,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001},
					{BuildID: 3002},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					100: {FieldID: 100, BuildID: 4001},
					101: {FieldID: 101, BuildID: 4002},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					200: {FieldID: 200, BuildID: 5001},
				},
			},
		},
	}

	// Mock SnapshotWriter.Save
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return metadataFilePath, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock catalog.SaveSnapshot
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		return nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert
	assert.NoError(t, err)
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)

	// Vector/scalar index build IDs
	assert.True(t, refIndex.ContainsBuildID(3001))
	assert.True(t, refIndex.ContainsBuildID(3002))
	// Text index build IDs
	assert.True(t, refIndex.ContainsBuildID(4001))
	assert.True(t, refIndex.ContainsBuildID(4002))
	// JSON key index build IDs
	assert.True(t, refIndex.ContainsBuildID(5001))
	// Non-existent build IDs
	assert.False(t, refIndex.ContainsBuildID(9999))
}

func TestSnapshotMeta_SaveSnapshot_SkipsZeroBuildIDs(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	metadataFilePath := "s3://bucket/snapshots/1/metadata/test-uuid.json"

	snapshotData := &SnapshotData{
		SnapshotInfo: createTestSnapshotInfoForMeta(),
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test_collection"},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1001,
				PartitionId: 1,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					100: {FieldID: 100, BuildID: 0}, // zero build ID, should be skipped
					101: {FieldID: 101, BuildID: 4001},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					200: {FieldID: 200, BuildID: 0}, // zero build ID, should be skipped
					201: {FieldID: 201, BuildID: 5001},
				},
			},
		},
	}

	// Mock SnapshotWriter.Save
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return metadataFilePath, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock catalog.SaveSnapshot
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		return nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert
	assert.NoError(t, err)
	refIndex, exists := sm.snapshotID2RefIndex.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)

	// Vector/scalar build IDs are always collected (no zero check)
	assert.True(t, refIndex.ContainsBuildID(3001))
	// Non-zero text/JSON build IDs should be collected
	assert.True(t, refIndex.ContainsBuildID(4001))
	assert.True(t, refIndex.ContainsBuildID(5001))
	// Zero build IDs should NOT be in the refIndex
	assert.False(t, refIndex.ContainsBuildID(0))

	// Verify BuildIDs slice on SnapshotData: should have 3001, 4001, 5001 only (3 items)
	assert.Len(t, snapshotData.BuildIDs, 3)
	assert.Contains(t, snapshotData.BuildIDs, int64(3001))
	assert.Contains(t, snapshotData.BuildIDs, int64(4001))
	assert.Contains(t, snapshotData.BuildIDs, int64(5001))
}

// --- Compaction Protection Tests ---

func TestSnapshotRefIndex_GetSegmentIDs(t *testing.T) {
	t.Run("nil segmentIDs", func(t *testing.T) {
		ri := NewSnapshotRefIndex()
		assert.Nil(t, ri.GetSegmentIDs())
	})

	t.Run("empty segmentIDs", func(t *testing.T) {
		ri := NewLoadedSnapshotRefIndex([]int64{}, nil)
		ids := ri.GetSegmentIDs()
		assert.NotNil(t, ids)
		assert.Empty(t, ids)
	})

	t.Run("returns copy of segment IDs", func(t *testing.T) {
		ri := NewLoadedSnapshotRefIndex([]int64{1, 2, 3}, nil)
		ids := ri.GetSegmentIDs()
		assert.Len(t, ids, 3)
		assert.ElementsMatch(t, []int64{1, 2, 3}, ids)
	})
}

func TestSnapshotRefIndex_GetBuildIDs(t *testing.T) {
	t.Run("nil buildIDs", func(t *testing.T) {
		ri := NewSnapshotRefIndex()
		assert.Nil(t, ri.GetBuildIDs())
	})

	t.Run("returns copy of build IDs", func(t *testing.T) {
		ri := NewLoadedSnapshotRefIndex(nil, []int64{10, 20, 30})
		ids := ri.GetBuildIDs()
		assert.Len(t, ids, 3)
		assert.ElementsMatch(t, []int64{10, 20, 30}, ids)
	})

	t.Run("loaded with nil buildIDs returns empty", func(t *testing.T) {
		ri := NewLoadedSnapshotRefIndex([]int64{1}, nil)
		ids := ri.GetBuildIDs()
		// NewLoadedSnapshotRefIndex initializes buildIDs as an empty UniqueSet when nil is
		// passed, so GetBuildIDs returns an empty slice (not nil). This exercises the
		// non-nil loaded path of GetBuildIDs.
		assert.NotNil(t, ids)
		assert.Empty(t, ids)
	})
}

func TestSnapshotMeta_IsSegmentCompactionProtected(t *testing.T) {
	t.Run("unprotected segment", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		assert.False(t, sm.IsSegmentCompactionProtected(999))
	})

	t.Run("protected segment with future expiry", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600 // 1 hour from now
		sm.segmentProtectionUntil[1001] = futureTs
		assert.True(t, sm.IsSegmentCompactionProtected(1001))
	})

	t.Run("expired protection", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		pastTs := uint64(time.Now().Unix()) - 10 // 10 seconds ago
		sm.segmentProtectionUntil[1001] = pastTs
		assert.False(t, sm.IsSegmentCompactionProtected(1001))
	})

	t.Run("protection exactly at current time", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		// Protection until now means it should NOT be protected (>= means expired)
		nowTs := uint64(time.Now().Unix())
		sm.segmentProtectionUntil[1001] = nowTs
		assert.False(t, sm.IsSegmentCompactionProtected(1001))
	})
}

func TestSnapshotMeta_RegisterSnapshotProtection(t *testing.T) {
	t.Run("TTL=0 still registers GC protection for segments and buildIDs", func(t *testing.T) {
		// P0 regression: a TTL=0 snapshot must still pin its files against GC,
		// otherwise the freshly-created snapshot's data could be deleted.
		sm := createTestSnapshotMetaLoaded(t)
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: 0,
		}
		sm.registerSnapshotProtection(info, []int64{1001, 1002}, []int64{2001, 2002})

		// Compaction protection: empty because TTL=0 means "no compaction block".
		assert.Empty(t, sm.segmentProtectionUntil)

		// GC protection: MUST be populated regardless of TTL.
		assert.True(t, sm.segmentReferencedByGC.Contain(1001))
		assert.True(t, sm.segmentReferencedByGC.Contain(1002))
		assert.True(t, sm.buildIDReferencedByGC.Contain(2001))
		assert.True(t, sm.buildIDReferencedByGC.Contain(2002))

		// Public API must also report GC-blocked.
		assert.True(t, sm.IsSegmentGCBlocked(100, 1001))
		assert.True(t, sm.IsBuildIDGCBlocked(100, 2001))
	})

	t.Run("TTL>0 registers both compaction and GC protection", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.registerSnapshotProtection(info, []int64{1001, 1002}, []int64{2001})

		// Compaction TTL set on both segments.
		assert.Equal(t, futureTs, sm.segmentProtectionUntil[1001])
		assert.Equal(t, futureTs, sm.segmentProtectionUntil[1002])

		// GC protection set on both segments and the buildID.
		assert.True(t, sm.segmentReferencedByGC.Contain(1001))
		assert.True(t, sm.segmentReferencedByGC.Contain(1002))
		assert.True(t, sm.buildIDReferencedByGC.Contain(2001))
	})

	t.Run("overlapping segments take max compaction expiry and union GC sets", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)

		// First snapshot protects segment 1001 until T+3600.
		ts1 := uint64(time.Now().Unix()) + 3600
		info1 := &datapb.SnapshotInfo{Id: 1, CollectionId: 100, CompactionExpireTime: ts1}
		sm.registerSnapshotProtection(info1, []int64{1001}, []int64{2001})
		assert.Equal(t, ts1, sm.segmentProtectionUntil[1001])

		// Second snapshot protects same segment until T+7200 (larger, should win).
		ts2 := uint64(time.Now().Unix()) + 7200
		info2 := &datapb.SnapshotInfo{Id: 2, CollectionId: 100, CompactionExpireTime: ts2}
		sm.registerSnapshotProtection(info2, []int64{1001}, []int64{2002})
		assert.Equal(t, ts2, sm.segmentProtectionUntil[1001])

		// Third snapshot protects same segment until T+1800 (smaller, must NOT downgrade).
		ts3 := uint64(time.Now().Unix()) + 1800
		info3 := &datapb.SnapshotInfo{Id: 3, CollectionId: 100, CompactionExpireTime: ts3}
		sm.registerSnapshotProtection(info3, []int64{1001}, []int64{2003})
		assert.Equal(t, ts2, sm.segmentProtectionUntil[1001]) // still ts2

		// GC sets are the union of all three snapshots' buildIDs.
		assert.True(t, sm.buildIDReferencedByGC.Contain(2001))
		assert.True(t, sm.buildIDReferencedByGC.Contain(2002))
		assert.True(t, sm.buildIDReferencedByGC.Contain(2003))
	})
}

func TestSnapshotMeta_RebuildAllSegmentProtection(t *testing.T) {
	t.Run("clears all when no snapshots", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		sm.segmentProtectionUntil[1001] = uint64(time.Now().Unix()) + 3600

		sm.rebuildAllSegmentProtection()
		assert.Empty(t, sm.segmentProtectionUntil)
	})

	t.Run("rebuilds from all snapshots", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)

		ts1 := uint64(time.Now().Unix()) + 3600
		ts2 := uint64(time.Now().Unix()) + 7200

		info1 := &datapb.SnapshotInfo{Id: 1, CompactionExpireTime: ts1}
		info2 := &datapb.SnapshotInfo{Id: 2, CompactionExpireTime: ts2}

		sm.snapshotID2Info.Insert(1, info1)
		sm.snapshotID2Info.Insert(2, info2)
		sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001, 1002}, nil))
		sm.snapshotID2RefIndex.Insert(2, NewLoadedSnapshotRefIndex([]int64{1001, 1003}, nil))

		sm.rebuildAllSegmentProtection()

		// 1001 referenced by both, should take max (ts2)
		assert.Equal(t, ts2, sm.segmentProtectionUntil[1001])
		// 1002 only in snapshot 1
		assert.Equal(t, ts1, sm.segmentProtectionUntil[1002])
		// 1003 only in snapshot 2
		assert.Equal(t, ts2, sm.segmentProtectionUntil[1003])
	})

	t.Run("skips expired and unloaded snapshots", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)

		pastTs := uint64(time.Now().Unix()) - 100
		futureTs := uint64(time.Now().Unix()) + 3600

		info1 := &datapb.SnapshotInfo{Id: 1, CompactionExpireTime: pastTs} // expired
		info2 := &datapb.SnapshotInfo{Id: 2, CompactionExpireTime: futureTs}
		info3 := &datapb.SnapshotInfo{Id: 3, CompactionExpireTime: futureTs} // will be unloaded

		sm.snapshotID2Info.Insert(1, info1)
		sm.snapshotID2Info.Insert(2, info2)
		sm.snapshotID2Info.Insert(3, info3)
		sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001}, nil))
		sm.snapshotID2RefIndex.Insert(2, NewLoadedSnapshotRefIndex([]int64{1002}, nil))
		sm.snapshotID2RefIndex.Insert(3, NewSnapshotRefIndex()) // not loaded

		sm.rebuildAllSegmentProtection()

		// 1001 from expired snapshot — should not be protected
		_, exists := sm.segmentProtectionUntil[1001]
		assert.False(t, exists)
		// 1002 from valid snapshot
		assert.Equal(t, futureTs, sm.segmentProtectionUntil[1002])
		// 1003 from unloaded snapshot — should not be protected at segment level
		_, exists = sm.segmentProtectionUntil[1003]
		assert.False(t, exists)
		// But snapshot 3's collection should be blocked (fail-closed)
		assert.True(t, sm.compactionBlockedCollections.Contain(info3.GetCollectionId()))
	})
}

func TestSnapshotMeta_IsCollectionCompactionBlocked(t *testing.T) {
	t.Run("not blocked when no protected snapshots", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("blocked when unloaded RefIndex with active protection", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex()) // not loaded

		sm.rebuildAllSegmentProtection()

		assert.True(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("not blocked when RefIndex is loaded", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001}, nil))

		sm.rebuildAllSegmentProtection()

		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("not blocked when protection expired", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		pastTs := uint64(time.Now().Unix()) - 100
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: pastTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex()) // not loaded but expired

		sm.rebuildAllSegmentProtection()

		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("unblocked after RefIndex loads", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		refIndex := NewSnapshotRefIndex() // not loaded
		sm.snapshotID2RefIndex.Insert(1, refIndex)

		sm.rebuildAllSegmentProtection()
		assert.True(t, sm.IsCollectionCompactionBlocked(100))

		// Simulate RefIndex loading
		refIndex.SetLoaded([]int64{1001}, nil)
		sm.rebuildAllSegmentProtection()

		assert.False(t, sm.IsCollectionCompactionBlocked(100))
		// And now segment-level protection should be in place
		assert.True(t, sm.IsSegmentCompactionProtected(1001))
	})

	t.Run("multiple collections only blocks affected one", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600

		// Collection 100: unloaded RefIndex → blocked
		info1 := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info1)
		sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex())

		// Collection 200: loaded RefIndex → not blocked
		info2 := &datapb.SnapshotInfo{
			Id:                   2,
			CollectionId:         200,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(2, info2)
		sm.snapshotID2RefIndex.Insert(2, NewLoadedSnapshotRefIndex([]int64{2001}, nil))

		sm.rebuildAllSegmentProtection()

		assert.True(t, sm.IsCollectionCompactionBlocked(100))
		assert.False(t, sm.IsCollectionCompactionBlocked(200))
	})

	t.Run("no RefIndex entry at all blocks collection", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		// Don't insert any RefIndex entry — simulates startup before loader runs

		sm.rebuildAllSegmentProtection()

		assert.True(t, sm.IsCollectionCompactionBlocked(100))
	})
}

func TestSnapshotMeta_SetClearSnapshotPending(t *testing.T) {
	t.Run("set snapshot pending blocks collection compaction", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		assert.False(t, sm.IsCollectionCompactionBlocked(100))

		sm.SetSnapshotPending(100)
		assert.True(t, sm.IsCollectionCompactionBlocked(100))
		// Other collections unaffected
		assert.False(t, sm.IsCollectionCompactionBlocked(200))
	})

	t.Run("clear snapshot pending unblocks collection compaction", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)

		sm.SetSnapshotPending(100)
		assert.True(t, sm.IsCollectionCompactionBlocked(100))

		sm.ClearSnapshotPending(100)
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("snapshot pending does not interfere with RefIndex blocking", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex()) // not loaded
		sm.rebuildAllSegmentProtection()

		// Blocked by RefIndex
		assert.True(t, sm.IsCollectionCompactionBlocked(100))

		// Clear snapshot pending should NOT unblock (still blocked by RefIndex)
		sm.ClearSnapshotPending(100)
		assert.True(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("clear snapshot pending is idempotent", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		// Clear without setting should not panic
		sm.ClearSnapshotPending(100)
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("multiple collections can be snapshot pending simultaneously", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)

		sm.SetSnapshotPending(100)
		sm.SetSnapshotPending(200)
		assert.True(t, sm.IsCollectionCompactionBlocked(100))
		assert.True(t, sm.IsCollectionCompactionBlocked(200))
		assert.False(t, sm.IsCollectionCompactionBlocked(300))

		sm.ClearSnapshotPending(100)
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
		assert.True(t, sm.IsCollectionCompactionBlocked(200))
	})

	t.Run("set snapshot pending is idempotent", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)

		sm.SetSnapshotPending(100)
		sm.SetSnapshotPending(100) // double set should not panic
		assert.True(t, sm.IsCollectionCompactionBlocked(100))

		sm.ClearSnapshotPending(100) // single clear should unblock
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("concurrent set and clear", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		done := make(chan struct{})

		// Concurrent set/clear should not race
		go func() {
			for i := 0; i < 100; i++ {
				sm.SetSnapshotPending(100)
				sm.ClearSnapshotPending(100)
			}
			close(done)
		}()

		for i := 0; i < 100; i++ {
			sm.IsCollectionCompactionBlocked(100)
		}
		<-done
		// After all goroutines finish, should be unblocked
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})

	t.Run("blocked by both snapshot pending and RefIndex", func(t *testing.T) {
		sm := createTestSnapshotMetaLoaded(t)
		futureTs := uint64(time.Now().Unix()) + 3600
		info := &datapb.SnapshotInfo{
			Id:                   1,
			CollectionId:         100,
			CompactionExpireTime: futureTs,
		}
		sm.snapshotID2Info.Insert(1, info)
		sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex()) // not loaded
		sm.rebuildAllSegmentProtection()

		sm.SetSnapshotPending(100)
		assert.True(t, sm.IsCollectionCompactionBlocked(100))

		// Clear snapshot pending — still blocked by RefIndex
		sm.ClearSnapshotPending(100)
		assert.True(t, sm.IsCollectionCompactionBlocked(100))

		// Load RefIndex — now unblocked
		sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001}, nil))
		sm.rebuildAllSegmentProtection()
		assert.False(t, sm.IsCollectionCompactionBlocked(100))
	})
}

func TestSnapshotMeta_SaveSnapshotWithProtection(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	futureTs := uint64(time.Now().Unix()) + 3600
	snapshot := createTestSnapshotDataForMeta()
	snapshot.SnapshotInfo.Name = "protected_snapshot"
	snapshot.SnapshotInfo.Id = 10
	snapshot.SnapshotInfo.CompactionExpireTime = futureTs

	cleanup := saveTestSnapshots(t, sm, snapshot)
	defer cleanup()

	// Segments from the snapshot should be protected
	for _, seg := range snapshot.Segments {
		assert.True(t, sm.IsSegmentCompactionProtected(seg.GetSegmentId()),
			"segment %d should be protected", seg.GetSegmentId())
	}
}

// P0 regression: a TTL=0 snapshot must still pin its referenced segments and buildIDs
// against GC, even though it intentionally contributes nothing to compaction protection.
// Before the fix, SaveSnapshot only called updateSegmentProtection which returned early
// when CompactionExpireTime==0, leaving segmentReferencedByGC / buildIDReferencedByGC
// completely empty. That made a freshly-created TTL=0 snapshot immediately eligible for
// GC deletion.
func TestSnapshotMeta_SaveSnapshotWithoutProtection(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	snapshot := createTestSnapshotDataForMeta()
	snapshot.SnapshotInfo.Name = "no_protection_snapshot"
	snapshot.SnapshotInfo.Id = 10
	snapshot.SnapshotInfo.CompactionExpireTime = 0
	collID := snapshot.SnapshotInfo.GetCollectionId()

	cleanup := saveTestSnapshots(t, sm, snapshot)
	defer cleanup()

	// Compaction protection: NOT set (TTL=0 means snapshot does not block compaction).
	for _, seg := range snapshot.Segments {
		assert.False(t, sm.IsSegmentCompactionProtected(seg.GetSegmentId()),
			"segment %d must not be compaction-protected when TTL=0", seg.GetSegmentId())
	}

	// GC protection: MUST be set. GC protection is unconditional on TTL because the
	// snapshot still needs its files for PIT recovery regardless of compaction semantics.
	for _, seg := range snapshot.Segments {
		segID := seg.GetSegmentId()
		assert.True(t, sm.IsSegmentGCBlocked(collID, segID),
			"segment %d must be GC-blocked even when TTL=0", segID)
		assert.True(t, sm.segmentReferencedByGC.Contain(segID),
			"segmentReferencedByGC must contain segment %d", segID)
	}
	// Every buildID referenced by any index file type must be GC-blocked too.
	for _, seg := range snapshot.Segments {
		for _, idxFile := range seg.GetIndexFiles() {
			buildID := idxFile.GetBuildID()
			if buildID == 0 {
				continue
			}
			assert.True(t, sm.IsBuildIDGCBlocked(collID, buildID),
				"buildID %d must be GC-blocked even when TTL=0", buildID)
		}
	}
}

func TestSnapshotMeta_SaveSnapshot_RollbackProtectionOnCommitFailure(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	futureTs := uint64(time.Now().Unix()) + 3600
	snapshot := createTestSnapshotDataForMeta()
	snapshot.SnapshotInfo.Name = "rollback_snapshot"
	snapshot.SnapshotInfo.Id = 15
	snapshot.SnapshotInfo.CompactionExpireTime = futureTs

	segID := snapshot.Segments[0].GetSegmentId()

	// Mock S3 writer to succeed
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, s *SnapshotData) (string, error) {
		return fmt.Sprintf("s3://bucket/snapshots/%d/metadata.json", s.SnapshotInfo.GetId()), nil
	}).Build()
	defer mock1.UnPatch()

	// First catalog save (PENDING) succeeds, second (COMMITTED) fails
	callCount := 0
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, info *datapb.SnapshotInfo) error {
		callCount++
		if callCount == 1 {
			return nil // PENDING save succeeds
		}
		return errors.New("catalog commit failed") // COMMITTED save fails
	}).Build()
	defer mock2.UnPatch()

	// SaveSnapshot should fail
	err := sm.SaveSnapshot(context.Background(), snapshot)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "catalog commit failed")

	// Protection should be rolled back — segment should NOT be protected
	assert.False(t, sm.IsSegmentCompactionProtected(segID),
		"segment %d should not be protected after rollback", segID)

	// P0 regression: GC protection must also be rolled back. Before the fix, the rollback
	// path only called rebuildSegmentProtection (targeted, compaction-only), so GC sets
	// were never reverted — a failed SaveSnapshot would leak segment/buildID entries into
	// segmentReferencedByGC/buildIDReferencedByGC permanently.
	assert.False(t, sm.IsSegmentGCBlocked(snapshot.SnapshotInfo.GetCollectionId(), segID),
		"segment %d must not be GC-blocked after rollback", segID)
	assert.False(t, sm.segmentReferencedByGC.Contain(segID),
		"segmentReferencedByGC must not retain rolled-back snapshot's segments")

	// Snapshot should not be in memory
	_, exists := sm.snapshotID2Info.Get(snapshot.SnapshotInfo.GetId())
	assert.False(t, exists, "snapshot should not remain in memory after rollback")
}

func TestSnapshotMeta_DropSnapshot_ClearsProtection(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	// Create a snapshot with compaction protection
	futureTs := uint64(time.Now().Unix()) + 3600
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = "protected_drop"
	snapshotData.SnapshotInfo.Id = 20
	snapshotData.SnapshotInfo.CompactionExpireTime = futureTs

	cleanup := saveTestSnapshots(t, sm, snapshotData)
	cleanup() // unpatch save mocks

	// Verify protection is active
	segID := snapshotData.Segments[0].GetSegmentId()
	assert.True(t, sm.IsSegmentCompactionProtected(segID))

	// Mock catalog and writer for DropSnapshot
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mock0.UnPatch()
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mock2.UnPatch()

	// Drop the snapshot
	err := sm.DropSnapshot(ctx, 100, "protected_drop")
	assert.NoError(t, err)

	// Protection should be cleared since no remaining snapshots reference this segment
	assert.False(t, sm.IsSegmentCompactionProtected(segID),
		"segment %d should no longer be protected after snapshot drop", segID)
}

// P1 regression: a TTL=0 snapshot contributes to GC protection (unconditional on TTL)
// but not to compaction protection. DropSnapshot must rebuild state UNCONDITIONALLY,
// otherwise a drop of a TTL=0 snapshot would leave stale entries in segmentReferencedByGC
// and buildIDReferencedByGC, permanently pinning files against GC.
func TestSnapshotMeta_DropSnapshot_ClearsGCProtectionForTTL0(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = "ttl0_drop"
	snapshotData.SnapshotInfo.Id = 21
	snapshotData.SnapshotInfo.CompactionExpireTime = 0 // no compaction TTL

	cleanup := saveTestSnapshots(t, sm, snapshotData)
	cleanup()

	segID := snapshotData.Segments[0].GetSegmentId()
	collID := snapshotData.SnapshotInfo.GetCollectionId()

	// Preconditions: GC protection registered despite TTL=0; no compaction protection.
	assert.True(t, sm.IsSegmentGCBlocked(collID, segID),
		"precondition: TTL=0 snapshot must still pin segment against GC")
	assert.False(t, sm.IsSegmentCompactionProtected(segID),
		"precondition: TTL=0 snapshot must not set compaction protection")

	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mock0.UnPatch()
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mock2.UnPatch()

	err := sm.DropSnapshot(ctx, 100, "ttl0_drop")
	assert.NoError(t, err)

	// GC protection must be cleared since no other snapshot references this segment.
	assert.False(t, sm.IsSegmentGCBlocked(collID, segID),
		"segment %d must be GC-unblocked after dropping the only snapshot referencing it", segID)
	assert.False(t, sm.segmentReferencedByGC.Contain(segID),
		"segmentReferencedByGC must not retain dropped snapshot's segments")
}

func TestSnapshotMeta_DropSnapshot_RetainsProtectionFromOtherSnapshot(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	futureTs1 := uint64(time.Now().Unix()) + 3600
	futureTs2 := uint64(time.Now().Unix()) + 7200

	// Create two snapshots referencing overlapping segments
	snap1 := createTestSnapshotDataForMeta()
	snap1.SnapshotInfo.Name = "snap1"
	snap1.SnapshotInfo.Id = 30
	snap1.SnapshotInfo.CompactionExpireTime = futureTs1

	snap2 := createTestSnapshotDataForMeta()
	snap2.SnapshotInfo.Name = "snap2"
	snap2.SnapshotInfo.Id = 31
	snap2.SnapshotInfo.CompactionExpireTime = futureTs2

	cleanup := saveTestSnapshots(t, sm, snap1, snap2)
	cleanup()

	segID := snap1.Segments[0].GetSegmentId()
	assert.True(t, sm.IsSegmentCompactionProtected(segID))

	// Drop snap1, snap2 still protects the same segment
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mock0.UnPatch()
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mock2.UnPatch()

	err := sm.DropSnapshot(ctx, 100, "snap1")
	assert.NoError(t, err)

	// Protection should be retained from snap2 (with higher expiry)
	assert.True(t, sm.IsSegmentCompactionProtected(segID),
		"segment %d should still be protected by snap2", segID)
	assert.Equal(t, futureTs2, sm.segmentProtectionUntil[segID])
}

// Regression for PR #48227 review comment #3: if a protected snapshot's RefIndex
// fails to load, the user must still be able to self-rescue a collection from the
// fail-closed compactionBlockedCollections state by dropping the snapshot. Before
// the fix, DropSnapshot skipped protection rebuild when RefIndex was not loaded,
// leaving the collection-level block imprint in place.
func TestSnapshotMeta_DropSnapshot_ClearsBlockWhenRefIndexFailed(t *testing.T) {
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)

	futureTs := uint64(time.Now().Unix()) + 3600
	collectionID := int64(9001)
	info := &datapb.SnapshotInfo{
		Id:                   42,
		Name:                 "stuck_snapshot",
		CollectionId:         collectionID,
		CompactionExpireTime: futureTs,
	}

	// Insert snapshot with a RefIndex that is explicitly Failed (simulating S3 load failure).
	sm.snapshotID2Info.Insert(info.GetId(), info)
	failedRef := NewSnapshotRefIndex()
	failedRef.SetFailed()
	sm.snapshotID2RefIndex.Insert(info.GetId(), failedRef)
	sm.addToSecondaryIndexes(info)

	// Simulate what refIndexLoaderLoop / newSnapshotMeta would do on startup:
	// a failed RefIndex with active protection imprints a collection-level block.
	sm.rebuildAllSegmentProtection()
	assert.True(t, sm.IsCollectionCompactionBlocked(collectionID),
		"precondition: collection must be blocked before drop")

	// Drop the snapshot. Even though RefIndex never loaded, DropSnapshot must
	// clear the collection-level block so the user can self-rescue.
	mock0 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mock0.UnPatch()
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mock2.UnPatch()

	err := sm.DropSnapshot(ctx, collectionID, "stuck_snapshot")
	assert.NoError(t, err)

	assert.False(t, sm.IsCollectionCompactionBlocked(collectionID),
		"collection must be unblocked after dropping the stuck snapshot")
}

// Regression for PR #48227 review comment #2: newSnapshotMeta must populate
// compactionBlockedCollections before returning, otherwise DataCoord starts serving
// compaction during the startup race window with an empty block set (fail-OPEN).
//
// The fail-closed guarantee is achieved by a pure in-memory synchronous
// rebuildAllSegmentProtection call in newSnapshotMeta that imprints a collection-level
// block for every snapshot whose RefIndex is not yet Loaded. At startup all RefIndexes
// are in Pending state (set by reload()), so every protected collection is blocked
// without any S3 I/O. This test asserts the block is in place immediately after
// newSnapshotMeta returns, with no sleep or wait.
func TestNewSnapshotMeta_BlockStateReadyBeforeReturn(t *testing.T) {
	ctx := context.Background()
	catalog := &kv_datacoord.Catalog{}

	futureTs := uint64(time.Now().Unix()) + 3600
	protected := &datapb.SnapshotInfo{
		Id:                   7,
		Name:                 "proto_blocked",
		CollectionId:         777,
		CompactionExpireTime: futureTs,
		State:                datapb.SnapshotState_SnapshotStateCommitted,
		S3Location:           "s3://bucket/snapshots/777/metadata/7.json",
	}
	// Catalog reload returns one active snapshot.
	mockList := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).Return(
		[]*datapb.SnapshotInfo{protected}, nil).Build()
	defer mockList.UnPatch()

	// Make the background loader's ReadSnapshot calls fail deterministically so the
	// assertion below is race-free: even if the async loader goroutine races ahead of
	// our assertion, a Failed RefIndex still keeps the collection blocked (IsLoaded()
	// returns false), which is the exact same state a Pending RefIndex produces.
	mockRead := mockey.Mock((*SnapshotReader).ReadSnapshot).Return(
		nil, errors.New("simulated S3 load failure")).Build()
	defer mockRead.UnPatch()

	tempDir := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	sm, err := newSnapshotMeta(ctx, catalog, cm)
	assert.NoError(t, err)
	defer sm.Close()

	// Block must already be in place before newSnapshotMeta returns, regardless of
	// whether the background loader goroutine has scheduled yet.
	assert.True(t, sm.IsCollectionCompactionBlocked(777),
		"collection must be blocked synchronously inside newSnapshotMeta")
}

// ----------------------------------------------------------------------------
// Unified snapshot protection: GC protection via O(1) precomputed state.
// ----------------------------------------------------------------------------

// TestSnapshotMeta_IsSegmentGCBlocked_PreciseProtection verifies that once RefIndex
// loading completes, IsSegmentGCBlocked returns the precise per-segment answer.
// It must be TRUE for segments referenced by any existing snapshot (regardless of
// CompactionExpireTime) and FALSE otherwise.
func TestSnapshotMeta_IsSegmentGCBlocked_PreciseProtection(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// Snapshot 1: active TTL + loaded refs.
	activeTTL := uint64(time.Now().Unix()) + 3600
	sm.snapshotID2Info.Insert(1, &datapb.SnapshotInfo{
		Id: 1, CollectionId: 100, CompactionExpireTime: activeTTL,
	})
	sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001, 1002}, []int64{2001}))

	// Snapshot 2: EXPIRED TTL but loaded refs. GC must still protect these.
	expiredTTL := uint64(time.Now().Unix()) - 100
	sm.snapshotID2Info.Insert(2, &datapb.SnapshotInfo{
		Id: 2, CollectionId: 200, CompactionExpireTime: expiredTTL,
	})
	sm.snapshotID2RefIndex.Insert(2, NewLoadedSnapshotRefIndex([]int64{1003}, []int64{2002}))

	sm.rebuildAllSegmentProtection()

	// Active-TTL snapshot contributes to BOTH compaction and GC protection.
	assert.True(t, sm.IsSegmentGCBlocked(100, 1001), "active-TTL segment must be GC-blocked")
	assert.True(t, sm.IsSegmentGCBlocked(100, 1002), "active-TTL segment must be GC-blocked")
	assert.True(t, sm.IsSegmentCompactionProtected(1001), "active-TTL segment must be compaction-protected")

	// Expired-TTL snapshot: GC protection stays, compaction protection goes away.
	assert.True(t, sm.IsSegmentGCBlocked(200, 1003),
		"expired-TTL segment must still be GC-blocked (snapshot still needs files)")
	assert.False(t, sm.IsSegmentCompactionProtected(1003),
		"expired-TTL segment must NOT be compaction-protected")

	// Unknown segment: no block.
	assert.False(t, sm.IsSegmentGCBlocked(100, 9999))
}

// TestSnapshotMeta_IsSegmentGCBlocked_FailClosedCollection verifies that when a
// collection has an unloaded (Pending or Failed) RefIndex, every segment in that
// collection is reported as blocked regardless of the precise referenced set.
func TestSnapshotMeta_IsSegmentGCBlocked_FailClosedCollection(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// Snapshot 1 (coll 100): RefIndex NOT loaded → coarse block.
	sm.snapshotID2Info.Insert(1, &datapb.SnapshotInfo{
		Id: 1, CollectionId: 100, CompactionExpireTime: uint64(time.Now().Unix()) + 3600,
	})
	sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex()) // Pending

	// Snapshot 2 (coll 200): loaded with precise refs.
	sm.snapshotID2Info.Insert(2, &datapb.SnapshotInfo{
		Id: 2, CollectionId: 200,
	})
	sm.snapshotID2RefIndex.Insert(2, NewLoadedSnapshotRefIndex([]int64{2001}, nil))

	sm.rebuildAllSegmentProtection()

	// Collection 100 has an unloaded RefIndex → every segment under it is blocked,
	// even ones we've never heard of.
	assert.True(t, sm.IsSegmentGCBlocked(100, 42), "unknown segment in blocked collection must be blocked")
	assert.True(t, sm.IsSegmentGCBlocked(100, 9999), "any segment in blocked collection must be blocked")

	// Collection 200 is fully loaded → precise lookup.
	assert.True(t, sm.IsSegmentGCBlocked(200, 2001), "referenced segment must be blocked")
	assert.False(t, sm.IsSegmentGCBlocked(200, 2002), "non-referenced segment must NOT be blocked")
}

// TestSnapshotMeta_IsSegmentGCBlocked_GlobalFailClosed verifies that passing
// collectionID < 0 triggers global fail-closed behavior: if ANY collection is in
// gcBlockedCollections, every segment is treated as blocked.
func TestSnapshotMeta_IsSegmentGCBlocked_GlobalFailClosed(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// One collection has an unloaded RefIndex.
	sm.snapshotID2Info.Insert(1, &datapb.SnapshotInfo{
		Id: 1, CollectionId: 100, CompactionExpireTime: uint64(time.Now().Unix()) + 3600,
	})
	sm.snapshotID2RefIndex.Insert(1, NewSnapshotRefIndex()) // Pending

	sm.rebuildAllSegmentProtection()

	// With collectionID = -1, any segment — even one clearly from a different
	// collection — must be fail-closed because at least one collection is unloaded.
	assert.True(t, sm.IsSegmentGCBlocked(-1, 42),
		"orphan segment walk must fail-closed while any collection is unloaded")
}

// TestSnapshotMeta_IsBuildIDGCBlocked_AllPaths exercises the three branches of
// IsBuildIDGCBlocked: loaded collection, fail-closed per-collection, fail-closed
// global (collectionID < 0).
func TestSnapshotMeta_IsBuildIDGCBlocked_AllPaths(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	// Loaded snapshot in coll 100 with buildIDs {3001, 3002}.
	sm.snapshotID2Info.Insert(1, &datapb.SnapshotInfo{Id: 1, CollectionId: 100})
	sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001}, []int64{3001, 3002}))

	// Unloaded snapshot in coll 200.
	sm.snapshotID2Info.Insert(2, &datapb.SnapshotInfo{
		Id: 2, CollectionId: 200, CompactionExpireTime: uint64(time.Now().Unix()) + 3600,
	})
	sm.snapshotID2RefIndex.Insert(2, NewSnapshotRefIndex()) // Pending → coll 200 fail-closed

	sm.rebuildAllSegmentProtection()

	// Per-collection loaded: precise buildID check.
	assert.True(t, sm.IsBuildIDGCBlocked(100, 3001), "referenced buildID must be blocked")
	assert.True(t, sm.IsBuildIDGCBlocked(100, 3002))
	assert.False(t, sm.IsBuildIDGCBlocked(100, 3999), "unknown buildID in loaded collection must NOT be blocked")

	// Per-collection fail-closed: every buildID under a blocked collection returns true.
	assert.True(t, sm.IsBuildIDGCBlocked(200, 3999), "buildID in blocked collection must be blocked")

	// Global fail-closed (collectionID = -1): any unloaded collection globally blocks.
	assert.True(t, sm.IsBuildIDGCBlocked(-1, 3999),
		"orphan buildID walk must fail-closed while any collection is unloaded")
}

// TestSnapshotMeta_RebuildAllSegmentProtection_UnifiedTwoDimensions verifies that
// the extended rebuildAllSegmentProtection function atomically populates BOTH the
// compaction protection state (TTL-bound) and the GC protection state (unconditional
// on TTL) in a single pass.
func TestSnapshotMeta_RebuildAllSegmentProtection_UnifiedTwoDimensions(t *testing.T) {
	sm := createTestSnapshotMetaLoaded(t)

	now := uint64(time.Now().Unix())

	// Active-TTL, loaded: contributes to BOTH compaction and GC state.
	sm.snapshotID2Info.Insert(1, &datapb.SnapshotInfo{
		Id: 1, CollectionId: 100, CompactionExpireTime: now + 3600,
	})
	sm.snapshotID2RefIndex.Insert(1, NewLoadedSnapshotRefIndex([]int64{1001}, []int64{2001}))

	// Expired-TTL, loaded: GC only (not compaction).
	sm.snapshotID2Info.Insert(2, &datapb.SnapshotInfo{
		Id: 2, CollectionId: 100, CompactionExpireTime: now - 100,
	})
	sm.snapshotID2RefIndex.Insert(2, NewLoadedSnapshotRefIndex([]int64{1002}, []int64{2002}))

	// Active-TTL, unloaded: both coarse blocks (compaction + GC).
	sm.snapshotID2Info.Insert(3, &datapb.SnapshotInfo{
		Id: 3, CollectionId: 200, CompactionExpireTime: now + 3600,
	})
	sm.snapshotID2RefIndex.Insert(3, NewSnapshotRefIndex()) // Pending

	// Zero TTL (never set), loaded: GC only. Does not affect compaction state at all.
	sm.snapshotID2Info.Insert(4, &datapb.SnapshotInfo{
		Id: 4, CollectionId: 300, CompactionExpireTime: 0,
	})
	sm.snapshotID2RefIndex.Insert(4, NewLoadedSnapshotRefIndex([]int64{1003}, []int64{2003}))

	sm.rebuildAllSegmentProtection()

	sm.segmentProtectionMu.RLock()
	defer sm.segmentProtectionMu.RUnlock()

	// Compaction state: only the active-TTL loaded snapshot contributes precisely,
	// and the active-TTL unloaded snapshot contributes a collection-level block.
	assert.Equal(t, 1, len(sm.segmentProtectionUntil),
		"only segment 1001 should have precise compaction protection")
	_, ok := sm.segmentProtectionUntil[1001]
	assert.True(t, ok)
	assert.True(t, sm.compactionBlockedCollections.Contain(200),
		"coll 200 must be compaction-blocked (active TTL + unloaded)")
	assert.False(t, sm.compactionBlockedCollections.Contain(100),
		"coll 100 must NOT be compaction-blocked (both snapshots are loaded)")
	assert.False(t, sm.compactionBlockedCollections.Contain(300),
		"coll 300 must NOT be compaction-blocked (no active TTL)")

	// GC state: every loaded snapshot contributes precise refs regardless of TTL,
	// and the unloaded snapshot contributes only a collection-level block.
	assert.True(t, sm.segmentReferencedByGC.Contain(1001), "loaded active-TTL seg must be in GC set")
	assert.True(t, sm.segmentReferencedByGC.Contain(1002), "loaded expired-TTL seg must be in GC set")
	assert.True(t, sm.segmentReferencedByGC.Contain(1003), "loaded zero-TTL seg must be in GC set")
	assert.False(t, sm.segmentReferencedByGC.Contain(9999), "unknown seg must NOT be in GC set")

	assert.True(t, sm.buildIDReferencedByGC.Contain(2001))
	assert.True(t, sm.buildIDReferencedByGC.Contain(2002))
	assert.True(t, sm.buildIDReferencedByGC.Contain(2003))

	assert.True(t, sm.gcBlockedCollections.Contain(200),
		"coll 200 must be GC-blocked (unloaded RefIndex)")
	assert.False(t, sm.gcBlockedCollections.Contain(100),
		"coll 100 must NOT be GC-blocked (all loaded)")
	assert.False(t, sm.gcBlockedCollections.Contain(300))
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
	_, err := sm.DropSnapshotsByCollection(ctx, int64(100))

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
	_, err := sm.DropSnapshotsByCollection(ctx, int64(999))

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
	_, err := sm.DropSnapshotsByCollection(ctx, int64(100))

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

	insertTestSnapshot(sm, snap1, []int64{1001}, nil)
	insertTestSnapshot(sm, snap2, []int64{1002}, nil)
	insertTestSnapshot(sm, snap3, []int64{1003}, nil)

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

	insertTestSnapshot(sm, snapA, []int64{3001}, nil)
	insertTestSnapshot(sm, snapB, []int64{3002}, nil)

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
	_, err = sm.DropSnapshotsByCollection(ctx, int64(100))

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
	_, err := sm.DropSnapshotsByCollection(ctx, int64(100))

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

// --- Snapshot-Level Pin/Unpin Tests ---

func TestSnapshotMeta_PinSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	// Setup: add a snapshot with no pins
	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)

	pinID1, _, err := sm.PinSnapshot(ctx, 100, "snap1", 0)
	assert.NoError(t, err)
	assert.NotZero(t, pinID1)
	current, _ := sm.snapshotID2Info.Get(1)
	assert.Len(t, current.GetPinIds(), 1)

	// Pin again — different pin ID
	pinID2, _, err := sm.PinSnapshot(ctx, 100, "snap1", 0)
	assert.NoError(t, err)
	assert.NotZero(t, pinID2)
	assert.NotEqual(t, pinID1, pinID2)
	current, _ = sm.snapshotID2Info.Get(1)
	assert.Len(t, current.GetPinIds(), 2)
}

func TestSnapshotMeta_PinSnapshot_NotFound(t *testing.T) {
	ctx := context.Background()

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	_, _, err := sm.PinSnapshot(ctx, 100, "nonexistent", 0)
	assert.Error(t, err)
}

func TestSnapshotMeta_PinSnapshot_WithTTL(t *testing.T) {
	ctx := context.Background()

	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)

	now := time.Now().UnixMilli()
	pinID, _, err := sm.PinSnapshot(ctx, 100, "snap1", 3600) // 1 hour TTL
	assert.NoError(t, err)
	assert.NotZero(t, pinID)
	current, _ := sm.snapshotID2Info.Get(1)
	assert.Len(t, current.GetPinIds(), 1)

	// Verify expiry was stored
	expireAt, exists := current.GetPinExpireAtMs()[pinID]
	assert.True(t, exists, "pin should have expiry entry")
	assert.InDelta(t, now+3600*1000, expireAt, 2000, "expiry should be ~1 hour from now")

	// Pin with no TTL — should NOT have expiry entry
	pinID2, _, err := sm.PinSnapshot(ctx, 100, "snap1", 0)
	assert.NoError(t, err)
	current, _ = sm.snapshotID2Info.Get(1)
	_, exists = current.GetPinExpireAtMs()[pinID2]
	assert.False(t, exists, "permanent pin should NOT have expiry entry")
}

func TestSnapshotMeta_CleanExpiredPins_CatalogError_Rollback(t *testing.T) {
	ctx := context.Background()

	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(errors.New("etcd error")).Build()
	defer mockSave.UnPatch()

	now := time.Now().UnixMilli()
	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{
		Id: 1, Name: "snap1", CollectionId: 100,
		PinIds:        []int64{5001, 5002},
		PinExpireAtMs: map[int64]int64{5001: now - 1000}, // 5001 expired, 5002 permanent
	}
	sm.snapshotID2Info.Insert(1, info)

	sm.cleanExpiredPinsForCollection(ctx, 100)

	// Catalog failure means the swap never happens — live pointer remains
	// untouched. PinIds AND PinExpireAtMs should both be intact.
	current, _ := sm.snapshotID2Info.Get(1)
	assert.Len(t, current.GetPinIds(), 2, "PinIds should be untouched on catalog failure")
	assert.Contains(t, current.GetPinIds(), int64(5001))
	expireAt, exists := current.GetPinExpireAtMs()[5001]
	assert.True(t, exists, "PinExpireAtMs should be untouched on catalog failure")
	assert.Equal(t, now-1000, expireAt)
}

func TestSnapshotMeta_UnpinSnapshot_Success(t *testing.T) {
	ctx := context.Background()

	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100, PinIds: []int64{5001, 5002}}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)
	// Pre-seed reverse index since we bypass PinSnapshot.
	sm.ensurePinMaps()
	sm.pinID2SnapshotID.Insert(5001, 1)
	sm.pinID2SnapshotID.Insert(5002, 1)

	_, _, _, err := sm.UnpinSnapshot(ctx, 5001)
	assert.NoError(t, err)
	current, _ := sm.snapshotID2Info.Get(1)
	assert.Len(t, current.GetPinIds(), 1)
	assert.Equal(t, int64(5002), current.GetPinIds()[0])
}

func TestSnapshotMeta_UnpinSnapshot_NotPinned(t *testing.T) {
	ctx := context.Background()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100}
	sm.snapshotID2Info.Insert(1, info)

	// Unpin a non-existent pin ID — should succeed (idempotent)
	_, _, _, err := sm.UnpinSnapshot(ctx, 99999)
	assert.NoError(t, err)
}

func TestSnapshotMeta_DropSnapshot_RejectPinned(t *testing.T) {
	ctx := context.Background()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100, PinIds: []int64{5001}}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)

	err := sm.DropSnapshot(ctx, 100, "snap1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pinned")
}

func TestSnapshotMeta_DropSnapshot_ExpiredPinAllowsDrop(t *testing.T) {
	ctx := context.Background()

	// Mock all catalog operations used by DropSnapshot
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()
	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()
	mockWriterDrop := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriterDrop.UnPatch()

	now := time.Now().UnixMilli()
	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		writer:                 &SnapshotWriter{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	// Snapshot with one expired pin — should NOT block drop
	info := &datapb.SnapshotInfo{
		Id: 1, Name: "snap1", CollectionId: 100,
		State:         datapb.SnapshotState_SnapshotStateCommitted,
		PinIds:        []int64{5001},
		PinExpireAtMs: map[int64]int64{5001: now - 1000}, // expired
	}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)
	snapSet := typeutil.NewUniqueSet()
	snapSet.Insert(1)
	sm.collectionID2Snapshots.Insert(100, snapSet)

	err := sm.DropSnapshot(ctx, 100, "snap1")
	assert.NoError(t, err, "expired pin should not block DropSnapshot")

	// Snapshot should be removed from memory
	_, exists := sm.snapshotID2Info.Get(1)
	assert.False(t, exists, "snapshot should be dropped")
}

func TestSnapshotMeta_DropSnapshot_CatalogFailure_RollbackPins(t *testing.T) {
	ctx := context.Background()

	now := time.Now().UnixMilli()
	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		writer:                 &SnapshotWriter{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	// Snapshot with 2 pins: one expired (5001), one active (5002)
	info := &datapb.SnapshotInfo{
		Id: 1, Name: "snap1", CollectionId: 100,
		State:  datapb.SnapshotState_SnapshotStateCommitted,
		PinIds: []int64{5001, 5002},
		PinExpireAtMs: map[int64]int64{
			5001: now - 1000, // expired
			5002: now + 999999999,
		},
	}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)

	// Mock catalog to fail — triggers rollback
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(errors.New("catalog fail")).Build()
	defer mockSave.UnPatch()

	// DropSnapshot will clean expired pin 5001, then reject because 5002 is still active.
	// Because 5002 is still active, DropSnapshot returns "pinned" error BEFORE catalog save.
	// The expired pin cleaning happens in-memory only but since we don't reach catalog save,
	// there's no rollback path to test here.
	//
	// To test the rollback path, we need ALL pins to be expired so DropSnapshot proceeds
	// to the catalog save step (which fails), then rolls back.
	info.PinIds = []int64{5001, 5002}
	info.PinExpireAtMs = map[int64]int64{
		5001: now - 1000, // expired
		5002: now - 500,  // also expired
	}
	info.State = datapb.SnapshotState_SnapshotStateCommitted

	err := sm.DropSnapshot(ctx, 100, "snap1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "catalog fail")

	// Verify rollback: both PinIds and PinExpireAtMs should be fully restored
	restored, exists := sm.snapshotID2Info.Get(1)
	require.True(t, exists, "snapshot should still be in memory after rollback")
	assert.Equal(t, datapb.SnapshotState_SnapshotStateCommitted, restored.State, "state should be rolled back")
	assert.Len(t, restored.PinIds, 2, "both pin IDs should be restored")
	assert.Contains(t, restored.PinIds, int64(5001))
	assert.Contains(t, restored.PinIds, int64(5002))
	assert.Len(t, restored.PinExpireAtMs, 2, "both pin expiry entries should be restored")
	assert.Equal(t, now-1000, restored.PinExpireAtMs[5001])
	assert.Equal(t, now-500, restored.PinExpireAtMs[5002])
}

func TestSnapshotMeta_DropSnapshotsByCollection_SkipPinned(t *testing.T) {
	ctx := context.Background()

	// Mock catalog operations used by DropSnapshot flow
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()
	mockDrop := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).Return(nil).Build()
	defer mockDrop.UnPatch()
	mockWriterDrop := mockey.Mock((*SnapshotWriter).Drop).Return(nil).Build()
	defer mockWriterDrop.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		writer:                 &SnapshotWriter{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	// Setup: 2 snapshots, one pinned, one not
	pinnedInfo := &datapb.SnapshotInfo{Id: 1, Name: "pinned_snap", CollectionId: 100, PinIds: []int64{5001}, State: datapb.SnapshotState_SnapshotStateCommitted}
	unpinnedInfo := &datapb.SnapshotInfo{Id: 2, Name: "unpinned_snap", CollectionId: 100, State: datapb.SnapshotState_SnapshotStateCommitted}
	sm.snapshotID2Info.Insert(1, pinnedInfo)
	sm.snapshotID2Info.Insert(2, unpinnedInfo)

	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("pinned_snap", 1)
	nameMap.Insert("unpinned_snap", 2)
	sm.snapshotName2ID.Insert(100, nameMap)

	snapSet := typeutil.NewUniqueSet()
	snapSet.Insert(1, 2)
	sm.collectionID2Snapshots.Insert(100, snapSet)

	_, err := sm.DropSnapshotsByCollection(ctx, 100)
	assert.NoError(t, err)

	// Pinned snapshot should still exist
	_, exists := sm.snapshotID2Info.Get(1)
	assert.True(t, exists, "pinned snapshot should survive collection drop")

	// Unpinned snapshot should be removed
	_, exists = sm.snapshotID2Info.Get(2)
	assert.False(t, exists, "unpinned snapshot should be dropped")
}

func TestSnapshotMeta_PinSnapshot_RejectDeleting(t *testing.T) {
	ctx := context.Background()

	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100, State: datapb.SnapshotState_SnapshotStateDeleting}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)

	_, _, err := sm.PinSnapshot(ctx, 100, "snap1", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "being deleted")
}

func TestSnapshotMeta_PinSnapshot_CatalogError_Rollback(t *testing.T) {
	ctx := context.Background()

	callCount := 0
	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(
		func(_ *kv_datacoord.Catalog, _ context.Context, _ *datapb.SnapshotInfo) error {
			callCount++
			return errors.New("etcd write failed")
		}).Build()
	defer mockSave.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100}
	sm.snapshotID2Info.Insert(1, info)
	nameMap := typeutil.NewConcurrentMap[string, UniqueID]()
	nameMap.Insert("snap1", 1)
	sm.snapshotName2ID.Insert(100, nameMap)

	_, _, err := sm.PinSnapshot(ctx, 100, "snap1", 0)
	assert.Error(t, err)
	// Clone-and-swap: on catalog failure the swap is skipped, so the live
	// pointer still has the original (empty) pin list.
	current, _ := sm.snapshotID2Info.Get(1)
	assert.Empty(t, current.GetPinIds(), "pin_ids should stay empty when catalog write fails")
}

func TestSnapshotMeta_UnpinSnapshot_CatalogError_Rollback(t *testing.T) {
	ctx := context.Background()

	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(errors.New("etcd write failed")).Build()
	defer mockSave.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	info := &datapb.SnapshotInfo{Id: 1, Name: "snap1", CollectionId: 100, PinIds: []int64{5001, 5002}}
	sm.snapshotID2Info.Insert(1, info)
	sm.ensurePinMaps()
	sm.pinID2SnapshotID.Insert(5001, 1)
	sm.pinID2SnapshotID.Insert(5002, 1)

	_, _, _, err := sm.UnpinSnapshot(ctx, 5001)
	assert.Error(t, err)
	// Clone-and-swap: on catalog failure the swap is skipped, so the live
	// pointer still carries both pin IDs.
	current, _ := sm.snapshotID2Info.Get(1)
	assert.Len(t, current.GetPinIds(), 2, "pin_ids should be untouched on catalog failure")
	assert.Contains(t, current.GetPinIds(), int64(5001))
}

func TestSnapshotMeta_CleanExpiredPinsForCollection(t *testing.T) {
	ctx := context.Background()

	mockSave := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).Return(nil).Build()
	defer mockSave.UnPatch()

	sm := &snapshotMeta{
		catalog:                &kv_datacoord.Catalog{},
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}

	now := time.Now().UnixMilli()

	// Snapshot with 2 pins: one expired, one active
	mixed := &datapb.SnapshotInfo{
		Id: 1, Name: "mixed", CollectionId: 100,
		PinIds:        []int64{5001, 5002},
		PinExpireAtMs: map[int64]int64{5001: now - 1000, 5002: now + 3600000}, // 5001 expired, 5002 active
	}
	// Snapshot with permanent pin (no TTL)
	permanent := &datapb.SnapshotInfo{
		Id: 2, Name: "permanent", CollectionId: 100,
		PinIds: []int64{6001},
		// no entry in PinExpireAtMs = permanent
	}
	// Different collection
	other := &datapb.SnapshotInfo{
		Id: 3, Name: "other", CollectionId: 200,
		PinIds:        []int64{7001},
		PinExpireAtMs: map[int64]int64{7001: now - 1000}, // expired but different collection
	}
	sm.snapshotID2Info.Insert(1, mixed)
	sm.snapshotID2Info.Insert(2, permanent)
	sm.snapshotID2Info.Insert(3, other)

	// Setup collection index
	snapSet100 := typeutil.NewUniqueSet()
	snapSet100.Insert(1, 2)
	sm.collectionID2Snapshots.Insert(100, snapSet100)
	snapSet200 := typeutil.NewUniqueSet()
	snapSet200.Insert(3)
	sm.collectionID2Snapshots.Insert(200, snapSet200)

	sm.cleanExpiredPinsForCollection(ctx, 100)

	// 5001 expired → removed; 5002 active → kept
	updatedMixed, _ := sm.snapshotID2Info.Get(1)
	assert.Len(t, updatedMixed.GetPinIds(), 1)
	assert.Equal(t, int64(5002), updatedMixed.GetPinIds()[0])
	// Permanent pin untouched
	updatedPermanent, _ := sm.snapshotID2Info.Get(2)
	assert.Len(t, updatedPermanent.GetPinIds(), 1)
	// Other collection untouched
	updatedOther, _ := sm.snapshotID2Info.Get(3)
	assert.Len(t, updatedOther.GetPinIds(), 1)
}

// TestSnapshotMeta_HasActivePins exercises the three branches of
// snapshotMeta.HasActivePins directly against a real snapshotMeta instance.
// This method is the lynchpin of the service-layer DropSnapshot pre-flight
// check and the only place where the pinMu read lock and expired-pin
// filtering actually execute end-to-end, so it needs coverage that doesn't
// mock out the implementation.
func TestSnapshotMeta_HasActivePins(t *testing.T) {
	newMeta := func() *snapshotMeta {
		return &snapshotMeta{
			snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
			snapshotName2ID:        typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[string, UniqueID]](),
			collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
		}
	}
	addSnapshot := func(sm *snapshotMeta, info *datapb.SnapshotInfo) {
		sm.snapshotID2Info.Insert(info.GetId(), info)
		nameMap, _ := sm.snapshotName2ID.GetOrInsert(
			info.GetCollectionId(),
			typeutil.NewConcurrentMap[string, UniqueID](),
		)
		nameMap.Insert(info.GetName(), info.GetId())
	}

	t.Run("snapshot_not_found_returns_error", func(t *testing.T) {
		ctx := context.Background()
		sm := newMeta()

		has, err := sm.HasActivePins(ctx, 100, "nonexistent")
		assert.Error(t, err)
		assert.False(t, has, "missing snapshot must report no pins")
	})

	t.Run("no_pins_returns_false", func(t *testing.T) {
		ctx := context.Background()
		sm := newMeta()
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 1, Name: "snap_empty", CollectionId: 100,
		})

		has, err := sm.HasActivePins(ctx, 100, "snap_empty")
		assert.NoError(t, err)
		assert.False(t, has, "snapshot with no pins must report no active pins")
	})

	t.Run("active_pin_returns_true", func(t *testing.T) {
		ctx := context.Background()
		sm := newMeta()
		now := time.Now().UnixMilli()
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 2, Name: "snap_active", CollectionId: 100,
			PinIds:        []int64{5001},
			PinExpireAtMs: map[int64]int64{5001: now + 3600*1000}, // 1h from now
		})

		has, err := sm.HasActivePins(ctx, 100, "snap_active")
		assert.NoError(t, err)
		assert.True(t, has, "non-expired pin must count as active")
	})

	t.Run("permanent_pin_returns_true", func(t *testing.T) {
		// Permanent pins have no entry in PinExpireAtMs and must count as
		// active forever. Regression guard for any "if expireAt == 0" bug.
		ctx := context.Background()
		sm := newMeta()
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 3, Name: "snap_permanent", CollectionId: 100,
			PinIds: []int64{6001},
			// PinExpireAtMs intentionally nil
		})

		has, err := sm.HasActivePins(ctx, 100, "snap_permanent")
		assert.NoError(t, err)
		assert.True(t, has, "permanent pin must count as active")
	})

	t.Run("only_expired_pins_returns_false", func(t *testing.T) {
		// Expired-pin filtering must not report a snapshot as pinned if every
		// pin is already past its TTL. This is the happy path for the service-
		// layer pre-flight check to allow drop to proceed.
		ctx := context.Background()
		sm := newMeta()
		now := time.Now().UnixMilli()
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 4, Name: "snap_expired", CollectionId: 100,
			PinIds:        []int64{7001, 7002},
			PinExpireAtMs: map[int64]int64{7001: now - 1000, 7002: now - 500},
		})

		has, err := sm.HasActivePins(ctx, 100, "snap_expired")
		assert.NoError(t, err)
		assert.False(t, has, "all-expired pins must not report active")
	})

	t.Run("mixed_expired_and_active_returns_true", func(t *testing.T) {
		ctx := context.Background()
		sm := newMeta()
		now := time.Now().UnixMilli()
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 5, Name: "snap_mixed", CollectionId: 100,
			PinIds:        []int64{8001, 8002},
			PinExpireAtMs: map[int64]int64{8001: now - 1000, 8002: now + 3600*1000},
		})

		has, err := sm.HasActivePins(ctx, 100, "snap_mixed")
		assert.NoError(t, err)
		assert.True(t, has, "at least one non-expired pin must report active")
	})

	t.Run("cross_collection_isolation", func(t *testing.T) {
		// Two snapshots in different collections reusing the same name: the
		// query must hit the (collectionID, name) tuple, not a global name
		// index. Regression guard for the pre-composite-key behavior where
		// HasActivePins on collection=100 could observe pins on collection=200.
		ctx := context.Background()
		sm := newMeta()
		now := time.Now().UnixMilli()
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 6, Name: "shared_name", CollectionId: 100,
		})
		addSnapshot(sm, &datapb.SnapshotInfo{
			Id: 7, Name: "shared_name", CollectionId: 200,
			PinIds:        []int64{9001},
			PinExpireAtMs: map[int64]int64{9001: now + 3600*1000},
		})

		has100, err := sm.HasActivePins(ctx, 100, "shared_name")
		assert.NoError(t, err)
		assert.False(t, has100, "collection 100 has no pins on this name")

		has200, err := sm.HasActivePins(ctx, 200, "shared_name")
		assert.NoError(t, err)
		assert.True(t, has200, "collection 200 has an active pin")
	})
}
