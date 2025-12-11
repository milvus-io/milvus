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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	kv_datacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

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
			},
		},
		Indexes: []*indexpb.IndexInfo{
			{
				IndexID: 2001,
			},
		},
	}
}

func createTestSnapshotDataInfo() *SnapshotDataInfo {
	return &SnapshotDataInfo{
		snapshotInfo: createTestSnapshotInfoForMeta(),
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}
}

func createTestSnapshotMeta(t *testing.T) *snapshotMeta {
	// Create empty Catalog for mockey to mock
	catalog := &kv_datacoord.Catalog{}

	// Use temporary directory to avoid polluting project directory
	tempDir := t.TempDir()
	tempChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))

	return &snapshotMeta{
		catalog:             catalog,
		snapshotID2DataInfo: typeutil.NewConcurrentMap[typeutil.UniqueID, *SnapshotDataInfo](),
		reader:              NewSnapshotReader(tempChunkManager),
		writer:              NewSnapshotWriter(tempChunkManager),
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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{snapshotInfo: snapshot1.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{snapshotInfo: snapshot2.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{snapshotInfo: snapshot1.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{snapshotInfo: snapshot2.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

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

	sm := createTestSnapshotMeta(t)

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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{snapshotInfo: snapshotData.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

	// Act
	result, err := sm.GetSnapshot(ctx, snapshotName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshotData.SnapshotInfo, result)
}

func TestSnapshotMeta_GetSnapshot_NotFound(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "nonexistent_snapshot"

	sm := createTestSnapshotMeta(t)

	// Act
	result, err := sm.GetSnapshot(ctx, snapshotName)

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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{snapshotInfo: snapshotData.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

	// Act
	result, err := sm.getSnapshotByName(ctx, snapshotName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshotData.SnapshotInfo, result)
}

func TestSnapshotMeta_GetSnapshotByName_NotFound(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "nonexistent_snapshot"

	sm := createTestSnapshotMeta(t)

	// Act
	result, err := sm.getSnapshotByName(ctx, snapshotName)

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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{snapshotInfo: snapshot1.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{snapshotInfo: snapshot2.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})
	sm.snapshotID2DataInfo.Insert(3, &SnapshotDataInfo{snapshotInfo: snapshot3.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

	// Act
	result, err := sm.getSnapshotByName(ctx, targetName)

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
	dataInfo1 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo1,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2
	dataInfo2 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo2,
		SegmentIDs:   typeutil.NewUniqueSet(1003, 1004),
		IndexIDs:     typeutil.NewUniqueSet(2002),
	}

	snapshotInfo3 := createTestSnapshotInfoForMeta()
	snapshotInfo3.CollectionId = 200 // Different collection
	snapshotInfo3.Id = 3
	dataInfo3 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo3,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2003),
	}

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, dataInfo1)
	sm.snapshotID2DataInfo.Insert(2, dataInfo2)
	sm.snapshotID2DataInfo.Insert(3, dataInfo3)

	// Act
	snapshotIDs := sm.GetSnapshotBySegment(ctx, collectionID, segmentID)

	// Assert
	assert.Len(t, snapshotIDs, 1)
	assert.Contains(t, snapshotIDs, UniqueID(1)) // Only dataInfo1 matches
}

func TestSnapshotMeta_GetSnapshotBySegment_EmptyResult(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	segmentID := UniqueID(9999) // Non-existent segment

	snapshotInfo := createTestSnapshotInfoForMeta()
	snapshotInfo.CollectionId = 100
	snapshotInfo.Id = 1
	dataInfo := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, dataInfo)

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
	dataInfo1 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo1,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2
	dataInfo2 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo2,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1003),
		IndexIDs:     typeutil.NewUniqueSet(2002),
	}

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, dataInfo1)
	sm.snapshotID2DataInfo.Insert(2, dataInfo2)

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
	dataInfo1 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo1,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2
	dataInfo2 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo2,
		SegmentIDs:   typeutil.NewUniqueSet(1002),
		IndexIDs:     typeutil.NewUniqueSet(2003, 2004),
	}

	snapshotInfo3 := createTestSnapshotInfoForMeta()
	snapshotInfo3.CollectionId = 200 // Different collection
	snapshotInfo3.Id = 3
	dataInfo3 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo3,
		SegmentIDs:   typeutil.NewUniqueSet(1003),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, dataInfo1)
	sm.snapshotID2DataInfo.Insert(2, dataInfo2)
	sm.snapshotID2DataInfo.Insert(3, dataInfo3)

	// Act
	snapshotIDs := sm.GetSnapshotByIndex(ctx, collectionID, indexID)

	// Assert
	assert.Len(t, snapshotIDs, 1)
	assert.Contains(t, snapshotIDs, UniqueID(1)) // Only dataInfo1 matches
}

func TestSnapshotMeta_GetSnapshotByIndex_EmptyResult(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := UniqueID(100)
	indexID := UniqueID(9999) // Non-existent index

	snapshotInfo := createTestSnapshotInfoForMeta()
	snapshotInfo.CollectionId = 100
	snapshotInfo.Id = 1
	dataInfo := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, dataInfo)

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
	dataInfo1 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo1,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}

	snapshotInfo2 := createTestSnapshotInfoForMeta()
	snapshotInfo2.CollectionId = 100
	snapshotInfo2.Id = 2
	dataInfo2 := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo2,
		SegmentIDs:   typeutil.NewUniqueSet(1002),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2003),
	}

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, dataInfo1)
	sm.snapshotID2DataInfo.Insert(2, dataInfo2)

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
	assert.Equal(t, mockCatalog, sm.catalog)
	assert.NotNil(t, sm.snapshotID2DataInfo)
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

func TestNewSnapshotMeta_ReaderError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	mockCatalog := &kv_datacoord.Catalog{}
	tempDir := t.TempDir()
	mockChunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(tempDir))
	snapshotInfo := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "test_snapshot",
	}
	expectedErr := errors.New("reader failed")

	// Mock catalog.ListSnapshots to return snapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{snapshotInfo}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot to return error
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, path string, includeSegments bool) (*SnapshotData, error) {
		return nil, expectedErr
	}).Build()
	defer mock2.UnPatch()

	// Act
	sm, err := newSnapshotMeta(ctx, mockCatalog, mockChunkManager)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, sm)
	assert.Contains(t, err.Error(), "reader failed")
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
	// Verify snapshot was inserted into map
	value, exists := sm.snapshotID2DataInfo.Get(snapshotData.SnapshotInfo.Id)
	assert.True(t, exists)
	assert.Equal(t, snapshotData.SnapshotInfo, value.snapshotInfo)
	// Verify segment and index IDs from fast path
	assert.True(t, value.SegmentIDs.Contain(1001))
	assert.True(t, value.IndexIDs.Contain(2001))
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

	// Verify all snapshots were loaded
	for _, snapshotInfo := range snapshotInfos {
		value, exists := sm.snapshotID2DataInfo.Get(snapshotInfo.Id)
		assert.True(t, exists, "snapshot %d should be loaded", snapshotInfo.Id)
		assert.Equal(t, snapshotInfo, value.snapshotInfo)

		// Verify segment and index IDs are correctly loaded from pre-computed lists
		expectedData, _ := snapshotDataMap.Load(snapshotInfo.Id)
		assert.Equal(t, len(expectedData.(*SnapshotData).SegmentIDs), value.SegmentIDs.Len())
		assert.Equal(t, len(expectedData.(*SnapshotData).IndexIDs), value.IndexIDs.Len())
	}
}

func TestSnapshotMeta_Reload_Concurrent_PartialFailure(t *testing.T) {
	// Test that if one snapshot fails to load, the entire reload fails
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

	expectedErr := errors.New("s3 read error")
	// Mock SnapshotReader.ReadSnapshot - fail on second snapshot (s3://bucket/1002)
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, metadataFilePath string, includeSegments bool) (*SnapshotData, error) {
		if metadataFilePath == "s3://bucket/1002" {
			return nil, expectedErr
		}
		// Find snapshot by S3Location
		for _, info := range snapshotInfos {
			if info.S3Location == metadataFilePath {
				return &SnapshotData{
					SnapshotInfo: &datapb.SnapshotInfo{Id: info.Id, CollectionId: info.CollectionId, S3Location: info.S3Location},
					Segments:     []*datapb.SegmentDescription{{SegmentId: info.Id * 10}},
					Indexes:      []*indexpb.IndexInfo{{IndexID: info.Id * 100}},
				}, nil
			}
		}
		return nil, errors.New("snapshot not found")
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "s3 read error")
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
	// Verify map is empty
	assert.Equal(t, 0, sm.snapshotID2DataInfo.Len())
}

// --- SaveSnapshot Tests (Mockey-based) ---

func TestSnapshotMeta_SaveSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
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
	// Verify snapshot data info was inserted
	dataInfo, exists := sm.snapshotID2DataInfo.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)
	assert.Equal(t, int64(100), dataInfo.snapshotInfo.CollectionId)
	assert.True(t, dataInfo.SegmentIDs.Contain(1001))
	assert.True(t, dataInfo.IndexIDs.Contain(2001))
	// Verify S3Location was set
	assert.Equal(t, metadataFilePath, snapshotData.SnapshotInfo.S3Location)
}

func TestSnapshotMeta_SaveSnapshot_WriterError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
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
	sm := createTestSnapshotMeta(t)
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
	sm := createTestSnapshotMeta(t)
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
	_, exists := sm.snapshotID2DataInfo.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists, "snapshot should not be in memory after Phase 2 failure")
}

// --- DropSnapshot Tests (Mockey-based) ---

func TestSnapshotMeta_DropSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName

	// Insert snapshot into map
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{snapshotInfo: snapshotData.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

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
	err := sm.DropSnapshot(ctx, snapshotName)

	// Assert
	assert.NoError(t, err)
	// Verify snapshot was removed from data info map
	_, exists := sm.snapshotID2DataInfo.Get(snapshotData.SnapshotInfo.GetId())
	assert.False(t, exists)
}

func TestSnapshotMeta_DropSnapshot_NotFound_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
	snapshotName := "nonexistent_snapshot"

	// Act
	err := sm.DropSnapshot(ctx, snapshotName)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSnapshotMeta_DropSnapshot_CatalogError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName
	expectedErr := errors.New("catalog drop failed")

	// Insert snapshot into map
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{snapshotInfo: snapshotData.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

	// Mock catalog.DropSnapshot to return error
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		return expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.DropSnapshot(ctx, snapshotName)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotMeta_DropSnapshot_WriterError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName
	expectedErr := errors.New("writer drop failed")

	// Insert snapshot into map
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{snapshotInfo: snapshotData.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

	// Mock catalog.DropSnapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Drop to return error - now takes metadataFilePath
	mock2 := mockey.Mock((*SnapshotWriter).Drop).To(func(ctx context.Context, metadataFilePath string) error {
		return expectedErr
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.DropSnapshot(ctx, snapshotName)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// --- Data Structure Tests ---

func TestSnapshotDataInfo_Creation(t *testing.T) {
	// Arrange & Act
	dataInfo := createTestSnapshotDataInfo()

	// Assert
	assert.Equal(t, int64(100), dataInfo.snapshotInfo.CollectionId)
	assert.True(t, dataInfo.SegmentIDs.Contain(1001))
	assert.True(t, dataInfo.SegmentIDs.Contain(1002))
	assert.True(t, dataInfo.IndexIDs.Contain(2001))
	assert.True(t, dataInfo.IndexIDs.Contain(2002))
	assert.Equal(t, 2, dataInfo.SegmentIDs.Len())
	assert.Equal(t, 2, dataInfo.IndexIDs.Len())
}

func TestSnapshotDataInfo_EmptySets(t *testing.T) {
	// Arrange & Act
	snapshotInfo := createTestSnapshotInfoForMeta()
	snapshotInfo.CollectionId = 100
	dataInfo := &SnapshotDataInfo{
		snapshotInfo: snapshotInfo,
		SegmentIDs:   typeutil.NewUniqueSet(),
		IndexIDs:     typeutil.NewUniqueSet(),
	}

	// Assert
	assert.Equal(t, int64(100), dataInfo.snapshotInfo.CollectionId)
	assert.Equal(t, 0, dataInfo.SegmentIDs.Len())
	assert.Equal(t, 0, dataInfo.IndexIDs.Len())
	assert.False(t, dataInfo.SegmentIDs.Contain(1001))
	assert.False(t, dataInfo.IndexIDs.Contain(2001))
}

// --- Concurrent Operations Tests ---

func TestSnapshotMeta_ConcurrentOperations(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta(t)

	// Test concurrent map operations don't panic
	snapshotData := createTestSnapshotDataForMeta()

	// Act - simulate concurrent operations
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{
		snapshotInfo: snapshotData.SnapshotInfo,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	})

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
	sm := createTestSnapshotMeta(t)

	// Act
	snapshots, err := sm.ListSnapshots(ctx, 100, 1)
	segmentSnapshots := sm.GetSnapshotBySegment(ctx, 100, 1001)
	indexSnapshots := sm.GetSnapshotByIndex(ctx, 100, 2001)
	snapshot, getErr := sm.GetSnapshot(ctx, "nonexistent")

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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{snapshotInfo: snapshot1.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{snapshotInfo: snapshot2.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

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

	sm := createTestSnapshotMeta(t)
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{snapshotInfo: snapshot1.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{snapshotInfo: snapshot2.SnapshotInfo, SegmentIDs: typeutil.NewUniqueSet(), IndexIDs: typeutil.NewUniqueSet()})

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

	// Verify snapshot was loaded (with empty ID sets for legacy format)
	value, exists := sm.snapshotID2DataInfo.Get(snapshotInfo.Id)
	assert.True(t, exists)
	assert.Equal(t, snapshotInfo, value.snapshotInfo)
	// Legacy snapshots will have empty ID sets
	assert.Equal(t, 0, value.SegmentIDs.Len())
	assert.Equal(t, 0, value.IndexIDs.Len())
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

	// Verify each snapshot was read exactly once (no slow path fallback)
	newCount, _ := callCounts.Load(newFormatInfo.S3Location)
	legacyCount, _ := callCounts.Load(legacyFormatInfo.S3Location)
	assert.Equal(t, int32(1), *newCount.(*int32))
	assert.Equal(t, int32(1), *legacyCount.(*int32))

	// Verify new format snapshot has pre-computed IDs
	newValue, exists := sm.snapshotID2DataInfo.Get(1001)
	assert.True(t, exists)
	assert.Equal(t, 2, newValue.SegmentIDs.Len())
	assert.True(t, newValue.SegmentIDs.Contain(10010))
	assert.True(t, newValue.SegmentIDs.Contain(10011))
	assert.Equal(t, 1, newValue.IndexIDs.Len())
	assert.True(t, newValue.IndexIDs.Contain(100100))

	// Verify legacy format snapshot has empty ID sets
	legacyValue, exists := sm.snapshotID2DataInfo.Get(1002)
	assert.True(t, exists)
	assert.Equal(t, 0, legacyValue.SegmentIDs.Len())
	assert.Equal(t, 0, legacyValue.IndexIDs.Len())
}
