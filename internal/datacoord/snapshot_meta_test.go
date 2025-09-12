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
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}
}

func createTestSnapshotMeta() *snapshotMeta {
	// Create empty Catalog for mockey to mock
	catalog := &kv_datacoord.Catalog{}

	return &snapshotMeta{
		catalog:             catalog,
		snapshotID2DataInfo: typeutil.NewConcurrentMap[typeutil.UniqueID, *SnapshotDataInfo](),
		reader:              NewSnapshotReader(&storage.LocalChunkManager{}),
		writer:              NewSnapshotWriter(&storage.LocalChunkManager{}),
	}
}

func createTestSnapshotMetaWithTempDir(t *testing.T) *snapshotMeta {
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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{SnapshotData: snapshot1})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{SnapshotData: snapshot2})

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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{SnapshotData: snapshot1})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{SnapshotData: snapshot2})

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

	sm := createTestSnapshotMeta()

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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{SnapshotData: snapshotData})

	// Act
	result, err := sm.GetSnapshot(ctx, snapshotName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshotData, result)
}

func TestSnapshotMeta_GetSnapshot_NotFound(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "nonexistent_snapshot"

	sm := createTestSnapshotMeta()

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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{SnapshotData: snapshotData})

	// Act
	result, err := sm.getSnapshotByName(ctx, snapshotName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshotData, result)
}

func TestSnapshotMeta_GetSnapshotByName_NotFound(t *testing.T) {
	// Arrange
	ctx := context.Background()
	snapshotName := "nonexistent_snapshot"

	sm := createTestSnapshotMeta()

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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{SnapshotData: snapshot1})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{SnapshotData: snapshot2})
	sm.snapshotID2DataInfo.Insert(3, &SnapshotDataInfo{SnapshotData: snapshot3})

	// Act
	result, err := sm.getSnapshotByName(ctx, targetName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, snapshot2, result)
}

// --- GetSnapshotBySegment Tests ---

func TestSnapshotMeta_GetSnapshotBySegment_Success(t *testing.T) {
	// Arrange
	ctx := context.Background()
	collectionID := typeutil.UniqueID(100)
	segmentID := typeutil.UniqueID(1001)

	dataInfo1 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	dataInfo2 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1003, 1004),
		IndexIDs:     typeutil.NewUniqueSet(2002),
	}

	dataInfo3 := &SnapshotDataInfo{
		CollectionID: 200, // Different collection
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2003),
	}

	sm := createTestSnapshotMeta()
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

	dataInfo := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	sm := createTestSnapshotMeta()
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

	dataInfo1 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1002),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	dataInfo2 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001, 1003),
		IndexIDs:     typeutil.NewUniqueSet(2002),
	}

	sm := createTestSnapshotMeta()
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

	dataInfo1 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}

	dataInfo2 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1002),
		IndexIDs:     typeutil.NewUniqueSet(2003, 2004),
	}

	dataInfo3 := &SnapshotDataInfo{
		CollectionID: 200, // Different collection
		SegmentIDs:   typeutil.NewUniqueSet(1003),
		IndexIDs:     typeutil.NewUniqueSet(2001),
	}

	sm := createTestSnapshotMeta()
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

	dataInfo := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}

	sm := createTestSnapshotMeta()
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

	dataInfo1 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2002),
	}

	dataInfo2 := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1002),
		IndexIDs:     typeutil.NewUniqueSet(2001, 2003),
	}

	sm := createTestSnapshotMeta()
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
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64, includeSegments bool) (*SnapshotData, error) {
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
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotInfo := &datapb.SnapshotInfo{
		Id:           1,
		CollectionId: 100,
		Name:         "test_snapshot",
	}
	snapshotData := createTestSnapshotDataForMeta()

	// Mock catalog.ListSnapshots
	mock1 := mockey.Mock((*kv_datacoord.Catalog).ListSnapshots).To(func(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
		return []*datapb.SnapshotInfo{snapshotInfo}, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotReader.ReadSnapshot
	mock2 := mockey.Mock((*SnapshotReader).ReadSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64, includeSegments bool) (*SnapshotData, error) {
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, int64(1), snapshotID)
		assert.True(t, includeSegments)
		return snapshotData, nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.reload(ctx)

	// Assert
	assert.NoError(t, err)
	// Verify snapshot was inserted into map
	value, exists := sm.snapshotID2DataInfo.Get(1)
	assert.True(t, exists)
	assert.Equal(t, snapshotData, value.SnapshotData)
}

func TestSnapshotMeta_Reload_CatalogError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaWithTempDir(t)
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

// --- SaveSnapshot Tests (Mockey-based) ---

func TestSnapshotMeta_SaveSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotData := createTestSnapshotDataForMeta()
	metadataFilePath := "s3://bucket/snapshots/100/metadata/00001-uuid.json"

	// Mock SnapshotWriter.Save
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		assert.Equal(t, snapshotData, snapshot)
		return metadataFilePath, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock catalog.SaveSnapshot
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		assert.Equal(t, metadataFilePath, snapshotInfo.S3Location)
		return nil
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert
	assert.NoError(t, err)
	// Verify snapshot data info was inserted
	dataInfo, exists := sm.snapshotID2DataInfo.Get(snapshotData.SnapshotInfo.GetId())
	assert.True(t, exists)
	assert.Equal(t, int64(100), dataInfo.CollectionID)
	assert.True(t, dataInfo.SegmentIDs.Contain(1001))
	assert.True(t, dataInfo.IndexIDs.Contain(2001))
	// Verify segments were cleared from snapshot data
	assert.Nil(t, snapshotData.Segments)
	assert.Equal(t, metadataFilePath, snapshotData.SnapshotInfo.S3Location)
}

func TestSnapshotMeta_SaveSnapshot_WriterError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotData := createTestSnapshotDataForMeta()
	expectedErr := errors.New("writer failed")

	// Mock SnapshotWriter.Save to return error
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return "", expectedErr
	}).Build()
	defer mock1.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSnapshotMeta_SaveSnapshot_CatalogError_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotData := createTestSnapshotDataForMeta()
	metadataFilePath := "s3://bucket/snapshots/100/metadata/00001-uuid.json"
	expectedErr := errors.New("catalog failed")

	// Mock SnapshotWriter.Save
	mock1 := mockey.Mock((*SnapshotWriter).Save).To(func(ctx context.Context, snapshot *SnapshotData) (string, error) {
		return metadataFilePath, nil
	}).Build()
	defer mock1.UnPatch()

	// Mock catalog.SaveSnapshot to return error
	mock2 := mockey.Mock((*kv_datacoord.Catalog).SaveSnapshot).To(func(ctx context.Context, snapshotInfo *datapb.SnapshotInfo) error {
		return expectedErr
	}).Build()
	defer mock2.UnPatch()

	// Act
	err := sm.SaveSnapshot(ctx, snapshotData)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// --- DropSnapshot Tests (Mockey-based) ---

func TestSnapshotMeta_DropSnapshot_Success_WithMockey(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName

	// Insert snapshot into map
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{SnapshotData: snapshotData})

	// Mock catalog.DropSnapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, int64(1), snapshotID)
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Drop
	mock2 := mockey.Mock((*SnapshotWriter).Drop).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, int64(1), snapshotID)
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
	sm := createTestSnapshotMetaWithTempDir(t)
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
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName
	expectedErr := errors.New("catalog drop failed")

	// Insert snapshot into map
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{SnapshotData: snapshotData})

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
	sm := createTestSnapshotMetaWithTempDir(t)
	snapshotName := "test_snapshot"
	snapshotData := createTestSnapshotDataForMeta()
	snapshotData.SnapshotInfo.Name = snapshotName
	expectedErr := errors.New("writer drop failed")

	// Insert snapshot into map
	sm.snapshotID2DataInfo.Insert(snapshotData.SnapshotInfo.GetId(), &SnapshotDataInfo{SnapshotData: snapshotData})

	// Mock catalog.DropSnapshot
	mock1 := mockey.Mock((*kv_datacoord.Catalog).DropSnapshot).To(func(ctx context.Context, collectionID, snapshotID int64) error {
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Mock SnapshotWriter.Drop to return error
	mock2 := mockey.Mock((*SnapshotWriter).Drop).To(func(ctx context.Context, collectionID, snapshotID int64) error {
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
	assert.Equal(t, int64(100), dataInfo.CollectionID)
	assert.True(t, dataInfo.SegmentIDs.Contain(1001))
	assert.True(t, dataInfo.SegmentIDs.Contain(1002))
	assert.True(t, dataInfo.IndexIDs.Contain(2001))
	assert.True(t, dataInfo.IndexIDs.Contain(2002))
	assert.Equal(t, 2, dataInfo.SegmentIDs.Len())
	assert.Equal(t, 2, dataInfo.IndexIDs.Len())
}

func TestSnapshotDataInfo_EmptySets(t *testing.T) {
	// Arrange & Act
	dataInfo := &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(),
		IndexIDs:     typeutil.NewUniqueSet(),
	}

	// Assert
	assert.Equal(t, int64(100), dataInfo.CollectionID)
	assert.Equal(t, 0, dataInfo.SegmentIDs.Len())
	assert.Equal(t, 0, dataInfo.IndexIDs.Len())
	assert.False(t, dataInfo.SegmentIDs.Contain(1001))
	assert.False(t, dataInfo.IndexIDs.Contain(2001))
}

// --- Concurrent Operations Tests ---

func TestSnapshotMeta_ConcurrentOperations(t *testing.T) {
	// Arrange
	ctx := context.Background()
	sm := createTestSnapshotMeta()

	// Test concurrent map operations don't panic
	snapshotData := createTestSnapshotDataForMeta()

	// Act - simulate concurrent operations
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{
		CollectionID: 100,
		SegmentIDs:   typeutil.NewUniqueSet(1001),
		IndexIDs:     typeutil.NewUniqueSet(2001),
		SnapshotData: snapshotData,
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
	sm := createTestSnapshotMeta()

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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{SnapshotData: snapshot1})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{SnapshotData: snapshot2})

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

	sm := createTestSnapshotMeta()
	sm.snapshotID2DataInfo.Insert(1, &SnapshotDataInfo{SnapshotData: snapshot1})
	sm.snapshotID2DataInfo.Insert(2, &SnapshotDataInfo{SnapshotData: snapshot2})

	// Act
	snapshots, err := sm.ListSnapshots(ctx, collectionID, partitionID)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, snapshots, 1)
	assert.Contains(t, snapshots, "snapshot2")
}
