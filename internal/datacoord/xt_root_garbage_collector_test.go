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

	"os"
	"path"

	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"

	"github.com/milvus-io/milvus/internal/metacache"

	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// TestGarbageCollector_recycleDroppedSegments_SnapshotReference tests that segments referenced by snapshots are not garbage collected
func TestGarbageCollector_recycleDroppedSegments_SnapshotReference(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Mock newSnapshotMeta
	smMeta := &snapshotMeta{}

	// Create meta
	meta := &meta{
		catalog:   catalog,
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(catalog),
	}
	meta.compactionProtection = smMeta

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0, // Set to 0 to immediately consider segments for GC
		snapshotMeta:     smMeta,
	})

	// Add dropped segments to meta
	droppedSegment1 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
		},
	}
	droppedSegment2 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
		},
	}

	meta.segments.SetSegment(1001, droppedSegment1)
	meta.segments.SetSegment(1002, droppedSegment2)

	// Setup mocks
	// New O(1) API: IsSegmentGCBlocked encodes both "loaded state" and "is referenced".
	// Block segment 1001; leave 1002 unblocked.
	mock2 := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).To(
		func(sm *snapshotMeta, collID, segID int64) bool {
			return segID == 1001
		}).Build()
	defer mock2.UnPatch()

	mock3 := mockey.Mock((*ServerHandler).ListLoadedSegments).To(func(h *ServerHandler, ctx context.Context) ([]int64, error) {
		return []int64{}, nil
	}).Build()
	defer mock3.UnPatch()

	mock4 := mockey.Mock((*datacoord.Catalog).ListChannelCheckpoint).To(func(c *datacoord.Catalog, ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
		return map[string]*msgpb.MsgPosition{}, nil
	}).Build()
	defer mock4.UnPatch()

	mock5 := mockey.Mock((*datacoord.Catalog).ListIndexes).To(func(c *datacoord.Catalog, ctx context.Context) ([]*model.Index, error) {
		return []*model.Index{}, nil
	}).Build()
	defer mock5.UnPatch()

	mock6 := mockey.Mock((*datacoord.Catalog).ListSegmentIndexes).To(func(c *datacoord.Catalog, ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
		return []*model.SegmentIndex{}, nil
	}).Build()
	defer mock6.UnPatch()

	mock7 := mockey.Mock((*datacoord.Catalog).ChannelExists).Return(true, nil).Build()
	defer mock7.UnPatch()

	dropSegmentCalled := false
	var droppedSegment *datapb.SegmentInfo
	mock8 := mockey.Mock((*datacoord.Catalog).DropSegment).To(func(c *datacoord.Catalog, ctx context.Context, segment *datapb.SegmentInfo) error {
		dropSegmentCalled = true
		droppedSegment = segment
		return nil
	}).Build()
	defer mock8.UnPatch()

	mock9 := mockey.Mock((*garbageCollector).removeObjectFiles).To(func(gc *garbageCollector, ctx context.Context, logs map[string]struct{}) error {
		return nil
	}).Build()
	defer mock9.UnPatch()

	// Execute
	gc.recycleDroppedSegments(ctx, nil)

	// Verify
	// Segment 1001 should still exist (not GC'd due to snapshot reference)
	assert.NotNil(t, meta.GetSegment(ctx, 1001))
	// Segment 1002 should be removed (GC'd)
	assert.Nil(t, meta.GetSegment(ctx, 1002))
	assert.True(t, dropSegmentCalled)
	if droppedSegment != nil {
		assert.Equal(t, int64(1002), droppedSegment.ID)
	}
}

// TestGarbageCollector_recycleUnusedSegIndexes_SnapshotReference tests that indexes referenced by snapshots are not garbage collected
func TestGarbageCollector_recycleUnusedSegIndexes_SnapshotReference(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Mock newSnapshotMeta
	smMeta := &snapshotMeta{}

	// Mock newIndexMeta
	idxMeta := &indexMeta{}

	// Create meta
	im := idxMeta
	sm := smMeta
	meta := &meta{
		catalog:      catalog,
		indexCatalog: idxMeta,
		segments:     newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore:    metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	}))

	// Create segment indexes
	segIdx1 := &model.SegmentIndex{
		SegmentID:    2001,
		CollectionID: 200,
		PartitionID:  20,
		IndexID:      301,
		BuildID:      401,
		NodeID:       501,
	}
	segIdx2 := &model.SegmentIndex{
		SegmentID:    2002,
		CollectionID: 200,
		PartitionID:  20,
		IndexID:      302,
		BuildID:      402,
		NodeID:       502,
	}

	// Setup mocks
	// New O(1) API: IsBuildIDGCBlocked encodes both "loaded state" and "is referenced".
	// Block buildID 401; leave 402 unblocked.
	mock1 := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).To(
		func(sm *snapshotMeta, collID, buildID int64) bool {
			return buildID == 401
		}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*datacoord.Catalog).ListSegmentIndexes).To(func(c *datacoord.Catalog, ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
		return []*model.SegmentIndex{segIdx1, segIdx2}, nil
	}).Build()
	defer mock2.UnPatch()

	mock3 := mockey.Mock(meta.GetSegment).To(func(ctx context.Context, segmentID int64) *SegmentInfo {
		return nil // All segments are not found
	}).Build()
	defer mock3.UnPatch()

	mock4 := mockey.Mock((*indexMeta).IsIndexExist).To(func(collID, indexID int64) bool {
		return false
	}).Build()
	defer mock4.UnPatch()

	mock5 := mockey.Mock((*indexMeta).GetAllSegIndexes).To(func() map[int64]*model.SegmentIndex {
		return map[int64]*model.SegmentIndex{
			segIdx1.BuildID: segIdx1,
			segIdx2.BuildID: segIdx2,
		}
	}).Build()
	defer mock5.UnPatch()

	mock6 := mockey.Mock((*garbageCollector).getAllIndexFilesOfIndex).To(func(gc *garbageCollector, segIdx *model.SegmentIndex) map[string]struct{} {
		return map[string]struct{}{
			fmt.Sprintf("index_%d", segIdx.BuildID): {},
		}
	}).Build()
	defer mock6.UnPatch()

	removeCallCount := 0
	mock7 := mockey.Mock((*garbageCollector).removeObjectFiles).To(func(gc *garbageCollector, ctx context.Context, logs map[string]struct{}) error {
		removeCallCount++
		return nil
	}).Build()
	defer mock7.UnPatch()

	removeSegmentIndexCalled := false
	removedBuildID := int64(0)
	mock8 := mockey.Mock((*indexMeta).RemoveSegmentIndex).To(func(ctx context.Context, buildID int64) error {
		removeSegmentIndexCalled = true
		removedBuildID = buildID
		return nil
	}).Build()
	defer mock8.UnPatch()

	// Execute
	gc.recycleUnusedSegIndexes(ctx, nil)

	// Verify
	// Only segIdx2 should have its files removed (segIdx1 is protected by snapshot)
	assert.Equal(t, 1, removeCallCount)
	assert.True(t, removeSegmentIndexCalled)
	assert.Equal(t, int64(402), removedBuildID)
}

// TestGarbageCollector_recycleUnusedBinlogFiles_SnapshotReference tests that binlog files of segments
// referenced by snapshots are not garbage collected
func TestGarbageCollector_recycleUnusedBinlogFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create segment that exists in meta (will pass checker for insert logs)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
		},
	}

	// Create meta with segment
	im := &indexMeta{}
	sm := &snapshotMeta{}
	meta := &meta{
		catalog: &datacoord.Catalog{},
		segments: newSegmentsInfoWithSegments(map[int64]*SegmentInfo{
			1001: segment,
		}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	// Track if Remove is called
	removeCalledPaths := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
		removeCalledPaths = append(removeCalledPaths, filePath)
		return nil
	}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to return a file from this segment
	chunkInfo := &storage.ChunkObjectInfo{
		FilePath:   "gc/segments/insert_logs/1001/0/1",
		ModifyTime: time.Now().Add(-time.Hour),
	}

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		fn(chunkInfo)
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	// Mock ParseSegmentIDByBinlog
	mockParseSegID := mockey.Mock(storage.ParseSegmentIDByBinlog).Return(int64(1001), nil).Build()
	defer mockParseSegID.UnPatch()

	// New O(1) API: IsSegmentGCBlocked directly returns "protected".
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	// Execute
	gc.recycleUnusedBinlogFiles(ctx)

	// Verify - Remove should NOT be called because segment is protected by snapshot reference
	assert.Empty(t, removeCalledPaths, "binlog files of snapshot-referenced segments should not be removed")
}

// TestGarbageCollector_recycleUnusedBinlogFiles_V3Orphan tests that V3-layout files whose
// segment was never registered in meta (e.g. output uploaded by a failed sort compaction
// attempt, issue #50962) are recycled after missingTolerance, while files of registered
// V3 segments are still skipped.
func TestGarbageCollector_recycleUnusedBinlogFiles_V3Orphan(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Registered V3 segment, its files must be skipped by the orphan scan.
	registered := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1001,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			InsertChannel:  "ch1",
			StorageVersion: storage.StorageV3,
		},
	}

	im := &indexMeta{}
	sm := &snapshotMeta{}
	meta := &meta{
		catalog: &datacoord.Catalog{},
		segments: newSegmentsInfoWithSegments(map[int64]*SegmentInfo{
			1001: registered,
		}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour,
		dropTolerance:    time.Hour * 24,
	}))

	var (
		removedMu    sync.Mutex
		removedPaths []string
	)
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
		removedMu.Lock()
		defer removedMu.Unlock()
		removedPaths = append(removedPaths, filePath)
		return nil
	}).Build()
	defer mockRemove.UnPatch()

	expired := time.Now().Add(-2 * time.Hour)
	insertPrefix := path.Join(cli.RootPath(), common.SegmentInsertLogPath)
	files := []*storage.ChunkObjectInfo{
		// deep V3 path of the registered segment: skipped
		{FilePath: path.Join(insertPrefix, "100/10/1001/_stats/bloom_filter.100/2001"), ModifyTime: expired},
		// deep V3 path of orphan segment 999 (not in meta), expired: removed
		{FilePath: path.Join(insertPrefix, "100/10/999/_stats/bloom_filter.100/2002"), ModifyTime: expired},
		// data file of the same orphan segment: removed
		{FilePath: path.Join(insertPrefix, "100/10/999/_data/2003_uuid.parquet"), ModifyTime: expired},
		// orphan V3 file still within missingTolerance: kept
		{FilePath: path.Join(insertPrefix, "100/10/998/_stats/bloom_filter.100/2004"), ModifyTime: time.Now()},
	}

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		if prefix != insertPrefix {
			return nil
		}
		for _, file := range files {
			if !fn(file) {
				break
			}
		}
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.ElementsMatch(t, []string{
		path.Join(insertPrefix, "100/10/999/_stats/bloom_filter.100/2002"),
		path.Join(insertPrefix, "100/10/999/_data/2003_uuid.parquet"),
	}, removedPaths)
}

// TestGarbageCollector_recycleDroppedSegments_SnapshotMetaNil tests that GC handles nil snapshotMeta gracefully
func TestGarbageCollector_recycleDroppedSegments_SnapshotMetaNil(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Create meta with nil snapshotMeta
	var sm *snapshotMeta // nil snapshot meta
	meta := &meta{
		catalog:   catalog,
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(catalog),
	}

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0, // Set to 0 to immediately consider segments for GC
	}))

	// Add dropped segment to meta
	droppedSegment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1003,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
		},
	}

	meta.segments.SetSegment(1003, droppedSegment)

	// Setup mocks
	mockListLoadedSegments := mockey.Mock((*ServerHandler).ListLoadedSegments).Return([]int64{}, nil).Build()
	defer mockListLoadedSegments.UnPatch()

	mockListChannelCP := mockey.Mock((*datacoord.Catalog).ListChannelCheckpoint).Return(map[string]*msgpb.MsgPosition{}, nil).Build()
	defer mockListChannelCP.UnPatch()

	mockListIndexes := mockey.Mock((*datacoord.Catalog).ListIndexes).Return([]*model.Index{}, nil).Build()
	defer mockListIndexes.UnPatch()

	mockListSegmentIndexes := mockey.Mock((*datacoord.Catalog).ListSegmentIndexes).Return([]*model.SegmentIndex{}, nil).Build()
	defer mockListSegmentIndexes.UnPatch()

	mockChannelExists := mockey.Mock((*datacoord.Catalog).ChannelExists).Return(true, nil).Build()
	defer mockChannelExists.UnPatch()

	dropSegmentCalled := false
	mockDropSegment := mockey.Mock((*datacoord.Catalog).DropSegment).To(func(c *datacoord.Catalog, ctx context.Context, segment *datapb.SegmentInfo) error {
		dropSegmentCalled = true
		return nil
	}).Build()
	defer mockDropSegment.UnPatch()

	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).Return(nil).Build()
	defer mockRemoveObjectFiles.UnPatch()

	// Execute - should not panic with nil snapshotMeta
	assert.NotPanics(t, func() {
		gc.recycleDroppedSegments(ctx, nil)
	})

	// Verify - segment should be dropped (GC'd) since snapshotMeta is nil
	assert.True(t, dropSegmentCalled, "DropSegment should be called")
}

// TestGarbageCollector_recycleUnusedIndexFilesV0_SnapshotReference tests that index files referenced
// by snapshots are not garbage collected
func TestGarbageCollector_recycleUnusedIndexFilesV0_SnapshotReference(t *testing.T) {
	// Setup
	ctx := context.Background()

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create necessary components
	catalog := &datacoord.Catalog{}
	handler := &ServerHandler{}

	// Mock snapshotMeta
	smMeta := &snapshotMeta{}

	// Create meta
	im := &indexMeta{}
	sm := smMeta
	meta := &meta{
		catalog:   catalog,
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	// Create garbage collector
	gc := newGarbageCollector(meta, handler, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	}))

	buildID := int64(401)
	collectionID := int64(100)
	indexID := int64(301)

	// Setup mock for WalkWithPrefix to find index directory.
	// Note: the outer walk runs against common.SegmentIndexV0Path ("index_files"), not "indexes".
	// We match that prefix so the walker callback actually fires and exercises the
	// segIdx != nil + IsBuildIDGCBlocked branch below.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		if strings.Contains(prefix, common.SegmentIndexV0Path) {
			chunkInfo := &storage.ChunkObjectInfo{
				FilePath:   fmt.Sprintf("gc/index_files/%d/1/", buildID),
				ModifyTime: time.Now().Add(-time.Hour),
			}
			fn(chunkInfo)
		}
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	// Mock CheckCleanSegmentIndex
	segIdx := &model.SegmentIndex{
		BuildID:      buildID,
		CollectionID: collectionID,
		IndexID:      indexID,
	}
	mockCheckClean := mockey.Mock((*indexMeta).CheckCleanSegmentIndex).Return(true, segIdx).Build()
	defer mockCheckClean.UnPatch()

	// New O(1) API: IsBuildIDGCBlocked returns true (buildID protected by snapshot).
	mockIsBuildIDBlocked := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).Return(true).Build()
	defer mockIsBuildIDBlocked.UnPatch()

	removeWithPrefixCalled := false
	mockRemoveWithPrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
		removeWithPrefixCalled = true
		return nil
	}).Build()
	defer mockRemoveWithPrefix.UnPatch()

	// Execute
	gc.recycleUnusedIndexFilesV0(ctx)

	// Verify - RemoveWithPrefix should NOT be called because index is referenced by snapshot
	assert.False(t, removeWithPrefixCalled, "RemoveWithPrefix should not be called for index files referenced by snapshot")
}

func TestGarbageCollector_recycleUnusedTextIndexFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	// Create segment with TextStatsLogs
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID:    101,
					Version:    2, // Current version
					BuildID:    401,
					Files:      []string{"file1"},
					LogSize:    1024,
					MemorySize: 2048,
				},
			},
		},
	}

	// Create meta
	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1001: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	// Track Remove calls
	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to simulate text index files. Production builds the prefix via
	// metautil.BuildTextIndexPrefix which uses common.TextIndexPath ("text_log"), so the
	// substring match MUST reference that constant — a human-readable string like
	// "text_index" would silently never match and hide regressions.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.TextIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/text_log/401/1/100/10/1001/101/file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true (segment protected by snapshot).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	// Execute
	gc.recycleUnusedTextIndexFiles(ctx, nil)

	// Verify - files should NOT be removed
	assert.Empty(t, removedFiles,
		"text index files should not be removed when segment is referenced by snapshot")
}

func TestGarbageCollector_recycleUnusedAnalyzeFiles_StaleVersions(t *testing.T) {
	ctx := context.Background()

	am := &analyzeMeta{
		ctx: ctx,
		tasks: map[int64]*indexpb.AnalyzeTask{
			401: {
				TaskID:  401,
				Version: 2,
				State:   indexpb.JobState_JobStateFinished,
			},
		},
	}
	meta := &meta{
		catalog: &datacoord.Catalog{},
	}

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{analyzeMeta: am}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	// The non-recursive walk lists the per-task top-level dirs under analyze_stats.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.AnalyzeStatsPath) {
				fn(&storage.ChunkObjectInfo{
					FilePath:   path.Join(prefix, "401") + "/",
					ModifyTime: time.Now().Add(-time.Hour),
				})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	removedPrefixes := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
			removedPrefixes = append(removedPrefixes, prefix)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	gc.recycleUnusedAnalyzeFiles(ctx, nil)

	// Analyze files are written under analyze_stats/{taskID}/{version}/... — for a
	// finished task at Version=2, exactly the stale versions 0 and 1 under the
	// task's own prefix must be removed, and version 2 must be kept.
	root := path.Join(cli.RootPath(), common.AnalyzeStatsPath) + "/"
	assert.ElementsMatch(t, []string{root + "401/0/", root + "401/1/"}, removedPrefixes)
}

func TestGarbageCollector_recycleUnusedJSONIndexFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	// Create segment with JsonKeyStats
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:    102,
					Version:    2,
					BuildID:    402,
					Files:      []string{"json_file1"},
					LogSize:    512,
					MemorySize: 1024,
				},
			},
		},
	}

	// Create meta
	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1002: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	// Create storage manager
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	// Track Remove calls
	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to simulate JSON index files. Production builds the prefix using
	// common.JSONIndexPath ("json_key_index_log"), so the substring match MUST reference
	// that constant — a shorthand like "json_index" would silently never match.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.JSONIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/json_key_index_log/402/1/100/10/1002/102/json_file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true (segment protected by snapshot).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	// Execute
	gc.recycleUnusedJSONIndexFiles(ctx, nil)

	// Verify - files should NOT be removed
	assert.Empty(t, removedFiles,
		"JSON index files should not be removed when segment is referenced by snapshot")
}

// TestGarbageCollector_recycleUnusedBinlogFiles_SkipWhenRefIndexNotLoaded tests that binlog GC
// is skipped when the unified IsSegmentGCBlocked returns true due to an unloaded RefIndex
// (fail-closed coarse block during the startup window).
func TestGarbageCollector_recycleUnusedBinlogFiles_SkipWhenRefIndexNotLoaded(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
		},
	}

	im := &indexMeta{}
	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1001: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
		removedFiles = append(removedFiles, filePath)
		return nil
	}).Build()
	defer mockRemove.UnPatch()

	chunkInfo := &storage.ChunkObjectInfo{
		FilePath:   "gc/segments/insert_logs/1001/0/1",
		ModifyTime: time.Now().Add(-time.Hour),
	}
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
		fn(chunkInfo)
		return nil
	}).Build()
	defer mockWalk.UnPatch()

	mockParseSegID := mockey.Mock(storage.ParseSegmentIDByBinlog).Return(int64(1001), nil).Build()
	defer mockParseSegID.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true because the collection's RefIndex
	// is unloaded (fail-closed coarse block during startup window).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.Empty(t, removedFiles, "binlog files should not be removed when RefIndex is not loaded")
}

// TestGarbageCollector_recycleUnusedTextIndexFiles_SkipWhenRefIndexNotLoaded tests that text index GC
// is skipped when the unified IsSegmentGCBlocked returns true due to an unloaded RefIndex.
func TestGarbageCollector_recycleUnusedTextIndexFiles_SkipWhenRefIndexNotLoaded(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID:    101,
					Version:    2,
					BuildID:    401,
					Files:      []string{"file1"},
					LogSize:    1024,
					MemorySize: 2048,
				},
			},
		},
	}

	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1001: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Substring match references common.TextIndexPath ("text_log") to stay in sync with
	// production metautil.BuildTextIndexPrefix — a shorthand would silently never match.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.TextIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/text_log/401/1/100/10/1001/101/file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true due to unloaded RefIndex (fail-closed).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedTextIndexFiles(ctx, nil)

	assert.Empty(t, removedFiles, "text index files should not be removed when RefIndex is not loaded")
}

// TestGarbageCollector_recycleUnusedJSONIndexFiles_SkipWhenRefIndexNotLoaded tests that JSON index GC
// is skipped when the unified IsSegmentGCBlocked returns true due to an unloaded RefIndex.
func TestGarbageCollector_recycleUnusedJSONIndexFiles_SkipWhenRefIndexNotLoaded(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:    102,
					Version:    2,
					BuildID:    402,
					Files:      []string{"json_file1"},
					LogSize:    512,
					MemorySize: 1024,
				},
			},
		},
	}

	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1002: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Substring match references common.JSONIndexPath ("json_key_index_log") to stay in
	// sync with production — a shorthand like "json_index" would silently never match.
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.JSONIndexPath) {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/json_key_index_log/402/1/100/10/1002/102/json_file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true due to unloaded RefIndex (fail-closed).
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedJSONIndexFiles(ctx, nil)

	assert.Empty(t, removedFiles, "JSON index files should not be removed when RefIndex is not loaded")
}

// TestGarbageCollector_recycleUnusedIndexFilesV0_SegIdxNil_SnapshotProtection tests that when segIdx is nil
// (buildID no longer in meta), index files are NOT deleted if a snapshot references the buildID.
func TestGarbageCollector_recycleUnusedIndexFilesV0_SegIdxNil_SnapshotProtection(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	smMeta := &snapshotMeta{}
	im := &indexMeta{}
	sm := smMeta
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	}))

	buildID := int64(501)

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			// Outer walk listing buildID directories
			chunkInfo := &storage.ChunkObjectInfo{
				FilePath:   fmt.Sprintf("gc/index_files/%d/", buildID),
				ModifyTime: time.Now().Add(-time.Hour),
			}
			fn(chunkInfo)
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// Mock CheckCleanSegmentIndex to return (true, nil) — buildID not in meta
	mockCheckClean := mockey.Mock((*indexMeta).CheckCleanSegmentIndex).Return(true, nil).Build()
	defer mockCheckClean.UnPatch()

	// Mock parseBuildIDFromFilePath
	mockParseBuildID := mockey.Mock(parseBuildIDFromFilePath).Return(buildID, nil).Build()
	defer mockParseBuildID.UnPatch()

	// New O(1) API: mock IsBuildIDGCBlocked to return true for our buildID (the orphan
	// branch passes collectionID = -1 since it has no segIdx context).
	mockIsBuildIDBlocked := mockey.Mock((*snapshotMeta).IsBuildIDGCBlocked).To(
		func(sm *snapshotMeta, collID, bid int64) bool {
			return bid == buildID
		}).Build()
	defer mockIsBuildIDBlocked.UnPatch()

	removeWithPrefixCalled := false
	mockRemoveWithPrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
			removeWithPrefixCalled = true
			return nil
		}).Build()
	defer mockRemoveWithPrefix.UnPatch()

	gc.recycleUnusedIndexFilesV0(ctx)

	assert.False(t, removeWithPrefixCalled,
		"RemoveWithPrefix should not be called for index files when segIdx is nil but buildID is snapshot-referenced")
}

// TestGarbageCollector_recycleUnusedBinlogFiles_SegmentNil_SnapshotProtection tests that binlog files
// are NOT deleted when segment meta is nil but the segment is referenced by a snapshot.
// This verifies the fix: collectionID=-1 scans all collections when segment is nil.
func TestGarbageCollector_recycleUnusedBinlogFiles_SegmentNil_SnapshotProtection(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	// Create meta with NO segments (segment is nil)
	im := &indexMeta{}
	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	// Track if Remove is called
	removeCalledPaths := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removeCalledPaths = append(removeCalledPaths, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	// Mock WalkWithPrefix to return a file
	chunkInfo := &storage.ChunkObjectInfo{
		FilePath:   "gc/insert_log/100/10/1001/101/10000001",
		ModifyTime: time.Now().Add(-time.Hour),
	}
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			fn(chunkInfo)
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// Mock ParseSegmentIDByBinlog to return segmentID 1001
	mockParseSegID := mockey.Mock(storage.ParseSegmentIDByBinlog).Return(int64(1001), nil).Build()
	defer mockParseSegID.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true so the file is kept.
	// The caller passes collectionID = -1 (segment is nil in this test), which exercises
	// the "global fail-closed" branch of the new API — but here we short-circuit to true
	// directly to simulate a snapshot referencing segment 1001.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.Empty(t, removeCalledPaths,
		"binlog files should not be removed when segment is nil but snapshot references the segment")
}

// TestGarbageCollector_recycleUnusedJSONStatsFiles_SnapshotReference tests that JSON stats files
// are NOT deleted when the segment is referenced by a snapshot.
func TestGarbageCollector_recycleUnusedJSONStatsFiles_SnapshotReference(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:    102,
					Version:    2,
					BuildID:    402,
					Files:      []string{"json_stats_file1"},
					LogSize:    512,
					MemorySize: 1024,
				},
			},
		},
	}

	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1002: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test"))

	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, "json_stats") {
				chunkInfo := &storage.ChunkObjectInfo{
					FilePath:   "gc/json_stats/1/402/1/100/10/1002/102/json_stats_file1",
					ModifyTime: time.Now().Add(-time.Hour),
				}
				fn(chunkInfo)
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	// New O(1) API: IsSegmentGCBlocked returns true for the referenced segment.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(true).Build()
	defer mockIsSegBlocked.UnPatch()

	gc.recycleUnusedJSONStatsFiles(ctx, nil)

	assert.Empty(t, removedFiles,
		"JSON stats files should not be removed when segment is referenced by snapshot")
}

func TestGarbageCollector_recycleUnusedJSONStatsFiles_GlobalFormatPrefixOnce(t *testing.T) {
	ctx := context.Background()
	segments := make(map[int64]*SegmentInfo)
	for segmentID := int64(1); segmentID <= 2; segmentID++ {
		segments[segmentID] = &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           segmentID,
				CollectionID: 100,
				PartitionID:  10,
				State:        commonpb.SegmentState_Flushed,
				JsonKeyStats: map[int64]*datapb.JsonKeyStats{
					102: {
						FieldID:                102,
						Version:                1,
						JsonKeyStatsDataFormat: 3,
					},
				},
			},
		}
	}

	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(segments),
		metaStore: metacache.NewMetaStore(nil),
	}
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("gc"))
	gc := newGarbageCollector(meta, &ServerHandler{}, GcOption{cli: cli})

	walkCalls := make(map[string]int)
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			walkCalls[prefix]++
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	gc.recycleUnusedJSONStatsFiles(ctx, nil)

	assert.Equal(t, map[string]int{
		"gc/json_stats/1": 1,
		"gc/json_stats/2": 1,
	}, walkCalls)
}

func TestGarbageCollector_recycleDroppedSegments_V3(t *testing.T) {
	ctx := context.Background()

	cli := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))

	catalog := &datacoord.Catalog{}
	smMeta := &snapshotMeta{}

	sm := smMeta
	m := &meta{
		catalog:   catalog,
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(catalog),
	}
	m.compactionProtection = sm

	basePath := path.Join(cli.RootPath(), "insert_log/100/10/2001")
	require.NoError(t, os.MkdirAll(basePath, 0o755))
	completeBasePath := basePath + "/"
	manifestPath := packed.MarshalManifestPath(basePath, 1)

	// V3 dropped segment with ManifestPath and StorageVersion=3
	v3Segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             2001,
			CollectionID:   100,
			PartitionID:    10,
			State:          commonpb.SegmentState_Dropped,
			DroppedAt:      uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel:  "ch1",
			StorageVersion: storage.StorageV3,
			ManifestPath:   manifestPath,
		},
	}

	// V1 dropped segment (no ManifestPath, StorageVersion=0)
	v1Segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            2002,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Dropped,
			DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
			InsertChannel: "ch1",
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 1,
					Binlogs: []*datapb.Binlog{{LogPath: "log1", LogSize: 100}},
				},
			},
		},
	}

	m.segments.SetSegment(2001, v3Segment)
	m.segments.SetSegment(2002, v1Segment)

	gc := newGarbageCollector(m, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: time.Hour * 24,
		dropTolerance:    0,
	}))

	// Track calls
	removeWithPrefixCalled := false
	var removeWithPrefixArg string
	removeObjectFilesCalled := false
	droppedSegmentIDs := []int64{}

	// Snapshot layer transparent: no segment is blocked.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()
	mockListLoaded := mockey.Mock((*ServerHandler).ListLoadedSegments).Return([]int64{}, nil).Build()
	defer mockListLoaded.UnPatch()
	mockChannelExists := mockey.Mock((*datacoord.Catalog).ChannelExists).Return(true, nil).Build()
	defer mockChannelExists.UnPatch()
	mockDropSegment := mockey.Mock((*datacoord.Catalog).DropSegment).To(func(c *datacoord.Catalog, ctx context.Context, segment *datapb.SegmentInfo) error {
		droppedSegmentIDs = append(droppedSegmentIDs, segment.ID)
		return nil
	}).Build()
	defer mockDropSegment.UnPatch()

	// Mock RemoveWithPrefix for V3 segment
	mockRemoveWithPrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string) error {
			removeWithPrefixCalled = true
			removeWithPrefixArg = prefix
			return nil
		}).Build()
	defer mockRemoveWithPrefix.UnPatch()

	// Mock removeObjectFiles for V1 segment
	mockRemoveObjectFiles := mockey.Mock((*garbageCollector).removeObjectFiles).To(
		func(gc *garbageCollector, ctx context.Context, logs map[string]struct{}) error {
			removeObjectFilesCalled = true
			return nil
		}).Build()
	defer mockRemoveObjectFiles.UnPatch()

	gc.recycleDroppedSegments(ctx, nil)

	// V3 segment should use RemoveWithPrefix with basePath
	assert.True(t, removeWithPrefixCalled, "V3 segment should use RemoveWithPrefix")
	assert.Equal(t, completeBasePath, removeWithPrefixArg, "RemoveWithPrefix should be called with a complete local path")

	// V1 segment should use removeObjectFiles
	assert.True(t, removeObjectFilesCalled, "V1 segment should use removeObjectFiles")

	// Both segments should be dropped from meta
	assert.Contains(t, droppedSegmentIDs, int64(2001))
	assert.Contains(t, droppedSegmentIDs, int64(2002))
	assert.Nil(t, m.GetSegment(ctx, 2001))
	assert.Nil(t, m.GetSegment(ctx, 2002))
}

func TestGarbageCollector_recycleUnusedBinlogFiles_SkipV3(t *testing.T) {
	ctx := context.Background()

	rootPath := "gc"
	cli := storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-gc-v3-orphan"))

	sm := &snapshotMeta{}
	m := &meta{
		segments:  newSegmentsInfoWithSegments(make(map[int64]*SegmentInfo)),
		metaStore: metacache.NewMetaStore(nil),
	}
	m.compactionProtection = sm

	// Add a V3 segment to meta
	v3Segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             500,
			CollectionID:   1,
			PartitionID:    10,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(rootPath+"/insert_log/1/10/500", 1),
		},
	}
	m.segments.SetSegment(500, v3Segment)

	gc := newGarbageCollector(m, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    0,
	}))

	removedFiles := []string{}

	// Snapshot layer transparent: no segment is blocked.
	mockIsSegBlocked := mockey.Mock((*snapshotMeta).IsSegmentGCBlocked).Return(false).Build()
	defer mockIsSegBlocked.UnPatch()

	// Mock WalkWithPrefix to return V3 files under insert_log
	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string,
			recursive bool, fn storage.ChunkObjectWalkFunc,
		) error {
			if strings.Contains(prefix, common.SegmentInsertLogPath) {
				// V3 file that matches 6-part format (ParseSegmentIDByBinlog will succeed)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/0/data.parquet",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _data/ (ParseSegmentIDByBinlog will fail, fallback to parseV3SegmentID)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_data/0_abc.parquet",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _stats/ (ParseSegmentIDByBinlog will fail)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_stats/bloom_filter.101/42",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _delta/ (ParseSegmentIDByBinlog will fail)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_delta/501",
					ModifyTime: time.Now().Add(-time.Hour),
				})
				// V3 file under _metadata/ (ParseSegmentIDByBinlog will fail)
				fn(&storage.ChunkObjectInfo{
					FilePath:   rootPath + "/insert_log/1/10/500/_metadata/manifest-1.avro",
					ModifyTime: time.Now().Add(-time.Hour),
				})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	// None of the V3 files should be removed — they are managed by loon
	assert.Empty(t, removedFiles, "V3 segment files should not be removed by orphan scan")
}

func TestGarbageCollector_recycleUnusedBinlogFiles_TextAndJSONStats(t *testing.T) {
	ctx := context.Background()

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1003,
			CollectionID:  100,
			PartitionID:   10,
			State:         commonpb.SegmentState_Flushed,
			InsertChannel: "ch1",
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID: 101,
					Version: 1,
					BuildID: 501,
					Files:   []string{"text_file_keep"},
				},
			},
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				102: {
					FieldID:                102,
					Version:                1,
					BuildID:                502,
					Files:                  []string{"json_file_keep"},
					JsonKeyStatsDataFormat: 1,
				},
				103: {
					FieldID:                103,
					Version:                1,
					BuildID:                503,
					Files:                  []string{"shared_key_index/json_file_keep"},
					JsonKeyStatsDataFormat: 3,
				},
			},
		},
	}

	im := &indexMeta{}
	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{1003: segment}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("gc"))
	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			switch {
			case strings.Contains(prefix, common.TextIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/text_log/501/1/100/10/1003/101/text_file_keep", ModifyTime: time.Now().Add(-time.Hour)})
				fn(&storage.ChunkObjectInfo{FilePath: "gc/text_log/501/1/100/10/1003/101/text_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONStatsPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_keep", ModifyTime: time.Now().Add(-time.Hour)})
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_key_index_log/502/1/100/10/1003/102/json_file_keep", ModifyTime: time.Now().Add(-time.Hour)})
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.ElementsMatch(t, []string{
		"gc/text_log/501/1/100/10/1003/101/text_file_gc",
		"gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc",
		"gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc",
	}, removedFiles)
}

func TestGarbageCollector_recycleUnusedBinlogFiles_TextAndJSONStats_SegmentNil(t *testing.T) {
	ctx := context.Background()

	im := &indexMeta{}
	sm := &snapshotMeta{}
	meta := &meta{
		catalog:   &datacoord.Catalog{},
		segments:  newSegmentsInfoWithSegments(map[int64]*SegmentInfo{}),
		metaStore: metacache.NewMetaStore(nil),
	}
	meta.compactionProtection = sm

	cli := storage.NewLocalChunkManager(objectstorage.RootPath("gc"))
	gc := newGarbageCollector(meta, &ServerHandler{}, gcTestDomainOption(&snapshotTestMetas{indexMeta: im, snapshotMeta: sm}, GcOption{
		cli:              cli,
		enabled:          true,
		checkInterval:    time.Millisecond * 10,
		scanInterval:     time.Hour * 7 * 24,
		missingTolerance: 0,
		dropTolerance:    time.Hour * 24,
	}))

	removedFiles := []string{}
	mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, filePath string) error {
			removedFiles = append(removedFiles, filePath)
			return nil
		}).Build()
	defer mockRemove.UnPatch()

	mockWalk := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).To(
		func(cm *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, fn storage.ChunkObjectWalkFunc) error {
			switch {
			case strings.Contains(prefix, common.TextIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/text_log/501/1/100/10/1003/101/text_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONStatsPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			case strings.Contains(prefix, common.JSONIndexPath):
				fn(&storage.ChunkObjectInfo{FilePath: "gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc", ModifyTime: time.Now().Add(-time.Hour)})
			}
			return nil
		}).Build()
	defer mockWalk.UnPatch()

	gc.recycleUnusedBinlogFiles(ctx)

	assert.ElementsMatch(t, []string{
		"gc/text_log/501/1/100/10/1003/101/text_file_gc",
		"gc/json_stats/3/503/1/100/10/1003/103/shared_key_index/json_file_gc",
		"gc/json_key_index_log/502/1/100/10/1003/102/json_file_gc",
	}, removedFiles)
}
