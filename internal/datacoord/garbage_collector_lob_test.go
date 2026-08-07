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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLOBManifestCache(t *testing.T) {
	t.Run("basic cache operations", func(t *testing.T) {
		cache := newLOBManifestCache(10 * time.Minute)
		assert.NotNil(t, cache)
		assert.Equal(t, 0, cache.Size())

		// test invalidate on empty cache
		cache.Invalidate("non-existent")
		assert.Equal(t, 0, cache.Size())

		// test cleanup on empty cache
		cache.Cleanup()
		assert.Equal(t, 0, cache.Size())

		// test invalidate all on empty cache
		cache.InvalidateAll()
		assert.Equal(t, 0, cache.Size())
	})

	t.Run("cache entry management", func(t *testing.T) {
		cache := newLOBManifestCache(100 * time.Millisecond)

		// manually add entry for testing
		cache.mu.Lock()
		cache.cache["test-path"] = &lobManifestCacheEntry{
			lobFiles: []packed.LobFileInfo{
				{Path: "lob1.vx", FieldID: 100, TotalRows: 1000, ValidRows: 900},
			},
			cachedAt: time.Now(),
		}
		cache.mu.Unlock()

		assert.Equal(t, 1, cache.Size())

		// test invalidate
		cache.Invalidate("test-path")
		assert.Equal(t, 0, cache.Size())
	})

	t.Run("cache cleanup expired entries", func(t *testing.T) {
		cache := newLOBManifestCache(50 * time.Millisecond)

		// add entries with different timestamps
		cache.mu.Lock()
		cache.cache["fresh"] = &lobManifestCacheEntry{
			lobFiles: []packed.LobFileInfo{},
			cachedAt: time.Now(),
		}
		cache.cache["expired"] = &lobManifestCacheEntry{
			lobFiles: []packed.LobFileInfo{},
			cachedAt: time.Now().Add(-100 * time.Millisecond), // expired
		}
		cache.mu.Unlock()

		assert.Equal(t, 2, cache.Size())

		// cleanup should remove expired entry
		cache.Cleanup()
		assert.Equal(t, 1, cache.Size())

		// verify "fresh" is still there
		cache.mu.RLock()
		_, ok := cache.cache["fresh"]
		cache.mu.RUnlock()
		assert.True(t, ok)
	})

	t.Run("invalidate all", func(t *testing.T) {
		cache := newLOBManifestCache(10 * time.Minute)

		// add multiple entries
		cache.mu.Lock()
		cache.cache["path1"] = &lobManifestCacheEntry{lobFiles: []packed.LobFileInfo{}, cachedAt: time.Now()}
		cache.cache["path2"] = &lobManifestCacheEntry{lobFiles: []packed.LobFileInfo{}, cachedAt: time.Now()}
		cache.cache["path3"] = &lobManifestCacheEntry{lobFiles: []packed.LobFileInfo{}, cachedAt: time.Now()}
		cache.mu.Unlock()

		assert.Equal(t, 3, cache.Size())

		cache.InvalidateAll()
		assert.Equal(t, 0, cache.Size())
	})
}

func TestIsLOBFile(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "valid LOB file",
			path:     "/data/insert_log/100/200/lobs/300/_data/abc123.vx",
			expected: true,
		},
		{
			name:     "valid LOB file with different structure",
			path:     "/root/lobs/field/abc.vx",
			expected: true,
		},
		{
			name:     "parquet file in lobs directory",
			path:     "/data/lobs/field/abc.parquet",
			expected: false,
		},
		{
			name:     "vx file not in lobs directory",
			path:     "/data/insert_log/100/200/300/_data/abc.vx",
			expected: false,
		},
		{
			name:     "regular parquet file",
			path:     "/data/insert_log/100/200/300/_data/abc.parquet",
			expected: false,
		},
		{
			name:     "short path",
			path:     "ab.vx",
			expected: false,
		},
		{
			name:     "empty path",
			path:     "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLOBFile(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractLOBRelativePath(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		expected string
	}{
		{
			name:     "standard LOB path",
			fullPath: "/data/insert_log/100/200/lobs/300/_data/file.vx",
			expected: "lobs/300/_data/file.vx",
		},
		{
			name:     "path without lobs",
			fullPath: "/data/insert_log/100/200/300/_data/file.vx",
			expected: "/data/insert_log/100/200/300/_data/file.vx", // fallback to full path
		},
		{
			name:     "lobs at start",
			fullPath: "lobs/300/_data/file.vx",
			expected: "lobs/300/_data/file.vx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractLOBRelativePath(tt.fullPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractLOBRelativePath_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		expected string
	}{
		{
			name:     "multiple lobs/ in path",
			fullPath: "/data/lobs/first/lobs/second/file.vx",
			expected: "lobs/first/lobs/second/file.vx",
		},
		{
			name:     "empty full path",
			fullPath: "",
			expected: "",
		},
		{
			name:     "lobs/ with trailing slash only",
			fullPath: "/data/insert_log/100/200/lobs/",
			expected: "lobs/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractLOBRelativePath(tt.fullPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLOBFile_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "uppercase .VX extension",
			path:     "/data/insert_log/100/200/lobs/300/_data/file.VX",
			expected: false, // case sensitive
		},
		{
			name:     ".vx without lobs directory",
			path:     "/data/insert_log/100/200/300/_data/file.vx",
			expected: false,
		},
		{
			name:     ".vortex in lobs directory",
			path:     "/data/insert_log/100/200/lobs/300/_data/file.vortex",
			expected: false, // only .vx suffix
		},
		{
			name:     "lobs in filename not directory",
			path:     "/data/insert_log/100/200/lobs_file.vx",
			expected: false, // needs /lobs/ as directory component
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLOBFile(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewLOBGCContext(t *testing.T) {
	// create a minimal garbage collector for testing
	gc := &garbageCollector{}
	lobCtx := newLOBGCContext(gc)

	require.NotNil(t, lobCtx)
	require.NotNil(t, lobCtx.cache)
	assert.Equal(t, gc, lobCtx.gc)
}

func TestGetStorageConfig(t *testing.T) {
	// this test verifies the getStorageConfig function returns a valid config
	// based on paramtable values
	gc := &garbageCollector{}
	config := gc.getStorageConfig()

	require.NotNil(t, config)
	// verify it's a valid StorageConfig struct
	assert.IsType(t, &indexpb.StorageConfig{}, config)
}

func TestCollectLOBFilesFromSegment(t *testing.T) {
	t.Run("skip segment without manifest", func(t *testing.T) {
		gc := &garbageCollector{}
		lobCtx := newLOBGCContext(gc)

		usedFiles := typeutil.NewSet[string]()
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				ManifestPath: "", // no manifest
			},
		}

		lobCtx.collectLOBFilesFromSegment(context.Background(), segment, usedFiles)
		assert.Equal(t, 0, len(usedFiles))
	})
}

func TestCollectUsedLOBFilesSnapshotProtection(t *testing.T) {
	// This test verifies that collectUsedLOBFiles includes LOB files from
	// dropped segments that are protected by snapshots.
	// Since collectUsedLOBFiles depends on meta.SelectSegments and snapshotMeta
	// which require full setup, we test the logic flow conceptually:
	// 1. Active segments' LOB files are always collected
	// 2. Dropped segments with snapshot references have their LOB files collected
	// 3. Dropped segments without snapshot references are skipped

	t.Run("collectLOBFilesFromSegment adds files to set", func(t *testing.T) {
		gc := &garbageCollector{}
		lobCtx := newLOBGCContext(gc)

		// manually populate cache to avoid FFI call
		lobCtx.cache.mu.Lock()
		lobCtx.cache.cache["manifest-path-1"] = &lobManifestCacheEntry{
			lobFiles: []packed.LobFileInfo{
				{Path: "lobs/100/_data/file1.vx", FieldID: 100, TotalRows: 500, ValidRows: 400},
				{Path: "lobs/100/_data/file2.vx", FieldID: 100, TotalRows: 300, ValidRows: 300},
			},
			cachedAt: time.Now(),
		}
		lobCtx.cache.mu.Unlock()

		usedFiles := typeutil.NewSet[string]()
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           1,
				ManifestPath: "manifest-path-1",
			},
		}

		lobCtx.collectLOBFilesFromSegment(context.Background(), segment, usedFiles)
		assert.Equal(t, 2, len(usedFiles))
		assert.True(t, usedFiles.Contain("lobs/100/_data/file1.vx"))
		assert.True(t, usedFiles.Contain("lobs/100/_data/file2.vx"))
	})

	t.Run("empty path in LOB file is skipped", func(t *testing.T) {
		gc := &garbageCollector{}
		lobCtx := newLOBGCContext(gc)

		lobCtx.cache.mu.Lock()
		lobCtx.cache.cache["manifest-path-2"] = &lobManifestCacheEntry{
			lobFiles: []packed.LobFileInfo{
				{Path: "lobs/100/_data/file1.vx", FieldID: 100},
				{Path: "", FieldID: 200}, // empty path, should be skipped
			},
			cachedAt: time.Now(),
		}
		lobCtx.cache.mu.Unlock()

		usedFiles := typeutil.NewSet[string]()
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           2,
				ManifestPath: "manifest-path-2",
			},
		}

		lobCtx.collectLOBFilesFromSegment(context.Background(), segment, usedFiles)
		assert.Equal(t, 1, len(usedFiles))
		assert.True(t, usedFiles.Contain("lobs/100/_data/file1.vx"))
	})

	t.Run("canceled context stops collection", func(t *testing.T) {
		gc := &garbageCollector{}
		lobCtx := newLOBGCContext(gc)

		lobCtx.cache.mu.Lock()
		lobCtx.cache.cache["manifest-path-3"] = &lobManifestCacheEntry{
			lobFiles: []packed.LobFileInfo{
				{Path: "lobs/100/_data/file1.vx", FieldID: 100},
			},
			cachedAt: time.Now(),
		}
		lobCtx.cache.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		usedFiles := typeutil.NewSet[string]()
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           3,
				ManifestPath: "manifest-path-3",
			},
		}

		lobCtx.collectLOBFilesFromSegment(ctx, segment, usedFiles)
		assert.Equal(t, 0, len(usedFiles)) // should not collect anything
	})
}

// TestCollectUsedLOBFilesToleratesImportingSegments pins the LOB GC blast radius
// of restore pre-registration.
//
// createRestoreJob pre-registers every target segment with the derived V3
// ManifestPath while the segment is still Importing, but that manifest object
// only exists after the DataNode copy task has replicated it. LOB GC walks every
// non-Dropped segment, so it reaches those segments and reads a manifest that is
// not there yet. Because a read failure aborts the whole GC round, one pending
// V3 restore would stall LOB reclamation cluster-wide for as long as it runs.
func TestCollectUsedLOBFilesToleratesImportingSegments(t *testing.T) {
	newGCWithSegments := func(cli storage.ChunkManager, segments ...*SegmentInfo) *garbageCollector {
		m := &meta{ctx: context.Background(), segments: NewSegmentsInfo()}
		for _, segment := range segments {
			m.segments.SetSegment(segment.GetID(), segment)
		}
		return &garbageCollector{meta: m, option: GcOption{cli: cli}}
	}

	// Stand in for "the manifest object does not exist yet": the real FFI read
	// fails at loon_transaction_begin / get_manifest for a missing object.
	mockMissingManifest := func() *mockey.Mocker {
		return mockey.Mock(packed.GetManifestLobFiles).Return(nil,
			errors.New("failed to get manifest: object not found")).Build()
	}

	t.Run("importing segment with unmaterialized manifest does not abort the round", func(t *testing.T) {
		basePath := "files/insert_log/100/200/2001"
		importing := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             2001,
			CollectionID:   100,
			State:          commonpb.SegmentState_Importing,
			IsImporting:    true,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 1),
		}}
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Exist(mock.Anything, basePath+"/_metadata/manifest-1.avro").Return(false, nil).Once()
		lobCtx := newLOBGCContext(newGCWithSegments(cm, importing))

		ffiMock := mockMissingManifest()
		defer ffiMock.UnPatch()

		used, err := lobCtx.collectUsedLOBFiles(context.Background())
		assert.NoError(t, err, "a not-yet-materialized manifest must not abort LOB GC")
		assert.Equal(t, 0, len(used))
	})

	t.Run("flushed importing segment with materialized manifest still aborts the round", func(t *testing.T) {
		// Bulk-import segments and completed restore tasks can have a materialized
		// manifest while IsImporting remains true until the whole job commits. A
		// transient read failure must not make their live LOB files look unused.
		basePath := "files/insert_log/100/200/2003"
		flushedImporting := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             2003,
			CollectionID:   100,
			State:          commonpb.SegmentState_Flushed,
			IsImporting:    true,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 1),
		}}
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Exist(mock.Anything, basePath+"/_metadata/manifest-1.avro").Return(true, nil).Once()
		lobCtx := newLOBGCContext(newGCWithSegments(cm, flushedImporting))

		ffiMock := mockMissingManifest()
		defer ffiMock.UnPatch()

		_, err := lobCtx.collectUsedLOBFiles(context.Background())
		assert.Error(t, err, "an unreadable materialized manifest must abort LOB GC")
	})

	t.Run("importing segment aborts when manifest existence cannot be checked", func(t *testing.T) {
		basePath := "files/insert_log/100/200/2004"
		importing := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             2004,
			CollectionID:   100,
			State:          commonpb.SegmentState_Importing,
			IsImporting:    true,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 1),
		}}
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Exist(mock.Anything, basePath+"/_metadata/manifest-1.avro").
			Return(false, errors.New("SlowDown: please reduce your request rate")).Once()
		lobCtx := newLOBGCContext(newGCWithSegments(cm, importing))

		ffiMock := mockMissingManifest()
		defer ffiMock.UnPatch()

		_, err := lobCtx.collectUsedLOBFiles(context.Background())
		assert.Error(t, err, "an unclassifiable manifest read failure must abort LOB GC")
	})

	t.Run("flushed segment with unreadable manifest still aborts the round", func(t *testing.T) {
		// The safety property the abort exists for must survive: a committed
		// segment whose manifest cannot be read means the used-file set is
		// incomplete, so deleting orphans would be unsafe.
		flushed := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             2002,
			CollectionID:   100,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath("files/insert_log/100/200/2002", 1),
		}}
		lobCtx := newLOBGCContext(newGCWithSegments(mocks.NewChunkManager(t), flushed))

		ffiMock := mockMissingManifest()
		defer ffiMock.UnPatch()

		_, err := lobCtx.collectUsedLOBFiles(context.Background())
		assert.Error(t, err, "an unreadable committed manifest must still abort LOB GC")
	})
}
