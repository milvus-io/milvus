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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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
		basePath string
		expected string
	}{
		{
			name:     "standard LOB path",
			fullPath: "/data/insert_log/100/200/lobs/300/_data/file.vx",
			basePath: "/data/insert_log",
			expected: "lobs/300/_data/file.vx",
		},
		{
			name:     "path without lobs",
			fullPath: "/data/insert_log/100/200/300/_data/file.vx",
			basePath: "/data/insert_log",
			expected: "/data/insert_log/100/200/300/_data/file.vx", // fallback to full path
		},
		{
			name:     "lobs at start",
			fullPath: "lobs/300/_data/file.vx",
			basePath: "",
			expected: "lobs/300/_data/file.vx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractLOBRelativePath(tt.fullPath, tt.basePath)
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
