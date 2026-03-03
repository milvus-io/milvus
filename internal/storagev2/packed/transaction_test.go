// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestToRelativePath(t *testing.T) {
	tests := []struct {
		name     string
		fullPath string
		basePath string
		rootPath string
		expected string
	}{
		{
			// basePath has depth 4 (insert_log/123/456/789), +1 for _delta/ = 5 levels of ..
			name:     "deltalog relative to insert_log",
			fullPath: "/data/delta_log/123/456/789/1",
			basePath: "/data/insert_log/123/456/789",
			rootPath: "/data",
			expected: "../../../../../delta_log/123/456/789/1",
		},
		{
			name:     "same collection different segment",
			fullPath: "/milvus/delta_log/100/200/300/5",
			basePath: "/milvus/insert_log/100/200/300",
			rootPath: "/milvus",
			expected: "../../../../../delta_log/100/200/300/5",
		},
		{
			name:     "path without rootPath prefix",
			fullPath: "/other/path/file",
			basePath: "/data/insert_log/123/456/789",
			rootPath: "/data",
			expected: "/other/path/file",
		},
		{
			name:     "empty rootPath",
			fullPath: "delta_log/123/456/789/1",
			basePath: "insert_log/123/456/789",
			rootPath: "",
			expected: "../../../../../delta_log/123/456/789/1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toRelativePath(tt.fullPath, tt.basePath, tt.rootPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeltaLogEntry(t *testing.T) {
	entry := DeltaLogEntry{
		Path:       "/data/delta_log/123/456/789/1",
		NumEntries: 100,
	}

	assert.Equal(t, "/data/delta_log/123/456/789/1", entry.Path)
	assert.Equal(t, int64(100), entry.NumEntries)
}

// createBaseManifest creates a base manifest via FFIPackedWriter for delta log tests.
// basePath should follow production pattern: filepath.Join(rootPath, "insert_log/collID/partID/segID")
func createBaseManifest(t *testing.T, basePath string, storageConfig *indexpb.StorageConfig) string {
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "ts",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	pw, err := NewFFIPackedWriter(basePath, 0, schema, columnGroups, storageConfig, nil)
	require.NoError(t, err)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.Int64Builder).Append(1000)
	rec := b.NewRecord()
	defer rec.Release()

	err = pw.WriteRecordBatch(rec)
	require.NoError(t, err)

	manifestPath, err := pw.Close()
	require.NoError(t, err)
	return manifestPath
}

func TestGetDeltaLogPathsFromManifest(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := t.TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	// Use basePath that includes rootPath, matching production pattern:
	// basePath = path.Join(storageConfig.GetRootPath(), "insert_log", collID, partID, segID)
	basePath := filepath.Join(dir, "insert_log/1/2/3")

	t.Run("no delta logs", func(t *testing.T) {
		manifestPath := createBaseManifest(t, basePath, storageConfig)
		paths, err := GetDeltaLogPathsFromManifest(manifestPath, storageConfig)
		assert.NoError(t, err)
		assert.Nil(t, paths)
	})

	t.Run("single delta log", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_single")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		// deltaPath follows production pattern: path.Join(rootPath, "delta_log", collID, partID, segID, logID)
		deltaFullPath := filepath.Join(dir, "delta_log/1/2/3/101")
		newManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, []DeltaLogEntry{
			{Path: deltaFullPath, NumEntries: 5},
		})
		require.NoError(t, err)

		paths, err := GetDeltaLogPathsFromManifest(newManifest, storageConfig)
		assert.NoError(t, err)
		require.Equal(t, 1, len(paths))
		// Verify the returned path resolves to the correct delta log location
		assert.Contains(t, paths[0], "delta_log/1/2/3/101")
	})

	t.Run("multiple delta logs", func(t *testing.T) {
		bp := filepath.Join(dir, "insert_log/1/2/3_multi")
		manifestPath := createBaseManifest(t, bp, storageConfig)

		deltaLogs := []DeltaLogEntry{
			{Path: filepath.Join(dir, "delta_log/1/2/3/201"), NumEntries: 5},
			{Path: filepath.Join(dir, "delta_log/1/2/3/202"), NumEntries: 3},
		}
		newManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, deltaLogs)
		require.NoError(t, err)

		paths, err := GetDeltaLogPathsFromManifest(newManifest, storageConfig)
		assert.NoError(t, err)
		require.Equal(t, 2, len(paths))
		assert.Contains(t, paths[0], "delta_log/1/2/3/201")
		assert.Contains(t, paths[1], "delta_log/1/2/3/202")
	})

	t.Run("empty deltaLogs input returns original manifest", func(t *testing.T) {
		manifestPath := createBaseManifest(t, basePath+"_empty", storageConfig)
		sameManifest, err := AddDeltaLogsToManifest(manifestPath, storageConfig, []DeltaLogEntry{})
		require.NoError(t, err)
		assert.Equal(t, manifestPath, sameManifest)
	})
}
