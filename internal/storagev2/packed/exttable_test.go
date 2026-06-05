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

//go:build dynamic

package packed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExploreFiles_EmptyColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	// Empty columns should still work
	files, _, err := ExploreFilesReturnManifestPath([]string{}, "parquet", "/tmp", "/nonexistent", config, ExternalSpecContext{})

	// Expect error due to nonexistent directory
	assert.Error(t, err)
	assert.Nil(t, files)
}

func TestExploreFiles_InvalidDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	files, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1", "col2"},
		"parquet",
		"/nonexistent/base",
		"/nonexistent/explore",
		config,
		ExternalSpecContext{},
	)

	assert.Error(t, err)
	assert.Nil(t, files)
}

func TestGetFileInfo_NonexistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	info, err := GetFileInfo("parquet", "/nonexistent/file.parquet", config, ExternalSpecContext{})

	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestGetFileInfo_InvalidFormat(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	// Create a temp file
	tmpFile := filepath.Join(tmpDir, "test.txt")
	err := os.WriteFile(tmpFile, []byte("not a parquet file"), 0o600)
	require.NoError(t, err)

	info, err := GetFileInfo("parquet", tmpFile, config, ExternalSpecContext{})

	// Should fail because it's not a valid parquet file
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestCreateManifestForSegment_EmptyFragments(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	manifestPath, err := CreateManifestForSegment(
		"/test",
		[]string{"col1"},
		"parquet",
		[]Fragment{},
		config,
	)

	assert.Error(t, err)
	assert.Empty(t, manifestPath)
	assert.Contains(t, err.Error(), "fragments cannot be empty")
}

func TestCreateManifestForSegment_InvalidBasePath(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}

	fragments := []Fragment{
		{
			FragmentID: 0,
			FilePath:   "/data/file.parquet",
			StartRow:   0,
			EndRow:     1000,
			RowCount:   1000,
		},
	}

	// NOTE: C FFI does not validate base path at creation time (paths are virtual in object storage),
	// so this may succeed. We only verify no panic occurs.
	_, _ = CreateManifestForSegment(
		"/test/path",
		[]string{"col1"},
		"parquet",
		fragments,
		config,
	)
}

func TestManifestColumnGroupsToFragments_DeduplicatesByPathRange(t *testing.T) {
	groups := []manifestColumnGroup{
		{
			Columns: []string{"id", "vector"},
			Fragments: []Fragment{
				{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10},
			},
		},
		{
			Columns: []string{"score"},
			Fragments: []Fragment{
				{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10},
			},
		},
	}

	fragments := manifestColumnGroupsToFragments(groups)
	assert.Len(t, fragments, 1)
	assert.Equal(t, "a.parquet", fragments[0].FilePath)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(10), fragments[0].EndRow)
}

func TestColumnsToAppend_SkipsExistingSameFragments(t *testing.T) {
	existing := []manifestColumnGroup{
		{
			Columns: []string{"id"},
			Fragments: []Fragment{
				{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10},
			},
		},
	}
	requested := []string{"id", "score"}
	fragments := []Fragment{{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10}}

	columns, err := columnsToAppend(existing, requested, fragments)
	assert.NoError(t, err)
	assert.Equal(t, []string{"score"}, columns)
}

func TestColumnsToAppendRejectsExistingDifferentFragments(t *testing.T) {
	existing := []manifestColumnGroup{
		{
			Columns: []string{"id"},
			Fragments: []Fragment{
				{FilePath: "a.parquet", StartRow: 0, EndRow: 5, RowCount: 5},
			},
		},
	}
	requested := []string{"id"}
	fragments := []Fragment{{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10}}

	_, err := columnsToAppend(existing, requested, fragments)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists with different fragments")
}

func TestColumnsToAppendRejectsDuplicateExistingColumnWithDifferentFragments(t *testing.T) {
	existing := []manifestColumnGroup{
		{
			Columns: []string{"id"},
			Fragments: []Fragment{
				{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10},
			},
		},
		{
			Columns: []string{"id"},
			Fragments: []Fragment{
				{FilePath: "a.parquet", StartRow: 10, EndRow: 20, RowCount: 10},
			},
		},
	}
	requested := []string{"id"}
	fragments := []Fragment{{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10}}

	_, err := columnsToAppend(existing, requested, fragments)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column id already exists with different fragments")
}

func TestColumnsToAppend_DeduplicatesDuplicateRequestedMissingColumns(t *testing.T) {
	requested := []string{"score", "id", "score", "id"}
	fragments := []Fragment{{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10}}

	columns, err := columnsToAppend(nil, requested, fragments)
	assert.NoError(t, err)
	assert.Equal(t, []string{"score", "id"}, columns)
}

func TestAppendSegmentManifestColumnsUsesCommitManifestUpdates(t *testing.T) {
	ctx := context.Background()
	config := &indexpb.StorageConfig{StorageType: "local"}
	oldManifestPath := MarshalManifestPath("segment", 3)
	fragments := []Fragment{{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10}}

	mockRead := mockey.Mock(readColumnGroupsFromManifest).Return(nil, nil).Build()
	defer mockRead.UnPatch()

	var gotBasePath string
	var gotVersion int64
	var gotAddNewColumnGroups bool
	mockCommit := mockey.Mock(CommitManifestUpdates).
		To(func(basePath string, baseVersion int64, storageConfig *indexpb.StorageConfig, updates *ManifestUpdates) (string, error) {
			gotBasePath = basePath
			gotVersion = baseVersion
			cgs, ok := updates.NewFiles.(*ColumnGroups)
			require.True(t, ok)
			require.NotNil(t, cgs.cColumnGroups)
			gotAddNewColumnGroups = cgs.addNewColumnGroups
			return MarshalManifestPath(basePath, baseVersion+1), nil
		}).Build()
	defer mockCommit.UnPatch()

	got, err := AppendSegmentManifestColumns(ctx, oldManifestPath, "parquet", []string{"score"}, fragments, config)
	require.NoError(t, err)
	assert.Equal(t, MarshalManifestPath("segment", 4), got)
	assert.Equal(t, "segment", gotBasePath)
	assert.Equal(t, int64(3), gotVersion)
	assert.True(t, gotAddNewColumnGroups)
}

func TestAppendSegmentManifestColumnsValidationPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := AppendSegmentManifestColumns(ctx, "manifest", "parquet", []string{"score"}, []Fragment{
		{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10},
	}, &indexpb.StorageConfig{StorageType: "local"})
	assert.ErrorIs(t, err, context.Canceled)

	oldManifestPath := MarshalManifestPath("segment", 3)
	got, err := AppendSegmentManifestColumns(context.Background(), oldManifestPath, "parquet", nil, []Fragment{
		{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10},
	}, &indexpb.StorageConfig{StorageType: "local"})
	assert.NoError(t, err)
	assert.Equal(t, oldManifestPath, got)

	_, err = AppendSegmentManifestColumns(context.Background(), oldManifestPath, "parquet", []string{"score"}, nil,
		&indexpb.StorageConfig{StorageType: "local"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fragments cannot be empty")
}

func TestAppendSegmentManifestColumnsReadAndNoopPaths(t *testing.T) {
	config := &indexpb.StorageConfig{StorageType: "local"}
	oldManifestPath := MarshalManifestPath("segment", 3)
	fragments := []Fragment{{FilePath: "a.parquet", StartRow: 0, EndRow: 10, RowCount: 10}}

	mockReadErr := mockey.Mock(readColumnGroupsFromManifest).Return(nil, fmt.Errorf("read failed")).Build()
	_, err := AppendSegmentManifestColumns(context.Background(), oldManifestPath, "parquet", []string{"score"}, fragments, config)
	mockReadErr.UnPatch()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read manifest column groups")

	existing := []manifestColumnGroup{{
		Columns:   []string{"score"},
		Fragments: fragments,
	}}
	mockReadNoop := mockey.Mock(readColumnGroupsFromManifest).Return(existing, nil).Build()
	got, err := AppendSegmentManifestColumns(context.Background(), oldManifestPath, "parquet", []string{"score"}, fragments, config)
	mockReadNoop.UnPatch()
	assert.NoError(t, err)
	assert.Equal(t, oldManifestPath, got)

	mockReadParse := mockey.Mock(readColumnGroupsFromManifest).Return(nil, nil).Build()
	_, err = AppendSegmentManifestColumns(context.Background(), "not-json", "parquet", []string{"score"}, fragments, config)
	mockReadParse.UnPatch()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse manifest path")
}

func TestFileInfo_Struct(t *testing.T) {
	info := FileInfo{
		FilePath: "/data/test.parquet",
		NumRows:  1000000,
	}

	assert.Equal(t, "/data/test.parquet", info.FilePath)
	assert.Equal(t, int64(1000000), info.NumRows)
}

func TestFragment_Struct(t *testing.T) {
	fragment := Fragment{
		FragmentID: 1,
		FilePath:   "/data/large.parquet",
		StartRow:   1000000,
		EndRow:     2000000,
		RowCount:   1000000,
	}

	assert.Equal(t, int64(1), fragment.FragmentID)
	assert.Equal(t, "/data/large.parquet", fragment.FilePath)
	assert.Equal(t, int64(1000000), fragment.StartRow)
	assert.Equal(t, int64(2000000), fragment.EndRow)
	assert.Equal(t, int64(1000000), fragment.RowCount)
}

func TestFragment_RowCountConsistency(t *testing.T) {
	// Test that RowCount should equal EndRow - StartRow
	testCases := []struct {
		startRow int64
		endRow   int64
	}{
		{0, 1000},
		{1000, 2000},
		{0, 1000000},
		{5000000, 10000000},
	}

	for _, tc := range testCases {
		fragment := Fragment{
			FragmentID: 0,
			FilePath:   "/data/test.parquet",
			StartRow:   tc.startRow,
			EndRow:     tc.endRow,
			RowCount:   tc.endRow - tc.startRow,
		}

		assert.Equal(t, fragment.EndRow-fragment.StartRow, fragment.RowCount)
	}
}

func TestNewFragmentIDGenerator(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	assert.Equal(t, int64(0), gen())
	assert.Equal(t, int64(1), gen())
	assert.Equal(t, int64(2), gen())

	gen2 := NewFragmentIDGenerator(100)
	assert.Equal(t, int64(100), gen2())
	assert.Equal(t, int64(101), gen2())
}

func TestSplitFileToFragments_SmallFile(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	fragments := SplitFileToFragments("/data/small.parquet", 500, 1000, gen)

	assert.Len(t, fragments, 1)
	assert.Equal(t, int64(0), fragments[0].FragmentID)
	assert.Equal(t, "/data/small.parquet", fragments[0].FilePath)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(500), fragments[0].EndRow)
	assert.Equal(t, int64(500), fragments[0].RowCount)
}

func TestSplitFileToFragments_ExactLimit(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	fragments := SplitFileToFragments("/data/exact.parquet", 1000, 1000, gen)

	assert.Len(t, fragments, 1)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(1000), fragments[0].EndRow)
}

func TestSplitFileToFragments_LargeFile(t *testing.T) {
	gen := NewFragmentIDGenerator(0)
	fragments := SplitFileToFragments("/data/large.parquet", 2500, 1000, gen)

	assert.Len(t, fragments, 3)
	// Fragment 0: [0, 1000)
	assert.Equal(t, int64(0), fragments[0].FragmentID)
	assert.Equal(t, int64(0), fragments[0].StartRow)
	assert.Equal(t, int64(1000), fragments[0].EndRow)
	assert.Equal(t, int64(1000), fragments[0].RowCount)
	// Fragment 1: [1000, 2000)
	assert.Equal(t, int64(1), fragments[1].FragmentID)
	assert.Equal(t, int64(1000), fragments[1].StartRow)
	assert.Equal(t, int64(2000), fragments[1].EndRow)
	// Fragment 2: [2000, 2500) — partial last fragment
	assert.Equal(t, int64(2), fragments[2].FragmentID)
	assert.Equal(t, int64(2000), fragments[2].StartRow)
	assert.Equal(t, int64(2500), fragments[2].EndRow)
	assert.Equal(t, int64(500), fragments[2].RowCount)
}

func TestSplitFileToFragments_FragmentIDContinuity(t *testing.T) {
	gen := NewFragmentIDGenerator(10)
	fragments := SplitFileToFragments("/data/f.parquet", 3000, 1000, gen)

	assert.Len(t, fragments, 3)
	assert.Equal(t, int64(10), fragments[0].FragmentID)
	assert.Equal(t, int64(11), fragments[1].FragmentID)
	assert.Equal(t, int64(12), fragments[2].FragmentID)
}

func TestGetColumnNamesFromSchema_Nil(t *testing.T) {
	assert.Nil(t, GetColumnNamesFromSchema(nil))
}

func TestGetColumnNamesFromSchema_NoExternalField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id"},
			{Name: "vector"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"id", "vector"}, columns)
}

func TestGetColumnNamesFromSchema_WithExternalField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "vector"},
			{Name: "text", ExternalField: "raw_text"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"external_id", "vector", "raw_text"}, columns)
}

func TestGetColumnNamesFromSchema_MilvusTableUsesSourceFieldIDs(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "__virtual_pk__"},
			{FieldID: 100, Name: "target_pk", ExternalField: "pk"},
			{FieldID: 101, Name: "target_vector", ExternalField: "vector"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"100", "101"}, columns)
}

func TestGetColumnNamesFromSchema_MilvusTableSourceSchemaUsesFieldIDs(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk"},
			{FieldID: 101, Name: "vector"},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"100", "101"}, columns)
}

func TestGetColumnNamesFromSchema_MilvusTableRealPKIncludesTimestamp(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", IsPrimaryKey: true},
			{FieldID: 101, Name: "vector"},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		},
	}

	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"100", "101", "1"}, columns)
}

func TestGetColumnNamesFromSchema_EmptyFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Nil(t, columns)
}

func TestGetColumnNamesFromSchema_ExternalSkipsFunctionOutputAndSystemFields(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/path",
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "external_id"},
			{Name: "virtual_pk"},
			{Name: "sparse", IsFunctionOutput: true},
		},
	}
	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"external_id"}, columns)
}

func TestGetColumnNamesFromSchema_MilvusTableSkipsFunctionOutput(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		ExternalSource: "s3://bucket/snapshots/100/metadata/200.json",
		ExternalSpec:   `{"format":"milvus-table"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", ExternalField: "text"},
			{FieldID: 101, Name: "vec", ExternalField: "vec"},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "bm25",
				InputFieldNames:  []string{"text"},
				OutputFieldNames: []string{"sparse"},
			},
		},
	}

	columns := GetColumnNamesFromSchema(schema)
	assert.Equal(t, []string{"100", "101"}, columns)
}

func TestHasExternalPrimaryKey(t *testing.T) {
	assert.False(t, HasExternalPrimaryKey(nil))

	assert.True(t, HasExternalPrimaryKey(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", IsPrimaryKey: true, ExternalField: "source_id"},
		},
	}))

	assert.False(t, HasExternalPrimaryKey(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "__virtual_pk__", IsPrimaryKey: true},
			{Name: "id", ExternalField: "source_id"},
		},
	}))

	assert.False(t, HasExternalPrimaryKey(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "id", ExternalField: "source_id"},
		},
	}))
}

func TestBuildCurrentSegmentFragments_NoManifest(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{ID: 1, NumOfRows: 1000},
		{ID: 2, NumOfRows: 2000},
	}
	result, err := BuildCurrentSegmentFragments(segments, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, result, 2)

	// Check virtual fragment for segment 1
	assert.Len(t, result[1], 1)
	assert.Equal(t, int64(1), result[1][0].FragmentID)
	assert.Equal(t, "", result[1][0].FilePath)
	assert.Equal(t, int64(0), result[1][0].StartRow)
	assert.Equal(t, int64(1000), result[1][0].EndRow)
	assert.Equal(t, int64(1000), result[1][0].RowCount)

	// Check virtual fragment for segment 2
	assert.Len(t, result[2], 1)
	assert.Equal(t, int64(2000), result[2][0].RowCount)
}

func TestBuildCurrentSegmentFragments_EmptySegments(t *testing.T) {
	result, err := BuildCurrentSegmentFragments(nil, nil, nil)
	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestBuildCurrentSegmentFragments_ManifestPathWithNilConfig(t *testing.T) {
	// When storageConfig is nil, manifest path should be skipped (use virtual fragment)
	segments := []*datapb.SegmentInfo{
		{ID: 1, NumOfRows: 500, ManifestPath: "some/path"},
	}
	result, err := BuildCurrentSegmentFragments(segments, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, result[1], 1)
	assert.Equal(t, int64(500), result[1][0].RowCount)
}

func TestBuildCurrentSegmentFragments_PassesColumns(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	segments := []*datapb.SegmentInfo{
		{ID: 1, NumOfRows: 500, ManifestPath: MarshalManifestPath("files/insert_log/1/2/3", 1)},
	}
	expected := []Fragment{{FragmentID: 7, FilePath: "/data/file.parquet", RowCount: 500}}

	var gotColumns []string
	mockRead := mockey.Mock(ReadFragmentsFromManifest).
		To(func(manifestPath string, storageConfig *indexpb.StorageConfig, columns []string) ([]Fragment, error) {
			gotColumns = columns
			return expected, nil
		}).Build()
	defer mockRead.UnPatch()

	result, err := BuildCurrentSegmentFragments(segments, storageConfig, []string{"text_col"})
	require.NoError(t, err)
	assert.Equal(t, expected, result[1])
	assert.Equal(t, []string{"text_col"}, gotColumns)
}

func TestBuildCurrentSegmentFragments_ManifestErrorsAndEmptyResult(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{RootPath: "files", StorageType: "local"}
	segments := []*datapb.SegmentInfo{
		{ID: 1, NumOfRows: 500, ManifestPath: MarshalManifestPath("files/insert_log/1/2/3", 1)},
	}

	mockRead := mockey.Mock(ReadFragmentsFromManifest).Return(nil, fmt.Errorf("read failed")).Build()
	_, err := BuildCurrentSegmentFragments(segments, storageConfig, []string{"text_col"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read manifest for segment 1")
	mockRead.UnPatch()

	mockRead = mockey.Mock(ReadFragmentsFromManifest).Return(nil, nil).Build()
	defer mockRead.UnPatch()
	result, err := BuildCurrentSegmentFragments(segments, storageConfig, []string{"missing_col"})
	require.NoError(t, err)
	require.Len(t, result[1], 1)
	assert.Equal(t, int64(1), result[1][0].FragmentID)
	assert.Equal(t, int64(500), result[1][0].RowCount)
}

func TestReadFragmentsFromManifest_FiltersColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}
	fragments := []Fragment{
		{FragmentID: 0, FilePath: filepath.Join(tmpDir, "data-1.parquet"), StartRow: 0, EndRow: 10, RowCount: 10},
		{FragmentID: 1, FilePath: filepath.Join(tmpDir, "data-2.parquet"), StartRow: 10, EndRow: 20, RowCount: 10},
	}

	manifestPath, err := CreateManifestForSegment(
		filepath.Join(tmpDir, "segment"),
		[]string{"text_col", "101"},
		"parquet",
		fragments,
		config,
	)
	require.NoError(t, err)

	got, err := ReadFragmentsFromManifest(manifestPath, config, []string{"text_col"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, fragments[0].FilePath, got[0].FilePath)
	assert.Equal(t, int64(0), got[0].StartRow)
	assert.Equal(t, int64(10), got[0].EndRow)
	assert.Equal(t, int64(10), got[0].RowCount)
	assert.Equal(t, fragments[1].FilePath, got[1].FilePath)

	got, err = ReadFragmentsFromManifest(manifestPath, config, []string{"missing_col"})
	require.NoError(t, err)
	assert.Empty(t, got)

	has, err := ManifestHasColumns(manifestPath, config, []string{"text_col", "101"})
	require.NoError(t, err)
	assert.True(t, has)

	has, err = ManifestHasColumns(manifestPath, config, []string{"text_col", "missing_col"})
	require.NoError(t, err)
	assert.False(t, has)

	has, err = ManifestHasColumns(manifestPath, config, nil)
	require.NoError(t, err)
	assert.True(t, has)

	_, err = ReadFragmentsFromManifest("bad manifest", config, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse manifest path")
}

func TestMarshalUnmarshalManifestPath(t *testing.T) {
	original := "/base/path/to/manifest"
	version := int64(42)

	marshaled := MarshalManifestPath(original, version)
	assert.NotEmpty(t, marshaled)

	basePath, ver, err := UnmarshalManifestPath(marshaled)
	assert.NoError(t, err)
	assert.Equal(t, original, basePath)
	assert.Equal(t, version, ver)
}

func TestUnmarshalManifestPath_InvalidJSON(t *testing.T) {
	_, _, err := UnmarshalManifestPath("not valid json")
	assert.Error(t, err)
}

func TestUnmarshalManifestPath_EmptyString(t *testing.T) {
	_, _, err := UnmarshalManifestPath("")
	assert.Error(t, err)
}

func TestMarshalManifestPath_EmptyBasePath(t *testing.T) {
	marshaled := MarshalManifestPath("", 0)
	basePath, ver, err := UnmarshalManifestPath(marshaled)
	assert.NoError(t, err)
	assert.Equal(t, "", basePath)
	assert.Equal(t, int64(0), ver)
}

func TestCreateSegmentManifestWithBasePath_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := CreateSegmentManifestWithBasePath(ctx, "/base", "parquet", []string{"col1"}, nil, nil)
	assert.ErrorIs(t, err, context.Canceled)
}

// The cgo bridge (loon_properties_inject_external_spec) and Tier-1/2 endpoint
// derivation logic are covered by the C++ gtest suite in test_external_take.cpp.
// Go-side coverage is exercised indirectly through callers that invoke
// injectExternalSpecProperties (GetFileInfo, ExploreFilesReturnManifestPath,
// SampleExternalFieldSizes).

func TestExtfsPrefixForCollection(t *testing.T) {
	assert.Equal(t, "extfs.42.", ExtfsPrefixForCollection(42))
	assert.Equal(t, "extfs.0.", ExtfsPrefixForCollection(0))
}

// NOTE: the Go-side BuildExtfsOverrides / NormalizeExternalSource /
// effectiveSpecAddress helpers were collapsed into the C++ InjectExternalSpecProperties
// pipeline. Tier-1/2 endpoint derivation and AWS-form rewriting are covered by
// the C++ gtest suite in test_external_take.cpp. Go callers exercise the cgo
// bridge indirectly through the public FFI wrappers.

func TestFilterFileInfosByFormat(t *testing.T) {
	files := []FileInfo{
		{FilePath: "data/file1.parquet", NumRows: 100},
		{FilePath: "data/file2.parquet", NumRows: 200},
		{FilePath: "data/_SUCCESS", NumRows: 0},
		{FilePath: "data/metadata.json", NumRows: 0},
		{FilePath: "data/file3.csv", NumRows: 50},
		{FilePath: "data/FILE4.PARQUET", NumRows: 300},
	}

	// Parquet: keep .parquet only (case-insensitive)
	filtered, skipped := filterFileInfosByFormat(files, "parquet")
	assert.Equal(t, 3, len(filtered))
	assert.Equal(t, 3, skipped)
	assert.Equal(t, "data/file1.parquet", filtered[0].FilePath)
	assert.Equal(t, "data/FILE4.PARQUET", filtered[2].FilePath)

	// Vortex: keep .vortex only
	vortexFiles := []FileInfo{
		{FilePath: "a.vortex"},
		{FilePath: "b.parquet"},
		{FilePath: "c.vortex"},
	}
	filtered, skipped = filterFileInfosByFormat(vortexFiles, "vortex")
	assert.Equal(t, 2, len(filtered))
	assert.Equal(t, 1, skipped)

	// Lance-table: keep all (directory-based, no extension filtering)
	filtered, skipped = filterFileInfosByFormat(files, "lance-table")
	assert.Equal(t, len(files), len(filtered))
	assert.Equal(t, 0, skipped)

	// Unknown format: keep all
	filtered, skipped = filterFileInfosByFormat(files, "unknown")
	assert.Equal(t, len(files), len(filtered))
	assert.Equal(t, 0, skipped)

	// Empty list
	filtered, skipped = filterFileInfosByFormat(nil, "parquet")
	assert.Equal(t, 0, len(filtered))
	assert.Equal(t, 0, skipped)
}

// Regression for index-drift bug: DataCoord and DataNode both call
// NormalizeFileInfos on the same raw manifest and MUST produce the
// identical indexed view, regardless of input ordering. Without sort,
// arrow's GetFileInfo can return files in different orders on each
// process and the [fileIndexBegin, fileIndexEnd) slice picks different
// files on the two sides, leading to silent data loss or "Invalid
// parquet magic" failures on Spark `_SUCCESS`/`.crc` strays.
func TestNormalizeFileInfos_StableSortAndFilter(t *testing.T) {
	rawA := []FileInfo{
		{FilePath: "data/_SUCCESS"},
		{FilePath: "data/part-2.parquet"},
		{FilePath: "data/.crc"},
		{FilePath: "data/part-1.parquet"},
		{FilePath: "data/README.md"},
	}
	// Reordered "DataNode side" view (arrow GetFileInfo gives no order).
	rawB := []FileInfo{
		{FilePath: "data/part-1.parquet"},
		{FilePath: "data/README.md"},
		{FilePath: "data/_SUCCESS"},
		{FilePath: "data/part-2.parquet"},
		{FilePath: "data/.crc"},
	}

	gotA, skippedA := NormalizeFileInfos(rawA, "parquet")
	gotB, skippedB := NormalizeFileInfos(rawB, "parquet")

	assert.Equal(t, 2, len(gotA))
	assert.Equal(t, 3, skippedA)
	assert.Equal(t, gotA, gotB, "two views of same manifest must produce identical indexed slice")
	assert.Equal(t, "data/part-1.parquet", gotA[0].FilePath)
	assert.Equal(t, "data/part-2.parquet", gotA[1].FilePath)
	assert.Equal(t, skippedA, skippedB)
}

func TestMakePropertiesFromStorageConfig_ExtraKVsOverride(t *testing.T) {
	// Test that extraKVs can add per-collection extfs properties
	config := &indexpb.StorageConfig{
		StorageType: "minio",
		BucketName:  "original-bucket",
		Address:     "localhost:9000",
		RootPath:    "/data",
	}
	prefix := ExtfsPrefixForCollection(42)
	extra := map[string]string{
		prefix + "bucket_name": "external-bucket",
		prefix + "address":     "http://s3.amazonaws.com",
	}
	props, err := MakePropertiesFromStorageConfig(config, extra)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

func TestMakePropertiesFromStorageConfig_UsesConfiguredStorageFormat(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	t.Cleanup(func() {
		_ = params.Reset(params.DataNodeCfg.StorageFormat.Key)
	})

	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  t.TempDir(),
		RootPath:    t.TempDir(),
	}

	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	assert.Equal(t, "parquet", loonPropertyString(props, PropertyWriterFormat))
}

func TestMakePropertiesFromStorageConfig_ExtraKVsOverrideStorageFormat(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "vortex"))
	t.Cleanup(func() {
		_ = params.Reset(params.DataNodeCfg.StorageFormat.Key)
	})

	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  t.TempDir(),
		RootPath:    t.TempDir(),
	}

	props, err := MakePropertiesFromStorageConfig(config, map[string]string{
		PropertyWriterFormat: "parquet",
	})
	require.NoError(t, err)
	defer FreeProperties(props)
	assert.Equal(t, "parquet", loonPropertyString(props, PropertyWriterFormat))
}

func TestNormalizeExternalPathForStorage_UsesInjectedExtfsEndpoint(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	extfs := ExternalSpecContext{
		CollectionID: 42,
		Source:       "s3://liyiyang-test/all_types_v2/",
		Spec:         `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}

	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	require.NoError(t, injectExternalSpecProperties(props, extfs.CollectionID, extfs.Source, extfs.Spec))

	got, err := normalizeExternalPathForStorage(extfs.Source, props, extfs)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/liyiyang-test/all_types_v2/", got)

	got, err = normalizeExternalPathForStorage("s3://liyiyang-test/a/b/file.parquet?versionId=1", props, extfs)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/liyiyang-test/a/b/file.parquet?versionId=1", got)
}

func TestNormalizeExternalPathForStorage_EndpointFormUnchanged(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	source := "s3://s3.us-west-2.amazonaws.com/liyiyang-test/all_types_v2/"
	extfs := ExternalSpecContext{
		CollectionID: 42,
		Source:       source,
		Spec:         `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}

	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	require.NoError(t, injectExternalSpecProperties(props, extfs.CollectionID, extfs.Source, extfs.Spec))

	got, err := normalizeExternalPathForStorage(source, props, extfs)
	require.NoError(t, err)
	assert.Equal(t, source, got)
}

func TestNormalizeExternalPathForStorage_NoRewriteCases(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	prefix := ExtfsPrefixForCollection(42)
	props, err := MakePropertiesFromStorageConfig(config, map[string]string{
		prefix + "address":     "https://s3.us-west-2.amazonaws.com",
		prefix + "bucket_name": "liyiyang-test",
	})
	require.NoError(t, err)
	defer FreeProperties(props)

	path := "s3://liyiyang-test/all_types_v2/"

	got, err := normalizeExternalPathForStorage(path, props, ExternalSpecContext{CollectionID: 42})
	require.NoError(t, err)
	assert.Equal(t, path, got)

	got, err = normalizeExternalPathForStorage(
		path,
		nil,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, path, got)

	got, err = normalizeExternalPathForStorage(
		"relative/path",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "relative/path", got)

	got, err = normalizeExternalPathForStorage(
		"s3://other-bucket/all_types_v2/",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://other-bucket/all_types_v2/", got)
}

func TestNormalizeExternalPathForStorage_BareAddress(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	prefix := ExtfsPrefixForCollection(42)
	props, err := MakePropertiesFromStorageConfig(config, map[string]string{
		prefix + "address":     "s3.us-west-2.amazonaws.com",
		prefix + "bucket_name": "liyiyang-test",
	})
	require.NoError(t, err)
	defer FreeProperties(props)

	got, err := normalizeExternalPathForStorage(
		"s3://liyiyang-test/all_types_v2/",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://liyiyang-test/all_types_v2/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/liyiyang-test/all_types_v2/", got)
}

func TestResolveExternalSourceRelativePath_UsesSourceBucketRoot(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	extfs := ExternalSpecContext{
		CollectionID: 42,
		Source:       "s3://source-bucket/snapshots/100/metadata/200.json",
		Spec:         `{"format":"milvus-table","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}

	props, err := MakePropertiesFromStorageConfig(config, nil)
	require.NoError(t, err)
	defer FreeProperties(props)
	require.NoError(t, injectExternalSpecProperties(props, extfs.CollectionID, extfs.Source, extfs.Spec))

	got, err := resolveExternalSourceRelativePath("files/insert_log/1/2/3", props, extfs)
	require.NoError(t, err)
	assert.Equal(t, "s3://s3.us-west-2.amazonaws.com/source-bucket/files/insert_log/1/2/3", got)
}

func TestNormalizeExternalPathForFilesystem_RemoteURI(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	prefix := ExtfsPrefixForCollection(42)
	props, err := MakePropertiesFromStorageConfig(config, map[string]string{
		prefix + "address":     "http://localhost:9000",
		prefix + "bucket_name": "a-bucket",
	})
	require.NoError(t, err)
	defer FreeProperties(props)

	lookupPath, filePath, err := normalizeExternalPathForFilesystem(
		"s3://localhost:9000/a-bucket/files/snapshots/metadata.json",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://localhost:9000/a-bucket/files/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://localhost:9000/a-bucket/files/snapshots/metadata.json", lookupPath)
	assert.Equal(t, "files/snapshots/metadata.json", filePath)

	lookupPath, filePath, err = normalizeExternalPathForFilesystem(
		"s3://a-bucket/files/snapshots/metadata.json",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://a-bucket/files/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://localhost:9000/a-bucket/files/snapshots/metadata.json", lookupPath)
	assert.Equal(t, "files/snapshots/metadata.json", filePath)

	lookupPath, filePath, err = normalizeExternalPathForFilesystem(
		"s3://other-bucket/files/snapshots/metadata.json",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://a-bucket/files/"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://other-bucket/files/snapshots/metadata.json", lookupPath)
	assert.Equal(t, "s3://other-bucket/files/snapshots/metadata.json", filePath)

	lookupPath, filePath, err = normalizeExternalPathForFilesystem(
		"files/source/_delta/9001",
		props,
		ExternalSpecContext{CollectionID: 42, Source: "s3://a-bucket/snapshots/100/metadata/200.json"},
	)
	require.NoError(t, err)
	assert.Equal(t, "s3://localhost:9000/a-bucket/files/source/_delta/9001", lookupPath)
	assert.Equal(t, "files/source/_delta/9001", filePath)
}

// ==================== SampleExternalFieldSizes Tests ====================

func TestSampleExternalFieldSizes_NilStorageConfig(t *testing.T) {
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/tmp","ver":1}`, 100, 42,
		"s3://bucket/data/", `{"format":"parquet"}`,
		nil,
		nil,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storageConfig is required")
}

func TestSampleExternalFieldSizes_EmptyManifestPath(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	result, err := SampleExternalFieldSizes(
		"", 100, 42,
		"", "",
		nil,
		config,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest_path is empty")
}

func TestSampleExternalFieldSizes_InvalidManifestPath(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	// Valid JSON but manifest file doesn't exist
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/nonexistent/path","ver":1}`, 100, 42,
		"", "",
		nil,
		config,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestSampleExternalFieldSizes_WithSpec(t *testing.T) {
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  "/tmp",
		RootPath:    "/tmp",
	}
	// Will fail at manifest read (no real file), but validates that
	// properties construction with extfs injection via cgo doesn't panic.
	result, err := SampleExternalFieldSizes(
		`{"base_path":"/nonexistent","ver":1}`, 100, 42,
		"s3://s3.us-west-2.amazonaws.com/ext-bucket/data/",
		`{"format":"parquet","extfs":{"region":"us-west-2","use_ssl":"true"}}`,
		nil,
		config,
	)
	assert.Nil(t, result)
	assert.Error(t, err)
}

// ==================== ExploreFilesReturnManifestPath Tests ====================

func TestExploreFilesReturnManifestPath_InvalidDir(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1"}, "parquet", "/nonexistent/base", "/nonexistent/explore", config, ExternalSpecContext{},
	)
	assert.Error(t, err)
}

func TestExploreFilesReturnManifestPath_EmptyColumns(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, _, err := ExploreFilesReturnManifestPath(
		[]string{}, "parquet", "/nonexistent", "/nonexistent", config, ExternalSpecContext{},
	)
	assert.Error(t, err)
}

func TestExploreFilesReturnManifestPath_PropertiesError(t *testing.T) {
	_, _, err := ExploreFilesReturnManifestPath(
		[]string{"col1"}, "parquet", "/base", "/explore", nil, ExternalSpecContext{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties")
}

func TestResolveMilvusTableSnapshotMetadataPathRejectsNonJSONPath(t *testing.T) {
	spec := `{"format":"milvus-table"}`

	t.Run("accepts snapshot metadata JSON path", func(t *testing.T) {
		metadataPath, err := resolveMilvusTableSnapshotMetadataPath("s3://bucket/snapshots/100/metadata/200.json", spec)
		require.NoError(t, err)
		assert.Equal(t, "s3://bucket/snapshots/100/metadata/200.json", metadataPath)
	})

	t.Run("rejects base path", func(t *testing.T) {
		_, err := resolveMilvusTableSnapshotMetadataPath("s3://bucket", spec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot metadata JSON path")
	})
}

// ==================== ReadFileInfosFromManifestPath Tests ====================

func TestReadFileInfosFromManifestPath_InvalidPath(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, err := ReadFileInfosFromManifestPath("/nonexistent/manifest.json", config)
	assert.Error(t, err)
}

func TestReadFileInfosFromManifestPath_NilConfig(t *testing.T) {
	_, err := ReadFileInfosFromManifestPath("/manifest.json", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties")
}

func TestBuildMilvusTableFileInfosFromSnapshotMetadata(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: 2,
		ManifestList: []string{
			"segment-20.avro",
			"segment-10.avro",
		},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{SegmentId: 20, Manifest: `{"base_path":"source/20","ver":1}`},
			{SegmentId: 10, Manifest: `{"base_path":"source/10","ver":2}`},
		},
	}
	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)

	segments := map[string]*datapb.SegmentDescription{
		"segment-10.avro": {SegmentId: 10, NumOfRows: 100},
		"segment-20.avro": {SegmentId: 20, NumOfRows: 200},
	}
	fileInfos, err := buildMilvusTableFileInfosFromSnapshotMetadata(metadataBytes, func(manifestPath string, formatVersion int32) (*datapb.SegmentDescription, error) {
		require.Equal(t, int32(2), formatVersion)
		return segments[manifestPath], nil
	}, nil)

	require.NoError(t, err)
	require.Equal(t, []FileInfo{
		{FilePath: `{"base_path":"source/10","ver":2}`, NumRows: 100, SourceSegmentID: 10},
		{FilePath: `{"base_path":"source/20","ver":1}`, NumRows: 200, SourceSegmentID: 20},
	}, fileInfos)
}

func TestBuildMilvusTableFileInfosFromSnapshotMetadata_ResolvesManifestBasePath(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: 2,
		ManifestList:  []string{"segment-manifest.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{SegmentId: 10, Manifest: `{"base_path":"files/insert_log/10","ver":2}`},
		},
	}
	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)

	resolvedManifest := MarshalManifestPath("s3://source-bucket/files/insert_log/10", 2)
	fileInfos, err := buildMilvusTableFileInfosFromSnapshotMetadata(
		metadataBytes,
		func(manifestPath string, formatVersion int32) (*datapb.SegmentDescription, error) {
			require.Equal(t, "segment-manifest.avro", manifestPath)
			require.Equal(t, int32(2), formatVersion)
			return &datapb.SegmentDescription{
				SegmentId: 10,
				NumOfRows: 100,
			}, nil
		},
		func(manifestPath string) (string, error) {
			return resolveMilvusTableSourceManifestPath(manifestPath, func(sourcePath string) (string, error) {
				return "s3://source-bucket/" + sourcePath, nil
			})
		},
	)

	require.NoError(t, err)
	require.Equal(t, []FileInfo{
		{FilePath: resolvedManifest, NumRows: 100, SourceSegmentID: 10},
	}, fileInfos)
}

func TestResolveMilvusTableSegmentDeltalogPaths(t *testing.T) {
	segment := &datapb.SegmentDescription{
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath: "files/delta_log/10/1",
			}},
		}},
	}

	err := resolveMilvusTableSegmentDeltalogPaths(segment, func(sourcePath string) (string, error) {
		return "s3://source-bucket/" + sourcePath, nil
	})

	require.NoError(t, err)
	require.Equal(t, "s3://source-bucket/files/delta_log/10/1", segment.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestBuildMilvusTableFileInfosFromSnapshotMetadata_NoStorageV2(t *testing.T) {
	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&datapb.SnapshotMetadata{})
	require.NoError(t, err)

	_, err = buildMilvusTableFileInfosFromSnapshotMetadata(metadataBytes, nil, nil)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMilvusTableStorageV2ManifestListMissing))
	assert.Contains(t, err.Error(), "storagev2_manifest_list")
	assert.Contains(t, err.Error(), "common.storage.useLoonFFI=true")
}

func TestBuildMilvusTableFileInfosFromSnapshotMetadata_SourceSegmentDeltalogsSkipped(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: 2,
		ManifestList:  []string{"segment-manifest.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{SegmentId: 10, Manifest: `{"base_path":"source/10","ver":2}`},
		},
	}
	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)

	expected := []*datapb.FieldBinlog{{
		Binlogs: []*datapb.Binlog{{LogPath: "files/source/10/_delta/1", EntriesNum: 8}},
	}}
	fileInfos, err := buildMilvusTableFileInfosFromSnapshotMetadata(metadataBytes, func(manifestPath string, formatVersion int32) (*datapb.SegmentDescription, error) {
		require.Equal(t, "segment-manifest.avro", manifestPath)
		require.Equal(t, int32(2), formatVersion)
		return &datapb.SegmentDescription{
			SegmentId:    10,
			SegmentLevel: datapb.SegmentLevel_L1,
			NumOfRows:    100,
			Deltalogs:    expected,
		}, nil
	}, nil)

	require.NoError(t, err)
	require.Len(t, fileInfos, 1)
	require.Equal(t, int64(10), fileInfos[0].SourceSegmentID)
	require.Empty(t, fileInfos[0].Deltalogs)
}

func TestBuildMilvusTableFileInfosFromSnapshotMetadata_L0Deltalogs(t *testing.T) {
	metadata := &datapb.SnapshotMetadata{
		FormatVersion: 2,
		ManifestList:  []string{"source-manifest.avro", "l0-manifest.avro"},
		Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
			{SegmentId: 10, Manifest: `{"base_path":"source/10","ver":2}`},
		},
	}
	metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
	require.NoError(t, err)

	l0Deltalogs := []*datapb.FieldBinlog{{
		FieldID: 100,
		Binlogs: []*datapb.Binlog{{
			LogPath:    "files/delta_log/1",
			EntriesNum: 8,
		}},
	}}
	fileInfos, err := buildMilvusTableFileInfosFromSnapshotMetadata(metadataBytes, func(manifestPath string, formatVersion int32) (*datapb.SegmentDescription, error) {
		require.Equal(t, int32(2), formatVersion)
		switch manifestPath {
		case "source-manifest.avro":
			return &datapb.SegmentDescription{
				SegmentId:    10,
				SegmentLevel: datapb.SegmentLevel_L1,
				NumOfRows:    100,
			}, nil
		case "l0-manifest.avro":
			return &datapb.SegmentDescription{
				SegmentId:    20,
				SegmentLevel: datapb.SegmentLevel_L0,
				Deltalogs:    l0Deltalogs,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected manifest path %s", manifestPath)
		}
	}, nil)

	require.NoError(t, err)
	require.Len(t, fileInfos, 1)
	require.Equal(t, int64(10), fileInfos[0].SourceSegmentID)
	require.Equal(t, l0Deltalogs, fileInfos[0].Deltalogs)
}

func TestBuildMilvusTableFileInfosFromSnapshotMetadata_DeltalogsBySegmentLevel(t *testing.T) {
	for _, tc := range []struct {
		name          string
		manifestList  []string
		wantDeltalogs bool
	}{
		{name: "source_segment", manifestList: []string{"source-manifest.avro"}, wantDeltalogs: false},
		{name: "l0_overlay", manifestList: []string{"source-manifest.avro", "l0-manifest.avro"}, wantDeltalogs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &datapb.SnapshotMetadata{
				FormatVersion: 2,
				ManifestList:  tc.manifestList,
				Storagev2ManifestList: []*datapb.StorageV2SegmentManifest{
					{SegmentId: 10, Manifest: `{"base_path":"source/10","ver":2}`},
				},
			}
			metadataBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(metadata)
			require.NoError(t, err)

			fileInfos, err := buildMilvusTableFileInfosFromSnapshotMetadata(
				metadataBytes,
				func(manifestPath string, formatVersion int32) (*datapb.SegmentDescription, error) {
					require.Equal(t, int32(2), formatVersion)
					switch manifestPath {
					case "source-manifest.avro":
						return &datapb.SegmentDescription{
							SegmentId:    10,
							SegmentLevel: datapb.SegmentLevel_L1,
							NumOfRows:    100,
						}, nil
					case "l0-manifest.avro":
						return &datapb.SegmentDescription{
							SegmentId:    20,
							SegmentLevel: datapb.SegmentLevel_L0,
							Deltalogs: []*datapb.FieldBinlog{{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{{
									LogPath:    "files/delta_log/1",
									EntriesNum: 1,
								}},
							}},
						}, nil
					default:
						return nil, fmt.Errorf("unexpected manifest path %s", manifestPath)
					}
				},
				nil,
			)

			require.NoError(t, err)
			require.Len(t, fileInfos, 1)
			if tc.wantDeltalogs {
				require.Len(t, fileInfos[0].Deltalogs, 1)
				require.Len(t, fileInfos[0].Deltalogs[0].GetBinlogs(), 1)
				assert.Equal(t, "files/delta_log/1", fileInfos[0].Deltalogs[0].GetBinlogs()[0].GetLogPath())
			} else {
				assert.Empty(t, fileInfos[0].Deltalogs)
			}
		})
	}
}

func TestReadMilvusSnapshotSegmentManifest_Deltalogs(t *testing.T) {
	data, err := snapshotio.MarshalSegmentManifest(&datapb.SegmentDescription{
		SegmentId:      100,
		PartitionId:    10,
		SegmentLevel:   datapb.SegmentLevel_L1,
		ChannelName:    "by-dev-rootcoord-dml_0_100v0",
		NumOfRows:      128,
		StorageVersion: 3,
		IsSorted:       true,
		Deltalogs: []*datapb.FieldBinlog{{
			FieldID: 100,
			Binlogs: []*datapb.Binlog{{
				EntriesNum:    8,
				TimestampFrom: 1,
				TimestampTo:   2,
				LogPath:       "delta/100/1",
				LogSize:       64,
				LogID:         1,
				MemorySize:    64,
			}},
		}},
	})
	require.NoError(t, err)

	segment, err := readMilvusSnapshotSegmentManifest("manifest.avro", snapshotio.SnapshotFormatVersion, func(path string) ([]byte, error) {
		require.Equal(t, "manifest.avro", path)
		return data, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(100), segment.GetSegmentId())
	require.Len(t, segment.GetDeltalogs(), 1)
	require.Len(t, segment.GetDeltalogs()[0].GetBinlogs(), 1)
	require.Equal(t, "delta/100/1", segment.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestReadMilvusSnapshotSegmentManifest_ParseErrorIncludesPath(t *testing.T) {
	_, err := readMilvusSnapshotSegmentManifest("bad-manifest.avro", snapshotio.SnapshotFormatVersion, func(path string) ([]byte, error) {
		require.Equal(t, "bad-manifest.avro", path)
		return []byte("not a segment manifest"), nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse source segment manifest bad-manifest.avro")
}

// ==================== GetFileInfo Additional Tests ====================

func TestGetFileInfo_PropertiesError(t *testing.T) {
	_, err := GetFileInfo("parquet", "/some/file.parquet", nil, ExternalSpecContext{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "properties")
}

func TestMakePropertiesFromStorageConfig_NilConfig(t *testing.T) {
	_, err := MakePropertiesFromStorageConfig(nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storageConfig is required")
}

func TestMakePropertiesFromStorageConfig_LocalType(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}
	props, err := MakePropertiesFromStorageConfig(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

func TestMakePropertiesFromStorageConfig_AllFields(t *testing.T) {
	config := &indexpb.StorageConfig{
		Address:           "localhost:9000",
		BucketName:        "test-bucket",
		AccessKeyID:       "access-key",
		SecretAccessKey:   "secret-key",
		RootPath:          "/data",
		StorageType:       "minio",
		CloudProvider:     "aws",
		IAMEndpoint:       "http://iam.example.com",
		Region:            "us-east-1",
		SslCACert:         "cert-content",
		GcpCredentialJSON: `{"type":"service_account"}`,
		UseSSL:            true,
		UseIAM:            true,
		UseVirtualHost:    true,
		RequestTimeoutMs:  5000,
	}
	props, err := MakePropertiesFromStorageConfig(config, nil)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

// ==================== FetchFragmentsFromExternalSourceWithRange Tests ====================

func TestFetchFragmentsFromExternalSourceWithRange_HappyPath(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	// Mock ReadFileInfosFromManifestPath to return 3 files
	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 1000},
		{FilePath: "f2.parquet", NumRows: 2000},
		{FilePath: "f3.parquet", NumRows: 3000},
	}, nil).Build()
	defer mockRead.UnPatch()

	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", []string{"col1"}, "s3://endpoint/bucket/path", config,
		0, 3, "/manifest.json", ExternalFetchOptions{CollectionID: 1, RowLimit: 5000},
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, fragments)
}

func TestFetchFragmentsFromExternalSourceWithRange_EmptyManifest(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 10, "", ExternalFetchOptions{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest path is required")
}

func TestFetchFragmentsFromExternalSourceWithRange_ReadManifestFailed(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).
		Return(nil, fmt.Errorf("manifest read error")).Build()
	defer mockRead.UnPatch()

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 10, "/manifest.json", ExternalFetchOptions{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest")
}

func TestFetchFragmentsFromExternalSourceWithRange_IndexOutOfRange(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 100},
	}, nil).Build()
	defer mockRead.UnPatch()

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		5, 10, "/manifest.json", ExternalFetchOptions{},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fileIndexBegin")
}

func TestFetchFragmentsFromExternalSourceWithRange_NeedsFileInfo(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	// File with NumRows=0 needs GetFileInfo
	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 0},
	}, nil).Build()
	defer mockRead.UnPatch()

	mockGetInfo := mockey.Mock(GetFileInfo).Return(&FileInfo{
		FilePath: "f1.parquet", NumRows: 500,
	}, nil).Build()
	defer mockGetInfo.UnPatch()

	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{CollectionID: 1, RowLimit: 1000},
	)
	assert.NoError(t, err)
	assert.Len(t, fragments, 1)
	assert.Equal(t, int64(500), fragments[0].RowCount)
}

func TestFetchFragmentsFromExternalSourceWithRange_GetFileInfoFailed(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 0},
	}, nil).Build()
	defer mockRead.UnPatch()

	mockGetInfo := mockey.Mock(GetFileInfo).
		Return(nil, fmt.Errorf("get file info error")).Build()
	defer mockGetInfo.UnPatch()

	_, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 1, "/manifest.json", ExternalFetchOptions{CollectionID: 1},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get file info")
}

func TestFetchFragmentsFromExternalSourceWithRange_FileIndexEndClamped(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{StorageType: "local", BucketName: tmpDir, RootPath: tmpDir}

	mockRead := mockey.Mock(ReadFileInfosFromManifestPath).Return([]FileInfo{
		{FilePath: "f1.parquet", NumRows: 100},
		{FilePath: "f2.parquet", NumRows: 200},
	}, nil).Build()
	defer mockRead.UnPatch()

	// fileIndexEnd (99) > len(fileInfos) (2), should be clamped
	fragments, err := FetchFragmentsFromExternalSourceWithRange(
		ctx, "parquet", nil, "", config,
		0, 99, "/manifest.json", ExternalFetchOptions{CollectionID: 1, RowLimit: 1000},
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, fragments)
}

func TestMakePropertiesFromStorageConfig_WithExtraKVs(t *testing.T) {
	tmpDir := t.TempDir()
	config := &indexpb.StorageConfig{
		StorageType: "local",
		BucketName:  tmpDir,
		RootPath:    tmpDir,
	}
	extra := map[string]string{
		"writer.policy": "split",
		"custom.key":    "custom.value",
	}
	props, err := MakePropertiesFromStorageConfig(config, extra)
	assert.NoError(t, err)
	assert.NotNil(t, props)
	defer FreeProperties(props)
}

// ==================== rowLimitOrDefault Tests ====================

func TestRowLimitOrDefault(t *testing.T) {
	t.Run("zero falls back to default", func(t *testing.T) {
		o := ExternalFetchOptions{RowLimit: 0}
		assert.Equal(t, int64(DefaultFragmentRowLimit), o.rowLimitOrDefault())
	})
	t.Run("negative falls back to default", func(t *testing.T) {
		o := ExternalFetchOptions{RowLimit: -1}
		assert.Equal(t, int64(DefaultFragmentRowLimit), o.rowLimitOrDefault())
	})
	t.Run("positive used as-is", func(t *testing.T) {
		o := ExternalFetchOptions{RowLimit: 1234}
		assert.Equal(t, int64(1234), o.rowLimitOrDefault())
	})
}

// ==================== fetchRowCountsConcurrently Tests ====================

func TestFetchRowCountsConcurrently_AllKnownSkipsFFI(t *testing.T) {
	// All fileInfos already have NumRows > 0 → no GetFileInfo call.
	mockCalled := int32(0)
	m := mockey.Mock(GetFileInfo).To(func(format, path string, cfg *indexpb.StorageConfig, extfs ExternalSpecContext) (*FileInfo, error) {
		atomic.AddInt32(&mockCalled, 1)
		return &FileInfo{FilePath: path, NumRows: 99}, nil
	}).Build()
	defer m.UnPatch()

	fileInfos := []FileInfo{
		{FilePath: "a", NumRows: 10},
		{FilePath: "b", NumRows: 20},
		{FilePath: "c", NumRows: 30},
	}
	got, err := fetchRowCountsConcurrently(context.Background(), "parquet", fileInfos, &indexpb.StorageConfig{}, ExternalSpecContext{})
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 20, 30}, got)
	assert.Equal(t, int32(0), atomic.LoadInt32(&mockCalled))
}

func TestFetchRowCountsConcurrently_MissingFilled(t *testing.T) {
	m := mockey.Mock(GetFileInfo).To(func(format, path string, cfg *indexpb.StorageConfig, extfs ExternalSpecContext) (*FileInfo, error) {
		return &FileInfo{FilePath: path, NumRows: 777}, nil
	}).Build()
	defer m.UnPatch()

	fileInfos := []FileInfo{
		{FilePath: "a", NumRows: 10},
		{FilePath: "b", NumRows: 0},
		{FilePath: "c", NumRows: -1},
	}
	got, err := fetchRowCountsConcurrently(context.Background(), "parquet", fileInfos, &indexpb.StorageConfig{}, ExternalSpecContext{})
	assert.NoError(t, err)
	assert.Equal(t, []int64{10, 777, 777}, got)
}

func TestFetchRowCountsConcurrently_WorkerErrorPropagated(t *testing.T) {
	m := mockey.Mock(GetFileInfo).Return(nil, fmt.Errorf("fake ffi failure")).Build()
	defer m.UnPatch()

	_, err := fetchRowCountsConcurrently(context.Background(), "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, ExternalSpecContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get file info")
}

func TestFetchRowCountsConcurrently_CtxPreCancelled(t *testing.T) {
	// Mock returns nil synchronously; ctx is canceled before launching workers.
	m := mockey.Mock(GetFileInfo).Return(&FileInfo{NumRows: 10}, nil).Build()
	defer m.UnPatch()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := fetchRowCountsConcurrently(ctx, "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, ExternalSpecContext{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestFetchRowCountsConcurrently_CtxCancelledDuringRun verifies the
// post-AwaitAll ctx.Err() guard: workers ignore ctx and return success, but
// the ctx is canceled mid-run — the function must still return ctx.Err()
// rather than a half-filled rowCounts slice.
func TestFetchRowCountsConcurrently_CtxCancelledDuringRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mockey.Mock(GetFileInfo).To(func(_ string, _ string, _ *indexpb.StorageConfig, _ ExternalSpecContext) (*FileInfo, error) {
		cancel()
		return &FileInfo{NumRows: 10}, nil
	}).Build()
	defer m.UnPatch()

	_, err := fetchRowCountsConcurrently(ctx, "parquet",
		[]FileInfo{{FilePath: "x", NumRows: 0}}, &indexpb.StorageConfig{}, ExternalSpecContext{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
