// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build dynamic

package packed

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSampleExternalFieldSizesMissingMappedColumnIsInputError(t *testing.T) {
	rootPath := t.TempDir()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}

	physicalSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "actual_column",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: false,
		Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"100"}),
	}}, nil)
	writer, err := NewFFIPackedWriter(
		"files/external_sample_source/1",
		physicalSchema,
		[]storagecommon.ColumnGroup{{Columns: []int{0}, GroupID: storagecommon.DefaultShortColumnGroupID}},
		storageConfig,
		nil,
	)
	require.NoError(t, err)
	defer writer.Destroy()

	builder := array.NewRecordBuilder(memory.DefaultAllocator, physicalSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.WriteRecordBatch(record))

	output, err := writer.Close()
	require.NoError(t, err)
	defer output.Destroy()
	sourceManifest, err := CommitManifestUpdates(
		"files/external_sample_source/1",
		ManifestEarliest,
		storageConfig,
		&ManifestUpdates{NewFiles: output},
	)
	require.NoError(t, err)
	fragments, err := ReadFragmentsFromManifest(sourceManifest, storageConfig, []string{"actual_column"})
	require.NoError(t, err)
	require.NotEmpty(t, fragments)
	collectionSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID:       100,
		Name:          "value",
		DataType:      schemapb.DataType_Int64,
		ExternalField: "actual_column",
	}}}
	fieldSizes, err := SampleExternalFieldSizes(
		sourceManifest,
		1,
		1,
		"",
		"",
		collectionSchema,
		storageConfig,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"actual_column": 8}, fieldSizes)

	externalManifestBase := "files/external_sample_manifest/1"
	for i := range fragments {
		fragments[i].FilePath, err = filepath.Rel(filepath.Join(externalManifestBase, "_data"), fragments[i].FilePath)
		require.NoError(t, err)
	}

	// External manifests use the requested mapping as their logical column list.
	// Keep the same physical parquet file but declare a column that it does not
	// contain, reproducing a stale/incorrect external_field mapping.
	externalManifest, err := CreateManifestForSegment(
		externalManifestBase,
		[]string{"missing_column"},
		"parquet",
		fragments,
		storageConfig,
	)
	require.NoError(t, err)

	collectionSchema.Fields[0].ExternalField = "missing_column"
	_, err = SampleExternalFieldSizes(
		externalManifest,
		1,
		1,
		"",
		"",
		collectionSchema,
		storageConfig,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Column 'missing_column' not found in schema")
	require.Equal(t, merr.InputError, merr.GetErrorType(err))
}

func TestSampleExternalFieldSizesCorruptFileRemainsSystemError(t *testing.T) {
	rootPath := t.TempDir()
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}

	corruptFile := "files/external_corrupt_source/data.parquet"
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(rootPath, corruptFile)), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rootPath, corruptFile), []byte("not parquet"), 0o600))

	externalManifestBase := "files/external_corrupt_manifest/1"
	relativeFile, err := filepath.Rel(filepath.Join(externalManifestBase, "_data"), corruptFile)
	require.NoError(t, err)
	externalManifest, err := CreateManifestForSegment(
		externalManifestBase,
		[]string{"missing_column"},
		"parquet",
		[]Fragment{{FilePath: relativeFile, StartRow: 0, EndRow: 1, RowCount: 1}},
		storageConfig,
	)
	require.NoError(t, err)

	collectionSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID:       100,
		Name:          "value",
		DataType:      schemapb.DataType_Int64,
		ExternalField: "missing_column",
	}}}
	_, err = SampleExternalFieldSizes(
		externalManifest,
		1,
		1,
		"",
		"",
		collectionSchema,
		storageConfig,
	)
	require.Error(t, err)
	require.NotEqual(t, merr.InputError, merr.GetErrorType(err))
}
