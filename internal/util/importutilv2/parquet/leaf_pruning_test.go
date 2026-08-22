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

package parquet

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
)

// leafPruningTestSchema builds a collection schema whose struct array field has
// three sub-fields, one of them an ArrayOfVector. Callers should pass a dim large
// enough that the vector sub-field dominates the on-disk size, otherwise read
// amplification is not measurable against parquet metadata overhead.
//
// Every field is non-nullable. Callers that need null rows must set Nullable on
// the fields they care about before writing.
func leafPruningTestSchema(dim string, maxCapacity string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_leaf_pruning",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "varchar_field",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					// testutil.CreateInsertData fills VarChar via
					// testutils.GenerateStringArray, which builds 5-10 word
					// sentences up to roughly 100 characters. A smaller
					// max_length makes reader.Read fail CheckVarcharLength
					// before any assertion in these tests is reached.
					{Key: common.MaxLengthKey, Value: "128"},
				},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 200,
				Name:    "struct_array",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     201,
						Name:        "struct_array[int_array]",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int32,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.MaxCapacityKey, Value: maxCapacity},
						},
					},
					{
						FieldID:     202,
						Name:        "struct_array[float_array]",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Float,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.MaxCapacityKey, Value: maxCapacity},
						},
					},
					{
						FieldID:     203,
						Name:        "struct_array[vector_array]",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: dim},
							{Key: common.MaxCapacityKey, Value: maxCapacity},
						},
					},
				},
			},
		},
	}
}

// writeLeafPruningParquet writes a parquet file for the given schema and returns
// its path and on-disk size. The caller is responsible for removing the file.
//
// nullPercent is forwarded to testutil.CreateInsertData, which only honors it
// for fields whose Nullable flag is set. A schema straight from
// leafPruningTestSchema has no nullable fields, so nullPercent has no effect
// unless the caller marks fields nullable first.
func writeLeafPruningParquet(t *testing.T, schema *schemapb.CollectionSchema, numRows int, nullPercent int) (string, int64) {
	t.Helper()

	filePath := fmt.Sprintf("/tmp/test_leaf_pruning_%d.parquet", rand.Int())
	f, err := os.Create(filePath)
	require.NoError(t, err)
	// writeParquet closes the sink itself: pqarrow.FileWriter.Close calls
	// file.Writer.Close, which closes the io.Writer it was handed. This deferred
	// close is only a safety net for the path where a require below aborts first,
	// so its error is deliberately ignored.
	defer func() { _ = f.Close() }()

	_, err = writeParquet(f, schema, numRows, nullPercent)
	require.NoError(t, err)

	info, err := os.Stat(filePath)
	require.NoError(t, err)
	return filePath, info.Size()
}

// findArrowFieldIndex returns the index of the named top-level field in an arrow
// schema, failing the test if it is absent.
func findArrowFieldIndex(t *testing.T, schema *arrow.Schema, name string) int {
	t.Helper()
	for i, f := range schema.Fields() {
		if f.Name == name {
			return i
		}
	}
	require.FailNowf(t, "field not found", "no top-level arrow field named %q", name)
	return -1
}

func TestCollectSubFieldLeaves(t *testing.T) {
	schema := leafPruningTestSchema("8", "4")
	filePath, _ := writeLeafPruningParquet(t, schema, 20, 0)
	defer os.Remove(filePath)

	osFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer osFile.Close()

	pqReader, err := file.NewParquetReader(osFile)
	require.NoError(t, err)
	defer pqReader.Close()

	fileReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 64}, memory.DefaultAllocator)
	require.NoError(t, err)

	// Locate the struct_array column among the top-level arrow fields.
	arrowSchema, err := fileReader.Schema()
	require.NoError(t, err)
	columnIndex := findArrowFieldIndex(t, arrowSchema, "struct_array")

	subFieldNames := []string{"int_array", "float_array", "vector_array"}
	union := make(map[int]bool)

	for fieldIndex, name := range subFieldNames {
		leaves, err := collectSubFieldLeaves(fileReader.Manifest, columnIndex, fieldIndex)
		require.NoError(t, err, "sub-field %s", name)
		require.NotEmpty(t, leaves, "sub-field %s resolved to zero leaves", name)

		for colIdx := range leaves {
			// Every resolved leaf must actually live under this sub-field.
			// This is what catches terminating recursion on IsLeaf(): a group
			// node carries a zero-value ColIndex of 0, which would resolve to
			// the first leaf in the file ("id") instead of the sub-field.
			path := pqReader.MetaData().Schema.Column(colIdx).Path()
			assert.Contains(t, path, name,
				"sub-field %s resolved to leaf %d whose path is %q", name, colIdx, path)

			assert.False(t, union[colIdx],
				"leaf %d claimed by more than one sub-field", colIdx)
			union[colIdx] = true
		}
	}

	assert.Len(t, union, len(subFieldNames),
		"expected one leaf per sub-field for this schema, got %v", union)
}

func TestCollectSubFieldLeavesRejectsBadIndex(t *testing.T) {
	schema := leafPruningTestSchema("8", "4")
	filePath, _ := writeLeafPruningParquet(t, schema, 20, 0)
	defer os.Remove(filePath)

	osFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer osFile.Close()

	pqReader, err := file.NewParquetReader(osFile)
	require.NoError(t, err)
	defer pqReader.Close()

	fileReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 64}, memory.DefaultAllocator)
	require.NoError(t, err)

	_, err = collectSubFieldLeaves(fileReader.Manifest, -1, 0)
	assert.Error(t, err)

	_, err = collectSubFieldLeaves(fileReader.Manifest, 9999, 0)
	assert.Error(t, err)

	_, err = collectSubFieldLeaves(nil, 0, 0)
	assert.Error(t, err)

	arrowSchema, err := fileReader.Schema()
	require.NoError(t, err)
	columnIndex := findArrowFieldIndex(t, arrowSchema, "struct_array")

	_, err = collectSubFieldLeaves(fileReader.Manifest, columnIndex, 9999)
	assert.Error(t, err, "out-of-range fieldIndex must be rejected")
}

// countingChunkManager wraps a ChunkManager and counts every byte pulled through
// the streaming Reader() path, which is the path the parquet import reader uses.
type countingChunkManager struct {
	storage.ChunkManager
	readBytes atomic.Int64
}

func (c *countingChunkManager) Reader(ctx context.Context, filePath string) (storage.FileReader, error) {
	r, err := c.ChunkManager.Reader(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return &countingFileReader{FileReader: r, counter: &c.readBytes}, nil
}

type countingFileReader struct {
	storage.FileReader
	counter *atomic.Int64
}

func (r *countingFileReader) Read(p []byte) (int, error) {
	n, err := r.FileReader.Read(p)
	r.counter.Add(int64(n))
	return n, err
}

func (r *countingFileReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.FileReader.ReadAt(p, off)
	r.counter.Add(int64(n))
	return n, err
}

// TestStructArrayReadAmplification guards against the struct array column being
// decoded once per sub-field. Before leaf pruning this reads roughly 3x the file
// (one full pass per sub-field); after pruning it reads it about once.
func TestStructArrayReadAmplification(t *testing.T) {
	ctx := context.Background()

	// dim 64 x capacity 16 makes the vector sub-field dominate the file, so the
	// amplification is far larger than parquet footer/page-header overhead.
	schema := leafPruningTestSchema("64", "16")
	filePath, fileSize := writeLeafPruningParquet(t, schema, 1000, 0)
	defer os.Remove(filePath)

	factory := storage.NewChunkManagerFactory("local", objectstorage.RootPath("/tmp"))
	baseCM, err := factory.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)

	cm := &countingChunkManager{ChunkManager: baseCM}

	reader, err := NewReader(ctx, cm, schema, filePath, 16*1024*1024)
	require.NoError(t, err)
	defer reader.Close()

	totalRows := 0
	for {
		data, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		totalRows += data.GetRowNum()
	}
	require.Equal(t, 1000, totalRows)

	readBytes := cm.readBytes.Load()
	t.Logf("file size = %d bytes, bytes read = %d (%.2fx)",
		fileSize, readBytes, float64(readBytes)/float64(fileSize))

	require.Positive(t, readBytes, "counting chunk manager saw no reads")
	assert.Less(t, readBytes, 2*fileSize,
		"struct array column is being read more than once: read %d bytes for a %d byte file",
		readBytes, fileSize)
}
