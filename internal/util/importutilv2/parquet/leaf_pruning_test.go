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
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
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

// TestStructArrayLeafPruningWithNulls exercises the null-bearing branches of the
// struct array readers under leaf pruning. After pruning, the struct validity
// bitmap is derived from the single surviving leaf's definition levels rather
// than from all leaves, so null rows need explicit coverage.
//
// The fixture is built directly with arrow builders instead of going through
// writeLeafPruningParquet (testutil.CreateInsertData + testutil.BuildArrayData).
// BuildArrayData's list<struct> builder (internal/util/testutil/test_util.go:1113
// and :1169) does an unchecked type assertion on the value returned by
// ArrayFieldData.GetRow, which is an untyped nil for an invalid row
// (internal/storage/insert_data.go:746-751) — so any nullable struct sub-field
// with an actual null row panics there, before this package's read path is ever
// reached. testutil.CreateInsertData also assigns each nullable field its own
// independent random validity, which cannot be asserted against precisely.
// Building the fixture by hand fixes the null positions exactly so the readback
// can be checked exactly against them.
func TestStructArrayLeafPruningWithNulls(t *testing.T) {
	ctx := context.Background()

	schema := leafPruningTestSchema("4", "8")
	// Sub-fields must be nullable, or readArrayOfVectorField/readArrayField
	// reject a null row with WrapNullRowErr instead of accepting it.
	for _, sub := range schema.StructArrayFields[0].Fields {
		sub.Nullable = true
	}

	// Derive the physical arrow layout from the schema instead of hand-rolling
	// it, so it always matches what the reader expects.
	pqSchema, err := ConvertToArrowSchemaForUT(schema, false)
	require.NoError(t, err)

	structColIdx := findArrowFieldIndex(t, pqSchema, "struct_array")

	listType, ok := pqSchema.Field(structColIdx).Type.(*arrow.ListType)
	require.True(t, ok, "struct_array column is not a list type, got %s", pqSchema.Field(structColIdx).Type)
	structType, ok := listType.Elem().(*arrow.StructType)
	require.True(t, ok, "struct_array element type is not a struct, got %s", listType.Elem())

	// ConvertToArrowSchemaForUT hardcodes Nullable: false on the outer
	// struct_array list field regardless of the schema's struct-level
	// Nullable flag. In parquet that makes the whole struct_array column a
	// required group, which can only ever be present-with-N-elements — it
	// cannot represent "this document's struct_array value is null" at all,
	// only "empty". Writing a document-row-level null against that schema
	// would silently collapse to an empty (but valid) list instead, which
	// would make this fixture assert something write-time coercion produced
	// rather than what leaf pruning does on read. Build a write-time-only
	// copy of the schema with that one field marked nullable so the null
	// survives the round trip and this test actually exercises the read path.
	writeFields := append([]arrow.Field(nil), pqSchema.Fields()...)
	writeFields[structColIdx].Nullable = true
	writeSchema := arrow.NewSchema(writeFields, nil)

	mem := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(mem)
	idBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	idArray := idBuilder.NewArray()
	idBuilder.Release()
	defer idArray.Release()

	varcharBuilder := array.NewStringBuilder(mem)
	varcharBuilder.AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	varcharArray := varcharBuilder.NewArray()
	varcharBuilder.Release()
	defer varcharArray.Release()

	listBuilder := array.NewListBuilder(mem, structType)
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	intBuilder := structBuilder.FieldBuilder(0).(*array.Int32Builder)
	floatBuilder := structBuilder.FieldBuilder(1).(*array.Float32Builder)
	vectorListBuilder := structBuilder.FieldBuilder(2).(*array.ListBuilder)
	vectorValueBuilder := vectorListBuilder.ValueBuilder().(*array.Float32Builder)

	appendElement := func(intVal int32, floatVal float32, vec []float32) {
		intBuilder.Append(intVal)
		floatBuilder.Append(floatVal)
		vectorListBuilder.Append(true)
		vectorValueBuilder.AppendValues(vec, nil)
		structBuilder.Append(true)
	}

	// Precisely specified null layout: rows 1 and 3 are entirely null (the
	// whole struct_array value is absent for that document row); the other
	// rows carry a varying number of struct elements, so row alignment across
	// the surviving leaf gets exercised too, not just the null flag.
	wantValid := []bool{true, false, true, false, true}
	wantVectors := map[int][][]float32{
		0: {{1, 2, 3, 4}, {5, 6, 7, 8}},
		2: {{9, 10, 11, 12}},
		4: {{13, 14, 15, 16}, {17, 18, 19, 20}, {21, 22, 23, 24}},
	}

	listBuilder.Append(true)
	appendElement(1, 1, wantVectors[0][0])
	appendElement(2, 2, wantVectors[0][1])

	listBuilder.Append(false)

	listBuilder.Append(true)
	appendElement(3, 3, wantVectors[2][0])

	listBuilder.Append(false)

	listBuilder.Append(true)
	appendElement(4, 4, wantVectors[4][0])
	appendElement(5, 5, wantVectors[4][1])
	appendElement(6, 6, wantVectors[4][2])

	structArrayArray := listBuilder.NewArray()
	listBuilder.Release()
	defer structArrayArray.Release()

	record := array.NewRecord(writeSchema, []arrow.Array{idArray, varcharArray, structArrayArray}, 5)
	defer record.Release()

	filePath := fmt.Sprintf("%s/test_struct_array_nulls_%d.parquet", t.TempDir(), rand.Int())
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	require.NoError(t, err)
	// Only fires if a require below aborts before the explicit Close; closing an
	// already-closed file returns an error we do not care about here.
	defer func() { _ = wf.Close() }()
	fw, err := pqarrow.NewFileWriter(writeSchema, wf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(5)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, fw.Write(record))
	require.NoError(t, fw.Close())

	factory := storage.NewChunkManagerFactory("local", objectstorage.RootPath("/tmp"))
	cm, err := factory.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)

	reader, err := NewReader(ctx, cm, schema, filePath, 16*1024*1024)
	require.NoError(t, err)
	defer reader.Close()

	totalRows := 0
	var validData []bool
	var vectorRows []*schemapb.VectorField
	for {
		data, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		// Every field must report the same row count as the rest of the
		// batch; a mismatch means pruning desynchronised the struct.
		for _, fieldID := range []int64{100, 101, 201, 202, 203} {
			fieldData, ok := data.Data[fieldID]
			require.True(t, ok, "field %d missing from batch", fieldID)
			assert.Equal(t, data.GetRowNum(), fieldData.RowNum(),
				"row count mismatch for field %d", fieldID)
		}

		vectorField, ok := data.Data[203].(*storage.VectorArrayFieldData)
		require.True(t, ok, "field 203 is not a VectorArrayFieldData")
		validData = append(validData, vectorField.ValidData...)
		vectorRows = append(vectorRows, vectorField.Data...)

		totalRows += data.GetRowNum()
	}

	require.Equal(t, 5, totalRows)
	t.Logf("readback ValidData for field 203 (vector_array) = %v", validData)

	// Core evidence for this task: the struct validity bitmap read back from
	// the single surviving vector_array leaf, after leaf pruning, must match
	// exactly the row-level nulls written above (rows 1 and 3) — not
	// something leaf pruning silently redistributed or dropped.
	assert.Equal(t, wantValid, validData,
		"struct validity bitmap changed under leaf pruning")

	// Non-null rows must carry the correct vector content, proving pruning
	// selected the right leaf column, not merely one with the right shape.
	for rowIdx, wantRowVectors := range wantVectors {
		require.Less(t, rowIdx, len(vectorRows))
		var want []float32
		for _, v := range wantRowVectors {
			want = append(want, v...)
		}
		assert.Equal(t, want, vectorRows[rowIdx].GetFloatVector().GetData(),
			"vector content mismatch at row %d", rowIdx)
	}
}
