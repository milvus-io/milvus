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
	"math"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
)

const (
	fixedSizeListPKFieldID   = int64(100)
	fixedSizeListDataFieldID = int64(101)
)

type fixedSizeInt32Row struct {
	valid  bool
	values []*int32
}

type fixedSizeStringRow struct {
	valid  bool
	values []*string
}

type fixedSizeFloat32Row struct {
	valid  bool
	values []*float32
}

func TestImportFixedSizeList_Int32Array(t *testing.T) {
	schema := fixedSizeListArraySchema(schemapb.DataType_Int32, 3, false)
	rows := []fixedSizeInt32Row{
		{valid: true, values: int32Ptrs(1, 2, 3)},
		{valid: true, values: int32Ptrs(4, 5, 6)},
	}

	insertData := readFixedSizeListParquet(t, schema, fixedSizeInt32ListArray(t, 3, rows))

	require.Equal(t, 2, insertData.Data[fixedSizeListDataFieldID].RowNum())
	require.EqualValues(t, []int32{1, 2, 3}, insertData.Data[fixedSizeListDataFieldID].GetRow(0).(*schemapb.ScalarField).GetIntData().GetData())
	require.EqualValues(t, []int32{4, 5, 6}, insertData.Data[fixedSizeListDataFieldID].GetRow(1).(*schemapb.ScalarField).GetIntData().GetData())
}

func TestImportFixedSizeList_StringArray(t *testing.T) {
	schema := fixedSizeListArraySchema(schemapb.DataType_VarChar, 3, false)
	rows := []fixedSizeStringRow{
		{valid: true, values: stringPtrs("red", "green", "blue")},
		{valid: true, values: stringPtrs("cyan", "magenta", "yellow")},
	}

	insertData := readFixedSizeListParquet(t, schema, fixedSizeStringListArray(t, 3, rows))

	require.Equal(t, 2, insertData.Data[fixedSizeListDataFieldID].RowNum())
	require.EqualValues(t, []string{"red", "green", "blue"}, insertData.Data[fixedSizeListDataFieldID].GetRow(0).(*schemapb.ScalarField).GetStringData().GetData())
	require.EqualValues(t, []string{"cyan", "magenta", "yellow"}, insertData.Data[fixedSizeListDataFieldID].GetRow(1).(*schemapb.ScalarField).GetStringData().GetData())
}

func TestImportFixedSizeList_FloatVector(t *testing.T) {
	schema := fixedSizeListFloatVectorSchema(4, false)
	rows := []fixedSizeFloat32Row{
		{valid: true, values: float32Ptrs(1, 2, 3, 4)},
		{valid: true, values: float32Ptrs(5, 6, 7, 8)},
	}

	insertData := readFixedSizeListParquet(t, schema, fixedSizeFloat32ListArray(t, 4, rows))

	require.Equal(t, 2, insertData.Data[fixedSizeListDataFieldID].RowNum())
	require.EqualValues(t, []float32{1, 2, 3, 4}, insertData.Data[fixedSizeListDataFieldID].GetRow(0))
	require.EqualValues(t, []float32{5, 6, 7, 8}, insertData.Data[fixedSizeListDataFieldID].GetRow(1))
}

func TestImportFixedSizeList_FloatVectorRejectsNonFiniteValues(t *testing.T) {
	tests := []struct {
		name  string
		value float32
	}{
		{name: "nan", value: float32(math.NaN())},
		{name: "positive infinity", value: float32(math.Inf(1))},
		{name: "negative infinity", value: float32(math.Inf(-1))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := fixedSizeListFloatVectorSchema(4, false)
			rows := []fixedSizeFloat32Row{
				{valid: true, values: float32Ptrs(1, 2, tt.value, 4)},
			}

			_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeFloat32ListArray(t, 4, rows))
			require.Error(t, err)
			require.Contains(t, err.Error(), "not a number or infinity")
		})
	}
}

func TestImportFixedSizeList_NullableFloatVector(t *testing.T) {
	schema := fixedSizeListFloatVectorSchema(4, true)
	rows := []fixedSizeFloat32Row{
		{valid: true, values: float32Ptrs(1, 2, 3, 4)},
		{valid: true, values: float32Ptrs(5, 6, 7, 8)},
		{valid: false},
	}

	insertData := readFixedSizeListParquet(t, schema, fixedSizeFloat32ListArray(t, 4, rows))

	require.Equal(t, 3, insertData.Data[fixedSizeListDataFieldID].RowNum())
	require.EqualValues(t, []float32{1, 2, 3, 4}, insertData.Data[fixedSizeListDataFieldID].GetRow(0))
	require.EqualValues(t, []float32{5, 6, 7, 8}, insertData.Data[fixedSizeListDataFieldID].GetRow(1))
	require.Nil(t, insertData.Data[fixedSizeListDataFieldID].GetRow(2))
}

func TestImportFixedSizeList_ArrayCapacityExceeded(t *testing.T) {
	schema := fixedSizeListArraySchema(schemapb.DataType_Int32, 2, false)
	rows := []fixedSizeInt32Row{{valid: true, values: int32Ptrs(1, 2, 3)}}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeInt32ListArray(t, 3, rows))
	require.Error(t, err)
	require.Contains(t, strings.ReplaceAll(err.Error(), "_", " "), "exceeds max capacity")
}

func TestImportFixedSizeList_VectorDimMismatch(t *testing.T) {
	schema := fixedSizeListFloatVectorSchema(4, false)
	rows := []fixedSizeFloat32Row{{valid: true, values: float32Ptrs(1, 2, 3)}}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeFloat32ListArray(t, 3, rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "length of vector is not aligned")
}

func TestImportFixedSizeList_NullElementRejected(t *testing.T) {
	schema := fixedSizeListArraySchema(schemapb.DataType_Int32, 3, false)
	rows := []fixedSizeInt32Row{{valid: true, values: []*int32{int32Ptr(1), nil, int32Ptr(3)}}}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeInt32ListArrayWithNullableElements(t, 3, rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "array element is not allowed to be null")
}

func TestImportFixedSizeList_NullableArrayNullElementRejected(t *testing.T) {
	schema := fixedSizeListArraySchema(schemapb.DataType_Int32, 3, true)
	rows := []fixedSizeInt32Row{{valid: true, values: []*int32{int32Ptr(1), nil, int32Ptr(3)}}}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeInt32ListArrayWithNullableElements(t, 3, rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "array element is not allowed to be null")
}

func TestImportFixedSizeList_VectorNullElementRejected(t *testing.T) {
	schema := fixedSizeListFloatVectorSchema(4, true)
	rows := []fixedSizeFloat32Row{{valid: true, values: []*float32{float32Ptr(1), nil, float32Ptr(3), float32Ptr(4)}}}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeFloat32ListArrayWithNullableElements(t, 4, rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "array element is not allowed to be null")
}

func TestImportFixedSizeList_WrongElementTypeRejected(t *testing.T) {
	schema := fixedSizeListArraySchema(schemapb.DataType_Int32, 3, false)
	rows := []fixedSizeStringRow{{valid: true, values: stringPtrs("1", "2", "3")}}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeStringListArray(t, 3, rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "field 'fsl_array' type mis-match")
	require.Contains(t, err.Error(), "expect arrow type 'list<item: int32")
	require.Contains(t, err.Error(), "get arrow data type 'fixed_size_list<item: utf8")
}

func TestImportFixedSizeList_NonNullableNullRowRejected(t *testing.T) {
	schema := fixedSizeListFloatVectorSchema(4, false)
	rows := []fixedSizeFloat32Row{
		{valid: true, values: float32Ptrs(1, 2, 3, 4)},
		{valid: false},
	}

	_, err := tryReadFixedSizeListParquet(t, schema, fixedSizeFloat32ListArray(t, 4, rows))
	require.Error(t, err)
	require.Contains(t, err.Error(), "the field 'fsl_vector' is not nullable but the file contains null value")
	require.NotContains(t, err.Error(), "array element is not allowed to be null")
}

func TestCanBulkCopyUint8ListValues(t *testing.T) {
	field := fixedSizeListFloat16VectorSchema(4, false).Fields[1]

	list := uint8ListArray(t, [][]byte{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
	}, false)
	defer list.Release()
	listReader, err := newListLikeArray(list, field)
	require.NoError(t, err)
	uint8Reader := listReader.ListValues().(*array.Uint8)
	require.True(t, canBulkCopyUint8ListValues(listReader, uint8Reader))

	listWithNullElement := uint8ListArray(t, [][]byte{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
	}, true)
	defer listWithNullElement.Release()
	listReader, err = newListLikeArray(listWithNullElement, field)
	require.NoError(t, err)
	uint8Reader = listReader.ListValues().(*array.Uint8)
	require.False(t, canBulkCopyUint8ListValues(listReader, uint8Reader))

	fixedSizeList := fixedSizeUint8ListArray(t, 4, [][]byte{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
	})
	defer fixedSizeList.Release()
	listReader, err = newListLikeArray(fixedSizeList, field)
	require.NoError(t, err)
	uint8Reader = listReader.ListValues().(*array.Uint8)
	require.False(t, canBulkCopyUint8ListValues(listReader, uint8Reader))
}

func readFixedSizeListParquet(t *testing.T, schema *schemapb.CollectionSchema, fixedSizeList arrow.Array) *storage.InsertData {
	insertData, err := tryReadFixedSizeListParquet(t, schema, fixedSizeList)
	require.NoError(t, err)
	return insertData
}

func tryReadFixedSizeListParquet(t *testing.T, schema *schemapb.CollectionSchema, fixedSizeList arrow.Array) (*storage.InsertData, error) {
	t.Helper()

	filePath := writeFixedSizeListParquet(t, schema, fixedSizeList)
	ctx := context.Background()
	factory := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
	cm, err := factory.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)

	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return reader.Read()
}

func writeFixedSizeListParquet(t *testing.T, schema *schemapb.CollectionSchema, fixedSizeList arrow.Array) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "fixed_size_list_*.parquet")
	require.NoError(t, err)
	defer file.Close()

	pk := int64Array(t, fixedSizeList.Len())
	defer pk.Release()
	defer fixedSizeList.Release()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{
			Name:     schema.Fields[1].GetName(),
			Type:     fixedSizeList.DataType(),
			Nullable: true,
		},
	}, nil)
	writer, err := pqarrow.NewFileWriter(
		arrowSchema,
		file,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(fixedSizeList.Len()))),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()),
	)
	require.NoError(t, err)

	record := array.NewRecord(arrowSchema, []arrow.Array{pk, fixedSizeList}, int64(fixedSizeList.Len()))
	defer record.Release()
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	assertFixedSizeListParquetSchema(t, file.Name(), schema.Fields[1].GetName())

	return file.Name()
}

func assertFixedSizeListParquetSchema(t *testing.T, filePath string, columnName string) {
	t.Helper()

	rf, err := os.Open(filePath)
	require.NoError(t, err)
	defer rf.Close()

	parquetReader, err := file.NewParquetReader(rf)
	require.NoError(t, err)
	defer parquetReader.Close()

	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)

	readSchema, err := arrowReader.Schema()
	require.NoError(t, err)
	fields, ok := readSchema.FieldsByName(columnName)
	require.True(t, ok)
	require.Len(t, fields, 1)
	require.Equal(t, arrow.FIXED_SIZE_LIST, fields[0].Type.ID())
}

func fixedSizeListArraySchema(elementType schemapb.DataType, maxCapacity int, nullable bool) *schemapb.CollectionSchema {
	typeParams := []*commonpb.KeyValuePair{
		{Key: common.MaxCapacityKey, Value: fmt.Sprintf("%d", maxCapacity)},
	}
	if elementType == schemapb.DataType_VarChar || elementType == schemapb.DataType_String {
		typeParams = append(typeParams, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "64"})
	}

	return fixedSizeListSchema(&schemapb.FieldSchema{
		FieldID:     fixedSizeListDataFieldID,
		Name:        "fsl_array",
		DataType:    schemapb.DataType_Array,
		ElementType: elementType,
		TypeParams:  typeParams,
		Nullable:    nullable,
	})
}

func fixedSizeListFloatVectorSchema(dim int, nullable bool) *schemapb.CollectionSchema {
	return fixedSizeListSchema(&schemapb.FieldSchema{
		FieldID:  fixedSizeListDataFieldID,
		Name:     "fsl_vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)},
		},
		Nullable: nullable,
	})
}

func fixedSizeListFloat16VectorSchema(dim int, nullable bool) *schemapb.CollectionSchema {
	return fixedSizeListSchema(&schemapb.FieldSchema{
		FieldID:  fixedSizeListDataFieldID,
		Name:     "fsl_vector",
		DataType: schemapb.DataType_Float16Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)},
		},
		Nullable: nullable,
	})
}

func fixedSizeListSchema(field *schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      fixedSizeListPKFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			field,
		},
	}
}

func int64Array(t *testing.T, rows int) arrow.Array {
	t.Helper()

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	for i := 0; i < rows; i++ {
		builder.Append(int64(i + 1))
	}
	return builder.NewArray()
}

func uint8ListArray(t *testing.T, rows [][]byte, withNullElement bool) arrow.Array {
	t.Helper()

	builder := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Uint8)
	defer builder.Release()
	valueBuilder := builder.ValueBuilder().(*array.Uint8Builder)
	for _, row := range rows {
		builder.Append(true)
		for i, value := range row {
			if withNullElement && i == 1 {
				valueBuilder.AppendNull()
				continue
			}
			valueBuilder.Append(value)
		}
	}
	return builder.NewArray()
}

func fixedSizeUint8ListArray(t *testing.T, listSize int32, rows [][]byte) arrow.Array {
	t.Helper()

	builder := array.NewFixedSizeListBuilderWithField(memory.DefaultAllocator, listSize, arrow.Field{
		Name:     "item",
		Type:     arrow.PrimitiveTypes.Uint8,
		Nullable: false,
	})
	defer builder.Release()
	valueBuilder := builder.ValueBuilder().(*array.Uint8Builder)
	validRows := make([]bool, len(rows))
	for i := range validRows {
		validRows[i] = true
	}
	builder.AppendValues(validRows)
	for _, row := range rows {
		require.Len(t, row, int(listSize))
		for _, value := range row {
			valueBuilder.Append(value)
		}
	}
	return builder.NewArray()
}

func fixedSizeInt32ListArray(t *testing.T, listSize int32, rows []fixedSizeInt32Row) arrow.Array {
	t.Helper()

	return fixedSizeInt32ListArrayWithElementNullability(t, listSize, rows, false)
}

func fixedSizeInt32ListArrayWithNullableElements(t *testing.T, listSize int32, rows []fixedSizeInt32Row) arrow.Array {
	t.Helper()

	return fixedSizeInt32ListArrayWithElementNullability(t, listSize, rows, true)
}

func fixedSizeInt32ListArrayWithElementNullability(t *testing.T, listSize int32, rows []fixedSizeInt32Row, nullableElement bool) arrow.Array {
	t.Helper()

	builder := array.NewFixedSizeListBuilderWithField(memory.DefaultAllocator, listSize, arrow.Field{
		Name:     "item",
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: nullableElement,
	})
	defer builder.Release()
	valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
	validRows := make([]bool, 0, len(rows))
	for _, row := range rows {
		validRows = append(validRows, row.valid)
	}
	builder.AppendValues(validRows)
	for _, row := range rows {
		if row.valid {
			require.Len(t, row.values, int(listSize))
		}
		for _, value := range row.values {
			if value == nil {
				require.True(t, nullableElement)
				valueBuilder.AppendNull()
				continue
			}
			valueBuilder.Append(*value)
		}
		for i := len(row.values); i < int(listSize); i++ {
			valueBuilder.Append(0)
		}
	}
	return builder.NewArray()
}

func fixedSizeStringListArray(t *testing.T, listSize int32, rows []fixedSizeStringRow) arrow.Array {
	t.Helper()

	builder := array.NewFixedSizeListBuilderWithField(memory.DefaultAllocator, listSize, arrow.Field{
		Name:     "item",
		Type:     arrow.BinaryTypes.String,
		Nullable: false,
	})
	defer builder.Release()
	valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
	validRows := make([]bool, 0, len(rows))
	for _, row := range rows {
		validRows = append(validRows, row.valid)
	}
	builder.AppendValues(validRows)
	for _, row := range rows {
		if row.valid {
			require.Len(t, row.values, int(listSize))
		}
		for _, value := range row.values {
			if value == nil {
				require.Fail(t, "unexpected null string element")
				continue
			}
			valueBuilder.Append(*value)
		}
		for i := len(row.values); i < int(listSize); i++ {
			valueBuilder.Append("")
		}
	}
	return builder.NewArray()
}

func fixedSizeFloat32ListArray(t *testing.T, listSize int32, rows []fixedSizeFloat32Row) arrow.Array {
	t.Helper()

	return fixedSizeFloat32ListArrayWithElementNullability(t, listSize, rows, false)
}

func fixedSizeFloat32ListArrayWithNullableElements(t *testing.T, listSize int32, rows []fixedSizeFloat32Row) arrow.Array {
	t.Helper()

	return fixedSizeFloat32ListArrayWithElementNullability(t, listSize, rows, true)
}

func fixedSizeFloat32ListArrayWithElementNullability(t *testing.T, listSize int32, rows []fixedSizeFloat32Row, nullableElement bool) arrow.Array {
	t.Helper()

	builder := array.NewFixedSizeListBuilderWithField(memory.DefaultAllocator, listSize, arrow.Field{
		Name:     "item",
		Type:     arrow.PrimitiveTypes.Float32,
		Nullable: nullableElement,
	})
	defer builder.Release()
	valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
	validRows := make([]bool, 0, len(rows))
	for _, row := range rows {
		validRows = append(validRows, row.valid)
	}
	builder.AppendValues(validRows)
	for _, row := range rows {
		if row.valid {
			require.Len(t, row.values, int(listSize))
		}
		for _, value := range row.values {
			if value == nil {
				require.True(t, nullableElement)
				valueBuilder.AppendNull()
				continue
			}
			valueBuilder.Append(*value)
		}
		for i := len(row.values); i < int(listSize); i++ {
			valueBuilder.Append(0)
		}
	}
	return builder.NewArray()
}

func int32Ptrs(values ...int32) []*int32 {
	ptrs := make([]*int32, 0, len(values))
	for _, value := range values {
		ptrs = append(ptrs, int32Ptr(value))
	}
	return ptrs
}

func int32Ptr(value int32) *int32 {
	return &value
}

func stringPtrs(values ...string) []*string {
	ptrs := make([]*string, 0, len(values))
	for _, value := range values {
		ptrs = append(ptrs, stringPtr(value))
	}
	return ptrs
}

func stringPtr(value string) *string {
	return &value
}

func float32Ptrs(values ...float32) []*float32 {
	ptrs := make([]*float32, 0, len(values))
	for _, value := range values {
		ptrs = append(ptrs, float32Ptr(value))
	}
	return ptrs
}

func float32Ptr(value float32) *float32 {
	return &value
}
