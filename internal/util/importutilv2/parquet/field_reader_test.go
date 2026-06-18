package parquet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func init() {
	paramtable.Init()
}

func TestInvalidUTF8(t *testing.T) {
	const (
		fieldID = int64(100)
		numRows = 100
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:    fieldID,
				Name:       "str",
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
			},
		},
	}

	data := make([]string, numRows)
	for i := 0; i < numRows-1; i++ {
		data[i] = randomString(16)
	}
	data[numRows-1] = "\xc3\x28" // invalid utf-8

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(t, err)

	pqSchema, err := ConvertToArrowSchemaForUT(schema, false)
	assert.NoError(t, err)
	fw, err := pqarrow.NewFileWriter(pqSchema, wf,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(numRows)), pqarrow.DefaultWriterProps())
	assert.NoError(t, err)

	insertData, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	err = insertData.Data[fieldID].AppendDataRows(data)
	assert.NoError(t, err)

	columns, err := testutil.BuildArrayData(schema, insertData, false)
	assert.NoError(t, err)

	recordBatch := array.NewRecord(pqSchema, columns, numRows)
	err = fw.Write(recordBatch)
	assert.NoError(t, err)
	fw.Close()

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	assert.NoError(t, err)

	_, err = reader.Read()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "contains invalid UTF-8 data"))
}

// TestParseSparseFloatRowVector tests the parseSparseFloatRowVector function
func TestParseSparseFloatRowVector(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantMaxIdx uint32
		wantErrMsg string
	}{
		{
			name:       "empty sparse vector",
			input:      "{}",
			wantMaxIdx: 0,
		},
		{
			name:       "key-value format",
			input:      "{\"275574541\":1.5383775}",
			wantMaxIdx: 275574542, // max index 275574541 + 1
		},
		{
			name:       "multiple key-value pairs",
			input:      "{\"1\":0.5,\"10\":1.5,\"100\":2.5}",
			wantMaxIdx: 101, // max index 100 + 1
		},
		{
			name:       "invalid format - missing braces",
			input:      "\"275574541\":1.5383775",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "invalid JSON format",
			input:      "{275574541:1.5383775}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "malformed JSON",
			input:      "{\"key\": value}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "non-numeric index",
			input:      "{\"abc\":1.5}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "non-numeric value",
			input:      "{\"123\":\"abc\"}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
		{
			name:       "negative index",
			input:      "{\"-1\":1.5}",
			wantErrMsg: "Invalid JSON string for SparseFloatVector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowVec, maxIdx, err := parseSparseFloatRowVector(tt.input)

			if tt.wantErrMsg != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantMaxIdx, maxIdx)

			// Verify the rowVec is properly formatted
			if maxIdx > 0 {
				elemCount := len(rowVec) / 8
				assert.Greater(t, elemCount, 0)

				// Check the last index matches our expectation
				lastIdx := typeutil.SparseFloatRowIndexAt(rowVec, elemCount-1)
				assert.Equal(t, tt.wantMaxIdx-1, lastIdx)
			} else {
				assert.Empty(t, rowVec)
			}
		})
	}
}

func TestReadNullableByteVectorBinaryRowsRejectWrongRowWidth(t *testing.T) {
	tests := []struct {
		name      string
		dataType  schemapb.DataType
		dim       string
		firstRow  []byte
		secondRow []byte
	}{
		{
			name:      "binary_vector",
			dataType:  schemapb.DataType_BinaryVector,
			dim:       "16",
			firstRow:  []byte{1},
			secondRow: []byte{2, 3, 4},
		},
		{
			name:      "float16_vector",
			dataType:  schemapb.DataType_Float16Vector,
			dim:       "2",
			firstRow:  []byte{1, 2},
			secondRow: []byte{3, 4, 5, 6, 7, 8},
		},
		{
			name:      "bfloat16_vector",
			dataType:  schemapb.DataType_BFloat16Vector,
			dim:       "2",
			firstRow:  []byte{1, 2},
			secondRow: []byte{3, 4, 5, 6, 7, 8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				pkFieldID  = int64(100)
				vecFieldID = int64(101)
				numRows    = int64(2)
			)
			schema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      pkFieldID,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
					{
						FieldID:  vecFieldID,
						Name:     "vec",
						DataType: tt.dataType,
						Nullable: true,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: tt.dim},
						},
					},
				},
			}

			filePath := fmt.Sprintf("/tmp/test_nullable_byte_vector_binary_width_%s_%d.parquet", tt.name, rand.Int())
			defer os.Remove(filePath)
			wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
			require.NoError(t, err)

			pqSchema := arrow.NewSchema([]arrow.Field{
				{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
				{Name: "vec", Type: arrow.BinaryTypes.Binary, Nullable: true},
			}, nil)
			fw, err := pqarrow.NewFileWriter(pqSchema, wf,
				parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(numRows)),
				pqarrow.DefaultWriterProps())
			require.NoError(t, err)

			pkBuilder := array.NewInt64Builder(memory.DefaultAllocator)
			defer pkBuilder.Release()
			pkBuilder.AppendValues([]int64{1, 2}, nil)
			pkArr := pkBuilder.NewArray()
			defer pkArr.Release()

			vecBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			defer vecBuilder.Release()
			vecBuilder.Append(tt.firstRow)
			vecBuilder.Append(tt.secondRow)
			vecArr := vecBuilder.NewArray()
			defer vecArr.Release()

			recordBatch := array.NewRecord(pqSchema, []arrow.Array{pkArr, vecArr}, numRows)
			defer recordBatch.Release()
			require.NoError(t, fw.Write(recordBatch))
			require.NoError(t, fw.Close())

			ctx := context.Background()
			f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
			cm, err := f.NewPersistentStorageChunkManager(ctx)
			require.NoError(t, err)
			reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
			require.NoError(t, err)
			defer reader.Close()

			_, err = reader.Read()
			require.Error(t, err)
			require.Contains(t, err.Error(), "vector row width mismatch")
		})
	}
}

func TestParseSparseFloatVectorStructs(t *testing.T) {
	mem := memory.NewGoAllocator()

	checkFunc := func(indices arrow.Array, values arrow.Array, expectSucceed bool) ([][]byte, uint32) {
		st := make(map[string]arrow.Array)
		if indices != nil {
			st[sparseVectorIndice] = indices
		}
		if values != nil {
			st[sparseVectorValues] = values
		}

		structs := make([]map[string]arrow.Array, 0)
		structs = append(structs, st)

		byteArr, maxDim, err := parseSparseFloatVectorStructs(structs)
		if expectSucceed {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
		return byteArr, maxDim
	}

	genInt32Arr := func(len int) *array.Int32 {
		builder := array.NewInt32Builder(mem)
		data := make([]int32, 0)
		validData := make([]bool, 0)
		for i := 0; i < len; i++ {
			data = append(data, (int32)(i))
			validData = append(validData, i%2 == 0)
		}

		builder.AppendValues(data, validData)
		return builder.NewInt32Array()
	}

	genFloat32Arr := func(len int) *array.Float32 {
		builder := array.NewFloat32Builder(mem)
		data := make([]float32, 0)
		validData := make([]bool, 0)
		for i := 0; i < len; i++ {
			data = append(data, (float32)(i))
			validData = append(validData, i%2 == 0)
		}
		builder.AppendValues(data, validData)
		return builder.NewFloat32Array()
	}

	genInt32ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Int32Type{})
		builder.Append(true)
		for _, v := range arr {
			builder.ValueBuilder().(*array.Int32Builder).Append((int32)(v))
		}
		return builder.NewListArray()
	}

	genUint32ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Uint32Type{})
		if arr != nil {
			builder.Append(true)
			for _, v := range arr {
				builder.ValueBuilder().(*array.Uint32Builder).Append(v)
			}
		}
		return builder.NewListArray()
	}

	genInt64ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Int64Type{})
		if arr != nil {
			builder.Append(true)
			for _, v := range arr {
				builder.ValueBuilder().(*array.Int64Builder).Append((int64)(v))
			}
		}
		return builder.NewListArray()
	}

	genUint64ArrList := func(arr []uint32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Uint64Type{})
		if arr != nil {
			builder.Append(true)
			for _, v := range arr {
				builder.ValueBuilder().(*array.Uint64Builder).Append((uint64)(v))
			}
		}
		return builder.NewListArray()
	}

	genFloat32ArrList := func(arr []float32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Float32Type{})
		if arr != nil {
			builder.Append(true)
			for _, v := range arr {
				builder.ValueBuilder().(*array.Float32Builder).Append(v)
			}
		}
		return builder.NewListArray()
	}

	genFloat64ArrList := func(arr []float32) *array.List {
		builder := array.NewListBuilder(mem, &arrow.Float64Type{})
		if arr != nil {
			builder.Append(true)
			for _, v := range arr {
				builder.ValueBuilder().(*array.Float64Builder).Append((float64)(v))
			}
		}
		return builder.NewListArray()
	}

	// idices field missed
	checkFunc(nil, genFloat32ArrList([]float32{0.1}), false)

	// values field missed
	checkFunc(genUint32ArrList([]uint32{1, 2}), nil, false)

	// indices is not array.List
	checkFunc(genInt32Arr(2), genFloat32ArrList([]float32{0.1, 0.2}), false)

	// values is not array.List
	checkFunc(genUint32ArrList([]uint32{1, 2}), genFloat32Arr(2), false)

	// indices is not list of int32/uint32/int64/uint64 array
	checkFunc(genFloat32ArrList([]float32{0.1, 0.2, 0.3}), genFloat32ArrList([]float32{0.1, 0.2, 0.3}), false)

	// values is not list of float32/float64 array
	checkFunc(genUint32ArrList([]uint32{1, 2, 3}), genUint32ArrList([]uint32{1, 2, 3}), false)

	// row number of indices and values are different
	checkFunc(genUint32ArrList([]uint32{1, 2}), genFloat32ArrList(nil), false)

	// element number of indices and values are different
	checkFunc(genUint32ArrList([]uint32{1, 2}), genFloat32ArrList([]float32{0.1}), false)

	// duplicated indices
	checkFunc(genUint32ArrList([]uint32{4, 5, 4}), genFloat32ArrList([]float32{0.11, 0.22, 0.23}), false)

	// check result is correct
	// can handle empty indices/values
	byteArr, maxDim := checkFunc(genUint32ArrList([]uint32{}), genFloat32ArrList([]float32{}), true)
	assert.Equal(t, uint32(0), maxDim)
	assert.Equal(t, 1, len(byteArr))
	assert.Equal(t, 0, len(byteArr[0]))

	// note that the input indices is not sorted, the parseSparseFloatVectorStructs
	// returns correct maxDim and byteArr
	indices := []uint32{25, 78, 56}
	values := []float32{0.11, 0.22, 0.23}
	sortedIndices, sortedValues := typeutil.SortSparseFloatRow(indices, values)
	rowBytes := typeutil.CreateSparseFloatRow(sortedIndices, sortedValues)

	isValidFunc := func(indices arrow.Array, values arrow.Array) {
		byteArr, maxDim := checkFunc(indices, values, true)
		assert.Equal(t, uint32(78), maxDim)
		assert.Equal(t, 1, len(byteArr))
		assert.Equal(t, rowBytes, byteArr[0])
	}

	// ensure all supported types are correct
	isValidFunc(genUint32ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genUint32ArrList(indices), genFloat64ArrList(values))
	isValidFunc(genInt32ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genInt32ArrList(indices), genFloat64ArrList(values))
	isValidFunc(genUint64ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genUint64ArrList(indices), genFloat64ArrList(values))
	isValidFunc(genInt64ArrList(indices), genFloat32ArrList(values))
	isValidFunc(genInt64ArrList(indices), genFloat64ArrList(values))
}

func TestReadNullableSparseFloatVectorStructKeepsCompactRows(t *testing.T) {
	rowA := typeutil.CreateSparseFloatRow([]uint32{1}, []float32{1})
	rowB := typeutil.CreateSparseFloatRow([]uint32{2}, []float32{2})
	rowC := typeutil.CreateSparseFloatRow([]uint32{3}, []float32{3})

	tests := []struct {
		name           string
		validData      []bool
		contents       [][]byte
		rowGroupLength int64
	}{
		{
			name:           "null_first",
			validData:      []bool{false, true, true},
			contents:       [][]byte{rowA, rowB},
			rowGroupLength: 3,
		},
		{
			name:           "valid_null_valid",
			validData:      []bool{true, false, true},
			contents:       [][]byte{rowA, rowB},
			rowGroupLength: 3,
		},
		{
			name:           "all_null",
			validData:      []bool{false, false},
			contents:       [][]byte{},
			rowGroupLength: 2,
		},
		{
			name:           "multi_row_group",
			validData:      []bool{true, false, true, false, true},
			contents:       [][]byte{rowA, rowB, rowC},
			rowGroupLength: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkNullableSparseFloatVectorStructRead(t, tt.validData, tt.contents, tt.rowGroupLength)
		})
	}
}

func checkNullableSparseFloatVectorStructRead(t *testing.T, validData []bool, contents [][]byte, rowGroupLength int64) {
	const (
		pkFieldID     = int64(100)
		sparseFieldID = int64(101)
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: pkFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: sparseFieldID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, Nullable: true},
		},
	}
	sparseFields := []arrow.Field{
		{Name: sparseVectorIndice, Type: arrow.ListOf(&arrow.Uint32Type{})},
		{Name: sparseVectorValues, Type: arrow.ListOf(&arrow.Float32Type{})},
	}
	pqSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: &arrow.Int64Type{}},
		{Name: "sparse", Type: arrow.StructOf(sparseFields...), Nullable: true},
	}, nil)

	numRows := int64(len(validData))
	filePath := fmt.Sprintf("/tmp/test_%d_nullable_sparse_struct_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	require.NoError(t, err)
	defer wf.Close()
	fw, err := pqarrow.NewFileWriter(pqSchema, wf,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(rowGroupLength)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	mem := memory.NewGoAllocator()
	pkBuilder := array.NewInt64Builder(mem)
	pks := make([]int64, len(validData))
	for i := range pks {
		pks[i] = int64(i + 1)
	}
	pkBuilder.AppendValues(pks, nil)
	pkArray := pkBuilder.NewArray()
	defer pkArray.Release()
	pkBuilder.Release()

	sparseArray, err := testutil.BuildSparseVectorData(mem, contents, pqSchema.Field(1).Type, validData)
	require.NoError(t, err)
	defer sparseArray.Release()

	recordBatch := array.NewRecord(pqSchema, []arrow.Array{pkArray, sparseArray}, numRows)
	require.NoError(t, fw.Write(recordBatch))
	recordBatch.Release()
	require.NoError(t, fw.Close())

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	require.NoError(t, err)
	defer reader.Close()

	gotInsertData, err := reader.Read()
	require.NoError(t, err)
	gotSparse := gotInsertData.Data[sparseFieldID].(*storage.SparseFloatVectorFieldData)
	require.Equal(t, validData, gotSparse.ValidData)
	require.Len(t, gotSparse.GetContents(), len(contents))
	if len(contents) > 0 {
		require.Equal(t, contents, gotSparse.GetContents())
	}

	physicalIdx := 0
	for rowIdx, valid := range validData {
		if !valid {
			require.Nil(t, gotSparse.GetRow(rowIdx))
			continue
		}
		require.Equal(t, contents[physicalIdx], gotSparse.GetRow(rowIdx))
		physicalIdx++
	}
}

func TestReadFieldData(t *testing.T) {
	checkFunc := func(t *testing.T, nullPercent int, readScehamIsNullable bool, dataType schemapb.DataType, elementType schemapb.DataType) {
		fieldName := dataType.String()
		if elementType != schemapb.DataType_None {
			fieldName = fieldName + "_" + elementType.String()
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     100,
					Name:        fieldName,
					DataType:    dataType,
					ElementType: elementType,
					Nullable:    nullPercent != 0,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "16",
						},
						{
							Key:   "max_length",
							Value: "1000",
						},
						{
							Key:   "max_capacity",
							Value: "50",
						},
					},
				},
			},
		}

		arrDataType, err := convertToArrowDataType(schema.Fields[0], false)
		assert.NoError(t, err)
		arrFields := make([]arrow.Field, 0)
		arrFields = append(arrFields, arrow.Field{
			Name:     schema.Fields[0].Name,
			Type:     arrDataType,
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
		pqSchema := arrow.NewSchema(arrFields, nil)

		filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
		defer os.Remove(filePath)
		wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
		assert.NoError(t, err)

		fw, err := pqarrow.NewFileWriter(pqSchema, wf,
			parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(100)), pqarrow.DefaultWriterProps())
		assert.NoError(t, err)

		rowCount := 5
		insertData, err := testutil.CreateInsertData(schema, rowCount, nullPercent)
		assert.NoError(t, err)
		columns, err := testutil.BuildArrayData(schema, insertData, false)
		assert.NoError(t, err)

		recordBatch := array.NewRecord(pqSchema, columns, int64(rowCount))
		err = fw.Write(recordBatch)
		assert.NoError(t, err)
		fw.Close()

		ctx := context.Background()
		f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
		cm, err := f.NewPersistentStorageChunkManager(ctx)
		assert.NoError(t, err)

		schema.Fields[0].Nullable = readScehamIsNullable
		reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		defer reader.Close()

		_, err = reader.Read()
		if !readScehamIsNullable && nullPercent != 0 {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}

	type testCase struct {
		name                 string
		nullPercent          int
		readScehamIsNullable bool
		dataType             schemapb.DataType
		elementType          schemapb.DataType
	}
	buildCaseFunc := func(nullPercent int, readScehamIsNullable bool, dataType schemapb.DataType, elementType schemapb.DataType) *testCase {
		name := fmt.Sprintf("nullPercent='%v' schemaNullable='%v' dataType='%s' elementType='%s'",
			nullPercent, readScehamIsNullable, dataType, elementType)
		return &testCase{
			name:                 name,
			nullPercent:          nullPercent,
			readScehamIsNullable: readScehamIsNullable,
			dataType:             dataType,
			elementType:          elementType,
		}
	}
	cases := make([]*testCase, 0)

	nullableDataTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
	}
	for _, dataType := range nullableDataTypes {
		for _, nullPercent := range []int{0, 50} {
			for _, readScehamIsNullable := range []bool{true, false} {
				cases = append(cases, buildCaseFunc(nullPercent, readScehamIsNullable, dataType, schemapb.DataType_None))
			}
		}
	}

	elementTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
	}
	for _, elementType := range elementTypes {
		for _, nullPercent := range []int{0, 50} {
			for _, readScehamIsNullable := range []bool{true, false} {
				cases = append(cases, buildCaseFunc(nullPercent, readScehamIsNullable, schemapb.DataType_Array, elementType))
			}
		}
	}

	notNullableTypes := []schemapb.DataType{
		schemapb.DataType_JSON,
	}
	for _, dataType := range notNullableTypes {
		cases = append(cases, buildCaseFunc(0, false, dataType, schemapb.DataType_None))
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			checkFunc(t, tt.nullPercent, tt.readScehamIsNullable, tt.dataType, tt.elementType)
		})
	}
}

func TestTypeMismatch(t *testing.T) {
	checkFunc := func(srcDataType schemapb.DataType, srcElementType schemapb.DataType, dstDataType schemapb.DataType, dstElementType schemapb.DataType, nullalbe bool) {
		fieldName := "test_field"
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     100,
					Name:        fieldName,
					DataType:    srcDataType,
					ElementType: srcElementType,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "16",
						},
						{
							Key:   "max_length",
							Value: "1000",
						},
						{
							Key:   "max_capacity",
							Value: "50",
						},
					},
				},
			},
		}

		arrDataType, err := convertToArrowDataType(schema.Fields[0], false)
		assert.NoError(t, err)
		arrFields := make([]arrow.Field, 0)
		arrFields = append(arrFields, arrow.Field{
			Name:     schema.Fields[0].Name,
			Type:     arrDataType,
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
		pqSchema := arrow.NewSchema(arrFields, nil)

		filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
		defer os.Remove(filePath)
		wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
		assert.NoError(t, err)

		fw, err := pqarrow.NewFileWriter(pqSchema, wf,
			parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(100)), pqarrow.DefaultWriterProps())
		assert.NoError(t, err)

		rowCount := 5
		insertData, err := testutil.CreateInsertData(schema, rowCount, 0)
		assert.NoError(t, err)
		columns, err := testutil.BuildArrayData(schema, insertData, false)
		assert.NoError(t, err)

		recordBatch := array.NewRecord(pqSchema, columns, int64(rowCount))
		err = fw.Write(recordBatch)
		assert.NoError(t, err)
		fw.Close()

		ctx := context.Background()
		f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
		cm, err := f.NewPersistentStorageChunkManager(ctx)
		assert.NoError(t, err)

		schema.Fields[0].DataType = dstDataType
		schema.Fields[0].ElementType = dstElementType
		schema.Fields[0].Nullable = nullalbe
		cmReader, err := cm.Reader(ctx, filePath)
		assert.NoError(t, err)
		reader, err := file.NewParquetReader(cmReader, file.WithReadProps(&parquet.ReaderProperties{
			BufferSize:            65535,
			BufferedStreamEnabled: true,
		}))
		assert.NoError(t, err)

		readProps := pqarrow.ArrowReadProperties{
			BatchSize: int64(rowCount),
		}
		fileReader, err := pqarrow.NewFileReader(reader, readProps, memory.DefaultAllocator)
		assert.NoError(t, err)
		columnReader, err := NewFieldReader(ctx, fileReader, 0, schema.Fields[0], common.DefaultTimezone)
		assert.NoError(t, err)

		_, _, err = columnReader.Next(int64(rowCount))
		if srcDataType != dstDataType || srcElementType != dstElementType {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}

	type testCase struct {
		name           string
		srcDataType    schemapb.DataType
		srcElementType schemapb.DataType
		dstDataType    schemapb.DataType
		dstElementType schemapb.DataType
		nullable       bool
	}
	buildCaseFunc := func(srcDataType schemapb.DataType, srcElementType schemapb.DataType, dstDataType schemapb.DataType, dstElementType schemapb.DataType, nullable bool) *testCase {
		name := fmt.Sprintf("srcDataType='%s' srcElementType='%s' dstDataType='%s' dstElementType='%s' nullable='%v'",
			srcDataType, srcElementType, dstDataType, dstElementType, nullable)
		return &testCase{
			name:           name,
			srcDataType:    srcDataType,
			srcElementType: srcElementType,
			dstDataType:    dstDataType,
			dstElementType: dstElementType,
			nullable:       nullable,
		}
	}
	cases := make([]*testCase, 0)

	scalarDataTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
	}
	for _, dataType := range scalarDataTypes {
		srcDataType := schemapb.DataType_Bool
		if dataType == schemapb.DataType_Bool {
			srcDataType = schemapb.DataType_Int8
		}
		cases = append(cases, buildCaseFunc(srcDataType, schemapb.DataType_None, dataType, schemapb.DataType_None, true))
		cases = append(cases, buildCaseFunc(srcDataType, schemapb.DataType_None, dataType, schemapb.DataType_None, false))
	}

	elementTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
	}
	for _, elementType := range elementTypes {
		srcElementType := schemapb.DataType_Bool
		if elementType == schemapb.DataType_Bool {
			srcElementType = schemapb.DataType_Int8
		}
		// element type mismatch
		cases = append(cases, buildCaseFunc(schemapb.DataType_Array, srcElementType, schemapb.DataType_Array, elementType, true))
		cases = append(cases, buildCaseFunc(schemapb.DataType_Array, srcElementType, schemapb.DataType_Array, elementType, false))
		// not a list
		cases = append(cases, buildCaseFunc(schemapb.DataType_Bool, schemapb.DataType_None, schemapb.DataType_Array, elementType, true))
		cases = append(cases, buildCaseFunc(schemapb.DataType_Bool, schemapb.DataType_None, schemapb.DataType_Array, elementType, false))
	}

	vectorTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
	}
	for _, dataType := range vectorTypes {
		srcDataType := schemapb.DataType_Bool
		cases = append(cases, buildCaseFunc(srcDataType, schemapb.DataType_None, dataType, schemapb.DataType_None, true))
		cases = append(cases, buildCaseFunc(srcDataType, schemapb.DataType_None, dataType, schemapb.DataType_None, false))

		cases = append(cases, buildCaseFunc(schemapb.DataType_Array, schemapb.DataType_Bool, dataType, schemapb.DataType_None, true))
		cases = append(cases, buildCaseFunc(schemapb.DataType_Array, schemapb.DataType_Bool, dataType, schemapb.DataType_None, false))
	}

	notNullableTypes := []schemapb.DataType{
		schemapb.DataType_JSON,
	}
	for _, dataType := range notNullableTypes {
		srcDataType := schemapb.DataType_Bool
		if dataType == schemapb.DataType_Bool {
			srcDataType = schemapb.DataType_Int8
		}
		// not a list
		cases = append(cases, buildCaseFunc(srcDataType, schemapb.DataType_None, dataType, schemapb.DataType_None, false))
		// element type mismatch
		cases = append(cases, buildCaseFunc(schemapb.DataType_Array, schemapb.DataType_Bool, dataType, schemapb.DataType_None, false))
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			checkFunc(tt.srcDataType, tt.srcElementType, tt.dstDataType, tt.dstElementType, tt.nullable)
		})
	}
}

func TestArrayNullElement(t *testing.T) {
	checkFunc := func(dataType schemapb.DataType, elementType schemapb.DataType) {
		fieldName := "test_field"
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     100,
					Name:        fieldName,
					DataType:    dataType,
					ElementType: elementType,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "16",
						},
						{
							Key:   "max_length",
							Value: "1000",
						},
						{
							Key:   "max_capacity",
							Value: "50",
						},
					},
				},
			},
		}

		arrDataType, err := convertToArrowDataType(schema.Fields[0], false)
		assert.NoError(t, err)
		arrFields := make([]arrow.Field, 0)
		arrFields = append(arrFields, arrow.Field{
			Name:     schema.Fields[0].Name,
			Type:     arrDataType,
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
		pqSchema := arrow.NewSchema(arrFields, nil)

		filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
		defer os.Remove(filePath)
		wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
		assert.NoError(t, err)

		fw, err := pqarrow.NewFileWriter(pqSchema, wf,
			parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(100)), pqarrow.DefaultWriterProps())
		assert.NoError(t, err)

		mem := memory.NewGoAllocator()
		columns := make([]arrow.Array, 0, len(schema.Fields))
		switch elementType {
		case schemapb.DataType_Bool:
			builder := array.NewListBuilder(mem, &arrow.BooleanType{})
			valueBuilder := builder.ValueBuilder().(*array.BooleanBuilder)
			valueBuilder.AppendValues([]bool{true, false}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Int8:
			builder := array.NewListBuilder(mem, &arrow.Int8Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
			valueBuilder.AppendValues([]int8{1, 2}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Int16:
			builder := array.NewListBuilder(mem, &arrow.Int16Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int16Builder)
			valueBuilder.AppendValues([]int16{1, 2}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Int32:
			builder := array.NewListBuilder(mem, &arrow.Int32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
			valueBuilder.AppendValues([]int32{1, 2}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Int64:
			builder := array.NewListBuilder(mem, &arrow.Int64Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int64Builder)
			valueBuilder.AppendValues([]int64{1, 2}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Float:
			builder := array.NewListBuilder(mem, &arrow.Float32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
			valueBuilder.AppendValues([]float32{0.1, 0.2}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Double:
			builder := array.NewListBuilder(mem, &arrow.Float64Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float64Builder)
			valueBuilder.AppendValues([]float64{0.1, 0.2}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			builder := array.NewListBuilder(mem, &arrow.StringType{})
			valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
			valueBuilder.AppendValues([]string{"a", "b"}, []bool{true, false})
			builder.AppendValues([]int32{0}, []bool{true})
			columns = append(columns, builder.NewListArray())
		default:
			break
		}

		recordBatch := array.NewRecord(pqSchema, columns, int64(1))
		err = fw.Write(recordBatch)
		assert.NoError(t, err)
		fw.Close()

		ctx := context.Background()
		f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
		cm, err := f.NewPersistentStorageChunkManager(ctx)
		assert.NoError(t, err)

		reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		defer reader.Close()

		_, err = reader.Read()
		assert.Error(t, err)
	}

	type testCase struct {
		name        string
		dataType    schemapb.DataType
		elementType schemapb.DataType
	}
	buildCaseFunc := func(dataType schemapb.DataType, elementType schemapb.DataType) *testCase {
		name := fmt.Sprintf("dataType='%s' elementType='%s'", dataType, elementType)
		return &testCase{
			name:        name,
			dataType:    dataType,
			elementType: elementType,
		}
	}
	cases := make([]*testCase, 0)

	elementTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
	}
	for _, elementType := range elementTypes {
		cases = append(cases, buildCaseFunc(schemapb.DataType_Array, elementType))
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			checkFunc(tt.dataType, tt.elementType)
		})
	}
}

func TestStructFieldReader_toScalarField_TypeMismatch(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		data        []interface{}
	}{
		{"Bool_wrong_type", schemapb.DataType_Bool, []interface{}{"not_a_bool"}},
		{"Int8_wrong_type", schemapb.DataType_Int8, []interface{}{"not_an_int8"}},
		{"Int16_wrong_type", schemapb.DataType_Int16, []interface{}{3.14}},
		{"Int32_wrong_type", schemapb.DataType_Int32, []interface{}{true}},
		{"Int64_wrong_type", schemapb.DataType_Int64, []interface{}{"not_an_int64"}},
		{"Float_wrong_type", schemapb.DataType_Float, []interface{}{int32(1)}},
		{"Double_wrong_type", schemapb.DataType_Double, []interface{}{int64(1)}},
		{"VarChar_wrong_type", schemapb.DataType_VarChar, []interface{}{123}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &StructFieldReader{
				field: &schemapb.FieldSchema{
					Name:        "test_field",
					DataType:    schemapb.DataType_Array,
					ElementType: tt.elementType,
				},
			}
			_, err := reader.toScalarField(tt.data)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "expected")
		})
	}
}

func TestStructFieldReader_NullableArrayOfVector(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{
		Name:     "vector_array",
		Type:     arrow.ListOf(arrow.PrimitiveTypes.Float32),
		Nullable: true,
	})
	listBuilder := array.NewListBuilder(mem, structType)
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	vectorBuilder := structBuilder.FieldBuilder(0).(*array.ListBuilder)
	floatBuilder := vectorBuilder.ValueBuilder().(*array.Float32Builder)

	appendVector := func(values ...float32) {
		vectorBuilder.Append(true)
		floatBuilder.AppendValues(values, nil)
		structBuilder.Append(true)
	}
	listBuilder.Append(true)
	appendVector(1, 2, 3, 4)
	appendVector(5, 6, 7, 8)
	listBuilder.Append(false)
	listBuilder.Append(true)
	appendVector(9, 10, 11, 12)

	arr := listBuilder.NewArray()
	listBuilder.Release()
	defer arr.Release()

	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer chunked.Release()

	reader := &StructFieldReader{
		field: &schemapb.FieldSchema{
			Name:        "struct_array[vector_array]",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			Nullable:    true,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "20"},
			},
		},
		fieldIndex: 0,
		dim:        4,
	}

	_, _, err := reader.readArrayOfVectorField(chunked)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ArrayOfVector does not support nullable")
}

func TestStructFieldReader_ArrayOfVectorFloat64(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{
		Name:     "vector_array",
		Type:     arrow.ListOf(arrow.PrimitiveTypes.Float64),
		Nullable: true,
	})
	listBuilder := array.NewListBuilder(mem, structType)
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	vectorBuilder := structBuilder.FieldBuilder(0).(*array.ListBuilder)
	floatBuilder := vectorBuilder.ValueBuilder().(*array.Float64Builder)

	appendVector := func(values ...float64) {
		vectorBuilder.Append(true)
		floatBuilder.AppendValues(values, nil)
		structBuilder.Append(true)
	}
	listBuilder.Append(true)
	appendVector(1, 2, 3, 4)
	appendVector(5, 6, 7, 8)
	listBuilder.Append(true)
	appendVector(9, 10, 11, 12)

	arr := listBuilder.NewArray()
	listBuilder.Release()
	defer arr.Release()

	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer chunked.Release()

	reader := &StructFieldReader{
		field: &schemapb.FieldSchema{
			Name:        "struct_array[vector_array]",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "20"},
			},
		},
		fieldIndex: 0,
		dim:        4,
	}

	data, validData, err := reader.readArrayOfVectorField(chunked)
	require.NoError(t, err)
	require.Nil(t, validData)
	require.Len(t, data, 2)

	vectors := data.([]*schemapb.VectorField)
	require.Equal(t, []float32{1, 2, 3, 4, 5, 6, 7, 8}, vectors[0].GetFloatVector().GetData())
	require.Equal(t, []float32{9, 10, 11, 12}, vectors[1].GetFloatVector().GetData())
}

func TestStructFieldReader_ArrayMaxCapacity(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{
		Name: "int_array",
		Type: arrow.PrimitiveTypes.Int32,
	})
	listBuilder := array.NewListBuilder(mem, structType)
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	intBuilder := structBuilder.FieldBuilder(0).(*array.Int32Builder)

	appendValue := func(value int32) {
		intBuilder.Append(value)
		structBuilder.Append(true)
	}
	listBuilder.Append(true)
	appendValue(1)
	appendValue(2)

	arr := listBuilder.NewArray()
	listBuilder.Release()
	defer arr.Release()

	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer chunked.Release()

	reader := &StructFieldReader{
		field: &schemapb.FieldSchema{
			Name:        "struct_array[int_array]",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int32,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "1"},
			},
		},
		fieldIndex: 0,
	}

	_, _, err := reader.readArrayField(chunked)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max_capacity")
}

func TestStructFieldReader_ArrayOfVectorMaxCapacity(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{
		Name:     "vector_array",
		Type:     arrow.ListOf(arrow.PrimitiveTypes.Float32),
		Nullable: true,
	})
	listBuilder := array.NewListBuilder(mem, structType)
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	vectorBuilder := structBuilder.FieldBuilder(0).(*array.ListBuilder)
	floatBuilder := vectorBuilder.ValueBuilder().(*array.Float32Builder)

	appendVector := func(values ...float32) {
		vectorBuilder.Append(true)
		floatBuilder.AppendValues(values, nil)
		structBuilder.Append(true)
	}
	listBuilder.Append(true)
	appendVector(1, 2, 3, 4)
	appendVector(5, 6, 7, 8)

	arr := listBuilder.NewArray()
	listBuilder.Release()
	defer arr.Release()

	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	defer chunked.Release()

	reader := &StructFieldReader{
		field: &schemapb.FieldSchema{
			Name:        "struct_array[vector_array]",
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "1"},
			},
		},
		fieldIndex: 0,
		dim:        4,
	}

	_, _, err := reader.readArrayOfVectorField(chunked)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds max_capacity")
}
