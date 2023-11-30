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

package importutil

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

// parquetSampleSchema() return a schema contains all supported data types with an int64 primary key
func parquetSampleSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:               "schema",
		Description:        "schema",
		AutoID:             true,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      102,
				Name:         "FieldBool",
				IsPrimaryKey: false,
				Description:  "bool",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      103,
				Name:         "FieldInt8",
				IsPrimaryKey: false,
				Description:  "int8",
				DataType:     schemapb.DataType_Int8,
			},
			{
				FieldID:      104,
				Name:         "FieldInt16",
				IsPrimaryKey: false,
				Description:  "int16",
				DataType:     schemapb.DataType_Int16,
			},
			{
				FieldID:      105,
				Name:         "FieldInt32",
				IsPrimaryKey: false,
				Description:  "int32",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      106,
				Name:         "FieldInt64",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      107,
				Name:         "FieldFloat",
				IsPrimaryKey: false,
				Description:  "float",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      108,
				Name:         "FieldDouble",
				IsPrimaryKey: false,
				Description:  "double",
				DataType:     schemapb.DataType_Double,
			},
			{
				FieldID:      109,
				Name:         "FieldString",
				IsPrimaryKey: false,
				Description:  "string",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "128"},
				},
			},
			{
				FieldID:      110,
				Name:         "FieldBinaryVector",
				IsPrimaryKey: false,
				Description:  "binary_vector",
				DataType:     schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "32"},
				},
			},
			{
				FieldID:      111,
				Name:         "FieldFloatVector",
				IsPrimaryKey: false,
				Description:  "float_vector",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
			{
				FieldID:      112,
				Name:         "FieldJSON",
				IsPrimaryKey: false,
				Description:  "json",
				DataType:     schemapb.DataType_JSON,
			},
			{
				FieldID:      113,
				Name:         "FieldArrayBool",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Bool,
			},
			{
				FieldID:      114,
				Name:         "FieldArrayInt8",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int8,
			},
			{
				FieldID:      115,
				Name:         "FieldArrayInt16",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int16,
			},
			{
				FieldID:      116,
				Name:         "FieldArrayInt32",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int32,
			},
			{
				FieldID:      117,
				Name:         "FieldArrayInt64",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int64,
			},
			{
				FieldID:      118,
				Name:         "FieldArrayFloat",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Float,
			},
			{
				FieldID:      118,
				Name:         "FieldArrayDouble",
				IsPrimaryKey: false,
				Description:  "int16 array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Double,
			},
			{
				FieldID:      120,
				Name:         "FieldArrayString",
				IsPrimaryKey: false,
				Description:  "string array",
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_VarChar,
			},
			{
				FieldID:      121,
				Name:         "$meta",
				IsPrimaryKey: false,
				Description:  "dynamic field",
				DataType:     schemapb.DataType_JSON,
				IsDynamic:    true,
			},
		},
	}
	return schema
}

func milvusDataTypeToArrowType(dataType schemapb.DataType, dim int) arrow.DataType {
	switch dataType {
	case schemapb.DataType_Bool:
		return &arrow.BooleanType{}
	case schemapb.DataType_Int8:
		return &arrow.Int8Type{}
	case schemapb.DataType_Int16:
		return &arrow.Int16Type{}
	case schemapb.DataType_Int32:
		return &arrow.Int32Type{}
	case schemapb.DataType_Int64:
		return &arrow.Int64Type{}
	case schemapb.DataType_Float:
		return &arrow.Float32Type{}
	case schemapb.DataType_Double:
		return &arrow.Float64Type{}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return &arrow.StringType{}
	case schemapb.DataType_Array:
		return &arrow.ListType{}
	case schemapb.DataType_JSON:
		return &arrow.StringType{}
	case schemapb.DataType_FloatVector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Float32Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
	case schemapb.DataType_BinaryVector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Uint8Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
	case schemapb.DataType_Float16Vector:
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     &arrow.Float16Type{},
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
	default:
		panic("unsupported data type")
	}
}

func convertMilvusSchemaToArrowSchema(schema *schemapb.CollectionSchema) *arrow.Schema {
	fields := make([]arrow.Field, 0)
	for _, field := range schema.GetFields() {
		dim, _ := getFieldDimension(field)
		if field.GetDataType() == schemapb.DataType_Array {
			fields = append(fields, arrow.Field{
				Name: field.GetName(),
				Type: arrow.ListOfField(arrow.Field{
					Name:     "item",
					Type:     milvusDataTypeToArrowType(field.GetElementType(), dim),
					Nullable: true,
					Metadata: arrow.Metadata{},
				}),
				Nullable: true,
				Metadata: arrow.Metadata{},
			})
			continue
		}
		fields = append(fields, arrow.Field{
			Name:     field.GetName(),
			Type:     milvusDataTypeToArrowType(field.GetDataType(), dim),
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
	}
	return arrow.NewSchema(fields, nil)
}

func buildArrayData(dataType, elementType schemapb.DataType, dim, rows int) arrow.Array {
	mem := memory.NewGoAllocator()
	switch dataType {
	case schemapb.DataType_Bool:
		builder := array.NewBooleanBuilder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(i%2 == 0)
		}
		return builder.NewBooleanArray()
	case schemapb.DataType_Int8:
		builder := array.NewInt8Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int8(i))
		}
		return builder.NewInt8Array()
	case schemapb.DataType_Int16:
		builder := array.NewInt16Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int16(i))
		}
		return builder.NewInt16Array()
	case schemapb.DataType_Int32:
		builder := array.NewInt32Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int32(i))
		}
		return builder.NewInt32Array()
	case schemapb.DataType_Int64:
		builder := array.NewInt64Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(int64(i))
		}
		return builder.NewInt64Array()
	case schemapb.DataType_Float:
		builder := array.NewFloat32Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(float32(i) * 0.1)
		}
		return builder.NewFloat32Array()
	case schemapb.DataType_Double:
		builder := array.NewFloat64Builder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(float64(i) * 0.02)
		}
		return builder.NewFloat64Array()
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		builder := array.NewStringBuilder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(randomString(10))
		}
		return builder.NewStringArray()
	case schemapb.DataType_FloatVector:
		builder := array.NewListBuilder(mem, &arrow.Float32Type{})
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0, rows)
		for i := 0; i < dim*rows; i++ {
			builder.ValueBuilder().(*array.Float32Builder).Append(float32(i))
		}
		for i := 0; i < rows; i++ {
			offsets = append(offsets, int32(i*dim))
			valid = append(valid, true)
		}
		builder.AppendValues(offsets, valid)
		return builder.NewListArray()
	case schemapb.DataType_BinaryVector:
		builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0)
		for i := 0; i < dim*rows/8; i++ {
			builder.ValueBuilder().(*array.Uint8Builder).Append(uint8(i))
		}
		for i := 0; i < rows; i++ {
			offsets = append(offsets, int32(dim*i/8))
			valid = append(valid, true)
		}
		builder.AppendValues(offsets, valid)
		return builder.NewListArray()
	case schemapb.DataType_JSON:
		builder := array.NewStringBuilder(mem)
		for i := 0; i < rows; i++ {
			builder.Append(fmt.Sprintf("{\"a\": \"%s\", \"b\": %d}", randomString(3), i))
		}
		return builder.NewStringArray()
	case schemapb.DataType_Array:
		offsets := make([]int32, 0, rows)
		valid := make([]bool, 0, rows)
		index := 0
		for i := 0; i < rows; i++ {
			index += i
			offsets = append(offsets, int32(index))
			valid = append(valid, true)
		}
		index += rows
		switch elementType {
		case schemapb.DataType_Bool:
			builder := array.NewListBuilder(mem, &arrow.BooleanType{})
			valueBuilder := builder.ValueBuilder().(*array.BooleanBuilder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(i%2 == 0)
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int8:
			builder := array.NewListBuilder(mem, &arrow.Int8Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int8(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int16:
			builder := array.NewListBuilder(mem, &arrow.Int16Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int16Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int16(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int32:
			builder := array.NewListBuilder(mem, &arrow.Int32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int32(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Int64:
			builder := array.NewListBuilder(mem, &arrow.Int64Type{})
			valueBuilder := builder.ValueBuilder().(*array.Int64Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(int64(i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Float:
			builder := array.NewListBuilder(mem, &arrow.Float32Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(float32(i) * 0.1)
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_Double:
			builder := array.NewListBuilder(mem, &arrow.Float64Type{})
			valueBuilder := builder.ValueBuilder().(*array.Float64Builder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(float64(i) * 0.02)
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			builder := array.NewListBuilder(mem, &arrow.StringType{})
			valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
			for i := 0; i < index; i++ {
				valueBuilder.Append(randomString(5) + "-" + fmt.Sprintf("%d", i))
			}
			builder.AppendValues(offsets, valid)
			return builder.NewListArray()
		}
	}
	return nil
}

func writeParquet(w io.Writer, milvusSchema *schemapb.CollectionSchema, numRows int) error {
	schema := convertMilvusSchemaToArrowSchema(milvusSchema)
	columns := make([]arrow.Array, 0, len(milvusSchema.Fields))
	for _, field := range milvusSchema.Fields {
		dim, _ := getFieldDimension(field)
		columnData := buildArrayData(field.DataType, field.ElementType, dim, numRows)
		columns = append(columns, columnData)
	}
	recordBatch := array.NewRecord(schema, columns, int64(numRows))
	fw, err := pqarrow.NewFileWriter(schema, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	err = fw.Write(recordBatch)
	if err != nil {
		return err
	}
	return nil
}

func randomString(length int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestParquetReader(t *testing.T) {
	filePath := "/tmp/wp.parquet"
	ctx := context.Background()
	schema := parquetSampleSchema()
	idAllocator := newIDAllocator(ctx, t, nil)
	defer os.Remove(filePath)

	writeFile := func() {
		wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
		assert.NoError(t, err)
		err = writeParquet(wf, schema, 100)
		assert.NoError(t, err)
	}
	writeFile()

	t.Run("read file", func(t *testing.T) {
		cm := createLocalChunkManager(t)
		flushFunc := func(fields BlockData, shardID int, partID int64) error {
			return nil
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		updateProgress := func(percent int64) {
			assert.Greater(t, percent, int64(0))
		}

		// parquet schema sizePreRecord = 5296
		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 102400, cm, filePath, flushFunc, updateProgress)
		assert.NoError(t, err)
		defer parquetParser.Close()
		err = parquetParser.Parse()
		assert.NoError(t, err)
	})

	t.Run("field not exist", func(t *testing.T) {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:     200,
			Name:        "invalid",
			Description: "invalid field",
			DataType:    schemapb.DataType_JSON,
		})

		cm := createLocalChunkManager(t)
		flushFunc := func(fields BlockData, shardID int, partID int64) error {
			return nil
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, cm, filePath, flushFunc, nil)
		assert.NoError(t, err)
		defer parquetParser.Close()
		err = parquetParser.Parse()
		assert.Error(t, err)

		// reset schema
		schema = parquetSampleSchema()
	})

	t.Run("schema mismatch", func(t *testing.T) {
		schema.Fields[0].DataType = schemapb.DataType_JSON
		cm := createLocalChunkManager(t)
		flushFunc := func(fields BlockData, shardID int, partID int64) error {
			return nil
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, cm, filePath, flushFunc, nil)
		assert.NoError(t, err)
		defer parquetParser.Close()
		err = parquetParser.Parse()
		assert.Error(t, err)

		// reset schema
		schema = parquetSampleSchema()
	})

	t.Run("data not match", func(t *testing.T) {
		cm := createLocalChunkManager(t)
		flushFunc := func(fields BlockData, shardID int, partID int64) error {
			return nil
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, cm, filePath, flushFunc, nil)
		assert.NoError(t, err)
		defer parquetParser.Close()

		err = parquetParser.createReaders()
		assert.NoError(t, err)
		t.Run("read not bool field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldInt8"]
			columnReader.dataType = schemapb.DataType_Bool
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int8 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldInt16"]
			columnReader.dataType = schemapb.DataType_Int8
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int16 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldInt32"]
			columnReader.dataType = schemapb.DataType_Int16
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int32 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldInt64"]
			columnReader.dataType = schemapb.DataType_Int32
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int64 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldFloat"]
			columnReader.dataType = schemapb.DataType_Int64
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not float field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldDouble"]
			columnReader.dataType = schemapb.DataType_Float
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not double field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Double
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not string field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_VarChar
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Bool
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not bool array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Bool
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int8 array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Int8
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int16 array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Int16
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int32 array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Int32
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int64 array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Int64
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not float array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Float
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not double array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Double
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not string array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayBool"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_VarChar
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not float vector field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayBool"]
			columnReader.dataType = schemapb.DataType_FloatVector
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read irregular float vector", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayFloat"]
			columnReader.dataType = schemapb.DataType_FloatVector
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read irregular float vector", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayDouble"]
			columnReader.dataType = schemapb.DataType_FloatVector
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not binary vector field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayBool"]
			columnReader.dataType = schemapb.DataType_BinaryVector
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not json field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_JSON
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read illegal json field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldString"]
			columnReader.dataType = schemapb.DataType_JSON
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read unknown field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldString"]
			columnReader.dataType = schemapb.DataType_None
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read unsupported array", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayString"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_JSON
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})
	})

	t.Run("flush failed", func(t *testing.T) {
		cm := createLocalChunkManager(t)
		flushFunc := func(fields BlockData, shardID int, partID int64) error {
			return fmt.Errorf("mock error")
		}
		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		updateProgress := func(percent int64) {
			assert.Greater(t, percent, int64(0))
		}

		// parquet schema sizePreRecord = 5296
		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 102400, cm, filePath, flushFunc, updateProgress)
		assert.NoError(t, err)
		defer parquetParser.Close()
		err = parquetParser.Parse()
		assert.Error(t, err)
	})
}

func TestNewParquetParser(t *testing.T) {
	ctx := context.Background()
	t.Run("nil collectionInfo", func(t *testing.T) {
		parquetParser, err := NewParquetParser(ctx, nil, nil, 10240, nil, "", nil, nil)
		assert.Error(t, err)
		assert.Nil(t, parquetParser)
	})

	t.Run("nil idAlloc", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(parquetSampleSchema(), 2, []int64{1})
		assert.NoError(t, err)

		parquetParser, err := NewParquetParser(ctx, collectionInfo, nil, 10240, nil, "", nil, nil)
		assert.Error(t, err)
		assert.Nil(t, parquetParser)
	})

	t.Run("nil chunk manager", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(parquetSampleSchema(), 2, []int64{1})
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)

		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, nil, "", nil, nil)
		assert.Error(t, err)
		assert.Nil(t, parquetParser)
	})

	t.Run("nil flush func", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(parquetSampleSchema(), 2, []int64{1})
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)
		cm := createLocalChunkManager(t)

		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, cm, "", nil, nil)
		assert.Error(t, err)
		assert.Nil(t, parquetParser)
	})
	//
	//t.Run("create reader with closed file", func(t *testing.T) {
	//	collectionInfo, err := NewCollectionInfo(parquetSampleSchema(), 2, []int64{1})
	//	assert.NoError(t, err)
	//
	//	idAllocator := newIDAllocator(ctx, t, nil)
	//	cm := createLocalChunkManager(t)
	//	flushFunc := func(fields BlockData, shardID int, partID int64) error {
	//		return nil
	//	}
	//
	//	rf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	//	assert.NoError(t, err)
	//	r := storage.NewLocalFile(rf)
	//
	//	parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, cm, filePath, flushFunc, nil)
	//	assert.Error(t, err)
	//	assert.Nil(t, parquetParser)
	//})
}

func TestVerifyFieldSchema(t *testing.T) {
	ok := verifyFieldSchema(schemapb.DataType_Bool, schemapb.DataType_None, arrow.Field{Type: &arrow.BooleanType{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Bool, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.BooleanType{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Int8, schemapb.DataType_None, arrow.Field{Type: &arrow.Int8Type{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Int8, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int8Type{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Int16, schemapb.DataType_None, arrow.Field{Type: &arrow.Int16Type{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Int16, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int16Type{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Int32, schemapb.DataType_None, arrow.Field{Type: &arrow.Int32Type{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Int32, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int32Type{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Int64, schemapb.DataType_None, arrow.Field{Type: &arrow.Int64Type{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Int64, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int64Type{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Float, schemapb.DataType_None, arrow.Field{Type: &arrow.Float32Type{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Float, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float32Type{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Double, schemapb.DataType_None, arrow.Field{Type: &arrow.Float64Type{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_Double, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float64Type{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_VarChar, schemapb.DataType_None, arrow.Field{Type: &arrow.StringType{}})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_VarChar, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.StringType{}})})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_FloatVector, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float32Type{}})})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_FloatVector, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float64Type{}})})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_FloatVector, schemapb.DataType_None, arrow.Field{Type: &arrow.Float32Type{}})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_BinaryVector, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Uint8Type{}})})
	assert.True(t, ok)
	ok = verifyFieldSchema(schemapb.DataType_BinaryVector, schemapb.DataType_None, arrow.Field{Type: &arrow.Uint8Type{}})
	assert.False(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Bool, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.BooleanType{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Int8, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int8Type{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Int16, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int16Type{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Int32, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int32Type{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Int64, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int64Type{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Float, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float32Type{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_Double, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float64Type{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_VarChar, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.StringType{}})})
	assert.True(t, ok)

	ok = verifyFieldSchema(schemapb.DataType_Array, schemapb.DataType_None, arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int64Type{}})})
	assert.False(t, ok)
}

func TestCalcRowCountPerBlock(t *testing.T) {
	t.Run("dim not valid", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "dim_invalid",
			Description: "dim not invalid",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					Description:  "pk",
					DataType:     schemapb.DataType_Int64,
					AutoID:       true,
				},
				{
					FieldID:     101,
					Name:        "vector",
					Description: "vector",
					DataType:    schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "invalid",
						},
					},
				},
			},
			EnableDynamicField: false,
		}

		collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
		assert.NoError(t, err)

		p := &ParquetParser{
			collectionInfo: collectionInfo,
		}

		_, err = p.calcRowCountPerBlock()
		assert.Error(t, err)

		err = p.consume()
		assert.Error(t, err)
	})

	t.Run("nil schema", func(t *testing.T) {
		collectionInfo := &CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name:               "nil_schema",
				Description:        "",
				AutoID:             false,
				Fields:             nil,
				EnableDynamicField: false,
			},
			ShardNum: 2,
		}
		p := &ParquetParser{
			collectionInfo: collectionInfo,
		}

		_, err := p.calcRowCountPerBlock()
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(parquetSampleSchema(), 2, []int64{1})
		assert.NoError(t, err)

		p := &ParquetParser{
			collectionInfo: collectionInfo,
			blockSize:      10,
		}

		_, err = p.calcRowCountPerBlock()
		assert.NoError(t, err)
	})
}
