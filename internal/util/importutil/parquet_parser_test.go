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
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
				FieldID:      119,
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
			{
				FieldID:      122,
				Name:         "FieldBinaryVector2",
				IsPrimaryKey: false,
				Description:  "binary_vector2",
				DataType:     schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "64"},
				},
			},
		},
	}
	return schema
}

func milvusDataTypeToArrowType(dataType schemapb.DataType, isBinary bool) arrow.DataType {
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
		if isBinary {
			return &arrow.BinaryType{}
		}
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
		if field.GetDataType() == schemapb.DataType_Array {
			fields = append(fields, arrow.Field{
				Name: field.GetName(),
				Type: arrow.ListOfField(arrow.Field{
					Name:     "item",
					Type:     milvusDataTypeToArrowType(field.GetElementType(), false),
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
			Type:     milvusDataTypeToArrowType(field.GetDataType(), field.Name == "FieldBinaryVector2"),
			Nullable: true,
			Metadata: arrow.Metadata{},
		})
	}
	return arrow.NewSchema(fields, nil)
}

func buildArrayData(dataType, elementType schemapb.DataType, dim, rows int, isBinary bool) arrow.Array {
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
		if isBinary {
			builder := array.NewBinaryBuilder(mem, &arrow.BinaryType{})
			for i := 0; i < rows; i++ {
				element := make([]byte, dim/8)
				for j := 0; j < dim/8; j++ {
					element[j] = randomString(1)[0]
				}
				builder.Append(element)
			}
			return builder.NewBinaryArray()
		}
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
			index += i % 10
			offsets = append(offsets, int32(index))
			valid = append(valid, true)
		}
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
	fw, err := pqarrow.NewFileWriter(schema, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	batch := 1000
	for i := 0; i <= numRows/batch; i++ {
		columns := make([]arrow.Array, 0, len(milvusSchema.Fields))
		for _, field := range milvusSchema.Fields {
			dim, _ := getFieldDimension(field)
			columnData := buildArrayData(field.DataType, field.ElementType, dim, batch, field.Name == "FieldBinaryVector2")
			columns = append(columns, columnData)
		}
		recordBatch := array.NewRecord(schema, columns, int64(batch))
		err = fw.Write(recordBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeLessFieldParquet(w io.Writer, milvusSchema *schemapb.CollectionSchema, numRows int) error {
	for i, field := range milvusSchema.Fields {
		if field.GetName() == "FieldInt64" {
			milvusSchema.Fields = append(milvusSchema.Fields[:i], milvusSchema.Fields[i+1:]...)
			break
		}
	}
	schema := convertMilvusSchemaToArrowSchema(milvusSchema)
	fw, err := pqarrow.NewFileWriter(schema, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	batch := 1000
	for i := 0; i <= numRows/batch; i++ {
		columns := make([]arrow.Array, 0, len(milvusSchema.Fields))
		for _, field := range milvusSchema.Fields {
			dim, _ := getFieldDimension(field)
			columnData := buildArrayData(field.DataType, field.ElementType, dim, batch, field.Name == "FieldBinaryVector2")
			columns = append(columns, columnData)
		}
		recordBatch := array.NewRecord(schema, columns, int64(batch))
		err = fw.Write(recordBatch)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeMoreFieldParquet(w io.Writer, milvusSchema *schemapb.CollectionSchema, numRows int) error {
	milvusSchema.Fields = append(milvusSchema.Fields, &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "FieldMore",
		DataType: schemapb.DataType_Int64,
	})
	schema := convertMilvusSchemaToArrowSchema(milvusSchema)
	fw, err := pqarrow.NewFileWriter(schema, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	batch := 1000
	for i := 0; i <= numRows/batch; i++ {
		columns := make([]arrow.Array, 0, len(milvusSchema.Fields)+1)
		for _, field := range milvusSchema.Fields {
			dim, _ := getFieldDimension(field)
			columnData := buildArrayData(field.DataType, field.ElementType, dim, batch, field.Name == "FieldBinaryVector2")
			columns = append(columns, columnData)
		}
		recordBatch := array.NewRecord(schema, columns, int64(batch))
		err = fw.Write(recordBatch)
		if err != nil {
			return err
		}
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

func TestParquetParser(t *testing.T) {
	paramtable.Init()
	filePath := "/tmp/parser.parquet"
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
}

func TestParquetReader_Error(t *testing.T) {
	paramtable.Init()
	filePath := "/tmp/par_err.parquet"
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

	t.Run("list data mismatch", func(t *testing.T) {
		schema.Fields[11].DataType = schemapb.DataType_Bool
		schema.Fields[11].ElementType = schemapb.DataType_None
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
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Int8
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int16 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Int16
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int32 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Int32
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not int64 field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Int64
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not float field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
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

		t.Run("read not array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_Int64
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not array field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_Array
			columnReader.elementType = schemapb.DataType_VarChar
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

		t.Run("read irregular float vector field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayFloat"]
			columnReader.dataType = schemapb.DataType_FloatVector
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read irregular float vector field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayDouble"]
			columnReader.dataType = schemapb.DataType_FloatVector
			data, err := parquetParser.readData(columnReader, 1024)
			assert.Error(t, err)
			assert.Nil(t, data)
		})

		t.Run("read not binary vector field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldBool"]
			columnReader.dataType = schemapb.DataType_BinaryVector
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

		t.Run("read irregular binary vector field", func(t *testing.T) {
			columnReader := parquetParser.columnMap["FieldArrayInt64"]
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
	paramtable.Init()
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

	t.Run("chunk manager reader fail", func(t *testing.T) {
		collectionInfo, err := NewCollectionInfo(parquetSampleSchema(), 2, []int64{1})
		assert.NoError(t, err)

		idAllocator := newIDAllocator(ctx, t, nil)
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error"))
		flushFunc := func(fields BlockData, shardID int, partID int64) error {
			return nil
		}

		parquetParser, err := NewParquetParser(ctx, collectionInfo, idAllocator, 10240, cm, "", flushFunc, nil)
		assert.Error(t, err)
		assert.Nil(t, parquetParser)
	})
}

func Test_convertArrowSchemaToDataType(t *testing.T) {
	type testcase struct {
		arrowField arrow.Field
		dataType   schemapb.DataType
		isArray    bool
	}
	testcases := []testcase{
		{arrow.Field{Type: &arrow.BooleanType{}}, schemapb.DataType_Bool, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.BooleanType{}})}, schemapb.DataType_Bool, true},

		{arrow.Field{Type: &arrow.Int8Type{}}, schemapb.DataType_Int8, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int8Type{}})}, schemapb.DataType_Int8, true},

		{arrow.Field{Type: &arrow.Int16Type{}}, schemapb.DataType_Int16, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int16Type{}})}, schemapb.DataType_Int16, true},

		{arrow.Field{Type: &arrow.Int32Type{}}, schemapb.DataType_Int32, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int32Type{}})}, schemapb.DataType_Int32, true},

		{arrow.Field{Type: &arrow.Int64Type{}}, schemapb.DataType_Int64, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Int64Type{}})}, schemapb.DataType_Int64, true},

		{arrow.Field{Type: &arrow.Float32Type{}}, schemapb.DataType_Float, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float32Type{}})}, schemapb.DataType_Float, true},

		{arrow.Field{Type: &arrow.Float64Type{}}, schemapb.DataType_Double, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float64Type{}})}, schemapb.DataType_Double, true},

		{arrow.Field{Type: &arrow.StringType{}}, schemapb.DataType_VarChar, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.StringType{}})}, schemapb.DataType_VarChar, true},

		{arrow.Field{Type: &arrow.BinaryType{}}, schemapb.DataType_BinaryVector, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Uint8Type{}})}, schemapb.DataType_BinaryVector, true},
		{arrow.Field{Type: &arrow.Uint8Type{}}, schemapb.DataType_None, false},

		{arrow.Field{Type: &arrow.Float16Type{}}, schemapb.DataType_None, false},
		{arrow.Field{Type: arrow.ListOfField(arrow.Field{Type: &arrow.Float16Type{}})}, schemapb.DataType_Float16Vector, true},

		{arrow.Field{Type: &arrow.DayTimeIntervalType{}}, schemapb.DataType_None, false},
	}

	for _, tt := range testcases {
		arrowType, isList := convertArrowSchemaToDataType(tt.arrowField, false)
		assert.Equal(t, tt.isArray, isList)
		assert.Equal(t, tt.dataType, arrowType)
	}
}

func Test_isConvertible(t *testing.T) {
	type testcase struct {
		arrowType schemapb.DataType
		dataType  schemapb.DataType
		isArray   bool
		expect    bool
	}
	testcases := []testcase{
		{schemapb.DataType_Bool, schemapb.DataType_Bool, false, true},
		{schemapb.DataType_Bool, schemapb.DataType_Bool, true, true},
		{schemapb.DataType_Bool, schemapb.DataType_Int8, false, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int8, true, false},
		{schemapb.DataType_Bool, schemapb.DataType_String, false, false},
		{schemapb.DataType_Bool, schemapb.DataType_String, true, false},

		{schemapb.DataType_Int8, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_Int8, schemapb.DataType_String, false, false},
		{schemapb.DataType_Int8, schemapb.DataType_JSON, false, false},
		{schemapb.DataType_Int8, schemapb.DataType_Int8, false, true},
		{schemapb.DataType_Int8, schemapb.DataType_Int8, true, true},
		{schemapb.DataType_Int8, schemapb.DataType_Int16, false, true},
		{schemapb.DataType_Int8, schemapb.DataType_Int32, false, true},
		{schemapb.DataType_Int8, schemapb.DataType_Int64, false, true},
		{schemapb.DataType_Int8, schemapb.DataType_Float, false, true},
		{schemapb.DataType_Int8, schemapb.DataType_Double, false, true},
		{schemapb.DataType_Int8, schemapb.DataType_FloatVector, false, false},

		{schemapb.DataType_Int16, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_Int16, schemapb.DataType_String, false, false},
		{schemapb.DataType_Int16, schemapb.DataType_JSON, false, false},
		{schemapb.DataType_Int16, schemapb.DataType_Int8, false, false},
		{schemapb.DataType_Int16, schemapb.DataType_Int16, false, true},
		{schemapb.DataType_Int16, schemapb.DataType_Int32, false, true},
		{schemapb.DataType_Int16, schemapb.DataType_Int64, false, true},
		{schemapb.DataType_Int16, schemapb.DataType_Float, false, true},
		{schemapb.DataType_Int16, schemapb.DataType_Double, false, true},
		{schemapb.DataType_Int16, schemapb.DataType_FloatVector, false, false},

		{schemapb.DataType_Int32, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_Int32, schemapb.DataType_String, false, false},
		{schemapb.DataType_Int32, schemapb.DataType_JSON, false, false},
		{schemapb.DataType_Int32, schemapb.DataType_Int8, false, false},
		{schemapb.DataType_Int32, schemapb.DataType_Int16, false, false},
		{schemapb.DataType_Int32, schemapb.DataType_Int32, false, true},
		{schemapb.DataType_Int32, schemapb.DataType_Int64, false, true},
		{schemapb.DataType_Int32, schemapb.DataType_Float, false, true},
		{schemapb.DataType_Int32, schemapb.DataType_Double, false, true},
		{schemapb.DataType_Int32, schemapb.DataType_FloatVector, false, false},

		{schemapb.DataType_Int64, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_Int64, schemapb.DataType_String, false, false},
		{schemapb.DataType_Int64, schemapb.DataType_JSON, false, false},
		{schemapb.DataType_Int64, schemapb.DataType_Int8, false, false},
		{schemapb.DataType_Int64, schemapb.DataType_Int16, false, false},
		{schemapb.DataType_Int64, schemapb.DataType_Int32, false, false},
		{schemapb.DataType_Int64, schemapb.DataType_Int64, false, true},
		{schemapb.DataType_Int64, schemapb.DataType_Float, false, true},
		{schemapb.DataType_Int64, schemapb.DataType_Double, false, true},
		{schemapb.DataType_Int64, schemapb.DataType_FloatVector, false, false},

		{schemapb.DataType_Float, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_Float, schemapb.DataType_String, false, false},
		{schemapb.DataType_Float, schemapb.DataType_JSON, false, false},
		{schemapb.DataType_Float, schemapb.DataType_Int8, false, false},
		{schemapb.DataType_Float, schemapb.DataType_Int16, false, false},
		{schemapb.DataType_Float, schemapb.DataType_Int32, false, false},
		{schemapb.DataType_Float, schemapb.DataType_Int64, false, false},
		{schemapb.DataType_Float, schemapb.DataType_Float, false, true},
		{schemapb.DataType_Float, schemapb.DataType_Double, false, true},
		{schemapb.DataType_Float, schemapb.DataType_FloatVector, true, true},

		{schemapb.DataType_Double, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_Double, schemapb.DataType_String, false, false},
		{schemapb.DataType_Double, schemapb.DataType_JSON, false, false},
		{schemapb.DataType_Double, schemapb.DataType_Int8, false, false},
		{schemapb.DataType_Double, schemapb.DataType_Int16, false, false},
		{schemapb.DataType_Double, schemapb.DataType_Int32, false, false},
		{schemapb.DataType_Double, schemapb.DataType_Int64, false, false},
		{schemapb.DataType_Double, schemapb.DataType_Float, false, false},
		{schemapb.DataType_Double, schemapb.DataType_Double, false, true},
		{schemapb.DataType_Double, schemapb.DataType_FloatVector, true, true},

		{schemapb.DataType_VarChar, schemapb.DataType_VarChar, false, true},
		{schemapb.DataType_VarChar, schemapb.DataType_JSON, false, true},
		{schemapb.DataType_VarChar, schemapb.DataType_Bool, false, false},
		{schemapb.DataType_VarChar, schemapb.DataType_Int64, false, false},
		{schemapb.DataType_VarChar, schemapb.DataType_Float, false, false},
		{schemapb.DataType_VarChar, schemapb.DataType_FloatVector, false, false},

		{schemapb.DataType_Float16Vector, schemapb.DataType_Float16Vector, true, true},
		{schemapb.DataType_Float16Vector, schemapb.DataType_Float16Vector, false, true},
		{schemapb.DataType_BinaryVector, schemapb.DataType_BinaryVector, true, true},
		{schemapb.DataType_BinaryVector, schemapb.DataType_BinaryVector, false, true},

		{schemapb.DataType_JSON, schemapb.DataType_JSON, false, true},
		{schemapb.DataType_JSON, schemapb.DataType_VarChar, false, false},

		{schemapb.DataType_Array, schemapb.DataType_Array, false, false},
	}
	for _, tt := range testcases {
		assert.Equal(t, tt.expect, isConvertible(tt.arrowType, tt.dataType, tt.isArray))
	}
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

func TestParquetParser_LessField(t *testing.T) {
	paramtable.Init()
	filePath := "/tmp/less_field.parquet"
	ctx := context.Background()
	schema := parquetSampleSchema()
	idAllocator := newIDAllocator(ctx, t, nil)
	defer os.Remove(filePath)

	writeFile := func() {
		wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
		assert.NoError(t, err)
		err = writeLessFieldParquet(wf, schema, 100)
		assert.NoError(t, err)
	}
	writeFile()

	schema = parquetSampleSchema()

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
		assert.Error(t, err)
	})
}

func TestParquetParser_MoreField(t *testing.T) {
	paramtable.Init()
	filePath := "/tmp/more_field.parquet"
	ctx := context.Background()
	schema := parquetSampleSchema()
	idAllocator := newIDAllocator(ctx, t, nil)
	defer os.Remove(filePath)

	writeFile := func() {
		wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
		assert.NoError(t, err)
		err = writeMoreFieldParquet(wf, schema, 100)
		assert.NoError(t, err)
	}
	writeFile()

	schema = parquetSampleSchema()

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
		assert.Error(t, err)
	})
}
