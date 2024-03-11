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
	"encoding/json"
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
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReaderSuite struct {
	suite.Suite

	numRows     int
	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType
}

func (s *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *ReaderSuite) SetupTest() {
	// default suite params
	s.numRows = 100
	s.pkDataType = schemapb.DataType_Int64
	s.vecDataType = schemapb.DataType_FloatVector
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

func randomString(length int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
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
			if i%4 == 0 {
				v, _ := json.Marshal(fmt.Sprintf("{\"a\": \"%s\", \"b\": %d}", randomString(3), i))
				builder.Append(string(v))
			} else if i%4 == 1 {
				v, _ := json.Marshal(i)
				builder.Append(string(v))
			} else if i%4 == 2 {
				v, _ := json.Marshal(float32(i) * 0.1)
				builder.Append(string(v))
			} else if i%4 == 3 {
				v, _ := json.Marshal(randomString(10))
				builder.Append(string(v))
			}
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

func writeParquet(w io.Writer, schema *schemapb.CollectionSchema, numRows int) error {
	pqSchema := convertMilvusSchemaToArrowSchema(schema)
	fw, err := pqarrow.NewFileWriter(pqSchema, w, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(numRows))), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	columns := make([]arrow.Array, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		var dim int64 = 1
		if typeutil.IsVectorType(field.GetDataType()) {
			dim, err = typeutil.GetDim(field)
			if err != nil {
				return err
			}
		}
		columnData := buildArrayData(field.DataType, field.ElementType, int(dim), numRows, field.Name == "FieldBinaryVector2")
		columns = append(columns, columnData)
	}
	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	err = fw.Write(recordBatch)
	if err != nil {
		return err
	}

	return nil
}

func (s *ReaderSuite) run(dt schemapb.DataType) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     s.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: s.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:     102,
				Name:        dt.String(),
				DataType:    dt,
				ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
			},
		},
	}

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(s.T(), err)
	err = writeParquet(wf, schema, s.numRows)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	cmReader, err := cm.Reader(ctx, filePath)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, schema, cmReader, 64*1024*1024)
	s.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		// expectInsertData := insertData
		for _, data := range actualInsertData.Data {
			s.Equal(expectRows, data.RowNum())
			// TODO: dyh, check rows
			// fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
			// for i := 0; i < expectRows; i++ {
			// 	 expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
			// 	 actual := data.GetRow(i)
			//	 if fieldDataType == schemapb.DataType_Array {
			//		 s.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
			//	 } else {
			//		 s.Equal(expect, actual)
			//	 }
			// }
		}
	}

	res, err := reader.Read()
	s.NoError(err)
	checkFn(res, 0, s.numRows)
}

func (s *ReaderSuite) failRun(dt schemapb.DataType, isDynamic bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     s.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: s.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:     102,
				Name:        dt.String(),
				DataType:    dt,
				ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
				},
				IsDynamic: isDynamic,
			},
		},
	}

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(s.T(), err)
	err = writeParquet(wf, schema, s.numRows)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	cmReader, err := cm.Reader(ctx, filePath)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, schema, cmReader, 64*1024*1024)
	s.NoError(err)

	_, err = reader.Read()
	s.Error(err)
}

func (s *ReaderSuite) TestReadScalarFields() {
	s.run(schemapb.DataType_Bool)
	s.run(schemapb.DataType_Int8)
	s.run(schemapb.DataType_Int16)
	s.run(schemapb.DataType_Int32)
	s.run(schemapb.DataType_Int64)
	s.run(schemapb.DataType_Float)
	s.run(schemapb.DataType_Double)
	s.run(schemapb.DataType_VarChar)
	s.run(schemapb.DataType_Array)
	s.run(schemapb.DataType_JSON)
	s.failRun(schemapb.DataType_JSON, true)
}

func (s *ReaderSuite) TestStringPK() {
	s.pkDataType = schemapb.DataType_VarChar
	s.run(schemapb.DataType_Int32)
}

func (s *ReaderSuite) TestBinaryAndFloat16Vector() {
	s.vecDataType = schemapb.DataType_BinaryVector
	s.run(schemapb.DataType_Int32)
	// s.vecDataType = schemapb.DataType_Float16Vector
	// s.run(schemapb.DataType_Int32) // TODO: dyh, support float16 vector
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
