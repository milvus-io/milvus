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
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

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

func randomString(length int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func buildArrayData(schema *schemapb.CollectionSchema, rows int) ([]arrow.Array, *storage.InsertData, error) {
	mem := memory.NewGoAllocator()
	columns := make([]arrow.Array, 0, len(schema.Fields))
	insertData, err := storage.NewInsertData(schema)
	if err != nil {
		return nil, nil, err
	}
	for _, field := range schema.Fields {
		dim := 1
		if typeutil.IsVectorType(field.GetDataType()) {
			dim2, err := typeutil.GetDim(field)
			if err != nil {
				return nil, nil, err
			}
			dim = int(dim2)
		}
		dataType := field.GetDataType()
		elementType := field.GetElementType()
		isBinary := field.GetName() == "FieldBinaryVector2"
		switch dataType {
		case schemapb.DataType_Bool:
			builder := array.NewBooleanBuilder(mem)
			boolData := make([]bool, 0)
			for i := 0; i < rows; i++ {
				boolData = append(boolData, i%2 == 0)
			}
			insertData.Data[field.GetFieldID()] = &storage.BoolFieldData{Data: boolData}
			builder.AppendValues(boolData, nil)
			columns = append(columns, builder.NewBooleanArray())
		case schemapb.DataType_Int8:
			builder := array.NewInt8Builder(mem)
			int8Data := make([]int8, 0)
			for i := 0; i < rows; i++ {
				int8Data = append(int8Data, int8(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int8FieldData{Data: int8Data}
			builder.AppendValues(int8Data, nil)
			columns = append(columns, builder.NewInt8Array())
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			builder := array.NewInt16Builder(mem)
			for i := 0; i < rows; i++ {
				int16Data = append(int16Data, int16(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int16FieldData{Data: int16Data}
			builder.AppendValues(int16Data, nil)
			columns = append(columns, builder.NewInt16Array())
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			builder := array.NewInt32Builder(mem)
			for i := 0; i < rows; i++ {
				int32Data = append(int32Data, int32(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int32FieldData{Data: int32Data}
			builder.AppendValues(int32Data, nil)
			columns = append(columns, builder.NewInt32Array())
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			builder := array.NewInt64Builder(mem)
			for i := 0; i < rows; i++ {
				int64Data = append(int64Data, int64(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int64FieldData{Data: int64Data}
			builder.AppendValues(int64Data, nil)
			columns = append(columns, builder.NewInt64Array())
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			builder := array.NewFloat32Builder(mem)
			for i := 0; i < rows; i++ {
				floatData = append(floatData, float32(i)*0.1)
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatFieldData{Data: floatData}
			builder.AppendValues(floatData, nil)
			columns = append(columns, builder.NewFloat32Array())
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			builder := array.NewFloat64Builder(mem)
			for i := 0; i < rows; i++ {
				doubleData = append(doubleData, float64(i)*0.02)
			}
			insertData.Data[field.GetFieldID()] = &storage.DoubleFieldData{Data: doubleData}
			builder.AppendValues(doubleData, nil)
			columns = append(columns, builder.NewFloat64Array())
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			varcharData := make([]string, 0)
			builder := array.NewStringBuilder(mem)
			for i := 0; i < rows; i++ {
				varcharData = append(varcharData, randomString(10))
			}
			insertData.Data[field.GetFieldID()] = &storage.StringFieldData{Data: varcharData}
			builder.AppendValues(varcharData, nil)
			columns = append(columns, builder.NewStringArray())
		case schemapb.DataType_FloatVector:
			floatVecData := make([]float32, 0)
			builder := array.NewListBuilder(mem, &arrow.Float32Type{})
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			for i := 0; i < dim*rows; i++ {
				floatVecData = append(floatVecData, float32(i))
			}
			builder.ValueBuilder().(*array.Float32Builder).AppendValues(floatVecData, nil)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(i*dim))
				valid = append(valid, true)
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatVectorFieldData{Data: floatVecData, Dim: dim}
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_Float16Vector:
			float16VecData := make([]byte, 0)
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			rowBytes := dim * 2
			for i := 0; i < rowBytes*rows; i++ {
				float16VecData = append(float16VecData, uint8(i%256))
			}
			builder.ValueBuilder().(*array.Uint8Builder).AppendValues(float16VecData, nil)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(rowBytes*i))
				valid = append(valid, true)
			}
			insertData.Data[field.GetFieldID()] = &storage.Float16VectorFieldData{Data: float16VecData, Dim: dim}
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_BFloat16Vector:
			bfloat16VecData := make([]byte, 0)
			builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			rowBytes := dim * 2
			for i := 0; i < rowBytes*rows; i++ {
				bfloat16VecData = append(bfloat16VecData, uint8(i%256))
			}
			builder.ValueBuilder().(*array.Uint8Builder).AppendValues(bfloat16VecData, nil)
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(rowBytes*i))
				valid = append(valid, true)
			}
			insertData.Data[field.GetFieldID()] = &storage.BFloat16VectorFieldData{Data: bfloat16VecData, Dim: dim}
			builder.AppendValues(offsets, valid)
			columns = append(columns, builder.NewListArray())
		case schemapb.DataType_BinaryVector:
			if isBinary {
				binVecData := make([][]byte, 0)
				builder := array.NewBinaryBuilder(mem, &arrow.BinaryType{})
				for i := 0; i < rows; i++ {
					element := make([]byte, dim/8)
					for j := 0; j < dim/8; j++ {
						element[j] = randomString(1)[0]
					}
					binVecData = append(binVecData, element)
				}
				builder.AppendValues(binVecData, nil)
				columns = append(columns, builder.NewBinaryArray())
				insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: lo.Flatten(binVecData), Dim: dim}
			} else {
				binVecData := make([]byte, 0)
				builder := array.NewListBuilder(mem, &arrow.Uint8Type{})
				offsets := make([]int32, 0, rows)
				valid := make([]bool, 0)
				rowBytes := dim / 8
				for i := 0; i < rowBytes*rows; i++ {
					binVecData = append(binVecData, uint8(i))
				}
				builder.ValueBuilder().(*array.Uint8Builder).AppendValues(binVecData, nil)
				for i := 0; i < rows; i++ {
					offsets = append(offsets, int32(rowBytes*i))
					valid = append(valid, true)
				}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
				insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: binVecData, Dim: dim}
			}
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			builder := array.NewStringBuilder(mem)
			for i := 0; i < rows; i++ {
				if i%4 == 0 {
					v, _ := json.Marshal(fmt.Sprintf("{\"a\": \"%s\", \"b\": %d}", randomString(3), i))
					jsonData = append(jsonData, v)
				} else if i%4 == 1 {
					v, _ := json.Marshal(i)
					jsonData = append(jsonData, v)
				} else if i%4 == 2 {
					v, _ := json.Marshal(float32(i) * 0.1)
					jsonData = append(jsonData, v)
				} else if i%4 == 3 {
					v, _ := json.Marshal(randomString(10))
					jsonData = append(jsonData, v)
				}
			}
			insertData.Data[field.GetFieldID()] = &storage.JSONFieldData{Data: jsonData}
			builder.AppendValues(lo.Map(jsonData, func(bs []byte, _ int) string {
				return string(bs)
			}), nil)
			columns = append(columns, builder.NewStringArray())
		case schemapb.DataType_Array:
			offsets := make([]int32, 0, rows)
			valid := make([]bool, 0, rows)
			index := 0
			for i := 0; i < rows; i++ {
				offsets = append(offsets, int32(index))
				valid = append(valid, true)
				index += 3
			}
			arrayData := make([]*schemapb.ScalarField, 0)
			switch elementType {
			case schemapb.DataType_Bool:
				builder := array.NewListBuilder(mem, &arrow.BooleanType{})
				valueBuilder := builder.ValueBuilder().(*array.BooleanBuilder)
				for i := 0; i < rows; i++ {
					data := []bool{i%2 == 0, i%3 == 0, i%4 == 0}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int8:
				builder := array.NewListBuilder(mem, &arrow.Int8Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int8Builder)
				for i := 0; i < rows; i++ {
					data := []int32{int32(i), int32(i + 1), int32(i + 2)}
					data2 := []int8{int8(i), int8(i + 1), int8(i + 2)}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data2, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int16:
				builder := array.NewListBuilder(mem, &arrow.Int16Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int16Builder)
				for i := 0; i < rows; i++ {
					data := []int32{int32(i), int32(i + 1), int32(i + 2)}
					data2 := []int16{int16(i), int16(i + 1), int16(i + 2)}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data2, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int32:
				builder := array.NewListBuilder(mem, &arrow.Int32Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int32Builder)
				for i := 0; i < rows; i++ {
					data := []int32{int32(i), int32(i + 1), int32(i + 2)}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Int64:
				builder := array.NewListBuilder(mem, &arrow.Int64Type{})
				valueBuilder := builder.ValueBuilder().(*array.Int64Builder)
				for i := 0; i < rows; i++ {
					data := []int32{int32(i), int32(i + 1), int32(i + 2)}
					data2 := []int64{int64(i), int64(i + 1), int64(i + 2)}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data2, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Float:
				builder := array.NewListBuilder(mem, &arrow.Float32Type{})
				valueBuilder := builder.ValueBuilder().(*array.Float32Builder)
				for i := 0; i < rows; i++ {
					data := []float32{float32(i) * 0.1, float32(i+1) * 0.1, float32(i+2) * 0.1}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_Double:
				builder := array.NewListBuilder(mem, &arrow.Float64Type{})
				valueBuilder := builder.ValueBuilder().(*array.Float64Builder)
				for i := 0; i < rows; i++ {
					data := []float64{float64(i) * 0.02, float64(i+1) * 0.02, float64(i+2) * 0.02}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			case schemapb.DataType_VarChar, schemapb.DataType_String:
				builder := array.NewListBuilder(mem, &arrow.StringType{})
				valueBuilder := builder.ValueBuilder().(*array.StringBuilder)
				for i := 0; i < rows; i++ {
					data := []string{
						randomString(5) + "-" + fmt.Sprintf("%d", i),
						randomString(5) + "-" + fmt.Sprintf("%d", i),
						randomString(5) + "-" + fmt.Sprintf("%d", i),
					}
					arrayData = append(arrayData, &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: data,
							},
						},
					})
					valueBuilder.AppendValues(data, nil)
				}
				insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
				builder.AppendValues(offsets, valid)
				columns = append(columns, builder.NewListArray())
			}
		}
	}
	return columns, insertData, nil
}

func writeParquet(w io.Writer, schema *schemapb.CollectionSchema, numRows int) (*storage.InsertData, error) {
	pqSchema, err := ConvertToArrowSchema(schema)
	if err != nil {
		return nil, err
	}
	fw, err := pqarrow.NewFileWriter(pqSchema, w, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(numRows))), pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	defer fw.Close()

	columns, insertData, err := buildArrayData(schema, numRows)
	if err != nil {
		return nil, err
	}
	recordBatch := array.NewRecord(pqSchema, columns, int64(numRows))
	err = fw.Write(recordBatch)
	if err != nil {
		return nil, err
	}

	return insertData, nil
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
	insertData, err := writeParquet(wf, schema, s.numRows)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	s.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			s.Equal(expectRows, data.RowNum())
			fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				if fieldDataType == schemapb.DataType_Array {
					s.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				} else {
					s.Equal(expect, actual)
				}
			}
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
	_, err = writeParquet(wf, schema, s.numRows)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", storage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
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

func (s *ReaderSuite) TestVector() {
	s.vecDataType = schemapb.DataType_BinaryVector
	s.run(schemapb.DataType_Int32)
	s.vecDataType = schemapb.DataType_FloatVector
	s.run(schemapb.DataType_Int32)
	s.vecDataType = schemapb.DataType_Float16Vector
	s.run(schemapb.DataType_Int32)
	s.vecDataType = schemapb.DataType_BFloat16Vector
	s.run(schemapb.DataType_Int32)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
