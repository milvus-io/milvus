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

package numpy

import (
	"bytes"
	"context"
	rand2 "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
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

func (suite *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *ReaderSuite) SetupTest() {
	// default suite params
	suite.numRows = 100
	suite.pkDataType = schemapb.DataType_Int64
	suite.vecDataType = schemapb.DataType_FloatVector
}

func createInsertData(t *testing.T, schema *schemapb.CollectionSchema, rowCount int) *storage.InsertData {
	insertData, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	for _, field := range schema.GetFields() {
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rowCount; i++ {
				boolData = append(boolData, i%3 != 0)
			}
			insertData.Data[field.GetFieldID()] = &storage.BoolFieldData{Data: boolData}
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rowCount; i++ {
				floatData = append(floatData, float32(i/2))
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatFieldData{Data: floatData}
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rowCount; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			insertData.Data[field.GetFieldID()] = &storage.DoubleFieldData{Data: doubleData}
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rowCount; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int8FieldData{Data: int8Data}
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rowCount; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int16FieldData{Data: int16Data}
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rowCount; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int32FieldData{Data: int32Data}
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rowCount; i++ {
				int64Data = append(int64Data, int64(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int64FieldData{Data: int64Data}
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			binVecData := make([]byte, 0)
			total := rowCount * int(dim) / 8
			for i := 0; i < total; i++ {
				binVecData = append(binVecData, byte(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: binVecData, Dim: int(dim)}
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			floatVecData := make([]float32, 0)
			total := rowCount * int(dim)
			for i := 0; i < total; i++ {
				floatVecData = append(floatVecData, rand.Float32())
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatVectorFieldData{Data: floatVecData, Dim: int(dim)}
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			total := int64(rowCount) * dim * 2
			float16VecData := make([]byte, total)
			_, err = rand2.Read(float16VecData)
			assert.NoError(t, err)
			insertData.Data[field.GetFieldID()] = &storage.Float16VectorFieldData{Data: float16VecData, Dim: int(dim)}
		case schemapb.DataType_BFloat16Vector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			total := int64(rowCount) * dim * 2
			bfloat16VecData := make([]byte, total)
			_, err = rand2.Read(bfloat16VecData)
			assert.NoError(t, err)
			insertData.Data[field.GetFieldID()] = &storage.BFloat16VectorFieldData{Data: bfloat16VecData, Dim: int(dim)}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rowCount; i++ {
				varcharData = append(varcharData, strconv.Itoa(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.StringFieldData{Data: varcharData}
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				if i%4 == 0 {
					v, _ := json.Marshal("{\"a\": \"%s\", \"b\": %d}")
					jsonData = append(jsonData, v)
				} else if i%4 == 1 {
					v, _ := json.Marshal(i)
					jsonData = append(jsonData, v)
				} else if i%4 == 2 {
					v, _ := json.Marshal(float32(i) * 0.1)
					jsonData = append(jsonData, v)
				} else if i%4 == 3 {
					v, _ := json.Marshal(strconv.Itoa(i))
					jsonData = append(jsonData, v)
				}
			}
			insertData.Data[field.GetFieldID()] = &storage.JSONFieldData{Data: jsonData}
		case schemapb.DataType_Array:
			arrayData := make([]*schemapb.ScalarField, 0)
			for i := 0; i < rowCount; i++ {
				arrayData = append(arrayData, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(i), int32(i + 1), int32(i + 2)},
						},
					},
				})
			}
			insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
		default:
			panic(fmt.Sprintf("unexpected data type: %s", field.GetDataType().String()))
		}
	}
	return insertData
}

func CreateReader(data interface{}) (io.Reader, error) {
	buf := new(bytes.Buffer)
	err := npyio.Write(buf, data)
	if err != nil {
		return nil, err
	}
	return strings.NewReader(buf.String()), nil
}

func (suite *ReaderSuite) run(dt schemapb.DataType) {
	const dim = 8
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
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
				DataType: suite.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: fmt.Sprintf("%d", dim),
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
	insertData := createInsertData(suite.T(), schema, suite.numRows)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	files := make(map[int64]string)
	for _, field := range schema.GetFields() {
		files[field.GetFieldID()] = fmt.Sprintf("%s.npy", field.GetName())
	}

	cm := mocks.NewChunkManager(suite.T())
	type mockReader struct {
		io.Reader
		io.Closer
		io.ReaderAt
		io.Seeker
	}
	for fieldID, fieldData := range insertData.Data {
		dataType := fieldIDToField[fieldID].GetDataType()
		if dataType == schemapb.DataType_JSON {
			jsonStrs := make([]string, 0, fieldData.RowNum())
			for i := 0; i < fieldData.RowNum(); i++ {
				row := fieldData.GetRow(i)
				jsonStrs = append(jsonStrs, string(row.([]byte)))
			}
			reader, err := CreateReader(jsonStrs)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else if dataType == schemapb.DataType_FloatVector {
			chunked := lo.Chunk(insertData.Data[fieldID].GetRows().([]float32), dim)
			chunkedRows := make([][dim]float32, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice[:])
			}
			reader, err := CreateReader(chunkedRows)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else if dataType == schemapb.DataType_Float16Vector || dataType == schemapb.DataType_BFloat16Vector {
			chunked := lo.Chunk(insertData.Data[fieldID].GetRows().([]byte), dim*2)
			chunkedRows := make([][dim * 2]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice[:])
			}
			reader, err := CreateReader(chunkedRows)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else if dataType == schemapb.DataType_BinaryVector {
			chunked := lo.Chunk(insertData.Data[fieldID].GetRows().([]byte), dim/8)
			chunkedRows := make([][dim / 8]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice[:])
			}
			reader, err := CreateReader(chunkedRows)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else {
			reader, err := CreateReader(insertData.Data[fieldID].GetRows())
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		}
	}

	reader, err := NewReader(context.Background(), schema, lo.Values(files), cm, math.MaxInt)
	suite.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			suite.Equal(expectRows, data.RowNum())
			fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				if fieldDataType == schemapb.DataType_Array {
					suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				} else {
					suite.Equal(expect, actual)
				}
			}
		}
	}

	res, err := reader.Read()
	suite.NoError(err)
	checkFn(res, 0, suite.numRows)
}

func (suite *ReaderSuite) failRun(dt schemapb.DataType, isDynamic bool) {
	const dim = 8
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
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
				DataType: suite.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: fmt.Sprintf("%d", dim),
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
	insertData := createInsertData(suite.T(), schema, suite.numRows)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	files := make(map[int64]string)
	for _, field := range schema.GetFields() {
		files[field.GetFieldID()] = fmt.Sprintf("%s.npy", field.GetName())
	}

	cm := mocks.NewChunkManager(suite.T())
	type mockReader struct {
		io.Reader
		io.Closer
		io.ReaderAt
		io.Seeker
	}
	for fieldID, fieldData := range insertData.Data {
		dataType := fieldIDToField[fieldID].GetDataType()
		if dataType == schemapb.DataType_JSON {
			jsonStrs := make([]string, 0, fieldData.RowNum())
			for i := 0; i < fieldData.RowNum(); i++ {
				row := fieldData.GetRow(i)
				jsonStrs = append(jsonStrs, string(row.([]byte)))
			}
			reader, err := CreateReader(jsonStrs)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else if dataType == schemapb.DataType_FloatVector {
			chunked := lo.Chunk(insertData.Data[fieldID].GetRows().([]float32), dim)
			chunkedRows := make([][dim]float32, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice[:])
			}
			reader, err := CreateReader(chunkedRows)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else if dataType == schemapb.DataType_Float16Vector || dataType == schemapb.DataType_BFloat16Vector {
			chunked := lo.Chunk(insertData.Data[fieldID].GetRows().([]byte), dim*2)
			chunkedRows := make([][dim * 2]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice[:])
			}
			reader, err := CreateReader(chunkedRows)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else if dataType == schemapb.DataType_BinaryVector {
			chunked := lo.Chunk(insertData.Data[fieldID].GetRows().([]byte), dim/8)
			chunkedRows := make([][dim / 8]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice[:])
			}
			reader, err := CreateReader(chunkedRows)
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		} else {
			reader, err := CreateReader(insertData.Data[fieldID].GetRows())
			suite.NoError(err)
			cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
				Reader: reader,
			}, nil)
		}
	}

	reader, err := NewReader(context.Background(), schema, lo.Values(files), cm, math.MaxInt)
	suite.NoError(err)

	_, err = reader.Read()
	suite.Error(err)
}

func (suite *ReaderSuite) TestReadScalarFields() {
	suite.run(schemapb.DataType_Bool)
	suite.run(schemapb.DataType_Int8)
	suite.run(schemapb.DataType_Int16)
	suite.run(schemapb.DataType_Int32)
	suite.run(schemapb.DataType_Int64)
	suite.run(schemapb.DataType_Float)
	suite.run(schemapb.DataType_Double)
	suite.run(schemapb.DataType_VarChar)
	suite.run(schemapb.DataType_JSON)
	suite.failRun(schemapb.DataType_JSON, true)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.run(schemapb.DataType_Int32)
}

func (suite *ReaderSuite) TestVector() {
	suite.vecDataType = schemapb.DataType_BinaryVector
	suite.run(schemapb.DataType_Int32)
	suite.vecDataType = schemapb.DataType_FloatVector
	suite.run(schemapb.DataType_Int32)
	suite.vecDataType = schemapb.DataType_Float16Vector
	suite.run(schemapb.DataType_Int32)
	suite.vecDataType = schemapb.DataType_BFloat16Vector
	suite.run(schemapb.DataType_Int32)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
