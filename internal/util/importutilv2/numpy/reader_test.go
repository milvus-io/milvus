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
	"fmt"
	"io"
	"math"
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
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	if dt == schemapb.DataType_VarChar {
		// Add a BM25 function if data type is VarChar
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:          103,
			Name:             "sparse",
			DataType:         schemapb.DataType_SparseFloatVector,
			IsFunctionOutput: true,
		})
		schema.Functions = append(schema.Functions, &schemapb.FunctionSchema{
			Id:               1000,
			Name:             "bm25",
			Type:             schemapb.FunctionType_BM25,
			InputFieldIds:    []int64{102},
			InputFieldNames:  []string{dt.String()},
			OutputFieldIds:   []int64{103},
			OutputFieldNames: []string{"sparse"},
		})
	}

	insertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	files := make(map[int64]string)
	for _, field := range schema.GetFields() {
		if field.GetIsFunctionOutput() {
			continue
		}
		files[field.GetFieldID()] = fmt.Sprintf("%s.npy", field.GetName())
	}

	cm := mocks.NewChunkManager(suite.T())
	type mockReader struct {
		io.Reader
		io.Closer
		io.ReaderAt
		io.Seeker
	}

	var data interface{}
	for fieldID, fieldData := range insertData.Data {
		dataType := fieldIDToField[fieldID].GetDataType()
		rowNum := fieldData.RowNum()
		switch dataType {
		case schemapb.DataType_JSON:
			jsonStrs := make([]string, 0, rowNum)
			for i := 0; i < rowNum; i++ {
				row := fieldData.GetRow(i)
				jsonStrs = append(jsonStrs, string(row.([]byte)))
			}
			data = jsonStrs
		case schemapb.DataType_BinaryVector:
			rows := fieldData.GetDataRows().([]byte)
			const rowBytes = dim / 8
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_FloatVector:
			rows := fieldData.GetDataRows().([]float32)
			chunked := lo.Chunk(rows, dim)
			chunkedRows := make([][dim]float32, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			rows := fieldData.GetDataRows().([]byte)
			const rowBytes = dim * 2
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		default:
			data = fieldData.GetDataRows()
		}

		reader, err := CreateReader(data)
		suite.NoError(err)
		cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
			Reader: reader,
		}, nil)
	}

	reader, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
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
	insertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)
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

	var data interface{}
	for fieldID, fieldData := range insertData.Data {
		dataType := fieldIDToField[fieldID].GetDataType()
		rowNum := fieldData.RowNum()
		switch dataType {
		case schemapb.DataType_JSON:
			jsonStrs := make([]string, 0, rowNum)
			for i := 0; i < rowNum; i++ {
				row := fieldData.GetRow(i)
				jsonStrs = append(jsonStrs, string(row.([]byte)))
			}
			data = jsonStrs
		case schemapb.DataType_BinaryVector:
			rows := fieldData.GetDataRows().([]byte)
			const rowBytes = dim / 8
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_FloatVector:
			rows := fieldData.GetDataRows().([]float32)
			chunked := lo.Chunk(rows, dim)
			chunkedRows := make([][dim]float32, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			rows := fieldData.GetDataRows().([]byte)
			const rowBytes = dim * 2
			chunked := lo.Chunk(rows, rowBytes)
			chunkedRows := make([][rowBytes]byte, len(chunked))
			for i, innerSlice := range chunked {
				copy(chunkedRows[i][:], innerSlice)
			}
			data = chunkedRows
		default:
			data = fieldData.GetDataRows()
		}

		reader, err := CreateReader(data)
		suite.NoError(err)
		cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(&mockReader{
			Reader: reader,
		}, nil)
	}

	reader, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
	suite.NoError(err)

	_, err = reader.Read()
	suite.Error(err)
}

func (suite *ReaderSuite) failRunNullable(dt schemapb.DataType, nullable bool) {
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
				Nullable: nullable,
			},
		},
	}
	files := make(map[int64]string)
	for _, field := range schema.GetFields() {
		files[field.GetFieldID()] = fmt.Sprintf("%s.npy", field.GetName())
	}

	cm := mocks.NewChunkManager(suite.T())
	_, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
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
	suite.failRunNullable(schemapb.DataType_Bool, true)
	suite.failRunNullable(schemapb.DataType_Int8, true)
	suite.failRunNullable(schemapb.DataType_Int16, true)
	suite.failRunNullable(schemapb.DataType_Int32, true)
	suite.failRunNullable(schemapb.DataType_Int64, true)
	suite.failRunNullable(schemapb.DataType_Float, true)
	suite.failRunNullable(schemapb.DataType_Double, true)
	suite.failRunNullable(schemapb.DataType_VarChar, true)
	suite.failRunNullable(schemapb.DataType_JSON, true)
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
	// suite.vecDataType = schemapb.DataType_SparseFloatVector
	// suite.run(schemapb.DataType_Int32)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

func TestCreateReaders(t *testing.T) {
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(nil, nil)

	// normal
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "vec", DataType: schemapb.DataType_FloatVector},
			{Name: "json", DataType: schemapb.DataType_JSON},
		},
	}
	_, err := CreateReaders(ctx, cm, schema, []string{"pk", "vec", "json"})
	assert.NoError(t, err)

	// auto id
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "vec", DataType: schemapb.DataType_FloatVector},
			{Name: "json", DataType: schemapb.DataType_JSON},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"pk", "vec", "json"})
	assert.Error(t, err)

	// $meta
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, AutoID: true},
			{Name: "vec", DataType: schemapb.DataType_FloatVector},
			{Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"pk", "vec"})
	assert.NoError(t, err)

	// auto id and Function
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "vec", DataType: schemapb.DataType_FloatVector},
			{Name: "text", DataType: schemapb.DataType_VarChar},
			{Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25", InputFieldNames: []string{"text"}, OutputFieldNames: []string{"sparse"}},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"vec", "text"})
	assert.NoError(t, err)
}
