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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	dim = 8
)

type ReaderSuite struct {
	suite.Suite

	numRows     int
	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType
}

func (suite *ReaderSuite) SetupTest() {
	// default suite params
	suite.numRows = 100
	suite.pkDataType = schemapb.DataType_Int64
	suite.vecDataType = schemapb.DataType_FloatVector
}

func createReader(fieldData storage.FieldData, dataType schemapb.DataType) (io.Reader, error) {
	var data interface{}
	rowNum := fieldData.RowNum()
	switch dataType {
	case schemapb.DataType_JSON:
		jsonStrs := make([]string, 0, rowNum)
		for i := 0; i < rowNum; i++ {
			row := fieldData.GetRow(i)
			jsonStrs = append(jsonStrs, string(row.([]byte)))
		}
		data = jsonStrs
	case schemapb.DataType_Geometry:
		geoStrs := make([]string, 0, rowNum)
		for i := 0; i < rowNum; i++ {
			row := fieldData.GetRow(i)
			geoStrs = append(geoStrs, string(row.([]byte)))
		}
		data = geoStrs
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
	case schemapb.DataType_Int8Vector:
		rows := fieldData.GetDataRows().([]int8)
		chunked := lo.Chunk(rows, dim)
		chunkedRows := make([][dim]int8, len(chunked))
		for i, innerSlice := range chunked {
			copy(chunkedRows[i][:], innerSlice)
		}
		data = chunkedRows
	default:
		data = fieldData.GetDataRows()
	}

	buf := new(bytes.Buffer)
	err := npyio.Write(buf, data)
	if err != nil {
		return nil, err
	}
	return strings.NewReader(buf.String()), nil
}

func (suite *ReaderSuite) run(dt schemapb.DataType) {
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
	for fieldID, fieldData := range insertData.Data {
		dataType := fieldIDToField[fieldID].GetDataType()
		reader, err := createReader(fieldData, dataType)
		suite.NoError(err)
		cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(importcommon.CustomMockReader(reader), nil)
		cm.EXPECT().Size(mock.Anything, files[fieldID]).Return(128, nil)
	}

	reader, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
	suite.NoError(err)
	suite.NotNil(reader)
	defer reader.Close()

	size, err := reader.Size()
	suite.NoError(err)
	suite.Equal(int64(128*len(files)), size)
	size2, err := reader.Size() // size is cached
	suite.NoError(err)
	suite.Equal(size, size2)

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
	for fieldID, fieldData := range insertData.Data {
		dataType := fieldIDToField[fieldID].GetDataType()
		reader, err := createReader(fieldData, dataType)
		suite.NoError(err)
		cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(importcommon.CustomMockReader(reader), nil)
	}

	reader, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
	suite.NoError(err)

	_, err = reader.Read()
	suite.Error(err)
}

func (suite *ReaderSuite) runNullable(dt schemapb.DataType, hasFile bool) {
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
				Nullable: true,
			},
		},
	}

	insertData, err := testutil.CreateInsertData(schema, suite.numRows, 0)
	suite.NoError(err)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	files := make(map[int64]string)
	for _, field := range schema.GetFields() {
		if field.GetNullable() && !hasFile {
			continue
		}
		files[field.GetFieldID()] = fmt.Sprintf("%s.npy", field.GetName())
	}

	cm := mocks.NewChunkManager(suite.T())

	for fieldID, fieldData := range insertData.Data {
		if fieldIDToField[fieldID].GetNullable() && !hasFile {
			continue
		}

		dataType := fieldIDToField[fieldID].GetDataType()
		reader, err := createReader(fieldData, dataType)
		suite.NoError(err)
		cm.EXPECT().Reader(mock.Anything, files[fieldID]).Return(importcommon.CustomMockReader(reader), nil)
	}

	reader, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
	suite.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			if data.GetNullable() {
				continue
			}
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

func (suite *ReaderSuite) TestReadScalarFields() {
	dataTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
		schemapb.DataType_JSON,
	}

	for _, dataType := range dataTypes {
		suite.run(dataType)

		suite.runNullable(dataType, true)
		suite.runNullable(dataType, false)
	}

	suite.failRun(schemapb.DataType_JSON, true)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.run(schemapb.DataType_Int32)
}

func (suite *ReaderSuite) TestVector() {
	dataTypes := []schemapb.DataType{
		schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
	}

	for _, dataType := range dataTypes {
		suite.vecDataType = dataType
		suite.run(schemapb.DataType_Int32)
	}
}

func (suite *ReaderSuite) TestReadRecoversFromPrematureEOF() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    101,
				Name:       "varchar",
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}},
			},
			{
				FieldID:    102,
				Name:       "vec",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)}},
			},
		},
	}
	insertData, err := testutil.CreateInsertData(schema, 3)
	suite.NoError(err)
	files := map[int64]string{
		100: "pk.npy",
		101: "varchar.npy",
		102: "vec.npy",
	}
	contents := make(map[string]string)
	for _, field := range schema.GetFields() {
		npyReader, err := createReader(insertData.Data[field.GetFieldID()], field.GetDataType())
		suite.NoError(err)
		contentBytes, err := io.ReadAll(npyReader)
		suite.NoError(err)
		contents[files[field.GetFieldID()]] = string(contentBytes)
	}
	openCount := make(map[string]int)

	cm := mocks.NewChunkManager(suite.T())
	for _, path := range files {
		path := path
		cm.EXPECT().Reader(mock.Anything, path).RunAndReturn(func(ctx context.Context, path string) (storage.FileReader, error) {
			openCount[path]++
			if openCount[path] == 1 {
				return importcommon.NewPrematureEOFReader(contents[path], len(contents[path])-2), nil
			}
			return importcommon.NewMockReader(contents[path]), nil
		})
	}

	reader, err := NewReader(context.Background(), cm, schema, lo.Values(files), math.MaxInt)
	suite.NoError(err)
	defer reader.Close()

	data, err := reader.Read()
	suite.NoError(err)
	suite.Equal(3, data.GetRowNum())
	for _, field := range schema.GetFields() {
		for i := 0; i < data.GetRowNum(); i++ {
			suite.Equal(insertData.Data[field.GetFieldID()].GetRow(i), data.Data[field.GetFieldID()].GetRow(i))
		}
		suite.GreaterOrEqual(openCount[files[field.GetFieldID()]], 2)
	}
}

func TestNumpyCreateReaders(t *testing.T) {
	ctx := context.Background()
	// CreateReaders now validates the header of every source it opens, so the
	// mock must serve a well-formed .npy prefix; the fields differ but the
	// prefix check is schema-independent.
	buf := new(bytes.Buffer)
	assert.NoError(t, npyio.Write(buf, []int64{1}))
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(importcommon.NewMockReader(buf.String()), nil)

	// normal case
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 3, Name: "json", DataType: schemapb.DataType_JSON},
		},
	}
	_, err := CreateReaders(ctx, cm, schema, []string{"pk", "vec", "json"})
	assert.NoError(t, err)

	// auto id should mot be provided
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 3, Name: "json", DataType: schemapb.DataType_JSON},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"pk", "vec", "json"})
	assert.Error(t, err)

	// $meta can be ignored or provided
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, AutoID: true},
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 3, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"pk", "vec"})
	assert.NoError(t, err)

	_, err = CreateReaders(ctx, cm, schema, []string{"pk", "vec", "$meta"})
	assert.NoError(t, err)

	// function output field should not be provided
	// redundant files can be ignored
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector},
			{FieldID: 3, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 4, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}, OutputFieldNames: []string{"sparse"}},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"vec", "text"})
	assert.NoError(t, err)

	_, err = CreateReaders(ctx, cm, schema, []string{"vec", "text", "dummy"})
	assert.NoError(t, err)

	_, err = CreateReaders(ctx, cm, schema, []string{"pk", "vec", "text"})
	assert.Error(t, err)

	// BM25 function output field is always rejected
	_, err = CreateReaders(ctx, cm, schema, []string{"vec", "text", "sparse"})
	assert.Error(t, err)

	_, err = CreateReaders(ctx, cm, schema, []string{"text"})
	assert.Error(t, err)

	// not support array and sparse
	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 2, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"sparse"})
	assert.Error(t, err)
	_, err = CreateReaders(ctx, cm, schema, []string{})
	assert.Error(t, err)

	schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{FieldID: 2, Name: "array", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
		},
	}
	_, err = CreateReaders(ctx, cm, schema, []string{"array"})
	assert.Error(t, err)
}

func TestNumpyCreateReadersError(t *testing.T) {
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, "pk").Return(nil, merr.WrapErrImportFailed("read error"))

	// read error
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	_, err := CreateReaders(ctx, cm, schema, []string{"pk"})
	assert.Error(t, err)

	_, err = NewReader(ctx, cm, schema, []string{"pk"}, 100)
	assert.Error(t, err)
}

func TestNumpyReader(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

// numpyStructSchema has a pk, a vector and a struct with an element-nullable
// scalar, a nested Array and an element-nullable vector sub-field.
func numpyStructSchema(nullable bool) *schemapb.CollectionSchema {
	capacity := &commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "4"}
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)}},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 102, Name: "s", Nullable: nullable,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 103, Name: "s[arr]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					Nullable: nullable, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{capacity},
				},
				{
					FieldID: 104, Name: "s[tags]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
					Nullable: nullable, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{capacity},
					TypeSchema: &schemapb.TypeSchema{
						Nullable:   true,
						TypeParams: []*commonpb.KeyValuePair{capacity},
						Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
							Nullable:   true,
							TypeParams: []*commonpb.KeyValuePair{capacity},
							Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}},
						}},
					},
				},
				{
					FieldID: 105, Name: "s[nvec]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
					Nullable: nullable, ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{capacity, {Key: common.DimKey, Value: fmt.Sprintf("%d", dim)}},
				},
			},
		}},
	}
}

func TestNumpyRejectsNonNullableStruct(t *testing.T) {
	ctx := context.Background()
	// The mock has no expectations: the schema check must fail before any file
	// is opened.
	cm := mocks.NewChunkManager(t)
	schema := numpyStructSchema(false)
	paths := []string{"pk.npy", "vec.npy"}
	wantMsg := "numpy import cannot fill struct field 's', which is not nullable"

	err := ValidateSchema(schema, paths)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.Contains(t, err.Error(), wantMsg)

	_, err = NewReader(ctx, cm, schema, paths, 100)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.Contains(t, err.Error(), wantMsg)

	_, _, _, err = NumRowsAndBytes(ctx, cm, schema, paths)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.Contains(t, err.Error(), wantMsg)

	assert.NoError(t, ValidateSchema(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	}, paths))
}

func TestNumpyRejectsStructFileNames(t *testing.T) {
	ctx := context.Background()
	schema := numpyStructSchema(true)
	// The stored sub-field name, the sub-field's own name and the struct name
	// all name struct data, with or without a directory.
	for _, path := range []string{"s.npy", "dir/s[arr].npy", "arr.npy", "s[nvec].npy", "dir/tags.npy"} {
		t.Run(path, func(t *testing.T) {
			cm := mocks.NewChunkManager(t)
			paths := []string{"pk.npy", "vec.npy", path}
			wantMsg := fmt.Sprintf("numpy import does not support struct data: file '%s' names struct field 's'", path)

			err := ValidateSchema(schema, paths)
			assert.ErrorIs(t, err, merr.ErrImportFailed)
			assert.Equal(t, merr.InputError, merr.GetErrorType(err))
			assert.Contains(t, err.Error(), wantMsg)

			_, err = NewReader(ctx, cm, schema, paths, 100)
			assert.ErrorIs(t, err, merr.ErrImportFailed)
			assert.Contains(t, err.Error(), wantMsg)

			_, _, _, err = NumRowsAndBytes(ctx, cm, schema, paths)
			assert.ErrorIs(t, err, merr.ErrImportFailed)
			assert.Contains(t, err.Error(), wantMsg)
		})
	}

	// A sub-field's own name that is also a top-level field belongs to that
	// field.
	shadowed := numpyStructSchema(true)
	shadowed.Fields = append(shadowed.Fields, &schemapb.FieldSchema{FieldID: 106, Name: "arr", DataType: schemapb.DataType_Int64, Nullable: true})
	assert.NoError(t, ValidateSchema(shadowed, []string{"pk.npy", "vec.npy", "arr.npy"}))
}

// TestNumpyNullableStructImport reads NumPy files into a collection with a
// nullable struct: the top-level fields are read and the struct sub-fields are
// left without rows, for the DataNode to fill with null struct rows
// (AppendNullableDefaultFieldsData).
func TestNumpyNullableStructImport(t *testing.T) {
	ctx := context.Background()
	schema := numpyStructSchema(true)
	const rows = 5
	pks := make([]int64, rows)
	vecs := make([]float32, rows*dim)
	for i := range pks {
		pks[i] = int64(i + 1)
		for d := 0; d < dim; d++ {
			vecs[i*dim+d] = float32(i*dim + d)
		}
	}
	pkData := &storage.Int64FieldData{Data: pks}
	vecData := &storage.FloatVectorFieldData{Data: vecs, Dim: dim}

	cm := mocks.NewChunkManager(t)
	files := map[string]storage.FieldData{"pk.npy": pkData, "vec.npy": vecData}
	types := map[string]schemapb.DataType{"pk.npy": schemapb.DataType_Int64, "vec.npy": schemapb.DataType_FloatVector}
	for path, fieldData := range files {
		r, err := createReader(fieldData, types[path])
		assert.NoError(t, err)
		cm.EXPECT().Reader(mock.Anything, path).Return(importcommon.CustomMockReader(r), nil)
		cm.EXPECT().Size(mock.Anything, path).Return(128, nil).Maybe()
	}

	assert.NoError(t, ValidateSchema(schema, lo.Keys(files)))
	reader, err := NewReader(ctx, cm, schema, lo.Keys(files), math.MaxInt)
	assert.NoError(t, err)
	defer reader.Close()

	data, err := reader.Read()
	assert.NoError(t, err)
	assert.Equal(t, pks, data.Data[100].GetDataRows())
	assert.Equal(t, vecs, data.Data[101].GetDataRows())
	for _, id := range []int64{103, 104, 105} {
		assert.Equal(t, 0, data.Data[id].RowNum(), "sub-field %d", id)
	}
	_, err = reader.Read()
	assert.ErrorIs(t, err, io.EOF)
}
