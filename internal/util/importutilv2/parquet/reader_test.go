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

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/nullutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

func writeParquet(w io.Writer, schema *schemapb.CollectionSchema, numRows int, nullPercent int) (*storage.InsertData, error) {
	useNullType := false
	if nullPercent == 100 {
		useNullType = true
	}
	pqSchema, err := ConvertToArrowSchema(schema, useNullType)
	if err != nil {
		return nil, err
	}
	fw, err := pqarrow.NewFileWriter(pqSchema, w, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(numRows))), pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	defer fw.Close()

	insertData, err := testutil.CreateInsertData(schema, numRows, nullPercent)
	if err != nil {
		return nil, err
	}
	columns, err := testutil.BuildArrayData(schema, insertData, useNullType)
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

func (s *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType, nullable bool, nullPercent int) {
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
				Name:        dataType.String(),
				DataType:    dataType,
				ElementType: elemType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "max_length",
						Value: "256",
					},
					{
						Key:   common.MaxCapacityKey,
						Value: "256",
					},
				},
				Nullable: nullable,
			},
		},
	}
	if dataType == schemapb.DataType_VarChar {
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
			InputFieldNames:  []string{dataType.String()},
			OutputFieldIds:   []int64{103},
			OutputFieldNames: []string{"sparse"},
		})
	}

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(s.T(), err)
	insertData, err := writeParquet(wf, schema, s.numRows, nullPercent)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	s.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			s.Equal(expectRows, data.RowNum())
			fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
			elementType := typeutil.GetField(schema, fieldID).GetElementType()
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				if fieldDataType == schemapb.DataType_Array && expect != nil {
					switch elementType {
					case schemapb.DataType_Bool:
						actualArray := actual.(*schemapb.ScalarField).GetBoolData().GetData()
						s.True(slices.Equal(expect.(*schemapb.ScalarField).GetBoolData().GetData(), actualArray))
						s.LessOrEqual(len(actualArray), len(expect.(*schemapb.ScalarField).GetBoolData().GetData()), "array size %d exceeds max_size %d", len(actualArray), len(expect.(*schemapb.ScalarField).GetBoolData().GetData()))
					case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
						actualArray := actual.(*schemapb.ScalarField).GetIntData().GetData()
						s.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actualArray))
						s.LessOrEqual(len(actualArray), len(expect.(*schemapb.ScalarField).GetIntData().GetData()), "array size %d exceeds max_size %d", len(actualArray), len(expect.(*schemapb.ScalarField).GetIntData().GetData()))
					case schemapb.DataType_Float:
						actualArray := actual.(*schemapb.ScalarField).GetFloatData().GetData()
						s.True(slices.Equal(expect.(*schemapb.ScalarField).GetFloatData().GetData(), actualArray))
						s.LessOrEqual(len(actualArray), len(expect.(*schemapb.ScalarField).GetFloatData().GetData()), "array size %d exceeds max_size %d", len(actualArray), len(expect.(*schemapb.ScalarField).GetFloatData().GetData()))
					case schemapb.DataType_Double:
						actualArray := actual.(*schemapb.ScalarField).GetDoubleData().GetData()
						s.True(slices.Equal(expect.(*schemapb.ScalarField).GetDoubleData().GetData(), actualArray))
						s.LessOrEqual(len(actualArray), len(expect.(*schemapb.ScalarField).GetDoubleData().GetData()), "array size %d exceeds max_size %d", len(actualArray), len(expect.(*schemapb.ScalarField).GetDoubleData().GetData()))
					case schemapb.DataType_String:
						actualArray := actual.(*schemapb.ScalarField).GetStringData().GetData()
						s.True(slices.Equal(expect.(*schemapb.ScalarField).GetStringData().GetData(), actualArray))
						s.LessOrEqual(len(actualArray), len(expect.(*schemapb.ScalarField).GetStringData().GetData()), "array size %d exceeds max_size %d", len(actualArray), len(expect.(*schemapb.ScalarField).GetStringData().GetData()))
					default:
						s.Fail("unsupported array element type")
					}
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
	_, err = writeParquet(wf, schema, s.numRows, 50)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	s.NoError(err)

	_, err = reader.Read()
	s.Error(err)
}

func (s *ReaderSuite) runWithDefaultValue(dataType schemapb.DataType, elemType schemapb.DataType, nullable bool, nullPercent int) {
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
		},
	}
	// here always set nullable==true just for test, insertData will store validData only nullable==true
	// if expectInsertData is nulls and set default value
	// actualData will be default_value
	fieldSchema, err := testutil.CreateFieldWithDefaultValue(dataType, 102, true)
	s.NoError(err)
	schema.Fields = append(schema.Fields, fieldSchema)

	filePath := fmt.Sprintf("/tmp/test_%d_reader.parquet", rand.Int())
	defer os.Remove(filePath)
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	assert.NoError(s.T(), err)
	insertData, err := writeParquet(wf, schema, s.numRows, nullPercent)
	assert.NoError(s.T(), err)

	ctx := context.Background()
	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath("/tmp/milvus_test/test_parquet_reader/"))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(s.T(), err)
	schema.Fields[2].Nullable = nullable
	reader, err := NewReader(ctx, cm, schema, filePath, 64*1024*1024)
	s.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			s.Equal(expectRows, data.RowNum())
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				if expect == nil {
					expect, err = nullutil.GetDefaultValue(fieldSchema)
					s.NoError(err)
				}
				s.Equal(expect, actual)
			}
		}
	}

	res, err := reader.Read()
	s.NoError(err)
	checkFn(res, 0, s.numRows)
}

func (s *ReaderSuite) TestReadScalarFieldsWithDefaultValue() {
	s.runWithDefaultValue(schemapb.DataType_Bool, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_Int8, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_Int16, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_Int32, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_Int64, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_Float, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_Double, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_String, schemapb.DataType_None, true, 0)
	s.runWithDefaultValue(schemapb.DataType_VarChar, schemapb.DataType_None, true, 0)

	s.runWithDefaultValue(schemapb.DataType_Bool, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_Int8, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_Int16, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_Int32, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_Int64, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_Float, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_String, schemapb.DataType_None, true, 50)
	s.runWithDefaultValue(schemapb.DataType_VarChar, schemapb.DataType_None, true, 50)

	s.runWithDefaultValue(schemapb.DataType_Bool, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_Int8, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_Int16, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_Int32, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_Int64, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_Float, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_String, schemapb.DataType_None, true, 100)
	s.runWithDefaultValue(schemapb.DataType_VarChar, schemapb.DataType_None, true, 100)

	s.runWithDefaultValue(schemapb.DataType_Bool, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_Int8, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_Int16, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_Int64, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_Float, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_Double, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_String, schemapb.DataType_None, false, 0)
	s.runWithDefaultValue(schemapb.DataType_VarChar, schemapb.DataType_None, false, 0)

	s.runWithDefaultValue(schemapb.DataType_Bool, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_Int8, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_Int16, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_Int32, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_Int64, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_Float, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_String, schemapb.DataType_None, false, 50)
	s.runWithDefaultValue(schemapb.DataType_VarChar, schemapb.DataType_None, false, 50)

	s.runWithDefaultValue(schemapb.DataType_Bool, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_Int8, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_Int16, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_Int32, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_Int64, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_Float, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_String, schemapb.DataType_None, false, 100)
	s.runWithDefaultValue(schemapb.DataType_VarChar, schemapb.DataType_None, false, 100)
}

func (s *ReaderSuite) TestReadScalarFields() {
	s.run(schemapb.DataType_Bool, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Int8, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Int16, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Int64, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Float, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Double, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_String, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_VarChar, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_JSON, schemapb.DataType_None, false, 0)

	s.run(schemapb.DataType_Array, schemapb.DataType_Bool, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int8, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int16, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int32, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int64, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_Float, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_Double, false, 0)
	s.run(schemapb.DataType_Array, schemapb.DataType_String, false, 0)

	s.run(schemapb.DataType_Bool, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_Int8, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_Int16, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_Int64, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_Float, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_String, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_VarChar, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_JSON, schemapb.DataType_None, true, 50)

	s.run(schemapb.DataType_Array, schemapb.DataType_Bool, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int8, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int16, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int32, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int64, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_Float, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_Double, true, 50)
	s.run(schemapb.DataType_Array, schemapb.DataType_String, true, 50)

	s.run(schemapb.DataType_Bool, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_Int8, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_Int16, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_Int64, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_Float, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_String, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_VarChar, schemapb.DataType_None, true, 100)
	s.run(schemapb.DataType_JSON, schemapb.DataType_None, true, 100)

	s.run(schemapb.DataType_Array, schemapb.DataType_Bool, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int8, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int16, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int32, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_Int64, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_Float, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_Double, true, 100)
	s.run(schemapb.DataType_Array, schemapb.DataType_String, true, 100)

	s.failRun(schemapb.DataType_JSON, true)
}

func (s *ReaderSuite) TestStringPK() {
	s.pkDataType = schemapb.DataType_VarChar
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, true, 50)
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, true, 100)
}

func (s *ReaderSuite) TestVector() {
	s.vecDataType = schemapb.DataType_BinaryVector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.vecDataType = schemapb.DataType_FloatVector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.vecDataType = schemapb.DataType_Float16Vector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.vecDataType = schemapb.DataType_BFloat16Vector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.vecDataType = schemapb.DataType_SparseFloatVector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
	s.vecDataType = schemapb.DataType_Int8Vector
	s.run(schemapb.DataType_Int32, schemapb.DataType_None, false, 0)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
