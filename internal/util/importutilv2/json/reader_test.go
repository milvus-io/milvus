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

package json

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/nullutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type mockReader struct {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Seeker
	size int64
}

func (mr *mockReader) Size() (int64, error) {
	return mr.size, nil
}

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

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType, nullable bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
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
						Key:   common.MaxLengthKey,
						Value: "128",
					},
					{
						Key:   common.MaxCapacityKey,
						Value: "128",
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

	insertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)

	rows, err := testutil.CreateInsertDataRowsForJSON(schema, insertData)
	suite.NoError(err)

	jsonBytes, err := json.Marshal(rows)
	suite.NoError(err)

	cm := mocks.NewChunkManager(suite.T())
	cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (storage.FileReader, error) {
		reader := strings.NewReader(string(jsonBytes))
		r := &mockReader{
			Reader: reader,
			Closer: io.NopCloser(reader),
		}
		return r, nil
	})
	cm.EXPECT().Size(mock.Anything, "mockPath").Return(128, nil)
	reader, err := NewReader(context.Background(), cm, schema, "mockPath", math.MaxInt)
	suite.NoError(err)
	suite.NotNil(reader)
	defer reader.Close()

	size, err := reader.Size()
	suite.NoError(err)
	suite.Equal(int64(128), size)
	size2, err := reader.Size() // size is cached
	suite.NoError(err)
	suite.Equal(size, size2)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			suite.Equal(expectRows, data.RowNum())
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				suite.Equal(expect, actual)
			}
		}
	}

	res, err := reader.Read()
	suite.NoError(err)
	checkFn(res, 0, suite.numRows)
}

func (suite *ReaderSuite) runWithDefaultValue(dataType schemapb.DataType, elemType schemapb.DataType, oldFormat bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
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
						Value: "8",
					},
				},
			},
		},
	}
	// here always set nullable==true just for test, insertData will store validData only nullable==true
	// jsonBytes Marshal from rows, if expectInsertData is nulls and set default value
	// actualData will be default_value
	fieldSchema, err := testutil.CreateFieldWithDefaultValue(dataType, 102, true)
	suite.NoError(err)
	schema.Fields = append(schema.Fields, fieldSchema)

	insertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)

	rows, err := testutil.CreateInsertDataRowsForJSON(schema, insertData)
	suite.NoError(err)

	var jsonBytes []byte
	if oldFormat {
		oldRows := make(map[string]any)
		oldRows["rows"] = rows
		jsonBytes, err = json.Marshal(oldRows)
		suite.NoError(err)
	} else {
		jsonBytes, err = json.Marshal(rows)
		suite.NoError(err)
	}

	cm := mocks.NewChunkManager(suite.T())
	cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (storage.FileReader, error) {
		r := &mockReader{Reader: strings.NewReader(string(jsonBytes))}
		return r, nil
	})
	reader, err := NewReader(context.Background(), cm, schema, "mockPath", math.MaxInt)
	suite.NoError(err)

	checkFn := func(actualInsertData *storage.InsertData, offsetBegin, expectRows int) {
		expectInsertData := insertData
		for fieldID, data := range actualInsertData.Data {
			suite.Equal(expectRows, data.RowNum())
			for i := 0; i < expectRows; i++ {
				expect := expectInsertData.Data[fieldID].GetRow(i + offsetBegin)
				actual := data.GetRow(i)
				if expect == nil {
					expect, err = nullutil.GetDefaultValue(fieldSchema)
					suite.NoError(err)
				}
				suite.Equal(expect, actual)
			}
		}
	}
	res, err := reader.Read()
	suite.NoError(err)
	checkFn(res, 0, suite.numRows)
}

func (suite *ReaderSuite) TestReadScalarFields() {
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
	scalarTypes := append(elementTypes, []schemapb.DataType{schemapb.DataType_JSON, schemapb.DataType_Array}...)

	for _, dataType := range scalarTypes {
		if dataType == schemapb.DataType_Array {
			for _, elementType := range elementTypes {
				suite.run(dataType, elementType, false)
				suite.run(dataType, elementType, true)
			}
		} else {
			suite.run(dataType, schemapb.DataType_None, false)
			suite.run(dataType, schemapb.DataType_None, true)
		}
	}
}

func (suite *ReaderSuite) TestReadScalarFieldsWithDefaultValue() {
	scalarTypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar,
	}

	for _, dataType := range scalarTypes {
		suite.runWithDefaultValue(dataType, schemapb.DataType_None, false)
		suite.runWithDefaultValue(dataType, schemapb.DataType_None, true)
	}
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestVector() {
	suite.vecDataType = schemapb.DataType_BinaryVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_FloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_Float16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_BFloat16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_SparseFloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_Int8Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestDecodeError() {
	testDecode := func(jsonContent string, ioErr error, initErr bool, decodeErr bool) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}

		cm := mocks.NewChunkManager(suite.T())
		cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (storage.FileReader, error) {
			r := &mockReader{Reader: strings.NewReader(jsonContent)}
			return r, ioErr
		})
		reader, err := NewReader(context.Background(), cm, schema, "mockPath", math.MaxInt)
		if initErr {
			suite.Error(err)
		} else {
			suite.NoError(err)
		}

		if err == nil {
			_, err = reader.Read()
			if decodeErr {
				suite.Error(err)
			} else {
				suite.NoError(err)
			}
		}
	}

	testDecode("", merr.WrapErrImportFailed("error"), true, true)
	testDecode("", nil, true, true)
	testDecode("a", nil, true, true)
	testDecode("2", nil, true, true)
	testDecode("{", nil, false, true)
	testDecode("{a", nil, false, true)
	testDecode("{\"a\":2}", nil, false, true)
	testDecode("{\"rows\"a}", nil, false, true)
	testDecode("{\"rows\":{}}", nil, false, true)
	testDecode("{\"rows\":[]}", nil, false, false)
	testDecode("{\"rows\":[a]}", nil, false, true)
	testDecode("{\"rows\":[{\"dummy\": 3}]}", nil, false, true)
	testDecode("{\"rows\":[{\"pk\": 3}]}", nil, false, false)
	testDecode("{\"rows\":[{\"pk\": 3}", nil, false, true)
}

func (suite *ReaderSuite) TestReadCount() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	rows := make([]any, 0, 100)
	for i := 0; i < 100; i++ {
		row := make(map[string]any)
		row["pk"] = int64(i)
		rows = append(rows, row)
	}
	jsonBytes, err := json.Marshal(rows)
	suite.NoError(err)

	cm := mocks.NewChunkManager(suite.T())
	cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (storage.FileReader, error) {
		r := &mockReader{Reader: strings.NewReader(string(jsonBytes))}
		return r, nil
	})

	// there are 100 rows to be parsed, each read batch is 40 rows
	// buffer size is 320 bytes, each read batch is 40 rows
	// Read() is called for 3 times, 40 + 40 + 20
	reader, err := NewReader(context.Background(), cm, schema, "mockPath", 320)
	suite.NoError(err)

	data, err := reader.Read()
	suite.NoError(err)
	suite.Equal(40, data.GetRowNum())

	data, err = reader.Read()
	suite.NoError(err)
	suite.Equal(40, data.GetRowNum())

	data, err = reader.Read()
	suite.NoError(err)
	suite.Equal(20, data.GetRowNum())
}

func TestJsonReader(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

func (suite *ReaderSuite) TestAllowInsertAutoID_KeepUserPK() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       true,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "8"},
				},
			},
		},
	}

	// build rows that explicitly include pk and a valid vec of dim 8
	rows := make([]any, 0, suite.numRows)
	for i := 0; i < suite.numRows; i++ {
		row := make(map[string]any)
		row["pk"] = int64(i + 1)
		vec := make([]float64, 8)
		for j := 0; j < 8; j++ {
			vec[j] = float64(j)
		}
		row["vec"] = vec
		rows = append(rows, row)
	}
	jsonBytes, err := json.Marshal(rows)
	suite.NoError(err)

	// allow_insert_autoid=false, providing PK should error
	{
		cm := mocks.NewChunkManager(suite.T())
		cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (storage.FileReader, error) {
			r := &mockReader{Reader: strings.NewReader(string(jsonBytes))}
			return r, nil
		})
		reader, err := NewReader(context.Background(), cm, schema, "mockPath", math.MaxInt)
		suite.NoError(err)
		_, err = reader.Read()
		suite.Error(err)
		suite.Contains(err.Error(), "is auto-generated, no need to provide")
	}

	// allow_insert_autoid=true, providing PK should be allowed
	{
		schema.Properties = []*commonpb.KeyValuePair{{Key: common.AllowInsertAutoIDKey, Value: "true"}}
		cm := mocks.NewChunkManager(suite.T())
		cm.EXPECT().Reader(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) (storage.FileReader, error) {
			r := &mockReader{Reader: strings.NewReader(string(jsonBytes))}
			return r, nil
		})
		reader, err := NewReader(context.Background(), cm, schema, "mockPath", math.MaxInt)
		suite.NoError(err)
		_, err = reader.Read()
		suite.NoError(err)
	}
}
