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

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
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

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType) {
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
				},
			},
		},
	}
	insertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)
	rows := make([]map[string]any, 0, suite.numRows)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	for i := 0; i < insertData.GetRowNum(); i++ {
		data := make(map[int64]interface{})
		for fieldID, v := range insertData.Data {
			field := fieldIDToField[fieldID]
			dataType := field.GetDataType()
			elemType := field.GetElementType()
			switch dataType {
			case schemapb.DataType_Array:
				switch elemType {
				case schemapb.DataType_Bool:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetBoolData().GetData()
				case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetIntData().GetData()
				case schemapb.DataType_Int64:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetLongData().GetData()
				case schemapb.DataType_Float:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetFloatData().GetData()
				case schemapb.DataType_Double:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetDoubleData().GetData()
				case schemapb.DataType_String:
					data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetStringData().GetData()
				}
			case schemapb.DataType_JSON:
				data[fieldID] = string(v.GetRow(i).([]byte))
			case schemapb.DataType_BinaryVector:
				bytes := v.GetRow(i).([]byte)
				ints := make([]int, 0, len(bytes))
				for _, b := range bytes {
					ints = append(ints, int(b))
				}
				data[fieldID] = ints
			case schemapb.DataType_Float16Vector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.Float16BytesToFloat32Vector(bytes)
			case schemapb.DataType_BFloat16Vector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.BFloat16BytesToFloat32Vector(bytes)
			case schemapb.DataType_SparseFloatVector:
				bytes := v.GetRow(i).([]byte)
				data[fieldID] = typeutil.SparseFloatBytesToMap(bytes)
			default:
				data[fieldID] = v.GetRow(i)
			}
		}
		row := lo.MapKeys(data, func(_ any, fieldID int64) string {
			return fieldIDToField[fieldID].GetName()
		})
		rows = append(rows, row)
	}

	jsonBytes, err := json.Marshal(rows)
	suite.NoError(err)
	type mockReader struct {
		io.Reader
		io.Closer
		io.ReaderAt
		io.Seeker
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
	suite.run(schemapb.DataType_Bool, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None)
	suite.run(schemapb.DataType_String, schemapb.DataType_None)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func (suite *ReaderSuite) TestVector() {
	suite.vecDataType = schemapb.DataType_BinaryVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_FloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_Float16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_BFloat16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.vecDataType = schemapb.DataType_SparseFloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
