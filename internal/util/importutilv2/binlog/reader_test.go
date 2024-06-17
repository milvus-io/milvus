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

package binlog

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
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

	schema  *schemapb.CollectionSchema
	numRows int

	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType

	deletePKs []storage.PrimaryKey
	deleteTss []int64

	tsStart uint64
	tsEnd   uint64
}

func (suite *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *ReaderSuite) SetupTest() {
	// default suite params
	suite.numRows = 100
	suite.tsStart = 0
	suite.tsEnd = math.MaxUint64
	suite.pkDataType = schemapb.DataType_Int64
	suite.vecDataType = schemapb.DataType_FloatVector
}

func createBinlogBuf(t *testing.T, field *schemapb.FieldSchema, data storage.FieldData) []byte {
	dataType := field.GetDataType()
	w := storage.NewInsertBinlogWriter(dataType, 1, 1, 1, field.GetFieldID())
	assert.NotNil(t, w)
	defer w.Close()

	var dim int64
	var err error
	dim, err = typeutil.GetDim(field)
	if err != nil || dim == 0 {
		dim = 1
	}

	evt, err := w.NextInsertEventWriter(int(dim))
	assert.NoError(t, err)

	evt.SetEventTimestamp(1, math.MaxInt64)
	w.SetEventTimeStamp(1, math.MaxInt64)

	// without the two lines, the case will crash at here.
	// the "original_size" is come from storage.originalSizeKey
	sizeTotal := data.GetMemorySize()
	w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))

	switch dataType {
	case schemapb.DataType_Bool:
		err = evt.AddBoolToPayload(data.(*storage.BoolFieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_Int8:
		err = evt.AddInt8ToPayload(data.(*storage.Int8FieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_Int16:
		err = evt.AddInt16ToPayload(data.(*storage.Int16FieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_Int32:
		err = evt.AddInt32ToPayload(data.(*storage.Int32FieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_Int64:
		err = evt.AddInt64ToPayload(data.(*storage.Int64FieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_Float:
		err = evt.AddFloatToPayload(data.(*storage.FloatFieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_Double:
		err = evt.AddDoubleToPayload(data.(*storage.DoubleFieldData).Data)
		assert.NoError(t, err)
	case schemapb.DataType_VarChar:
		values := data.(*storage.StringFieldData).Data
		for _, val := range values {
			err = evt.AddOneStringToPayload(val)
			assert.NoError(t, err)
		}
	case schemapb.DataType_JSON:
		rows := data.(*storage.JSONFieldData).Data
		for i := 0; i < len(rows); i++ {
			err = evt.AddOneJSONToPayload(rows[i])
			assert.NoError(t, err)
		}
	case schemapb.DataType_Array:
		rows := data.(*storage.ArrayFieldData).Data
		for i := 0; i < len(rows); i++ {
			err = evt.AddOneArrayToPayload(rows[i])
			assert.NoError(t, err)
		}
	case schemapb.DataType_BinaryVector:
		vectors := data.(*storage.BinaryVectorFieldData).Data
		err = evt.AddBinaryVectorToPayload(vectors, int(dim))
		assert.NoError(t, err)
	case schemapb.DataType_FloatVector:
		vectors := data.(*storage.FloatVectorFieldData).Data
		err = evt.AddFloatVectorToPayload(vectors, int(dim))
		assert.NoError(t, err)
	case schemapb.DataType_Float16Vector:
		vectors := data.(*storage.Float16VectorFieldData).Data
		err = evt.AddFloat16VectorToPayload(vectors, int(dim))
		assert.NoError(t, err)
	case schemapb.DataType_BFloat16Vector:
		vectors := data.(*storage.BFloat16VectorFieldData).Data
		err = evt.AddBFloat16VectorToPayload(vectors, int(dim))
		assert.NoError(t, err)
	case schemapb.DataType_SparseFloatVector:
		vectors := data.(*storage.SparseFloatVectorFieldData)
		err = evt.AddSparseFloatVectorToPayload(vectors)
		assert.NoError(t, err)
	default:
		assert.True(t, false)
		return nil
	}

	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)
	return buf
}

func createDeltaBuf(t *testing.T, deletePKs []storage.PrimaryKey, deleteTss []int64) []byte {
	assert.Equal(t, len(deleteTss), len(deletePKs))
	deleteData := storage.NewDeleteData(nil, nil)
	for i := range deletePKs {
		deleteData.Append(deletePKs[i], uint64(deleteTss[i]))
	}
	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(1, 1, 1, deleteData)
	assert.NoError(t, err)
	return blob.Value
}

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType) {
	const (
		insertPrefix = "mock-insert-binlog-prefix"
		deltaPrefix  = "mock-delta-binlog-prefix"
	)
	insertBinlogs := map[int64][]string{
		0: {
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735801",
		},
		1: {
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735811",
		},
		100: {
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735821",
		},
		101: {
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735831",
		},
		102: {
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735841",
		},
	}
	var deltaLogs []string
	if len(suite.deletePKs) != 0 {
		deltaLogs = []string{
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
		}
	}
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
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
			},
		},
	}
	cm := mocks.NewChunkManager(suite.T())
	schema = typeutil.AppendSystemFields(schema)

	originalInsertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)
	insertLogs := lo.Flatten(lo.Values(insertBinlogs))

	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			for _, filePath := range insertLogs {
				if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
					return nil
				}
			}
			return nil
		})
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			for _, filePath := range deltaLogs {
				if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
					return nil
				}
			}
			return nil
		})
	for fieldID, paths := range insertBinlogs {
		field := typeutil.GetField(schema, fieldID)
		suite.NotNil(field)
		buf0 := createBinlogBuf(suite.T(), field, originalInsertData.Data[fieldID])
		cm.EXPECT().Read(mock.Anything, paths[0]).Return(buf0, nil)
	}

	if len(suite.deletePKs) != 0 {
		for _, path := range deltaLogs {
			buf := createDeltaBuf(suite.T(), suite.deletePKs, suite.deleteTss)
			cm.EXPECT().Read(mock.Anything, path).Return(buf, nil)
		}
	}

	reader, err := NewReader(context.Background(), cm, schema, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd)
	suite.NoError(err)
	insertData, err := reader.Read()
	suite.NoError(err)

	pks, err := storage.GetPkFromInsertData(schema, originalInsertData)
	suite.NoError(err)
	tss, err := storage.GetTimestampFromInsertData(originalInsertData)
	suite.NoError(err)
	expectInsertData, err := storage.NewInsertData(schema)
	suite.NoError(err)
	for _, field := range schema.GetFields() {
		expectInsertData.Data[field.GetFieldID()], err = storage.NewFieldData(field.GetDataType(), field, suite.numRows)
		suite.NoError(err)
	}
OUTER:
	for i := 0; i < suite.numRows; i++ {
		if uint64(tss.Data[i]) < suite.tsStart || uint64(tss.Data[i]) > suite.tsEnd {
			continue
		}
		for j := 0; j < len(suite.deletePKs); j++ {
			if suite.deletePKs[j].GetValue() == pks.GetRow(i) && suite.deleteTss[j] > tss.Data[i] {
				continue OUTER
			}
		}
		err = expectInsertData.Append(originalInsertData.GetRow(i))
		suite.NoError(err)
	}

	expectRowCount := expectInsertData.GetRowNum()
	for fieldID, data := range insertData.Data {
		suite.Equal(expectRowCount, data.RowNum())
		fieldData := expectInsertData.Data[fieldID]
		fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
		for i := 0; i < expectRowCount; i++ {
			expect := fieldData.GetRow(i)
			actual := data.GetRow(i)
			if fieldDataType == schemapb.DataType_Array {
				suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
			} else {
				suite.Equal(expect, actual)
			}
		}
	}
}

func (suite *ReaderSuite) TestReadScalarFields() {
	suite.run(schemapb.DataType_Bool, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None)
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

func (suite *ReaderSuite) TestWithTSRangeAndDelete() {
	suite.numRows = 10
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(4),
		storage.NewInt64PrimaryKey(6),
		storage.NewInt64PrimaryKey(8),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.numRows = 10
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewVarCharPrimaryKey("1"),
		storage.NewVarCharPrimaryKey("4"),
		storage.NewVarCharPrimaryKey("6"),
		storage.NewVarCharPrimaryKey("8"),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
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
