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
	"encoding/binary"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/exp/slices"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReaderSuite struct {
	suite.Suite

	// test param
	schema  *schemapb.CollectionSchema
	numRows int

	deleteData []any
	tsField    []uint64
	tsStart    uint64
	tsEnd      uint64
}

func (suite *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *ReaderSuite) SetupTest() {

}

func createBinlogBuf(t *testing.T, dataType schemapb.DataType, data interface{}) []byte {
	w := storage.NewInsertBinlogWriter(dataType, 10, 20, 30, 40)
	assert.NotNil(t, w)
	defer w.Close()

	dim := 0
	if dataType == schemapb.DataType_BinaryVector {
		vectors := data.([][]byte)
		if len(vectors) > 0 {
			dim = len(vectors[0]) * 8
		}
	} else if dataType == schemapb.DataType_FloatVector {
		vectors := data.([][]float32)
		if len(vectors) > 0 {
			dim = len(vectors[0])
		}
	} else if dataType == schemapb.DataType_Float16Vector {
		vectors := data.([][]byte)
		if len(vectors) > 0 {
			dim = len(vectors[0]) / 2
		}
	}

	evt, err := w.NextInsertEventWriter(dim)
	assert.NoError(t, err)
	assert.NotNil(t, evt)

	evt.SetEventTimestamp(100, 200)
	w.SetEventTimeStamp(1000, 2000)

	switch dataType {
	case schemapb.DataType_Bool:
		err = evt.AddBoolToPayload(data.([]bool))
		assert.NoError(t, err)
		// without the two lines, the case will crash at here.
		// the "original_size" is come from storage.originalSizeKey
		sizeTotal := len(data.([]bool))
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int8:
		err = evt.AddInt8ToPayload(data.([]int8))
		assert.NoError(t, err)
		sizeTotal := len(data.([]int8))
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int16:
		err = evt.AddInt16ToPayload(data.([]int16))
		assert.NoError(t, err)
		sizeTotal := len(data.([]int16)) * 2
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int32:
		err = evt.AddInt32ToPayload(data.([]int32))
		assert.NoError(t, err)
		sizeTotal := len(data.([]int32)) * 4
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Int64:
		err = evt.AddInt64ToPayload(data.([]int64))
		assert.NoError(t, err)
		sizeTotal := len(data.([]int64)) * 8
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Float:
		err = evt.AddFloatToPayload(data.([]float32))
		assert.NoError(t, err)
		sizeTotal := len(data.([]float32)) * 4
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Double:
		err = evt.AddDoubleToPayload(data.([]float64))
		assert.NoError(t, err)
		sizeTotal := len(data.([]float64)) * 8
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_VarChar:
		values := data.([]string)
		sizeTotal := 0
		for _, val := range values {
			err = evt.AddOneStringToPayload(val)
			assert.NoError(t, err)
			sizeTotal += binary.Size(val)
		}
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_JSON:
		rows := data.([][]byte)
		sizeTotal := 0
		for i := 0; i < len(rows); i++ {
			err = evt.AddOneJSONToPayload(rows[i])
			assert.NoError(t, err)
			sizeTotal += binary.Size(rows[i])
		}
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Array:
		rows := data.([]*schemapb.ScalarField)
		sizeTotal := 0
		for i := 0; i < len(rows); i++ {
			err = evt.AddOneArrayToPayload(rows[i])
			assert.NoError(t, err)
			sizeTotal += binary.Size(rows[i])
		}
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_BinaryVector:
		vectors := data.([][]byte)
		for i := 0; i < len(vectors); i++ {
			err = evt.AddBinaryVectorToPayload(vectors[i], dim)
			assert.NoError(t, err)
		}
		sizeTotal := len(vectors) * dim / 8
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_FloatVector:
		vectors := data.([][]float32)
		for i := 0; i < len(vectors); i++ {
			err = evt.AddFloatVectorToPayload(vectors[i], dim)
			assert.NoError(t, err)
		}
		sizeTotal := len(vectors) * dim * 4
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	case schemapb.DataType_Float16Vector:
		vectors := data.([][]byte)
		for i := 0; i < len(vectors); i++ {
			err = evt.AddFloat16VectorToPayload(vectors[i], dim)
			assert.NoError(t, err)
		}
		sizeTotal := len(vectors) * dim * 2
		w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))
	default:
		assert.True(t, false)
		return nil
	}

	err = w.Finish()
	assert.NoError(t, err)

	buf, err := w.GetBuffer()
	assert.NoError(t, err)
	assert.NotNil(t, buf)

	return buf
}

func createDeltaBuf(t *testing.T, deleteList interface{}, varcharType bool) []byte {
	deleteData := &storage.DeleteData{
		Pks:      make([]storage.PrimaryKey, 0),
		Tss:      make([]storage.Timestamp, 0),
		RowCount: 0,
	}

	if varcharType {
		deltaData := deleteList.([]string)
		assert.NotNil(t, deltaData)
		for i, id := range deltaData {
			deleteData.Pks = append(deleteData.Pks, storage.NewVarCharPrimaryKey(id))
			deleteData.Tss = append(deleteData.Tss, uint64(i))
			deleteData.RowCount++
		}
	} else {
		deltaData := deleteList.([]int64)
		assert.NotNil(t, deltaData)
		for i, id := range deltaData {
			deleteData.Pks = append(deleteData.Pks, storage.NewInt64PrimaryKey(id))
			deleteData.Tss = append(deleteData.Tss, uint64(i))
			deleteData.RowCount++
		}
	}

	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(1, 1, 1, deleteData)
	assert.NoError(t, err)
	assert.NotNil(t, blob)

	return blob.Value
}

func createFieldsData(t *testing.T, collectionSchema *schemapb.CollectionSchema, rowCount int) map[storage.FieldID]interface{} {
	fieldsData := make(map[storage.FieldID]interface{})

	// internal fields
	rowIDData := make([]int64, 0)
	timestampData := make([]int64, 0)
	for i := 0; i < rowCount; i++ {
		rowIDData = append(rowIDData, int64(i))
		timestampData = append(timestampData, int64(i))
	}
	fieldsData[common.RowIDField] = rowIDData
	fieldsData[common.TimeStampField] = timestampData

	// user-defined fields
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		switch schema.DataType {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rowCount; i++ {
				boolData = append(boolData, i%3 != 0)
			}
			fieldsData[schema.GetFieldID()] = boolData
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rowCount; i++ {
				floatData = append(floatData, float32(i/2))
			}
			fieldsData[schema.GetFieldID()] = floatData
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rowCount; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			fieldsData[schema.GetFieldID()] = doubleData
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rowCount; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			fieldsData[schema.GetFieldID()] = int8Data
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rowCount; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			fieldsData[schema.GetFieldID()] = int16Data
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rowCount; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			fieldsData[schema.GetFieldID()] = int32Data
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rowCount; i++ {
				int64Data = append(int64Data, int64(i))
			}
			fieldsData[schema.GetFieldID()] = int64Data
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(schema)
			assert.NoError(t, err)
			binVecData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				vec := make([]byte, 0)
				for k := 0; k < int(dim)/8; k++ {
					vec = append(vec, byte(i%256))
				}
				binVecData = append(binVecData, vec)
			}
			fieldsData[schema.GetFieldID()] = binVecData
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(schema)
			assert.NoError(t, err)
			floatVecData := make([][]float32, 0)
			for i := 0; i < rowCount; i++ {
				vec := make([]float32, 0)
				for k := 0; k < int(dim); k++ {
					vec = append(vec, rand.Float32())
				}
				floatVecData = append(floatVecData, vec)
			}
			fieldsData[schema.GetFieldID()] = floatVecData
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rowCount; i++ {
				varcharData = append(varcharData, "no."+strconv.Itoa(i))
			}
			fieldsData[schema.GetFieldID()] = varcharData
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				jsonData = append(jsonData, []byte(fmt.Sprintf("{\"y\": %d}", i)))
			}
			fieldsData[schema.GetFieldID()] = jsonData
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
			fieldsData[schema.GetFieldID()] = arrayData
		default:
			return nil
		}
	}
	return fieldsData
}

func (suite *ReaderSuite) run(dt schemapb.DataType) {
	const (
		insertPrefix = "mock-insert-binlog-prefix"
		deltaPrefix  = "mock-delta-binlog-prefix"

		rowCount0 = 6
		rowCount1 = 4
	)
	var (
		insertBinlogs = map[int64][]string{
			0: {
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735801",
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735802",
			},
			1: {
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735811",
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735812",
			},
			100: {
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735821",
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735822",
			},
			101: {
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735831",
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735832",
			},
			102: {
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735841",
				"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735842",
			},
		}
		deltaLogs = []string{
			//"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
			//"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415106",
		}
	)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:  102,
				Name:     dt.String(),
				DataType: dt,
			},
		},
	}
	cm := mocks.NewChunkManager(suite.T())
	typeutil.AppendSystemFields(schema)

	fieldsData := createFieldsData(suite.T(), schema, rowCount0+rowCount1)
	fieldsData0 := make(map[storage.FieldID]interface{})
	fieldsData1 := make(map[storage.FieldID]interface{})
	for fieldID, data := range fieldsData {
		sliceValue := reflect.ValueOf(data)
		slice0 := reflect.MakeSlice(sliceValue.Type(), rowCount0, rowCount0)
		slice1 := reflect.MakeSlice(sliceValue.Type(), rowCount1, rowCount1)
		for i := 0; i < sliceValue.Len(); i++ {
			if i < rowCount0 {
				slice0.Index(i).Set(sliceValue.Index(i))
			} else {
				slice1.Index(i - rowCount0).Set(sliceValue.Index(i))
			}
		}
		fieldsData0[fieldID] = slice0.Interface()
		fieldsData1[fieldID] = slice1.Interface()
	}
	insertLogs := lo.Flatten(lo.Values(insertBinlogs))

	cm.EXPECT().ListWithPrefix(mock.Anything, insertPrefix, mock.Anything).Return(insertLogs, nil, nil)
	cm.EXPECT().ListWithPrefix(mock.Anything, deltaPrefix, mock.Anything).Return(deltaLogs, nil, nil)
	for fieldID, paths := range insertBinlogs {
		dataType := typeutil.GetField(schema, fieldID).GetDataType()
		buf0 := createBinlogBuf(suite.T(), dataType, fieldsData0[fieldID])
		cm.EXPECT().Read(mock.Anything, paths[0]).Return(buf0, nil)
		buf1 := createBinlogBuf(suite.T(), dataType, fieldsData1[fieldID])
		cm.EXPECT().Read(mock.Anything, paths[1]).Return(buf1, nil)
	}

	for _, path := range deltaLogs {
		//buf := createDeltaBuf(suite.T(), []int64{}, false)
		cm.EXPECT().Read(mock.Anything, path).Return(nil, nil)
	}

	reader, err := NewReader(cm, schema, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd)
	suite.NoError(err)
	insertData, err := reader.Next(-1)
	suite.NoError(err)
	expectRowCount := rowCount0 - int(suite.tsStart)
	for fieldID, data := range insertData.Data {
		suite.Equal(expectRowCount, data.RowNum())
		fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
		values, err := typeutil.InterfaceToInterfaceSlice(fieldsData0[fieldID])
		suite.NoError(err)
		for i := 0; i < expectRowCount; i++ {
			expect := values[i+int(suite.tsStart)]
			actual := data.GetRow(i)
			if fieldDataType == schemapb.DataType_Array {
				suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
			} else {
				suite.Equal(expect, actual)
			}
		}
	}

	insertData, err = reader.Next(-1)
	suite.NoError(err)
	if suite.tsEnd != math.MaxUint64 {
		expectRowCount = int(suite.tsEnd) - rowCount0 + 1
	} else {
		expectRowCount = rowCount1
	}
	for fieldID, data := range insertData.Data {
		suite.Equal(expectRowCount, data.RowNum())
		fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
		values, err := typeutil.InterfaceToInterfaceSlice(fieldsData1[fieldID])
		suite.NoError(err)
		for i := 0; i < expectRowCount; i++ {
			expect := values[i]
			actual := data.GetRow(i)
			if fieldDataType == schemapb.DataType_Array {
				suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
			} else {
				suite.Equal(expect, actual)
			}
		}
	}
}

func (suite *ReaderSuite) TestRead() {
	suite.tsStart = 0
	suite.tsEnd = math.MaxUint64
	suite.run(schemapb.DataType_Bool)
	suite.run(schemapb.DataType_Int8)
	suite.run(schemapb.DataType_Int16)
	suite.run(schemapb.DataType_Int32)
	suite.run(schemapb.DataType_Int64)
	suite.run(schemapb.DataType_Float)
	suite.run(schemapb.DataType_Double)
	suite.run(schemapb.DataType_VarChar)
	suite.run(schemapb.DataType_Array)
	suite.run(schemapb.DataType_JSON)
}

func (suite *ReaderSuite) TestWithTSRangeAndDelete() {
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.run(schemapb.DataType_Int32)
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
