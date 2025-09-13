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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	insertPrefix = "mock-insert-binlog-prefix"
	deltaPrefix  = "mock-delta-binlog-prefix"
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

func genBinlogPath(fieldID int64) string {
	return fmt.Sprintf("backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/%d/6666", fieldID)
}

func genBinlogPaths(fieldIDs []int64) map[int64][]string {
	binlogPaths := make(map[int64][]string)
	for _, fieldID := range fieldIDs {
		binlogPaths[fieldID] = []string{genBinlogPath(fieldID)}
	}
	return binlogPaths
}

func createBinlogBuf(t *testing.T, field *schemapb.FieldSchema, data storage.FieldData) []byte {
	dataType := field.GetDataType()
	w := storage.NewInsertBinlogWriter(dataType, 1, 1, 1, field.GetFieldID(), field.GetNullable())
	assert.NotNil(t, w)
	defer w.Close()

	var dim int64
	var err error
	dim, err = typeutil.GetDim(field)
	if err != nil || dim == 0 {
		dim = 1
	}

	evt, err := w.NextInsertEventWriter(storage.WithDim(int(dim)), storage.WithNullable(field.GetNullable()), storage.WithElementType(field.GetElementType()))
	assert.NoError(t, err)

	evt.SetEventTimestamp(1, math.MaxInt64)
	w.SetEventTimeStamp(1, math.MaxInt64)

	// without the two lines, the case will crash at here.
	// the "original_size" is come from storage.originalSizeKey
	sizeTotal := data.GetMemorySize()
	w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))

	switch dataType {
	case schemapb.DataType_Bool:
		err = evt.AddBoolToPayload(data.(*storage.BoolFieldData).Data, data.(*storage.BoolFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int8:
		err = evt.AddInt8ToPayload(data.(*storage.Int8FieldData).Data, data.(*storage.Int8FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int16:
		err = evt.AddInt16ToPayload(data.(*storage.Int16FieldData).Data, data.(*storage.Int16FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int32:
		err = evt.AddInt32ToPayload(data.(*storage.Int32FieldData).Data, data.(*storage.Int32FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int64:
		err = evt.AddInt64ToPayload(data.(*storage.Int64FieldData).Data, data.(*storage.Int64FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Float:
		err = evt.AddFloatToPayload(data.(*storage.FloatFieldData).Data, data.(*storage.FloatFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Double:
		err = evt.AddDoubleToPayload(data.(*storage.DoubleFieldData).Data, data.(*storage.DoubleFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_VarChar:
		values := data.(*storage.StringFieldData).Data
		validValues := data.(*storage.StringFieldData).ValidData
		for i, val := range values {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneStringToPayload(val, valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_JSON:
		rows := data.(*storage.JSONFieldData).Data
		validValues := data.(*storage.JSONFieldData).ValidData
		for i := 0; i < len(rows); i++ {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneJSONToPayload(rows[i], valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_Array:
		rows := data.(*storage.ArrayFieldData).Data
		validValues := data.(*storage.ArrayFieldData).ValidData
		for i := 0; i < len(rows); i++ {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneArrayToPayload(rows[i], valid)
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
	case schemapb.DataType_Int8Vector:
		vectors := data.(*storage.Int8VectorFieldData).Data
		err = evt.AddInt8VectorToPayload(vectors, int(dim))
		assert.NoError(t, err)
	case schemapb.DataType_ArrayOfVector:
		elementType := field.GetElementType()
		switch elementType {
		case schemapb.DataType_FloatVector:
			vectors := data.(*storage.VectorArrayFieldData)
			err = evt.AddVectorArrayFieldDataToPayload(vectors)
			assert.NoError(t, err)
		default:
			assert.True(t, false)
			return nil
		}
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

func (suite *ReaderSuite) createMockChunk(schema *schemapb.CollectionSchema, insertBinlogs map[int64][]string, expectRead bool) (*mocks.ChunkManager, *storage.InsertData) {
	var deltaLogs []string
	if len(suite.deletePKs) != 0 {
		deltaLogs = []string{
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
		}
	}

	cm := mocks.NewChunkManager(suite.T())

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

	if expectRead {
		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, filePath := range deltaLogs {
					if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
						return nil
					}
				}
				return nil
			})

		var (
			paths = make([]string, 0)
			bytes = make([][]byte, 0)
		)
		allFields := typeutil.GetAllFieldSchemas(schema)
		for _, field := range allFields {
			fieldID := field.GetFieldID()
			logs, ok := insertBinlogs[fieldID]
			if ok && len(logs) > 0 {
				paths = append(paths, insertBinlogs[fieldID][0])

				// the testutil.CreateInsertData() doesn't create data for function output field
				// add data here to avoid crash
				if field.IsFunctionOutput {
					data, dim := testutils.GenerateSparseFloatVectorsData(suite.numRows)
					originalInsertData.Data[fieldID] = &storage.SparseFloatVectorFieldData{
						SparseFloatArray: schemapb.SparseFloatArray{
							Contents: data,
							Dim:      dim,
						},
					}
				}
				bytes = append(bytes, createBinlogBuf(suite.T(), field, originalInsertData.Data[fieldID]))
			}
		}
		cm.EXPECT().MultiRead(mock.Anything, paths).Return(bytes, nil)

		if len(suite.deletePKs) != 0 {
			for _, path := range deltaLogs {
				buf := createDeltaBuf(suite.T(), suite.deletePKs, suite.deleteTss)
				cm.EXPECT().Read(mock.Anything, path).Return(buf, nil)
			}
		}
	}

	return cm, originalInsertData
}

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType, nullable bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      int64(common.RowIDField),
				Name:         common.RowIDFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      int64(common.TimeStampField),
				Name:         common.TimeStampFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
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
				Nullable:    nullable,
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 103,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     104,
						Name:        "struct_str",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_VarChar,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MaxLengthKey,
								Value: "256",
							},
							{
								Key:   common.MaxCapacityKey,
								Value: "20",
							},
						},
					},
					{
						FieldID:     105,
						Name:        "struct_float_vector",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MaxCapacityKey,
								Value: "20",
							},
							{
								Key:   common.DimKey,
								Value: "8",
							},
						},
					},
				},
			},
		},
	}
	allFields := typeutil.GetAllFieldSchemas(schema)
	insertBinlogs := genBinlogPaths(lo.Map(allFields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
		return fieldSchema.GetFieldID()
	}))
	cm, originalInsertData := suite.createMockChunk(schema, insertBinlogs, true)
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(128, nil)

	reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024)
	suite.NoError(err)
	insertData, err := reader.Read()
	suite.NoError(err)
	size, err := reader.Size()
	suite.NoError(err)
	suite.Equal(int64(128*len(lo.Flatten(lo.Values(insertBinlogs)))), size)
	size2, err := reader.Size() // size is cached
	suite.NoError(err)
	suite.Equal(size, size2)

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
				if expect == nil {
					suite.Nil(expect)
				} else {
					suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				}
			} else if fieldDataType == schemapb.DataType_ArrayOfVector {
				suite.True(slices.Equal(expect.(*schemapb.VectorField).GetFloatVector().GetData(), actual.(*schemapb.VectorField).GetFloatVector().GetData()))
			} else {
				suite.Equal(expect, actual)
			}
		}
	}
}

func (suite *ReaderSuite) TestReadScalarFields() {
	suite.run(schemapb.DataType_Bool, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None, false)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String, false)

	suite.run(schemapb.DataType_Bool, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None, true)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String, true)
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
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
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

func (suite *ReaderSuite) TestVerify() {
	suite.deletePKs = []storage.PrimaryKey{}

	pkFieldID := int64(100)
	vecFieldID := int64(101)
	nullableFieldID := int64(102)
	functionFieldID := int64(103)
	dynamicFieldID := int64(104)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  vecFieldID,
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
				FieldID:  nullableFieldID,
				Name:     "nullable",
				DataType: schemapb.DataType_Double,
				Nullable: true,
			},
			{
				FieldID:          functionFieldID,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
			{
				FieldID:   dynamicFieldID,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}
	insertBinlogs := genBinlogPaths(lo.Map(schema.Fields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
		return fieldSchema.GetFieldID()
	}))

	checkFunc := func() {
		cm, _ := suite.createMockChunk(schema, insertBinlogs, false)
		reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024)
		suite.Error(err)
		suite.Nil(reader)
	}

	// no insert binlogs to import
	reader, err := NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{}, suite.tsStart, suite.tsEnd, 64*1024*1024)
	suite.Error(err)
	suite.Nil(reader)

	// too many input paths
	reader, err = NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix, "dummy"}, suite.tsStart, suite.tsEnd, 64*1024*1024)
	suite.Error(err)
	suite.Nil(reader)

	// no binlog for RowID
	insertBinlogs[common.RowIDField] = []string{}
	checkFunc()

	// no binlog for RowID
	insertBinlogs[common.RowIDField] = []string{genBinlogPath(common.RowIDField)}
	insertBinlogs[common.TimeStampField] = []string{}
	checkFunc()

	// binlog count not equal
	insertBinlogs[common.TimeStampField] = []string{genBinlogPath(common.TimeStampField)}
	insertBinlogs[vecFieldID] = []string{genBinlogPath(vecFieldID), genBinlogPath(vecFieldID)}
	checkFunc()

	// vector field is required
	insertBinlogs[vecFieldID] = []string{}
	checkFunc()

	// primary key is required
	insertBinlogs[vecFieldID] = []string{genBinlogPath(vecFieldID)}
	insertBinlogs[pkFieldID] = []string{}
	checkFunc()

	// function output field is required
	insertBinlogs[pkFieldID] = []string{genBinlogPath(pkFieldID)}
	insertBinlogs[functionFieldID] = []string{}
	checkFunc()
}

func (suite *ReaderSuite) TestZeroDeltaRead() {
	suite.deletePKs = []storage.PrimaryKey{}

	mockChunkFunc := func(sourceSchema *schemapb.CollectionSchema, expectReadBinlogs map[int64][]string) *mocks.ChunkManager {
		sourceBinlogs := genBinlogPaths(lo.Map(sourceSchema.Fields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
			return fieldSchema.GetFieldID()
		}))

		cm := mocks.NewChunkManager(suite.T())

		sourceInsertData, err := testutil.CreateInsertData(sourceSchema, suite.numRows)
		suite.NoError(err)

		sourceInsertLogs := lo.Flatten(lo.Values(sourceBinlogs))

		cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, filePath := range sourceInsertLogs {
					if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
						return nil
					}
				}
				return nil
			})

		var (
			paths = make([]string, 0)
			bytes = make([][]byte, 0)
		)
		for _, field := range sourceSchema.Fields {
			fieldID := field.GetFieldID()
			logs, ok := expectReadBinlogs[fieldID]
			if ok && len(logs) > 0 {
				paths = append(paths, expectReadBinlogs[fieldID][0])

				// the testutil.CreateInsertData() doesn't create data for function output field
				// add data here to avoid crash
				if field.IsFunctionOutput {
					data, dim := testutils.GenerateSparseFloatVectorsData(suite.numRows)
					sourceInsertData.Data[fieldID] = &storage.SparseFloatVectorFieldData{
						SparseFloatArray: schemapb.SparseFloatArray{
							Contents: data,
							Dim:      dim,
						},
					}
				}
				bytes = append(bytes, createBinlogBuf(suite.T(), field, sourceInsertData.Data[fieldID]))
			}
		}
		cm.EXPECT().MultiRead(mock.Anything, paths).Return(bytes, nil)

		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				return nil
			})

		return cm
	}

	rowID := int64(common.RowIDField)
	tsID := int64(common.TimeStampField)
	pkFieldID := int64(100)
	vecFieldID := int64(101)
	functionFieldID := int64(102)
	nullableFieldID := int64(103)
	dynamicFieldID := int64(104)
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      rowID,
				Name:         common.RowIDFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      tsID,
				Name:         common.TimeStampFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  vecFieldID,
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
				FieldID:          functionFieldID,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
			{
				FieldID:      nullableFieldID,
				Name:         "nullable",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:   dynamicFieldID,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}

	checkFunc := func(targetSchema *schemapb.CollectionSchema, expectReadBinlogs map[int64][]string) {
		cm := mockChunkFunc(sourceSchema, expectReadBinlogs)
		reader, err := NewReader(context.Background(), cm, targetSchema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024)
		suite.NoError(err)
		suite.NotNil(reader)

		readData, err := reader.Read()
		suite.NoError(err)
		suite.Equal(suite.numRows, readData.GetRowNum())

		for _, field := range targetSchema.Fields {
			fieldID := field.GetFieldID()
			fieldData, ok := readData.Data[fieldID]
			if !ok {
				// if this field has no data, it must be nullable/default or dynamic
				suite.True(field.GetIsDynamic() || field.GetNullable() || field.GetDefaultValue() != nil)
			} else {
				suite.Equal(suite.numRows, fieldData.RowNum())
			}
		}
	}

	targetSchemaFunc := func(from int, to int, newFields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
		fields := make([]*schemapb.FieldSchema, 0)
		fields = append(fields, sourceSchema.Fields[from:to]...)
		fields = append(fields, newFields...)
		return &schemapb.CollectionSchema{Fields: fields}
	}

	// the target schema lacks some fields(not required field), can import
	checkFunc(targetSchemaFunc(0, 3), map[int64][]string{
		rowID:     {genBinlogPath(rowID)},
		tsID:      {genBinlogPath(tsID)},
		pkFieldID: {genBinlogPath(pkFieldID)},
	})

	// the target schema has a new nullable field, can import
	checkFunc(targetSchemaFunc(0, len(sourceSchema.Fields), &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "new",
		DataType: schemapb.DataType_Double,
		Nullable: true,
	}), map[int64][]string{
		rowID:           {genBinlogPath(rowID)},
		tsID:            {genBinlogPath(tsID)},
		pkFieldID:       {genBinlogPath(pkFieldID)},
		vecFieldID:      {genBinlogPath(vecFieldID)},
		functionFieldID: {genBinlogPath(functionFieldID)},
		nullableFieldID: {genBinlogPath(nullableFieldID)},
		dynamicFieldID:  {genBinlogPath(dynamicFieldID)},
	})

	// the target schema has a new dynamic field, can import
	checkFunc(targetSchemaFunc(0, len(sourceSchema.Fields), &schemapb.FieldSchema{
		FieldID:   200,
		Name:      "new",
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}), map[int64][]string{
		rowID:           {genBinlogPath(rowID)},
		tsID:            {genBinlogPath(tsID)},
		pkFieldID:       {genBinlogPath(pkFieldID)},
		vecFieldID:      {genBinlogPath(vecFieldID)},
		functionFieldID: {genBinlogPath(functionFieldID)},
		nullableFieldID: {genBinlogPath(nullableFieldID)},
		dynamicFieldID:  {genBinlogPath(dynamicFieldID)},
	})
}

func TestBinlogReader(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
