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
	"io"
	"math"
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

type storageV3DeltaRecordReader struct {
	record storage.Record
	read   bool
}

func (r *storageV3DeltaRecordReader) Next() (storage.Record, error) {
	if r.read {
		if r.record != nil {
			r.record.Release()
			r.record = nil
		}
		return nil, io.EOF
	}
	r.read = true
	return r.record, nil
}

func (r *storageV3DeltaRecordReader) Close() error {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	return nil
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
		err = evt.AddBinaryVectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_FloatVector:
		vectors := data.(*storage.FloatVectorFieldData).Data
		err = evt.AddFloatVectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_Float16Vector:
		vectors := data.(*storage.Float16VectorFieldData).Data
		err = evt.AddFloat16VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_BFloat16Vector:
		vectors := data.(*storage.BFloat16VectorFieldData).Data
		err = evt.AddBFloat16VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_SparseFloatVector:
		vectors := data.(*storage.SparseFloatVectorFieldData)
		err = evt.AddSparseFloatVectorToPayload(vectors)
		assert.NoError(t, err)
	case schemapb.DataType_Int8Vector:
		vectors := data.(*storage.Int8VectorFieldData).Data
		err = evt.AddInt8VectorToPayload(vectors, int(dim), nil)
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
				cm.EXPECT().MultiRead(mock.Anything, []string{path}).Return([][]byte{buf}, nil)
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

	reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
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
			switch fieldDataType {
			case schemapb.DataType_Array:
				if expect == nil {
					suite.Nil(expect)
				} else {
					suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				}
			case schemapb.DataType_ArrayOfVector:
				suite.True(slices.Equal(expect.(*schemapb.VectorField).GetFloatVector().GetData(), actual.(*schemapb.VectorField).GetFloatVector().GetData()))
			default:
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
	suite.pkDataType = schemapb.DataType_Int64
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
		reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
		suite.Error(err)
		suite.Nil(reader)
	}

	// no insert binlogs to import
	reader, err := NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.Error(err)
	suite.Nil(reader)

	// too many input paths
	reader, err = NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix, "dummy"}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
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
		reader, err := NewReader(context.Background(), cm, targetSchema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
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

func TestSnapshotManifestDeletesRequireFolding(t *testing.T) {
	manifest := packed.MarshalManifestPath("snapshot/data", 7)
	fields := mockey.Mock(packed.GetManifestFieldIDs).Return(map[int64]struct{}{0: {}, 1: {}, 100: {}}, nil).Build()
	defer fields.UnPatch()
	fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return(nil, nil).Build()
	defer fragments.UnPatch()
	deltas := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string{"snapshot/delete"}, nil).Build()
	defer deltas.UnPatch()
	open := mockey.Mock(storage.NewManifestRecordReader).Return(nil, io.EOF).Build()
	defer open.UnPatch()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	reader, err := NewStorageV3ManifestReader(context.Background(), nil, schema, nil,
		manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, nil, nil)
	require.Nil(t, reader)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	require.Zero(t, open.Times(), "no rows may be emitted while deletes are unsupported")
}

func TestSelectImportFields_BackupCompatibility(t *testing.T) {
	base := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "renamed", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "nullable", DataType: schemapb.DataType_Int64, Nullable: true},
			{
				FieldID: 103, Name: "default", DataType: schemapb.DataType_Int64,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
			},
			{FieldID: 104, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	})
	for _, tc := range []struct {
		name    string
		present []int64
		mutate  func(*schemapb.CollectionSchema)
		want    []int64
		errText string
	}{
		{name: "omit_optional_and_extra", present: []int64{100, 101, 999}, want: []int64{0, 1, 100, 101}},
		{name: "retain_present_optional", present: []int64{100, 101, 102, 103, 104}, want: []int64{0, 1, 100, 101, 102, 103, 104}},
		{name: "required_missing", present: []int64{100}, errText: "no binlog for field:renamed"},
		{name: "autoid_still_requires_pk", present: []int64{101}, mutate: func(s *schemapb.CollectionSchema) {
			typeutil.GetField(s, 100).AutoID = true
		}, errText: "no binlog for field:pk"},
		{name: "function_output_required", present: []int64{100}, mutate: func(s *schemapb.CollectionSchema) {
			typeutil.GetField(s, 101).IsFunctionOutput = true
		}, errText: "no binlog for field:renamed"},
		{name: "nullable_struct_absent", present: []int64{100, 101}, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Nullable: true, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "a"}, {FieldID: 202, Name: "b"},
			}}}
		}, want: []int64{0, 1, 100, 101}},
		{name: "struct_present", present: []int64{100, 101, 201, 202}, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "a"}, {FieldID: 202, Name: "b"},
			}}}
		}, want: []int64{0, 1, 100, 101, 201, 202}},
		{name: "partial_struct_rejected", present: []int64{100, 101, 201}, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Nullable: true, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "a"}, {FieldID: 202, Name: "b"},
			}}}
		}, errText: "no binlog for struct field:b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := typeutil.Clone(base)
			if tc.mutate != nil {
				tc.mutate(schema)
			}
			before := typeutil.Clone(schema)
			logs := map[int64][]string{0: {"rowid"}, 1: {"timestamp"}}
			for _, id := range tc.present {
				logs[id] = []string{fmt.Sprint(id)}
			}
			readSchema, err := selectImportFields(schema, func(id int64) bool { _, ok := logs[id]; return ok })
			validLogs, legacySchema, legacyErr := verify(schema, storage.StorageV1, logs)
			require.Equal(t, before, schema, "selection must not change the task's target schema")
			if tc.errText != "" {
				require.ErrorIs(t, err, merr.ErrImportFailed)
				require.ErrorContains(t, err, tc.errText)
				require.ErrorIs(t, legacyErr, merr.ErrImportFailed)
				require.ErrorContains(t, legacyErr, tc.errText)
				return
			}
			require.NoError(t, err)
			require.NoError(t, legacyErr)
			require.Equal(t, legacySchema, readSchema)
			require.ElementsMatch(t, tc.want, lo.Keys(validLogs))
			_, dynamicPresent := logs[104]
			require.Equal(t, dynamicPresent, readSchema.GetEnableDynamicField())
		})
	}
}

func patchStorageV3TestFieldIDs(t *testing.T, ids ...int64) {
	t.Helper()
	present := map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}}
	for _, id := range ids {
		present[id] = struct{}{}
	}
	p := mockey.Mock(packed.GetManifestFieldIDs).Return(present, nil).Build()
	t.Cleanup(func() { p.UnPatch() })
}

func TestStorageV3Reader_ManifestFieldSelectionErrors(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name    string
		readErr error
		want    error
	}{
		{"read_error", merr.WrapErrIoKeyNotFound("manifest unavailable"), merr.ErrIoKeyNotFound},
		{"missing_required", nil, merr.ErrImportFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := mockey.Mock(packed.GetManifestFieldIDs).
				Return(map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}}, tc.readErr).Build()
			defer p.UnPatch()
			r, err := NewStorageV3ManifestReader(context.Background(), nil,
				&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}},
				nil, packed.MarshalManifestPath("snapshot/segment/10", 7), 0, math.MaxUint64, 1024, SourceEncryption{}, nil, nil)
			require.Nil(t, r)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestStorageV3Reader_UsesExactSnapshotManifest(t *testing.T) {
	patchStorageV3TestFieldIDs(t, 100)
	segmentPath := "snapshot/files/segment/10"
	exactManifest := packed.MarshalManifestPath(segmentPath, 7)
	var openedManifest string

	manifestReaderPatch := mockey.Mock(storage.NewManifestRecordReader).To(
		func(_ context.Context, manifestPath string, _ *schemapb.CollectionSchema, _ ...storage.RwOption) (storage.RecordReader, error) {
			openedManifest = manifestPath
			return &storageV3DeltaRecordReader{read: true}, nil
		}).Build()
	defer manifestReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()

	r, err := NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
		&indexpb.StorageConfig{},
		exactManifest,
		0,
		math.MaxUint64,
		1024,
		SourceEncryption{},
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, exactManifest, openedManifest)
	r.Close()

	_, err = NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{},
		&indexpb.StorageConfig{},
		packed.MarshalManifestPath(segmentPath, packed.ManifestLatest),
		0,
		math.MaxUint64,
		1024,
		SourceEncryption{},
		nil,
		nil,
	)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.ErrorContains(t, err, "exact StorageV3 manifest version")
}

func TestStorageV3Reader_CMEKWithoutTextUsesPackedReaderContext(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	expectedContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 10,
		EncryptionKey:    "encoded-source-key",
	}

	// The constructor consumes explicit dependencies; neither EZK parsing nor
	// source/target key lookup belongs to a per-segment reader anymore.
	parsePatch := mockey.Mock(hookutil.GetEzIDByImportEzk).Return(int64(0), merr.ErrServiceInternal).Build()
	defer parsePatch.UnPatch()
	contextPatch := mockey.Mock(hookutil.GetCPluginContextByEzID).Return(nil, merr.ErrServiceInternal).Build()
	defer contextPatch.UnPatch()
	encryptionPatch := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	defer encryptionPatch.UnPatch()
	fieldIDsPatch := mockey.Mock(packed.GetManifestFieldIDs).
		Return(map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}, 100: {}}, nil).Build()
	defer fieldIDsPatch.UnPatch()

	var capturedContext *indexcgopb.StoragePluginContext
	innerReaderPatch := mockey.Mock(storage.NewRecordReaderFromManifest).To(
		func(_ string,
			schema *schemapb.CollectionSchema,
			_ int64,
			_ *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			_ ...storage.RwOption,
		) (storage.RecordReader, error) {
			capturedContext = pluginContext
			require.False(t, typeutil.HasTextField(schema), "absent target-only TEXT must not select the LOB reader")
			record, err := storage.ValueSerializer([]*storage.Value{{Value: map[int64]any{
				common.RowIDField: int64(1), common.TimeStampField: int64(100), 100: int64(42),
			}}}, schema)
			return &storageV3DeltaRecordReader{record: record}, err
		}).Build()
	defer innerReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()

	for _, tc := range []struct {
		name       string
		encryption SourceEncryption
		addedText  bool
	}{
		{"cmek", SourceEncryption{Encrypted: true, PluginContext: expectedContext}, false},
		{"cmek_nullable_text", SourceEncryption{Encrypted: true, PluginContext: expectedContext}, true},
		{"cmek_disabled_plugin", SourceEncryption{Encrypted: true}, false},
		{"plaintext", SourceEncryption{}, false},
		{"plaintext_nullable_text", SourceEncryption{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}, Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "99"}}}
			if tc.addedText {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID: 101, Name: "target_only_text", DataType: schemapb.DataType_Text, Nullable: true,
				})
			}
			capturedContext = &indexcgopb.StoragePluginContext{EncryptionZoneId: 99}
			r, err := NewStorageV3ManifestReader(context.Background(), nil, schema, &indexpb.StorageConfig{},
				manifestPath, 0, math.MaxUint64, 1024, tc.encryption, nil, nil)
			require.NoError(t, err)
			defer r.Close()
			require.Equal(t, tc.encryption.PluginContext, capturedContext, "an explicit nil must not fall back to the target key")
			data, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, int64(42), data.Data[100].GetRow(0))
			require.NotContains(t, data.Data, int64(101), "Import fills the absent nullable field later")
			require.Equal(t, tc.addedText, typeutil.HasTextField(schema), "keep the target schema for NULL filling")
		})
	}
	require.Zero(t, parsePatch.Times())
	require.Zero(t, contextPatch.Times())
}

func TestStorageV3Reader_CMEKWithTextIsUnsupported(t *testing.T) {
	patchStorageV3TestFieldIDs(t, 100, 101)
	openPatch := mockey.Mock(storage.NewManifestRecordReader).To(func(_ context.Context, _ string,
		_ *schemapb.CollectionSchema, _ ...storage.RwOption,
	) (storage.RecordReader, error) {
		t.Fatal("encrypted TEXT must be rejected before opening the data reader")
		return nil, nil
	}).Build()
	defer openPatch.UnPatch()
	for _, nullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("present_text_nullable=%t", nullable), func(t *testing.T) {
			_, err := NewStorageV3ManifestReader(
				context.Background(),
				nil,
				&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, DataType: schemapb.DataType_Text, Nullable: nullable},
				}},
				&indexpb.StorageConfig{},
				packed.MarshalManifestPath("snapshot/files/segment/10", 7),
				0,
				math.MaxUint64,
				1024,
				SourceEncryption{Encrypted: true},
				nil,
				nil,
			)
			assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
			assert.ErrorContains(t, err, "TEXT/LOB")
		})
	}
}

func TestStorageV3Reader_RejectsLegacyPathInput(t *testing.T) {
	r := &reader{storageVersion: storage.StorageV3}
	err := r.init([]string{"backup/insert_log/1/2/3"}, 0, math.MaxUint64)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.ErrorContains(t, err, "exact manifest from a snapshot source")
}

func TestStorageV3Reader_CollectsManifestReferencedFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	partitionPath := path.Join(root, "backup/insert_log/1/2")
	segmentPath := path.Join(partitionPath, "10")
	manifestPath := packed.MarshalManifestPath(segmentPath, 7)
	segmentDataPath := path.Join(segmentPath, "_data/0.parquet")
	orphanDataPath := path.Join(segmentPath, "_data/orphan.parquet")
	lobWithSizePath := path.Join(partitionPath, "lobs/101/_data/a.vx")
	lobWithoutSizePath := path.Join(partitionPath, "lobs/101/_data/b.vx")
	cm := storage.NewLocalChunkManager()
	assert.NoError(t, cm.Write(ctx, segmentDataPath, []byte("segment-10")))
	assert.NoError(t, cm.Write(ctx, orphanDataPath, []byte("orphan")))
	assert.NoError(t, cm.Write(ctx, lobWithoutSizePath, []byte("lob")))

	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).To(
		func(gotManifest string, _ *indexpb.StorageConfig, columns []string) ([]packed.Fragment, error) {
			assert.Equal(t, manifestPath, gotManifest)
			assert.Nil(t, columns)
			return []packed.Fragment{{FilePath: segmentDataPath}}, nil
		}).Build()
	defer fragmentsPatch.UnPatch()

	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{
		{Path: lobWithSizePath, FileSizeBytes: 64},
		{Path: lobWithoutSizePath},
	}, nil).Build()
	defer lobPatch.UnPatch()

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{DataType: schemapb.DataType_Text}}},
		storageVersion: storage.StorageV3,
		fileSize:       atomic.NewInt64(0),
	}
	assert.NoError(t, r.collectStorageV3Files(manifestPath))
	assert.ElementsMatch(t, []string{
		segmentDataPath,
		lobWithoutSizePath,
	}, r.storageV3Files)
	assert.NotContains(t, r.storageV3Files, orphanDataPath)
	assert.Equal(t, int64(64), r.storageV3LobSize)
	size, err := r.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(len("segment-10")+len("lob"))+64, size)
}

func TestStorageV3Reader_RejectsInvalidManifestSizeMetadata(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("backup/insert_log/1/2/3", 7)
	tests := []struct {
		name          string
		fragments     []packed.Fragment
		lobFiles      []packed.LobFileInfo
		fragmentErr   error
		lobErr        error
		wantError     string
		dataIntegrity bool
	}{
		{
			name:        "data fragment read failure",
			fragmentErr: errors.New("fragment read failed"),
			wantError:   "failed to read StorageV3 data files from manifest",
		},
		{
			name:          "data fragment without path",
			fragments:     []packed.Fragment{{}},
			wantError:     "data fragment without a path",
			dataIntegrity: true,
		},
		{
			name:      "LOB metadata read failure",
			lobErr:    errors.New("LOB metadata read failed"),
			wantError: "failed to read StorageV3 LOB files from manifest",
		},
		{
			name:          "LOB file without path",
			lobFiles:      []packed.LobFileInfo{{FileSizeBytes: 1}},
			wantError:     "LOB file without a path",
			dataIntegrity: true,
		},
		{
			name:          "LOB file with negative size",
			lobFiles:      []packed.LobFileInfo{{Path: "lobs/1", FileSizeBytes: -1}},
			wantError:     "with negative size",
			dataIntegrity: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
				Return(test.fragments, test.fragmentErr).Build()
			defer fragmentsPatch.UnPatch()
			lobPatch := mockey.Mock(packed.GetManifestLobFiles).
				Return(test.lobFiles, test.lobErr).Build()
			defer lobPatch.UnPatch()

			r := &reader{ctx: context.Background(), schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{DataType: schemapb.DataType_Text}}}}
			err := r.collectStorageV3Files(manifestPath)
			assert.ErrorContains(t, err, test.wantError)
			if test.dataIntegrity {
				assert.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		})
	}
}

func TestStorageV3Reader_InvalidSourceManifest(t *testing.T) {
	paramtable.Init()
	patchStorageV3TestFieldIDs(t)
	for _, manifest := range []string{"", "not-json", packed.MarshalManifestPath("snapshot/segment/10", packed.ManifestLatest)} {
		t.Run(manifest, func(t *testing.T) {
			r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
				manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}, nil)
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.Nil(t, r)
		})
	}
	manifest := packed.MarshalManifestPath("snapshot/segment/10", 7)
	openErr := merr.WrapErrIoKeyNotFound("missing source object")
	fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
	defer fragments.UnPatch()
	delta := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer delta.UnPatch()
	openPatch := mockey.Mock(storage.NewManifestRecordReader).Return(nil, openErr).Build()
	defer openPatch.UnPatch()
	r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
		manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}, nil)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	require.Nil(t, r)
}

func TestStorageV3Reader_DeleteFilterRequiresPrimaryKey(t *testing.T) {
	f, err := FilterWithDelete(&reader{schema: &schemapb.CollectionSchema{}})
	require.Error(t, err)
	require.Nil(t, f)
}

func TestDeltaLogListing_RetryOnTransientError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	insertPrefix := "backup/insert_log/seg/"
	deltaPrefix := "backup/delta_log/seg/"

	// Insert walk succeeds immediately with one field
	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "0/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "1/file1"})
			return nil
		}).Once()

	// Delta walk: first call returns partial results + transient error, second call succeeds with empty result
	// Empty result triggers early return (len(deltaLogs) == 0) so readDelete is never called.
	deltaCallCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			deltaCallCount++
			if deltaCallCount == 1 {
				walkFunc(&storage.ChunkObjectInfo{FilePath: deltaPrefix + "partial"})
				return errors.New("net/http: timeout awaiting response headers")
			}
			// Second attempt: empty walk (no delta logs) — triggers early nil return
			return nil
		}).Times(2)

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}}},
		storageVersion: storage.StorageV1,
		retryAttempts:  5,
	}

	err := r.init([]string{insertPrefix, deltaPrefix}, 0, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, 2, deltaCallCount, "delta log WalkWithPrefix should have retried on transient error")
}

func TestDeltaLogListing_NonRetryableErrorFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	insertPrefix := "backup/insert_log/seg/"
	deltaPrefix := "backup/delta_log/seg/"

	// Insert walk succeeds
	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "0/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "1/file1"})
			return nil
		}).Once()

	// Delta walk: return non-retryable error
	deltaCallCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			deltaCallCount++
			return merr.WrapErrIoPermissionDenied(deltaPrefix, errors.New("access denied"))
		}).Once()

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}}},
		storageVersion: storage.StorageV1,
		retryAttempts:  5,
	}

	err := r.init([]string{insertPrefix, deltaPrefix}, 0, math.MaxUint64)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, 1, deltaCallCount, "non-retryable error should not retry")
}

func TestMultiReadWithRetry_NonRetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cm := mocks.NewChunkManager(t)
	callCount := 0
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			callCount++
			return nil, merr.WrapErrIoPermissionDenied("test/path", fmt.Errorf("access denied"))
		})

	r := &reader{ctx: ctx, cm: cm, retryAttempts: 3}
	_, err := r.multiReadWithRetry(ctx, []string{"test/path"})
	assert.Error(t, err)
	assert.True(t, merr.IsNonRetryableErr(err))
	assert.Equal(t, 1, callCount, "non-retryable error should not be retried")
}

func TestMultiReadWithRetry_RetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cm := mocks.NewChunkManager(t)
	callCount := 0
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			callCount++
			if callCount < 3 {
				return nil, merr.WrapErrIoFailed("test/path", fmt.Errorf("transient error"))
			}
			return [][]byte{[]byte("data")}, nil
		})

	r := &reader{ctx: ctx, cm: cm, retryAttempts: 3}
	result, err := r.multiReadWithRetry(ctx, []string{"test/path"})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("data")}, result)
	assert.Equal(t, 3, callCount, "retryable error should be retried until success")
}
