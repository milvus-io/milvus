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

package storage

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestCheckTsField(t *testing.T) {
	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	assert.False(t, checkTsField(data))

	data.Data[common.TimeStampField] = &BoolFieldData{}
	assert.False(t, checkTsField(data))

	data.Data[common.TimeStampField] = &Int64FieldData{}
	assert.True(t, checkTsField(data))
}

func TestCheckRowIDField(t *testing.T) {
	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	assert.False(t, checkRowIDField(data))

	data.Data[common.RowIDField] = &BoolFieldData{}
	assert.False(t, checkRowIDField(data))

	data.Data[common.RowIDField] = &Int64FieldData{}
	assert.True(t, checkRowIDField(data))
}

func TestCheckNumRows(t *testing.T) {
	assert.True(t, checkNumRows())

	f1 := &Int64FieldData{
		Data: []int64{1, 2, 3},
	}
	f2 := &Int64FieldData{
		Data: []int64{1, 2, 3},
	}
	f3 := &Int64FieldData{
		Data: []int64{1, 2, 3, 4},
	}

	assert.True(t, checkNumRows(f1, f2))
	assert.False(t, checkNumRows(f1, f3))
	assert.False(t, checkNumRows(f2, f3))
	assert.False(t, checkNumRows(f1, f2, f3))
}

func TestSortFieldDataList(t *testing.T) {
	f1 := &Int16FieldData{
		Data: []int16{1, 2, 3},
	}
	f2 := &Int32FieldData{
		Data: []int32{4, 5, 6},
	}
	f3 := &Int64FieldData{
		Data: []int64{7, 8, 9},
	}

	ls := fieldDataList{
		IDs:   []FieldID{1, 3, 2},
		datas: []FieldData{f1, f3, f2},
	}

	assert.Equal(t, 3, ls.Len())
	sortFieldDataList(ls)
	assert.ElementsMatch(t, []FieldID{1, 2, 3}, ls.IDs)
	assert.ElementsMatch(t, []FieldData{f1, f2, f3}, ls.datas)
}

func TestTransferColumnBasedInsertDataToRowBased(t *testing.T) {
	var err error

	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}

	// no ts
	_, _, _, err = TransferColumnBasedInsertDataToRowBased(data)
	assert.Error(t, err)

	tss := &Int64FieldData{
		Data: []int64{1, 2, 3},
	}
	data.Data[common.TimeStampField] = tss

	// no row ids
	_, _, _, err = TransferColumnBasedInsertDataToRowBased(data)
	assert.Error(t, err)

	rowIDsF := &Int64FieldData{
		Data: []int64{1, 2, 3, 4},
	}
	data.Data[common.RowIDField] = rowIDsF

	// row num mismatch
	_, _, _, err = TransferColumnBasedInsertDataToRowBased(data)
	assert.Error(t, err)

	data.Data[common.RowIDField] = &Int64FieldData{
		Data: []int64{1, 2, 3},
	}

	f1 := &BoolFieldData{
		Data: []bool{true, false, true},
	}
	f2 := &Int8FieldData{
		Data: []int8{0, 0xf, 0x1f},
	}
	f3 := &Int16FieldData{
		Data: []int16{0, 0xff, 0x1fff},
	}
	f4 := &Int32FieldData{
		Data: []int32{0, 0xffff, 0x1fffffff},
	}
	f5 := &Int64FieldData{
		Data: []int64{0, 0xffffffff, 0x1fffffffffffffff},
	}
	f6 := &FloatFieldData{
		Data: []float32{0, 0, 0},
	}
	f7 := &DoubleFieldData{
		Data: []float64{0, 0, 0},
	}
	// maybe we cannot support string now, no matter what the length of string is fixed or not.
	// f8 := &StringFieldData{
	// 	Data: []string{"1", "2", "3"},
	// }
	f9 := &BinaryVectorFieldData{
		Dim:  8,
		Data: []byte{1, 2, 3},
	}
	f10 := &FloatVectorFieldData{
		Dim:  1,
		Data: []float32{0, 0, 0},
	}
	f11 := &Float16VectorFieldData{
		Dim:  1,
		Data: []byte{1, 1, 2, 2, 3, 3},
	}
	f12 := &BFloat16VectorFieldData{
		Dim:  1,
		Data: []byte{1, 1, 2, 2, 3, 3},
	}
	f13 := &Int8VectorFieldData{
		Dim:  1,
		Data: []int8{1, 2, 3},
	}

	data.Data[101] = f1
	data.Data[102] = f2
	data.Data[103] = f3
	data.Data[104] = f4
	data.Data[105] = f5
	data.Data[106] = f6
	data.Data[107] = f7
	// data.Data[108] = f8
	data.Data[109] = f9
	data.Data[110] = f10
	data.Data[111] = f11
	data.Data[112] = f12
	data.Data[113] = f13

	utss, rowIDs, rows, err := TransferColumnBasedInsertDataToRowBased(data)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []uint64{1, 2, 3}, utss)
	assert.ElementsMatch(t, []int64{1, 2, 3}, rowIDs)
	assert.Equal(t, 3, len(rows))
	// b := []byte("1")[0]
	if common.Endian == binary.LittleEndian {
		// low byte in high address

		assert.ElementsMatch(t,
			[]byte{
				1,    // true
				0,    // 0
				0, 0, // 0
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 1, // "1"
				1,          // 1
				0, 0, 0, 0, // 0
				1, 1,
				1, 1,
				1,
			},
			rows[0].Value)
		assert.ElementsMatch(t,
			[]byte{
				0,       // false
				0xf,     // 0xf
				0, 0xff, // 0xff
				0, 0, 0xff, 0xff, // 0xffff
				0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, // 0xffffffff
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 2, // "2"
				2,          // 2
				0, 0, 0, 0, // 0
				2, 2,
				2, 2,
				2,
			},
			rows[1].Value)
		assert.ElementsMatch(t,
			[]byte{
				1,          // true
				0x1f,       // 0x1f
				0xff, 0x1f, // 0x1fff
				0xff, 0xff, 0xff, 0x1f, // 0x1fffffff
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, // 0x1fffffffffffffff
				0, 0, 0, 0, // 0
				0, 0, 0, 0, 0, 0, 0, 0, // 0
				// b + 3, // "3"
				3,          // 3
				0, 0, 0, 0, // 0
				3, 3,
				3, 3,
				3,
			},
			rows[2].Value)
	}
}

func TestGetDimFromParams(t *testing.T) {
	dim := 8
	params1 := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: strconv.Itoa(dim),
		},
	}
	got, err := GetDimFromParams(params1)
	assert.NoError(t, err)
	assert.Equal(t, dim, got)

	params2 := []*commonpb.KeyValuePair{
		{
			Key:   common.DimKey,
			Value: "not in int format",
		},
	}
	_, err = GetDimFromParams(params2)
	assert.Error(t, err)

	params3 := []*commonpb.KeyValuePair{
		{
			Key:   "not dim",
			Value: strconv.Itoa(dim),
		},
	}
	_, err = GetDimFromParams(params3)
	assert.Error(t, err)
}

func TestReadBinary(t *testing.T) {
	reader := bytes.NewReader(
		[]byte{
			1,          // true
			0x1f,       // 0x1f
			0xff, 0x1f, // 0x1fff
			0xff, 0xff, 0xff, 0x1f, // 0x1fffffff
			0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f, // 0x1fffffffffffffff
			// hard to compare float value
			// 0, 0, 0, 0, // 0
			// 0, 0, 0, 0, 0, 0, 0, 0, // 0
			// b + 3, // "3"
			3, // 3
			// 0, 0, 0, 0, // 0
		},
	)

	if common.Endian == binary.LittleEndian {
		var b bool
		ReadBinary(reader, &b, schemapb.DataType_Bool)
		assert.True(t, b)

		var i8 int8
		ReadBinary(reader, &i8, schemapb.DataType_Int8)
		assert.Equal(t, int8(0x1f), i8)

		var i16 int16
		ReadBinary(reader, &i16, schemapb.DataType_Int16)
		assert.Equal(t, int16(0x1fff), i16)

		var i32 int32
		ReadBinary(reader, &i32, schemapb.DataType_Int32)
		assert.Equal(t, int32(0x1fffffff), i32)

		var i64 int64
		ReadBinary(reader, &i64, schemapb.DataType_Int64)
		assert.Equal(t, int64(0x1fffffffffffffff), i64)

		bvec := make([]byte, 1)
		ReadBinary(reader, &bvec, schemapb.DataType_BinaryVector)
		assert.Equal(t, []byte{3}, bvec)

		// should print error here, no content in reader.
		ReadBinary(reader, &bvec, schemapb.DataType_BinaryVector)
	}
}

func genAllFieldsSchema(dim int, withSparse bool) (schema *schemapb.CollectionSchema, pkFieldID UniqueID, fieldIDs []UniqueID) {
	schema = &schemapb.CollectionSchema{
		Name:        "all_fields_schema",
		Description: "all_fields_schema",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				DataType: schemapb.DataType_Bool,
			},
			{
				DataType: schemapb.DataType_Int8,
			},
			{
				DataType: schemapb.DataType_Int16,
			},
			{
				DataType: schemapb.DataType_Int32,
			},
			{
				DataType: schemapb.DataType_Float,
			},
			{
				DataType: schemapb.DataType_Double,
			},
			{
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_Array,
			},
			{
				DataType: schemapb.DataType_JSON,
			},
		},
	}
	if withSparse {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			DataType: schemapb.DataType_SparseFloatVector,
		})
	}
	fieldIDs = make([]UniqueID, 0)
	for idx := range schema.Fields {
		fID := int64(common.StartOfUserFieldID + idx)
		schema.Fields[idx].FieldID = fID
		if schema.Fields[idx].IsPrimaryKey {
			pkFieldID = fID
		}
		fieldIDs = append(fieldIDs, fID)
	}
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      common.RowIDField,
		Name:         common.RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
	})
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      common.TimeStampField,
		Name:         common.TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
	})
	return schema, pkFieldID, fieldIDs
}

func genAllFieldsSchemaNullable(dim int, withSparse bool) (schema *schemapb.CollectionSchema, pkFieldID UniqueID, fieldIDs []UniqueID) {
	schema = &schemapb.CollectionSchema{
		Name:        "all_fields_schema_nullable",
		Description: "all_fields_schema_nullable",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				DataType: schemapb.DataType_Bool,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_Int8,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_Int16,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_Int32,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_Int64,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_Float,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_Double,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
				},
			},
			{
				DataType: schemapb.DataType_Array,
				Nullable: true,
			},
			{
				DataType: schemapb.DataType_JSON,
				Nullable: true,
			},
		},
	}
	if withSparse {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			DataType: schemapb.DataType_SparseFloatVector,
		})
	}
	fieldIDs = make([]UniqueID, 0)
	for idx := range schema.Fields {
		fID := int64(common.StartOfUserFieldID + idx)
		schema.Fields[idx].FieldID = fID
		if schema.Fields[idx].IsPrimaryKey {
			pkFieldID = fID
		}
		fieldIDs = append(fieldIDs, fID)
	}
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      common.RowIDField,
		Name:         common.RowIDFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
	})
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      common.TimeStampField,
		Name:         common.TimeStampFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
	})
	return schema, pkFieldID, fieldIDs
}

func generateInt32ArrayList(numRows int) []*schemapb.ScalarField {
	ret := make([]*schemapb.ScalarField, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{
					Data: []int32{rand.Int31(), rand.Int31()},
				},
			},
		})
	}
	return ret
}

func genRowWithAllFields(dim int) (blob *commonpb.Blob, pk int64, row []interface{}) {
	schema, _, _ := genAllFieldsSchema(dim, true)
	ret := &commonpb.Blob{
		Value: nil,
	}
	row = make([]interface{}, 0)
	for _, field := range schema.Fields {
		var buffer bytes.Buffer
		switch field.DataType {
		case schemapb.DataType_FloatVector:
			fVec := testutils.GenerateFloatVectors(1, dim)
			_ = binary.Write(&buffer, common.Endian, fVec)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, fVec)
		case schemapb.DataType_BinaryVector:
			bVec := testutils.GenerateBinaryVectors(1, dim)
			_ = binary.Write(&buffer, common.Endian, bVec)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, bVec)
		case schemapb.DataType_Float16Vector:
			f16Vec := testutils.GenerateFloat16Vectors(1, dim)
			_ = binary.Write(&buffer, common.Endian, f16Vec)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, f16Vec)
		case schemapb.DataType_BFloat16Vector:
			bf16Vec := testutils.GenerateBFloat16Vectors(1, dim)
			_ = binary.Write(&buffer, common.Endian, bf16Vec)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, bf16Vec)
		case schemapb.DataType_Int8Vector:
			iVec := testutils.GenerateInt8Vectors(1, dim)
			_ = binary.Write(&buffer, common.Endian, iVec)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, iVec)
		case schemapb.DataType_Bool:
			data := rand.Int()%2 == 0
			_ = binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_Int8:
			data := int8(rand.Int())
			_ = binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_Int16:
			data := int16(rand.Int())
			_ = binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_Int32:
			data := int32(rand.Int())
			_ = binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_Int64:
			pk = int64(rand.Int())
			_ = binary.Write(&buffer, common.Endian, pk)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, pk)
		case schemapb.DataType_Float:
			data := rand.Float32()
			_ = binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_Double:
			data := rand.Float64()
			_ = binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_Array:
			data := &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3},
					},
				},
			}
			bytes, _ := proto.Marshal(data)
			binary.Write(&buffer, common.Endian, bytes)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		case schemapb.DataType_JSON:
			data := []byte(`{"key":"value"}`)
			binary.Write(&buffer, common.Endian, data)
			ret.Value = append(ret.Value, buffer.Bytes()...)
			row = append(row, data)
		}
	}
	return ret, pk, row
}

func genRowBasedInsertMsg(numRows, dim int) (msg *msgstream.InsertMsg, pks []int64, columns [][]interface{}) {
	msg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            nil,
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     nil,
			MsgPosition:    nil,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			RowData: nil,
			Version: msgpb.InsertDataVersion_RowBased,
		},
	}
	pks = make([]int64, 0)
	raws := make([][]interface{}, 0)
	for i := 0; i < numRows; i++ {
		row, pk, raw := genRowWithAllFields(dim)
		msg.InsertRequest.RowData = append(msg.InsertRequest.RowData, row)
		pks = append(pks, pk)
		raws = append(raws, raw)
	}
	numColumns := len(raws[0])
	columns = make([][]interface{}, numColumns)
	for _, raw := range raws {
		for j, data := range raw {
			columns[j] = append(columns[j], data)
		}
	}
	return msg, pks, columns
}

func genColumnBasedInsertMsg(schema *schemapb.CollectionSchema, numRows, dim int) (msg *msgstream.InsertMsg, pks []int64, columns [][]interface{}) {
	msg = &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            nil,
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     nil,
			MsgPosition:    nil,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			FieldsData: nil,
			NumRows:    uint64(numRows),
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     lo.RepeatBy(numRows, func(idx int) int64 { return int64(idx) }),
			Timestamps: lo.RepeatBy(numRows, func(idx int) uint64 { return uint64(idx) }),
		},
	}
	pks = make([]int64, 0)
	columns = make([][]interface{}, len(schema.Fields))

	for idx, field := range schema.Fields {
		switch field.DataType {
		case schemapb.DataType_Bool:
			data := testutils.GenerateBoolArray(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}
		case schemapb.DataType_Int8:
			data := testutils.GenerateInt32Array(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], int8(d))
			}
		case schemapb.DataType_Int16:
			data := testutils.GenerateInt32Array(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], int16(d))
			}
		case schemapb.DataType_Int32:
			data := testutils.GenerateInt32Array(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}
		case schemapb.DataType_Int64:
			data := testutils.GenerateInt64Array(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}
			pks = data
		case schemapb.DataType_Float:
			data := testutils.GenerateFloat32Array(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}
		case schemapb.DataType_Double:
			data := testutils.GenerateFloat64Array(numRows)
			f := &schemapb.FieldData{
				Type:      field.DataType,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}
		case schemapb.DataType_FloatVector:
			data := testutils.GenerateFloatVectors(numRows, dim)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_FloatVector,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for nrows := 0; nrows < numRows; nrows++ {
				columns[idx] = append(columns[idx], data[nrows*dim:(nrows+1)*dim])
			}
		case schemapb.DataType_BinaryVector:
			data := testutils.GenerateBinaryVectors(numRows, dim)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_BinaryVector,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: data,
						},
					},
				},
				FieldId: field.FieldID,
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for nrows := 0; nrows < numRows; nrows++ {
				columns[idx] = append(columns[idx], data[nrows*dim/8:(nrows+1)*dim/8])
			}
		case schemapb.DataType_Float16Vector:
			data := testutils.GenerateFloat16Vectors(numRows, dim)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_Float16Vector,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: data,
						},
					},
				},
				FieldId: field.FieldID,
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for nrows := 0; nrows < numRows; nrows++ {
				columns[idx] = append(columns[idx], data[nrows*dim*2:(nrows+1)*dim*2])
			}
		case schemapb.DataType_BFloat16Vector:
			data := testutils.GenerateBFloat16Vectors(numRows, dim)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_BFloat16Vector,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: data,
						},
					},
				},
				FieldId: field.FieldID,
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for nrows := 0; nrows < numRows; nrows++ {
				columns[idx] = append(columns[idx], data[nrows*dim*2:(nrows+1)*dim*2])
			}
		case schemapb.DataType_SparseFloatVector:
			data := testutils.GenerateSparseFloatVectors(numRows)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_SparseFloatVector,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: data.Dim,
						Data: &schemapb.VectorField_SparseFloatVector{
							SparseFloatVector: data,
						},
					},
				},
				FieldId: field.FieldID,
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for nrows := 0; nrows < numRows; nrows++ {
				columns[idx] = append(columns[idx], data.Contents[nrows])
			}
		case schemapb.DataType_Int8Vector:
			data := testutils.GenerateInt8Vectors(numRows, dim)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_Int8Vector,
				FieldName: field.Name,
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: typeutil.Int8ArrayToBytes(data),
						},
					},
				},
				FieldId: field.FieldID,
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for nrows := 0; nrows < numRows; nrows++ {
				columns[idx] = append(columns[idx], data[nrows*dim:(nrows+1)*dim])
			}
		case schemapb.DataType_Array:
			data := generateInt32ArrayList(numRows)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_Array,
				FieldName: field.GetName(),
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{
							ArrayData: &schemapb.ArrayArray{
								Data:        data,
								ElementType: schemapb.DataType_Int32,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}

		case schemapb.DataType_JSON:
			data := testutils.GenerateBytesArray(numRows)
			f := &schemapb.FieldData{
				Type:      schemapb.DataType_Array,
				FieldName: field.GetName(),
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_JsonData{
							JsonData: &schemapb.JSONArray{
								Data: data,
							},
						},
					},
				},
				FieldId: field.FieldID,
			}
			if field.GetNullable() {
				f.ValidData = testutils.GenerateBoolArray(numRows)
			}
			msg.FieldsData = append(msg.FieldsData, f)
			for _, d := range data {
				columns[idx] = append(columns[idx], d)
			}
		}
	}

	return msg, pks, columns
}

func TestRowBasedInsertMsgToInsertData(t *testing.T) {
	numRows, dim := 10, 8
	schema, _, fieldIDs := genAllFieldsSchema(dim, false)
	fieldIDs = fieldIDs[:len(fieldIDs)-2]
	msg, _, columns := genRowBasedInsertMsg(numRows, dim)

	idata, err := RowBasedInsertMsgToInsertData(msg, schema, false)
	assert.NoError(t, err)
	for idx, fID := range fieldIDs {
		column := columns[idx]
		fData, ok := idata.Data[fID]
		assert.True(t, ok)
		assert.Equal(t, len(column), fData.RowNum())
		for j := range column {
			assert.Equal(t, fData.GetRow(j), column[j])
		}
	}
}

func TestRowBasedTransferInsertMsgToInsertRecord(t *testing.T) {
	numRows, dim := 10, 8
	schema, _, _ := genAllFieldsSchema(dim, false)
	msg, _, _ := genRowBasedInsertMsg(numRows, dim)

	_, err := TransferInsertMsgToInsertRecord(schema, msg)
	assert.NoError(t, err)
}

func TestRowBasedInsertMsgToInsertFloat16VectorDataError(t *testing.T) {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            nil,
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     nil,
			MsgPosition:    nil,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			RowData: nil,
			Version: msgpb.InsertDataVersion_RowBased,
		},
	}
	schema := &schemapb.CollectionSchema{
		Name:        "float16_vector_fields_schema",
		Description: "float16_vector_fields_schema",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				DataType: schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: common.DimKey,
					},
				},
			},
		},
	}
	_, err := RowBasedInsertMsgToInsertData(msg, schema, false)
	assert.Error(t, err)
}

func TestRowBasedInsertMsgToInsertBFloat16VectorDataError(t *testing.T) {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            nil,
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     nil,
			MsgPosition:    nil,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			RowData: nil,
			Version: msgpb.InsertDataVersion_RowBased,
		},
	}
	schema := &schemapb.CollectionSchema{
		Name:        "bfloat16_vector_fields_schema",
		Description: "bfloat16_vector_fields_schema",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				DataType: schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: common.DimKey,
					},
				},
			},
		},
	}
	_, err := RowBasedInsertMsgToInsertData(msg, schema, false)
	assert.Error(t, err)
}

func TestColumnBasedInsertMsgToInsertData(t *testing.T) {
	numRows, dim := 2, 8
	schema, _, fieldIDs := genAllFieldsSchema(dim, true)
	msg, _, columns := genColumnBasedInsertMsg(schema, numRows, dim)

	idata, err := ColumnBasedInsertMsgToInsertData(msg, schema)
	assert.NoError(t, err)
	for idx, fID := range fieldIDs {
		column := columns[idx]
		fData, ok := idata.Data[fID]
		assert.True(t, ok)
		assert.Equal(t, len(column), fData.RowNum())
		for j := range column {
			assert.Equal(t, fData.GetRow(j), column[j])
		}
	}
}

func TestColumnBasedInsertMsgToInsertDataNullable(t *testing.T) {
	numRows, dim := 2, 8
	schema, _, fieldIDs := genAllFieldsSchemaNullable(dim, true)
	msg, _, columns := genColumnBasedInsertMsg(schema, numRows, dim)

	idata, err := ColumnBasedInsertMsgToInsertData(msg, schema)
	assert.NoError(t, err)
	for idx, fID := range fieldIDs {
		column := columns[idx]
		fData, ok := idata.Data[fID]
		assert.True(t, ok)
		assert.Equal(t, len(column), fData.RowNum())
		for j := range column {
			assert.Equal(t, fData.GetRow(j), column[j])
		}
	}
}

func TestColumnBasedInsertMsgToInsertFloat16VectorDataError(t *testing.T) {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            nil,
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     nil,
			MsgPosition:    nil,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			FieldsData: nil,
			NumRows:    uint64(2),
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}
	schema := &schemapb.CollectionSchema{
		Name:        "float16_vector_fields_schema",
		Description: "float16_vector_fields_schema",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				DataType: schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: common.DimKey,
					},
				},
			},
		},
	}
	_, err := ColumnBasedInsertMsgToInsertData(msg, schema)
	assert.Error(t, err)
}

func TestColumnBasedInsertMsgToInsertBFloat16VectorDataError(t *testing.T) {
	msg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:            nil,
			BeginTimestamp: 0,
			EndTimestamp:   0,
			HashValues:     nil,
			MsgPosition:    nil,
		},
		InsertRequest: &msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Insert,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  0,
			},
			FieldsData: nil,
			NumRows:    uint64(2),
			Version:    msgpb.InsertDataVersion_ColumnBased,
		},
	}
	schema := &schemapb.CollectionSchema{
		Name:        "bfloat16_vector_fields_schema",
		Description: "bfloat16_vector_fields_schema",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				DataType: schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key: common.DimKey,
					},
				},
			},
		},
	}
	_, err := ColumnBasedInsertMsgToInsertData(msg, schema)
	assert.Error(t, err)
}

func TestInsertMsgToInsertData(t *testing.T) {
	numRows, dim := 10, 8
	schema, _, fieldIDs := genAllFieldsSchema(dim, false)
	fieldIDs = fieldIDs[:len(fieldIDs)-2]
	msg, _, columns := genRowBasedInsertMsg(numRows, dim)

	idata, err := InsertMsgToInsertData(msg, schema)
	assert.NoError(t, err)
	for idx, fID := range fieldIDs {
		column := columns[idx]
		fData, ok := idata.Data[fID]
		assert.True(t, ok, "fID =", fID)
		assert.Equal(t, len(column), fData.RowNum())
		for j := range column {
			assert.Equal(t, fData.GetRow(j), column[j])
		}
	}
}

func TestInsertMsgToInsertData2(t *testing.T) {
	numRows, dim := 2, 8
	schema, _, fieldIDs := genAllFieldsSchema(dim, true)
	msg, _, columns := genColumnBasedInsertMsg(schema, numRows, dim)

	idata, err := InsertMsgToInsertData(msg, schema)
	assert.NoError(t, err)
	for idx, fID := range fieldIDs {
		column := columns[idx]
		fData, ok := idata.Data[fID]
		assert.True(t, ok)
		assert.Equal(t, len(column), fData.RowNum())
		for j := range column {
			assert.Equal(t, fData.GetRow(j), column[j])
		}
	}
}

func TestMergeInsertData(t *testing.T) {
	t.Run("empty data in buffer", func(t *testing.T) {
		d1 := &InsertData{
			Data:  make(map[FieldID]FieldData),
			Infos: []BlobInfo{},
		}
		d2 := &InsertData{
			Data: map[int64]FieldData{
				common.RowIDField: &Int64FieldData{
					Data: []int64{2},
				},
				common.TimeStampField: &Int64FieldData{
					Data: []int64{2},
				},
				BoolField: &BoolFieldData{
					Data: []bool{false},
				},
				Int8Field: &Int8FieldData{
					Data: []int8{2},
				},
				Int16Field: &Int16FieldData{
					Data: []int16{2},
				},
				Int32Field: &Int32FieldData{
					Data: []int32{2},
				},
				Int64Field: &Int64FieldData{
					Data: []int64{2},
				},
				FloatField: &FloatFieldData{
					Data: []float32{0},
				},
				DoubleField: &DoubleFieldData{
					Data: []float64{0},
				},
				StringField: &StringFieldData{
					Data: []string{"2"},
				},
				BinaryVectorField: &BinaryVectorFieldData{
					Data: []byte{0},
					Dim:  8,
				},
				FloatVectorField: &FloatVectorFieldData{
					Data: []float32{0},
					Dim:  1,
				},
				Float16VectorField: &Float16VectorFieldData{
					Data: []byte{2, 3},
					Dim:  1,
				},
				BFloat16VectorField: &BFloat16VectorFieldData{
					Data: []byte{2, 3},
					Dim:  1,
				},
				SparseFloatVectorField: &SparseFloatVectorFieldData{
					SparseFloatArray: schemapb.SparseFloatArray{
						Dim: 600,
						Contents: [][]byte{
							typeutil.CreateSparseFloatRow([]uint32{170, 300, 579}, []float32{3.1, 3.2, 3.3}),
						},
					},
				},
				Int8VectorField: &Int8VectorFieldData{
					Data: []int8{0},
					Dim:  1,
				},
				ArrayField: &ArrayFieldData{
					Data: []*schemapb.ScalarField{
						{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{
									Data: []int32{4, 5, 6},
								},
							},
						},
					},
				},
				JSONField: &JSONFieldData{
					Data: [][]byte{[]byte(`{"hello":"world"}`)},
				},
			},
			Infos: nil,
		}

		MergeInsertData(d1, d2)

		f, ok := d1.Data[common.RowIDField]
		assert.True(t, ok)
		assert.Equal(t, []int64{2}, f.(*Int64FieldData).Data)

		f, ok = d1.Data[common.TimeStampField]
		assert.True(t, ok)
		assert.Equal(t, []int64{2}, f.(*Int64FieldData).Data)

		f, ok = d1.Data[BoolField]
		assert.True(t, ok)
		assert.Equal(t, []bool{false}, f.(*BoolFieldData).Data)

		f, ok = d1.Data[Int8Field]
		assert.True(t, ok)
		assert.Equal(t, []int8{2}, f.(*Int8FieldData).Data)

		f, ok = d1.Data[Int16Field]
		assert.True(t, ok)
		assert.Equal(t, []int16{2}, f.(*Int16FieldData).Data)

		f, ok = d1.Data[Int32Field]
		assert.True(t, ok)
		assert.Equal(t, []int32{2}, f.(*Int32FieldData).Data)

		f, ok = d1.Data[Int64Field]
		assert.True(t, ok)
		assert.Equal(t, []int64{2}, f.(*Int64FieldData).Data)

		f, ok = d1.Data[FloatField]
		assert.True(t, ok)
		assert.Equal(t, []float32{0}, f.(*FloatFieldData).Data)

		f, ok = d1.Data[DoubleField]
		assert.True(t, ok)
		assert.Equal(t, []float64{0}, f.(*DoubleFieldData).Data)

		f, ok = d1.Data[StringField]
		assert.True(t, ok)
		assert.Equal(t, []string{"2"}, f.(*StringFieldData).Data)

		f, ok = d1.Data[BinaryVectorField]
		assert.True(t, ok)
		assert.Equal(t, []byte{0}, f.(*BinaryVectorFieldData).Data)

		f, ok = d1.Data[FloatVectorField]
		assert.True(t, ok)
		assert.Equal(t, []float32{0}, f.(*FloatVectorFieldData).Data)

		f, ok = d1.Data[Float16VectorField]
		assert.True(t, ok)
		assert.Equal(t, []byte{2, 3}, f.(*Float16VectorFieldData).Data)

		f, ok = d1.Data[BFloat16VectorField]
		assert.True(t, ok)
		assert.Equal(t, []byte{2, 3}, f.(*BFloat16VectorFieldData).Data)

		f, ok = d1.Data[SparseFloatVectorField]
		assert.True(t, ok)
		assert.Equal(t, &SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim: 600,
				Contents: [][]byte{
					typeutil.CreateSparseFloatRow([]uint32{170, 300, 579}, []float32{3.1, 3.2, 3.3}),
				},
			},
		}, f.(*SparseFloatVectorFieldData))

		f, ok = d1.Data[Int8VectorField]
		assert.True(t, ok)
		assert.Equal(t, []int8{0}, f.(*Int8VectorFieldData).Data)

		f, ok = d1.Data[ArrayField]
		assert.True(t, ok)
		assert.Equal(t, []int32{4, 5, 6}, f.(*ArrayFieldData).Data[0].GetIntData().GetData())

		f, ok = d1.Data[JSONField]
		assert.True(t, ok)
		assert.EqualValues(t, [][]byte{[]byte(`{"hello":"world"}`)}, f.(*JSONFieldData).Data)
	})
	t.Run("normal case", func(t *testing.T) {
		d1 := &InsertData{
			Data: map[int64]FieldData{
				common.RowIDField: &Int64FieldData{
					Data: []int64{1},
				},
				common.TimeStampField: &Int64FieldData{
					Data: []int64{1},
				},
				BoolField: &BoolFieldData{
					Data: []bool{true},
				},
				Int8Field: &Int8FieldData{
					Data: []int8{1},
				},
				Int16Field: &Int16FieldData{
					Data: []int16{1},
				},
				Int32Field: &Int32FieldData{
					Data: []int32{1},
				},
				Int64Field: &Int64FieldData{
					Data: []int64{1},
				},
				FloatField: &FloatFieldData{
					Data: []float32{0},
				},
				DoubleField: &DoubleFieldData{
					Data: []float64{0},
				},
				StringField: &StringFieldData{
					Data: []string{"1"},
				},
				BinaryVectorField: &BinaryVectorFieldData{
					Data: []byte{0},
					Dim:  8,
				},
				FloatVectorField: &FloatVectorFieldData{
					Data: []float32{0},
					Dim:  1,
				},
				Float16VectorField: &Float16VectorFieldData{
					Data: []byte{0, 1},
					Dim:  1,
				},
				BFloat16VectorField: &BFloat16VectorFieldData{
					Data: []byte{0, 1},
					Dim:  1,
				},
				SparseFloatVectorField: &SparseFloatVectorFieldData{
					SparseFloatArray: schemapb.SparseFloatArray{
						Dim: 600,
						Contents: [][]byte{
							typeutil.CreateSparseFloatRow([]uint32{30, 41, 52}, []float32{1.1, 1.2, 1.3}),
							typeutil.CreateSparseFloatRow([]uint32{60, 80, 230}, []float32{2.1, 2.2, 2.3}),
						},
					},
				},
				Int8VectorField: &Int8VectorFieldData{
					Data: []int8{0},
					Dim:  1,
				},
				ArrayField: &ArrayFieldData{
					Data: []*schemapb.ScalarField{
						{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{
									Data: []int32{1, 2, 3},
								},
							},
						},
					},
				},
				JSONField: &JSONFieldData{
					Data: [][]byte{[]byte(`{"key":"value"}`)},
				},
			},
			Infos: nil,
		}
		d2 := &InsertData{
			Data: map[int64]FieldData{
				common.RowIDField: &Int64FieldData{
					Data: []int64{2},
				},
				common.TimeStampField: &Int64FieldData{
					Data: []int64{2},
				},
				BoolField: &BoolFieldData{
					Data: []bool{false},
				},
				Int8Field: &Int8FieldData{
					Data: []int8{2},
				},
				Int16Field: &Int16FieldData{
					Data: []int16{2},
				},
				Int32Field: &Int32FieldData{
					Data: []int32{2},
				},
				Int64Field: &Int64FieldData{
					Data: []int64{2},
				},
				FloatField: &FloatFieldData{
					Data: []float32{0},
				},
				DoubleField: &DoubleFieldData{
					Data: []float64{0},
				},
				StringField: &StringFieldData{
					Data: []string{"2"},
				},
				BinaryVectorField: &BinaryVectorFieldData{
					Data: []byte{0},
					Dim:  8,
				},
				FloatVectorField: &FloatVectorFieldData{
					Data: []float32{0},
					Dim:  1,
				},
				Float16VectorField: &Float16VectorFieldData{
					Data: []byte{2, 3},
					Dim:  1,
				},
				BFloat16VectorField: &BFloat16VectorFieldData{
					Data: []byte{2, 3},
					Dim:  1,
				},
				SparseFloatVectorField: &SparseFloatVectorFieldData{
					SparseFloatArray: schemapb.SparseFloatArray{
						Dim: 600,
						Contents: [][]byte{
							typeutil.CreateSparseFloatRow([]uint32{170, 300, 579}, []float32{3.1, 3.2, 3.3}),
						},
					},
				},
				Int8VectorField: &Int8VectorFieldData{
					Data: []int8{1},
					Dim:  1,
				},
				ArrayField: &ArrayFieldData{
					Data: []*schemapb.ScalarField{
						{
							Data: &schemapb.ScalarField_IntData{
								IntData: &schemapb.IntArray{
									Data: []int32{4, 5, 6},
								},
							},
						},
					},
				},
				JSONField: &JSONFieldData{
					Data: [][]byte{[]byte(`{"hello":"world"}`)},
				},
			},
			Infos: nil,
		}

		MergeInsertData(d1, d2)

		f, ok := d1.Data[common.RowIDField]
		assert.True(t, ok)
		assert.Equal(t, []int64{1, 2}, f.(*Int64FieldData).Data)

		f, ok = d1.Data[common.TimeStampField]
		assert.True(t, ok)
		assert.Equal(t, []int64{1, 2}, f.(*Int64FieldData).Data)

		f, ok = d1.Data[BoolField]
		assert.True(t, ok)
		assert.Equal(t, []bool{true, false}, f.(*BoolFieldData).Data)

		f, ok = d1.Data[Int8Field]
		assert.True(t, ok)
		assert.Equal(t, []int8{1, 2}, f.(*Int8FieldData).Data)

		f, ok = d1.Data[Int16Field]
		assert.True(t, ok)
		assert.Equal(t, []int16{1, 2}, f.(*Int16FieldData).Data)

		f, ok = d1.Data[Int32Field]
		assert.True(t, ok)
		assert.Equal(t, []int32{1, 2}, f.(*Int32FieldData).Data)

		f, ok = d1.Data[Int64Field]
		assert.True(t, ok)
		assert.Equal(t, []int64{1, 2}, f.(*Int64FieldData).Data)

		f, ok = d1.Data[FloatField]
		assert.True(t, ok)
		assert.Equal(t, []float32{0, 0}, f.(*FloatFieldData).Data)

		f, ok = d1.Data[DoubleField]
		assert.True(t, ok)
		assert.Equal(t, []float64{0, 0}, f.(*DoubleFieldData).Data)

		f, ok = d1.Data[StringField]
		assert.True(t, ok)
		assert.Equal(t, []string{"1", "2"}, f.(*StringFieldData).Data)

		f, ok = d1.Data[BinaryVectorField]
		assert.True(t, ok)
		assert.Equal(t, []byte{0, 0}, f.(*BinaryVectorFieldData).Data)

		f, ok = d1.Data[FloatVectorField]
		assert.True(t, ok)
		assert.Equal(t, []float32{0, 0}, f.(*FloatVectorFieldData).Data)

		f, ok = d1.Data[Float16VectorField]
		assert.True(t, ok)
		assert.Equal(t, []byte{0, 1, 2, 3}, f.(*Float16VectorFieldData).Data)

		f, ok = d1.Data[BFloat16VectorField]
		assert.True(t, ok)
		assert.Equal(t, []byte{0, 1, 2, 3}, f.(*BFloat16VectorFieldData).Data)

		f, ok = d1.Data[SparseFloatVectorField]
		assert.True(t, ok)
		assert.Equal(t, &SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim: 600,
				Contents: [][]byte{
					typeutil.CreateSparseFloatRow([]uint32{30, 41, 52}, []float32{1.1, 1.2, 1.3}),
					typeutil.CreateSparseFloatRow([]uint32{60, 80, 230}, []float32{2.1, 2.2, 2.3}),
					typeutil.CreateSparseFloatRow([]uint32{170, 300, 579}, []float32{3.1, 3.2, 3.3}),
				},
			},
		}, f.(*SparseFloatVectorFieldData))

		f, ok = d1.Data[Int8VectorField]
		assert.True(t, ok)
		assert.Equal(t, []int8{0, 1}, f.(*Int8VectorFieldData).Data)

		f, ok = d1.Data[ArrayField]
		assert.True(t, ok)
		assert.Equal(t, []int32{1, 2, 3}, f.(*ArrayFieldData).Data[0].GetIntData().GetData())
		assert.Equal(t, []int32{4, 5, 6}, f.(*ArrayFieldData).Data[1].GetIntData().GetData())

		f, ok = d1.Data[JSONField]
		assert.True(t, ok)
		assert.EqualValues(t, [][]byte{[]byte(`{"key":"value"}`), []byte(`{"hello":"world"}`)}, f.(*JSONFieldData).Data)
	})
}

func TestMergeFloat16VectorField(t *testing.T) {
	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	fid := FieldID(1)
	field := &Float16VectorFieldData{
		Data: []byte{0, 1},
		Dim:  1,
	}
	mergeFloat16VectorField(data, fid, field)
	expectedData := &Float16VectorFieldData{
		Data: []byte{0, 1},
		Dim:  1,
	}
	assert.Equal(t, expectedData, data.Data[fid])
}

func TestMergeBFloat16VectorField(t *testing.T) {
	data := &InsertData{
		Data: make(map[FieldID]FieldData),
	}
	fid := FieldID(1)
	field := &BFloat16VectorFieldData{
		Data: []byte{0, 1},
		Dim:  1,
	}
	mergeBFloat16VectorField(data, fid, field)
	expectedData := &BFloat16VectorFieldData{
		Data: []byte{0, 1},
		Dim:  1,
	}
	assert.Equal(t, expectedData, data.Data[fid])
}

func TestGetPkFromInsertData(t *testing.T) {
	var nilSchema *schemapb.CollectionSchema
	_, err := GetPkFromInsertData(nilSchema, nil)
	assert.Error(t, err)

	noPfSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "no_pf_schema",
				IsPrimaryKey: false,
				Description:  "no pf schema",
				DataType:     schemapb.DataType_Int64,
				AutoID:       false,
			},
		},
	}
	_, err = GetPkFromInsertData(noPfSchema, nil)
	assert.Error(t, err)

	pfSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pf_schema",
				IsPrimaryKey: true,
				Description:  "pf schema",
				DataType:     schemapb.DataType_Int64,
				AutoID:       false,
			},
		},
	}
	noPfData := &InsertData{
		Data:  map[FieldID]FieldData{},
		Infos: nil,
	}
	_, err = GetPkFromInsertData(pfSchema, noPfData)
	assert.Error(t, err)

	notInt64Data := &InsertData{
		Data: map[FieldID]FieldData{
			common.StartOfUserFieldID: &BoolFieldData{},
		},
		Infos: nil,
	}
	_, err = GetPkFromInsertData(pfSchema, notInt64Data)
	assert.Error(t, err)

	realInt64Data := &InsertData{
		Data: map[FieldID]FieldData{
			common.StartOfUserFieldID: &Int64FieldData{
				Data: []int64{1, 2, 3},
			},
		},
		Infos: nil,
	}
	d, err := GetPkFromInsertData(pfSchema, realInt64Data)
	assert.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, d.(*Int64FieldData).Data)
}

func Test_GetTimestampFromInsertData(t *testing.T) {
	type testCase struct {
		tag string

		data        *InsertData
		expectError bool
		expectData  *Int64FieldData
	}

	cases := []testCase{
		{
			tag:         "nil data",
			expectError: true,
		},
		{
			tag:         "no timestamp",
			expectError: true,
			data: &InsertData{
				Data: map[FieldID]FieldData{
					common.StartOfUserFieldID: &Int64FieldData{Data: []int64{1, 2, 3}},
				},
			},
		},
		{
			tag:         "timestamp wrong type",
			expectError: true,
			data: &InsertData{
				Data: map[FieldID]FieldData{
					common.TimeStampField: &Int32FieldData{Data: []int32{1, 2, 3}},
				},
			},
		},
		{
			tag: "normal insert data",
			data: &InsertData{
				Data: map[FieldID]FieldData{
					common.TimeStampField:     &Int64FieldData{Data: []int64{1, 2, 3}},
					common.StartOfUserFieldID: &Int32FieldData{Data: []int32{1, 2, 3}},
				},
			},
			expectData: &Int64FieldData{Data: []int64{1, 2, 3}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			result, err := GetTimestampFromInsertData(tc.data)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectData, result)
			}
		})
	}
}

func Test_boolFieldDataToBytes(t *testing.T) {
	field := &BoolFieldData{Data: []bool{true, false}}
	bs, err := boolFieldDataToPbBytes(field)
	assert.NoError(t, err)
	var arr schemapb.BoolArray
	err = proto.Unmarshal(bs, &arr)
	assert.NoError(t, err)
	assert.ElementsMatch(t, field.Data, arr.Data)
}

func Test_stringFieldDataToBytes(t *testing.T) {
	field := &StringFieldData{Data: []string{"true", "false"}}
	bs, err := stringFieldDataToPbBytes(field)
	assert.NoError(t, err)
	var arr schemapb.StringArray
	err = proto.Unmarshal(bs, &arr)
	assert.NoError(t, err)
	assert.ElementsMatch(t, field.Data, arr.Data)
}

func binaryRead(endian binary.ByteOrder, bs []byte, receiver interface{}) error {
	reader := bytes.NewReader(bs)
	return binary.Read(reader, endian, receiver)
}

func TestJson(t *testing.T) {
	extras := make(map[string]string)
	extras["IndexBuildID"] = "10"
	extras["KEY"] = "IVF_1"
	ExtraBytes, err := json.Marshal(extras)
	assert.NoError(t, err)
	ExtraLength := int32(len(ExtraBytes))

	t.Log(string(ExtraBytes))
	t.Log(ExtraLength)
}

func TestBM25Checker(t *testing.T) {
	f1 := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
	}
	f2 := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_BM25,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"sparse"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{103},
	}

	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector",
				DataType:         schemapb.DataType_FloatVector,
				IsFunctionOutput: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
			{
				FieldID: 103, Name: "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{f1, f2},
	}

	for _, field := range schema.Fields {
		isBm25 := IsBM25FunctionOutputField(field, schema)
		if field.FieldID == 103 {
			assert.True(t, isBm25)
		} else {
			assert.False(t, isBm25)
		}
	}
}
