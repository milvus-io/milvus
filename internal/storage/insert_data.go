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
	"encoding/binary"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO: fill it
// info for each blob
type BlobInfo struct {
	Length int
}

// InsertData example row_schema: {float_field, int_field, float_vector_field, string_field}
// Data {<0, row_id>, <1, timestamp>, <100, float_field>, <101, int_field>, <102, float_vector_field>, <103, string_field>}
//
// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...
type InsertData struct {
	// TODO, data should be zero copy by passing data directly to event reader or change Data to map[FieldID]FieldDataArray
	Data  map[FieldID]FieldData // field id to field data
	Infos []BlobInfo
}

func NewInsertData(schema *schemapb.CollectionSchema) (*InsertData, error) {
	return NewInsertDataWithCap(schema, 0)
}

func NewInsertDataWithCap(schema *schemapb.CollectionSchema, cap int) (*InsertData, error) {
	if schema == nil {
		return nil, merr.WrapErrParameterMissing("collection schema")
	}

	idata := &InsertData{
		Data: make(map[FieldID]FieldData),
	}

	for _, field := range schema.GetFields() {
		fieldData, err := NewFieldData(field.DataType, field, cap)
		if err != nil {
			return nil, err
		}
		idata.Data[field.FieldID] = fieldData
	}
	return idata, nil
}

func (iData *InsertData) IsEmpty() bool {
	if iData == nil {
		return true
	}

	timeFieldData, ok := iData.Data[common.TimeStampField]
	return (!ok) || (timeFieldData.RowNum() <= 0)
}

func (i *InsertData) GetRowNum() int {
	if i == nil || i.Data == nil || len(i.Data) == 0 {
		return 0
	}
	var rowNum int
	for _, data := range i.Data {
		rowNum = data.RowNum()
		if rowNum > 0 {
			break
		}
	}
	return rowNum
}

func (i *InsertData) GetMemorySize() int {
	var size int
	if i.Data == nil || len(i.Data) == 0 {
		return size
	}

	for _, data := range i.Data {
		size += data.GetMemorySize()
	}

	return size
}

func (i *InsertData) Append(row map[FieldID]interface{}) error {
	for fID, v := range row {
		field, ok := i.Data[fID]
		if !ok {
			return fmt.Errorf("Missing field when appending row, got %d", fID)
		}

		if err := field.AppendRow(v); err != nil {
			return merr.WrapErrParameterInvalidMsg(fmt.Sprintf("append data for field %d failed, err=%s", fID, err.Error()))
		}
	}

	return nil
}

func (i *InsertData) GetRow(idx int) map[FieldID]interface{} {
	res := make(map[FieldID]interface{})
	for field, data := range i.Data {
		res[field] = data.GetRow(idx)
	}
	return res
}

func (i *InsertData) GetRowSize(idx int) int {
	size := 0
	for _, data := range i.Data {
		size += data.GetRowSize(idx)
	}
	return size
}

// FieldData defines field data interface
type FieldData interface {
	GetMemorySize() int
	RowNum() int
	GetRow(i int) any
	GetRowSize(i int) int
	GetRows() any
	AppendRow(row interface{}) error
	AppendRows(rows interface{}) error
	GetDataType() schemapb.DataType
}

func NewFieldData(dataType schemapb.DataType, fieldSchema *schemapb.FieldSchema, cap int) (FieldData, error) {
	typeParams := fieldSchema.GetTypeParams()
	switch dataType {
	case schemapb.DataType_Float16Vector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &Float16VectorFieldData{
			Data: make([]byte, 0, cap),
			Dim:  dim,
		}, nil
	case schemapb.DataType_BFloat16Vector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &BFloat16VectorFieldData{
			Data: make([]byte, 0, cap),
			Dim:  dim,
		}, nil
	case schemapb.DataType_FloatVector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &FloatVectorFieldData{
			Data: make([]float32, 0, cap),
			Dim:  dim,
		}, nil
	case schemapb.DataType_BinaryVector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		return &BinaryVectorFieldData{
			Data: make([]byte, 0, cap),
			Dim:  dim,
		}, nil
	case schemapb.DataType_SparseFloatVector:
		return &SparseFloatVectorFieldData{}, nil
	case schemapb.DataType_Bool:
		return &BoolFieldData{
			Data: make([]bool, 0, cap),
		}, nil

	case schemapb.DataType_Int8:
		return &Int8FieldData{
			Data: make([]int8, 0, cap),
		}, nil

	case schemapb.DataType_Int16:
		return &Int16FieldData{
			Data: make([]int16, 0, cap),
		}, nil

	case schemapb.DataType_Int32:
		return &Int32FieldData{
			Data: make([]int32, 0, cap),
		}, nil

	case schemapb.DataType_Int64:
		return &Int64FieldData{
			Data: make([]int64, 0, cap),
		}, nil
	case schemapb.DataType_Float:
		return &FloatFieldData{
			Data: make([]float32, 0, cap),
		}, nil

	case schemapb.DataType_Double:
		return &DoubleFieldData{
			Data: make([]float64, 0, cap),
		}, nil
	case schemapb.DataType_JSON:
		return &JSONFieldData{
			Data: make([][]byte, 0, cap),
		}, nil
	case schemapb.DataType_Array:
		return &ArrayFieldData{
			Data:        make([]*schemapb.ScalarField, 0, cap),
			ElementType: fieldSchema.GetElementType(),
		}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return &StringFieldData{
			Data:     make([]string, 0, cap),
			DataType: dataType,
		}, nil
	default:
		return nil, fmt.Errorf("Unexpected schema data type: %d", dataType)
	}
}

type BoolFieldData struct {
	Data []bool
}
type Int8FieldData struct {
	Data []int8
}
type Int16FieldData struct {
	Data []int16
}
type Int32FieldData struct {
	Data []int32
}
type Int64FieldData struct {
	Data []int64
}
type FloatFieldData struct {
	Data []float32
}
type DoubleFieldData struct {
	Data []float64
}
type StringFieldData struct {
	Data     []string
	DataType schemapb.DataType
}
type ArrayFieldData struct {
	ElementType schemapb.DataType
	Data        []*schemapb.ScalarField
}
type JSONFieldData struct {
	Data [][]byte
}
type BinaryVectorFieldData struct {
	Data []byte
	Dim  int
}
type FloatVectorFieldData struct {
	Data []float32
	Dim  int
}
type Float16VectorFieldData struct {
	Data []byte
	Dim  int
}
type BFloat16VectorFieldData struct {
	Data []byte
	Dim  int
}

type SparseFloatVectorFieldData struct {
	schemapb.SparseFloatArray
}

func (dst *SparseFloatVectorFieldData) AppendAllRows(src *SparseFloatVectorFieldData) {
	if len(src.Contents) == 0 {
		return
	}
	if dst.Dim < src.Dim {
		dst.Dim = src.Dim
	}
	dst.Contents = append(dst.Contents, src.Contents...)
}

// RowNum implements FieldData.RowNum
func (data *BoolFieldData) RowNum() int          { return len(data.Data) }
func (data *Int8FieldData) RowNum() int          { return len(data.Data) }
func (data *Int16FieldData) RowNum() int         { return len(data.Data) }
func (data *Int32FieldData) RowNum() int         { return len(data.Data) }
func (data *Int64FieldData) RowNum() int         { return len(data.Data) }
func (data *FloatFieldData) RowNum() int         { return len(data.Data) }
func (data *DoubleFieldData) RowNum() int        { return len(data.Data) }
func (data *StringFieldData) RowNum() int        { return len(data.Data) }
func (data *ArrayFieldData) RowNum() int         { return len(data.Data) }
func (data *JSONFieldData) RowNum() int          { return len(data.Data) }
func (data *BinaryVectorFieldData) RowNum() int  { return len(data.Data) * 8 / data.Dim }
func (data *FloatVectorFieldData) RowNum() int   { return len(data.Data) / data.Dim }
func (data *Float16VectorFieldData) RowNum() int { return len(data.Data) / 2 / data.Dim }
func (data *BFloat16VectorFieldData) RowNum() int {
	return len(data.Data) / 2 / data.Dim
}
func (data *SparseFloatVectorFieldData) RowNum() int { return len(data.Contents) }

// GetRow implements FieldData.GetRow
func (data *BoolFieldData) GetRow(i int) any   { return data.Data[i] }
func (data *Int8FieldData) GetRow(i int) any   { return data.Data[i] }
func (data *Int16FieldData) GetRow(i int) any  { return data.Data[i] }
func (data *Int32FieldData) GetRow(i int) any  { return data.Data[i] }
func (data *Int64FieldData) GetRow(i int) any  { return data.Data[i] }
func (data *FloatFieldData) GetRow(i int) any  { return data.Data[i] }
func (data *DoubleFieldData) GetRow(i int) any { return data.Data[i] }
func (data *StringFieldData) GetRow(i int) any { return data.Data[i] }
func (data *ArrayFieldData) GetRow(i int) any  { return data.Data[i] }
func (data *JSONFieldData) GetRow(i int) any   { return data.Data[i] }
func (data *BinaryVectorFieldData) GetRow(i int) any {
	return data.Data[i*data.Dim/8 : (i+1)*data.Dim/8]
}

func (data *SparseFloatVectorFieldData) GetRow(i int) interface{} {
	return data.Contents[i]
}

func (data *FloatVectorFieldData) GetRow(i int) interface{} {
	return data.Data[i*data.Dim : (i+1)*data.Dim]
}

func (data *Float16VectorFieldData) GetRow(i int) interface{} {
	return data.Data[i*data.Dim*2 : (i+1)*data.Dim*2]
}

func (data *BFloat16VectorFieldData) GetRow(i int) interface{} {
	return data.Data[i*data.Dim*2 : (i+1)*data.Dim*2]
}

func (data *BoolFieldData) GetRows() any              { return data.Data }
func (data *Int8FieldData) GetRows() any              { return data.Data }
func (data *Int16FieldData) GetRows() any             { return data.Data }
func (data *Int32FieldData) GetRows() any             { return data.Data }
func (data *Int64FieldData) GetRows() any             { return data.Data }
func (data *FloatFieldData) GetRows() any             { return data.Data }
func (data *DoubleFieldData) GetRows() any            { return data.Data }
func (data *StringFieldData) GetRows() any            { return data.Data }
func (data *ArrayFieldData) GetRows() any             { return data.Data }
func (data *JSONFieldData) GetRows() any              { return data.Data }
func (data *BinaryVectorFieldData) GetRows() any      { return data.Data }
func (data *FloatVectorFieldData) GetRows() any       { return data.Data }
func (data *Float16VectorFieldData) GetRows() any     { return data.Data }
func (data *BFloat16VectorFieldData) GetRows() any    { return data.Data }
func (data *SparseFloatVectorFieldData) GetRows() any { return data.Contents }

// AppendRow implements FieldData.AppendRow
func (data *BoolFieldData) AppendRow(row interface{}) error {
	v, ok := row.(bool)
	if !ok {
		return merr.WrapErrParameterInvalid("bool", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int8FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int8)
	if !ok {
		return merr.WrapErrParameterInvalid("int8", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int16FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int16)
	if !ok {
		return merr.WrapErrParameterInvalid("int16", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int32FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int32)
	if !ok {
		return merr.WrapErrParameterInvalid("int32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int64FieldData) AppendRow(row interface{}) error {
	v, ok := row.(int64)
	if !ok {
		return merr.WrapErrParameterInvalid("int64", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *FloatFieldData) AppendRow(row interface{}) error {
	v, ok := row.(float32)
	if !ok {
		return merr.WrapErrParameterInvalid("float32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *DoubleFieldData) AppendRow(row interface{}) error {
	v, ok := row.(float64)
	if !ok {
		return merr.WrapErrParameterInvalid("float64", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *StringFieldData) AppendRow(row interface{}) error {
	v, ok := row.(string)
	if !ok {
		return merr.WrapErrParameterInvalid("string", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *ArrayFieldData) AppendRow(row interface{}) error {
	v, ok := row.(*schemapb.ScalarField)
	if !ok {
		return merr.WrapErrParameterInvalid("*schemapb.ScalarField", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *JSONFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *BinaryVectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim/8 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *FloatVectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]float32)
	if !ok || len(v) != data.Dim {
		return merr.WrapErrParameterInvalid("[]float32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Float16VectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim*2 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *BFloat16VectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim*2 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *SparseFloatVectorFieldData) AppendRow(row interface{}) error {
	v, ok := row.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("SparseFloatVectorRowData", row, "Wrong row type")
	}
	if err := typeutil.ValidateSparseFloatRows(v); err != nil {
		return err
	}
	rowDim := typeutil.SparseFloatRowDim(v)
	if data.Dim < rowDim {
		data.Dim = rowDim
	}
	data.Contents = append(data.Contents, v)
	return nil
}

func (data *BoolFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int8FieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]int8)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int8", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int16FieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]int16)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int16", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int32FieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]int32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int32", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int64FieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]int64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int64", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *FloatFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]float32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float32", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *DoubleFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]float64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float64", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *StringFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]string)
	if !ok {
		return merr.WrapErrParameterInvalid("[]string", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *ArrayFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]*schemapb.ScalarField)
	if !ok {
		return merr.WrapErrParameterInvalid("[]*schemapb.ScalarField", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *JSONFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([][]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[][]byte", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// AppendRows appends FLATTEN vectors to field data.
func (data *BinaryVectorFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", rows, "Wrong rows type")
	}
	if len(v)%(data.Dim/8) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim/8, len(v), "Wrong vector size")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// AppendRows appends FLATTEN vectors to field data.
func (data *FloatVectorFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]float32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float32", rows, "Wrong rows type")
	}
	if len(v)%(data.Dim) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim, len(v), "Wrong vector size")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// AppendRows appends FLATTEN vectors to field data.
func (data *Float16VectorFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", rows, "Wrong rows type")
	}
	if len(v)%(data.Dim*2) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim*2, len(v), "Wrong vector size")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// AppendRows appends FLATTEN vectors to field data.
func (data *BFloat16VectorFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", rows, "Wrong rows type")
	}
	if len(v)%(data.Dim*2) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim*2, len(v), "Wrong vector size")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *SparseFloatVectorFieldData) AppendRows(rows interface{}) error {
	v, ok := rows.(*SparseFloatVectorFieldData)
	if !ok {
		return merr.WrapErrParameterInvalid("SparseFloatVectorFieldData", rows, "Wrong rows type")
	}
	data.Contents = append(data.SparseFloatArray.Contents, v.Contents...)
	if data.Dim < v.Dim {
		data.Dim = v.Dim
	}
	return nil
}

// GetMemorySize implements FieldData.GetMemorySize
func (data *BoolFieldData) GetMemorySize() int           { return binary.Size(data.Data) }
func (data *Int8FieldData) GetMemorySize() int           { return binary.Size(data.Data) }
func (data *Int16FieldData) GetMemorySize() int          { return binary.Size(data.Data) }
func (data *Int32FieldData) GetMemorySize() int          { return binary.Size(data.Data) }
func (data *Int64FieldData) GetMemorySize() int          { return binary.Size(data.Data) }
func (data *FloatFieldData) GetMemorySize() int          { return binary.Size(data.Data) }
func (data *DoubleFieldData) GetMemorySize() int         { return binary.Size(data.Data) }
func (data *BinaryVectorFieldData) GetMemorySize() int   { return binary.Size(data.Data) + 4 }
func (data *FloatVectorFieldData) GetMemorySize() int    { return binary.Size(data.Data) + 4 }
func (data *Float16VectorFieldData) GetMemorySize() int  { return binary.Size(data.Data) + 4 }
func (data *BFloat16VectorFieldData) GetMemorySize() int { return binary.Size(data.Data) + 4 }

func (data *SparseFloatVectorFieldData) GetMemorySize() int {
	// TODO(SPARSE): should this be the memory size of serialzied size?
	return proto.Size(&data.SparseFloatArray)
}

// GetDataType implements FieldData.GetDataType
func (data *BoolFieldData) GetDataType() schemapb.DataType   { return schemapb.DataType_Bool }
func (data *Int8FieldData) GetDataType() schemapb.DataType   { return schemapb.DataType_Int8 }
func (data *Int16FieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Int16 }
func (data *Int32FieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Int32 }
func (data *Int64FieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Int64 }
func (data *FloatFieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Float }
func (data *DoubleFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Double }
func (data *StringFieldData) GetDataType() schemapb.DataType { return data.DataType }
func (data *ArrayFieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Array }
func (data *JSONFieldData) GetDataType() schemapb.DataType   { return schemapb.DataType_JSON }
func (data *BinaryVectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_BinaryVector
}

func (data *FloatVectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_FloatVector
}

func (data *Float16VectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Float16Vector
}

func (data *BFloat16VectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_BFloat16Vector
}

func (data *SparseFloatVectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_SparseFloatVector
}

// why not binary.Size(data) directly? binary.Size(data) return -1
// binary.Size returns how many bytes Write would generate to encode the value v, which
// must be a fixed-size value or a slice of fixed-size values, or a pointer to such data.
// If v is neither of these, binary.Size returns -1.
func (data *StringFieldData) GetMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size
}

func (data *ArrayFieldData) GetMemorySize() int {
	var size int
	for _, val := range data.Data {
		switch data.ElementType {
		case schemapb.DataType_Bool:
			size += binary.Size(val.GetBoolData().GetData())
		case schemapb.DataType_Int8:
			size += binary.Size(val.GetIntData().GetData()) / 4
		case schemapb.DataType_Int16:
			size += binary.Size(val.GetIntData().GetData()) / 2
		case schemapb.DataType_Int32:
			size += binary.Size(val.GetIntData().GetData())
		case schemapb.DataType_Int64:
			size += binary.Size(val.GetLongData().GetData())
		case schemapb.DataType_Float:
			size += binary.Size(val.GetFloatData().GetData())
		case schemapb.DataType_Double:
			size += binary.Size(val.GetDoubleData().GetData())
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			size += (&StringFieldData{Data: val.GetStringData().GetData()}).GetMemorySize()
		}
	}
	return size
}

func (data *JSONFieldData) GetMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size
}

func (data *BoolFieldData) GetRowSize(i int) int           { return 1 }
func (data *Int8FieldData) GetRowSize(i int) int           { return 1 }
func (data *Int16FieldData) GetRowSize(i int) int          { return 2 }
func (data *Int32FieldData) GetRowSize(i int) int          { return 4 }
func (data *Int64FieldData) GetRowSize(i int) int          { return 8 }
func (data *FloatFieldData) GetRowSize(i int) int          { return 4 }
func (data *DoubleFieldData) GetRowSize(i int) int         { return 8 }
func (data *BinaryVectorFieldData) GetRowSize(i int) int   { return data.Dim / 8 }
func (data *FloatVectorFieldData) GetRowSize(i int) int    { return data.Dim * 4 }
func (data *Float16VectorFieldData) GetRowSize(i int) int  { return data.Dim * 2 }
func (data *BFloat16VectorFieldData) GetRowSize(i int) int { return data.Dim * 2 }
func (data *StringFieldData) GetRowSize(i int) int         { return len(data.Data[i]) + 16 }
func (data *JSONFieldData) GetRowSize(i int) int           { return len(data.Data[i]) + 16 }
func (data *ArrayFieldData) GetRowSize(i int) int {
	switch data.ElementType {
	case schemapb.DataType_Bool:
		return binary.Size(data.Data[i].GetBoolData().GetData())
	case schemapb.DataType_Int8:
		return binary.Size(data.Data[i].GetIntData().GetData()) / 4
	case schemapb.DataType_Int16:
		return binary.Size(data.Data[i].GetIntData().GetData()) / 2
	case schemapb.DataType_Int32:
		return binary.Size(data.Data[i].GetIntData().GetData())
	case schemapb.DataType_Int64:
		return binary.Size(data.Data[i].GetLongData().GetData())
	case schemapb.DataType_Float:
		return binary.Size(data.Data[i].GetFloatData().GetData())
	case schemapb.DataType_Double:
		return binary.Size(data.Data[i].GetDoubleData().GetData())
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return (&StringFieldData{Data: data.Data[i].GetStringData().GetData()}).GetMemorySize()
	}
	return 0
}

func (data *SparseFloatVectorFieldData) GetRowSize(i int) int {
	return len(data.Contents[i])
}
