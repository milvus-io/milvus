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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// FieldData represents the data of a field.
//
// Memory tracking is disabled by default. For variable-width fields, pass true as
// the optional final constructor argument to enable it for an initially empty field.
// All Append* methods then update the cached size, including data appended before an error,
// and GetMemorySize returns that cache. With tracking disabled, GetMemorySize calculates the current size by scanning the data.
// Fixed-width scalar and dense vector fields always calculate their sizes in constant time;
// they do not store a tracking flag or maintain a size cache.
//
// With tracking enabled, do not modify struct fields or backing data returned by getters directly,
// as those changes bypass accounting. The mode is fixed at creation.
type FieldData interface {
	RowNum() int

	// get size
	GetRowSize(i int) int
	GetMemorySize() int

	// get data/data type
	GetRow(i int) any
	GetDataRows() any
	GetDataType() schemapb.DataType
	GetNullable() bool
	GetValidData() []bool

	// append data
	AppendRow(row interface{}) error
	AppendRows(dataRows interface{}, validDataRows interface{}) error
	AppendDataRows(rows interface{}) error
	AppendValidDataRows(rows interface{}) error
}

// NewFieldData creates an empty FieldData. The optional final flag enables memory tracking; omitted or false leaves tracking disabled.
// Cap is expressed in rows;
// vector columns scale it by their per-row element count, except nullable columns,
// which reserve only cap: a nullable column missing from the source
// only appends ValidData (see AppendNullableDefaultFieldsData),
// so a dim-scaled Data reservation would stay invisible to GetMemorySize and never trigger spill.
func NewFieldData(dataType schemapb.DataType, fieldSchema *schemapb.FieldSchema, cap int, trackMemory ...bool) (FieldData, error) {
	nullable := fieldSchema.GetNullable()
	switch dataType {
	case schemapb.DataType_Bool:
		return NewBoolFieldData(nullable, cap), nil
	case schemapb.DataType_Int8:
		return NewInt8FieldData(nullable, cap), nil
	case schemapb.DataType_Int16:
		return NewInt16FieldData(nullable, cap), nil
	case schemapb.DataType_Int32:
		return NewInt32FieldData(nullable, cap), nil
	case schemapb.DataType_Int64:
		return NewInt64FieldData(nullable, cap), nil
	case schemapb.DataType_Float:
		return NewFloatFieldData(nullable, cap), nil
	case schemapb.DataType_Double:
		return NewDoubleFieldData(nullable, cap), nil
	case schemapb.DataType_Timestamptz:
		return NewTimestamptzFieldData(nullable, cap), nil
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return NewStringFieldData(dataType, nullable, cap, trackMemory...), nil
	case schemapb.DataType_JSON:
		return NewJSONFieldData(nullable, cap, trackMemory...), nil
	case schemapb.DataType_Geometry:
		return NewGeometryFieldData(nullable, cap, trackMemory...), nil
	case schemapb.DataType_Array:
		return NewArrayFieldData(fieldSchema.GetElementType(), nullable, fieldSchema.GetElementNullable(), cap, trackMemory...), nil
	case schemapb.DataType_BinaryVector:
		dim, err := GetDimFromParams(fieldSchema.GetTypeParams())
		if err != nil {
			return nil, err
		}
		return NewBinaryVectorFieldData(dim, nullable, cap), nil
	case schemapb.DataType_FloatVector:
		dim, err := GetDimFromParams(fieldSchema.GetTypeParams())
		if err != nil {
			return nil, err
		}
		return NewFloatVectorFieldData(dim, nullable, cap), nil
	case schemapb.DataType_Float16Vector:
		dim, err := GetDimFromParams(fieldSchema.GetTypeParams())
		if err != nil {
			return nil, err
		}
		return NewFloat16VectorFieldData(dim, nullable, cap), nil
	case schemapb.DataType_BFloat16Vector:
		dim, err := GetDimFromParams(fieldSchema.GetTypeParams())
		if err != nil {
			return nil, err
		}
		return NewBFloat16VectorFieldData(dim, nullable, cap), nil
	case schemapb.DataType_Int8Vector:
		dim, err := GetDimFromParams(fieldSchema.GetTypeParams())
		if err != nil {
			return nil, err
		}
		return NewInt8VectorFieldData(dim, nullable, cap), nil
	case schemapb.DataType_SparseFloatVector:
		return NewSparseFloatVectorFieldData(nullable, cap, trackMemory...), nil
	case schemapb.DataType_ArrayOfVector:
		dim, err := GetDimFromParams(fieldSchema.GetTypeParams())
		if err != nil {
			return nil, err
		}
		return NewVectorArrayFieldData(fieldSchema.GetElementType(), int64(dim), nullable, fieldSchema.GetElementNullable(), cap, trackMemory...), nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("unexpected schema data type: %d", dataType)
	}
}

// --------------- Fixed-width scalar fields.  begin  -----------

type BoolFieldData struct {
	Data      []bool
	ValidData []bool
	Nullable  bool
}

// NewBoolFieldData creates an empty field. Its memory size is computed in constant time.
func NewBoolFieldData(nullable bool, capacity int) *BoolFieldData {
	data := &BoolFieldData{Data: make([]bool, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

// RowNum implements FieldData.RowNum
func (data *BoolFieldData) RowNum() int { return len(data.Data) }

func (data *BoolFieldData) GetRowSize(i int) int { return 1 }

// GetMemorySize implements FieldData.GetMemorySize
func (data *BoolFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

// GetRow implements FieldData.GetRow
func (data *BoolFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *BoolFieldData) GetDataRows() any { return data.Data }

// GetDataType implements FieldData.GetDataType
func (data *BoolFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Bool }

func (data *BoolFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *BoolFieldData) GetValidData() []bool { return data.ValidData }

// AppendRow implements FieldData.AppendRow
func (data *BoolFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]bool, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(bool)
	if !ok {
		return merr.WrapErrParameterInvalid("bool", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

func (data *BoolFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *BoolFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *BoolFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type Int8FieldData struct {
	Data      []int8
	ValidData []bool
	Nullable  bool
}

// NewInt8FieldData creates an empty field. Its memory size is computed in constant time.
func NewInt8FieldData(nullable bool, capacity int) *Int8FieldData {
	data := &Int8FieldData{Data: make([]int8, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *Int8FieldData) RowNum() int { return len(data.Data) }

func (data *Int8FieldData) GetRowSize(i int) int { return 1 }

func (data *Int8FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int8FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int8FieldData) GetDataRows() any { return data.Data }

func (data *Int8FieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Int8 }

func (data *Int8FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int8FieldData) GetValidData() []bool { return data.ValidData }

func (data *Int8FieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]int8, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(int8)
	if !ok {
		return merr.WrapErrParameterInvalid("int8", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int8FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int8FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int8)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int8", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int8FieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type Int16FieldData struct {
	Data      []int16
	ValidData []bool
	Nullable  bool
}

// NewInt16FieldData creates an empty field. Its memory size is computed in constant time.
func NewInt16FieldData(nullable bool, capacity int) *Int16FieldData {
	data := &Int16FieldData{Data: make([]int16, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *Int16FieldData) RowNum() int { return len(data.Data) }

func (data *Int16FieldData) GetRowSize(i int) int { return 2 }

func (data *Int16FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int16FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int16FieldData) GetDataRows() any { return data.Data }

func (data *Int16FieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Int16 }

func (data *Int16FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int16FieldData) GetValidData() []bool { return data.ValidData }

func (data *Int16FieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]int16, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(int16)
	if !ok {
		return merr.WrapErrParameterInvalid("int16", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int16FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int16FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int16)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int16", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int16FieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type Int32FieldData struct {
	Data      []int32
	ValidData []bool
	Nullable  bool
}

// NewInt32FieldData creates an empty field. Its memory size is computed in constant time.
func NewInt32FieldData(nullable bool, capacity int) *Int32FieldData {
	data := &Int32FieldData{Data: make([]int32, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *Int32FieldData) RowNum() int { return len(data.Data) }

func (data *Int32FieldData) GetRowSize(i int) int { return 4 }

func (data *Int32FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int32FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int32FieldData) GetDataRows() any { return data.Data }

func (data *Int32FieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Int32 }

func (data *Int32FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int32FieldData) GetValidData() []bool { return data.ValidData }

func (data *Int32FieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]int32, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(int32)
	if !ok {
		return merr.WrapErrParameterInvalid("int32", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int32FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int32FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int32", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int32FieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type Int64FieldData struct {
	Data      []int64
	ValidData []bool
	Nullable  bool
}

// NewInt64FieldData creates an empty field. Its memory size is computed in constant time.
func NewInt64FieldData(nullable bool, capacity int) *Int64FieldData {
	data := &Int64FieldData{Data: make([]int64, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *Int64FieldData) RowNum() int { return len(data.Data) }

func (data *Int64FieldData) GetRowSize(i int) int { return 8 }

func (data *Int64FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int64FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int64FieldData) GetDataRows() any { return data.Data }

func (data *Int64FieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Int64 }

func (data *Int64FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int64FieldData) GetValidData() []bool { return data.ValidData }

func (data *Int64FieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]int64, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(int64)
	if !ok {
		return merr.WrapErrParameterInvalid("int64", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *Int64FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int64FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int64", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int64FieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type FloatFieldData struct {
	Data      []float32
	ValidData []bool
	Nullable  bool
}

// NewFloatFieldData creates an empty field. Its memory size is computed in constant time.
func NewFloatFieldData(nullable bool, capacity int) *FloatFieldData {
	data := &FloatFieldData{Data: make([]float32, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *FloatFieldData) RowNum() int { return len(data.Data) }

func (data *FloatFieldData) GetRowSize(i int) int { return 4 }

func (data *FloatFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *FloatFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *FloatFieldData) GetDataRows() any { return data.Data }

func (data *FloatFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Float }

func (data *FloatFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *FloatFieldData) GetValidData() []bool { return data.ValidData }

func (data *FloatFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]float32, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(float32)
	if !ok {
		return merr.WrapErrParameterInvalid("float32", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *FloatFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *FloatFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]float32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float32", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *FloatFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type DoubleFieldData struct {
	Data      []float64
	ValidData []bool
	Nullable  bool
}

// NewDoubleFieldData creates an empty field. Its memory size is computed in constant time.
func NewDoubleFieldData(nullable bool, capacity int) *DoubleFieldData {
	data := &DoubleFieldData{Data: make([]float64, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *DoubleFieldData) RowNum() int { return len(data.Data) }

func (data *DoubleFieldData) GetRowSize(i int) int { return 8 }

func (data *DoubleFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *DoubleFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *DoubleFieldData) GetDataRows() any { return data.Data }

func (data *DoubleFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Double }

func (data *DoubleFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *DoubleFieldData) GetValidData() []bool { return data.ValidData }

func (data *DoubleFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]float64, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(float64)
	if !ok {
		return merr.WrapErrParameterInvalid("float64", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *DoubleFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *DoubleFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]float64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float64", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *DoubleFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type TimestamptzFieldData struct {
	Data      []int64
	ValidData []bool
	Nullable  bool
}

// NewTimestamptzFieldData creates an empty field. Its memory size is computed in constant time.
func NewTimestamptzFieldData(nullable bool, capacity int) *TimestamptzFieldData {
	data := &TimestamptzFieldData{Data: make([]int64, 0, capacity), Nullable: nullable}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *TimestamptzFieldData) RowNum() int { return len(data.Data) }

func (data *TimestamptzFieldData) GetRowSize(i int) int { return 8 }

func (data *TimestamptzFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *TimestamptzFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *TimestamptzFieldData) GetDataRows() any { return data.Data }

func (data *TimestamptzFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Timestamptz
}

func (data *TimestamptzFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *TimestamptzFieldData) GetValidData() []bool { return data.ValidData }

func (data *TimestamptzFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([]int64, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(int64)
	if !ok {
		return merr.WrapErrParameterInvalid("timestamptz", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *TimestamptzFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *TimestamptzFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]timestamptz", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *TimestamptzFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

// --------------- Fixed-width scalar fields.  end  -----------

// --------------- Variable-length scalar fields.  begin  -----------

type StringFieldData struct {
	Data      []string
	DataType  schemapb.DataType
	ValidData []bool
	Nullable  bool

	trackMemory bool
	memorySize  int
}

// NewStringFieldData creates an empty string field with size tracking initialized.
// dataType is String, VarChar, or Text; capacity reserves rows and is not charged.
func NewStringFieldData(dataType schemapb.DataType, nullable bool, capacity int, trackMemory ...bool) *StringFieldData {
	data := &StringFieldData{
		trackMemory: len(trackMemory) > 0 && trackMemory[0],
		Data:        make([]string, 0, capacity),
		DataType:    dataType,
		Nullable:    nullable,
		memorySize:  1, // Nullable; same accounting as GetMemorySize.
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *StringFieldData) RowNum() int { return len(data.Data) }

func (data *StringFieldData) GetRowSize(i int) int { return len(data.Data[i]) + 16 }

// why not binary.Size(data) directly? binary.Size(data) return -1
// binary.Size returns how many bytes Write would generate to encode the value v, which
// must be a fixed-size value or a slice of fixed-size values, or a pointer to such data.
// If v is neither of these, binary.Size returns -1.
func (data *StringFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// getMemorySize calculates the size independently of the incremental cache.
func (data *StringFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *StringFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *StringFieldData) GetDataRows() any { return data.Data }

func (data *StringFieldData) GetDataType() schemapb.DataType { return data.DataType }

func (data *StringFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *StringFieldData) GetValidData() []bool { return data.ValidData }

// AppendRow appends a row and accounts for the data and validity added.
func (data *StringFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, "")
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.(string)
	if !ok {
		return merr.WrapErrParameterInvalid("string", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

// AppendRows preserves AppendRows' partial-success behavior: data remains
// appended and counted if appending validity subsequently fails.
func (data *StringFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	if err := data.AppendDataRows(dataRows); err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

// AppendDataRows accounts only for newly appended data, not validity.
func (data *StringFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	v, ok := rows.([]string)
	if !ok {
		return merr.WrapErrParameterInvalid("[]string", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// AppendValidDataRows accounts only for newly appended validity bytes.
func (data *StringFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

func (data *StringFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += len(value) + 16
	}
	data.memorySize += len(data.ValidData) - oldValid
}

type JSONFieldData struct {
	Data      [][]byte
	ValidData []bool
	Nullable  bool

	trackMemory bool
	memorySize  int
}

// NewJSONFieldData creates an empty field with size tracking initialized.
func NewJSONFieldData(nullable bool, capacity int, trackMemory ...bool) *JSONFieldData {
	data := &JSONFieldData{trackMemory: len(trackMemory) > 0 && trackMemory[0], Data: make([][]byte, 0, capacity), Nullable: nullable, memorySize: 1}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *JSONFieldData) RowNum() int { return len(data.Data) }

func (data *JSONFieldData) GetRowSize(i int) int { return len(data.Data[i]) + 16 }

func (data *JSONFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// getMemorySize calculates the size independently of the incremental cache.
func (data *JSONFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *JSONFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *JSONFieldData) GetDataRows() any { return data.Data }

func (data *JSONFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_JSON }

func (data *JSONFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *JSONFieldData) GetValidData() []bool { return data.ValidData }

func (data *JSONFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([][]byte, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	data.Data = append(data.Data, v)
	return nil
}

func (data *JSONFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *JSONFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	v, ok := rows.([][]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[][]byte", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *JSONFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

func (data *JSONFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += len(value) + 16
	}
	data.memorySize += len(data.ValidData) - oldValid
}

type GeometryFieldData struct {
	Data      [][]byte
	ValidData []bool
	Nullable  bool

	trackMemory bool
	memorySize  int
}

// NewGeometryFieldData creates an empty field with size tracking initialized.
func NewGeometryFieldData(nullable bool, capacity int, trackMemory ...bool) *GeometryFieldData {
	data := &GeometryFieldData{trackMemory: len(trackMemory) > 0 && trackMemory[0], Data: make([][]byte, 0, capacity), Nullable: nullable, memorySize: 1}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *GeometryFieldData) RowNum() int { return len(data.Data) }

func (data *GeometryFieldData) GetRowSize(i int) int { return len(data.Data[i]) + 16 }

func (data *GeometryFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// getMemorySize calculates the size independently of the incremental cache.
func (data *GeometryFieldData) getMemorySize() int {
	var size int
	// what's the meaning of 16?
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *GeometryFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *GeometryFieldData) GetDataRows() any { return data.Data }

func (data *GeometryFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Geometry
}

func (data *GeometryFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *GeometryFieldData) GetValidData() []bool { return data.ValidData }

func (data *GeometryFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, make([][]byte, 1)...)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	switch v := row.(type) {
	case []byte:
		data.Data = append(data.Data, v)
	case string:
		data.Data = append(data.Data, []byte(v))
	default:
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

func (data *GeometryFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *GeometryFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	switch v := rows.(type) {
	case [][]byte:
		data.Data = append(data.Data, v...)
	case []string:
		for _, row := range v {
			data.Data = append(data.Data, []byte(row))
		}
	default:
		return merr.WrapErrParameterInvalid("[][]byte", rows, "Wrong rows type")
	}
	return nil
}

func (data *GeometryFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

func (data *GeometryFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += len(value) + 16
	}
	data.memorySize += len(data.ValidData) - oldValid
}

// --------------- Variable-length scalar fields.  end  -----------

// --------------- Scalar array fields.  begin  -----------

type ArrayFieldData struct {
	ElementType     schemapb.DataType
	Data            []*schemapb.ScalarField
	ValidData       []bool
	Nullable        bool
	ElementNullable bool

	trackMemory bool
	memorySize  int
}

// NewArrayFieldData creates an empty field with size tracking initialized.
func NewArrayFieldData(elementType schemapb.DataType, nullable, elementNullable bool, capacity int, trackMemory ...bool) *ArrayFieldData {
	data := &ArrayFieldData{
		trackMemory: len(trackMemory) > 0 && trackMemory[0],
		Data:        make([]*schemapb.ScalarField, 0, capacity),
		ElementType: elementType,
		Nullable:    nullable, ElementNullable: elementNullable,
		memorySize: 2, // Nullable and ElementNullable.
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *ArrayFieldData) RowNum() int { return len(data.Data) }

func (data *ArrayFieldData) GetRowSize(i int) int {
	return getArrayScalarFieldMemorySize(data.Data[i], data.ElementType)
}

func (data *ArrayFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// getMemorySize calculates the size independently of the incremental cache.
func (data *ArrayFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += getArrayScalarFieldMemorySize(val, data.ElementType)
		size += binary.Size(typeutil.GetArrayElementValidData(val))
	}
	size += binary.Size(data.ValidData) + binary.Size(data.Nullable)
	size += binary.Size(data.ElementNullable)
	return size
}

func (data *ArrayFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *ArrayFieldData) GetDataRows() any { return data.Data }

func (data *ArrayFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Array }

func (data *ArrayFieldData) GetElementType() schemapb.DataType {
	return data.ElementType
}

func (data *ArrayFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *ArrayFieldData) GetElementNullable() bool {
	return data.ElementNullable
}

func (data *ArrayFieldData) GetValidData() []bool { return data.ValidData }

func (data *ArrayFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, nil)
		data.ValidData = append(data.ValidData, false)
		return nil
	}

	v, ok := row.(*schemapb.ScalarField)
	if !ok {
		return merr.WrapErrParameterInvalid("*schemapb.ScalarField", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)

	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

func (data *ArrayFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *ArrayFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	v, ok := rows.([]*schemapb.ScalarField)
	if !ok {
		return merr.WrapErrParameterInvalid("[]*schemapb.ScalarField", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *ArrayFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

func (data *ArrayFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += getArrayScalarFieldMemorySize(value, data.ElementType) + len(typeutil.GetArrayElementValidData(value))
	}
	data.memorySize += len(data.ValidData) - oldValid
}

func getArrayScalarFieldMemorySize(data *schemapb.ScalarField, elementType schemapb.DataType) int {
	if data == nil {
		return 0
	}

	// Nested Array rows are recursively encoded as ScalarField_ArrayData.
	// Descend into that payload before dispatching on the leaf element type.
	if arrayData := data.GetArrayData(); arrayData != nil {
		var size int
		for _, element := range arrayData.GetData() {
			size += getArrayScalarFieldMemorySize(element, arrayData.GetElementType())
		}
		return size
	}

	switch elementType {
	case schemapb.DataType_Bool:
		return binary.Size(data.GetBoolData().GetData())
	case schemapb.DataType_Int8:
		return binary.Size(data.GetIntData().GetData()) / 4
	case schemapb.DataType_Int16:
		return binary.Size(data.GetIntData().GetData()) / 2
	case schemapb.DataType_Int32:
		return binary.Size(data.GetIntData().GetData())
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		return binary.Size(data.GetLongData().GetData())
	case schemapb.DataType_Float:
		return binary.Size(data.GetFloatData().GetData())
	case schemapb.DataType_Double:
		return binary.Size(data.GetDoubleData().GetData())
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return (&StringFieldData{Data: data.GetStringData().GetData()}).GetMemorySize()
	default:
		return 0
	}
}

// --------------- Scalar array fields.  end  -----------

// --------------- Shared nullable-vector mapping and validation.  begin  -----------

// LogicalToPhysicalMapping maps logical offset to physical offset for nullable vector
type LogicalToPhysicalMapping struct {
	validCount int
	l2pMap     map[int]int
}

func (m *LogicalToPhysicalMapping) GetMemorySize() int {
	size := 8                  // validCount int
	size += len(m.l2pMap) * 16 // map[int]int, roughly 16 bytes per entry
	return size
}

func (m *LogicalToPhysicalMapping) GetPhysicalOffset(logicalOffset int) int {
	if m.l2pMap == nil {
		return logicalOffset
	}
	if physicalOffset, ok := m.l2pMap[logicalOffset]; ok {
		return physicalOffset
	}
	return -1
}

func (m *LogicalToPhysicalMapping) GetValidCount() int {
	return m.validCount
}

func (m *LogicalToPhysicalMapping) Build(validData []bool, startLogical, totalCount int) {
	if totalCount == 0 {
		return
	}
	if len(validData) < totalCount {
		return
	}

	if m.l2pMap == nil {
		m.l2pMap = make(map[int]int)
	}

	physicalIdx := m.validCount
	for i := 0; i < totalCount; i++ {
		if validData[i] {
			m.l2pMap[startLogical+i] = physicalIdx
			physicalIdx++
		}
	}
	m.validCount = physicalIdx
}

func getNullableVectorPhysicalOffset(validData []bool, mapping *LogicalToPhysicalMapping, logicalOffset int) int {
	if mapping != nil && mapping.l2pMap != nil {
		return mapping.GetPhysicalOffset(logicalOffset)
	}
	physicalOffset := 0
	for i := 0; i < logicalOffset; i++ {
		if validData[i] {
			physicalOffset++
		}
	}
	return physicalOffset
}

func validateNullableVectorCompactRows(nullable bool, validDataRows interface{}, physicalRows int) error {
	if !nullable {
		return nil
	}
	validData, ok := validDataRows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", validDataRows, "nullable vector requires valid data")
	}
	if err := funcutil.ValidateNullableVectorCompactRows("vector", validData, uint64(physicalRows), 0, true); err != nil {
		return merr.WrapErrParameterInvalidMsg(err.Error())
	}
	return nil
}

func validateNullableVectorTotalRows(nullable bool, existingValidData []bool, newValidData []bool, physicalRows int) error {
	if !nullable {
		return nil
	}
	validCount := funcutil.CountValidRows(existingValidData) + funcutil.CountValidRows(newValidData)
	if uint64(physicalRows) != validCount {
		return merr.WrapErrParameterInvalidMsg(
			"nullable vector compact data row count mismatch: physical rows=%d, valid rows=%d",
			physicalRows,
			validCount)
	}
	return nil
}

// --------------- Shared nullable-vector mapping and validation.  end  -----------

// --------------- Vector fields.  begin  -----------

type BinaryVectorFieldData struct {
	Data       []byte
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

// NewBinaryVectorFieldData creates an empty field. Its memory size is computed in constant time.
// Capacity is expressed in rows, using the same reservation policy as NewFieldData.
func NewBinaryVectorFieldData(dim int, nullable bool, capacity int) *BinaryVectorFieldData {
	dataCapacity := capacity * dim / 8
	if nullable {
		dataCapacity = capacity
	}
	data := &BinaryVectorFieldData{
		Data: make([]byte, 0, dataCapacity), Dim: dim, Nullable: nullable,
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *BinaryVectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) * 8 / data.Dim
}

func (data *BinaryVectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim / 8
}

func (data *BinaryVectorFieldData) GetMemorySize() int {
	// Data + ValidData + Dim(4) + Nullable(1) + L2PMapping
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *BinaryVectorFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	physicalIdx := i
	if data.GetNullable() {
		physicalIdx = getNullableVectorPhysicalOffset(data.ValidData, &data.L2PMapping, i)
	}
	return data.Data[physicalIdx*data.Dim/8 : (physicalIdx+1)*data.Dim/8]
}

func (data *BinaryVectorFieldData) GetDataRows() any { return data.Data }

func (data *BinaryVectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_BinaryVector
}

func (data *BinaryVectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *BinaryVectorFieldData) GetValidData() []bool { return data.ValidData }

func (data *BinaryVectorFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.L2PMapping.Build([]bool{false}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim/8 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	if data.GetNullable() {
		data.L2PMapping.Build([]bool{true}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *BinaryVectorFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	v, ok := dataRows.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", dataRows, "Wrong rows type")
	}
	rowWidth := data.Dim / 8
	if len(v)%rowWidth != 0 {
		return merr.WrapErrParameterInvalid(rowWidth, len(v), "Wrong vector size")
	}
	if err := validateNullableVectorCompactRows(data.GetNullable(), validDataRows, len(v)/rowWidth); err != nil {
		return err
	}
	data.Data = append(data.Data, v...)
	return data.AppendValidDataRows(validDataRows)
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *BinaryVectorFieldData) AppendDataRows(rows interface{}) error {
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

// AppendValidDataRows appends FLATTEN vectors to field data.
func (data *BinaryVectorFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	if err := validateNullableVectorTotalRows(data.GetNullable(), data.ValidData, v, len(data.Data)/(data.Dim/8)); err != nil {
		return err
	}
	data.L2PMapping.Build(v, len(data.ValidData), len(v))
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type FloatVectorFieldData struct {
	Data       []float32
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

// NewFloatVectorFieldData creates an empty field. Its memory size is computed in constant time.
// Capacity is expressed in rows, using the same reservation policy as NewFieldData.
func NewFloatVectorFieldData(dim int, nullable bool, capacity int) *FloatVectorFieldData {
	dataCapacity := capacity * dim
	if nullable {
		dataCapacity = capacity
	}
	data := &FloatVectorFieldData{
		Data: make([]float32, 0, dataCapacity), Dim: dim, Nullable: nullable,
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *FloatVectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / data.Dim
}

func (data *FloatVectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim * 4
}

func (data *FloatVectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *FloatVectorFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	physicalIdx := i
	if data.GetNullable() {
		physicalIdx = getNullableVectorPhysicalOffset(data.ValidData, &data.L2PMapping, i)
	}
	return data.Data[physicalIdx*data.Dim : (physicalIdx+1)*data.Dim]
}

func (data *FloatVectorFieldData) GetDataRows() any { return data.Data }

func (data *FloatVectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_FloatVector
}

func (data *FloatVectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *FloatVectorFieldData) GetValidData() []bool { return data.ValidData }

func (data *FloatVectorFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.L2PMapping.Build([]bool{false}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.([]float32)
	if !ok || len(v) != data.Dim {
		return merr.WrapErrParameterInvalid("[]float32", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	if data.GetNullable() {
		data.L2PMapping.Build([]bool{true}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *FloatVectorFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	v, ok := dataRows.([]float32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float32", dataRows, "Wrong rows type")
	}
	if len(v)%(data.Dim) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim, len(v), "Wrong vector size")
	}
	if err := validateNullableVectorCompactRows(data.GetNullable(), validDataRows, len(v)/data.Dim); err != nil {
		return err
	}
	data.Data = append(data.Data, v...)
	return data.AppendValidDataRows(validDataRows)
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *FloatVectorFieldData) AppendDataRows(rows interface{}) error {
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

// AppendValidDataRows appends FLATTEN vectors to field data.
func (data *FloatVectorFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	if err := validateNullableVectorTotalRows(data.GetNullable(), data.ValidData, v, len(data.Data)/data.Dim); err != nil {
		return err
	}
	data.L2PMapping.Build(v, len(data.ValidData), len(v))
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type Float16VectorFieldData struct {
	Data       []byte
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

// NewFloat16VectorFieldData creates an empty field. Its memory size is computed in constant time.
// Capacity is expressed in rows, using the same reservation policy as NewFieldData.
func NewFloat16VectorFieldData(dim int, nullable bool, capacity int) *Float16VectorFieldData {
	dataCapacity := capacity * dim * 2
	if nullable {
		dataCapacity = capacity
	}
	data := &Float16VectorFieldData{
		Data: make([]byte, 0, dataCapacity), Dim: dim, Nullable: nullable,
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *Float16VectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / 2 / data.Dim
}

func (data *Float16VectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim * 2
}

func (data *Float16VectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *Float16VectorFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	physicalIdx := i
	if data.GetNullable() {
		physicalIdx = getNullableVectorPhysicalOffset(data.ValidData, &data.L2PMapping, i)
	}
	return data.Data[physicalIdx*data.Dim*2 : (physicalIdx+1)*data.Dim*2]
}

func (data *Float16VectorFieldData) GetDataRows() any { return data.Data }

func (data *Float16VectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Float16Vector
}

func (data *Float16VectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Float16VectorFieldData) GetValidData() []bool { return data.ValidData }

func (data *Float16VectorFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.L2PMapping.Build([]bool{false}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim*2 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	if data.GetNullable() {
		data.L2PMapping.Build([]bool{true}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *Float16VectorFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	v, ok := dataRows.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", dataRows, "Wrong rows type")
	}
	rowWidth := data.Dim * 2
	if len(v)%rowWidth != 0 {
		return merr.WrapErrParameterInvalid(rowWidth, len(v), "Wrong vector size")
	}
	if err := validateNullableVectorCompactRows(data.GetNullable(), validDataRows, len(v)/rowWidth); err != nil {
		return err
	}
	data.Data = append(data.Data, v...)
	return data.AppendValidDataRows(validDataRows)
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *Float16VectorFieldData) AppendDataRows(rows interface{}) error {
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

// AppendValidDataRows appends FLATTEN vectors to field data.
func (data *Float16VectorFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	if err := validateNullableVectorTotalRows(data.GetNullable(), data.ValidData, v, len(data.Data)/(data.Dim*2)); err != nil {
		return err
	}
	data.L2PMapping.Build(v, len(data.ValidData), len(v))
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type BFloat16VectorFieldData struct {
	Data       []byte
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

// NewBFloat16VectorFieldData creates an empty field. Its memory size is computed in constant time.
// Capacity is expressed in rows, using the same reservation policy as NewFieldData.
func NewBFloat16VectorFieldData(dim int, nullable bool, capacity int) *BFloat16VectorFieldData {
	dataCapacity := capacity * dim * 2
	if nullable {
		dataCapacity = capacity
	}
	data := &BFloat16VectorFieldData{
		Data: make([]byte, 0, dataCapacity), Dim: dim, Nullable: nullable,
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *BFloat16VectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / 2 / data.Dim
}

func (data *BFloat16VectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim * 2
}

func (data *BFloat16VectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *BFloat16VectorFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	physicalIdx := i
	if data.GetNullable() {
		physicalIdx = getNullableVectorPhysicalOffset(data.ValidData, &data.L2PMapping, i)
	}
	return data.Data[physicalIdx*data.Dim*2 : (physicalIdx+1)*data.Dim*2]
}

func (data *BFloat16VectorFieldData) GetDataRows() any { return data.Data }

func (data *BFloat16VectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_BFloat16Vector
}

func (data *BFloat16VectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *BFloat16VectorFieldData) GetValidData() []bool { return data.ValidData }

func (data *BFloat16VectorFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.L2PMapping.Build([]bool{false}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.([]byte)
	if !ok || len(v) != data.Dim*2 {
		return merr.WrapErrParameterInvalid("[]byte", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	if data.GetNullable() {
		data.L2PMapping.Build([]bool{true}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *BFloat16VectorFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	v, ok := dataRows.([]byte)
	if !ok {
		return merr.WrapErrParameterInvalid("[]byte", dataRows, "Wrong rows type")
	}
	rowWidth := data.Dim * 2
	if len(v)%rowWidth != 0 {
		return merr.WrapErrParameterInvalid(rowWidth, len(v), "Wrong vector size")
	}
	if err := validateNullableVectorCompactRows(data.GetNullable(), validDataRows, len(v)/rowWidth); err != nil {
		return err
	}
	data.Data = append(data.Data, v...)
	return data.AppendValidDataRows(validDataRows)
}

// AppendDataRows appends FLATTEN vectors to field data.
func (data *BFloat16VectorFieldData) AppendDataRows(rows interface{}) error {
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

// AppendValidDataRows appends FLATTEN vectors to field data.
func (data *BFloat16VectorFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	if err := validateNullableVectorTotalRows(data.GetNullable(), data.ValidData, v, len(data.Data)/(data.Dim*2)); err != nil {
		return err
	}
	data.L2PMapping.Build(v, len(data.ValidData), len(v))
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type Int8VectorFieldData struct {
	Data       []int8
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

// NewInt8VectorFieldData creates an empty field. Its memory size is computed in constant time.
// Capacity is expressed in rows, using the same reservation policy as NewFieldData.
func NewInt8VectorFieldData(dim int, nullable bool, capacity int) *Int8VectorFieldData {
	dataCapacity := capacity * dim
	if nullable {
		dataCapacity = capacity
	}
	data := &Int8VectorFieldData{
		Data: make([]int8, 0, dataCapacity), Dim: dim, Nullable: nullable,
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *Int8VectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / data.Dim
}

func (data *Int8VectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim
}

func (data *Int8VectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *Int8VectorFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	physicalIdx := i
	if data.GetNullable() {
		physicalIdx = getNullableVectorPhysicalOffset(data.ValidData, &data.L2PMapping, i)
	}
	return data.Data[physicalIdx*data.Dim : (physicalIdx+1)*data.Dim]
}

func (data *Int8VectorFieldData) GetDataRows() any { return data.Data }

func (data *Int8VectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Int8Vector
}

func (data *Int8VectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int8VectorFieldData) GetValidData() []bool { return data.ValidData }

func (data *Int8VectorFieldData) AppendRow(row interface{}) error {
	if data.GetNullable() && row == nil {
		data.L2PMapping.Build([]bool{false}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
	v, ok := row.([]int8)
	if !ok || len(v) != data.Dim {
		return merr.WrapErrParameterInvalid("[]int8", row, "Wrong row type")
	}
	data.Data = append(data.Data, v...)
	if data.GetNullable() {
		data.L2PMapping.Build([]bool{true}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

func (data *Int8VectorFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	v, ok := dataRows.([]int8)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int8", dataRows, "Wrong rows type")
	}
	if len(v)%(data.Dim) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim, len(v), "Wrong vector size")
	}
	if err := validateNullableVectorCompactRows(data.GetNullable(), validDataRows, len(v)/data.Dim); err != nil {
		return err
	}
	data.Data = append(data.Data, v...)
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int8VectorFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int8)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int8", rows, "Wrong rows type")
	}
	if len(v)%(data.Dim) != 0 {
		return merr.WrapErrParameterInvalid(data.Dim, len(v), "Wrong vector size")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int8VectorFieldData) AppendValidDataRows(rows interface{}) error {
	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	if err := validateNullableVectorTotalRows(data.GetNullable(), data.ValidData, v, len(data.Data)/data.Dim); err != nil {
		return err
	}
	data.L2PMapping.Build(v, len(data.ValidData), len(v))
	data.ValidData = append(data.ValidData, v...)
	return nil
}

type SparseFloatVectorFieldData struct {
	schemapb.SparseFloatArray
	ValidData  []bool
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping

	trackMemory bool
	memorySize  int
}

// NewSparseFloatVectorFieldData creates an empty field with size tracking initialized.
// Capacity reserves validity entries; sparse contents grow on append, as in NewFieldData.
func NewSparseFloatVectorFieldData(nullable bool, capacity int, trackMemory ...bool) *SparseFloatVectorFieldData {
	data := &SparseFloatVectorFieldData{
		trackMemory: len(trackMemory) > 0 && trackMemory[0],
		Nullable:    nullable, memorySize: 9, // Nullable and empty L2PMapping.
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *SparseFloatVectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Contents)
}

func (data *SparseFloatVectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	physicalIdx := data.L2PMapping.GetPhysicalOffset(i)
	return len(data.Contents[physicalIdx])
}

func (data *SparseFloatVectorFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// getMemorySize calculates the size independently of the incremental cache.
func (data *SparseFloatVectorFieldData) getMemorySize() int {
	// TODO(SPARSE): should this be the memory size of serialzied size?
	// SparseFloatArray + ValidData + Nullable(1) + L2PMapping
	return proto.Size(&data.SparseFloatArray) + binary.Size(data.ValidData) + 1 + data.L2PMapping.GetMemorySize()
}

func (data *SparseFloatVectorFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	physicalIdx := i
	if data.GetNullable() {
		physicalIdx = getNullableVectorPhysicalOffset(data.ValidData, &data.L2PMapping, i)
	}
	return data.Contents[physicalIdx]
}

func (data *SparseFloatVectorFieldData) GetDataRows() any { return data.Contents }

func (data *SparseFloatVectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_SparseFloatVector
}

func (data *SparseFloatVectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *SparseFloatVectorFieldData) GetValidData() []bool { return data.ValidData }

func (data *SparseFloatVectorFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			proto.Size(&schemapb.SparseFloatArray{Dim: data.Dim}), data.L2PMapping.GetMemorySize())
	}

	if data.GetNullable() && row == nil {
		data.L2PMapping.Build([]bool{false}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, false)
		return nil
	}
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
	if data.GetNullable() {
		data.L2PMapping.Build([]bool{true}, len(data.ValidData), 1)
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

func (data *SparseFloatVectorFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	v, ok := dataRows.(*SparseFloatVectorFieldData)
	if !ok {
		return merr.WrapErrParameterInvalid("SparseFloatVectorFieldData", dataRows, "Wrong rows type")
	}
	if err := validateNullableVectorCompactRows(data.GetNullable(), validDataRows, len(v.Contents)); err != nil {
		return err
	}
	if err := data.AppendDataRows(v); err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *SparseFloatVectorFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			proto.Size(&schemapb.SparseFloatArray{Dim: data.Dim}), data.L2PMapping.GetMemorySize())
	}

	v, ok := rows.(*SparseFloatVectorFieldData)
	if !ok {
		return merr.WrapErrParameterInvalid("SparseFloatVectorFieldData", rows, "Wrong rows type")
	}
	data.Contents = append(data.Contents, v.Contents...)
	if data.Dim < v.Dim {
		data.Dim = v.Dim
	}
	return nil
}

func (data *SparseFloatVectorFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			proto.Size(&schemapb.SparseFloatArray{Dim: data.Dim}), data.L2PMapping.GetMemorySize())
	}

	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	if err := validateNullableVectorTotalRows(data.GetNullable(), data.ValidData, v, len(data.Contents)); err != nil {
		return err
	}
	data.L2PMapping.Build(v, len(data.ValidData), len(v))
	data.ValidData = append(data.ValidData, v...)
	return nil
}

func (data *SparseFloatVectorFieldData) trackAppendedSize(oldRows, oldValid, oldDimSize, oldMappingSize int) {
	// Size only the new rows and the change in the shared Dim encoding.
	data.memorySize += proto.Size(&schemapb.SparseFloatArray{
		Contents: data.Contents[oldRows:], Dim: data.Dim,
	}) - oldDimSize
	data.memorySize += len(data.ValidData) - oldValid
	data.memorySize += data.L2PMapping.GetMemorySize() - oldMappingSize
}

func (data *SparseFloatVectorFieldData) AppendAllRows(src *SparseFloatVectorFieldData) {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			proto.Size(&schemapb.SparseFloatArray{Dim: data.Dim}), data.L2PMapping.GetMemorySize())
	}

	if data.Dim < src.Dim {
		data.Dim = src.Dim
	}
	data.Contents = append(data.Contents, src.Contents...)
	if src.Nullable {
		if data.ValidData == nil {
			data.ValidData = make([]bool, 0, len(src.ValidData))
		}
		data.L2PMapping.Build(src.ValidData, len(data.ValidData), len(src.ValidData))
		data.ValidData = append(data.ValidData, src.ValidData...)
		data.Nullable = true
	}
}

// --------------- Vector fields.  end  -----------

// --------------- Vector array fields.  begin  -----------

type VectorArrayFieldData struct {
	Dim             int64
	ElementType     schemapb.DataType
	Data            []*schemapb.VectorField
	ValidData       []bool
	Nullable        bool
	ElementNullable bool

	trackMemory bool
	memorySize  int
}

// NewVectorArrayFieldData creates an empty field with size tracking initialized.
func NewVectorArrayFieldData(elementType schemapb.DataType, dim int64, nullable, elementNullable bool, capacity int, trackMemory ...bool) *VectorArrayFieldData {
	data := &VectorArrayFieldData{
		trackMemory: len(trackMemory) > 0 && trackMemory[0],
		Data:        make([]*schemapb.VectorField, 0, capacity),
		ElementType: elementType, Dim: dim,
		Nullable: nullable, ElementNullable: elementNullable,
		memorySize: 2, // Nullable and ElementNullable.
	}
	if nullable {
		data.ValidData = make([]bool, 0, capacity)
	}
	return data
}

func (data *VectorArrayFieldData) RowNum() int { return len(data.Data) }

func (data *VectorArrayFieldData) GetRowSize(i int) int {
	return GetVectorSize(data.Data[i], data.ElementType)
}

func (data *VectorArrayFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// getMemorySize calculates the size independently of the incremental cache.
func (data *VectorArrayFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += GetVectorSize(val, data.ElementType)
		size += binary.Size(typeutil.GetVectorArrayElementValidData(val))
	}
	size += binary.Size(data.ValidData) + binary.Size(data.Nullable)
	size += binary.Size(data.ElementNullable)
	return size
}

func (data *VectorArrayFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *VectorArrayFieldData) GetDataRows() any { return data.Data }

func (data *VectorArrayFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_ArrayOfVector
}

func (data *VectorArrayFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *VectorArrayFieldData) GetElementNullable() bool {
	return data.ElementNullable
}

func (data *VectorArrayFieldData) GetValidData() []bool { return data.ValidData }

func (data *VectorArrayFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if data.GetNullable() && row == nil {
		data.Data = append(data.Data, emptyPerRowVectorField(data.Dim, data.ElementType))
		data.ValidData = append(data.ValidData, false)
		return nil
	}

	v, ok := row.(*schemapb.VectorField)
	if !ok {
		return merr.WrapErrParameterInvalid("*schemapb.VectorField", row, "Wrong row type")
	}
	data.Data = append(data.Data, v)

	if data.GetNullable() {
		data.ValidData = append(data.ValidData, true)
	}
	return nil
}

func (data *VectorArrayFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *VectorArrayFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	v, ok := rows.([]*schemapb.VectorField)
	if !ok {
		return merr.WrapErrParameterInvalid("[]*schemapb.VectorField", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *VectorArrayFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	if rows == nil {
		return nil
	}
	v, ok := rows.([]bool)
	if !ok {
		return merr.WrapErrParameterInvalid("[]bool", rows, "Wrong rows type")
	}
	data.ValidData = append(data.ValidData, v...)
	return nil
}

func (data *VectorArrayFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += GetVectorSize(value, data.ElementType) + len(typeutil.GetVectorArrayElementValidData(value))
	}
	data.memorySize += len(data.ValidData) - oldValid
}

func emptyPerRowVectorField(dim int64, elementType schemapb.DataType) *schemapb.VectorField {
	vf := &schemapb.VectorField{Dim: dim}
	switch elementType {
	case schemapb.DataType_FloatVector:
		vf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}
	case schemapb.DataType_BinaryVector:
		vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: []byte{}}
	case schemapb.DataType_Float16Vector:
		vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: []byte{}}
	case schemapb.DataType_BFloat16Vector:
		vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{}}
	case schemapb.DataType_Int8Vector:
		vf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: []byte{}}
	}
	return vf
}

func GetVectorSize(vector *schemapb.VectorField, vectorType schemapb.DataType) int {
	if vector == nil {
		return 0
	}
	size := 0
	switch vectorType {
	case schemapb.DataType_BinaryVector:
		size += binary.Size(vector.GetBinaryVector()) + 4
	case schemapb.DataType_FloatVector:
		size += binary.Size(vector.GetFloatVector().Data) + 4
	case schemapb.DataType_Float16Vector:
		size += binary.Size(vector.GetFloat16Vector()) + 4
	case schemapb.DataType_BFloat16Vector:
		size += binary.Size(vector.GetBfloat16Vector()) + 4
	case schemapb.DataType_SparseFloatVector:
		panic("not implemented")
	case schemapb.DataType_Int8Vector:
		size += binary.Size(vector.GetInt8Vector()) + 4
	default:
		panic("unreachable")
	}
	return size
}

// --------------- Vector array fields.  end  -----------
