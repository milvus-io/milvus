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

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	return NewInsertDataWithCap(schema, 0, false)
}

func NewInsertDataWithFunctionOutputField(schema *schemapb.CollectionSchema) (*InsertData, error) {
	return NewInsertDataWithCap(schema, 0, true)
}

func NewInsertDataWithCap(schema *schemapb.CollectionSchema, cap int, withFunctionOutput bool) (*InsertData, error) {
	return newInsertDataWithCap(schema, cap, withFunctionOutput, false)
}

// NewInsertDataWithMemoryTracking creates an empty batch including function output fields.
// Variable-width fields use incremental sizing; the batch total sums its current fields.
// Use Append* or CopyFrom methods to change tracked fields; appended payloads and getter views are read-only.
func NewInsertDataWithMemoryTracking(schema *schemapb.CollectionSchema) (*InsertData, error) {
	return newInsertDataWithCap(schema, 0, true, true)
}

func newInsertDataWithCap(schema *schemapb.CollectionSchema, cap int, withFunctionOutput, trackMemory bool) (*InsertData, error) {
	if schema == nil {
		// A nil schema here is an internal invariant violation (callers within
		// Milvus always pass a schema), not a client-supplied missing parameter.
		// ErrParameterMissing defaults to InputError, so mark this one site as a
		// system error to keep it out of the client-fault (cause="user") bucket.
		return nil, merr.WrapErrAsSysError(merr.WrapErrParameterMissing("collection schema"))
	}

	idata := &InsertData{
		Data: make(map[FieldID]FieldData),
	}

	appendField := func(field *schemapb.FieldSchema) error {
		if field.IsPrimaryKey && field.GetNullable() {
			return merr.WrapErrParameterInvalidMsg("primary key field should not be nullable (field: %s)", field.Name)
		}
		if field.IsPartitionKey && field.GetNullable() {
			return merr.WrapErrParameterInvalidMsg("partition key field should not be nullable (field: %s)", field.Name)
		}
		if field.IsFunctionOutput {
			if field.IsPrimaryKey || field.IsPartitionKey {
				return merr.WrapErrParameterInvalidMsg("function output field should not be primary key or partition key (field: %s)", field.Name)
			}
			if field.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("function output field should not be nullable (field: %s)", field.Name)
			}
			if !withFunctionOutput {
				return nil
			}
		}
		fieldData, err := newFieldData(field.DataType, field, cap, trackMemory)
		if err != nil {
			return err
		}
		idata.Data[field.FieldID] = fieldData
		return nil
	}

	for _, field := range schema.Fields {
		if err := appendField(field); err != nil {
			return nil, err
		}
	}
	for _, structField := range schema.StructArrayFields {
		for _, field := range structField.GetFields() {
			if err := appendField(field); err != nil {
				return nil, err
			}
		}
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
			return merr.WrapErrServiceInternalMsg("missing field when appending row, got %d", fID)
		}

		if err := field.AppendRow(v); err != nil {
			return merr.WrapErrParameterInvalidMsg("append data for field %d failed, err=%s", fID, err.Error())
		}
	}

	return nil
}

// CopyFrom appends the selected source rows column by column in indices order.
// Indices must refer to valid source rows. Nil or empty indices append nothing.
func (i *InsertData) CopyFrom(src *InsertData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	for fieldID, source := range src.Data {
		target, ok := i.Data[fieldID]
		if !ok {
			return merr.WrapErrServiceInternalMsg("missing field when appending row, got %d", fieldID)
		}
		// Preserve AppendRow's value-level compatibility and error behavior
		// for mismatched source columns instead of panicking on an assertion.
		if source.GetDataType() != target.GetDataType() || source.GetNullable() != target.GetNullable() {
			for _, i := range indices {
				if err := target.AppendRow(source.GetRow(i)); err != nil {
					return merr.Wrapf(err, "append data for field %d failed", fieldID)
				}
			}
			continue
		}
		if err := target.CopyFrom(source, indices); err != nil {
			return merr.Wrapf(err, "copy data for field %d failed", fieldID)
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

// FieldData represents a field's rows and validity.
//
// Memory tracking is disabled by default.
// NewFieldDataWithMemoryTracking and NewInsertDataWithMemoryTracking enable it for variable-width fields.
// Append* and CopyFrom methods update the size cache, including changes retained after an error.
// GetMemorySize returns the cache when enabled; otherwise, it scans variable-width data.
// Fixed-width scalars and dense vectors always calculate their sizes in constant time.
//
// Tracking is fixed at construction.
// With tracking enabled, only Append* and CopyFrom methods may change the field's data and metadata.
// Appended payloads and backing data returned by getters must remain unchanged.
type FieldData interface {
	RowNum() int

	// Size accounting.
	GetRowSize(i int) int
	GetMemorySize() int

	// Data and field metadata.
	GetRow(i int) any
	GetDataRows() any
	GetDataType() schemapb.DataType
	GetNullable() bool
	GetValidData() []bool

	// Append rows, payloads, or validity.
	AppendRow(row interface{}) error
	AppendRows(dataRows interface{}, validDataRows interface{}) error
	AppendDataRows(rows interface{}) error
	AppendValidDataRows(rows interface{}) error

	// CopyFrom appends the selected source rows in indices order.
	// Source and destination must have matching types and nullability;
	// dense vectors must also have matching dimensions.
	// Indices must refer to valid source rows. Nil or empty indices append nothing.
	CopyFrom(src FieldData, indices []int) error
}

// NewFieldData creates an empty FieldData with memory tracking disabled.
// cap is a row-capacity hint.
// Non-nullable dense vectors scale payload capacity by dimension.
// Nullable dense vectors reserve only cap payload elements.
// Sparse vector contents grow on append.
// Unused slice capacity is not included in GetMemorySize.
func NewFieldData(dataType schemapb.DataType, fieldSchema *schemapb.FieldSchema, cap int) (FieldData, error) {
	return newFieldData(dataType, fieldSchema, cap, false)
}

// NewFieldDataWithMemoryTracking creates an empty FieldData.
// Variable-width fields use incremental sizing; capacity follows NewFieldData.
// Use Append* or CopyFrom methods to change tracked fields; appended payloads and getter views are read-only.
func NewFieldDataWithMemoryTracking(dataType schemapb.DataType, fieldSchema *schemapb.FieldSchema, cap int) (FieldData, error) {
	return newFieldData(dataType, fieldSchema, cap, true)
}

func newFieldData(dataType schemapb.DataType, fieldSchema *schemapb.FieldSchema, cap int, trackMemory bool) (FieldData, error) {
	typeParams := fieldSchema.GetTypeParams()
	// Nullable vectors reserve only cap payload elements, including validity-only columns.
	nullableDataCap := func(scaled int) int {
		if fieldSchema.GetNullable() {
			return cap
		}
		return scaled
	}
	switch dataType {
	case schemapb.DataType_Float16Vector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		data := &Float16VectorFieldData{
			Data:     make([]byte, 0, nullableDataCap(cap*dim*2)),
			Dim:      dim,
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_BFloat16Vector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		data := &BFloat16VectorFieldData{
			Data:     make([]byte, 0, nullableDataCap(cap*dim*2)),
			Dim:      dim,
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_FloatVector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		data := &FloatVectorFieldData{
			Data:     make([]float32, 0, nullableDataCap(cap*dim)),
			Dim:      dim,
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_BinaryVector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		data := &BinaryVectorFieldData{
			Data:     make([]byte, 0, nullableDataCap(cap*dim/8)),
			Dim:      dim,
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_SparseFloatVector:
		return newSparseFloatVectorFieldData(fieldSchema, cap, trackMemory)
	case schemapb.DataType_Int8Vector:
		dim, err := GetDimFromParams(typeParams)
		if err != nil {
			return nil, err
		}
		data := &Int8VectorFieldData{
			Data:     make([]int8, 0, nullableDataCap(cap*dim)),
			Dim:      dim,
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_Bool:
		data := &BoolFieldData{
			Data:     make([]bool, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil

	case schemapb.DataType_Int8:
		data := &Int8FieldData{
			Data:     make([]int8, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil

	case schemapb.DataType_Int16:
		data := &Int16FieldData{
			Data:     make([]int16, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil

	case schemapb.DataType_Int32:
		data := &Int32FieldData{
			Data:     make([]int32, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil

	case schemapb.DataType_Int64:
		data := &Int64FieldData{
			Data:     make([]int64, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_Float:
		data := &FloatFieldData{
			Data:     make([]float32, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil

	case schemapb.DataType_Double:
		data := &DoubleFieldData{
			Data:     make([]float64, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_Timestamptz:
		data := &TimestamptzFieldData{
			Data:     make([]int64, 0, cap),
			Nullable: fieldSchema.GetNullable(),
		}
		if fieldSchema.GetNullable() {
			data.ValidData = make([]bool, 0, cap)
		}
		return data, nil
	case schemapb.DataType_JSON:
		return newJSONFieldData(fieldSchema, cap, trackMemory)
	case schemapb.DataType_Geometry:
		return newGeometryFieldData(fieldSchema, cap, trackMemory)
	case schemapb.DataType_Array:
		return newArrayFieldData(fieldSchema, cap, trackMemory)
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return newStringFieldData(dataType, fieldSchema, cap, trackMemory)
	case schemapb.DataType_ArrayOfVector:
		return newVectorArrayFieldData(fieldSchema, cap, trackMemory)
	default:
		return nil, merr.WrapErrServiceInternalMsg("unexpected schema data type: %d", dataType)
	}
}

type BoolFieldData struct {
	Data      []bool
	ValidData []bool
	Nullable  bool
}
type Int8FieldData struct {
	Data      []int8
	ValidData []bool
	Nullable  bool
}
type Int16FieldData struct {
	Data      []int16
	ValidData []bool
	Nullable  bool
}
type Int32FieldData struct {
	Data      []int32
	ValidData []bool
	Nullable  bool
}
type Int64FieldData struct {
	Data      []int64
	ValidData []bool
	Nullable  bool
}
type FloatFieldData struct {
	Data      []float32
	ValidData []bool
	Nullable  bool
}
type DoubleFieldData struct {
	Data      []float64
	ValidData []bool
	Nullable  bool
}
type StringFieldData struct {
	Data      []string
	DataType  schemapb.DataType
	ValidData []bool
	Nullable  bool

	trackMemory bool
	memorySize  int
}

func newStringFieldData(dataType schemapb.DataType, schema *schemapb.FieldSchema, capacity int, trackMemory bool) (FieldData, error) {
	data := &StringFieldData{
		Data:     make([]string, 0, capacity),
		DataType: dataType,
		Nullable: schema.GetNullable(),
	}
	if schema.GetNullable() {
		data.ValidData = make([]bool, 0, capacity)
	}
	if trackMemory {
		data.trackMemory = true
		data.memorySize = 1 // Nullable flag.
	}
	return data, nil
}

type ArrayFieldData struct {
	ElementType     schemapb.DataType
	Data            []*schemapb.ScalarField
	ValidData       []bool
	Nullable        bool
	ElementNullable bool

	trackMemory bool
	memorySize  int
}

func newArrayFieldData(schema *schemapb.FieldSchema, capacity int, trackMemory bool) (FieldData, error) {
	data := &ArrayFieldData{
		ElementType:     schema.GetElementType(),
		Data:            make([]*schemapb.ScalarField, 0, capacity),
		Nullable:        schema.GetNullable(),
		ElementNullable: schema.GetElementNullable(),
	}
	if schema.GetNullable() {
		data.ValidData = make([]bool, 0, capacity)
	}
	if trackMemory {
		data.trackMemory = true
		data.memorySize = 2 // Nullable and ElementNullable flags.
	}
	return data, nil
}

type JSONFieldData struct {
	Data      [][]byte
	ValidData []bool
	Nullable  bool

	trackMemory bool
	memorySize  int
}

func newJSONFieldData(schema *schemapb.FieldSchema, capacity int, trackMemory bool) (FieldData, error) {
	data := &JSONFieldData{
		Data:     make([][]byte, 0, capacity),
		Nullable: schema.GetNullable(),
	}
	if schema.GetNullable() {
		data.ValidData = make([]bool, 0, capacity)
	}
	if trackMemory {
		data.trackMemory = true
		data.memorySize = 1 // Nullable flag.
	}
	return data, nil
}

type TimestamptzFieldData struct {
	Data      []int64
	ValidData []bool
	Nullable  bool
}
type GeometryFieldData struct {
	Data      [][]byte
	ValidData []bool
	Nullable  bool

	trackMemory bool
	memorySize  int
}

func newGeometryFieldData(schema *schemapb.FieldSchema, capacity int, trackMemory bool) (FieldData, error) {
	data := &GeometryFieldData{
		Data:     make([][]byte, 0, capacity),
		Nullable: schema.GetNullable(),
	}
	if schema.GetNullable() {
		data.ValidData = make([]bool, 0, capacity)
	}
	if trackMemory {
		data.trackMemory = true
		data.memorySize = 1 // Nullable flag.
	}
	return data, nil
}

// LogicalToPhysicalMapping maps logical offset to physical offset for nullable vector
type LogicalToPhysicalMapping struct {
	validCount int
	l2pMap     map[int]int
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

func (m *LogicalToPhysicalMapping) GetMemorySize() int {
	size := 8                  // validCount int
	size += len(m.l2pMap) * 16 // map[int]int, roughly 16 bytes per entry
	return size
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

// countSelectedValidRows counts non-null rows in the selection, including repeated indices.
func countSelectedValidRows(validData []bool, indices []int) int {
	count := 0
	for _, i := range indices {
		if validData[i] {
			count++
		}
	}
	return count
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

type BinaryVectorFieldData struct {
	Data       []byte
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}
type FloatVectorFieldData struct {
	Data       []float32
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}
type Float16VectorFieldData struct {
	Data       []byte
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}
type BFloat16VectorFieldData struct {
	Data       []byte
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

type SparseFloatVectorFieldData struct {
	schemapb.SparseFloatArray
	ValidData  []bool
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping

	trackMemory bool
	memorySize  int
}

func newSparseFloatVectorFieldData(schema *schemapb.FieldSchema, capacity int, trackMemory bool) (FieldData, error) {
	data := &SparseFloatVectorFieldData{
		Nullable: schema.GetNullable(),
	}
	if schema.GetNullable() {
		data.ValidData = make([]bool, 0, capacity)
	}
	if trackMemory {
		data.trackMemory = true
		data.memorySize = 9 // Nullable flag (1 byte) and empty L2PMapping (8 bytes).
	}
	return data, nil
}

type Int8VectorFieldData struct {
	Data       []int8
	ValidData  []bool
	Dim        int
	Nullable   bool
	L2PMapping LogicalToPhysicalMapping
}

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

func newVectorArrayFieldData(schema *schemapb.FieldSchema, capacity int, trackMemory bool) (FieldData, error) {
	dim, err := GetDimFromParams(schema.GetTypeParams())
	if err != nil {
		return nil, err
	}
	data := &VectorArrayFieldData{
		Dim:             int64(dim),
		ElementType:     schema.GetElementType(),
		Data:            make([]*schemapb.VectorField, 0, capacity),
		Nullable:        schema.GetNullable(),
		ElementNullable: schema.GetElementNullable(),
	}
	if schema.GetNullable() {
		data.ValidData = make([]bool, 0, capacity)
	}
	if trackMemory {
		data.trackMemory = true
		data.memorySize = 2 // Nullable and ElementNullable flags.
	}
	return data, nil
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

func (data *SparseFloatVectorFieldData) AppendAllRows(src *SparseFloatVectorFieldData) {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			data.Dim, data.L2PMapping.GetMemorySize())
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

// RowNum implements FieldData.RowNum
func (data *BoolFieldData) RowNum() int        { return len(data.Data) }
func (data *Int8FieldData) RowNum() int        { return len(data.Data) }
func (data *Int16FieldData) RowNum() int       { return len(data.Data) }
func (data *Int32FieldData) RowNum() int       { return len(data.Data) }
func (data *Int64FieldData) RowNum() int       { return len(data.Data) }
func (data *FloatFieldData) RowNum() int       { return len(data.Data) }
func (data *DoubleFieldData) RowNum() int      { return len(data.Data) }
func (data *TimestamptzFieldData) RowNum() int { return len(data.Data) }
func (data *StringFieldData) RowNum() int      { return len(data.Data) }
func (data *ArrayFieldData) RowNum() int       { return len(data.Data) }
func (data *JSONFieldData) RowNum() int        { return len(data.Data) }
func (data *GeometryFieldData) RowNum() int    { return len(data.Data) }
func (data *BinaryVectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) * 8 / data.Dim
}

func (data *FloatVectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / data.Dim
}

func (data *Float16VectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / 2 / data.Dim
}

func (data *BFloat16VectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / 2 / data.Dim
}

func (data *SparseFloatVectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Contents)
}

func (data *Int8VectorFieldData) RowNum() int {
	if data.Nullable {
		return len(data.ValidData)
	}
	return len(data.Data) / data.Dim
}

func (data *VectorArrayFieldData) RowNum() int { return len(data.Data) }

// GetRow implements FieldData.GetRow
func (data *BoolFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int8FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int16FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int32FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *Int64FieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *TimestamptzFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *FloatFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *DoubleFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *StringFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *ArrayFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *JSONFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *GeometryFieldData) GetRow(i int) any {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
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

func (data *VectorArrayFieldData) GetRow(i int) interface{} {
	if data.GetNullable() && !data.ValidData[i] {
		return nil
	}
	return data.Data[i]
}

func (data *BoolFieldData) GetDataRows() any              { return data.Data }
func (data *Int8FieldData) GetDataRows() any              { return data.Data }
func (data *Int16FieldData) GetDataRows() any             { return data.Data }
func (data *Int32FieldData) GetDataRows() any             { return data.Data }
func (data *Int64FieldData) GetDataRows() any             { return data.Data }
func (data *FloatFieldData) GetDataRows() any             { return data.Data }
func (data *DoubleFieldData) GetDataRows() any            { return data.Data }
func (data *TimestamptzFieldData) GetDataRows() any       { return data.Data }
func (data *StringFieldData) GetDataRows() any            { return data.Data }
func (data *ArrayFieldData) GetDataRows() any             { return data.Data }
func (data *JSONFieldData) GetDataRows() any              { return data.Data }
func (data *GeometryFieldData) GetDataRows() any          { return data.Data }
func (data *BinaryVectorFieldData) GetDataRows() any      { return data.Data }
func (data *FloatVectorFieldData) GetDataRows() any       { return data.Data }
func (data *Float16VectorFieldData) GetDataRows() any     { return data.Data }
func (data *BFloat16VectorFieldData) GetDataRows() any    { return data.Data }
func (data *SparseFloatVectorFieldData) GetDataRows() any { return data.Contents }
func (data *Int8VectorFieldData) GetDataRows() any        { return data.Data }
func (data *VectorArrayFieldData) GetDataRows() any       { return data.Data }

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

func (data *BoolFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*BoolFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("bool CopyFrom requires a non-nil BoolFieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *Int8FieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*Int8FieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("Int8FieldData.CopyFrom requires a non-nil Int8FieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *Int16FieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*Int16FieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("Int16FieldData.CopyFrom requires a non-nil Int16FieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *Int32FieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*Int32FieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("Int32FieldData.CopyFrom requires a non-nil Int32FieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *Int64FieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*Int64FieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("Int64FieldData.CopyFrom requires a non-nil Int64FieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *FloatFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*FloatFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("FloatFieldData.CopyFrom requires a non-nil FloatFieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *DoubleFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*DoubleFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("DoubleFieldData.CopyFrom requires a non-nil DoubleFieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *TimestamptzFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*TimestamptzFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("TimestamptzFieldData.CopyFrom requires a non-nil TimestamptzFieldData source")
	}
	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

// AppendRow updates the size cache when memory tracking is enabled.
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

func (data *StringFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*StringFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("StringFieldData.CopyFrom requires a non-nil StringFieldData source")
	}
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *ArrayFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*ArrayFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("ArrayFieldData.CopyFrom requires a non-nil ArrayFieldData source")
	}
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *JSONFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*JSONFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("JSONFieldData.CopyFrom requires a non-nil JSONFieldData source")
	}
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *GeometryFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*GeometryFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("GeometryFieldData.CopyFrom requires a non-nil GeometryFieldData source")
	}
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
	}
	return nil
}

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

func (data *BinaryVectorFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*BinaryVectorFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("BinaryVectorFieldData.CopyFrom requires a non-nil BinaryVectorFieldData source")
	}
	if source.Dim != data.Dim {
		return merr.WrapErrServiceInternalMsg("BinaryVectorFieldData.CopyFrom dimension mismatch: source=%d, destination=%d", source.Dim, data.Dim)
	}
	rowWidth := data.Dim / 8
	if data.Nullable {
		validCount := countSelectedValidRows(source.ValidData, indices)
		// Reserve only the selected non-null vectors, preserving existing data.
		if required := len(data.Data) + validCount*rowWidth; required > cap(data.Data) {
			values := make([]byte, len(data.Data), required)
			copy(values, data.Data)
			data.Data = values
		}
	}
	for _, i := range indices {
		if data.Nullable {
			valid := source.ValidData[i]
			data.L2PMapping.Build([]bool{valid}, len(data.ValidData), 1)
			data.ValidData = append(data.ValidData, valid)
			if !valid {
				continue
			}
			i = getNullableVectorPhysicalOffset(source.ValidData, &source.L2PMapping, i)
		}
		data.Data = append(data.Data, source.Data[i*rowWidth:(i+1)*rowWidth]...)
	}
	return nil
}

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

func (data *FloatVectorFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*FloatVectorFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("FloatVectorFieldData.CopyFrom requires a non-nil FloatVectorFieldData source")
	}
	if source.Dim != data.Dim {
		return merr.WrapErrServiceInternalMsg("FloatVectorFieldData.CopyFrom dimension mismatch: source=%d, destination=%d", source.Dim, data.Dim)
	}
	rowWidth := data.Dim
	if data.Nullable {
		validCount := countSelectedValidRows(source.ValidData, indices)
		// Reserve only the selected non-null vectors, preserving existing data.
		if required := len(data.Data) + validCount*rowWidth; required > cap(data.Data) {
			values := make([]float32, len(data.Data), required)
			copy(values, data.Data)
			data.Data = values
		}
	}
	for _, i := range indices {
		if data.Nullable {
			valid := source.ValidData[i]
			data.L2PMapping.Build([]bool{valid}, len(data.ValidData), 1)
			data.ValidData = append(data.ValidData, valid)
			if !valid {
				continue
			}
			i = getNullableVectorPhysicalOffset(source.ValidData, &source.L2PMapping, i)
		}
		data.Data = append(data.Data, source.Data[i*rowWidth:(i+1)*rowWidth]...)
	}
	return nil
}

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

func (data *Float16VectorFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*Float16VectorFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("Float16VectorFieldData.CopyFrom requires a non-nil Float16VectorFieldData source")
	}
	if source.Dim != data.Dim {
		return merr.WrapErrServiceInternalMsg("Float16VectorFieldData.CopyFrom dimension mismatch: source=%d, destination=%d", source.Dim, data.Dim)
	}
	rowWidth := data.Dim * 2
	if data.Nullable {
		validCount := countSelectedValidRows(source.ValidData, indices)
		// Reserve only the selected non-null vectors, preserving existing data.
		if required := len(data.Data) + validCount*rowWidth; required > cap(data.Data) {
			values := make([]byte, len(data.Data), required)
			copy(values, data.Data)
			data.Data = values
		}
	}
	for _, i := range indices {
		if data.Nullable {
			valid := source.ValidData[i]
			data.L2PMapping.Build([]bool{valid}, len(data.ValidData), 1)
			data.ValidData = append(data.ValidData, valid)
			if !valid {
				continue
			}
			i = getNullableVectorPhysicalOffset(source.ValidData, &source.L2PMapping, i)
		}
		data.Data = append(data.Data, source.Data[i*rowWidth:(i+1)*rowWidth]...)
	}
	return nil
}

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

func (data *BFloat16VectorFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*BFloat16VectorFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("BFloat16VectorFieldData.CopyFrom requires a non-nil BFloat16VectorFieldData source")
	}
	if source.Dim != data.Dim {
		return merr.WrapErrServiceInternalMsg("BFloat16VectorFieldData.CopyFrom dimension mismatch: source=%d, destination=%d", source.Dim, data.Dim)
	}
	rowWidth := data.Dim * 2
	if data.Nullable {
		validCount := countSelectedValidRows(source.ValidData, indices)
		// Reserve only the selected non-null vectors, preserving existing data.
		if required := len(data.Data) + validCount*rowWidth; required > cap(data.Data) {
			values := make([]byte, len(data.Data), required)
			copy(values, data.Data)
			data.Data = values
		}
	}
	for _, i := range indices {
		if data.Nullable {
			valid := source.ValidData[i]
			data.L2PMapping.Build([]bool{valid}, len(data.ValidData), 1)
			data.ValidData = append(data.ValidData, valid)
			if !valid {
				continue
			}
			i = getNullableVectorPhysicalOffset(source.ValidData, &source.L2PMapping, i)
		}
		data.Data = append(data.Data, source.Data[i*rowWidth:(i+1)*rowWidth]...)
	}
	return nil
}

func (data *SparseFloatVectorFieldData) AppendRow(row interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			data.Dim, data.L2PMapping.GetMemorySize())
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

// CopyFrom appends the selected source rows without validating sparse vector indices or values.
// Callers must ensure that source data has already been validated.
func (data *SparseFloatVectorFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*SparseFloatVectorFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("SparseFloatVectorFieldData.CopyFrom requires a non-nil SparseFloatVectorFieldData source")
	}
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			data.Dim, data.L2PMapping.GetMemorySize())
	}

	for _, i := range indices {
		if data.Nullable {
			valid := source.ValidData[i]
			data.L2PMapping.Build([]bool{valid}, len(data.ValidData), 1)
			data.ValidData = append(data.ValidData, valid)
			if !valid {
				continue
			}
			i = getNullableVectorPhysicalOffset(source.ValidData, &source.L2PMapping, i)
		}
		row := source.Contents[i]
		data.Contents = append(data.Contents, row)
		if dim := typeutil.SparseFloatRowDim(row); data.Dim < dim {
			data.Dim = dim
		}
	}
	return nil
}

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

func (data *Int8VectorFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*Int8VectorFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("Int8VectorFieldData.CopyFrom requires a non-nil Int8VectorFieldData source")
	}
	if source.Dim != data.Dim {
		return merr.WrapErrServiceInternalMsg("Int8VectorFieldData.CopyFrom dimension mismatch: source=%d, destination=%d", source.Dim, data.Dim)
	}
	rowWidth := data.Dim
	if data.Nullable {
		validCount := countSelectedValidRows(source.ValidData, indices)
		// Reserve only the selected non-null vectors, preserving existing data.
		if required := len(data.Data) + validCount*rowWidth; required > cap(data.Data) {
			values := make([]int8, len(data.Data), required)
			copy(values, data.Data)
			data.Data = values
		}
	}
	for _, i := range indices {
		if data.Nullable {
			valid := source.ValidData[i]
			data.L2PMapping.Build([]bool{valid}, len(data.ValidData), 1)
			data.ValidData = append(data.ValidData, valid)
			if !valid {
				continue
			}
			i = getNullableVectorPhysicalOffset(source.ValidData, &source.L2PMapping, i)
		}
		data.Data = append(data.Data, source.Data[i*rowWidth:(i+1)*rowWidth]...)
	}
	return nil
}

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

func (data *VectorArrayFieldData) CopyFrom(src FieldData, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	source, ok := src.(*VectorArrayFieldData)
	if !ok || source == nil {
		return merr.WrapErrServiceInternalMsg("VectorArrayFieldData.CopyFrom requires a non-nil VectorArrayFieldData source")
	}
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Data), len(data.ValidData))
	}

	for _, i := range indices {
		data.Data = append(data.Data, source.Data[i])
		if data.Nullable {
			data.ValidData = append(data.ValidData, source.ValidData[i])
		}
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

func (data *Int8FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int16FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int32FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *Int64FieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *FloatFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *DoubleFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *TimestamptzFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

// AppendRows retains appended data if validity appending fails.
// Each append step updates its own part of the size cache when tracking is enabled.
func (data *StringFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	if err := data.AppendDataRows(dataRows); err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *ArrayFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *JSONFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
}

func (data *GeometryFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
	err := data.AppendDataRows(dataRows)
	if err != nil {
		return err
	}
	return data.AppendValidDataRows(validDataRows)
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

func (data *VectorArrayFieldData) AppendRows(dataRows interface{}, validDataRows interface{}) error {
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

func (data *Int8FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int8)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int8", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int16FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int16)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int16", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int32FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int32", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *Int64FieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]int64", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *FloatFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]float32)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float32", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *DoubleFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]float64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]float64", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

func (data *TimestamptzFieldData) AppendDataRows(rows interface{}) error {
	v, ok := rows.([]int64)
	if !ok {
		return merr.WrapErrParameterInvalid("[]timestamptz", rows, "Wrong rows type")
	}
	data.Data = append(data.Data, v...)
	return nil
}

// AppendDataRows updates only the payload portion of the size cache when tracking is enabled.
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

func (data *SparseFloatVectorFieldData) AppendDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			data.Dim, data.L2PMapping.GetMemorySize())
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

// AppendValidDataRows updates only the validity portion of the size cache when tracking is enabled.
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

func (data *SparseFloatVectorFieldData) AppendValidDataRows(rows interface{}) error {
	if data.trackMemory {
		defer data.trackAppendedSize(len(data.Contents), len(data.ValidData),
			data.Dim, data.L2PMapping.GetMemorySize())
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

// GetMemorySize implements FieldData.GetMemorySize
func (data *BoolFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int8FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int16FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int32FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *Int64FieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *FloatFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *DoubleFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *TimestamptzFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *BinaryVectorFieldData) GetMemorySize() int {
	// Data + ValidData + Dim(4) + Nullable(1) + L2PMapping
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *FloatVectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *Float16VectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *BFloat16VectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
}

func (data *SparseFloatVectorFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

func (data *Int8VectorFieldData) GetMemorySize() int {
	return binary.Size(data.Data) + binary.Size(data.ValidData) + 4 + 1 + data.L2PMapping.GetMemorySize()
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

func (data *VectorArrayFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// GetDataType implements FieldData.GetDataType
func (data *BoolFieldData) GetDataType() schemapb.DataType   { return schemapb.DataType_Bool }
func (data *Int8FieldData) GetDataType() schemapb.DataType   { return schemapb.DataType_Int8 }
func (data *Int16FieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Int16 }
func (data *Int32FieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Int32 }
func (data *Int64FieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Int64 }
func (data *FloatFieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Float }
func (data *DoubleFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_Double }
func (data *TimestamptzFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Timestamptz
}
func (data *StringFieldData) GetDataType() schemapb.DataType { return data.DataType }
func (data *ArrayFieldData) GetDataType() schemapb.DataType  { return schemapb.DataType_Array }
func (data *ArrayFieldData) GetElementType() schemapb.DataType {
	return data.ElementType
}
func (data *JSONFieldData) GetDataType() schemapb.DataType { return schemapb.DataType_JSON }
func (data *GeometryFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Geometry
}

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

func (data *Int8VectorFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_Int8Vector
}

func (data *VectorArrayFieldData) GetDataType() schemapb.DataType {
	return schemapb.DataType_ArrayOfVector
}

// GetMemorySize returns the cached size or calculates the size from current data.
func (data *StringFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

// nestedArrayElementOverhead is the fixed heap cost of one inner array of a
// nested Array row, on top of its leaf payload: the *ScalarField slot in the
// parent slice (8 bytes), the ScalarField message (80), its oneof wrapper (8)
// and the typed array message such as LongArray or StringArray (64), measured
// on 64-bit Go. A null or empty inner array has no payload, so without it a
// row of null inner arrays would count as nothing and memory-bounded batches
// (import readers, insert buffers) would hold far more than their limit. Like
// the string header in StringFieldData, it is a Go in-memory cost.
const nestedArrayElementOverhead = 160

func getArrayScalarFieldMemorySize(data *schemapb.ScalarField, elementType schemapb.DataType) int {
	if data == nil {
		return 0
	}

	// Nested Array rows are recursively encoded as ScalarField_ArrayData.
	// Descend into that payload before dispatching on the leaf element type.
	if arrayData := data.GetArrayData(); arrayData != nil {
		var size int
		for _, element := range arrayData.GetData() {
			size += nestedArrayElementOverhead
			size += getArrayScalarFieldMemorySize(element, arrayData.GetElementType())
			size += binary.Size(typeutil.GetArrayElementValidData(element))
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

func (data *ArrayFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

func (data *JSONFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

func (data *GeometryFieldData) GetMemorySize() int {
	if data.trackMemory {
		return data.memorySize
	}
	return data.getMemorySize()
}

func (data *BoolFieldData) GetRowSize(i int) int        { return 1 }
func (data *Int8FieldData) GetRowSize(i int) int        { return 1 }
func (data *Int16FieldData) GetRowSize(i int) int       { return 2 }
func (data *Int32FieldData) GetRowSize(i int) int       { return 4 }
func (data *Int64FieldData) GetRowSize(i int) int       { return 8 }
func (data *FloatFieldData) GetRowSize(i int) int       { return 4 }
func (data *DoubleFieldData) GetRowSize(i int) int      { return 8 }
func (data *TimestamptzFieldData) GetRowSize(i int) int { return 8 }
func (data *BinaryVectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim / 8
}

func (data *FloatVectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim * 4
}

func (data *Float16VectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim * 2
}

func (data *BFloat16VectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim * 2
}

func (data *Int8VectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	return data.Dim
}
func (data *StringFieldData) GetRowSize(i int) int   { return len(data.Data[i]) + 16 }
func (data *JSONFieldData) GetRowSize(i int) int     { return len(data.Data[i]) + 16 }
func (data *GeometryFieldData) GetRowSize(i int) int { return len(data.Data[i]) + 16 }
func (data *ArrayFieldData) GetRowSize(i int) int {
	return getArrayScalarFieldMemorySize(data.Data[i], data.ElementType)
}

func (data *SparseFloatVectorFieldData) GetRowSize(i int) int {
	if data.GetNullable() && !data.ValidData[i] {
		return 0
	}
	physicalIdx := data.L2PMapping.GetPhysicalOffset(i)
	return len(data.Contents[physicalIdx])
}

func (data *VectorArrayFieldData) GetRowSize(i int) int {
	return GetVectorSize(data.Data[i], data.ElementType)
}

func (data *BoolFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int8FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int16FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int32FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int64FieldData) GetNullable() bool {
	return data.Nullable
}

func (data *FloatFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *DoubleFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *TimestamptzFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *BFloat16VectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *BinaryVectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *FloatVectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *SparseFloatVectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Float16VectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *Int8VectorFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *StringFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *ArrayFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *ArrayFieldData) GetElementNullable() bool {
	return data.ElementNullable
}

func (data *JSONFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *VectorArrayFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *VectorArrayFieldData) GetElementNullable() bool {
	return data.ElementNullable
}

func (data *GeometryFieldData) GetNullable() bool {
	return data.Nullable
}

func (data *BoolFieldData) GetValidData() []bool              { return data.ValidData }
func (data *Int8FieldData) GetValidData() []bool              { return data.ValidData }
func (data *Int16FieldData) GetValidData() []bool             { return data.ValidData }
func (data *Int32FieldData) GetValidData() []bool             { return data.ValidData }
func (data *Int64FieldData) GetValidData() []bool             { return data.ValidData }
func (data *FloatFieldData) GetValidData() []bool             { return data.ValidData }
func (data *DoubleFieldData) GetValidData() []bool            { return data.ValidData }
func (data *TimestamptzFieldData) GetValidData() []bool       { return data.ValidData }
func (data *StringFieldData) GetValidData() []bool            { return data.ValidData }
func (data *ArrayFieldData) GetValidData() []bool             { return data.ValidData }
func (data *JSONFieldData) GetValidData() []bool              { return data.ValidData }
func (data *GeometryFieldData) GetValidData() []bool          { return data.ValidData }
func (data *BinaryVectorFieldData) GetValidData() []bool      { return data.ValidData }
func (data *FloatVectorFieldData) GetValidData() []bool       { return data.ValidData }
func (data *Float16VectorFieldData) GetValidData() []bool     { return data.ValidData }
func (data *BFloat16VectorFieldData) GetValidData() []bool    { return data.ValidData }
func (data *SparseFloatVectorFieldData) GetValidData() []bool { return data.ValidData }
func (data *Int8VectorFieldData) GetValidData() []bool        { return data.ValidData }
func (data *VectorArrayFieldData) GetValidData() []bool       { return data.ValidData }

// getMemorySize calculates the size without using the cache.
func (data *StringFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *StringFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += len(value) + 16
	}
	data.memorySize += len(data.ValidData) - oldValid
}

// getMemorySize calculates the size without using the cache.
func (data *JSONFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *JSONFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += len(value) + 16
	}
	data.memorySize += len(data.ValidData) - oldValid
}

// getMemorySize calculates the size without using the cache.
func (data *GeometryFieldData) getMemorySize() int {
	var size int
	for _, val := range data.Data {
		size += len(val) + 16
	}
	return size + binary.Size(data.ValidData) + binary.Size(data.Nullable)
}

func (data *GeometryFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += len(value) + 16
	}
	data.memorySize += len(data.ValidData) - oldValid
}

// getMemorySize calculates the size without using the cache.
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

func (data *ArrayFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += getArrayScalarFieldMemorySize(value, data.ElementType) + len(typeutil.GetArrayElementValidData(value))
	}
	data.memorySize += len(data.ValidData) - oldValid
}

// getMemorySize calculates the size without using the cache.
func (data *SparseFloatVectorFieldData) getMemorySize() int {
	// Vector data uses protobuf size; validity, Nullable and L2P metadata are added separately.
	return proto.Size(&data.SparseFloatArray) + binary.Size(data.ValidData) + 1 + data.L2PMapping.GetMemorySize()
}

func (data *SparseFloatVectorFieldData) trackAppendedSize(oldRows, oldValid int, oldDim int64, oldMappingSize int) {
	// +-------+--------+----------+-------+--------+----------+-----+-------+-------+
	// | tag 1 | length | row 0    | tag 1 | length | row 1    | ... | tag 2 | Dim   |
	// +-------+--------+----------+-------+--------+----------+-----+-------+-------+
	// |<----- Contents[0] ------->|<----- Contents[1] ------->| ... |<---- Dim ---->|
	// SizeBytes includes both the length prefix and the payload.
	for _, row := range data.Contents[oldRows:] {
		data.memorySize += protowire.SizeTag(1) + protowire.SizeBytes(len(row))
	}
	if data.Dim != oldDim {
		if data.Dim != 0 {
			data.memorySize += protowire.SizeTag(2) + protowire.SizeVarint(uint64(data.Dim))
		}
		if oldDim != 0 {
			data.memorySize -= protowire.SizeTag(2) + protowire.SizeVarint(uint64(oldDim))
		}
	}
	data.memorySize += len(data.ValidData) - oldValid
	data.memorySize += data.L2PMapping.GetMemorySize() - oldMappingSize
}

// getMemorySize calculates the size without using the cache.
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

func (data *VectorArrayFieldData) trackAppendedSize(oldRows, oldValid int) {
	for _, value := range data.Data[oldRows:] {
		data.memorySize += GetVectorSize(value, data.ElementType) + len(typeutil.GetVectorArrayElementValidData(value))
	}
	data.memorySize += len(data.ValidData) - oldValid
}
