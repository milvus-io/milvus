// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package transformio converts logical FunctionChain input/output records.
// Lists remain Arrow lists rather than storage's encoded protobuf binaries.
package transformio

import (
	"math"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const DataTypeMetadataKey = "milvus.data_type"

// FromFieldData borrows input protos and returns an owned, one-group table.
func FromFieldData(fields []*schemapb.FieldData, rows int64, alloc memory.Allocator) (arrow.Table, error) {
	return FromFieldDataWithTimezone(fields, rows, alloc, "UTC")
}

// FromFieldDataWithTimezone accepts the public TIMESTAMPTZ string wire format
// while exposing canonical UTC microseconds to functions. Other fields are
// borrowed, and normalization never mutates the original request.
func FromFieldDataWithTimezone(fields []*schemapb.FieldData, rows int64, alloc memory.Allocator, timezone string) (arrow.Table, error) {
	if rows < 0 || rows > math.MaxUint32 || len(fields) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("transform input requires fields and a valid row count")
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	arrays := make([]arrow.Array, 0, len(fields))
	defer func() {
		for _, values := range arrays {
			values.Release()
		}
	}()
	arrowFields := make([]arrow.Field, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == nil || strings.TrimSpace(field.GetFieldName()) == "" {
			return nil, merr.WrapErrParameterInvalidMsg("transform input has an unnamed field")
		}
		if _, duplicate := seen[field.GetFieldName()]; duplicate {
			return nil, merr.WrapErrParameterInvalidMsg("duplicate transform input field %q", field.GetFieldName())
		}
		seen[field.GetFieldName()] = struct{}{}
		if field.GetType() == schemapb.DataType_Timestamptz && field.GetScalars().GetStringData() != nil {
			cloned := proto.Clone(field).(*schemapb.FieldData)
			strings := field.GetScalars().GetStringData().GetData()
			data := make([]int64, len(strings))
			valid := typeutil.GetFieldDataValidData(field)
			for index, value := range strings {
				if len(valid) == len(strings) && !valid[index] {
					continue
				}
				normalized, err := timestamptz.ValidateAndReturnUnixMicroTz(value, timezone)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidErr(err, "invalid transform TIMESTAMPTZ input %q", field.GetFieldName())
				}
				data[index] = normalized
			}
			cloned.GetScalars().Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: data}}
			field = cloned
		}
		values, err := fieldToArrow(field, rows, alloc)
		if err != nil {
			return nil, merr.Wrapf(err, "transform input field %q", field.GetFieldName())
		}
		arrays = append(arrays, values)
		arrowFields = append(arrowFields, arrow.Field{Name: field.GetFieldName(), Type: values.DataType(), Nullable: true,
			Metadata: arrow.NewMetadata([]string{DataTypeMetadataKey, "milvus.field_id", "dim", "element_type"}, []string{strconv.Itoa(int(field.GetType())), strconv.FormatInt(field.GetFieldId(), 10), strconv.FormatInt(field.GetVectors().GetDim(), 10), strconv.Itoa(int(field.GetScalars().GetArrayData().GetElementType()))})})
	}
	record := array.NewRecord(arrow.NewSchema(arrowFields, nil), arrays, rows)
	defer record.Release()
	return array.NewTableFromRecords(record.Schema(), []arrow.Record{record}), nil
}

func scalarType(dataType schemapb.DataType) (arrow.DataType, error) {
	switch dataType {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return arrow.BinaryTypes.String, nil
	case schemapb.DataType_JSON, schemapb.DataType_Geometry:
		return arrow.BinaryTypes.Binary, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported transform scalar type %s", dataType)
	}
}

func scalarLen(field *schemapb.ScalarField, dataType schemapb.DataType) int {
	switch dataType {
	case schemapb.DataType_Bool:
		return len(field.GetBoolData().GetData())
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return len(field.GetIntData().GetData())
	case schemapb.DataType_Int64:
		return len(field.GetLongData().GetData())
	case schemapb.DataType_Timestamptz:
		return len(field.GetTimestamptzData().GetData())
	case schemapb.DataType_Float:
		return len(field.GetFloatData().GetData())
	case schemapb.DataType_Double:
		return len(field.GetDoubleData().GetData())
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return len(field.GetStringData().GetData())
	case schemapb.DataType_JSON:
		return len(field.GetJsonData().GetData())
	case schemapb.DataType_Geometry:
		return len(field.GetGeometryData().GetData())
	default:
		return -1
	}
}

func appendScalar(builder array.Builder, field *schemapb.ScalarField, dataType schemapb.DataType, index int) error {
	switch dataType {
	case schemapb.DataType_Bool:
		builder.(*array.BooleanBuilder).Append(field.GetBoolData().Data[index])
	case schemapb.DataType_Int8:
		v := field.GetIntData().Data[index]
		if v < math.MinInt8 || v > math.MaxInt8 {
			return merr.WrapErrParameterInvalidMsg("int8 value overflows")
		}
		builder.(*array.Int8Builder).Append(int8(v))
	case schemapb.DataType_Int16:
		v := field.GetIntData().Data[index]
		if v < math.MinInt16 || v > math.MaxInt16 {
			return merr.WrapErrParameterInvalidMsg("int16 value overflows")
		}
		builder.(*array.Int16Builder).Append(int16(v))
	case schemapb.DataType_Int32:
		builder.(*array.Int32Builder).Append(field.GetIntData().Data[index])
	case schemapb.DataType_Int64:
		builder.(*array.Int64Builder).Append(field.GetLongData().Data[index])
	case schemapb.DataType_Timestamptz:
		builder.(*array.Int64Builder).Append(field.GetTimestamptzData().Data[index])
	case schemapb.DataType_Float:
		builder.(*array.Float32Builder).Append(field.GetFloatData().Data[index])
	case schemapb.DataType_Double:
		builder.(*array.Float64Builder).Append(field.GetDoubleData().Data[index])
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		builder.(*array.StringBuilder).Append(field.GetStringData().Data[index])
	case schemapb.DataType_JSON:
		builder.(*array.BinaryBuilder).Append(field.GetJsonData().Data[index])
	case schemapb.DataType_Geometry:
		builder.(*array.BinaryBuilder).Append(field.GetGeometryData().Data[index])
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported transform scalar type %s", dataType)
	}
	return nil
}

// logicalIndexes accepts full scalar payloads and compact payloads accompanied
// by a row validity mask, and rejects shape errors before indexing any slice.
func logicalIndexes(valid []bool, rows int64, dataLen int) ([]int, error) {
	if len(valid) != 0 && int64(len(valid)) != rows {
		return nil, merr.WrapErrParameterInvalidMsg("validity length does not match row count")
	}
	validCount := int(rows)
	if len(valid) != 0 {
		validCount = 0
		for _, present := range valid {
			if present {
				validCount++
			}
		}
	}
	if dataLen != int(rows) && dataLen != validCount {
		return nil, merr.WrapErrParameterInvalidMsg("payload length %d does not match rows %d or non-null rows %d", dataLen, rows, validCount)
	}
	indexes := make([]int, rows)
	compact := dataLen != int(rows)
	next := 0
	for row := range indexes {
		if len(valid) != 0 && !valid[row] {
			indexes[row] = -1
			continue
		}
		indexes[row] = row
		if compact {
			indexes[row] = next
			next++
		}
	}
	return indexes, nil
}

func fieldToArrow(field *schemapb.FieldData, rows int64, alloc memory.Allocator) (arrow.Array, error) {
	if field.GetStructArrays() != nil {
		return structToArrow(field, rows, alloc)
	}
	if field.GetVectors() != nil {
		return vectorToArrow(field, rows, alloc)
	}
	scalars := field.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrParameterInvalidMsg("scalar payload is missing")
	}
	if field.GetType() == schemapb.DataType_Array {
		data := scalars.GetArrayData()
		if data == nil {
			return nil, merr.WrapErrParameterInvalidMsg("array payload is missing")
		}
		elementType, err := scalarType(data.GetElementType())
		if err != nil {
			return nil, err
		}
		indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(data.Data))
		if err != nil {
			return nil, err
		}
		builder := array.NewListBuilder(alloc, elementType)
		defer builder.Release()
		for _, index := range indexes {
			builder.Append(index >= 0)
			if index < 0 {
				continue
			}
			row := data.Data[index]
			if row == nil {
				return nil, merr.WrapErrParameterInvalidMsg("array row payload is missing")
			}
			count := scalarLen(row, data.GetElementType())
			valid := typeutil.GetArrayElementValidData(row)
			logicalRows := count
			if len(valid) > 0 {
				logicalRows = len(valid)
			}
			elements, err := logicalIndexes(valid, int64(logicalRows), count)
			if err != nil {
				return nil, err
			}
			for _, element := range elements {
				if element < 0 {
					builder.ValueBuilder().AppendNull()
				} else if err := appendScalar(builder.ValueBuilder(), row, data.ElementType, element); err != nil {
					return nil, err
				}
			}
		}
		return builder.NewArray(), nil
	}
	dataType, err := scalarType(field.GetType())
	if err != nil {
		return nil, err
	}
	indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, scalarLen(scalars, field.GetType()))
	if err != nil {
		return nil, err
	}
	builder := array.NewBuilder(alloc, dataType)
	defer builder.Release()
	for _, index := range indexes {
		if index < 0 {
			builder.AppendNull()
		} else if err := appendScalar(builder, scalars, field.GetType(), index); err != nil {
			return nil, err
		}
	}
	return builder.NewArray(), nil
}

func vectorToArrow(field *schemapb.FieldData, rows int64, alloc memory.Allocator) (arrow.Array, error) {
	vectors := field.GetVectors()
	dim := vectors.GetDim()
	if field.GetType() == schemapb.DataType_SparseFloatVector {
		contents := vectors.GetSparseFloatVector().GetContents()
		indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(contents))
		if err != nil {
			return nil, err
		}
		builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		defer builder.Release()
		for _, index := range indexes {
			if index < 0 {
				builder.AppendNull()
			} else {
				builder.Append(contents[index])
			}
		}
		return builder.NewArray(), nil
	}
	if dim <= 0 || dim > math.MaxInt32 {
		return nil, merr.WrapErrParameterInvalidMsg("vector dimension must be positive")
	}
	if field.GetType() == schemapb.DataType_FloatVector {
		data := vectors.GetFloatVector().GetData()
		if len(data)%int(dim) != 0 {
			return nil, merr.WrapErrParameterInvalidMsg("float vector payload is not dimension aligned")
		}
		indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(data)/int(dim))
		if err != nil {
			return nil, err
		}
		builder := array.NewFixedSizeListBuilder(alloc, int32(dim), arrow.PrimitiveTypes.Float32)
		defer builder.Release()
		for _, index := range indexes {
			if index < 0 {
				builder.AppendNull()
				continue
			}
			builder.Append(true)
			builder.ValueBuilder().(*array.Float32Builder).AppendValues(data[index*int(dim):(index+1)*int(dim)], nil)
		}
		return builder.NewArray(), nil
	}
	var data []byte
	width := int(dim)
	switch field.GetType() {
	case schemapb.DataType_BinaryVector:
		data = vectors.GetBinaryVector()
		if dim%8 != 0 {
			return nil, merr.WrapErrParameterInvalidMsg("binary vector dimension must be byte aligned")
		}
		width /= 8
	case schemapb.DataType_Float16Vector:
		data = vectors.GetFloat16Vector()
		width *= 2
	case schemapb.DataType_BFloat16Vector:
		data = vectors.GetBfloat16Vector()
		width *= 2
	case schemapb.DataType_Int8Vector:
		data = vectors.GetInt8Vector()
	case schemapb.DataType_SparseFloatVector:
		contents := vectors.GetSparseFloatVector().GetContents()
		indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(contents))
		if err != nil {
			return nil, err
		}
		builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		defer builder.Release()
		for _, index := range indexes {
			if index < 0 {
				builder.AppendNull()
			} else {
				builder.Append(contents[index])
			}
		}
		return builder.NewArray(), nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported transform vector type %s", field.GetType())
	}
	if width <= 0 || len(data)%width != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("vector payload is not dimension aligned")
	}
	indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(data)/width)
	if err != nil {
		return nil, err
	}
	builder := array.NewFixedSizeBinaryBuilder(alloc, &arrow.FixedSizeBinaryType{ByteWidth: width})
	defer builder.Release()
	for _, index := range indexes {
		if index < 0 {
			builder.AppendNull()
		} else {
			builder.Append(data[index*width : (index+1)*width])
		}
	}
	return builder.NewArray(), nil
}

func structToArrow(field *schemapb.FieldData, rows int64, alloc memory.Allocator) (arrow.Array, error) {
	children := field.GetStructArrays().GetFields()
	if len(children) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("struct array requires child fields")
	}
	arrays := make([]*array.List, 0, len(children))
	fields := make([]arrow.Field, 0, len(children))
	defer func() {
		for _, child := range arrays {
			child.Release()
		}
	}()
	for _, child := range children {
		values, err := fieldToArrow(child, rows, alloc)
		if err != nil {
			return nil, err
		}
		list, ok := values.(*array.List)
		if !ok {
			values.Release()
			return nil, merr.WrapErrParameterInvalidMsg("struct child %q must be a scalar array", child.GetFieldName())
		}
		arrays = append(arrays, list)
		fields = append(fields, arrow.Field{Name: child.GetFieldName(), Type: list.ListValues().DataType(), Nullable: true})
	}
	builder := array.NewListBuilder(alloc, arrow.StructOf(fields...))
	defer builder.Release()
	structBuilder := builder.ValueBuilder().(*array.StructBuilder)
	for row := 0; row < int(rows); row++ {
		first := arrays[0]
		start, end := first.ValueOffsets(row)
		builder.Append(first.IsValid(row))
		if first.IsNull(row) {
			for _, child := range arrays {
				if !child.IsNull(row) {
					return nil, merr.WrapErrParameterInvalidMsg("struct children have different row validity")
				}
			}
			continue
		}
		for _, child := range arrays {
			childStart, childEnd := child.ValueOffsets(row)
			if child.IsNull(row) || childEnd-childStart != end-start {
				return nil, merr.WrapErrParameterInvalidMsg("struct children have different element counts")
			}
		}
		for element := int64(0); element < end-start; element++ {
			structBuilder.Append(true)
			for column, child := range arrays {
				childStart, _ := child.ValueOffsets(row)
				if err := appendArrowValue(structBuilder.FieldBuilder(column), child.ListValues(), int(childStart+element)); err != nil {
					return nil, err
				}
			}
		}
	}
	return builder.NewArray(), nil
}

func appendArrowValue(builder array.Builder, values arrow.Array, index int) error {
	if values.IsNull(index) {
		builder.AppendNull()
		return nil
	}
	switch values := values.(type) {
	case *array.Boolean:
		builder.(*array.BooleanBuilder).Append(values.Value(index))
	case *array.Int8:
		builder.(*array.Int8Builder).Append(values.Value(index))
	case *array.Int16:
		builder.(*array.Int16Builder).Append(values.Value(index))
	case *array.Int32:
		builder.(*array.Int32Builder).Append(values.Value(index))
	case *array.Int64:
		builder.(*array.Int64Builder).Append(values.Value(index))
	case *array.Float32:
		builder.(*array.Float32Builder).Append(values.Value(index))
	case *array.Float64:
		builder.(*array.Float64Builder).Append(values.Value(index))
	case *array.String:
		builder.(*array.StringBuilder).Append(values.Value(index))
	case *array.Binary:
		builder.(*array.BinaryBuilder).Append(values.Value(index))
	default:
		return merr.WrapErrFunctionFailedMsg("unsupported struct child Arrow type %s", values.DataType())
	}
	return nil
}

// ToFieldData binds output columns by name. A nil schema list infers response
// types; an explicit list enforces storage types and dimensions. No Arrow
// buffer is retained by the returned protos.
func ToFieldData(table arrow.Table, schemas []*schemapb.FieldSchema) ([]*schemapb.FieldData, error) {
	if table == nil {
		return nil, merr.WrapErrServiceInternalMsg("transform output table is nil")
	}
	byName := make(map[string]*schemapb.FieldSchema, len(schemas))
	for _, schema := range schemas {
		byName[schema.GetName()] = schema
	}
	result := make([]*schemapb.FieldData, 0, table.NumCols())
	for column, field := range table.Schema().Fields() {
		schema := byName[field.Name]
		if len(schemas) != 0 && schema == nil {
			return nil, merr.WrapErrFunctionFailedMsg("transform output %q is not a collection field; select final storage fields", field.Name)
		}
		if schema == nil {
			var err error
			schema, err = inferredSchema(field)
			if err != nil {
				return nil, err
			}
		}
		var values arrow.Array
		var err error
		if len(table.Column(column).Data().Chunks()) == 0 {
			values = array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, 0)
		} else {
			values, err = array.Concatenate(table.Column(column).Data().Chunks(), memory.DefaultAllocator)
		}
		if err != nil {
			return nil, merr.WrapErrFunctionFailed(err, "cannot concatenate transform output column %q", field.Name)
		}
		data, err := arrowToField(values, schema)
		values.Release()
		if err != nil {
			return nil, merr.Wrapf(err, "transform output field %q", field.Name)
		}
		result = append(result, data)
	}
	return result, nil
}

func inferredSchema(field arrow.Field) (*schemapb.FieldSchema, error) {
	schema := &schemapb.FieldSchema{Name: field.Name, Nullable: field.Nullable}
	if value, exists := field.Metadata.GetValue("dim"); exists && value != "0" {
		schema.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: value}}
	}
	if value, exists := field.Metadata.GetValue("element_type"); exists {
		if parsed, err := strconv.ParseInt(value, 10, 32); err == nil {
			schema.ElementType = schemapb.DataType(parsed)
		}
	}
	if value, exists := field.Metadata.GetValue(DataTypeMetadataKey); exists {
		if parsed, err := strconv.ParseInt(value, 10, 32); err == nil {
			schema.DataType = schemapb.DataType(parsed)
		}
	}
	if schema.GetDataType() == schemapb.DataType_None {
		switch field.Type.ID() {
		case arrow.BOOL:
			schema.DataType = schemapb.DataType_Bool
		case arrow.INT8:
			schema.DataType = schemapb.DataType_Int8
		case arrow.INT16:
			schema.DataType = schemapb.DataType_Int16
		case arrow.INT32:
			schema.DataType = schemapb.DataType_Int32
		case arrow.INT64:
			schema.DataType = schemapb.DataType_Int64
		case arrow.FLOAT32:
			schema.DataType = schemapb.DataType_Float
		case arrow.FLOAT64:
			schema.DataType = schemapb.DataType_Double
		case arrow.STRING, arrow.LARGE_STRING:
			schema.DataType = schemapb.DataType_VarChar
		case arrow.BINARY, arrow.STRUCT:
			schema.DataType = schemapb.DataType_JSON
		case arrow.LIST, arrow.LARGE_LIST:
			list := field.Type.(arrow.ListLikeType)
			if list.Elem().ID() == arrow.STRUCT {
				schema.DataType = schemapb.DataType_ArrayOfStruct
			} else {
				schema.DataType = schemapb.DataType_Array
				child, err := inferredSchema(arrow.Field{Type: list.Elem()})
				if err != nil {
					return nil, err
				}
				schema.ElementType = child.DataType
			}
		case arrow.FIXED_SIZE_LIST:
			schema.DataType = schemapb.DataType_FloatVector
		default:
			return nil, merr.WrapErrFunctionFailedMsg("unsupported response Arrow type %s", field.Type)
		}
	}
	if schema.GetDataType() == schemapb.DataType_FloatVector {
		if list, ok := field.Type.(*arrow.FixedSizeListType); ok {
			schema.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(int(list.Len()))}}
		}
	}
	if schema.GetDataType() == schemapb.DataType_Array && schema.GetElementType() == schemapb.DataType_None {
		if list, ok := field.Type.(arrow.ListLikeType); ok {
			child, err := inferredSchema(arrow.Field{Type: list.Elem()})
			if err != nil {
				return nil, err
			}
			schema.ElementType = child.DataType
		}
	}
	return schema, nil
}

func arrowToField(values arrow.Array, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	field := &schemapb.FieldData{Type: schema.GetDataType(), FieldName: schema.GetName(), FieldId: schema.GetFieldID(), IsDynamic: schema.GetIsDynamic()}
	valid := make([]bool, values.Len())
	for row := range valid {
		valid[row] = values.IsValid(row)
	}
	if typeutil.IsVectorType(schema.GetDataType()) {
		vector, err := arrowToVector(values, schema)
		if err != nil {
			return nil, err
		}
		field.Field = &schemapb.FieldData_Vectors{Vectors: vector}
	} else {
		if schema.GetDataType() == schemapb.DataType_ArrayOfStruct {
			return arrowToStruct(values, schema)
		}
		scalars, err := arrowToScalar(values, schema.GetDataType(), schema.GetElementType())
		if err != nil {
			return nil, err
		}
		field.Field = &schemapb.FieldData_Scalars{Scalars: scalars}
	}
	if values.NullN() > 0 || schema.GetNullable() {
		typeutil.SetFieldDataValidData(field, valid)
	}
	return field, nil
}

func arrowToScalar(values arrow.Array, dataType, elementType schemapb.DataType) (*schemapb.ScalarField, error) {
	result := &schemapb.ScalarField{}
	switch dataType {
	case schemapb.DataType_Bool:
		v, ok := values.(*array.Boolean)
		if !ok {
			return nil, typeMismatch(dataType, values)
		}
		data := make([]bool, values.Len())
		for i := range data {
			if v.IsValid(i) {
				data[i] = v.Value(i)
			}
		}
		result.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		data := make([]int32, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			v, ok := integerValue(values, i)
			if !ok || v < math.MinInt32 || v > math.MaxInt32 || dataType == schemapb.DataType_Int8 && (v < math.MinInt8 || v > math.MaxInt8) || dataType == schemapb.DataType_Int16 && (v < math.MinInt16 || v > math.MaxInt16) {
				return nil, typeMismatch(dataType, values)
			}
			data[i] = int32(v)
		}
		result.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz:
		data := make([]int64, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			v, ok := integerValue(values, i)
			if !ok {
				return nil, typeMismatch(dataType, values)
			}
			data[i] = v
		}
		if dataType == schemapb.DataType_Timestamptz {
			result.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: data}}
		} else {
			result.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}
		}
	case schemapb.DataType_Float:
		data := make([]float32, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			v, ok := floatValue(values, i)
			if !ok || math.Abs(v) > math.MaxFloat32 {
				return nil, typeMismatch(dataType, values)
			}
			data[i] = float32(v)
		}
		result.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}
	case schemapb.DataType_Double:
		data := make([]float64, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			v, ok := floatValue(values, i)
			if !ok {
				return nil, typeMismatch(dataType, values)
			}
			data[i] = v
		}
		result.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		data := make([]string, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			switch v := values.(type) {
			case *array.String:
				data[i] = strings.Clone(v.Value(i))
			case *array.LargeString:
				data[i] = strings.Clone(v.Value(i))
			default:
				return nil, typeMismatch(dataType, values)
			}
		}
		result.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}
	case schemapb.DataType_JSON, schemapb.DataType_Geometry:
		data := make([][]byte, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			switch v := values.(type) {
			case *array.Binary:
				data[i] = append([]byte(nil), v.Value(i)...)
			case *array.String:
				data[i] = []byte(v.Value(i))
			default:
				if dataType != schemapb.DataType_JSON {
					return nil, typeMismatch(dataType, values)
				}
				var err error
				data[i], err = encodeJSONOutput(values, i)
				if err != nil {
					return nil, err
				}
			}
			if dataType == schemapb.DataType_JSON && !validJSONOutput(data[i]) {
				return nil, merr.WrapErrFunctionFailedMsg("transform JSON output is malformed")
			}
		}
		if dataType == schemapb.DataType_JSON {
			result.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}}
		} else {
			result.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: data}}
		}
	case schemapb.DataType_Array:
		data := make([]*schemapb.ScalarField, values.Len())
		for row := range data {
			start, end, child, ok := listValues(values, row)
			if !ok {
				return nil, typeMismatch(dataType, values)
			}
			sliced := array.NewSlice(child, start, end)
			inner, err := arrowToScalar(sliced, elementType, schemapb.DataType_None)
			if sliced.NullN() > 0 {
				innerValid := make([]bool, sliced.Len())
				for i := range innerValid {
					innerValid[i] = sliced.IsValid(i)
				}
				if inner != nil {
					inner.ValidData = innerValid
				}
			}
			sliced.Release()
			if err != nil {
				return nil, err
			}
			data[row] = inner
		}
		result.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: data, ElementType: elementType}}
	default:
		return nil, merr.WrapErrFunctionFailedMsg("unsupported output scalar type %s", dataType)
	}
	return result, nil
}

func integerValue(values arrow.Array, index int) (int64, bool) {
	switch v := values.(type) {
	case *array.Int8:
		return int64(v.Value(index)), true
	case *array.Int16:
		return int64(v.Value(index)), true
	case *array.Int32:
		return int64(v.Value(index)), true
	case *array.Int64:
		return v.Value(index), true
	default:
		return 0, false
	}
}
func floatValue(values arrow.Array, index int) (float64, bool) {
	switch v := values.(type) {
	case *array.Float32:
		return float64(v.Value(index)), true
	case *array.Float64:
		return v.Value(index), true
	default:
		return 0, false
	}
}
func typeMismatch(expected schemapb.DataType, values arrow.Array) error {
	return merr.WrapErrFunctionFailedMsg("output Arrow type %s does not match %s", values.DataType(), expected)
}
func listValues(values arrow.Array, row int) (int64, int64, arrow.Array, bool) {
	switch list := values.(type) {
	case *array.List:
		start, end := list.ValueOffsets(row)
		return start, end, list.ListValues(), true
	case *array.LargeList:
		start, end := list.ValueOffsets(row)
		return start, end, list.ListValues(), true
	case *array.FixedSizeList:
		start, end := list.ValueOffsets(row)
		return start, end, list.ListValues(), true
	default:
		return 0, 0, nil, false
	}
}

func arrowToVector(values arrow.Array, schema *schemapb.FieldSchema) (*schemapb.VectorField, error) {
	if schema.GetDataType() == schemapb.DataType_SparseFloatVector {
		binaryValues, ok := values.(*array.Binary)
		if !ok {
			return nil, typeMismatch(schema.GetDataType(), values)
		}
		var contents [][]byte
		var dimension int64
		for row := 0; row < values.Len(); row++ {
			if values.IsNull(row) {
				continue
			}
			data := append([]byte(nil), binaryValues.Value(row)...)
			if err := typeutil.ValidateSparseFloatRows(data); err != nil {
				return nil, merr.Wrap(err, "transform sparse vector output")
			}
			contents = append(contents, data)
			dimension = max(dimension, typeutil.SparseFloatRowDim(data))
		}
		return &schemapb.VectorField{Dim: dimension, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: dimension, Contents: contents}}}, nil
	}
	dim, err := typeutil.GetDim(schema)
	if err != nil {
		return nil, merr.Wrap(err, "transform vector output dimension")
	}
	vector := &schemapb.VectorField{Dim: dim}
	if schema.GetDataType() == schemapb.DataType_FloatVector {
		var data []float32
		for row := 0; row < values.Len(); row++ {
			if values.IsNull(row) {
				continue
			}
			start, end, child, ok := listValues(values, row)
			if !ok || end-start != dim {
				return nil, merr.WrapErrFunctionFailedMsg("float vector output does not match dimension %d", dim)
			}
			for i := start; i < end; i++ {
				v, ok := floatValue(child, int(i))
				if !ok || child.IsNull(int(i)) || math.Abs(v) > math.MaxFloat32 {
					return nil, typeMismatch(schema.GetDataType(), values)
				}
				data = append(data, float32(v))
			}
		}
		vector.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}}
		return vector, nil
	}
	var data []byte
	width := int(dim)
	if schema.GetDataType() == schemapb.DataType_BinaryVector {
		width /= 8
	}
	if schema.GetDataType() == schemapb.DataType_Float16Vector || schema.GetDataType() == schemapb.DataType_BFloat16Vector {
		width *= 2
	}
	for row := 0; row < values.Len(); row++ {
		if values.IsNull(row) {
			continue
		}
		var bytes []byte
		switch v := values.(type) {
		case *array.FixedSizeBinary:
			bytes = v.Value(row)
		case *array.Binary:
			bytes = v.Value(row)
		default:
			return nil, typeMismatch(schema.GetDataType(), values)
		}
		if len(bytes) != width {
			return nil, merr.WrapErrFunctionFailedMsg("vector byte output does not match dimension")
		}
		data = append(data, bytes...)
	}
	switch schema.GetDataType() {
	case schemapb.DataType_BinaryVector:
		vector.Data = &schemapb.VectorField_BinaryVector{BinaryVector: data}
	case schemapb.DataType_Float16Vector:
		vector.Data = &schemapb.VectorField_Float16Vector{Float16Vector: data}
	case schemapb.DataType_BFloat16Vector:
		vector.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data}
	case schemapb.DataType_Int8Vector:
		vector.Data = &schemapb.VectorField_Int8Vector{Int8Vector: data}
	default:
		return nil, merr.WrapErrFunctionFailedMsg("unsupported output vector type %s", schema.GetDataType())
	}
	return vector, nil
}

func arrowToStruct(values arrow.Array, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	var raw arrow.Array
	switch list := values.(type) {
	case *array.List:
		raw = list.ListValues()
	case *array.LargeList:
		raw = list.ListValues()
	default:
		return nil, typeMismatch(schema.GetDataType(), values)
	}
	structs, ok := raw.(*array.Struct)
	if !ok {
		return nil, typeMismatch(schema.GetDataType(), values)
	}
	children := make([]*schemapb.FieldData, structs.NumField())
	for column := range children {
		childField := structs.DataType().(*arrow.StructType).Field(column)
		childSchema, err := inferredSchema(childField)
		if err != nil {
			return nil, err
		}
		rows := make([]*schemapb.ScalarField, values.Len())
		valid := make([]bool, values.Len())
		for row := range rows {
			start, end, _, _ := listValues(values, row)
			valid[row] = values.IsValid(row)
			sliced := array.NewSlice(structs.Field(column), start, end)
			inner, err := arrowToScalar(sliced, childSchema.GetDataType(), schemapb.DataType_None)
			if err != nil {
				sliced.Release()
				return nil, err
			}
			if sliced.NullN() > 0 {
				inner.ValidData = make([]bool, sliced.Len())
				for i := range inner.ValidData {
					inner.ValidData[i] = sliced.IsValid(i)
				}
			}
			sliced.Release()
			rows[row] = inner
		}
		children[column] = &schemapb.FieldData{Type: schemapb.DataType_Array, FieldName: childField.Name, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows, ElementType: childSchema.GetDataType()}}, ValidData: valid}}}
	}
	return &schemapb.FieldData{Type: schemapb.DataType_ArrayOfStruct, FieldName: schema.GetName(), FieldId: schema.GetFieldID(), Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}}}, nil
}
