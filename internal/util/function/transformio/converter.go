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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const DataTypeMetadataKey = types.MetadataKeyMilvusDataType

// FromFieldData borrows input protos and returns an owned, one-group table.
func FromFieldData(fields []*schemapb.FieldData, rows int64, alloc memory.Allocator, schemas ...*schemapb.FieldSchema) (arrow.Table, error) {
	return FromFieldDataWithTimezone(fields, rows, alloc, "UTC", schemas...)
}

// FromFieldDataWithTimezone accepts the public TIMESTAMPTZ string wire format
// while exposing canonical UTC microseconds to functions. Other fields are
// borrowed, and normalization never mutates the original request.
func FromFieldDataWithTimezone(fields []*schemapb.FieldData, rows int64, alloc memory.Allocator, timezone string, schemas ...*schemapb.FieldSchema) (arrow.Table, error) {
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
		var declared *schemapb.FieldSchema
		for _, schema := range schemas {
			if schema.GetName() == field.GetFieldName() {
				declared = schema
				break
			}
		}
		if field.GetType() == schemapb.DataType_Geometry && field.GetScalars().GetGeometryWktData() != nil {
			normalized := proto.Clone(field).(*schemapb.FieldData)
			strings := field.GetScalars().GetGeometryWktData().GetData()
			data := make([][]byte, len(strings))
			valid := typeutil.GetFieldDataValidData(field)
			for index, value := range strings {
				if len(valid) == len(strings) && !valid[index] {
					continue
				}
				encoded, err := common.ConvertWKTToWKB(value)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidErr(err, "invalid transform geometry WKT input")
				}
				data[index] = encoded
			}
			normalized.GetScalars().Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: data}}
			field = normalized
		}
		values, err := fieldToArrowWithSchema(field, rows, alloc, declared)
		if err != nil {
			return nil, merr.Wrapf(err, "transform input field %q", field.GetFieldName())
		}
		arrays = append(arrays, values)
		metadataSchema := fieldDataSchema(field)
		if declared != nil {
			metadataSchema = proto.Clone(declared).(*schemapb.FieldSchema)
		}
		metadataSchema.Nullable = true // A wire payload alone does not declare non-nullability.
		encoded, err := protojson.Marshal(metadataSchema)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "serialize transform input schema")
		}
		metadata := map[string]string{DataTypeMetadataKey: strconv.Itoa(int(field.GetType())), types.MetadataKeyMilvusFieldSchema: string(encoded), "milvus.field_id": strconv.FormatInt(field.GetFieldId(), 10), "element_type": strconv.Itoa(int(metadataSchema.GetElementType()))}
		for _, param := range metadataSchema.GetTypeParams() {
			metadata[param.GetKey()] = param.GetValue()
		}
		arrowFields = append(arrowFields, arrow.Field{Name: field.GetFieldName(), Type: values.DataType(), Nullable: true, Metadata: arrow.MetadataFrom(metadata)})
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
	case schemapb.DataType_Int32, schemapb.DataType_Date:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz, schemapb.DataType_Time:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return arrow.BinaryTypes.String, nil
	case schemapb.DataType_JSON, schemapb.DataType_Geometry, schemapb.DataType_Mol:
		return arrow.BinaryTypes.Binary, nil
	case schemapb.DataType_Decimal:
		return &arrow.FixedSizeBinaryType{ByteWidth: 8}, nil
	case schemapb.DataType_UUID:
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}, nil
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
	case schemapb.DataType_Mol:
		return len(field.GetMolData().GetData())
	case schemapb.DataType_Date:
		return len(field.GetDateData().GetData())
	case schemapb.DataType_Time:
		return len(field.GetTimeData().GetData())
	case schemapb.DataType_Decimal, schemapb.DataType_UUID:
		return len(field.GetBytesData().GetData())
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
	case schemapb.DataType_Mol:
		builder.(*array.BinaryBuilder).Append(field.GetMolData().Data[index])
	case schemapb.DataType_Date:
		builder.(*array.Int32Builder).Append(field.GetDateData().Data[index])
	case schemapb.DataType_Time:
		builder.(*array.Int64Builder).Append(field.GetTimeData().Data[index])
	case schemapb.DataType_Decimal, schemapb.DataType_UUID:
		value := field.GetBytesData().Data[index]
		width := builder.(*array.FixedSizeBinaryBuilder).Type().(*arrow.FixedSizeBinaryType).ByteWidth
		if len(value) != width {
			return merr.WrapErrParameterInvalidMsg("%s payload has invalid byte width", dataType)
		}
		builder.(*array.FixedSizeBinaryBuilder).Append(value)
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
	return fieldToArrowWithSchema(field, rows, alloc, nil)
}

func fieldDataSchema(field *schemapb.FieldData) *schemapb.FieldSchema {
	schema := &schemapb.FieldSchema{Name: field.GetFieldName(), FieldID: field.GetFieldId(), DataType: field.GetType(), Nullable: true, ElementNullable: true}
	if data := field.GetScalars().GetArrayData(); data != nil {
		schema.ElementType = data.GetElementType()
	}
	if vector := field.GetVectors(); vector != nil {
		dim := vector.GetDim()
		if vectors := vector.GetVectorArray(); vectors != nil {
			dim = vectors.GetDim()
			schema.ElementType = vectors.GetElementType()
		}
		schema.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.FormatInt(dim, 10)}}
	}
	return schema
}

func fieldToArrowWithSchema(field *schemapb.FieldData, rows int64, alloc memory.Allocator, schema *schemapb.FieldSchema) (arrow.Array, error) {
	if field.GetStructArrays() != nil {
		return structToArrow(field, rows, alloc)
	}
	if field.GetVectors() != nil {
		if field.GetType() == schemapb.DataType_ArrayOfVector {
			return vectorArrayToArrow(field, rows, alloc)
		}
		return vectorToArrow(field, rows, alloc)
	}
	scalars := field.GetScalars()
	if scalars == nil {
		return nil, merr.WrapErrParameterInvalidMsg("scalar payload is missing")
	}
	if err := validateScalarInputType(scalars, field.GetType()); err != nil {
		return nil, err
	}
	if field.GetType() == schemapb.DataType_Array {
		data := scalars.GetArrayData()
		if data == nil {
			return nil, merr.WrapErrParameterInvalidMsg("array payload is missing")
		}
		if data.GetElementType() == schemapb.DataType_Array || schema != nil && schema.GetTypeSchema().GetArrayElement().GetArrayElement() != nil {
			return nestedArrayToArrow(field, rows, alloc, schema)
		}
		elementType, err := scalarType(data.GetElementType())
		if err != nil {
			return nil, err
		}
		indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(data.Data))
		if err != nil {
			return nil, err
		}
		elementField := arrow.Field{Name: "item", Type: elementType, Nullable: true, Metadata: arrow.NewMetadata([]string{types.MetadataKeyMilvusDataType}, []string{strconv.Itoa(int(data.GetElementType()))})}
		builder := array.NewListBuilderWithField(alloc, elementField)
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
		childSchema := fieldDataSchema(child)
		childSchema.DataType = childSchema.GetElementType()
		childSchema.ElementType = schemapb.DataType_None
		encoded, err := protojson.Marshal(childSchema)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "encode transform struct child schema")
		}
		metadata := map[string]string{DataTypeMetadataKey: strconv.Itoa(int(childSchema.GetDataType())), types.MetadataKeyMilvusFieldSchema: string(encoded), "milvus.field_id": strconv.FormatInt(child.GetFieldId(), 10)}
		for _, param := range childSchema.GetTypeParams() {
			metadata[param.GetKey()] = param.GetValue()
		}
		fields = append(fields, arrow.Field{Name: child.GetFieldName(), Type: list.ListValues().DataType(), Nullable: true, Metadata: arrow.MetadataFrom(metadata)})
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
	case *array.LargeBinary:
		builder.(*array.BinaryBuilder).Append(values.Value(index))
	case *array.LargeString:
		builder.(*array.StringBuilder).Append(values.Value(index))
	case *array.FixedSizeBinary:
		builder.(*array.FixedSizeBinaryBuilder).Append(values.Value(index))
	case *array.FixedSizeList:
		target, ok := builder.(*array.FixedSizeListBuilder)
		if !ok {
			return nilBuilderType(values)
		}
		target.Append(true)
		start, end := values.ValueOffsets(index)
		for child := start; child < end; child++ {
			if err := appendArrowValue(target.ValueBuilder(), values.ListValues(), int(child)); err != nil {
				return err
			}
		}
	case *array.List:
		target, ok := builder.(*array.ListBuilder)
		if !ok {
			return nilBuilderType(values)
		}
		target.Append(true)
		start, end := values.ValueOffsets(index)
		for child := start; child < end; child++ {
			if err := appendArrowValue(target.ValueBuilder(), values.ListValues(), int(child)); err != nil {
				return err
			}
		}
	case *array.Struct:
		target, ok := builder.(*array.StructBuilder)
		if !ok {
			return nilBuilderType(values)
		}
		target.Append(true)
		for child := 0; child < values.NumField(); child++ {
			if err := appendArrowValue(target.FieldBuilder(child), values.Field(child), index); err != nil {
				return err
			}
		}
	default:
		return merr.WrapErrFunctionFailedMsg("unsupported struct child Arrow type %s", values.DataType())
	}
	return nil
}

func nilBuilderType(values arrow.Array) error {
	return merr.WrapErrFunctionFailedMsg("nested Arrow output builder differs from %s", values.DataType())
}

// ToFieldData binds output columns by name. A nil schema list infers response
// types; an explicit list enforces storage types and dimensions. No Arrow
// buffer is retained by the returned protos.
func ToFieldData(table arrow.Table, schemas []*schemapb.FieldSchema, structs ...*schemapb.StructArrayFieldSchema) ([]*schemapb.FieldData, error) {
	if table == nil {
		return nil, merr.WrapErrServiceInternalMsg("transform output table is nil")
	}
	if err := types.ValidateRuntimeArrowSchema(table.Schema()); err != nil {
		return nil, err
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
		if len(schemas) > 0 {
			if raw, ok := field.Metadata.GetValue(DataTypeMetadataKey); ok {
				parsed, err := strconv.ParseInt(raw, 10, 32)
				if err != nil {
					return nil, merr.WrapErrFunctionFailedMsg("invalid output logical type metadata")
				}
				actual := schemapb.DataType(parsed)
				special := func(t schemapb.DataType) bool {
					return typeutil.IsVectorType(t) || t == schemapb.DataType_ArrayOfVector || t == schemapb.DataType_JSON || t == schemapb.DataType_Geometry || t == schemapb.DataType_Mol || t == schemapb.DataType_Timestamptz || t == schemapb.DataType_Date || t == schemapb.DataType_Time || t == schemapb.DataType_Decimal || t == schemapb.DataType_UUID
				}
				materializesJSON := schema.GetDataType() == schemapb.DataType_JSON && !special(actual)
				materializesGeometry := schema.GetDataType() == schemapb.DataType_Geometry && (actual == schemapb.DataType_String || actual == schemapb.DataType_VarChar || actual == schemapb.DataType_Text)
				materializesTimestamp := schema.GetDataType() == schemapb.DataType_Timestamptz && actual == schemapb.DataType_Int64
				if actual != schema.GetDataType() && (special(actual) || special(schema.GetDataType())) && !materializesJSON && !materializesGeometry && !materializesTimestamp {
					return nil, merr.WrapErrFunctionFailedMsg("output logical type %s differs from destination %s", actual, schema.GetDataType())
				}
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
		var data *schemapb.FieldData
		if err := validateStorageNullability(values, schema); err != nil {
			values.Release()
			return nil, err
		}
		var children []*schemapb.FieldSchema
		for _, structure := range structs {
			if structure.GetName() == schema.GetName() {
				children = structure.GetFields()
				break
			}
		}
		if schema.GetDataType() == schemapb.DataType_ArrayOfStruct {
			data, err = arrowToStruct(values, schema, children...)
		} else {
			data, err = arrowToField(values, schema)
		}
		values.Release()
		if err != nil {
			return nil, merr.Wrapf(err, "transform output field %q", field.Name)
		}
		result = append(result, data)
	}
	return result, nil
}

func inferredSchema(field arrow.Field) (*schemapb.FieldSchema, error) {
	// Scalar Struct has no FieldData wire carrier. The historical HITS export
	// projects it as JSON; independent TABLE results retain the complete Arrow
	// structure and its schema.
	if field.Type.ID() == arrow.STRUCT {
		return &schemapb.FieldSchema{Name: field.Name, DataType: schemapb.DataType_JSON, Nullable: field.Nullable}, nil
	}
	return types.MilvusSchemaFromArrowField(field)
}

func arrowToField(values arrow.Array, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	field := &schemapb.FieldData{Type: schema.GetDataType(), FieldName: schema.GetName(), FieldId: schema.GetFieldID(), IsDynamic: schema.GetIsDynamic()}
	valid := make([]bool, values.Len())
	for row := range valid {
		valid[row] = values.IsValid(row)
	}
	if typeutil.IsVectorType(schema.GetDataType()) || schema.GetDataType() == schemapb.DataType_ArrayOfVector {
		vector, err := arrowToVector(values, schema)
		if err != nil {
			return nil, err
		}
		field.Field = &schemapb.FieldData_Vectors{Vectors: vector}
	} else {
		if schema.GetDataType() == schemapb.DataType_ArrayOfStruct {
			return arrowToStruct(values, schema)
		}
		scalars, err := arrowToScalarForSchema(values, schema)
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
	if err := validateScalarOutputType(values, dataType); err != nil {
		return nil, err
	}
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
	case schemapb.DataType_Int64, schemapb.DataType_Timestamptz, schemapb.DataType_Time:
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
		if dataType == schemapb.DataType_Time {
			result.Data = &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: data}}
		} else if dataType == schemapb.DataType_Timestamptz {
			result.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: data}}
		} else {
			result.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}
		}
	case schemapb.DataType_Date:
		data := make([]int32, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			v, ok := integerValue(values, i)
			if !ok || v < math.MinInt32 || v > math.MaxInt32 {
				return nil, typeMismatch(dataType, values)
			}
			data[i] = int32(v)
		}
		result.Data = &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: data}}
	case schemapb.DataType_Decimal, schemapb.DataType_UUID:
		data := make([][]byte, values.Len())
		width := 8
		if dataType == schemapb.DataType_UUID {
			width = 16
		}
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			var raw []byte
			switch v := values.(type) {
			case *array.FixedSizeBinary:
				raw = v.Value(i)
			case *array.Binary:
				raw = v.Value(i)
			case *array.LargeBinary:
				raw = v.Value(i)
			default:
				return nil, typeMismatch(dataType, values)
			}
			if len(raw) != width {
				return nil, merr.WrapErrFunctionFailedMsg("%s output has invalid byte width", dataType)
			}
			data[i] = append([]byte(nil), raw...)
		}
		result.Data = &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: data}}
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
	case schemapb.DataType_JSON, schemapb.DataType_Geometry, schemapb.DataType_Mol:
		data := make([][]byte, values.Len())
		for i := range data {
			if values.IsNull(i) {
				continue
			}
			switch v := values.(type) {
			case *array.Binary:
				data[i] = append([]byte(nil), v.Value(i)...)
			case *array.LargeBinary:
				data[i] = append([]byte(nil), v.Value(i)...)
			case *array.String:
				if dataType == schemapb.DataType_Geometry {
					encoded, err := common.ConvertWKTToWKB(v.Value(i))
					if err != nil {
						return nil, merr.WrapErrFunctionFailed(err, "invalid geometry WKT output")
					}
					data[i] = encoded
				} else {
					data[i] = []byte(v.Value(i))
				}
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
		if dataType == schemapb.DataType_Mol {
			result.Data = &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: data}}
		} else if dataType == schemapb.DataType_JSON {
			result.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}}
		} else {
			result.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: data}}
		}
	case schemapb.DataType_Array:
		if elementType == schemapb.DataType_None {
			childSchema, err := types.MilvusSchemaFromArrowField(values.DataType().(arrow.ListLikeType).ElemField())
			if err != nil {
				return nil, err
			}
			elementType = childSchema.GetDataType()
		}
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
	if schema.GetDataType() == schemapb.DataType_ArrayOfVector {
		return arrowToVectorArray(values, schema)
	}
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
	if dim <= 0 || dim > math.MaxInt32 {
		return nil, merr.WrapErrFunctionFailedMsg("output vector dimension must be positive")
	}
	if schema.GetDataType() == schemapb.DataType_BinaryVector && dim%8 != 0 {
		return nil, merr.WrapErrFunctionFailedMsg("output binary vector dimension must be byte aligned")
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
		case *array.LargeBinary:
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

func arrowToStruct(values arrow.Array, schema *schemapb.FieldSchema, schemas ...*schemapb.FieldSchema) (*schemapb.FieldData, error) {
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
	for row := 0; row < values.Len(); row++ {
		if values.IsNull(row) {
			continue
		}
		start, end, _, _ := listValues(values, row)
		for element := start; element < end; element++ {
			if structs.IsNull(int(element)) {
				return nil, merr.WrapErrFunctionFailedMsg("Milvus StructArray cannot encode null struct elements")
			}
		}
	}
	if len(schemas) > 0 && len(schemas) != structs.NumField() {
		return nil, merr.WrapErrFunctionFailedMsg("struct output child count differs from destination schema")
	}
	children := make([]*schemapb.FieldData, structs.NumField())
	for column := range children {
		childField := structs.DataType().(*arrow.StructType).Field(column)
		childSchema, err := inferredSchema(childField)
		if len(schemas) > 0 {
			childSchema = nil
			for _, candidate := range schemas {
				if candidate.GetName() == childField.Name || strings.TrimSuffix(strings.TrimPrefix(candidate.GetName(), schema.GetName()+"["), "]") == childField.Name {
					childSchema = candidate
					break
				}
			}
			if childSchema == nil {
				return nil, merr.WrapErrFunctionFailedMsg("unknown struct output child %q", childField.Name)
			}
			err = nil
		}
		if err != nil {
			return nil, err
		}
		destinationID := childSchema.GetFieldID()
		leafSchema := proto.Clone(childSchema).(*schemapb.FieldSchema)
		if childSchema.GetDataType() == schemapb.DataType_Array || childSchema.GetDataType() == schemapb.DataType_ArrayOfVector {
			leafSchema.DataType = childSchema.GetElementType()
			leafSchema.ElementType = schemapb.DataType_None
			leafSchema.Nullable = childSchema.GetElementNullable()
		}
		if typeutil.IsVectorType(leafSchema.GetDataType()) {
			vectorRows := make([]*schemapb.VectorField, values.Len())
			valid := make([]bool, values.Len())
			for row := range vectorRows {
				start, end, _, _ := listValues(values, row)
				valid[row] = values.IsValid(row)
				sliced := array.NewSlice(structs.Field(column), start, end)
				if sliced.NullN() > 0 && !leafSchema.GetNullable() {
					sliced.Release()
					return nil, merr.WrapErrFunctionFailedMsg("nonnullable struct vector child %q contains null", childField.Name)
				}
				vector, err := arrowToVector(sliced, leafSchema)
				if err != nil {
					sliced.Release()
					return nil, err
				}
				if sliced.NullN() > 0 {
					vector.ValidData = make([]bool, sliced.Len())
					for i := range vector.ValidData {
						vector.ValidData[i] = sliced.IsValid(i)
					}
				}
				sliced.Release()
				vectorRows[row] = vector
			}
			dim, err := typeutil.GetDim(leafSchema)
			if err != nil {
				return nil, merr.Wrap(err, "struct vector child dimension")
			}
			children[column] = &schemapb.FieldData{Type: schemapb.DataType_ArrayOfVector, FieldName: childField.Name, FieldId: destinationID, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, ValidData: valid, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: dim, ElementType: leafSchema.GetDataType(), Data: vectorRows}}}}}
			continue
		}
		childSchema = leafSchema
		rows := make([]*schemapb.ScalarField, values.Len())
		valid := make([]bool, values.Len())
		for row := range rows {
			start, end, _, _ := listValues(values, row)
			valid[row] = values.IsValid(row)
			sliced := array.NewSlice(structs.Field(column), start, end)
			if sliced.NullN() > 0 && !childSchema.GetNullable() {
				sliced.Release()
				return nil, merr.WrapErrFunctionFailedMsg("nonnullable struct output child %q contains null", childField.Name)
			}
			inner, err := arrowToScalar(sliced, childSchema.GetDataType(), childSchema.GetElementType())
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
		children[column] = &schemapb.FieldData{Type: schemapb.DataType_Array, FieldName: childField.Name, FieldId: destinationID, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows, ElementType: childSchema.GetDataType()}}, ValidData: valid}}}
	}
	return &schemapb.FieldData{Type: schemapb.DataType_ArrayOfStruct, FieldName: schema.GetName(), FieldId: schema.GetFieldID(), Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}}}, nil
}
