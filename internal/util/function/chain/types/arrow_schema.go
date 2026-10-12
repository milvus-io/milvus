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

package types

import (
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	MetadataKeyMilvusDataType    = "milvus.data_type"
	MetadataKeyMilvusFieldSchema = "milvus.field_schema"
	maxRuntimeTypeDepth          = 64
)

// SchemaAwareFunctionExpr preserves logical types that share the same physical
// Arrow representation. Expressions which only need arrays retain FunctionExpr.
type SchemaAwareFunctionExpr interface {
	ExecuteWithSchema(ctx *FuncContext, schema *arrow.Schema, inputs []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error)
}

// ValidateRuntimeArrowDataType checks the finite transport type vocabulary.
// It does not infer logical JSON, geometry, or vector types from byte widths.
func ValidateRuntimeArrowDataType(dt arrow.DataType) error {
	return validateRuntimeArrowType(dt, 0)
}

func validateRuntimeArrowType(dt arrow.DataType, depth int) error {
	if dt == nil || depth > maxRuntimeTypeDepth {
		return merr.WrapErrFunctionFailedMsg("invalid or excessively nested runtime Arrow type")
	}
	switch dt.ID() {
	case arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING,
		arrow.BINARY, arrow.LARGE_BINARY, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION:
		return nil
	case arrow.FIXED_SIZE_BINARY:
		if dt.(*arrow.FixedSizeBinaryType).ByteWidth <= 0 {
			return merr.WrapErrFunctionFailedMsg("runtime fixed binary width must be positive")
		}
		return nil
	case arrow.DECIMAL128:
		d := dt.(*arrow.Decimal128Type)
		if d.Precision < 1 || d.Precision > 38 || d.Scale < 0 || d.Scale > d.Precision {
			return merr.WrapErrFunctionFailedMsg("invalid runtime decimal precision/scale")
		}
		return nil
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		if d, ok := dt.(*arrow.FixedSizeListType); ok && d.Len() <= 0 {
			return merr.WrapErrFunctionFailedMsg("runtime fixed list width must be positive")
		}
		return validateRuntimeArrowType(dt.(arrow.ListLikeType).Elem(), depth+1)
	case arrow.STRUCT:
		return validateRuntimeArrowFields(dt.(*arrow.StructType).Fields(), depth+1)
	default:
		return merr.WrapErrFunctionFailedMsg("unsupported runtime Arrow type %s", dt)
	}
}

func validateRuntimeArrowFields(fields []arrow.Field, depth int) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if strings.TrimSpace(field.Name) == "" {
			return merr.WrapErrFunctionFailedMsg("runtime Arrow field name must be nonblank")
		}
		if _, ok := seen[field.Name]; ok {
			return merr.WrapErrFunctionFailedMsg("duplicate runtime Arrow field %q", field.Name)
		}
		seen[field.Name] = struct{}{}
		if err := validateRuntimeArrowType(field.Type, depth); err != nil {
			return merr.Wrapf(err, "runtime field %q", field.Name)
		}
		if err := validateLogicalArrowMetadata(field); err != nil {
			return merr.Wrapf(err, "runtime field %q", field.Name)
		}
	}
	return nil
}

func ValidateRuntimeArrowSchema(schema *arrow.Schema) error {
	if schema == nil {
		return merr.WrapErrFunctionFailedMsg("runtime Arrow schema is nil")
	}
	return validateRuntimeArrowFields(schema.Fields(), 0)
}

// ArrowFieldFromMilvusSchema encodes the destination schema in Arrow metadata.
// ArrayOfStruct/Struct children come from the collection's struct schema (or an
// already materialized runtime structure), rather than a guessed scalar type.
func ArrowFieldFromMilvusSchema(schema *schemapb.FieldSchema, children ...arrow.Field) (arrow.Field, error) {
	if schema == nil || schema.GetDataType() == schemapb.DataType_None {
		return arrow.Field{}, merr.WrapErrParameterInvalidMsg("Milvus field schema requires a concrete type")
	}
	var dt arrow.DataType
	var err error
	if recursive := schema.GetTypeSchema(); recursive != nil {
		dt, err = arrowTypeFromRecursiveMilvus(recursive, 0)
	} else {
		dt, err = arrowTypeFromMilvus(schema.GetDataType(), schema.GetElementType(), schema.GetTypeParams(), children, 0)
	}
	if err != nil {
		return arrow.Field{}, err
	}
	if schema.GetTypeSchema() == nil && (schema.GetDataType() == schemapb.DataType_Array || schema.GetDataType() == schemapb.DataType_ArrayOfVector) {
		list := dt.(arrow.ListLikeType)
		element := list.ElemField()
		element.Nullable = schema.GetElementNullable()
		dt = arrow.ListOfField(element)
	}
	encoded, err := protojson.Marshal(schema)
	if err != nil {
		return arrow.Field{}, merr.WrapErrServiceInternalErr(err, "serialize Milvus runtime field schema")
	}
	metadata := map[string]string{
		MetadataKeyMilvusDataType:    strconv.Itoa(int(schema.GetDataType())),
		MetadataKeyMilvusFieldSchema: string(encoded),
		"milvus.field_id":            strconv.FormatInt(schema.GetFieldID(), 10),
		"element_type":               strconv.Itoa(int(schema.GetElementType())),
	}
	for _, param := range schema.GetTypeParams() {
		metadata[param.GetKey()] = param.GetValue()
	}
	field := arrow.Field{Name: schema.GetName(), Type: dt, Nullable: schema.GetNullable(), Metadata: arrow.MetadataFrom(metadata)}
	if err := ValidateArrowFieldMilvusType(field, schema.GetDataType()); err != nil {
		return arrow.Field{}, err
	}
	return field, nil
}

func arrowTypeFromRecursiveMilvus(schema *schemapb.TypeSchema, depth int) (arrow.DataType, error) {
	if schema == nil || depth > maxRuntimeTypeDepth {
		return nil, merr.WrapErrParameterInvalidMsg("invalid recursive Milvus runtime type")
	}
	if element := schema.GetArrayElement(); element != nil {
		dt, err := arrowTypeFromRecursiveMilvus(element, depth+1)
		if err != nil {
			return nil, err
		}
		childType := element.GetLeafType()
		childElement := schemapb.DataType_None
		if nested := element.GetArrayElement(); nested != nil {
			childType = schemapb.DataType_Array
			childElement = nested.GetLeafType()
			if nested.GetArrayElement() != nil {
				childElement = schemapb.DataType_Array
			}
		}
		childSchema := &schemapb.FieldSchema{Name: "item", DataType: childType, ElementType: childElement, Nullable: element.GetNullable(), TypeSchema: element, TypeParams: element.GetTypeParams()}
		encoded, err := protojson.Marshal(childSchema)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "serialize nested runtime type schema")
		}
		metadata := map[string]string{MetadataKeyMilvusDataType: strconv.Itoa(int(childType)), MetadataKeyMilvusFieldSchema: string(encoded)}
		for _, param := range element.GetTypeParams() {
			metadata[param.GetKey()] = param.GetValue()
		}
		return arrow.ListOfField(arrow.Field{Name: "item", Type: dt, Nullable: element.GetNullable(), Metadata: arrow.MetadataFrom(metadata)}), nil
	}
	return arrowTypeFromMilvus(schema.GetLeafType(), schemapb.DataType_None, schema.GetTypeParams(), nil, depth+1)
}

func arrowTypeFromMilvus(dt, elem schemapb.DataType, params []*commonpb.KeyValuePair, children []arrow.Field, depth int) (arrow.DataType, error) {
	if depth > maxRuntimeTypeDepth {
		return nil, merr.WrapErrParameterInvalidMsg("excessively nested Milvus runtime type")
	}
	switch dt {
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
	case schemapb.DataType_JSON, schemapb.DataType_Geometry, schemapb.DataType_Mol, schemapb.DataType_SparseFloatVector:
		return arrow.BinaryTypes.Binary, nil
	case schemapb.DataType_UUID:
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}, nil
	case schemapb.DataType_Decimal:
		return &arrow.FixedSizeBinaryType{ByteWidth: 8}, nil
	case schemapb.DataType_Array:
		child, err := arrowTypeFromMilvus(elem, schemapb.DataType_None, params, nil, depth+1)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(child), nil
	case schemapb.DataType_Struct, schemapb.DataType_ArrayOfStruct:
		if len(children) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("runtime struct schema requires child fields")
		}
		if err := validateRuntimeArrowFields(children, depth+1); err != nil {
			return nil, err
		}
		structure := arrow.StructOf(children...)
		if dt == schemapb.DataType_Struct {
			return structure, nil
		}
		return arrow.ListOf(structure), nil
	case schemapb.DataType_ArrayOfVector:
		if !isRuntimeVectorLogicalType(elem) {
			return nil, merr.WrapErrParameterInvalidMsg("vector array element type must be a vector")
		}
		child, err := arrowTypeFromMilvus(elem, schemapb.DataType_None, params, nil, depth+1)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(child), nil
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		dim, err := positiveRuntimeParam(params, "dim")
		if err != nil {
			return nil, err
		}
		if dt == schemapb.DataType_FloatVector {
			return arrow.FixedSizeListOfField(int32(dim), arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Float32, Nullable: false}), nil
		}
		width := dim
		if dt == schemapb.DataType_BinaryVector {
			if dim%8 != 0 {
				return nil, merr.WrapErrParameterInvalidMsg("binary vector dimension must be divisible by eight")
			}
			width /= 8
		}
		if dt == schemapb.DataType_Float16Vector || dt == schemapb.DataType_BFloat16Vector {
			width *= 2
		}
		return &arrow.FixedSizeBinaryType{ByteWidth: width}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported Milvus runtime type %s", dt)
	}
}

func positiveRuntimeParam(params []*commonpb.KeyValuePair, key string) (int, error) {
	for _, p := range params {
		if p.GetKey() == key {
			n, err := strconv.ParseInt(p.GetValue(), 10, 32)
			if err != nil || n <= 0 || n > 1<<30 {
				return 0, merr.WrapErrParameterInvalidMsg("invalid runtime %s parameter", key)
			}
			return int(n), nil
		}
	}
	return 0, merr.WrapErrParameterMissingMsg("runtime field requires %s", key)
}

// MilvusSchemaFromArrowField prefers explicit logical metadata. Unannotated
// binary values have no unique Milvus type and must be bound by an endpoint.
func MilvusSchemaFromArrowField(field arrow.Field) (*schemapb.FieldSchema, error) {
	if err := ValidateRuntimeArrowDataType(field.Type); err != nil {
		return nil, err
	}
	schema := &schemapb.FieldSchema{Name: field.Name, Nullable: field.Nullable}
	if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusFieldSchema); ok {
		if err := protojson.Unmarshal([]byte(raw), schema); err != nil {
			return nil, merr.WrapErrFunctionFailed(err, "invalid runtime Milvus field schema metadata")
		}
		schema.Name = field.Name
		schema.Nullable = field.Nullable
	} else if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusDataType); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, merr.WrapErrFunctionFailedMsg("invalid runtime Milvus data type metadata")
		}
		schema.DataType = schemapb.DataType(parsed)
		if raw, ok := field.Metadata.GetValue("element_type"); ok {
			parsed, err := strconv.ParseInt(raw, 10, 32)
			if err != nil {
				return nil, merr.WrapErrFunctionFailedMsg("invalid runtime element type metadata")
			}
			schema.ElementType = schemapb.DataType(parsed)
		}
		for _, key := range []string{"dim", "max_length", "max_capacity", "precision", "scale"} {
			if v, ok := field.Metadata.GetValue(key); ok && v != "0" {
				schema.TypeParams = append(schema.TypeParams, &commonpb.KeyValuePair{Key: key, Value: v})
			}
		}
	} else {
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
		case arrow.STRUCT:
			schema.DataType = schemapb.DataType_Struct
		case arrow.LIST, arrow.LARGE_LIST:
			list := field.Type.(arrow.ListLikeType)
			if list.Elem().ID() == arrow.STRUCT {
				schema.DataType = schemapb.DataType_ArrayOfStruct
				break
			}
			child, err := MilvusSchemaFromArrowField(list.ElemField())
			if err != nil {
				return nil, err
			}
			schema.DataType = schemapb.DataType_Array
			if isRuntimeVectorLogicalType(child.GetDataType()) {
				schema.DataType = schemapb.DataType_ArrayOfVector
				schema.TypeParams = child.GetTypeParams()
			}
			schema.ElementType = child.GetDataType()
			schema.ElementNullable = list.ElemField().Nullable
			if child.GetDataType() == schemapb.DataType_Array || child.GetTypeSchema() != nil {
				schema.TypeSchema, err = recursiveSchemaFromArrowField(field, 0)
				if err != nil {
					return nil, err
				}
			}

		case arrow.FIXED_SIZE_LIST:
			list := field.Type.(*arrow.FixedSizeListType)
			if list.Elem().ID() != arrow.FLOAT32 {
				return nil, merr.WrapErrFunctionFailedMsg("unannotated fixed list has no unique Milvus type")
			}
			schema.DataType = schemapb.DataType_FloatVector
			schema.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(int(list.Len()))}}
		default:
			return nil, merr.WrapErrFunctionFailedMsg("Arrow type %s requires logical Milvus metadata or destination binding", field.Type)
		}
	}
	if schema.GetDataType() == schemapb.DataType_Array {
		list, ok := field.Type.(arrow.ListLikeType)
		if !ok {
			return nil, merr.WrapErrFunctionFailedMsg("runtime array schema does not describe a list")
		}
		if schema.GetElementType() == schemapb.DataType_None {
			child, err := MilvusSchemaFromArrowField(list.ElemField())
			if err != nil {
				return nil, err
			}
			schema.ElementType = child.GetDataType()
			schema.ElementNullable = list.ElemField().Nullable
		}
		if schema.GetElementType() == schemapb.DataType_Array && schema.GetTypeSchema() == nil {
			recursive, err := recursiveSchemaFromArrowField(field, 0)
			if err != nil {
				return nil, err
			}
			schema.TypeSchema = recursive
		}
	}
	if _, ok := schemapb.DataType_name[int32(schema.GetDataType())]; !ok || schema.GetDataType() == schemapb.DataType_None {
		return nil, merr.WrapErrFunctionFailedMsg("invalid runtime Milvus logical type")
	}
	if err := ValidateArrowFieldMilvusType(field, schema.GetDataType()); err != nil {
		return nil, err
	}
	return schema, nil
}

func ValidateArrowFieldMilvusType(field arrow.Field, expected schemapb.DataType) error {
	if expected == schemapb.DataType_None {
		return merr.WrapErrFunctionFailedMsg("runtime logical type cannot be None")
	}
	if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusDataType); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || schemapb.DataType(parsed) != expected {
			return merr.WrapErrFunctionFailedMsg("Arrow field %q logical type differs from %s", field.Name, expected)
		}
	}
	if err := ValidateRuntimeArrowDataType(field.Type); err != nil {
		return err
	}
	ok := false
	switch expected {
	case schemapb.DataType_Bool:
		ok = field.Type.ID() == arrow.BOOL
	case schemapb.DataType_Int8:
		ok = field.Type.ID() == arrow.INT8
	case schemapb.DataType_Int16:
		ok = field.Type.ID() == arrow.INT16
	case schemapb.DataType_Int32, schemapb.DataType_Date:
		ok = field.Type.ID() == arrow.INT32
	case schemapb.DataType_Int64, schemapb.DataType_Time, schemapb.DataType_Timestamptz:
		ok = field.Type.ID() == arrow.INT64
	case schemapb.DataType_Float:
		ok = field.Type.ID() == arrow.FLOAT32
	case schemapb.DataType_Double:
		ok = field.Type.ID() == arrow.FLOAT64
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		ok = field.Type.ID() == arrow.STRING || field.Type.ID() == arrow.LARGE_STRING
	case schemapb.DataType_JSON, schemapb.DataType_Geometry, schemapb.DataType_Mol, schemapb.DataType_SparseFloatVector:
		ok = field.Type.ID() == arrow.BINARY || field.Type.ID() == arrow.LARGE_BINARY
	case schemapb.DataType_Decimal:
		if dt, yes := field.Type.(*arrow.FixedSizeBinaryType); yes {
			ok = dt.ByteWidth == 8
		}
	case schemapb.DataType_UUID:
		if dt, yes := field.Type.(*arrow.FixedSizeBinaryType); yes {
			ok = dt.ByteWidth == 16
		}
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		ok = field.Type.ID() == arrow.FIXED_SIZE_BINARY || field.Type.ID() == arrow.BINARY || field.Type.ID() == arrow.LARGE_BINARY
	case schemapb.DataType_FloatVector:
		if dt, yes := field.Type.(arrow.ListLikeType); yes {
			ok = dt.Elem().ID() == arrow.FLOAT32 || dt.Elem().ID() == arrow.FLOAT64
		}
	case schemapb.DataType_Array, schemapb.DataType_ArrayOfVector:
		_, ok = field.Type.(arrow.ListLikeType)
	case schemapb.DataType_ArrayOfStruct:
		if dt, yes := field.Type.(arrow.ListLikeType); yes {
			ok = dt.Elem().ID() == arrow.STRUCT
		}
	case schemapb.DataType_Struct:
		ok = field.Type.ID() == arrow.STRUCT
	}
	if !ok {
		return merr.WrapErrFunctionFailedMsg("Arrow field %q type %s does not represent %s", field.Name, field.Type, expected)
	}
	if raw, exists := field.Metadata.GetValue("dim"); exists && raw != "0" && (expected == schemapb.DataType_FloatVector || expected == schemapb.DataType_BinaryVector || expected == schemapb.DataType_Float16Vector || expected == schemapb.DataType_BFloat16Vector || expected == schemapb.DataType_Int8Vector) {
		dim, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || dim <= 0 {
			return merr.WrapErrFunctionFailedMsg("invalid runtime vector dimension metadata")
		}
		if list, yes := field.Type.(*arrow.FixedSizeListType); yes && int64(list.Len()) != dim {
			return merr.WrapErrFunctionFailedMsg("runtime vector dimension differs from fixed list width")
		}
		width := dim
		if expected == schemapb.DataType_BinaryVector {
			if dim%8 != 0 {
				return merr.WrapErrFunctionFailedMsg("runtime binary vector dimension is not byte aligned")
			}
			width /= 8
		}
		if expected == schemapb.DataType_Float16Vector || expected == schemapb.DataType_BFloat16Vector {
			width *= 2
		}
		if binary, yes := field.Type.(*arrow.FixedSizeBinaryType); yes && int64(binary.ByteWidth) != width {
			return merr.WrapErrFunctionFailedMsg("runtime vector dimension differs from fixed binary width")
		}
	}
	return nil
}

// ValidateArrowArrayAgainstField checks declared type and nested nullability
// only for reachable values. Null list/struct parents hide their child slots.
func ValidateArrowArrayAgainstField(values arrow.Array, field arrow.Field) error {
	if values == nil || field.Type == nil || !arrow.TypeEqual(values.DataType(), field.Type) {
		return merr.WrapErrFunctionFailedMsg("runtime field %q array type differs from its schema", field.Name)
	}
	if err := ValidateRuntimeArrowDataType(field.Type); err != nil {
		return err
	}
	if err := validateLogicalArrowMetadata(field); err != nil {
		return err
	}
	field, err := normalizeRuntimeValidationField(field)
	if err != nil {
		return err
	}
	if _, nested := field.Type.(arrow.NestedType); !nested {
		if !field.Nullable && values.NullN() > 0 {
			return merr.WrapErrFunctionFailedMsg("nonnullable runtime field %q contains null", field.Name)
		}
		// Fixed widths/dimensions are checked once in the schema. Variable
		// binary vector payloads require a per-row width check below.
		if field.Type.ID() != arrow.BINARY && field.Type.ID() != arrow.LARGE_BINARY {
			return nil
		}
	}
	for row := 0; row < values.Len(); row++ {
		if err := validateRuntimeArrowValue(values, field, row, 0); err != nil {
			return merr.Wrapf(err, "runtime field %q row %d", field.Name, row)
		}
	}
	return nil
}

func validateRuntimeArrowValue(values arrow.Array, field arrow.Field, row, depth int) error {
	if depth > maxRuntimeTypeDepth || row < 0 || row >= values.Len() {
		return merr.WrapErrFunctionFailedMsg("invalid runtime nested value position")
	}
	if values.IsNull(row) {
		if !field.Nullable {
			return merr.WrapErrFunctionFailedMsg("nonnullable runtime field %q contains null", field.Name)
		}
		return nil
	}
	if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusDataType); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return merr.WrapErrFunctionFailedMsg("invalid runtime logical type metadata")
		}
		logical := schemapb.DataType(parsed)
		if logical == schemapb.DataType_BinaryVector || logical == schemapb.DataType_Float16Vector || logical == schemapb.DataType_BFloat16Vector || logical == schemapb.DataType_Int8Vector {
			if field.Type.ID() == arrow.BINARY || field.Type.ID() == arrow.LARGE_BINARY {
				dim, err := runtimeFieldDimension(field)
				if err != nil {
					return err
				}
				width := dim
				if logical == schemapb.DataType_BinaryVector {
					width /= 8
				}
				if logical == schemapb.DataType_Float16Vector || logical == schemapb.DataType_BFloat16Vector {
					width *= 2
				}
				var raw []byte
				switch v := values.(type) {
				case *array.Binary:
					raw = v.Value(row)
				case *array.LargeBinary:
					raw = v.Value(row)
				}
				if int64(len(raw)) != width {
					return merr.WrapErrFunctionFailedMsg("runtime vector row byte width differs from schema")
				}
			}
		}
		if logical == schemapb.DataType_FloatVector {
			var start, end int64
			var children arrow.Array
			switch list := values.(type) {
			case *array.List:
				start, end = list.ValueOffsets(row)
				children = list.ListValues()
			case *array.LargeList:
				start, end = list.ValueOffsets(row)
				children = list.ListValues()
			case *array.FixedSizeList:
				start, end = list.ValueOffsets(row)
				children = list.ListValues()
			}
			if children == nil {
				return merr.WrapErrFunctionFailedMsg("float vector output is not a list")
			}
			dim, err := runtimeFieldDimension(field)
			if err != nil || end-start != dim {
				return merr.WrapErrFunctionFailedMsg("float vector row differs from declared dimension")
			}
			child := field.Type.(arrow.ListLikeType).ElemField()
			child.Nullable = false
			return validateRuntimeArrowRange(children, child, start, end, depth+1)
		}
	}
	switch v := values.(type) {
	case *array.Struct:
		for i, child := range field.Type.(*arrow.StructType).Fields() {
			if err := validateRuntimeArrowValue(v.Field(i), child, row, depth+1); err != nil {
				return err
			}
		}
	case *array.List:
		start, end := v.ValueOffsets(row)
		return validateRuntimeArrowRange(v.ListValues(), field.Type.(arrow.ListLikeType).ElemField(), start, end, depth+1)
	case *array.LargeList:
		start, end := v.ValueOffsets(row)
		return validateRuntimeArrowRange(v.ListValues(), field.Type.(arrow.ListLikeType).ElemField(), start, end, depth+1)
	case *array.FixedSizeList:
		start, end := v.ValueOffsets(row)
		return validateRuntimeArrowRange(v.ListValues(), field.Type.(arrow.ListLikeType).ElemField(), start, end, depth+1)
	}
	return nil
}

// Validate logical annotations without guessing from shared physical encodings.
// Empty arrays run this same check, so contradictory dimensions cannot hide in
// a zero-hit group.
func validateLogicalArrowMetadata(field arrow.Field) error {
	var declared *schemapb.FieldSchema
	if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusFieldSchema); ok {
		declared = &schemapb.FieldSchema{}
		if err := protojson.Unmarshal([]byte(raw), declared); err != nil {
			return merr.WrapErrFunctionFailed(err, "invalid runtime Milvus schema metadata")
		}
		if declared.GetDataType() == schemapb.DataType_None {
			return merr.WrapErrFunctionFailedMsg("runtime schema metadata has None logical type")
		}
		if _, known := schemapb.DataType_name[int32(declared.GetDataType())]; !known {
			return merr.WrapErrFunctionFailedMsg("runtime schema metadata has unknown logical type")
		}
		if declared.GetNullable() != field.Nullable {
			return merr.WrapErrFunctionFailedMsg("runtime schema metadata has contradictory nullability")
		}
	}
	if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusDataType); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || parsed == 0 {
			return merr.WrapErrFunctionFailedMsg("invalid runtime logical type metadata")
		}
		logical := schemapb.DataType(parsed)
		if _, known := schemapb.DataType_name[int32(logical)]; !known {
			return merr.WrapErrFunctionFailedMsg("unknown runtime logical type metadata")
		}
		if declared != nil && declared.GetDataType() != logical {
			return merr.WrapErrFunctionFailedMsg("runtime logical type tags conflict")
		}
		if declared == nil {
			declared = &schemapb.FieldSchema{DataType: logical, Nullable: field.Nullable}
		}
	}
	if declared != nil {
		if err := ValidateArrowFieldMilvusType(field, declared.GetDataType()); err != nil {
			return err
		}
		params := make(map[string]string)
		for _, param := range declared.GetTypeParams() {
			if _, duplicate := params[param.GetKey()]; duplicate {
				return merr.WrapErrFunctionFailedMsg("duplicate runtime type parameter")
			}
			params[param.GetKey()] = param.GetValue()
		}
		for _, key := range []string{"dim", "max_length", "max_capacity", "precision", "scale"} {
			if raw, ok := field.Metadata.GetValue(key); ok && raw != "0" {
				if value, exists := params[key]; exists && value != raw {
					return merr.WrapErrFunctionFailedMsg("runtime %s metadata conflicts with field schema", key)
				}
				params[key] = raw
			}
		}
		if raw, ok := field.Metadata.GetValue("element_type"); ok && raw != "0" {
			parsed, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || declared.GetElementType() != schemapb.DataType_None && declared.GetElementType() != schemapb.DataType(parsed) {
				return merr.WrapErrFunctionFailedMsg("runtime array element type metadata conflicts")
			}
			declared.ElementType = schemapb.DataType(parsed)
		}
		vectorType := declared.GetDataType()
		if vectorType == schemapb.DataType_ArrayOfVector {
			vectorType = declared.GetElementType()
		}
		// Sparse vectors encode their coordinates per row, including when they
		// are collected into a list. Only dense vector elements have a fixed dim.
		if isRuntimeVectorLogicalType(vectorType) && vectorType != schemapb.DataType_SparseFloatVector && params["dim"] == "" {
			return merr.WrapErrFunctionFailedMsg("runtime vector schema requires dimension metadata")
		}
		if raw, ok := params["dim"]; ok {
			dim, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || dim < 0 || dim == 0 && vectorType != schemapb.DataType_SparseFloatVector {
				return merr.WrapErrFunctionFailedMsg("invalid runtime vector dimension")
			}
			if binary, ok := field.Type.(*arrow.FixedSizeBinaryType); ok {
				width := dim
				switch declared.GetDataType() {
				case schemapb.DataType_BinaryVector:
					if dim%8 != 0 {
						return merr.WrapErrFunctionFailedMsg("runtime binary vector dimension is not byte aligned")
					}
					width /= 8
				case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
					width *= 2
				}
				if int64(binary.ByteWidth) != width {
					return merr.WrapErrFunctionFailedMsg("runtime vector width differs from logical schema dimension")
				}
			}
			if list, ok := field.Type.(*arrow.FixedSizeListType); ok && declared.GetDataType() == schemapb.DataType_FloatVector && int64(list.Len()) != dim {
				return merr.WrapErrFunctionFailedMsg("runtime vector width differs from logical schema dimension")
			}
		}
		if declared.GetDataType() == schemapb.DataType_Decimal {
			precision, err := strconv.ParseInt(params["precision"], 10, 32)
			scale, scaleErr := strconv.ParseInt(params["scale"], 10, 32)
			if err != nil || scaleErr != nil || precision < 1 || precision > 18 || scale < 0 || scale > precision {
				return merr.WrapErrFunctionFailedMsg("invalid canonical decimal precision/scale metadata")
			}
		}
		if recursive := declared.GetTypeSchema(); recursive != nil {
			if err := validateRecursiveRuntimeMilvusType(field.Type, recursive, 0); err != nil {
				return err
			}
		}
		if declared.GetDataType() == schemapb.DataType_ArrayOfVector && !isRuntimeVectorLogicalType(declared.GetElementType()) {
			return merr.WrapErrFunctionFailedMsg("runtime vector array schema requires a vector element type")
		}
		if list, ok := field.Type.(arrow.ListLikeType); ok && (declared.GetDataType() == schemapb.DataType_Array || declared.GetDataType() == schemapb.DataType_ArrayOfVector) && declared.GetElementType() != schemapb.DataType_None {
			child := list.ElemField()
			if declared.GetDataType() == schemapb.DataType_ArrayOfVector {
				metadata := child.Metadata.ToMap()
				metadata[MetadataKeyMilvusDataType] = strconv.Itoa(int(declared.GetElementType()))
				if dim, exists := params["dim"]; exists {
					metadata["dim"] = dim
				}
				child.Metadata = arrow.MetadataFrom(metadata)
			}
			if err := ValidateArrowFieldMilvusType(child, declared.GetElementType()); err != nil {
				return err
			}
		}
	}
	if nested, ok := field.Type.(arrow.NestedType); ok {
		for _, child := range nested.Fields() {
			if err := validateLogicalArrowMetadata(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRuntimeArrowRange(values arrow.Array, field arrow.Field, start, end int64, depth int) error {
	if start < 0 || end < start || end > int64(values.Len()) {
		return merr.WrapErrFunctionFailedMsg("runtime list has invalid offsets")
	}
	if _, nested := field.Type.(arrow.NestedType); !nested && (field.Nullable || values.NullN() == 0) {
		return nil
	}
	for row := start; row < end; row++ {
		if err := validateRuntimeArrowValue(values, field, int(row), depth); err != nil {
			return err
		}
	}
	return nil
}

func runtimeFieldDimension(field arrow.Field) (int64, error) {
	raw, exists := field.Metadata.GetValue("dim")
	if !exists {
		if encoded, ok := field.Metadata.GetValue(MetadataKeyMilvusFieldSchema); ok {
			schema := &schemapb.FieldSchema{}
			if err := protojson.Unmarshal([]byte(encoded), schema); err != nil {
				return 0, merr.WrapErrFunctionFailed(err, "invalid runtime schema metadata")
			}
			for _, param := range schema.GetTypeParams() {
				if param.GetKey() == "dim" {
					raw = param.GetValue()
					break
				}
			}
		}
	}
	dim, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || dim <= 0 {
		return 0, merr.WrapErrFunctionFailedMsg("invalid runtime vector dimension")
	}
	return dim, nil
}

func validateRecursiveRuntimeMilvusType(actual arrow.DataType, declared *schemapb.TypeSchema, depth int) error {
	if declared == nil || depth > maxRuntimeTypeDepth {
		return merr.WrapErrFunctionFailedMsg("invalid recursive runtime logical type")
	}
	if child := declared.GetArrayElement(); child != nil {
		list, ok := actual.(arrow.ListLikeType)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("recursive runtime schema requires a list")
		}
		return validateRecursiveRuntimeMilvusType(list.Elem(), child, depth+1)
	}
	if _, ok := schemapb.DataType_name[int32(declared.GetLeafType())]; !ok || declared.GetLeafType() == schemapb.DataType_None {
		return merr.WrapErrFunctionFailedMsg("recursive runtime schema has invalid leaf type")
	}
	metadata := map[string]string{MetadataKeyMilvusDataType: strconv.Itoa(int(declared.GetLeafType()))}
	for _, param := range declared.GetTypeParams() {
		metadata[param.GetKey()] = param.GetValue()
	}
	return ValidateArrowFieldMilvusType(arrow.Field{Name: "item", Type: actual, Nullable: declared.GetNullable(), Metadata: arrow.MetadataFrom(metadata)}, declared.GetLeafType())
}

func recursiveSchemaFromArrowField(field arrow.Field, depth int) (*schemapb.TypeSchema, error) {
	if depth > maxRuntimeTypeDepth {
		return nil, merr.WrapErrFunctionFailedMsg("excessively nested inferred runtime schema")
	}
	logical := schemapb.DataType_None
	if raw, ok := field.Metadata.GetValue(MetadataKeyMilvusDataType); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, merr.WrapErrFunctionFailedMsg("invalid inferred runtime logical type")
		}
		logical = schemapb.DataType(parsed)
	}
	if list, ok := field.Type.(arrow.ListLikeType); ok && (logical == schemapb.DataType_None && field.Type.ID() != arrow.FIXED_SIZE_LIST || logical == schemapb.DataType_Array || logical == schemapb.DataType_ArrayOfVector) {
		child, err := recursiveSchemaFromArrowField(list.ElemField(), depth+1)
		if err != nil {
			return nil, err
		}
		return &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: child}, Nullable: field.Nullable}, nil
	}
	schema, err := MilvusSchemaFromArrowField(field)
	if err != nil {
		return nil, err
	}
	return &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schema.GetDataType()}, TypeParams: schema.GetTypeParams(), Nullable: field.Nullable}, nil
}

// Parse complete metadata once per field, then reuse compact logical tags while
// walking reachable values. This avoids parsing protojson per candidate row.
func normalizeRuntimeValidationField(field arrow.Field) (arrow.Field, error) {
	metadata := field.Metadata.ToMap()
	var declared *schemapb.FieldSchema
	if raw, ok := metadata[MetadataKeyMilvusFieldSchema]; ok {
		declared = &schemapb.FieldSchema{}
		if err := protojson.Unmarshal([]byte(raw), declared); err != nil {
			return arrow.Field{}, merr.WrapErrFunctionFailed(err, "invalid runtime schema metadata")
		}
		metadata[MetadataKeyMilvusDataType] = strconv.Itoa(int(declared.GetDataType()))
		for _, param := range declared.GetTypeParams() {
			if _, exists := metadata[param.GetKey()]; !exists {
				metadata[param.GetKey()] = param.GetValue()
			}
		}
	}
	field.Metadata = arrow.MetadataFrom(metadata)
	if structure, ok := field.Type.(*arrow.StructType); ok {
		children := structure.Fields()
		for index := range children {
			child, err := normalizeRuntimeValidationField(children[index])
			if err != nil {
				return arrow.Field{}, err
			}
			children[index] = child
		}
		field.Type = arrow.StructOf(children...)
	}
	if list, ok := field.Type.(arrow.ListLikeType); ok {
		child := list.ElemField()
		if declared != nil && (declared.GetDataType() == schemapb.DataType_Array || declared.GetDataType() == schemapb.DataType_ArrayOfVector) {
			child.Nullable = declared.GetElementNullable()
			if recursive := declared.GetTypeSchema(); recursive.GetArrayElement() != nil {
				child.Nullable = recursive.GetArrayElement().GetNullable()
			}
		}
		if declared != nil && declared.GetDataType() == schemapb.DataType_ArrayOfVector {
			m := child.Metadata.ToMap()
			m[MetadataKeyMilvusDataType] = strconv.Itoa(int(declared.GetElementType()))
			if dim, exists := metadata["dim"]; exists {
				m["dim"] = dim
			}
			child.Metadata = arrow.MetadataFrom(m)
		}
		normalized, err := normalizeRuntimeValidationField(child)
		if err != nil {
			return arrow.Field{}, err
		}
		switch original := field.Type.(type) {
		case *arrow.ListType:
			field.Type = arrow.ListOfField(normalized)
		case *arrow.LargeListType:
			field.Type = arrow.LargeListOfField(normalized)
		case *arrow.FixedSizeListType:
			field.Type = arrow.FixedSizeListOfField(original.Len(), normalized)
		}
	}
	return field, nil
}

func isRuntimeVectorLogicalType(t schemapb.DataType) bool {
	switch t {
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_SparseFloatVector, schemapb.DataType_Int8Vector:
		return true
	default:
		return false
	}
}
