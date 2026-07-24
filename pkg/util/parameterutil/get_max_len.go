package parameterutil

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GetMaxLength get max length of field. Maybe also helpful outside.
func GetMaxLength(field *schemapb.FieldSchema) (int64, error) {
	dataType, typeParams, ok := findStringTypeParams(field)
	if !ok {
		msg := fmt.Sprintf("%s is not of string type", field.GetDataType())
		return 0, merr.WrapErrParameterInvalid(schemapb.DataType_VarChar, field.GetDataType(), msg)
	}
	return getMaxLengthFromParams(dataType, append(field.GetIndexParams(), typeParams...))
}

func findStringTypeParams(field *schemapb.FieldSchema) (schemapb.DataType, []*commonpb.KeyValuePair, bool) {
	if typeutil.IsStringType(field.GetDataType()) {
		return field.GetDataType(), field.GetTypeParams(), true
	}
	if typeutil.IsStringType(field.GetElementType()) {
		return field.GetElementType(), field.GetTypeParams(), true
	}
	if field.GetDataType() == schemapb.DataType_Array && field.GetElementSchema() != nil {
		return findStringTypeParamsInTypeSchema(field.GetElementSchema())
	}
	return schemapb.DataType_None, nil, false
}

func findStringTypeParamsInTypeSchema(typeSchema *schemapb.TypeSchema) (schemapb.DataType, []*commonpb.KeyValuePair, bool) {
	if typeSchema == nil {
		return schemapb.DataType_None, nil, false
	}
	if typeSchema.GetDataType() == schemapb.DataType_Array {
		if typeutil.IsStringType(typeSchema.GetElementType()) {
			return typeSchema.GetElementType(), typeSchema.GetTypeParams(), true
		}
		return findStringTypeParamsInTypeSchema(typeSchema.GetElementSchema())
	}
	return schemapb.DataType_None, nil, false
}

func getMaxLengthFromParams(dataType schemapb.DataType, params []*commonpb.KeyValuePair) (int64, error) {
	h := typeutil.NewKvPairs(params)
	maxLengthStr, err := h.Get(common.MaxLengthKey)
	if err != nil {
		msg := "max length not found"
		return 0, merr.WrapErrParameterInvalid("max length key in type parameters", "not found", msg)
	}
	maxLength, err := strconv.Atoi(maxLengthStr)
	if err != nil {
		msg := fmt.Sprintf("invalid max length: %s", maxLengthStr)
		return 0, merr.WrapErrParameterInvalid("value of max length should be of int", maxLengthStr, msg)
	}
	if !typeutil.IsStringType(dataType) {
		msg := fmt.Sprintf("%s is not of string type", dataType)
		return 0, merr.WrapErrParameterInvalid(schemapb.DataType_VarChar, dataType, msg)
	}
	return int64(maxLength), nil
}

// GetMaxCapacity get max capacity of array field. Maybe also helpful outside.
func GetMaxCapacity(field *schemapb.FieldSchema) (int64, error) {
	if !typeutil.IsArrayType(field.GetDataType()) && !typeutil.IsVectorArrayType(field.GetDataType()) {
		msg := fmt.Sprintf("%s is not of array/vector array type", field.GetDataType())
		return 0, merr.WrapErrParameterInvalid(schemapb.DataType_Array, field.GetDataType(), msg)
	}
	return getMaxCapacityFromParams(append(field.GetIndexParams(), field.GetTypeParams()...))
}

// GetMaxCapacityByLevel gets max capacity at an array nesting level.
// Level 0 is the top-level array field, level 1 is its nested array element.
func GetMaxCapacityByLevel(field *schemapb.FieldSchema, level int) (int64, error) {
	if level < 0 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid array level: %d", level)
	}
	if level == 0 {
		return GetMaxCapacity(field)
	}
	if !isNestedCapacityField(field) {
		msg := fmt.Sprintf("%s is not a nested array type", field.GetDataType())
		return 0, merr.WrapErrParameterInvalid(schemapb.DataType_Array, field.GetDataType(), msg)
	}

	typeSchema := field.GetElementSchema()
	nestedDataType := field.GetDataType()
	for i := 1; i <= level; i++ {
		if typeSchema == nil || typeSchema.GetDataType() != nestedDataType {
			msg := fmt.Sprintf("array level %d not found", level)
			return 0, merr.WrapErrParameterInvalid("array type schema", "not found", msg)
		}
		if i == level {
			return getMaxCapacityFromParams(typeSchema.GetTypeParams())
		}
		typeSchema = typeSchema.GetElementSchema()
	}
	return 0, merr.WrapErrParameterInvalidMsg("array level %d not found", level)
}

func isNestedCapacityField(field *schemapb.FieldSchema) bool {
	elementSchema := field.GetElementSchema()
	if elementSchema == nil {
		return false
	}
	switch field.GetDataType() {
	case schemapb.DataType_Array:
		return elementSchema.GetDataType() == schemapb.DataType_Array
	default:
		return false
	}
}

func getMaxCapacityFromParams(params []*commonpb.KeyValuePair) (int64, error) {
	h := typeutil.NewKvPairs(params)
	maxCapacityStr, err := h.Get(common.MaxCapacityKey)
	if err != nil {
		msg := "max capacity not found"
		return 0, merr.WrapErrParameterInvalid("max capacity key in type parameters", "not found", msg)
	}
	maxCapacity, err := strconv.Atoi(maxCapacityStr)
	if err != nil {
		msg := fmt.Sprintf("invalid max capacity: %s", maxCapacityStr)
		return 0, merr.WrapErrParameterInvalid("value of max length should be of int", maxCapacityStr, msg)
	}
	return int64(maxCapacity), nil
}
