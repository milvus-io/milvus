package parameterutil

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// GetMaxLength get max length of field. Maybe also helpful outside.
func GetMaxLength(field *schemapb.FieldSchema) (int64, error) {
	if !typeutil.IsStringType(field.GetDataType()) && !typeutil.IsStringType(field.GetElementType()) {
		msg := fmt.Sprintf("%s is not of string type", field.GetDataType())
		return 0, merr.WrapErrParameterInvalid(schemapb.DataType_VarChar, field.GetDataType(), msg)
	}
	h := typeutil.NewKvPairs(append(field.GetIndexParams(), field.GetTypeParams()...))
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
	return int64(maxLength), nil
}

// GetMaxCapacity get max capacity of array field. Maybe also helpful outside.
func GetMaxCapacity(field *schemapb.FieldSchema) (int64, error) {
	if !typeutil.IsArrayType(field.GetDataType()) {
		msg := fmt.Sprintf("%s is not of array type", field.GetDataType())
		return 0, merr.WrapErrParameterInvalid(schemapb.DataType_Array, field.GetDataType(), msg)
	}
	h := typeutil.NewKvPairs(append(field.GetIndexParams(), field.GetTypeParams()...))
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
