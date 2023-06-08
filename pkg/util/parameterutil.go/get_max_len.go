package parameterutil

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// GetMaxLength get max length of field. Maybe also helpful outside.
func GetMaxLength(field *schemapb.FieldSchema) (int64, error) {
	if !typeutil.IsStringType(field.GetDataType()) {
		msg := fmt.Sprintf("%s is not of string type", field.GetDataType())
		return 0, errors.New(msg)
	}
	h := typeutil.NewKvPairs(append(field.GetIndexParams(), field.GetTypeParams()...))
	maxLengthStr, err := h.Get("max_length")
	if err != nil {
		msg := "max length not found"
		return 0, errors.New(msg)
	}
	maxLength, err := strconv.Atoi(maxLengthStr)
	if err != nil {
		msg := fmt.Sprintf("invalid max length: %s", maxLengthStr)
		return 0, errors.New(msg)
	}
	return int64(maxLength), nil
}
