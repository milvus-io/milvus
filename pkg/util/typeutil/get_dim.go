package typeutil

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

// GetDim get dimension of field. Maybe also helpful outside.
func GetDim(field *schemapb.FieldSchema) (int64, error) {
	if !IsVectorType(field.GetDataType()) {
		return 0, fmt.Errorf("%s is not of vector type", field.GetDataType())
	}
	h := NewKvPairs(append(field.GetIndexParams(), field.GetTypeParams()...))
	dimStr, err := h.Get(common.DimKey)
	if err != nil {
		return 0, fmt.Errorf("dim not found")
	}
	dim, err := strconv.Atoi(dimStr)
	if err != nil {
		return 0, fmt.Errorf("invalid dimension: %s", dimStr)
	}
	return int64(dim), nil
}
