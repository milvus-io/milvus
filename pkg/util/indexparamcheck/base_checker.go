package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

type baseChecker struct {
}

func (c *baseChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return errOutOfRange(DIM, DefaultMinDim, DefaultMaxDim)
	}

	return nil
}

// CheckValidDataType check whether the field data type is supported for the index type
func (c *baseChecker) CheckValidDataType(dType schemapb.DataType) error {
	return nil
}

func newBaseChecker() IndexChecker {
	return &baseChecker{}
}
