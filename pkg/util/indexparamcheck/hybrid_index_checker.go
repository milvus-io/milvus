package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type HYBRIDChecker struct {
	scalarIndexChecker
}

func (c *HYBRIDChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, common.BitmapCardinalityLimitKey, 1, MaxBitmapCardinalityLimit) {
		return fmt.Errorf("failed to check bitmap cardinality limit, should be larger than 0 and smaller than %d",
			MaxBitmapCardinalityLimit)
	}
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *HYBRIDChecker) CheckValidDataType(field *schemapb.FieldSchema) error {
	mainType := field.GetDataType()
	elemType := field.GetElementType()
	if !typeutil.IsBoolType(mainType) && !typeutil.IsIntegerType(mainType) &&
		!typeutil.IsStringType(mainType) && !typeutil.IsArrayType(mainType) {
		return fmt.Errorf("hybrid index are only supported on bool, int, string and array field")
	}
	if typeutil.IsArrayType(mainType) {
		if !typeutil.IsBoolType(elemType) && !typeutil.IsIntegerType(elemType) &&
			!typeutil.IsStringType(elemType) {
			return fmt.Errorf("hybrid index are only supported on bool, int, string for array field")
		}
	}
	return nil
}

func newHYBRIDChecker() *HYBRIDChecker {
	return &HYBRIDChecker{}
}

func IsHYBRIDChecker(checker interface{}) bool {
	_, ok := checker.(*HYBRIDChecker)
	return ok
}
