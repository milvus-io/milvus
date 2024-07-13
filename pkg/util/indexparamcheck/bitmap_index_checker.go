package indexparamcheck

import (
	"fmt"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type BITMAPChecker struct {
	scalarIndexChecker
}

func (c *BITMAPChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, common.BitmapCardinalityLimitKey, 1, math.MaxInt) {
		return fmt.Errorf("failed to check bitmap cardinality limit, should be larger than 0 and smaller than math.MaxInt")
	}
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *BITMAPChecker) CheckValidDataType(field *schemapb.FieldSchema) error {
	mainType := field.GetDataType()
	elemType := field.GetElementType()
	if !typeutil.IsBoolType(mainType) && !typeutil.IsIntegerType(mainType) &&
		!typeutil.IsStringType(mainType) && !typeutil.IsArrayType(mainType) {
		return fmt.Errorf("bitmap index are only supported on bool, int, string and array field")
	}
	if typeutil.IsArrayType(mainType) {
		if !typeutil.IsBoolType(elemType) && !typeutil.IsIntegerType(elemType) &&
			!typeutil.IsStringType(elemType) {
			return fmt.Errorf("bitmap index are only supported on bool, int, string for array field")
		}
	}
	return nil
}

func newBITMAPChecker() *BITMAPChecker {
	return &BITMAPChecker{}
}
