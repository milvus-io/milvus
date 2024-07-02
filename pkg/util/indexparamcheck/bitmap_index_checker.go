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
	main_type := field.GetDataType()
	elem_type := field.GetElementType()
	if !typeutil.IsBoolType(main_type) && !typeutil.IsIntegerType(main_type) &&
		!typeutil.IsStringType(main_type) && !typeutil.IsArrayType(main_type) {
		return fmt.Errorf("bitmap index are only supported on bool, int, string and array field")
	}
	if typeutil.IsArrayType(main_type) {
		if !typeutil.IsBoolType(elem_type) && !typeutil.IsIntegerType(elem_type) &&
			!typeutil.IsStringType(elem_type) {
			return fmt.Errorf("bitmap index are only supported on bool, int, string for array field")
		}
	}
	return nil
}

func newBITMAPChecker() *BITMAPChecker {
	return &BITMAPChecker{}
}
