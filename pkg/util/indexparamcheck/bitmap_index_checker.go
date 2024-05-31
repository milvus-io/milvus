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

func (c *BITMAPChecker) CheckValidDataType(dType schemapb.DataType) error {
	if !typeutil.IsArithmetic(dType) && !typeutil.IsStringType(dType) && !typeutil.IsArrayType(dType) {
		return fmt.Errorf("bitmap index are only supported on numeric, string and array field")
	}
	return nil
}

func newBITMAPChecker() *BITMAPChecker {
	return &BITMAPChecker{}
}
