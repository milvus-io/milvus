package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// STLSORTChecker checks if a STL_SORT index can be built.
type BITMAPChecker struct {
	scalarIndexChecker
}

func (c *BITMAPChecker) CheckTrain(params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *BITMAPChecker) CheckValidDataType(dType schemapb.DataType) error {
	if !typeutil.IsArithmetic(dType) && !typeutil.IsStringType(dType) {
		return fmt.Errorf("bitmap index are only supported on numeric and string field")
	}
	return nil
}

func newBITMAPChecker() *BITMAPChecker {
	return &BITMAPChecker{}
}
