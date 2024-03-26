package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// STLSORTChecker checks if a STL_SORT index can be built.
type STLSORTChecker struct {
	scalarIndexChecker
}

func (c *STLSORTChecker) CheckTrain(params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *STLSORTChecker) CheckValidDataType(dType schemapb.DataType) error {
	if !typeutil.IsArithmetic(dType) {
		return fmt.Errorf("STL_SORT are only supported on numeric field")
	}
	return nil
}

func newSTLSORTChecker() *STLSORTChecker {
	return &STLSORTChecker{}
}
