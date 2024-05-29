package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// INVERTEDChecker checks if a INVERTED index can be built.
type INVERTEDChecker struct {
	scalarIndexChecker
}

func (c *INVERTEDChecker) CheckTrain(params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(params)
}

func (c *INVERTEDChecker) CheckValidDataType(dType schemapb.DataType) error {
	if !typeutil.IsBoolType(dType) && !typeutil.IsArithmetic(dType) && !typeutil.IsStringType(dType) &&
		!typeutil.IsArrayType(dType) {
		return fmt.Errorf("INVERTED are not supported on %s field", dType.String())
	}
	return nil
}

func newINVERTEDChecker() *INVERTEDChecker {
	return &INVERTEDChecker{}
}
