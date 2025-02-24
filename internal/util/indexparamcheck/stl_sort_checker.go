package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// STLSORTChecker checks if a STL_SORT index can be built.
type STLSORTChecker struct {
	scalarIndexChecker
}

func (c *STLSORTChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(dataType, params)
}

func (c *STLSORTChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsArithmetic(field.GetDataType()) {
		return fmt.Errorf("STL_SORT are only supported on numeric field")
	}
	return nil
}

func newSTLSORTChecker() *STLSORTChecker {
	return &STLSORTChecker{}
}
