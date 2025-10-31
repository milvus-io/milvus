package indexparamcheck

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// STLSORTChecker checks if a STL_SORT index can be built.
type STLSORTChecker struct {
	scalarIndexChecker
}

func (c *STLSORTChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *STLSORTChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	dataType := field.GetDataType()
	if !typeutil.IsArithmetic(dataType) && !typeutil.IsStringType(dataType) && !typeutil.IsTimestamptzType(dataType) {
		return errors.New(fmt.Sprintf("STL_SORT are only supported on numeric, varchar or timestamptz field, got %s", field.GetDataType()))
	}
	return nil
}

func newSTLSORTChecker() *STLSORTChecker {
	return &STLSORTChecker{}
}
