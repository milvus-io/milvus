package indexparamcheck

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// BTREEChecker checks if a BTREE index can be built.
type BTREEChecker struct {
	scalarIndexChecker
}

func (c *BTREEChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *BTREEChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsStringType(field.GetDataType()) {
		return errors.New("BTREE indexes are only supported on varchar field")
	}
	return nil
}

func newBTREEChecker() *BTREEChecker {
	return &BTREEChecker{}
}
