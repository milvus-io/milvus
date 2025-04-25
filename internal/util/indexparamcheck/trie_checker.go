package indexparamcheck

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// TRIEChecker checks if a TRIE index can be built.
type TRIEChecker struct {
	scalarIndexChecker
}

func (c *TRIEChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	return c.scalarIndexChecker.CheckTrain(dataType, params)
}

func (c *TRIEChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsStringType(field.GetDataType()) {
		return errors.New("TRIE are only supported on varchar field")
	}
	return nil
}

func newTRIEChecker() *TRIEChecker {
	return &TRIEChecker{}
}
