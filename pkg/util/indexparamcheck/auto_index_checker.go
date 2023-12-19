package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// AutoIndexChecker checks if a AUTOINDEX index can be built.
type AutoIndexChecker struct {
	baseChecker
}

func (c *AutoIndexChecker) CheckTrain(params map[string]string) error {
	return nil
}

func (c *AutoIndexChecker) CheckValidDataType(dType schemapb.DataType) error {
	return nil
}

func newAutoIndexChecker() *AutoIndexChecker {
	return &AutoIndexChecker{}
}
