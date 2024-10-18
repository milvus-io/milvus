package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// AUTOINDEXChecker checks if a TRIE index can be built.
type AUTOINDEXChecker struct {
	baseChecker
}

func (c *AUTOINDEXChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	return nil
}

func (c *AUTOINDEXChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	return nil
}

func newAUTOINDEXChecker() *AUTOINDEXChecker {
	return &AUTOINDEXChecker{}
}
