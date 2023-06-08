package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

// TODO: check index parameters according to the index type & data type.
func CheckIndexValid(dType schemapb.DataType, indexType IndexType, indexParams map[string]string) error {
	return nil
}
