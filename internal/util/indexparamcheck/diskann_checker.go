package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

// diskannChecker checks if an diskann index can be built.
type diskannChecker struct {
	floatVectorBaseChecker
}

func (c diskannChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	return c.staticCheck(params)
}

func newDiskannChecker() IndexChecker {
	return &diskannChecker{}
}
