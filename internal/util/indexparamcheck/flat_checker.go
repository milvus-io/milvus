package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type flatChecker struct {
	floatVectorBaseChecker
}

func (c flatChecker) StaticCheck(dataType schemapb.DataType, m map[string]string) error {
	return c.staticCheck(m)
}

func newFlatChecker() IndexChecker {
	return &flatChecker{}
}
