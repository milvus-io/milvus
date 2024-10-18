package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type binFlatChecker struct {
	binaryVectorBaseChecker
}

func (c binFlatChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	return c.binaryVectorBaseChecker.CheckTrain(dataType, params)
}

func (c binFlatChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	return c.staticCheck(params)
}

func newBinFlatChecker() IndexChecker {
	return &binFlatChecker{}
}
