package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type scalarIndexChecker struct {
	baseChecker
}

func (c scalarIndexChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	return nil
}
