package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

type ivfBaseChecker struct {
	floatVectorBaseChecker
}

func (c ivfBaseChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return errOutOfRange(NLIST, MinNList, MaxNList)
	}

	// skip check number of rows

	return c.floatVectorBaseChecker.staticCheck(params)
}

func (c ivfBaseChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.StaticCheck(dataType, params); err != nil {
		return err
	}
	return c.floatVectorBaseChecker.CheckTrain(dataType, params)
}

func newIVFBaseChecker() IndexChecker {
	return &ivfBaseChecker{}
}
