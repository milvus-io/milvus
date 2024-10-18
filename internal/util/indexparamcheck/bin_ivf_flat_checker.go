package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type binIVFFlatChecker struct {
	binaryVectorBaseChecker
}

func (c binIVFFlatChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	if !CheckStrByValues(params, Metric, BinIvfMetrics) {
		return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], BinIvfMetrics)
	}

	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return errOutOfRange(NLIST, MinNList, MaxNList)
	}

	return nil
}

func (c binIVFFlatChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.binaryVectorBaseChecker.CheckTrain(dataType, params); err != nil {
		return err
	}

	return c.StaticCheck(schemapb.DataType_BinaryVector, params)
}

func newBinIVFFlatChecker() IndexChecker {
	return &binIVFFlatChecker{}
}
