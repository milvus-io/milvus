package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

type binaryVectorBaseChecker struct {
	baseChecker
}

func (c *binaryVectorBaseChecker) CheckTrain(params map[string]string) error {
	if err := c.baseChecker.CheckTrain(params); err != nil {
		return err
	}

	if !CheckStrByValues(params, Metric, BinIDMapMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", BinIDMapMetrics)
	}

	return nil
}

func (c *binaryVectorBaseChecker) CheckValidDataType(dType schemapb.DataType) error {
	if dType != schemapb.DataType_BinaryVector {
		return fmt.Errorf("binary vector is only supported")
	}
	return nil
}

func newBinaryVectorBaseChecker() IndexChecker {
	return &binaryVectorBaseChecker{}
}
