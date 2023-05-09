package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

type floatVectorBaseChecker struct {
	baseChecker
}

func (c *floatVectorBaseChecker) CheckTrain(params map[string]string) error {
	if err := c.baseChecker.CheckTrain(params); err != nil {
		return err
	}

	if !CheckStrByValues(params, Metric, METRICS) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", METRICS)
	}

	return nil
}

func (c *floatVectorBaseChecker) CheckValidDataType(dType schemapb.DataType) error {
	if dType != schemapb.DataType_FloatVector {
		return fmt.Errorf("float vector is only supported")
	}
	return nil
}

func newFloatVectorBaseChecker() IndexChecker {
	return &floatVectorBaseChecker{}
}
