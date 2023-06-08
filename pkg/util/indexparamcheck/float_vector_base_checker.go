package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/common"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type floatVectorBaseChecker struct {
	baseChecker
}

func (c floatVectorBaseChecker) staticCheck(params map[string]string) error {
	if !CheckStrByValues(params, Metric, METRICS) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", METRICS)
	}

	return nil
}

func (c floatVectorBaseChecker) CheckTrain(params map[string]string) error {
	if err := c.baseChecker.CheckTrain(params); err != nil {
		return err
	}

	return c.staticCheck(params)
}

func (c floatVectorBaseChecker) CheckValidDataType(dType schemapb.DataType) error {
	if dType != schemapb.DataType_FloatVector {
		return fmt.Errorf("float vector is only supported")
	}
	return nil
}

func (c floatVectorBaseChecker) SetDefaultMetricTypeIfNotExist(params map[string]string) {
	setDefaultIfNotExist(params, common.MetricTypeKey, FloatVectorDefaultMetricType)
}

func newFloatVectorBaseChecker() IndexChecker {
	return &floatVectorBaseChecker{}
}
