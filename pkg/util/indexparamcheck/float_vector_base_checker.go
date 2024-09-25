package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type floatVectorBaseChecker struct {
	baseChecker
}

func (c floatVectorBaseChecker) staticCheck(params map[string]string) error {
	if !CheckStrByValues(params, Metric, FloatVectorMetrics) {
		return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], FloatVectorMetrics)
	}

	return nil
}

func (c floatVectorBaseChecker) CheckTrain(params map[string]string) error {
	if err := c.baseChecker.CheckTrain(params); err != nil {
		return err
	}

	return c.staticCheck(params)
}

func (c floatVectorBaseChecker) CheckValidDataType(field *schemapb.FieldSchema) error {
	if !typeutil.IsDenseFloatVectorType(field.GetDataType()) {
		return fmt.Errorf("data type should be FloatVector, Float16Vector or BFloat16Vector")
	}
	return nil
}

func (c floatVectorBaseChecker) SetDefaultMetricTypeIfNotExist(params map[string]string, dType schemapb.DataType) {
	setDefaultIfNotExist(params, common.MetricTypeKey, FloatVectorDefaultMetricType)
}

func newFloatVectorBaseChecker() IndexChecker {
	return &floatVectorBaseChecker{}
}
