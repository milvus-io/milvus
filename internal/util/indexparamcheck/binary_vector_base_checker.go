package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

type binaryVectorBaseChecker struct {
	baseChecker
}

func (c binaryVectorBaseChecker) staticCheck(params map[string]string) error {
	if !CheckStrByValues(params, Metric, BinIDMapMetrics) {
		return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], BinIDMapMetrics)
	}

	return nil
}

func (c binaryVectorBaseChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.baseChecker.CheckTrain(dataType, params); err != nil {
		return err
	}

	return c.staticCheck(params)
}

func (c binaryVectorBaseChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if field.GetDataType() != schemapb.DataType_BinaryVector {
		return fmt.Errorf("binary vector is only supported")
	}
	return nil
}

func (c binaryVectorBaseChecker) SetDefaultMetricTypeIfNotExist(dType schemapb.DataType, params map[string]string) {
	setDefaultIfNotExist(params, common.MetricTypeKey, BinaryVectorDefaultMetricType)
}

func newBinaryVectorBaseChecker() IndexChecker {
	return &binaryVectorBaseChecker{}
}
