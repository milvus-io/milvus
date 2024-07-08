package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type hnswChecker struct {
	baseChecker
}

func (c hnswChecker) StaticCheck(params map[string]string) error {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return errOutOfRange(EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction)
	}
	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return errOutOfRange(HNSWM, HNSWMinM, HNSWMaxM)
	}
	if !CheckStrByValues(params, Metric, HnswMetrics) {
		return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], HnswMetrics)
	}
	return nil
}

func (c hnswChecker) CheckTrain(params map[string]string) error {
	if err := c.StaticCheck(params); err != nil {
		return err
	}
	return c.baseChecker.CheckTrain(params)
}

func (c hnswChecker) CheckValidDataType(field *schemapb.FieldSchema) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
		return fmt.Errorf("can't build hnsw in not vector type")
	}
	return nil
}

func (c hnswChecker) SetDefaultMetricTypeIfNotExist(params map[string]string, dType schemapb.DataType) {
	if typeutil.IsDenseFloatVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, FloatVectorDefaultMetricType)
	} else if typeutil.IsSparseFloatVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, SparseFloatVectorDefaultMetricType)
	} else if typeutil.IsBinaryVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, BinaryVectorDefaultMetricType)
	}
}

func newHnswChecker() IndexChecker {
	return &hnswChecker{}
}
