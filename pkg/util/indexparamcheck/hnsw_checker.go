package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type hnswChecker struct {
	floatVectorBaseChecker
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

func (c hnswChecker) CheckValidDataType(dType schemapb.DataType) error {
	if !typeutil.IsVectorType(dType) {
		return fmt.Errorf("can't build hnsw in not vector type.")
	}
	return nil
}

func newHnswChecker() IndexChecker {
	return &hnswChecker{}
}
