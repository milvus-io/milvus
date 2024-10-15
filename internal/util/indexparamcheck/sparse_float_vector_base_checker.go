package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// sparse vector don't check for dim, but baseChecker does, thus not including baseChecker
type sparseFloatVectorBaseChecker struct{}

func (c sparseFloatVectorBaseChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	if !CheckStrByValues(params, Metric, SparseMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", SparseMetrics)
	}

	return nil
}

func (c sparseFloatVectorBaseChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	dropRatioBuildStr, exist := params[SparseDropRatioBuild]
	if exist {
		dropRatioBuild, err := strconv.ParseFloat(dropRatioBuildStr, 64)
		if err != nil || dropRatioBuild < 0 || dropRatioBuild >= 1 {
			return fmt.Errorf("invalid drop_ratio_build: %s, must be in range [0, 1)", dropRatioBuildStr)
		}
	}

	return nil
}

func (c sparseFloatVectorBaseChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		return fmt.Errorf("only sparse float vector is supported for the specified index tpye")
	}
	return nil
}

func (c sparseFloatVectorBaseChecker) SetDefaultMetricTypeIfNotExist(dType schemapb.DataType, params map[string]string) {
	setDefaultIfNotExist(params, common.MetricTypeKey, SparseFloatVectorDefaultMetricType)
}

func newSparseFloatVectorBaseChecker() IndexChecker {
	return &sparseFloatVectorBaseChecker{}
}
