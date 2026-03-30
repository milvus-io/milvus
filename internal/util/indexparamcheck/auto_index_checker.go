package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// AUTOINDEXChecker checks if a TRIE index can be built.
type AUTOINDEXChecker struct {
	baseChecker
}

func (c *AUTOINDEXChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	switch {
	case typeutil.IsDenseFloatVectorType(dataType):
		if !CheckStrByValues(params, Metric, FloatVectorMetrics) {
			return fmt.Errorf("float vector index does not support metric type: %s", params[Metric])
		}
	case typeutil.IsSparseFloatVectorType(dataType):
		if !CheckStrByValues(params, Metric, SparseMetrics) {
			return fmt.Errorf("only IP&BM25 is the supported metric type for sparse index")
		}
	case typeutil.IsBinaryVectorType(dataType):
		if !CheckStrByValues(params, Metric, BinIvfMetrics) {
			return fmt.Errorf("binary vector index does not support metric type: %s", params[Metric])
		}
	case typeutil.IsIntVectorType(dataType):
		if !CheckStrByValues(params, Metric, IntVectorMetrics) {
			return fmt.Errorf("int vector index does not support metric type: %s", params[Metric])
		}
	case typeutil.IsArrayOfVectorType(dataType):
		if !CheckStrByValues(params, Metric, EmbListMetrics) {
			return fmt.Errorf("embedding list index does not support metric type: %s", params[Metric])
		}
	}
	return nil
}

func (c *AUTOINDEXChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
		return fmt.Errorf("index %s only supports vector data type", indexType)
	}
	return nil
}

func (c *AUTOINDEXChecker) SetDefaultMetricTypeIfNotExist(dType schemapb.DataType, params map[string]string) {
	paramtable.SetDefaultMetricTypeIfNotExist(dType, params)
}

func newAUTOINDEXChecker() *AUTOINDEXChecker {
	return &AUTOINDEXChecker{}
}
