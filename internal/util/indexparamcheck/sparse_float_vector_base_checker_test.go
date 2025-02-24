package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func Test_sparseFloatVectorBaseChecker_StaticCheck(t *testing.T) {
	validParams := map[string]string{
		common.IndexTypeKey: "SPARSE_INVERTED_INDEX",
		Metric:              "IP",
	}

	invalidParams := map[string]string{
		common.IndexTypeKey: "SPARSE_INVERTED_INDEX",
		Metric:              "L2",
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("SPARSE_INVERTED_INDEX")

	t.Run("valid metric", func(t *testing.T) {
		err := c.StaticCheck(schemapb.DataType_SparseFloatVector, validParams)
		assert.NoError(t, err)
	})

	t.Run("invalid metric", func(t *testing.T) {
		err := c.StaticCheck(schemapb.DataType_SparseFloatVector, invalidParams)
		assert.Error(t, err)
	})
}

func Test_sparseFloatVectorBaseChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		common.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
		Metric:               "IP",
		SparseDropRatioBuild: "0.5",
		BM25K1:               "1.5",
		BM25B:                "0.5",
	}

	invalidDropRatio := map[string]string{
		common.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
		Metric:               "IP",
		SparseDropRatioBuild: "1.5",
	}

	invalidBM25K1 := map[string]string{
		common.IndexTypeKey: "SPARSE_INVERTED_INDEX",
		Metric:              "IP",
		BM25K1:              "3.5",
	}

	invalidBM25B := map[string]string{
		common.IndexTypeKey: "SPARSE_INVERTED_INDEX",
		Metric:              "IP",
		BM25B:               "1.5",
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("SPARSE_INVERTED_INDEX")

	t.Run("valid params", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, validParams)
		assert.NoError(t, err)
	})

	t.Run("invalid drop ratio", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, invalidDropRatio)
		assert.Error(t, err)
	})

	t.Run("invalid BM25K1", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, invalidBM25K1)
		assert.Error(t, err)
	})

	t.Run("invalid BM25B", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, invalidBM25B)
		assert.Error(t, err)
	})
}

func Test_sparseFloatVectorBaseChecker_CheckValidDataType(t *testing.T) {
	c, _ := GetIndexCheckerMgrInstance().GetChecker("SPARSE_INVERTED_INDEX")

	t.Run("valid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{DataType: schemapb.DataType_SparseFloatVector}
		err := c.CheckValidDataType("SPARSE_WAND", field)
		assert.NoError(t, err)
	})

	t.Run("invalid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
		err := c.CheckValidDataType("SPARSE_WAND", field)
		assert.Error(t, err)
	})
}
