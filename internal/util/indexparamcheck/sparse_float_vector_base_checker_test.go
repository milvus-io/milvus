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
		err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, validParams)
		assert.NoError(t, err)
	})

	t.Run("invalid metric", func(t *testing.T) {
		err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, invalidParams)
		assert.Error(t, err)
	})

	t.Run("valid inverted_index_algo", func(t *testing.T) {
		for _, algo := range SparseInvertedIndexAlgos {
			params := map[string]string{
				common.IndexTypeKey:   "SPARSE_INVERTED_INDEX",
				Metric:                "IP",
				SparseInvertedIndexAlgo: algo,
			}
			err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
			assert.NoError(t, err, "algo %s should be valid", algo)
		}
	})

	t.Run("invalid inverted_index_algo", func(t *testing.T) {
		for _, algo := range []string{"INVALID_ALGO", "", "taat_naive", "DAAT_WAND_EXTRA"} {
			params := map[string]string{
				common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
				Metric:                  "IP",
				SparseInvertedIndexAlgo: algo,
			}
			err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
			assert.Error(t, err, "algo %q should be rejected", algo)
			assert.Contains(t, err.Error(), "not found or not supported", "algo %q should produce a descriptive error", algo)
		}
	})

	t.Run("invalid inverted_index_algo for SPARSE_WAND", func(t *testing.T) {
		cWand, _ := GetIndexCheckerMgrInstance().GetChecker("SPARSE_WAND")
		params := map[string]string{
			common.IndexTypeKey:     "SPARSE_WAND",
			Metric:                  "IP",
			SparseInvertedIndexAlgo: "INVALID_ALGO",
		}
		err := cWand.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sparse inverted index algo INVALID_ALGO not found or not supported")
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
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, validParams)
		assert.NoError(t, err)
	})

	t.Run("invalid drop ratio", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, invalidDropRatio)
		assert.Error(t, err)
	})

	t.Run("invalid BM25K1", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, invalidBM25K1)
		assert.Error(t, err)
	})

	t.Run("invalid BM25B", func(t *testing.T) {
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, invalidBM25B)
		assert.Error(t, err)
	})

	t.Run("invalid inverted_index_algo propagates through CheckTrain", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
			Metric:                  "IP",
			SparseInvertedIndexAlgo: "INVALID_ALGO",
		}
		err := c.CheckTrain(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sparse inverted index algo INVALID_ALGO not found or not supported")
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
