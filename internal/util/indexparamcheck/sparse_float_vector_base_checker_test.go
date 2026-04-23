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
				common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
				Metric:                  "IP",
				SparseInvertedIndexAlgo: algo,
			}
			err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
			assert.NoError(t, err, "algo %s should be valid", algo)
		}
	})

	t.Run("invalid inverted_index_algo", func(t *testing.T) {
		for _, algo := range []string{
			"INVALID_ALGO",       // completely unknown
			"",                   // empty string
			"taat_naive",         // lowercase of valid value
			"DAAT_WAND_EXTRA",    // valid prefix with extra suffix
			"BLOCK_MAX",          // partial name of new algo
			"sindi",              // lowercase of new algo
			"block_max_maxscore", // lowercase of new algo
			"BLOCK_MAX_MAXSCOR",  // new algo with typo
			"BLOCK_MAX_WAND_",    // new algo with trailing underscore
			"Sindi",              // new algo with mixed case
		} {
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

	t.Run("new algos BLOCK_MAX_MAXSCORE BLOCK_MAX_WAND SINDI are valid", func(t *testing.T) {
		for _, algo := range []string{"BLOCK_MAX_MAXSCORE", "BLOCK_MAX_WAND", "SINDI"} {
			params := map[string]string{
				common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
				Metric:                  "IP",
				SparseInvertedIndexAlgo: algo,
			}
			err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
			assert.NoError(t, err, "algo %s should be accepted", algo)
		}
	})

	t.Run("metric error takes priority over algo error", func(t *testing.T) {
		// If both metric and algo are invalid, the metric check fires first.
		params := map[string]string{
			common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
			Metric:                  "L2",
			SparseInvertedIndexAlgo: "INVALID_ALGO",
		}
		err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "metric type")
		assert.NotContains(t, err.Error(), "inverted_index_algo")
	})

	t.Run("error message contains full supported list", func(t *testing.T) {
		params := map[string]string{
			common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
			Metric:                  "IP",
			SparseInvertedIndexAlgo: "UNKNOWN",
		}
		err := c.StaticCheck(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
		assert.Error(t, err)
		// Verify the error message lists every supported algorithm.
		for _, supported := range SparseInvertedIndexAlgos {
			assert.Contains(t, err.Error(), supported)
		}
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

	t.Run("new algos BLOCK_MAX_MAXSCORE BLOCK_MAX_WAND SINDI valid through CheckTrain", func(t *testing.T) {
		for _, algo := range []string{"BLOCK_MAX_MAXSCORE", "BLOCK_MAX_WAND", "SINDI"} {
			params := map[string]string{
				common.IndexTypeKey:     "SPARSE_INVERTED_INDEX",
				Metric:                  "IP",
				SparseInvertedIndexAlgo: algo,
			}
			err := c.CheckTrain(schemapb.DataType_SparseFloatVector, schemapb.DataType_None, params)
			assert.NoError(t, err, "algo %s should be accepted through CheckTrain", algo)
		}
	})
}

// Test_SparseInvertedIndexAlgos_Contract pins the exact set of supported algorithms.
// If an algorithm is added or removed from knowhere the constant must be updated here too.
func Test_SparseInvertedIndexAlgos_Contract(t *testing.T) {
	expected := []string{
		"TAAT_NAIVE",
		"DAAT_WAND",
		"DAAT_MAXSCORE",
		"BLOCK_MAX_MAXSCORE",
		"BLOCK_MAX_WAND",
		"SINDI",
	}
	assert.Equal(t, len(expected), len(SparseInvertedIndexAlgos),
		"SparseInvertedIndexAlgos has wrong number of entries; update both the constant and this test")
	for _, algo := range expected {
		assert.Contains(t, SparseInvertedIndexAlgos, algo,
			"expected algo %q to be present in SparseInvertedIndexAlgos", algo)
	}
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
