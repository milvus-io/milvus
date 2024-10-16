package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_sparseFloatVectorBaseChecker_StaticCheck(t *testing.T) {
	validParams := map[string]string{
		Metric: "IP",
	}

	invalidParams := map[string]string{
		Metric: "L2",
	}

	c := newSparseFloatVectorBaseChecker()

	t.Run("valid metric", func(t *testing.T) {
		err := c.StaticCheck(validParams)
		assert.NoError(t, err)
	})

	t.Run("invalid metric", func(t *testing.T) {
		err := c.StaticCheck(invalidParams)
		assert.Error(t, err)
	})
}

func Test_sparseFloatVectorBaseChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		SparseDropRatioBuild: "0.5",
		BM25K1:               "1.5",
		BM25B:                "0.5",
	}

	invalidDropRatio := map[string]string{
		SparseDropRatioBuild: "1.5",
	}

	invalidBM25K1 := map[string]string{
		BM25K1: "3.5",
	}

	invalidBM25B := map[string]string{
		BM25B: "1.5",
	}

	c := newSparseFloatVectorBaseChecker()

	t.Run("valid params", func(t *testing.T) {
		err := c.CheckTrain(validParams)
		assert.NoError(t, err)
	})

	t.Run("invalid drop ratio", func(t *testing.T) {
		err := c.CheckTrain(invalidDropRatio)
		assert.Error(t, err)
	})

	t.Run("invalid BM25K1", func(t *testing.T) {
		err := c.CheckTrain(invalidBM25K1)
		assert.Error(t, err)
	})

	t.Run("invalid BM25B", func(t *testing.T) {
		err := c.CheckTrain(invalidBM25B)
		assert.Error(t, err)
	})
}

func Test_sparseFloatVectorBaseChecker_CheckValidDataType(t *testing.T) {
	c := newSparseFloatVectorBaseChecker()

	t.Run("valid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{DataType: schemapb.DataType_SparseFloatVector}
		err := c.CheckValidDataType(field)
		assert.NoError(t, err)
	})

	t.Run("invalid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
		err := c.CheckValidDataType(field)
		assert.Error(t, err)
	})
}
