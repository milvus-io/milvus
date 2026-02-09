package bm25

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestBuildSparseFieldData(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "sparse_field",
		DataType: schemapb.DataType_SparseFloatVector,
	}

	sparseArray := &schemapb.SparseFloatArray{
		Dim:      128,
		Contents: [][]byte{{1, 2, 3, 4}},
	}

	result := BuildSparseFieldData(field, sparseArray)

	assert.NotNil(t, result)
	assert.Equal(t, schemapb.DataType_SparseFloatVector, result.Type)
	assert.Equal(t, "sparse_field", result.FieldName)
	assert.Equal(t, int64(100), result.FieldId)

	vectors := result.GetVectors()
	assert.NotNil(t, vectors)
	assert.Equal(t, int64(128), vectors.Dim)

	sparseVector := vectors.GetSparseFloatVector()
	assert.NotNil(t, sparseVector)
	assert.Equal(t, sparseArray, sparseVector)
}
