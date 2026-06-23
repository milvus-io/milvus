package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func Test_flattenedBinaryVectorsToByteVectors(t *testing.T) {
	flattenedVectors := []byte{0, 1, 2, 3, 4, 5}
	dimension := 24

	actual := flattenedBinaryVectorsToByteVectors(flattenedVectors, dimension)
	expected := [][]byte{
		{0, 1, 2},
		{3, 4, 5},
	}

	assert.Equal(t, expected, actual)
}

func Test_flattenedFloat16VectorsToByteVectors(t *testing.T) {
	flattenedVectors := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	dimension := 2

	actual := flattenedFloat16VectorsToByteVectors(flattenedVectors, dimension)
	expected := [][]byte{
		{0, 1, 2, 3},
		{4, 5, 6, 7},
	}

	assert.Equal(t, expected, actual)
}

func Test_flattenedBFloat16VectorsToByteVectors(t *testing.T) {
	flattenedVectors := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	dimension := 2

	actual := flattenedBFloat16VectorsToByteVectors(flattenedVectors, dimension)
	expected := [][]byte{
		{0, 1, 2, 3},
		{4, 5, 6, 7},
	}

	assert.Equal(t, expected, actual)
}

func Test_flattenedInt8VectorsToByteVectors(t *testing.T) {
	flattenedVectors := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	dimension := 4

	actual := flattenedInt8VectorsToByteVectors(flattenedVectors, dimension)
	expected := [][]byte{
		{0, 1, 2, 3},
		{4, 5, 6, 7},
	}

	assert.Equal(t, expected, actual)
}

func TestFieldDataToPlaceholderGroupBytesWithCount_AllNullSparseVector(t *testing.T) {
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_SparseFloatVector,
		FieldName: "sparse_vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{},
				},
			},
		},
		ValidData: []bool{false, false, false},
	}

	_, valueCount, err := FieldDataToPlaceholderGroupBytesWithCount(fieldData)
	assert.NoError(t, err)
	assert.Equal(t, 0, valueCount)
}
