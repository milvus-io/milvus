package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_flattenedByteVectorsToByteVectors(t *testing.T) {
	flattenedVectors := []byte{0, 1, 2, 3, 4, 5}
	dimension := 3

	actual := flattenedByteVectorsToByteVectors(flattenedVectors, dimension)
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
