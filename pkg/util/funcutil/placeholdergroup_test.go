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
