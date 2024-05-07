package segments

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterZeroValuesFromSlice(t *testing.T) {
	var ints []int64
	ints = append(ints, 10)
	ints = append(ints, 0)
	ints = append(ints, 5)
	ints = append(ints, 13)
	ints = append(ints, 0)

	filteredInts := FilterZeroValuesFromSlice(ints)
	assert.Equal(t, 3, len(filteredInts))
	assert.EqualValues(t, []int64{10, 5, 13}, filteredInts)
}
