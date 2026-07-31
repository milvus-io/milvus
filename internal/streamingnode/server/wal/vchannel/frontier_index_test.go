package vchannel

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegmentFrontierIndexUpdatesMinimumIncrementally(t *testing.T) {
	index := newSegmentFrontierIndex()

	index.Update(1, 100, 10, 10)
	index.Update(2, 100, 20, 20)
	assert.Equal(t, uint64(10), index.All())
	assert.Equal(t, uint64(10), index.Partition(100, 10))
	assert.Equal(t, uint64(20), index.Partition(100, 20))

	index.Update(1, 100, 10, 30)
	assert.Equal(t, uint64(20), index.All())

	index.Update(2, 100, 20, math.MaxUint64)
	assert.Equal(t, uint64(30), index.All())
	assert.Equal(t, uint64(math.MaxUint64), index.Partition(100, 20))
}

func TestMinimumFrontierIndexUpdatesAndRemovesEntries(t *testing.T) {
	index := newMinimumFrontierIndex[string]()

	index.Update("v1", 10)
	index.Update("v2", 20)
	index.Update("v1", 30)
	assert.Equal(t, uint64(20), index.Minimum())

	index.Remove("v2")
	assert.Equal(t, uint64(30), index.Minimum())
}

func TestSegmentFrontierIndexMovesAndRemovesSegments(t *testing.T) {
	index := newSegmentFrontierIndex()
	index.Update(1, 100, 10, 10)
	index.Update(2, 100, 10, 20)
	index.Update(3, 100, 20, 30)
	assert.Len(t, index.all.items, 2)

	index.Update(1, 100, 20, 40)
	assert.Equal(t, uint64(20), index.All())
	assert.Equal(t, uint64(20), index.Partition(100, 10))
	assert.Equal(t, uint64(30), index.Partition(100, 20))

	index.Remove(2)
	assert.Equal(t, uint64(math.MaxUint64), index.Partition(100, 10))
	assert.Equal(t, uint64(30), index.All())

	index.Remove(3)
	assert.Equal(t, uint64(40), index.All())
	index.Remove(1)
	assert.Equal(t, uint64(math.MaxUint64), index.All())
}
