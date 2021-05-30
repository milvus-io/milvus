package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegmentCache(t *testing.T) {
	nsc := newSegmentCache()

	assert.True(t, nsc.checkIfFlushingOrFlushed(0))

	nsc.setIsFlushing(UniqueID(0))
	assert.False(t, nsc.checkIfFlushingOrFlushed(0))
}
