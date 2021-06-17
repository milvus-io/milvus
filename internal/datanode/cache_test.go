package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegmentCache(t *testing.T) {
	segCache := newCache()

	assert.False(t, segCache.checkIfCached(0))

	segCache.Cache(UniqueID(0))
	assert.True(t, segCache.checkIfCached(0))
}
