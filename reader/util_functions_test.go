package reader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUtilFunctions_GetKey2Segments(t *testing.T) {
	// TODO: Add GetKey2Segments test
}

func TestUtilFunctions_GetCollectionByCollectionName(t *testing.T) {
	// 1. Construct node, and collections
	node := NewQueryNode(0, 0)
	var _ = node.NewCollection("collection0", "fake schema")

	// 2. Get collection by collectionName
	var c0, err = node.GetCollectionByCollectionName("collection0")
	assert.NoError(t, err)
	assert.Equal(t, c0.CollectionName, "collection0")
}

func TestUtilFunctions_GetSegmentBySegmentID(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection("collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var _ = partition.NewSegment(0)

	// 2. Get segment by segment id
	var s0, err = node.GetSegmentBySegmentID(0)
	assert.NoError(t, err)
	assert.Equal(t, s0.SegmentId, 0)
}
