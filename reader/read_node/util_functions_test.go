package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtilFunctions_GetKey2Segments(t *testing.T) {
	// TODO: Add GetKey2Segments test
}

func TestUtilFunctions_GetCollectionByCollectionName(t *testing.T) {
	// 1. Construct node, and collections
	node := NewQueryNode(0, 0)
	var _ = node.NewCollection(0, "collection0", "fake schema")

	// 2. Get collection by collectionName
	var c0, err = node.GetCollectionByCollectionName("collection0")
	assert.NoError(t, err)
	assert.Equal(t, c0.CollectionName, "collection0")
	c0 = node.GetCollectionByID(0)
	assert.NotNil(t, c0)
	assert.Equal(t, c0.CollectionID, 0)
}

func TestUtilFunctions_GetSegmentBySegmentID(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(0, 0)
	var collection = node.NewCollection(0, "collection0", "fake schema")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// 2. Get segment by segment id
	var s0, err = node.GetSegmentBySegmentID(0)
	assert.NoError(t, err)
	assert.Equal(t, s0.SegmentId, int64(0))
}
