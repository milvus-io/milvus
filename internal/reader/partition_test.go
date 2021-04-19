package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartition_NewSegment(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0, 0)

	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")

	var segment = partition.NewSegment(0)
	node.SegmentsMap[int64(0)] = segment

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, collection.CollectionID, uint64(0))
	assert.Equal(t, partition.PartitionName, "partition0")
	assert.Equal(t, node.Collections[0].Partitions[0].Segments[0].SegmentId, int64(0))

	assert.Equal(t, len(collection.Partitions), 1)
	assert.Equal(t, len(node.Collections), 1)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 1)

	assert.Equal(t, segment.SegmentId, int64(0))
	assert.Equal(t, node.FoundSegmentBySegmentID(int64(0)), true)
}

func TestPartition_DeleteSegment(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	ctx := context.Background()
	node := NewQueryNode(ctx, 0, 0)

	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")

	var segment = partition.NewSegment(0)
	node.SegmentsMap[int64(0)] = segment

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, collection.CollectionID, uint64(0))
	assert.Equal(t, partition.PartitionName, "partition0")
	assert.Equal(t, node.Collections[0].Partitions[0].Segments[0].SegmentId, int64(0))

	assert.Equal(t, len(collection.Partitions), 1)
	assert.Equal(t, len(node.Collections), 1)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 1)

	assert.Equal(t, segment.SegmentId, int64(0))

	// 2. Destruct collection, partition and segment
	partition.DeleteSegment(node, segment)

	assert.Equal(t, len(collection.Partitions), 1)
	assert.Equal(t, len(node.Collections), 1)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 0)
	assert.Equal(t, node.FoundSegmentBySegmentID(int64(0)), false)
}
