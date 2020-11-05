package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartition_NewSegment(t *testing.T) {
	ctx := context.Background()
	pulsarUrl := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarUrl)

	var collection = node.newCollection(0, "collection0", "")
	var partition = collection.newPartition("partition0")

	var segment = partition.newSegment(0)
	node.SegmentsMap[int64(0)] = segment

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, collection.CollectionID, int64(0))
	assert.Equal(t, partition.PartitionName, "partition0")
	assert.Equal(t, node.Collections[0].Partitions[0].Segments[0].SegmentID, int64(0))

	assert.Equal(t, len(collection.Partitions), 1)
	assert.Equal(t, len(node.Collections), 1)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 1)

	assert.Equal(t, segment.SegmentID, int64(0))
	assert.Equal(t, node.foundSegmentBySegmentID(int64(0)), true)
}

func TestPartition_DeleteSegment(t *testing.T) {
	// 1. Construct node, collection, partition and segment
	ctx := context.Background()
	pulsarUrl := "pulsar://localhost:6650"
	node := NewQueryNode(ctx, 0, pulsarUrl)

	var collection = node.newCollection(0, "collection0", "")
	var partition = collection.newPartition("partition0")

	var segment = partition.newSegment(0)
	node.SegmentsMap[int64(0)] = segment

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, collection.CollectionID, int64(0))
	assert.Equal(t, partition.PartitionName, "partition0")
	assert.Equal(t, node.Collections[0].Partitions[0].Segments[0].SegmentID, int64(0))

	assert.Equal(t, len(collection.Partitions), 1)
	assert.Equal(t, len(node.Collections), 1)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 1)

	assert.Equal(t, segment.SegmentID, int64(0))

	// 2. Destruct collection, partition and segment
	partition.deleteSegment(node, segment)

	assert.Equal(t, len(collection.Partitions), 1)
	assert.Equal(t, len(node.Collections), 1)
	assert.Equal(t, len(node.Collections[0].Partitions[0].Segments), 0)
	assert.Equal(t, node.foundSegmentBySegmentID(int64(0)), false)
}
