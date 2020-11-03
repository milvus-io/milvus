package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollection_NewPartition(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0, 0)

	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, collection.CollectionID, uint64(0))
	assert.Equal(t, partition.PartitionName, "partition0")
	assert.Equal(t, len(collection.Partitions), 1)
}

func TestCollection_DeletePartition(t *testing.T) {
	ctx := context.Background()
	node := NewQueryNode(ctx, 0, 0)

	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, collection.CollectionID, uint64(0))
	assert.Equal(t, partition.PartitionName, "partition0")
	assert.Equal(t, len(collection.Partitions), 1)

	collection.DeletePartition(node, partition)

	assert.Equal(t, len(collection.Partitions), 0)
}
