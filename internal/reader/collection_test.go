package reader

//func TestCollection_NewPartition(t *testing.T) {
//	ctx := context.Background()
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, collection.CollectionID, int64(0))
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, len(collection.Partitions), 1)
//}
//
//func TestCollection_DeletePartition(t *testing.T) {
//	ctx := context.Background()
//	pulsarUrl := "pulsar://localhost:6650"
//	node := NewQueryNode(ctx, 0, pulsarUrl)
//
//	var collection = node.newCollection(0, "collection0", "")
//	var partition = collection.newPartition("partition0")
//
//	assert.Equal(t, collection.CollectionName, "collection0")
//	assert.Equal(t, collection.CollectionID, int64(0))
//	assert.Equal(t, partition.partitionTag, "partition0")
//	assert.Equal(t, len(collection.Partitions), 1)
//
//	collection.deletePartition(node, partition)
//
//	assert.Equal(t, len(collection.Partitions), 0)
//}
