package shards

// GetLastL0FlushTimeTick returns the last L0 flush time tick of the partition.
func (m *shardManagerImpl) GetLastL0FlushTimeTick() map[PartitionUniqueKey]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	lastL0FlushTimeTick := make(map[PartitionUniqueKey]uint64, 0)
	for collectionID, collection := range m.collections {
		for partitionID, partition := range collection.Partitions {
			lastL0FlushTimeTick[PartitionUniqueKey{CollectionID: collectionID, PartitionID: partitionID}] = partition.LastL0FlushTimeTick
		}
	}
	return lastL0FlushTimeTick
}
