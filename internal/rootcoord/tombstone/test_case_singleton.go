package tombstone

func InitCollectionTombstoneForTest(tombstoneInterface CollectionTombstoneInterface) {
	collectionTombstone.Store(&tombstoneInterface)
}
