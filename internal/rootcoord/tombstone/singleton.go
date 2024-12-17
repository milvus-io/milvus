package tombstone

import "go.uber.org/atomic"

// collectionTombstone is the singleton instance of CollectionTtombstone
var (
	collectionTombstone atomic.Pointer[CollectionTombstoneInterface]
)

// InitCollectionTombstone initializes the collection tombstone
func InitCollectionTombstone(tombstoneInterface CollectionTombstoneInterface) {
	if tombstoneInterface == nil {
		panic("InitCollectionTombstone with nil")
	}
	if !collectionTombstone.CompareAndSwap(nil, &tombstoneInterface) {
		panic("CollectionTombstone already initialized")
	}
}

// CollectionTombstone returns the singleton instance of CollectionTombstone
func CollectionTombstone() CollectionTombstoneInterface {
	return *collectionTombstone.Load()
}
