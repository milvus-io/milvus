package meta

import "errors"

var (
	// Read errors
	ErrCollectionNotFound = errors.New("CollectionNotFound")
	ErrPartitionNotFound  = errors.New("PartitionNotFound")
	ErrReplicaNotFound    = errors.New("ReplicaNotFound")

	// Store errors
	ErrStoreCollectionFailed = errors.New("StoreCollectionFailed")
	ErrStoreReplicaFailed    = errors.New("StoreReplicaFailed")
)
