package tombstone

import (
	"context"
)

type CollectionTombstoneInterface interface {
	// CheckIfVChannelAvailable checks if a vchannel is available.
	CheckIfVChannelAvailable(ctx context.Context, vchannel string) bool

	// CheckIfPartitionAvailable checks if a partition is available
	CheckIfPartitionAvailable(ctx context.Context, collectionID int64, partitionID int64) bool
}
