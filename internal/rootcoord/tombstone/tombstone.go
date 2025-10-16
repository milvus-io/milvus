package tombstone

import "context"

type TombstoneSweeper interface {
	AddTombstone(tombstone Tombstone)
	Close()
}

// Tombstone is the interface for the tombstone.
type Tombstone interface {
	// ID returns the unique identifier of the tombstone.
	ID() string

	// ConfirmCanBeRemoved checks if the tombstone can be removed forever.
	ConfirmCanBeRemoved(ctx context.Context) (bool, error)

	// Remove removes the tombstone.
	Remove(ctx context.Context) error
}
