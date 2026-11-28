package replicates

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

type replicateAckerImpl func(err error)

func (r replicateAckerImpl) Ack(err error) {
	r(err)
}

// ReplicateAcker is a guard for replicate message.
type ReplicateAcker interface {
	// Ack acknowledges the replicate message operation is done.
	// A successful append applies the configuration change or advances replication progress.
	Ack(err error)
}

// ReplicatesManager manages the replicate operation on one wal.
// There are two states:
// 1. primary: wal will only receive the non-replicate message.
// 2. secondary: wal will only receive the replicate message.
type ReplicatesManager interface {
	// Role returns the role of the replicate manager.
	Role() replicateutil.Role

	// BeginReplicateMessage begins the replicate one-replicated-message operation.
	// ReplicateAcker's Ack method should be called if returned without error.
	BeginReplicateMessage(ctx context.Context, msg message.MutableMessage) (ReplicateAcker, error)

	// GetReplicateCheckpoint gets current replicate checkpoint.
	// return ReplicateViolationError if the replicate mode is not replicating.
	GetReplicateCheckpoint() (*utility.ReplicateCheckpoint, error)

	// GetSalvageCheckpoint returns all salvage checkpoints captured during force promote.
	// Returns an empty slice if no force promote has occurred.
	GetSalvageCheckpoint() []*utility.ReplicateCheckpoint
}
