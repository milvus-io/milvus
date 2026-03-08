package replicates

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

type replicateAckerImpl func(err error)

func (r replicateAckerImpl) Ack(err error) {
	r(err)
}

// ReplicateAcker is a guard for replicate message.
type ReplicateAcker interface {
	// Ack acknowledges the replicate message operation is done.
	// It will push forward the in-memory checkpoint if the err is nil.
	Ack(err error)
}

// ReplicateManager manages the replicate operation on one wal.
// There are two states:
// 1. primary: wal will only receive the non-replicate message.
// 2. secondary: wal will only receive the replicate message.
type ReplicateManager interface {
	// Role returns the role of the replicate manager.
	Role() replicateutil.Role

	// SwitchReplicateMode switches the replicate mode.
	// following cases will happens:
	// 1. primary->secondary: will transit into replicating mode, the message without replicate header will be rejected.
	// 2. primary->primary: nothing happens,
	// 3. secondary->primary: will transit into non-replicating mode, the secondary replica state (remote cluster replicating checkpoint...) will be dropped.
	// 4. secondary->secondary with the source cluster is changed: the previous remote cluster replicating checkpoint will be dropped.
	// 5. secondary->secondary without the source cluster is changed: nothing happens.
	SwitchReplicateMode(ctx context.Context, msg message.MutableAlterReplicateConfigMessageV2) error

	// BeginReplicateMessage begins the replicate one-replicated-message operation.
	// ReplicateAcker's Ack method should be called if returned without error.
	BeginReplicateMessage(ctx context.Context, msg message.MutableMessage) (ReplicateAcker, error)

	// GetReplicateCheckpoint gets current replicate checkpoint.
	// return ReplicateViolationError if the replicate mode is not replicating.
	GetReplicateCheckpoint() (*utility.ReplicateCheckpoint, error)
}
