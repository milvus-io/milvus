package replicates

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

// ReplicateAckerOutcome describes the transaction state after an append.
type ReplicateAckerOutcome uint8

const (
	ReplicateAckerOutcomeNone ReplicateAckerOutcome = iota
	ReplicateAckerOutcomeAbortPending
	ReplicateAckerOutcomeAborted
)

type replicateAckerImpl func(err error, outcome ReplicateAckerOutcome)

func (r replicateAckerImpl) Ack(err error) {
	r(err, ReplicateAckerOutcomeNone)
}

func (r replicateAckerImpl) AckWithOutcome(err error, outcome ReplicateAckerOutcome) {
	r(err, outcome)
}

// ReplicateAckerWithOutcome is the explicit outcome extension implemented by
// the production acker. It keeps the legacy Ack method available to callers
// that only need the append error.
type ReplicateAckerWithOutcome interface {
	ReplicateAcker
	AckWithOutcome(error, ReplicateAckerOutcome)
}

// AckReplicateMessage reports an append result and its transaction outcome.
// Test doubles that implement only ReplicateAcker retain the legacy behavior.
func AckReplicateMessage(acker ReplicateAcker, err error, outcome ReplicateAckerOutcome) {
	if ackerWithOutcome, ok := acker.(ReplicateAckerWithOutcome); ok {
		ackerWithOutcome.AckWithOutcome(err, outcome)
		return
	}
	acker.Ack(err)
}

// ReplicateAcker is a guard for replicate message.
type ReplicateAcker interface {
	// Ack acknowledges the replicate message operation is done.
	// It will push forward the in-memory checkpoint if the err is nil.
	Ack(err error)
}

// ReplicatesManager manages the replicate operation on one wal.
// There are two states:
// 1. primary: wal will only receive the non-replicate message.
// 2. secondary: wal will only receive the replicate message.
type ReplicatesManager interface {
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

	// GetSalvageCheckpoint returns all salvage checkpoints captured during force promote.
	// Returns an empty slice if no force promote has occurred.
	GetSalvageCheckpoint() []*utility.ReplicateCheckpoint
}
