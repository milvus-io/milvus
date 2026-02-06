package broadcaster

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var (
	ErrNotPrimary   = errors.New("cluster is not primary, cannot do any DDL/DCL")
	ErrNotSecondary = errors.New("cluster is not secondary, cannot perform force promote")
)

type Broadcaster interface {
	// WithResourceKeys sets the resource keys of the broadcast operation.
	// It will acquire locks of the resource keys and return the broadcast api.
	// Once the broadcast api is returned, the Close() method of the broadcast api should be called to release the resource safely.
	// Return ErrNotPrimary if the cluster is not primary, so no DDL message can be broadcasted.
	WithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (BroadcastAPI, error)

	// WithSecondaryClusterResourceKey acquires an exclusive cluster-level resource key
	// and verifies the cluster is secondary. Returns error if the cluster is primary.
	// This is used for force promote operations that should only be executed on secondary clusters.
	WithSecondaryClusterResourceKey(ctx context.Context) (BroadcastAPI, error)

	// LegacyAck is the legacy ack interface for the 2.6.0 import message.
	LegacyAck(ctx context.Context, broadcastID uint64, vchannel string) error

	// Ack acknowledges the message at the specified vchannel.
	Ack(ctx context.Context, msg message.ImmutableMessage) error

	// FixIncompleteBroadcastsForForcePromote fixes incomplete broadcasts for force promote.
	// It marks incomplete AlterReplicateConfig messages with ignore=true before supplementing
	// them to remaining vchannels. This ensures old incomplete messages don't overwrite
	// the force promote configuration.
	FixIncompleteBroadcastsForForcePromote(ctx context.Context) error

	// Close closes the broadcaster.
	Close()
}

type BroadcastAPI interface {
	// Broadcast broadcasts the message to all channels.
	Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error)

	// Close releases the resource keys that broadcast api holds.
	Close()
}

// AppendOperator is used to append messages, there's only two implement of this interface:
// 1. streaming.WAL()
// 2. old msgstream interface [deprecated]
type AppendOperator interface {
	AppendMessages(ctx context.Context, msgs ...message.MutableMessage) types.AppendResponses
}
