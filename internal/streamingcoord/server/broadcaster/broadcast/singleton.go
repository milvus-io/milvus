package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

var (
	singleton       = syncutil.NewFuture[broadcaster.Broadcaster]()
	ErrNotPrimary   = broadcaster.ErrNotPrimary
	ErrNotSecondary = broadcaster.ErrNotSecondary
)

// Register registers the broadcaster.
func Register(broadcaster broadcaster.Broadcaster) {
	singleton.Set(broadcaster)
}

// GetWithContext gets the broadcaster with context.
func GetWithContext(ctx context.Context) (broadcaster.Broadcaster, error) {
	return singleton.GetWithContext(ctx)
}

// StartBroadcastWithResourceKeys starts a broadcast with resource keys.
// Return ErrNotPrimary if the cluster is not primary, so no DDL message can be broadcasted.
func StartBroadcastWithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
	broadcaster, err := singleton.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.WaitUntilWALbasedDDLReady(ctx); err != nil {
		return nil, merr.Wrap(err, "failed to wait until WAL based DDL ready")
	}
	return broadcaster.WithResourceKeys(ctx, resourceKeys...)
}

// StartBroadcastWithSecondaryClusterResourceKey starts a broadcast with exclusive cluster resource key
// and verifies the cluster is secondary. Returns error if the cluster is primary.
// This is used for force promote operations that should only be executed on secondary clusters.
func StartBroadcastWithSecondaryClusterResourceKey(ctx context.Context) (broadcaster.BroadcastAPI, error) {
	broadcaster, err := singleton.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.WaitUntilWALbasedDDLReady(ctx); err != nil {
		return nil, merr.Wrap(err, "failed to wait until WAL based DDL ready")
	}
	return broadcaster.WithSecondaryClusterResourceKey(ctx)
}

// GetPendingSchemaFileResources returns pending schema file resource IDs
// from the broadcaster. Must be called after Register.
func GetPendingSchemaFileResources() map[int64][]int64 {
	if !singleton.Ready() {
		return nil
	}
	return singleton.Get().GetPendingSchemaFileResources()
}

// Release releases the broadcaster.
func Release() {
	if !singleton.Ready() {
		return
	}
	singleton.Get().Close()
}

// StartBroadcastWithIdempotencyKey resolves retries before waiting for resource
// keys retained by a paired Begin. Ordinary DDL keeps the existing entry point.
func StartBroadcastWithIdempotencyKey(ctx context.Context, msgType message.MessageType, key message.IdempotencyKey, resourceKeys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
	bc, err := singleton.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := balancer.WaitUntilWALbasedDDLReady(ctx); err != nil {
		return nil, err
	}
	return bc.WithResourceKeysForMessage(ctx, msgType, key, resourceKeys...)
}
