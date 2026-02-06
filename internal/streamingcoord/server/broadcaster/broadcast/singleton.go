package broadcast

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
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
		return nil, errors.Wrap(err, "failed to wait until WAL based DDL ready")
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
		return nil, errors.Wrap(err, "failed to wait until WAL based DDL ready")
	}
	return broadcaster.WithSecondaryClusterResourceKey(ctx)
}

// Release releases the broadcaster.
func Release() {
	if !singleton.Ready() {
		return
	}
	singleton.Get().Close()
}
