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
	singleton     = syncutil.NewFuture[broadcaster.Broadcaster]()
	ErrNotPrimary = broadcaster.ErrNotPrimary
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

// Release releases the broadcaster.
func Release() {
	if !singleton.Ready() {
		return
	}
	singleton.Get().Close()
}
