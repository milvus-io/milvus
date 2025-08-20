package broadcast

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var singleton = syncutil.NewFuture[broadcaster.Broadcaster]()

// Register registers the broadcaster.
func Register(broadcaster broadcaster.Broadcaster) {
	singleton.Set(broadcaster)
}

// GetWithContext gets the broadcaster with context.
func GetWithContext(ctx context.Context) (broadcaster.Broadcaster, error) {
	return singleton.GetWithContext(ctx)
}

// StartBroadcastWithResourceKeys starts a broadcast with resource keys.
func StartBroadcastWithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
	broadcaster, err := singleton.GetWithContext(ctx)
	if err != nil {
		return nil, err
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
