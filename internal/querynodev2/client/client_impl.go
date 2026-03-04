package client

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ Client = (*clientImpl)(nil)

// clientImpl implements Client using etcd session discovery.
type clientImpl struct {
	lifetime *typeutil.Lifetime
	stopped  chan struct{}

	rb      resolver.Builder
	service lazygrpc.Service[viewpb.ViewSyncServiceClient]
}

func (c *clientImpl) WatchNodeChanged(ctx context.Context) (<-chan struct{}, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode client is closing")
	}
	defer c.lifetime.Done()

	resultCh := make(chan struct{}, 1)
	go func() {
		defer close(resultCh)
		c.rb.Resolver().Watch(ctx, func(state resolver.VersionedState) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.stopped:
				return status.NewOnShutdownError("querynode client is closing")
			case resultCh <- struct{}{}:
			}
			return nil
		})
	}()
	return resultCh, nil
}

func (c *clientImpl) GetAllQueryNodes(ctx context.Context) (map[int64]qviews.QueryNode, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode client is closing")
	}
	defer c.lifetime.Done()

	state, err := c.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]qviews.QueryNode, len(state.State.Addresses))
	for serverID := range state.Sessions() {
		result[serverID] = qviews.NewQueryNode(serverID)
	}
	return result, nil
}

func (c *clientImpl) GetViewSyncClient(ctx context.Context) (viewpb.ViewSyncServiceClient, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode client is closing")
	}
	defer c.lifetime.Done()

	return c.service.GetService(ctx)
}

func (c *clientImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	close(c.stopped)
	c.lifetime.Wait()

	c.service.Close()
	c.rb.Close()
}
