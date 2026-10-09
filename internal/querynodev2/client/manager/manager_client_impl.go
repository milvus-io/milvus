package manager

import (
	"context"
	"maps"
	"sync"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ ManagerClient = (*managerClientImpl)(nil)

// managerClientImpl implements ManagerClient using etcd session discovery.
type managerClientImpl struct {
	lifetime *typeutil.Lifetime
	stopped  chan struct{}

	rb      resolver.Builder
	service lazygrpc.Service[viewpb.ViewSyncServiceClient]

	mu                   sync.RWMutex
	nodeChangedNotifiers []func()
	watchStarted         bool
	nodeListeners        map[uint64]func(int64, *NodeInfo)
	nextNodeListener     uint64
	nodes                map[int64]*NodeInfo
	discoveryReady       chan struct{}
	watchCancel          context.CancelFunc
	watchWG              sync.WaitGroup
}

func (c *managerClientImpl) RegisterNodeChangedNotifier(notifier func()) {
	if notifier == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodeChangedNotifiers = append(c.nodeChangedNotifiers, notifier)
	c.startWatchLocked()
}

func (c *managerClientImpl) startWatchLocked() {
	if c.watchStarted {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.watchStarted = true
	c.discoveryReady = make(chan struct{})
	c.watchCancel = cancel
	c.watchWG.Add(1)
	go c.watchNodeChanged(ctx)
}

func (c *managerClientImpl) watchNodeChanged(ctx context.Context) {
	defer c.watchWG.Done()

	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer c.lifetime.Done()

	_ = c.rb.Resolver().Watch(ctx, func(state resolver.VersionedState) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopped:
			return status.NewOnShutdownError("querynode manager client is closing")
		default:
		}
		c.publishNodeState(state)
		c.notifyNodeChanged()
		return nil
	})
}

func (c *managerClientImpl) notifyNodeChanged() {
	c.mu.RLock()
	notifiers := append([]func(){}, c.nodeChangedNotifiers...)
	c.mu.RUnlock()

	for _, notifier := range notifiers {
		notifier()
	}
}

func (c *managerClientImpl) GetAllQueryNodes(ctx context.Context) (map[int64]*NodeInfo, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode manager client is closing")
	}
	defer c.lifetime.Done()

	state, err := c.rb.Resolver().GetLatestState(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]*NodeInfo, len(state.State.Addresses))
	for serverID, session := range state.Sessions() {
		result[serverID] = &NodeInfo{
			ServerID:     serverID,
			Address:      session.Address,
			Stopping:     session.Stopping,
			ServerLabels: copyServerLabels(session.ServerLabels),
		}
	}
	return result, nil
}

func copyServerLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cp := make(map[string]string, len(labels))
	for k, v := range labels {
		cp[k] = v
	}
	return cp
}

func (c *managerClientImpl) CreateViewSyncClient(ctx context.Context, queryNodeID int64) (viewpb.ViewSyncServiceClient, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("querynode manager client is closing")
	}
	defer c.lifetime.Done()

	client, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}
	return &routedViewSyncServiceClient{
		queryNodeID: queryNodeID,
		client:      client,
	}, nil
}

func (c *managerClientImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.mu.RLock()
	cancel := c.watchCancel
	c.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
	close(c.stopped)
	c.lifetime.Wait()
	c.watchWG.Wait()

	c.service.Close()
	c.rb.Close()
}

type routedViewSyncServiceClient struct {
	queryNodeID int64
	client      viewpb.ViewSyncServiceClient
}

func (c *routedViewSyncServiceClient) SyncQueryView(ctx context.Context, opts ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	return c.client.SyncQueryView(contextutil.WithPickServerID(ctx, c.queryNodeID), opts...)
}

func (c *routedViewSyncServiceClient) SyncDataView(ctx context.Context, in *viewpb.SyncDataViewRequest, opts ...grpc.CallOption) (*viewpb.SyncDataViewResponse, error) {
	return c.client.SyncDataView(contextutil.WithPickServerID(ctx, c.queryNodeID), in, opts...)
}

func (c *managerClientImpl) RegisterQueryNodeListener(ctx context.Context, listener func(int64, *NodeInfo)) (func(), error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, merr.WrapErrServiceUnavailableMsg("querynode manager client is closing")
	}
	defer c.lifetime.Done()
	c.mu.Lock()
	c.startWatchLocked()
	ready := c.discoveryReady
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stopped:
		return nil, merr.WrapErrServiceUnavailableMsg("querynode manager client is closing")
	case <-ready:
	}
	c.mu.Lock()
	if c.nodeListeners == nil {
		c.nodeListeners = make(map[uint64]func(int64, *NodeInfo))
	}
	c.nextNodeListener++
	id := c.nextNodeListener
	c.nodeListeners[id] = listener
	for nodeID, node := range c.nodes {
		listener(nodeID, node)
	}
	c.mu.Unlock()
	return func() { c.mu.Lock(); delete(c.nodeListeners, id); c.mu.Unlock() }, nil
}

func (c *managerClientImpl) publishNodeState(state resolver.VersionedState) {
	next := make(map[int64]*NodeInfo)
	for id, session := range state.Sessions() {
		next[id] = &NodeInfo{ServerID: id, Address: session.Address, Stopping: session.Stopping, ServerLabels: copyServerLabels(session.ServerLabels)}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.nodes {
		if _, ok := next[id]; !ok {
			for _, listener := range c.nodeListeners {
				listener(id, nil)
			}
		}
	}
	for id, node := range next {
		old := c.nodes[id]
		if old != nil && old.Address == node.Address && old.Stopping == node.Stopping && maps.Equal(old.ServerLabels, node.ServerLabels) {
			continue
		}
		for _, listener := range c.nodeListeners {
			listener(id, node)
		}
	}
	if c.nodes == nil {
		close(c.discoveryReady)
	}
	c.nodes = next
}
