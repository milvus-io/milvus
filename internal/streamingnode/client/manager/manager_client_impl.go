package manager

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ ManagerClient = (*managerClientImpl)(nil)

// managerClientImpl implements ManagerClient.
type managerClientImpl struct {
	lifetime *typeutil.Lifetime
	stopped  chan struct{}

	rb      resolver.Builder
	service lazygrpc.Service[streamingpb.StreamingNodeManagerServiceClient]
}

func (c *managerClientImpl) WatchNodeChanged(ctx context.Context) (<-chan struct{}, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("manager client is closing")
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
				return status.NewOnShutdownError("manager client is closing")
			case resultCh <- struct{}{}:
			}
			return nil
		})
	}()
	return resultCh, nil
}

// CollectAllStatus collects status in all underlying streamingnode.
func (c *managerClientImpl) CollectAllStatus(ctx context.Context) (map[int64]*types.StreamingNodeStatus, error) {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// Get all discovered streamingnode.
	state := c.rb.Resolver().GetLatestState()
	if len(state.State.Addresses) == 0 {
		return make(map[int64]*types.StreamingNodeStatus), nil
	}

	// Collect status of all streamingnode.
	result, err := c.getAllStreamingNodeStatus(ctx, state)
	if err != nil {
		return nil, err
	}

	// Collect status may cost some time, so we need to check the lifetime again.
	newState := c.rb.Resolver().GetLatestState()
	if newState.Version.GT(state.Version) {
		newSession := newState.Sessions()
		for serverID := range result {
			if session, ok := newSession[serverID]; !ok {
				result[serverID].Err = types.ErrNotAlive
			} else if session.Stopping {
				result[serverID].Err = types.ErrStopping
			}
		}
	}
	return result, nil
}

func (c *managerClientImpl) getAllStreamingNodeStatus(ctx context.Context, state discoverer.VersionedState) (map[int64]*types.StreamingNodeStatus, error) {
	log := log.Ctx(ctx)
	// wait for manager service ready.
	manager, err := c.service.GetService(ctx)
	if err != nil {
		return nil, err
	}

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(10)
	var mu sync.Mutex
	result := make(map[int64]*types.StreamingNodeStatus, len(state.State.Addresses))
	for serverID, session := range state.Sessions() {
		serverID := serverID
		address := session.Address
		g.Go(func() error {
			ctx := contextutil.WithPickServerID(ctx, serverID)
			resp, err := manager.CollectStatus(ctx, &streamingpb.StreamingNodeManagerCollectStatusRequest{})
			mu.Lock()
			defer mu.Unlock()
			result[serverID] = &types.StreamingNodeStatus{
				StreamingNodeInfo: types.StreamingNodeInfo{
					ServerID: serverID,
					Address:  address,
				},
				Err: err,
			}

			if err != nil {
				log.Warn("collect status failed, skip", zap.Int64("serverID", serverID), zap.Error(err))
				return err
			}
			log.Debug("collect status success", zap.Int64("serverID", serverID), zap.Any("status", resp))
			return nil
		})
	}
	g.Wait()

	return result, nil
}

// Assign a wal instance for the channel on log node of given server id.
func (c *managerClientImpl) Assign(ctx context.Context, pchannel types.PChannelInfoAssigned) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// wait for manager service ready.
	manager, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}

	// Select a log node to assign the wal instance.
	ctx = contextutil.WithPickServerID(ctx, pchannel.Node.ServerID)
	_, err = manager.Assign(ctx, &streamingpb.StreamingNodeManagerAssignRequest{
		Pchannel: types.NewProtoFromPChannelInfo(pchannel.Channel),
	})
	return err
}

// Remove the wal instance for the channel on log node of given server id.
func (c *managerClientImpl) Remove(ctx context.Context, pchannel types.PChannelInfoAssigned) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// wait for manager service ready.
	manager, err := c.service.GetService(ctx)
	if err != nil {
		return err
	}

	// Select a streaming node to remove the wal instance.
	ctx = contextutil.WithPickServerID(ctx, pchannel.Node.ServerID)
	_, err = manager.Remove(ctx, &streamingpb.StreamingNodeManagerRemoveRequest{
		Pchannel: types.NewProtoFromPChannelInfo(pchannel.Channel),
	})
	// The following error can be treated as success.
	// 1. err is nil, a real remove operation at streaming node has been happened.
	// 2. err is ErrSubConnNoExist, the streaming node is not alive at view of session, so the wal on it is already removed.
	// 3. err is SkippedOperation, the streaming node is not the owner of the wal, so the wal on it is already removed.
	if err == nil || picker.IsErrSubConnNoExist(err) {
		return nil
	}
	statusErr := status.AsStreamingError(err)
	if statusErr == nil || statusErr.IsSkippedOperation() {
		return nil
	}
	return err
}

// Close closes the manager client.
func (c *managerClientImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	close(c.stopped)
	c.lifetime.Wait()

	c.service.Close()
	c.rb.Close()
}
