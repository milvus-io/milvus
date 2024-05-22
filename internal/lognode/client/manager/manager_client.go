package manager

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"go.uber.org/zap"
)

var _ ManagerClient = (*managerClientImpl)(nil)

type ManagerClient interface {
	// WatchNodeChanged returns a channel that receive a node change.
	WatchNodeChanged(ctx context.Context) <-chan map[int64]*sessionutil.Session

	// CollectStatus collects status of all wal instances in all lognode.
	CollectAllStatus(ctx context.Context) (map[int64]*layout.NodeStatus, error)

	// Assign a wal instance for the channel on log node of given server id.
	Assign(ctx context.Context, serverID int64, channel logpb.PChannelInfo) error

	// Remove the wal instance for the channel on log node of given server id.
	Remove(ctx context.Context, serverID int64, channel logpb.PChannelInfo) error

	// Close closes the manager client.
	Close()
}

// managerClientImpl implements ManagerClient.
type managerClientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]

	rb   resolver.Builder
	conn *lazyconn.LazyGRPCConn
}

func (c *managerClientImpl) WatchNodeChanged(ctx context.Context) <-chan map[int64]*sessionutil.Session {
	resultCh := make(chan map[int64]*sessionutil.Session)
	go func() {
		defer close(resultCh)
		c.rb.Resolver().Watch(ctx, func(state resolver.VersionedState) error {
			select {
			case <-ctx.Done():
			case <-c.lifetime.CloseCh():
				return ctx.Err()
			case resultCh <- state.Sessions():
			}
			return nil
		})
	}()
	return resultCh
}

func (c *managerClientImpl) getManagerService(ctx context.Context) (logpb.LogNodeManagerServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return logpb.NewLogNodeManagerServiceClient(conn), nil
}

// CollectAllStatus collects status of all wal instances in all underlying lognode.
func (c *managerClientImpl) CollectAllStatus(ctx context.Context) (map[int64]*layout.NodeStatus, error) {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	state := c.rb.Resolver().GetLatestState()
	result := make(map[int64]*layout.NodeStatus, len(state.State.Addresses))
	if len(state.State.Addresses) == 0 {
		return result, nil
	}
	mu := &sync.Mutex{}
	wg := sync.WaitGroup{}

	// wait for manager service ready.
	manager, err := c.getManagerService(ctx)
	if err != nil {
		return nil, err
	}

	// Select all log nodes to collect status.
	for id, session := range state.Sessions() {
		address := session.Address
		wg.Add(1)
		go func(serverID int64) {
			defer wg.Done()

			ctx = contextutil.WithPickServerID(ctx, serverID)
			resp, err := manager.CollectStatus(ctx, &logpb.LogNodeManagerCollectStatusRequest{})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Warn("collect status failed, skip", zap.Int64("serverID", serverID), zap.Error(err))
				result[serverID] = &layout.NodeStatus{
					ServerID: serverID,
					Address:  address,
					Error:    err,
				}
				return
			}
			log.Debug("collect status success", zap.Int64("serverID", serverID), zap.Any("status", resp))

			channels := make(map[string]*logpb.PChannelInfo)
			for _, ch := range resp.Channels {
				channels[ch.Name] = ch
			}
			result[serverID] = &layout.NodeStatus{
				ServerID:          serverID,
				Address:           address,
				Channels:          channels,
				BalanceAttributes: resp.BalancerAttributes,
			}
		}(id)
	}
	wg.Wait()
	return result, nil
}

// Assign a wal instance for the channel on log node of given server id.
func (c *managerClientImpl) Assign(ctx context.Context, serverID int64, channel logpb.PChannelInfo) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// wait for manager service ready.
	manager, err := c.getManagerService(ctx)
	if err != nil {
		return err
	}

	// Select a log node to assign the wal instance.
	ctx = contextutil.WithPickServerID(ctx, serverID)
	_, err = manager.Assign(ctx, &logpb.LogNodeManagerAssignRequest{
		Channel: &channel,
	})
	return err
}

// Remove the wal instance for the channel on log node of given server id.
func (c *managerClientImpl) Remove(ctx context.Context, serverID int64, channel logpb.PChannelInfo) error {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("manager client is closing")
	}
	defer c.lifetime.Done()

	// wait for manager service ready.
	manager, err := c.getManagerService(ctx)
	if err != nil {
		return err
	}

	// Select a log node to remove the wal instance.
	ctx = contextutil.WithPickServerID(ctx, serverID)
	_, err = manager.Remove(ctx, &logpb.LogNodeManagerRemoveRequest{
		ChannelName: channel.Name,
		Term:        channel.Term,
	})
	return err
}

// Close closes the manager client.
func (c *managerClientImpl) Close() {
	c.lifetime.SetState(lifetime.Stopped)
	c.lifetime.Wait()
	c.lifetime.Close()
	c.conn.Close()
	c.rb.Close()
}
