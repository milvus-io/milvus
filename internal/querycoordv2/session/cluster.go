package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

const (
	updateTickerDuration = 1 * time.Minute
	segmentBufferSize    = 16
	bufferFlushPeriod    = 500 * time.Millisecond
)

var (
	ErrNodeNotFound = errors.New("NodeNotFound")
)

func WrapErrNodeNotFound(nodeID int64) error {
	return fmt.Errorf("%w(%v)", ErrNodeNotFound, nodeID)
}

type Cluster interface {
	WatchDmChannels(ctx context.Context, nodeID int64, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	UnsubDmChannel(ctx context.Context, nodeID int64, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, nodeID int64, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, nodeID int64, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	GetDataDistribution(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error)
	GetMetrics(ctx context.Context, nodeID int64, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	SyncDistribution(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error)
	Start(ctx context.Context)
	Stop()
}

// QueryCluster is used to send requests to QueryNodes and manage connections
type QueryCluster struct {
	*clients
	nodeManager *NodeManager
	wg          sync.WaitGroup
	ch          chan struct{}
}

func NewCluster(nodeManager *NodeManager) *QueryCluster {
	c := &QueryCluster{
		clients:     newClients(),
		nodeManager: nodeManager,
		ch:          make(chan struct{}),
	}
	return c
}

func (c *QueryCluster) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.updateLoop()
}

func (c *QueryCluster) Stop() {
	c.clients.closeAll()
	close(c.ch)
	c.wg.Wait()
}

func (c *QueryCluster) updateLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(updateTickerDuration)
	for {
		select {
		case <-c.ch:
			log.Info("cluster closed")
			return
		case <-ticker.C:
			nodes := c.clients.getAllNodeIDs()
			for _, id := range nodes {
				if c.nodeManager.Get(id) == nil {
					c.clients.close(id)
				}
			}
		}
	}
}

func (c *QueryCluster) LoadSegments(ctx context.Context, nodeID int64, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.LoadSegments(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) WatchDmChannels(ctx context.Context, nodeID int64, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.WatchDmChannels(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) UnsubDmChannel(ctx context.Context, nodeID int64, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.UnsubDmChannel(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) ReleaseSegments(ctx context.Context, nodeID int64, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.ReleaseSegments(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) GetDataDistribution(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	var resp *querypb.GetDataDistributionResponse
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		resp, err = cli.GetDataDistribution(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return resp, err
}

func (c *QueryCluster) GetMetrics(ctx context.Context, nodeID int64, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	var (
		resp *milvuspb.GetMetricsResponse
		err  error
	)
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		resp, err = cli.GetMetrics(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return resp, err
}

func (c *QueryCluster) SyncDistribution(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	var (
		resp *commonpb.Status
		err  error
	)
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		resp, err = cli.SyncDistribution(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return resp, err
}

func (c *QueryCluster) send(ctx context.Context, nodeID int64, fn func(cli *grpcquerynodeclient.Client)) error {
	node := c.nodeManager.Get(nodeID)
	if node == nil {
		return WrapErrNodeNotFound(nodeID)
	}

	cli, err := c.clients.getOrCreate(ctx, node)
	if err != nil {
		return err
	}

	fn(cli)
	return nil
}

type clients struct {
	sync.RWMutex
	clients map[int64]*grpcquerynodeclient.Client // nodeID -> client
}

func (c *clients) getAllNodeIDs() []int64 {
	c.RLock()
	defer c.RUnlock()

	ret := make([]int64, 0, len(c.clients))
	for k := range c.clients {
		ret = append(ret, k)
	}
	return ret
}

func (c *clients) getOrCreate(ctx context.Context, node *NodeInfo) (*grpcquerynodeclient.Client, error) {
	if cli := c.get(node.ID()); cli != nil {
		return cli, nil
	}

	newCli, err := createNewClient(context.Background(), node.Addr())
	if err != nil {
		return nil, err
	}
	c.set(node.ID(), newCli)
	return c.get(node.ID()), nil
}

func createNewClient(ctx context.Context, addr string) (*grpcquerynodeclient.Client, error) {
	newCli, err := grpcquerynodeclient.NewClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err = newCli.Init(); err != nil {
		return nil, err
	}
	if err = newCli.Start(); err != nil {
		return nil, err
	}
	return newCli, nil
}

func (c *clients) set(nodeID int64, client *grpcquerynodeclient.Client) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.clients[nodeID]; ok {
		if err := client.Stop(); err != nil {
			log.Warn("close new created client error", zap.Int64("nodeID", nodeID), zap.Error(err))
			return
		}
		log.Info("use old client", zap.Int64("nodeID", nodeID))
	}
	c.clients[nodeID] = client
}

func (c *clients) get(nodeID int64) *grpcquerynodeclient.Client {
	c.RLock()
	defer c.RUnlock()
	return c.clients[nodeID]
}

func (c *clients) close(nodeID int64) {
	c.Lock()
	defer c.Unlock()
	if cli, ok := c.clients[nodeID]; ok {
		if err := cli.Stop(); err != nil {
			log.Warn("error occurred during stopping client", zap.Int64("nodeID", nodeID), zap.Error(err))
		}
		delete(c.clients, nodeID)
	}
}

func (c *clients) closeAll() {
	c.Lock()
	defer c.Unlock()
	for nodeID, cli := range c.clients {
		if err := cli.Stop(); err != nil {
			log.Warn("error occurred during stopping client", zap.Int64("nodeID", nodeID), zap.Error(err))
		}
	}
}

func newClients() *clients {
	return &clients{clients: make(map[int64]*grpcquerynodeclient.Client)}
}
