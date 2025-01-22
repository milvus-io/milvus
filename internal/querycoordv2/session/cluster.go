// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var ErrNodeNotFound = errors.New("NodeNotFound")

func WrapErrNodeNotFound(nodeID int64) error {
	return fmt.Errorf("%w(%v)", ErrNodeNotFound, nodeID)
}

type Cluster interface {
	WatchDmChannels(ctx context.Context, nodeID int64, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	UnsubDmChannel(ctx context.Context, nodeID int64, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, nodeID int64, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, nodeID int64, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	LoadPartitions(ctx context.Context, nodeID int64, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, nodeID int64, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	GetDataDistribution(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error)
	GetMetrics(ctx context.Context, nodeID int64, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
	SyncDistribution(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error)
	GetComponentStates(ctx context.Context, nodeID int64) (*milvuspb.ComponentStates, error)
	Start()
	Stop()
}

// QueryCluster is used to send requests to QueryNodes and manage connections
type QueryCluster struct {
	*clients
	nodeManager *NodeManager
	wg          sync.WaitGroup
	ch          chan struct{}
	stopOnce    sync.Once
}

type QueryNodeCreator func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)

func DefaultQueryNodeCreator(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	return grpcquerynodeclient.NewClient(ctx, addr, nodeID)
}

func NewCluster(nodeManager *NodeManager, queryNodeCreator QueryNodeCreator) *QueryCluster {
	c := &QueryCluster{
		clients:     newClients(queryNodeCreator),
		nodeManager: nodeManager,
		ch:          make(chan struct{}),
	}
	return c
}

func (c *QueryCluster) Start() {
	c.wg.Add(1)
	go c.updateLoop()
}

func (c *QueryCluster) Stop() {
	c.stopOnce.Do(func() {
		c.clients.closeAll()
		close(c.ch)
		c.wg.Wait()
	})
}

func (c *QueryCluster) updateLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(paramtable.Get().QueryCoordCfg.CheckNodeSessionInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
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
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.LoadSegmentsRequest)
		req.Base.TargetID = nodeID
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
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.WatchDmChannelsRequest)
		req.Base.TargetID = nodeID
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
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.UnsubDmChannelRequest)
		req.Base.TargetID = nodeID
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
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.ReleaseSegmentsRequest)
		req.Base.TargetID = nodeID
		status, err = cli.ReleaseSegments(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) LoadPartitions(ctx context.Context, nodeID int64, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.LoadPartitionsRequest)
		req.Base.TargetID = nodeID
		status, err = cli.LoadPartitions(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) ReleasePartitions(ctx context.Context, nodeID int64, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.ReleasePartitionsRequest)
		req.Base.TargetID = nodeID
		status, err = cli.ReleasePartitions(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *QueryCluster) GetDataDistribution(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	var resp *querypb.GetDataDistributionResponse
	var err error
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.GetDataDistributionRequest)
		req.Base = &commonpb.MsgBase{
			TargetID: nodeID,
		}
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
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
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
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		req := proto.Clone(req).(*querypb.SyncDistributionRequest)
		req.Base.TargetID = nodeID
		resp, err = cli.SyncDistribution(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return resp, err
}

func (c *QueryCluster) GetComponentStates(ctx context.Context, nodeID int64) (*milvuspb.ComponentStates, error) {
	var (
		resp *milvuspb.ComponentStates
		err  error
	)
	err1 := c.send(ctx, nodeID, func(cli types.QueryNodeClient) {
		resp, err = cli.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
	if err1 != nil {
		return nil, err1
	}
	return resp, err
}

func (c *QueryCluster) send(ctx context.Context, nodeID int64, fn func(cli types.QueryNodeClient)) error {
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
	clients          map[int64]types.QueryNodeClient // nodeID -> client
	queryNodeCreator QueryNodeCreator
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

func (c *clients) getOrCreate(ctx context.Context, node *NodeInfo) (types.QueryNodeClient, error) {
	if cli := c.get(node.ID()); cli != nil {
		return cli, nil
	}
	return c.create(node)
}

func createNewClient(ctx context.Context, addr string, nodeID int64, queryNodeCreator QueryNodeCreator) (types.QueryNodeClient, error) {
	newCli, err := queryNodeCreator(ctx, addr, nodeID)
	if err != nil {
		return nil, err
	}
	return newCli, nil
}

func (c *clients) create(node *NodeInfo) (types.QueryNodeClient, error) {
	c.Lock()
	defer c.Unlock()
	if cli, ok := c.clients[node.ID()]; ok {
		return cli, nil
	}
	cli, err := createNewClient(context.Background(), node.Addr(), node.ID(), c.queryNodeCreator)
	if err != nil {
		return nil, err
	}
	c.clients[node.ID()] = cli
	return cli, nil
}

func (c *clients) get(nodeID int64) types.QueryNodeClient {
	c.RLock()
	defer c.RUnlock()
	return c.clients[nodeID]
}

func (c *clients) close(nodeID int64) {
	c.Lock()
	defer c.Unlock()
	if cli, ok := c.clients[nodeID]; ok {
		if err := cli.Close(); err != nil {
			log.Warn("error occurred during stopping client", zap.Int64("nodeID", nodeID), zap.Error(err))
		}
		delete(c.clients, nodeID)
	}
}

func (c *clients) closeAll() {
	c.Lock()
	defer c.Unlock()
	for nodeID, cli := range c.clients {
		if err := cli.Close(); err != nil {
			log.Warn("error occurred during stopping client", zap.Int64("nodeID", nodeID), zap.Error(err))
		}
	}
}

func newClients(queryNodeCreator QueryNodeCreator) *clients {
	return &clients{
		clients:          make(map[int64]types.QueryNodeClient),
		queryNodeCreator: queryNodeCreator,
	}
}
