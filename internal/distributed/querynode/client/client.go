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

package grpcquerynodeclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// Client is the grpc client of QueryNode.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient    querypb.QueryNodeClient
	conn          *grpc.ClientConn
	grpcClientMtx sync.RWMutex

	addr string

	getGrpcClient func() (querypb.QueryNodeClient, error)
}

func (c *Client) setGetGrpcClientFunc() {
	c.getGrpcClient = c.getGrpcClientFunc
}

func (c *Client) getGrpcClientFunc() (querypb.QueryNodeClient, error) {
	c.grpcClientMtx.RLock()
	if c.grpcClient != nil {
		defer c.grpcClientMtx.RUnlock()
		return c.grpcClient, nil
	}
	c.grpcClientMtx.RUnlock()

	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()

	if c.grpcClient != nil {
		return c.grpcClient, nil
	}

	// FIXME(dragondriver): how to handle error here?
	// if we return nil here, then we should check if client is nil outside,
	err := c.connect(retry.Attempts(20))
	if err != nil {
		return nil, err
	}

	return c.grpcClient, nil
}

func (c *Client) resetConnection() {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()

	c.conn = nil
	c.grpcClient = nil
}

// NewClient creates a new QueryNode client.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	ctx, cancel := context.WithCancel(ctx)

	client := &Client{
		ctx:    ctx,
		cancel: cancel,
		addr:   addr,
	}

	client.setGetGrpcClientFunc()
	return client, nil
}

// Init initializes QueryNode's grpc client.
func (c *Client) Init() error {
	Params.Init()
	return nil
}

func (c *Client) connect(retryOptions ...retry.Option) error {
	connectGrpcFunc := func() error {
		opts := trace.GetInterceptorOpts()
		log.Debug("QueryNodeClient try connect ", zap.String("address", c.addr))
		ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, c.addr,
			grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(Params.ClientMaxRecvSize),
				grpc.MaxCallSendMsgSize(Params.ClientMaxSendSize)),
			grpc.WithUnaryInterceptor(
				grpc_middleware.ChainUnaryClient(
					grpc_retry.UnaryClientInterceptor(
						grpc_retry.WithMax(3),
						grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
					),
					grpc_opentracing.UnaryClientInterceptor(opts...),
				)),
			grpc.WithStreamInterceptor(
				grpc_middleware.ChainStreamClient(
					grpc_retry.StreamClientInterceptor(grpc_retry.WithMax(3),
						grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
					),
					grpc_opentracing.StreamClientInterceptor(opts...),
				)),
		)
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}

	err := retry.Do(c.ctx, connectGrpcFunc, retryOptions...)
	if err != nil {
		log.Debug("QueryNodeClient try connect failed", zap.Error(err))
		return err
	}
	log.Debug("QueryNodeClient try connect success")
	c.grpcClient = querypb.NewQueryNodeClient(c.conn)
	return nil
}

func (c *Client) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	log.Debug("QueryNode Client grpc error", zap.Error(err))

	c.resetConnection()

	ret, err = caller()
	if err == nil {
		return ret, nil
	}
	return ret, err
}

// Start starts QueryNode's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops QueryNode's grpc client server.
func (c *Client) Stop() error {
	c.cancel()
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

// GetComponentStates gets the component states of QueryNode.
func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.ComponentStates), err
}

// GetTimeTickChannel gets the time tick channel of QueryNode.
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// GetStatisticsChannel gets the statistics channel of QueryNode.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// AddQueryChannel adds query channel for QueryNode component.
func (c *Client) AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.AddQueryChannel(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// RemoveQueryChannel removes the query channel for QueryNode component.
func (c *Client) RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.RemoveQueryChannel(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// WatchDmChannels watches the channels about data manipulation.
func (c *Client) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.WatchDmChannels(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// LoadSegments loads the segments to search.
func (c *Client) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.LoadSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleaseCollection releases the data of the specified collection in QueryNode.
func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ReleaseCollection(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleasePartitions releases the data of the specified partitions in QueryNode.
func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ReleasePartitions(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (c *Client) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ReleaseSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetSegmentInfo gets the information of the specified segments in QueryNode.
func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetSegmentInfo(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*querypb.GetSegmentInfoResponse), err
}

// GetMetrics gets the metrics information of QueryNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}
