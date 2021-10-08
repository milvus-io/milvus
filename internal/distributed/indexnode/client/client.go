// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package grpcindexnodeclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// Client is the grpc client of IndexNode.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient    indexpb.IndexNodeClient
	conn          *grpc.ClientConn
	grpcClientMtx sync.RWMutex

	addr string

	getGrpcClient func() (indexpb.IndexNodeClient, error)
}

func (c *Client) setGetGrpcClientFunc() {
	c.getGrpcClient = c.getGrpcClientFunc
}

func (c *Client) getGrpcClientFunc() (indexpb.IndexNodeClient, error) {
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

	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.grpcClient = nil
}

// NewClient creates a new IndexNode client.
func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
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

// Init initializes IndexNode's grpc client.
func (c *Client) Init() error {
	Params.Init()
	return nil
}

func (c *Client) connect(retryOptions ...retry.Option) error {
	connectGrpcFunc := func() error {
		opts := trace.GetInterceptorOpts()
		log.Debug("IndexNodeClient try connect ", zap.String("address", c.addr))
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
					grpc_retry.StreamClientInterceptor(
						grpc_retry.WithMax(3),
						grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
					),
					grpc_opentracing.StreamClientInterceptor(opts...),
				)),
		)
		if err != nil {
			return err
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
		c.conn = conn
		return nil
	}

	err := retry.Do(c.ctx, connectGrpcFunc, retryOptions...)
	if err != nil {
		log.Debug("IndexNodeClient try connect failed", zap.Error(err))
		return err
	}
	log.Debug("IndexNodeClient try connect success", zap.String("address", c.addr))
	c.grpcClient = indexpb.NewIndexNodeClient(c.conn)
	return nil
}

func (c *Client) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	log.Debug("IndexNode Client grpc error", zap.Error(err))

	c.resetConnection()

	ret, err = caller()
	if err == nil {
		return ret, nil
	}
	return ret, err
}

// Start starts IndexNode's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops IndexNode's grpc client.
func (c *Client) Stop() error {
	c.cancel()
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	return nil
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

// GetComponentStates gets the component states of IndexNode.
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

// GetTimeTickChannel gets the time tick channel of IndexNode.
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

// GetStatisticsChannel gets the statistics channel of IndexNode.
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

// CreateIndex sends the build index request to IndexNode.
func (c *Client) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.CreateIndex(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetMetrics gets the metrics info of IndexNode.
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
