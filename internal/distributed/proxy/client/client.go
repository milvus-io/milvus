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

package grpcproxyclient

import (
	"context"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient proxypb.ProxyClient
	conn       *grpc.ClientConn
	ctx        context.Context

	addr      string
	timeout   time.Duration
	reconnTry int
	recallTry int
}

func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		addr:      addr,
		ctx:       context.Background(),
		timeout:   timeout,
		recallTry: 3,
		reconnTry: 10,
	}
}

func (c *Client) Init() error {
	// for now, we must try many times in Init Stage
	initFunc := func() error {
		return c.connect()
	}
	err := retry.Retry(10000, 3*time.Second, initFunc)
	return err
}

func (c *Client) connect() error {
	connectGrpcFunc := func() error {
		ctx, cancelFunc := context.WithTimeout(c.ctx, c.timeout)
		defer cancelFunc()
		opts := trace.GetInterceptorOpts()
		log.Debug("ProxyNodeClient try connect ", zap.String("address", c.addr))
		conn, err := grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				grpc_middleware.ChainUnaryClient(
					grpc_retry.UnaryClientInterceptor(),
					grpc_opentracing.UnaryClientInterceptor(opts...),
				)),
			grpc.WithStreamInterceptor(
				grpc_middleware.ChainStreamClient(
					grpc_retry.StreamClientInterceptor(),
					grpc_opentracing.StreamClientInterceptor(opts...),
				)),
		)
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}

	err := retry.Retry(c.reconnTry, 500*time.Millisecond, connectGrpcFunc)
	if err != nil {
		log.Debug("ProxyNodeClient try connect failed", zap.Error(err))
		return err
	}
	log.Debug("ProxyNodeClient connect success")
	c.grpcClient = proxypb.NewProxyClient(c.conn)
	return nil
}

func (c *Client) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	for i := 0; i < c.reconnTry; i++ {
		err = c.connect()
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	ret, err = caller()
	if err == nil {
		return ret, nil
	}
	return ret, err
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return nil
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	return ret.(*internalpb.ComponentStates), err
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

func (c *Client) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.InvalidateCollectionMetaCache(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) ReleaseDQLMessageStream(ctx context.Context, req *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ReleaseDQLMessageStream(ctx, req)
	})
	return ret.(*commonpb.Status), err
}
