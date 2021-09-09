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

package grpcquerynodeclient

import (
	"context"
	"errors"
	"fmt"
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

type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient querypb.QueryNodeClient
	conn       *grpc.ClientConn

	addr string
}

func NewClient(ctx context.Context, addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	ctx, cancel := context.WithCancel(ctx)

	return &Client{
		ctx:    ctx,
		cancel: cancel,
		addr:   addr,
	}, nil
}

func (c *Client) Init() error {
	Params.Init()
	return c.connect(retry.Attempts(20))
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
	err = c.connect()
	if err != nil {
		return ret, errors.New("Connect to querynode failed with error:\n" + err.Error())
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
	c.cancel()
	if c.conn != nil {
		c.conn.Close()
	}
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

func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

func (c *Client) AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AddQueryChannel(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.RemoveQueryChannel(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.WatchDmChannels(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.LoadSegments(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ReleaseCollection(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ReleasePartitions(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ReleaseSegments(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetSegmentInfo(ctx, req)
	})
	return ret.(*querypb.GetSegmentInfoResponse), err
}

func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetMetrics(ctx, req)
	})
	return ret.(*milvuspb.GetMetricsResponse), err
}
