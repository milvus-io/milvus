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
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/retry"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
)

type Client struct {
	ctx        context.Context
	grpcClient querypb.QueryNodeClient
	conn       *grpc.ClientConn

	addr string

	timeout   time.Duration
	reconnTry int
	recallTry int
}

func NewClient(addr string, timeout time.Duration) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	return &Client{
		ctx:       context.Background(),
		addr:      addr,
		timeout:   timeout,
		recallTry: 3,
		reconnTry: 10,
	}, nil
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
	tracer := opentracing.GlobalTracer()
	var err error
	connectGrpcFunc := func() error {
		ctx, cancelFunc := context.WithTimeout(c.ctx, c.timeout)
		defer cancelFunc()
		log.Debug("QueryNodeClient try connect ", zap.String("address", c.addr))
		conn, err := grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(
				otgrpc.OpenTracingStreamClientInterceptor(tracer)))
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}

	err = retry.Retry(c.reconnTry, 500*time.Millisecond, connectGrpcFunc)
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
	for i := 0; i < c.recallTry; i++ {
		err = c.connect()
		if err == nil {
			ret, err = caller()
			if err == nil {
				return ret, nil
			}
		}
	}
	return ret, err
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return c.conn.Close()
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
