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

package grpcdatanodeclient

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Client struct {
	ctx context.Context

	grpc datapb.DataNodeClient
	conn *grpc.ClientConn

	address string

	timeout   time.Duration
	reconnTry int
	recallTry int
}

func NewClient(address string, timeout time.Duration) (*Client, error) {
	if address == "" {
		return nil, fmt.Errorf("address is empty")
	}

	return &Client{
		grpc:      nil,
		conn:      nil,
		address:   address,
		ctx:       context.Background(),
		timeout:   timeout,
		recallTry: 3,
		reconnTry: 10,
	}, nil
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	connectGrpcFunc := func() error {
		log.Debug("DataNodeClient try connect ", zap.String("address", c.address))
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock(),
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

	err := retry.Retry(c.reconnTry, time.Millisecond*500, connectGrpcFunc)
	if err != nil {
		log.Debug("DataNodeClient try connect failed", zap.Error(err))
		return err
	}
	log.Debug("DataNodeClient connect success")
	c.grpc = datapb.NewDataNodeClient(c.conn)
	return nil
}

func (c *Client) reconnect() error {
	tracer := opentracing.GlobalTracer()
	var err error
	connectGrpcFunc := func() error {
		log.Debug("DataNodeClient try reconnect ", zap.String("address", c.address))
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock(),
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
		log.Debug("DataNodeClient try reconnect failed", zap.Error(err))
		return err
	}
	log.Debug("DataNodeClient reconnect success")
	c.grpc = datapb.NewDataNodeClient(c.conn)
	return nil
}

func (c *Client) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	for i := 0; i < c.recallTry; i++ {
		err = c.reconnect()
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
		return c.grpc.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	return ret.(*internalpb.ComponentStates), err
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpc.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

func (c *Client) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpc.WatchDmChannels(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpc.FlushSegments(ctx, req)
	})
	return ret.(*commonpb.Status), err
}
