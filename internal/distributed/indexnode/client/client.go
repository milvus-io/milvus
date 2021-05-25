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
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/retry"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type Client struct {
	grpcClient indexpb.IndexNodeClient
	address    string
	ctx        context.Context
}

func NewClient(nodeAddress string) (*Client, error) {
	return &Client{
		address: nodeAddress,
		ctx:     context.Background(),
	}, nil
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	connectGrpcFunc := func() error {
		log.Debug("indexnode connect ", zap.String("address", c.address))
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(
				otgrpc.OpenTracingStreamClientInterceptor(tracer)))
		if err != nil {
			return err
		}
		c.grpcClient = indexpb.NewIndexNodeClient(conn)
		return nil
	}
	err := retry.Retry(100000, time.Millisecond*200, connectGrpcFunc)
	if err != nil {
		return err
	}
	return nil
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
	return c.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
}

func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
}

func (c *Client) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*commonpb.Status, error) {
	return c.grpcClient.BuildIndex(ctx, req)
}

func (c *Client) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return c.grpcClient.DropIndex(ctx, req)
}
