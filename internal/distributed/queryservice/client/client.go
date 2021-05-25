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

package grpcqueryserviceclient

import (
	"context"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type Client struct {
	grpcClient querypb.QueryServiceClient
	conn       *grpc.ClientConn

	addr    string
	timeout time.Duration
	retry   int
}

func NewClient(address string, timeout time.Duration) (*Client, error) {

	return &Client{
		grpcClient: nil,
		conn:       nil,
		addr:       address,
		timeout:    timeout,
		retry:      300,
	}, nil
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	var err error
	for i := 0; i < c.retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(
				otgrpc.OpenTracingStreamClientInterceptor(tracer))); err == nil {
			break
		}
	}

	if err != nil {
		return err
	}

	c.grpcClient = querypb.NewQueryServiceClient(c.conn)
	log.Debug("connected to queryService", zap.String("queryService", c.addr))
	return nil
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
	return c.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
}

func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
}

func (c *Client) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return c.grpcClient.RegisterNode(ctx, req)
}

func (c *Client) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return c.grpcClient.ShowCollections(ctx, req)
}

func (c *Client) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadCollection(ctx, req)
}

func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseCollection(ctx, req)
}

func (c *Client) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return c.grpcClient.ShowPartitions(ctx, req)
}

func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadPartitions(ctx, req)
}

func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleasePartitions(ctx, req)
}

func (c *Client) CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error) {
	return c.grpcClient.CreateQueryChannel(ctx, &querypb.CreateQueryChannelRequest{})
}

func (c *Client) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return c.grpcClient.GetPartitionStates(ctx, req)
}

func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return c.grpcClient.GetSegmentInfo(ctx, req)
}
