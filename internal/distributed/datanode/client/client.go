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
	"time"

	"github.com/milvus-io/milvus/internal/distributed"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

type Client struct {
	conn       *grpc.ClientConn
	grpcClient datapb.DataNodeClient
}

// BuildClients creates a datanode client etcdCli and endpoints name.
func BuildClients(etcdCli *clientv3.Client, opts ...distributed.BuildOption) (*Client, error) {
	conn, err := distributed.BuildConnections(etcdCli, typeutil.DataNodeRole, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:       conn,
		grpcClient: datapb.NewDataNodeClient(conn),
	}, nil
}

func (c *Client) WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest, opts ...distributed.CallOption) (*commonpb.Status, error) {
	cfg := distributed.NewCallConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	ctx = context.WithValue(ctx, distributed.AddressKey{}, cfg.Address)

	var cancel context.CancelFunc
	if cfg.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
		defer cancel()
	}

	return c.grpcClient.WatchDmChannels(ctx, req)
}
func (c *Client) FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest, opts ...distributed.CallOption) (*commonpb.Status, error) {
	cfg := distributed.NewCallConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	ctx = context.WithValue(ctx, distributed.AddressKey{}, cfg.Address)

	var cancel context.CancelFunc
	if cfg.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
		defer cancel()
	}

	return c.grpcClient.FlushSegments(ctx, req)
}

func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...distributed.CallOption) (*milvuspb.GetMetricsResponse, error) {
	cfg := distributed.NewCallConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	ctx = context.WithValue(ctx, distributed.AddressKey{}, cfg.Address)

	var cancel context.CancelFunc
	if cfg.Timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
		defer cancel()
	}

	return c.grpcClient.GetMetrics(ctx, req)
}

func (c *Client) Close() {
	c.conn.Close()
}
