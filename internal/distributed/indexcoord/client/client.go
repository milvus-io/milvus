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

package grpcindexcoordclient

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var ClientParams paramtable.GrpcClientConfig

// Client is the grpc client of IndexCoord.
type Client struct {
	grpcClient grpcclient.GrpcClient
	sess       *sessionutil.Session
}

// NewClient creates a new IndexCoord client.
func NewClient(ctx context.Context, metaRoot string, etcdCli *clientv3.Client) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdCli)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("IndexCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	ClientParams.InitOnce(typeutil.IndexCoordRole)
	client := &Client{
		grpcClient: &grpcclient.ClientBase{
			ClientMaxRecvSize:      ClientParams.ClientMaxRecvSize,
			ClientMaxSendSize:      ClientParams.ClientMaxSendSize,
			DialTimeout:            ClientParams.DialTimeout,
			KeepAliveTime:          ClientParams.KeepAliveTime,
			KeepAliveTimeout:       ClientParams.KeepAliveTimeout,
			RetryServiceNameConfig: "milvus.proto.index.IndexCoord",
			MaxAttempts:            ClientParams.MaxAttempts,
			InitialBackoff:         ClientParams.InitialBackoff,
			MaxBackoff:             ClientParams.MaxBackoff,
			BackoffMultiplier:      ClientParams.BackoffMultiplier,
		},
		sess: sess,
	}
	client.grpcClient.SetRole(typeutil.IndexCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getIndexCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client, nil
}

// Init initializes IndexCoord's grpc client.
func (c *Client) Init() error {
	return nil
}

// Start starts IndexCoord's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops IndexCoord's grpc client.
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func (c *Client) getIndexCoordAddr() (string, error) {
	key := c.grpcClient.GetRole()
	msess, _, err := c.sess.GetSessions(key)
	if err != nil {
		log.Debug("IndexCoordClient GetSessions failed", zap.Any("key", key), zap.Error(err))
		return "", err
	}
	log.Debug("IndexCoordClient GetSessions success", zap.Any("key", key), zap.Any("msess", msess))
	ms, ok := msess[key]
	if !ok {
		log.Debug("IndexCoordClient msess key not existed", zap.Any("key", key), zap.Any("len of msess", len(msess)))
		return "", fmt.Errorf("find no available indexcoord, check indexcoord state")
	}
	return ms.Address, nil
}

// newGrpcClient create a new grpc client of IndexCoord.
func (c *Client) newGrpcClient(cc *grpc.ClientConn) interface{} {
	return indexpb.NewIndexCoordClient(cc)
}

// GetComponentStates gets the component states of IndexCoord.
func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.ComponentStates), err
}

// GetStatisticsChannel gets the statistics channel of IndexCoord.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// CreateIndex sends the build index request to IndexCoord.
func (c *Client) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).CreateIndex(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropIndex sends the drop index request to IndexCoord.
func (c *Client) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).DropIndex(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetIndexStates gets the index states from IndexCoord.
func (c *Client) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).GetIndexState(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*indexpb.GetIndexStateResponse), err
}

// GetIndexFilePaths gets the index file paths from IndexCoord.
func (c *Client) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).GetIndexFilePaths(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*indexpb.GetIndexFilePathsResponse), err
}

// DescribeIndex describe the index info of the collection.
func (c *Client) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).DescribeIndex(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*indexpb.DescribeIndexResponse), err
}

// GetMetrics gets the metrics info of IndexCoord.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexCoordClient).GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}
