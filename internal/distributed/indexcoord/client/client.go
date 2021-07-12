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

package grpcindexcoordclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient indexpb.IndexCoordClient
	conn       *grpc.ClientConn

	addr string
	sess *sessionutil.Session
}

func getIndexCoordAddr(sess *sessionutil.Session) (string, error) {
	key := typeutil.IndexCoordRole
	msess, _, err := sess.GetSessions(key)
	if err != nil {
		log.Debug("IndexCoordClient GetSessions failed", zap.Any("key", key), zap.Error(err))
		return "", err
	}
	log.Debug("IndexCoordClient GetSessions success", zap.Any("key", key), zap.Any("msess", msess))
	ms, ok := msess[key]
	if !ok {
		log.Debug("IndexCoordClient msess key not existed", zap.Any("key", key), zap.Any("len of msess", len(msess)))
		return "", fmt.Errorf("number of indexcoord is incorrect, %d", len(msess))
	}
	return ms.Address, nil
}

func NewClient(ctx context.Context, metaRoot string, etcdEndpoints []string) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdEndpoints)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("RootCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Client{
		ctx:    ctx,
		cancel: cancel,
		sess:   sess,
	}, nil
}

func (c *Client) Init() error {
	return c.connect(retry.Attempts(20))
}

func (c *Client) connect(retryOptions ...retry.Option) error {
	var err error
	connectIndexCoordaddrFn := func() error {
		c.addr, err = getIndexCoordAddr(c.sess)
		if err != nil {
			log.Debug("IndexCoordClient getIndexCoordAddress failed")
			return err
		}
		opts := trace.GetInterceptorOpts()
		log.Debug("IndexCoordClient try connect ", zap.String("address", c.addr))
		conn, err := grpc.DialContext(c.ctx, c.addr,
			grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second),
			grpc.WithUnaryInterceptor(
				grpc_middleware.ChainUnaryClient(
					grpc_retry.UnaryClientInterceptor(grpc_retry.WithMax(3), grpc_retry.WithPerRetryTimeout(time.Second*3)),
					grpc_opentracing.UnaryClientInterceptor(opts...),
				)),
			grpc.WithStreamInterceptor(
				grpc_middleware.ChainStreamClient(
					grpc_retry.StreamClientInterceptor(grpc_retry.WithMax(3), grpc_retry.WithPerRetryTimeout(time.Second*3)),
					grpc_opentracing.StreamClientInterceptor(opts...),
				)),
		)
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}

	err = retry.Do(c.ctx, connectIndexCoordaddrFn, retryOptions...)
	if err != nil {
		log.Debug("IndexCoordClient try connect failed", zap.Error(err))
		return err
	}
	log.Debug("IndexCoordClient connect success")
	c.grpcClient = indexpb.NewIndexCoordClient(c.conn)
	return nil
}

func (c *Client) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	log.Debug("IndexCoord Client grpc error", zap.Error(err))
	err = c.connect()
	if err != nil {
		return ret, errors.New("Connect to indexcoord failed with error:\n" + err.Error())
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

func (c *Client) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.BuildIndex(ctx, req)
	})
	return ret.(*indexpb.BuildIndexResponse), err
}

func (c *Client) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DropIndex(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetIndexStates(ctx, req)
	})
	return ret.(*indexpb.GetIndexStatesResponse), err
}
func (c *Client) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetIndexFilePaths(ctx, req)
	})
	return ret.(*indexpb.GetIndexFilePathsResponse), err
}
