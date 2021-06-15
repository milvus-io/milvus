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

package grpcindexserviceclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

type UniqueID = typeutil.UniqueID

type Client struct {
	ctx        context.Context
	grpcClient indexpb.IndexServiceClient
	conn       *grpc.ClientConn

	addr string
	sess *sessionutil.Session

	timeout   time.Duration
	recallTry int
	reconnTry int
}

func getIndexServiceaddr(sess *sessionutil.Session) (string, error) {
	key := typeutil.IndexServiceRole
	msess, _, err := sess.GetSessions(key)
	if err != nil {
		log.Debug("IndexServiceClient GetSessions failed", zap.Any("key", key), zap.Error(err))
		return "", err
	}
	log.Debug("IndexServiceClient GetSessions success", zap.Any("key", key), zap.Any("msess", msess))
	ms, ok := msess[key]
	if !ok {
		log.Debug("IndexServiceClient msess key not existed", zap.Any("key", key), zap.Any("len of msess", len(msess)))
		return "", fmt.Errorf("number of indexservice is incorrect, %d", len(msess))
	}
	return ms.Address, nil
}

func NewClient(metaRoot string, etcdEndpoints []string, timeout time.Duration) *Client {
	sess := sessionutil.NewSession(context.Background(), metaRoot, etcdEndpoints)
	return &Client{
		ctx:       context.Background(),
		sess:      sess,
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
	tracer := opentracing.GlobalTracer()
	var err error
	getIndexServiceaddrFn := func() error {
		c.addr, err = getIndexServiceaddr(c.sess)
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(c.reconnTry, 3*time.Second, getIndexServiceaddrFn)
	if err != nil {
		log.Debug("IndexServiceClient getIndexServiceAddress failed", zap.Error(err))
		return err
	}
	log.Debug("IndexServiceClient getIndexServiceAddress success")
	connectGrpcFunc := func() error {
		ctx, cancelFunc := context.WithTimeout(c.ctx, c.timeout)
		defer cancelFunc()
		log.Debug("IndexServiceClient try connect ", zap.String("address", c.addr))
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
		log.Debug("IndexServiceClient try connect failed", zap.Error(err))
		return err
	}
	log.Debug("IndexServiceClient connect success")
	c.grpcClient = indexpb.NewIndexServiceClient(c.conn)
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

func (c *Client) RegisterNode(ctx context.Context, req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.RegisterNode(ctx, req)
	})
	return ret.(*indexpb.RegisterNodeResponse), err
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
