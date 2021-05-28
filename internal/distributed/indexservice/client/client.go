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

	address   string
	sess      *sessionutil.Session
	timeout   time.Duration
	recallTry int
	reconnTry int
}

func getIndexServiceAddress(sess *sessionutil.Session) (string, error) {
	key := typeutil.IndexServiceRole
	msess, _, err := sess.GetSessions(key)
	if err != nil {
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		return "", fmt.Errorf("number of master service is incorrect, %d", len(msess))
	}
	return ms.Address, nil
}

func NewClient(address, metaRoot string, etcdAddr []string, timeout time.Duration) *Client {
	sess := sessionutil.NewSession(context.Background(), metaRoot, etcdAddr)
	return &Client{
		address:   address,
		ctx:       context.Background(),
		sess:      sess,
		timeout:   timeout,
		recallTry: 3,
		reconnTry: 10,
	}
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	if c.address != "" {
		connectGrpcFunc := func() error {
			log.Debug("indexservice connect ", zap.String("address", c.address))
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
		err := retry.Retry(100000, time.Millisecond*200, connectGrpcFunc)
		if err != nil {
			return err
		}
	} else {
		return c.reconnect()
	}
	c.grpcClient = indexpb.NewIndexServiceClient(c.conn)
	return nil
}

func (c *Client) reconnect() error {
	tracer := opentracing.GlobalTracer()
	var err error
	getIndexServiceAddressFn := func() error {
		c.address, err = getIndexServiceAddress(c.sess)
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(c.reconnTry, 3*time.Second, getIndexServiceAddressFn)
	if err != nil {
		return err
	}
	connectGrpcFunc := func() error {
		log.Debug("IndexService connect ", zap.String("address", c.address))
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
		return err
	}
	c.grpcClient = indexpb.NewIndexServiceClient(c.conn)
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
