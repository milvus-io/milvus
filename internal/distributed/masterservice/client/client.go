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

package grpcmasterserviceclient

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// grpc client
type GrpcClient struct {
	grpcClient masterpb.MasterServiceClient
	conn       *grpc.ClientConn

	//inner member
	addr      string
	timeout   time.Duration
	reconnTry int
	recallTry int

	sess *sessionutil.Session
}

func getMasterServiceAddr(sess *sessionutil.Session) (string, error) {
	key := typeutil.MasterServiceRole
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

func NewClient(addr string, etcdAddr []string, timeout time.Duration) (*GrpcClient, error) {
	sess := sessionutil.NewSession(context.Background(), etcdAddr)
	if sess == nil {
		return nil, fmt.Errorf("new session error, maybe can not connect to etcd")
	}

	return &GrpcClient{
		grpcClient: nil,
		conn:       nil,
		addr:       addr,
		timeout:    timeout,
		reconnTry:  300,
		recallTry:  3,
		sess:       sess,
	}, nil
}

func (c *GrpcClient) reconnect() error {
	addr, err := getMasterServiceAddr(c.sess)
	if err != nil {
		return nil
	}
	tracer := opentracing.GlobalTracer()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	for i := 0; i < c.reconnTry; i++ {
		if c.conn, err = grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock(),
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
	c.grpcClient = masterpb.NewMasterServiceClient(c.conn)
	return nil
}

func (c *GrpcClient) Init() error {
	tracer := opentracing.GlobalTracer()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	var err error
	if c.addr != "" {
		for i := 0; i < c.reconnTry; i++ {
			if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
				grpc.WithUnaryInterceptor(
					otgrpc.OpenTracingClientInterceptor(tracer)),
				grpc.WithStreamInterceptor(
					otgrpc.OpenTracingStreamClientInterceptor(tracer))); err == nil {
				break
			}
		}
		if err != nil {
			return fmt.Errorf("connect to specific address gprc error")
		}
	} else {
		return c.reconnect()
	}
	c.grpcClient = masterpb.NewMasterServiceClient(c.conn)
	return nil
}

func (c *GrpcClient) Start() error {
	return nil
}
func (c *GrpcClient) Stop() error {
	return c.conn.Close()
}

// Register dummy
func (c *GrpcClient) Register() error {
	return nil
}

func (c *GrpcClient) recall(caller func() (interface{}, error)) (interface{}, error) {
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

// TODO: timeout need to be propagated through ctx
func (c *GrpcClient) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	return ret.(*internalpb.ComponentStates), err
}
func (c *GrpcClient) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

//just define a channel, not used currently
func (c *GrpcClient) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (c *GrpcClient) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

//DDL request
func (c *GrpcClient) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.CreateCollection(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DropCollection(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.HasCollection(ctx, in)
	})
	return ret.(*milvuspb.BoolResponse), err
}
func (c *GrpcClient) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DescribeCollection(ctx, in)
	})
	return ret.(*milvuspb.DescribeCollectionResponse), err
}

func (c *GrpcClient) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ShowCollections(ctx, in)
	})
	return ret.(*milvuspb.ShowCollectionsResponse), err
}
func (c *GrpcClient) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.CreatePartition(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DropPartition(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.HasPartition(ctx, in)
	})
	return ret.(*milvuspb.BoolResponse), err
}

func (c *GrpcClient) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ShowPartitions(ctx, in)
	})
	return ret.(*milvuspb.ShowPartitionsResponse), err
}

//index builder service
func (c *GrpcClient) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.CreateIndex(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DropIndex(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DescribeIndex(ctx, in)
	})
	return ret.(*milvuspb.DescribeIndexResponse), err
}

//global timestamp allocator
func (c *GrpcClient) AllocTimestamp(ctx context.Context, in *masterpb.AllocTimestampRequest) (*masterpb.AllocTimestampResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AllocTimestamp(ctx, in)
	})
	return ret.(*masterpb.AllocTimestampResponse), err
}

func (c *GrpcClient) AllocID(ctx context.Context, in *masterpb.AllocIDRequest) (*masterpb.AllocIDResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AllocID(ctx, in)
	})
	return ret.(*masterpb.AllocIDResponse), err
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *GrpcClient) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.UpdateChannelTimeTick(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

//receiver time tick from proxy service, and put it into this channel
func (c *GrpcClient) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.DescribeSegment(ctx, in)
	})
	return ret.(*milvuspb.DescribeSegmentResponse), err
}

func (c *GrpcClient) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ShowSegments(ctx, in)
	})
	return ret.(*milvuspb.ShowSegmentsResponse), err
}
