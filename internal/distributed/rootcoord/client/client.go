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

package grpcrootcoordclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// GrpcClient grpc client
type GrpcClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient rootcoordpb.RootCoordClient
	conn       *grpc.ClientConn

	sess *sessionutil.Session
	addr string
}

func getRootCoordAddr(sess *sessionutil.Session) (string, error) {
	key := typeutil.RootCoordRole
	msess, _, err := sess.GetSessions(key)
	if err != nil {
		log.Debug("RootCoordClient GetSessions failed", zap.Any("key", key))
		return "", err
	}
	log.Debug("RootCoordClient GetSessions success")
	ms, ok := msess[key]
	if !ok {
		log.Debug("RootCoordClient mess key not exist", zap.Any("key", key))
		return "", fmt.Errorf("number of RootCoord is incorrect, %d", len(msess))
	}
	return ms.Address, nil
}

// NewClient create root coordinator client with specified ectd info and timeout
// ctx execution control context
// metaRoot is the path in etcd for root coordinator registration
// etcdEndpoints are the address list for etcd end points
// timeout is default setting for each grpc call
func NewClient(ctx context.Context, metaRoot string, etcdEndpoints []string) (*GrpcClient, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdEndpoints)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("RootCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)

	return &GrpcClient{
		ctx:    ctx,
		cancel: cancel,
		sess:   sess,
	}, nil
}

func (c *GrpcClient) Init() error {
	Params.Init()
	return c.connect(retry.Attempts(20))
}

func (c *GrpcClient) connect(retryOptions ...retry.Option) error {
	var err error
	connectRootCoordAddrFn := func() error {
		c.addr, err = getRootCoordAddr(c.sess)
		if err != nil {
			log.Debug("RootCoordClient getRootCoordAddr failed", zap.Error(err))
			return err
		}
		opts := trace.GetInterceptorOpts()
		log.Debug("RootCoordClient try reconnect ", zap.String("address", c.addr))
		ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, c.addr,
			grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(Params.ClientMaxRecvSize),
				grpc.MaxCallSendMsgSize(Params.ClientMaxSendSize)),
			grpc.WithUnaryInterceptor(
				grpc_middleware.ChainUnaryClient(
					grpc_retry.UnaryClientInterceptor(
						grpc_retry.WithMax(3),
						grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
					),
					grpc_opentracing.UnaryClientInterceptor(opts...),
				)),
			grpc.WithStreamInterceptor(
				grpc_middleware.ChainStreamClient(
					grpc_retry.StreamClientInterceptor(grpc_retry.WithMax(3),
						grpc_retry.WithCodes(codes.Aborted, codes.Unavailable),
					),
					grpc_opentracing.StreamClientInterceptor(opts...),
				)),
		)
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}

	err = retry.Do(c.ctx, connectRootCoordAddrFn, retryOptions...)
	if err != nil {
		log.Debug("RootCoordClient try reconnect failed", zap.Error(err))
		return err
	}
	log.Debug("RootCoordClient try reconnect success")
	c.grpcClient = rootcoordpb.NewRootCoordClient(c.conn)
	return nil
}

func (c *GrpcClient) Start() error {
	return nil
}

func (c *GrpcClient) Stop() error {
	c.cancel()
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
	log.Debug("RootCoord Client grpc error", zap.Error(err))
	err = c.connect()
	if err != nil {
		return ret, errors.New("Connect to rootcoord failed with error:\n" + err.Error())
	}
	ret, err = caller()
	if err == nil {
		return ret, nil
	}
	return ret, err
}

// GetComponentStates TODO: timeout need to be propagated through ctx
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

// GetStatisticsChannel just define a channel, not used currently
func (c *GrpcClient) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
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

// CreateIndex index builder service
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

// AllocTimestamp global timestamp allocator
func (c *GrpcClient) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AllocTimestamp(ctx, in)
	})
	return ret.(*rootcoordpb.AllocTimestampResponse), err
}

func (c *GrpcClient) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AllocID(ctx, in)
	})
	return ret.(*rootcoordpb.AllocIDResponse), err
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *GrpcClient) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.UpdateChannelTimeTick(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

// DescribeSegment receiver time tick from proxy service, and put it into this channel
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
func (c *GrpcClient) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ReleaseDQLMessageStream(ctx, in)
	})
	return ret.(*commonpb.Status), err
}
func (c *GrpcClient) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.SegmentFlushCompleted(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

func (c *GrpcClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetMetrics(ctx, in)
	})
	return ret.(*milvuspb.GetMetricsResponse), err
}
