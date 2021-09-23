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
	"fmt"
	"sync"
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

	grpcClient    rootcoordpb.RootCoordClient
	conn          *grpc.ClientConn
	grpcClientMtx sync.RWMutex

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

// Init initialize grpc parameters
func (c *GrpcClient) Init() error {
	Params.Init()
	return nil
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
		if c.conn != nil {
			_ = c.conn.Close()
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

func (c *GrpcClient) getGrpcClient() (rootcoordpb.RootCoordClient, error) {
	c.grpcClientMtx.RLock()
	if c.grpcClient != nil {
		defer c.grpcClientMtx.RUnlock()
		return c.grpcClient, nil
	}
	c.grpcClientMtx.RUnlock()

	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()

	if c.grpcClient != nil {
		return c.grpcClient, nil
	}

	// FIXME(dragondriver): how to handle error here?
	// if we return nil here, then we should check if client is nil outside,
	err := c.connect(retry.Attempts(20))
	if err != nil {
		return nil, err
	}

	return c.grpcClient, nil
}

// Start dummy
func (c *GrpcClient) Start() error {
	return nil
}

// Stop terminate grpc connection
func (c *GrpcClient) Stop() error {
	c.cancel()
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Register dummy
func (c *GrpcClient) Register() error {
	return nil
}

func (c *GrpcClient) resetConnection() {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.grpcClient = nil
}

func (c *GrpcClient) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	log.Debug("RootCoord Client grpc error", zap.Error(err))

	c.resetConnection()

	ret, err = caller()
	if err == nil {
		return ret, nil
	}
	return ret, err
}

// GetComponentStates TODO: timeout need to be propagated through ctx
func (c *GrpcClient) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.ComponentStates), err
}

// GetTimeTickChannel get timetick channel name
func (c *GrpcClient) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// GetStatisticsChannel just define a channel, not used currently
func (c *GrpcClient) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// CreateCollection create collection
func (c *GrpcClient) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.CreateCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropCollection drop collection
func (c *GrpcClient) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DropCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// HasCollection check collection existence
func (c *GrpcClient) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.HasCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.BoolResponse), err
}

// DescribeCollection return collection info
func (c *GrpcClient) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DescribeCollection(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.DescribeCollectionResponse), err
}

// ShowCollections list all collection names
func (c *GrpcClient) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ShowCollections(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowCollectionsResponse), err
}

// CreatePartition create partition
func (c *GrpcClient) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.CreatePartition(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropPartition drop partition
func (c *GrpcClient) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DropPartition(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// HasPartition check partition existence
func (c *GrpcClient) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.HasPartition(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.BoolResponse), err
}

// ShowPartitions list all partitions in collection
func (c *GrpcClient) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ShowPartitions(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowPartitionsResponse), err
}

// CreateIndex create index
func (c *GrpcClient) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.CreateIndex(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropIndex drop index
func (c *GrpcClient) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DropIndex(ctx, in)
	})
	return ret.(*commonpb.Status), err
}

// DescribeIndex return index info
func (c *GrpcClient) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DescribeIndex(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.DescribeIndexResponse), err
}

// AllocTimestamp global timestamp allocator
func (c *GrpcClient) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.AllocTimestamp(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*rootcoordpb.AllocTimestampResponse), err
}

// AllocID global ID allocator
func (c *GrpcClient) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.AllocID(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*rootcoordpb.AllocIDResponse), err
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *GrpcClient) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.UpdateChannelTimeTick(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DescribeSegment receiver time tick from proxy service, and put it into this channel
func (c *GrpcClient) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DescribeSegment(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.DescribeSegmentResponse), err
}

// ShowSegments list all segments
func (c *GrpcClient) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ShowSegments(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ShowSegmentsResponse), err
}

// ReleaseDQLMessageStream release DQL msgstream
func (c *GrpcClient) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ReleaseDQLMessageStream(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// SegmentFlushCompleted check whether segment flush is completed
func (c *GrpcClient) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.SegmentFlushCompleted(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetMetrics get metrics
func (c *GrpcClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetMetrics(ctx, in)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}

// CreateAlias create collection alias
func (c *GrpcClient) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.CreateAlias(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// DropAlias drop collection alias
func (c *GrpcClient) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.DropAlias(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// AlterAlias alter collection alias
func (c *GrpcClient) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.AlterAlias(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}
