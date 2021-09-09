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

package grpcdatacoordclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient datapb.DataCoordClient
	conn       *grpc.ClientConn

	sess *sessionutil.Session
	addr string
}

func getDataCoordAddress(sess *sessionutil.Session) (string, error) {
	key := typeutil.DataCoordRole
	msess, _, err := sess.GetSessions(key)
	if err != nil {
		log.Debug("DataCoordClient, getSessions failed", zap.Any("key", key), zap.Error(err))
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		log.Debug("DataCoordClient, not existed in msess ", zap.Any("key", key), zap.Any("len of msess", len(msess)))
		return "", fmt.Errorf("number of datacoord is incorrect, %d", len(msess))
	}
	return ms.Address, nil
}

func NewClient(ctx context.Context, metaRoot string, etcdEndpoints []string) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdEndpoints)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("DataCoordClient NewClient failed", zap.Error(err))
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
	Params.Init()
	return c.connect(retry.Attempts(20))
}

func (c *Client) connect(retryOptions ...retry.Option) error {
	var err error
	connectDataCoordFn := func() error {
		c.addr, err = getDataCoordAddress(c.sess)
		if err != nil {
			log.Debug("DataCoordClient getDataCoordAddr failed", zap.Error(err))
			return err
		}
		opts := trace.GetInterceptorOpts()
		log.Debug("DataCoordClient try reconnect ", zap.String("address", c.addr))
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

	err = retry.Do(c.ctx, connectDataCoordFn, retryOptions...)
	if err != nil {
		log.Debug("DataCoord try reconnect failed", zap.Error(err))
		return err
	}
	c.grpcClient = datapb.NewDataCoordClient(c.conn)
	return nil
}

func (c *Client) recall(caller func() (interface{}, error)) (interface{}, error) {
	ret, err := caller()
	if err == nil {
		return ret, nil
	}
	log.Debug("DataCoord Client grpc error", zap.Error(err))
	err = c.connect()
	if err != nil {
		return ret, errors.New("Connect to datacoord failed with error:\n" + err.Error())
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

// Register dumy
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

func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.Flush(ctx, req)
	})
	return ret.(*datapb.FlushResponse), err
}

func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AssignSegmentID(ctx, req)
	})
	return ret.(*datapb.AssignSegmentIDResponse), err
}

func (c *Client) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetSegmentStates(ctx, req)
	})
	return ret.(*datapb.GetSegmentStatesResponse), err
}

func (c *Client) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetInsertBinlogPaths(ctx, req)
	})
	return ret.(*datapb.GetInsertBinlogPathsResponse), err
}

func (c *Client) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetCollectionStatistics(ctx, req)
	})
	return ret.(*datapb.GetCollectionStatisticsResponse), err
}

func (c *Client) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetPartitionStatistics(ctx, req)
	})
	return ret.(*datapb.GetPartitionStatisticsResponse), err
}

func (c *Client) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	})
	return ret.(*milvuspb.StringResponse), err
}

func (c *Client) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetSegmentInfo(ctx, req)
	})
	return ret.(*datapb.GetSegmentInfoResponse), err
}

func (c *Client) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return c.grpcClient.SaveBinlogPaths(ctx, req)
}

func (c *Client) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetRecoveryInfo(ctx, req)
	})
	return ret.(*datapb.GetRecoveryInfoResponse), err
}

func (c *Client) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetFlushedSegments(ctx, req)
	})
	return ret.(*datapb.GetFlushedSegmentsResponse), err
}

func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetMetrics(ctx, req)
	})
	return ret.(*milvuspb.GetMetricsResponse), err
}
