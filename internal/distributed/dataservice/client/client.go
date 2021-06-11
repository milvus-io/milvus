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

package grpcdataserviceclient

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type Client struct {
	grpcClient datapb.DataServiceClient
	conn       *grpc.ClientConn
	ctx        context.Context
	addr       string

	sess *sessionutil.Session

	timeout   time.Duration
	recallTry int
	reconnTry int
}

func getDataServiceAddress(sess *sessionutil.Session) (string, error) {
	key := typeutil.DataServiceRole
	msess, _, err := sess.GetSessions(key)
	if err != nil {
		log.Debug("DataServiceClient, getSessions failed", zap.Any("key", key), zap.Error(err))
		return "", err
	}
	ms, ok := msess[key]
	if !ok {
		log.Debug("DataServiceClient, not existed in msess ", zap.Any("key", key), zap.Any("len of msess", len(msess)))
		return "", fmt.Errorf("number of dataservice is incorrect, %d", len(msess))
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
	getDataServiceAddressFn := func() error {
		c.addr, err = getDataServiceAddress(c.sess)
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(c.reconnTry, 3*time.Second, getDataServiceAddressFn)
	if err != nil {
		log.Debug("DataServiceClient try reconnect getDataServiceAddressFn failed", zap.Error(err))
		return err
	}
	connectGrpcFunc := func() error {
		ctx, cancelFunc := context.WithTimeout(c.ctx, c.timeout)
		defer cancelFunc()
		log.Debug("DataServiceClient try reconnect ", zap.String("address", c.addr))
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
		log.Debug("DataService try reconnect failed", zap.Error(err))
		return err
	}
	c.grpcClient = datapb.NewDataServiceClient(c.conn)
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

func (c *Client) RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.RegisterNode(ctx, req)
	})
	return ret.(*datapb.RegisterNodeResponse), err
}

func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.Flush(ctx, req)
	})
	return ret.(*commonpb.Status), err
}

func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.AssignSegmentID(ctx, req)
	})
	return ret.(*datapb.AssignSegmentIDResponse), err
}

func (c *Client) ShowSegments(ctx context.Context, req *datapb.ShowSegmentsRequest) (*datapb.ShowSegmentsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.ShowSegments(ctx, req)
	})
	return ret.(*datapb.ShowSegmentsResponse), err
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

func (c *Client) GetInsertChannels(ctx context.Context, req *datapb.GetInsertChannelsRequest) (*internalpb.StringList, error) {
	ret, err := c.recall(func() (interface{}, error) {
		return c.grpcClient.GetInsertChannels(ctx, req)
	})
	return ret.(*internalpb.StringList), err
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
