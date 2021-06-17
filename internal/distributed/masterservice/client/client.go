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
	"errors"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GrpcClient grpc client
type GrpcClient struct {
	grpcClient masterpb.MasterServiceClient
	conn       *grpc.ClientConn
	ctx        context.Context

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
		log.Debug("MasterServiceClient GetSessions failed", zap.Any("key", key))
		return "", err
	}
	log.Debug("MasterServiceClient GetSessions success")
	ms, ok := msess[key]
	if !ok {
		log.Debug("MasterServiceClient mess key not exist", zap.Any("key", key))
		return "", fmt.Errorf("number of master service is incorrect, %d", len(msess))
	}
	return ms.Address, nil
}

// NewClient create master service client with specified ectd info and timeout
// ctx execution control context
// metaRoot is the path in etcd for master registration
// etcdEndpoints are the address list for etcd end points
// timeout is default setting for each grpc call
func NewClient(ctx context.Context, metaRoot string, etcdEndpoints []string, timeout time.Duration) (*GrpcClient, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdEndpoints)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("MasterServiceClient NewClient failed", zap.Error(err))
		return nil, err
	}

	return &GrpcClient{
		grpcClient: nil,
		conn:       nil,
		ctx:        ctx,
		timeout:    timeout,
		reconnTry:  300,
		recallTry:  3,
		sess:       sess,
	}, nil
}

func (c *GrpcClient) connect() error {
	tracer := opentracing.GlobalTracer()
	var err error
	getMasterServiceAddrFn := func() error {
		ch := make(chan struct{}, 1)
		var err error
		go func() {
			c.addr, err = getMasterServiceAddr(c.sess)
			ch <- struct{}{}
		}()
		select {
		case <-c.ctx.Done():
			return retry.NoRetryError(errors.New("context canceled"))
		case <-ch:
		}
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(c.reconnTry, 3*time.Second, getMasterServiceAddrFn)
	if err != nil {
		log.Debug("MasterServiceClient getMasterServiceAddr failed", zap.Error(err))
		return err
	}
	connectGrpcFunc := func() error {
		log.Debug("MasterServiceClient try reconnect ", zap.String("address", c.addr))
		ctx, cancelFunc := context.WithTimeout(c.ctx, c.timeout)
		defer cancelFunc()
		var conn *grpc.ClientConn
		var err error
		ch := make(chan struct{}, 1)
		go func() {
			conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
				grpc.WithUnaryInterceptor(
					otgrpc.OpenTracingClientInterceptor(tracer)),
				grpc.WithStreamInterceptor(
					otgrpc.OpenTracingStreamClientInterceptor(tracer)))
			ch <- struct{}{}
		}()
		select {
		case <-c.ctx.Done():
			return retry.NoRetryError(errors.New("context canceled"))
		case <-ch:
		}
		if err == nil {
			c.conn = conn
		}
		return err
	}

	err = retry.Retry(c.reconnTry, 500*time.Millisecond, connectGrpcFunc)
	if err != nil {
		log.Debug("MasterServiceClient try reconnect failed", zap.Error(err))
		return err
	}
	log.Debug("MasterServiceClient try reconnect success")
	c.grpcClient = masterpb.NewMasterServiceClient(c.conn)
	return nil
}

func (c *GrpcClient) Init() error {
	// for now, we must try many times in Init Stage
	initFunc := func() error {
		return c.connect()
	}
	err := retry.Retry(10000, 3*time.Second, initFunc)
	return err
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
	ch := make(chan struct{}, 1)
	var ret interface{}
	var err error
	go func() {
		ret, err = caller()
		if err == nil {
			ch <- struct{}{}
			return
		}
		for i := 0; i < c.recallTry; i++ {
			err = c.connect()
			if err == nil {
				ret, err = caller()
				if err == nil {
					ch <- struct{}{}
					return
				}
			}
		}
		ch <- struct{}{}
	}()
	select {
	case <-c.ctx.Done():
		return nil, errors.New("context canceled")
	case <-ch:
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
