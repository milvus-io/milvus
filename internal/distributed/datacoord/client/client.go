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

package grpcdatacoordclient

import (
	"context"
	"fmt"
	"sync"
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

// Client is the datacoord grpc client
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient    datapb.DataCoordClient
	conn          *grpc.ClientConn
	grpcClientMtx sync.RWMutex

	sess *sessionutil.Session
	addr string

	getGrpcClient func() (datapb.DataCoordClient, error)
}

func (c *Client) setGetGrpcClientFunc() {
	c.getGrpcClient = c.getGrpcClientFunc
}

func (c *Client) getGrpcClientFunc() (datapb.DataCoordClient, error) {
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

func (c *Client) resetConnection() {
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.grpcClient = nil
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

// NewClient creates a new client instance
func NewClient(ctx context.Context, metaRoot string, etcdEndpoints []string) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdEndpoints)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("DataCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	client := &Client{
		ctx:    ctx,
		cancel: cancel,
		sess:   sess,
	}

	client.setGetGrpcClientFunc()
	return client, nil
}

// Init initializes the client
func (c *Client) Init() error {
	Params.Init()
	return nil
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
		if c.conn != nil {
			_ = c.conn.Close()
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

	c.resetConnection()

	ret, err = caller()
	if err == nil {
		return ret, nil
	}
	return ret, err
}

// Start enables the client
func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	c.cancel()
	c.grpcClientMtx.Lock()
	defer c.grpcClientMtx.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Register dumy
func (c *Client) Register() error {
	return nil
}

func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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

func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
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

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
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

func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.Flush(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.FlushResponse), err
}

// AssignSegmentID applies allocations for specified Coolection/Partition and related Channel Name(Virtial Channel)
//
// ctx is the context to control request deadline and cancellation
// req contains the requester's info(id and role) and the list of Assignment Request,
// which coontains the specified collection, partitaion id, the related VChannel Name and row count it needs
//
// response struct `AssignSegmentIDResponse` contains the the assignment result for each request
// error is returned only when some communication issue occurs
// if some error occurs in the process of `AssignSegmentID`, it will be recorded and returned in `Status` field of response
//
// `AssignSegmentID` will applies current configured allocation policies for each request
// if the VChannel is newly used, `WatchDmlChannels` will be invoked to notify a `DataNode`(selected by policy) to watch it
// if there is anything make the allocation impossible, the response will not contain the corresponding result
func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.AssignSegmentID(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.AssignSegmentIDResponse), err
}

// GetSegmentStates requests segment state information
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment id to query
//
// response struct `GetSegmentStatesResponse` contains the list of each state query result
// 	when the segment is not found, the state entry will has the field `Status`  to identify failure
// 	otherwise the Segment State and Start position information will be returned
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetSegmentStates(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetSegmentStatesResponse), err
}

// GetInsertBinlogPaths requests binlog paths for specified segment
//
// ctx is the context to control request deadline and cancellation
// req contains the segment id to query
//
// response struct `GetInsertBinlogPathsResponse` contains the fields list
// 	and corresponding binlog path list
// error is returned only when some communication issue occurs
func (c *Client) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetInsertBinlogPaths(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetInsertBinlogPathsResponse), err
}

// GetCollectionStatistics requests collection statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection id to query
//
// response struct `GetCollectionStatisticsResponse` contains the key-value list fields returning related data
// 	only row count for now
// error is returned only when some communication issue occurs
func (c *Client) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetCollectionStatistics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetCollectionStatisticsResponse), err
}

// GetPartitionStatistics requests partition statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection and partition id to query
//
// response struct `GetPartitionStatisticsResponse` contains the key-value list fields returning related data
// 	only row count for now
// error is returned only when some communication issue occurs
func (c *Client) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetPartitionStatistics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetPartitionStatisticsResponse), err
}

// GetSegmentInfoChannel DEPRECATED
// legacy api to get SegmentInfo Channel name
func (c *Client) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// GetSegmentInfo requests segment info
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment ids to query
//
// response struct `GetSegmentInfoResponse` contains the list of segment info
// error is returned only when some communication issue occurs
func (c *Client) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetSegmentInfo(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetSegmentInfoResponse), err
}

// SaveBinlogPaths updates segments binlogs(including insert binlogs, stats logs and delta logs)
//  and related message stream positions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response status contains the status/error code and failing reason if any
// error is returned only when some communication issue occurs
//
// there is a constraint that the `SaveBinlogPaths` requests of same segment shall be passed in sequence
// 	the root reason is each `SaveBinlogPaths` will overwrite the checkpoint position
//  if the constraint is broken, the checkpoint position will not be monotonically increasing and the integrity will be compromised
func (c *Client) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	// FIXME(dragondriver): why not to recall here?
	client, err := c.getGrpcClient()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return client.SaveBinlogPaths(ctx, req)
}

// GetRecoveryInfo request segment recovery info of collection/partition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *Client) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetRecoveryInfo(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetRecoveryInfoResponse), err
}

// GetFlushedSegments returns flushed segment list of requested collection/parition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//  when partition is lesser or equal to 0, all flushed segments of collection will be returned
//
// response struct `GetFlushedSegmentsResponse` contains flushed segment id list
// error is returned only when some communication issue occurs
func (c *Client) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetFlushedSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetFlushedSegmentsResponse), err
}

// GetMetrics gets all metrics of datacoord
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}

func (c *Client) CompleteCompaction(ctx context.Context, req *datapb.CompactionResult) (*commonpb.Status, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.CompleteCompaction(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

func (c *Client) ManualCompaction(ctx context.Context, req *datapb.ManualCompactionRequest) (*datapb.ManualCompactionResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.ManualCompaction(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.ManualCompactionResponse), err
}

func (c *Client) GetCompactionState(ctx context.Context, req *datapb.GetCompactionStateRequest) (*datapb.GetCompactionStateResponse, error) {
	ret, err := c.recall(func() (interface{}, error) {
		client, err := c.getGrpcClient()
		if err != nil {
			return nil, err
		}

		return client.GetCompactionState(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetCompactionStateResponse), err
}
