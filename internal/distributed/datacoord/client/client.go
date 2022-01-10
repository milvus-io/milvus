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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ClientParams is the parameters of client singleton
var ClientParams paramtable.GrpcClientConfig

// Client is the datacoord grpc client
type Client struct {
	grpcClient grpcclient.GrpcClient
	sess       *sessionutil.Session
}

// NewClient creates a new client instance
func NewClient(ctx context.Context, metaRoot string, etcdCli *clientv3.Client) (*Client, error) {
	sess := sessionutil.NewSession(ctx, metaRoot, etcdCli)
	if sess == nil {
		err := fmt.Errorf("new session error, maybe can not connect to etcd")
		log.Debug("DataCoordClient NewClient failed", zap.Error(err))
		return nil, err
	}
	ClientParams.InitOnce(typeutil.DataCoordRole)
	client := &Client{
		grpcClient: &grpcclient.ClientBase{
			ClientMaxRecvSize: ClientParams.ClientMaxRecvSize,
			ClientMaxSendSize: ClientParams.ClientMaxSendSize,
		},
		sess: sess,
	}
	client.grpcClient.SetRole(typeutil.DataCoordRole)
	client.grpcClient.SetGetAddrFunc(client.getDataCoordAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client, nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) interface{} {
	return datapb.NewDataCoordClient(cc)
}

func (c *Client) getDataCoordAddr() (string, error) {
	key := c.grpcClient.GetRole()
	msess, _, err := c.sess.GetSessions(key)
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

// Init initializes the client
func (c *Client) Init() error {
	return nil
}

// Start enables the client
func (c *Client) Start() error {
	return nil
}

// Stop stops the client
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

// GetComponentStates calls DataCoord GetComponentStates services
func (c *Client) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*internalpb.ComponentStates), err
}

// GetTimeTickChannel return the name of time tick channel.
func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// GetStatisticsChannel return the name of statistics channel.
func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.Call(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// Flush flushes a collection's data
func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).Flush(ctx, req)
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).AssignSegmentID(ctx, req)
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetSegmentStates(ctx, req)
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetInsertBinlogPaths(ctx, req)
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetCollectionStatistics(ctx, req)
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetPartitionStatistics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetPartitionStatisticsResponse), err
}

// GetSegmentInfoChannel DEPRECATED
// legacy api to get SegmentInfo Channel name
func (c *Client) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetSegmentInfoChannel(ctx, &datapb.GetSegmentInfoChannelRequest{})
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetSegmentInfo(ctx, req)
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
	// use Call here on purpose
	ret, err := c.grpcClient.Call(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).SaveBinlogPaths(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetRecoveryInfo request segment recovery info of collection/partition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *Client) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetRecoveryInfo(ctx, req)
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
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetFlushedSegments(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.GetFlushedSegmentsResponse), err
}

// GetMetrics gets all metrics of datacoord
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}

// CompleteCompaction completes a compaction with the result
func (c *Client) CompleteCompaction(ctx context.Context, req *datapb.CompactionResult) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).CompleteCompaction(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// ManualCompaction triggers a compaction for a collection
func (c *Client) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).ManualCompaction(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ManualCompactionResponse), err
}

// GetCompactionState gets the state of a compaction
func (c *Client) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetCompactionState(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetCompactionStateResponse), err
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (c *Client) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetCompactionStateWithPlans(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetCompactionPlansResponse), err
}

// WatchChannels notifies DataCoord to watch vchannels of a collection
func (c *Client) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).WatchChannels(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.WatchChannelsResponse), err
}

// GetFlushState gets the flush state of multiple segments
func (c *Client) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).GetFlushState(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetFlushStateResponse), err
}

// DropVirtualChannel drops virtual channel in datacoord.
func (c *Client) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(datapb.DataCoordClient).DropVirtualChannel(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*datapb.DropVirtualChannelResponse), err
}
