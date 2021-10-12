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
package querycoord

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type queryNodeClientMock struct {
	ctx    context.Context
	cancel context.CancelFunc

	grpcClient querypb.QueryNodeClient
	conn       *grpc.ClientConn

	addr string
}

func newQueryNodeTest(ctx context.Context, address string, id UniqueID, kv *etcdkv.EtcdKV) (Node, error) {
	collectionInfo := make(map[UniqueID]*querypb.CollectionInfo)
	watchedChannels := make(map[UniqueID]*querypb.QueryChannelInfo)
	childCtx, cancel := context.WithCancel(ctx)
	client, err := newQueryNodeClientMock(childCtx, address)
	if err != nil {
		cancel()
		return nil, err
	}
	node := &queryNode{
		ctx:                  childCtx,
		cancel:               cancel,
		id:                   id,
		address:              address,
		client:               client,
		kvClient:             kv,
		collectionInfos:      collectionInfo,
		watchedQueryChannels: watchedChannels,
	}

	return node, nil
}

func newQueryNodeClientMock(ctx context.Context, addr string) (*queryNodeClientMock, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr is empty")
	}
	ctx, cancel := context.WithCancel(ctx)
	return &queryNodeClientMock{
		ctx:    ctx,
		cancel: cancel,
		addr:   addr,
	}, nil
}

func (client *queryNodeClientMock) Init() error {
	ctx, cancel := context.WithTimeout(client.ctx, time.Second*2)
	defer cancel()
	conn, err := grpc.DialContext(ctx, client.addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	client.conn = conn
	log.Debug("QueryNodeClient try connect success")
	client.grpcClient = querypb.NewQueryNodeClient(conn)
	return nil
}

func (client *queryNodeClientMock) Start() error {
	return nil
}

func (client *queryNodeClientMock) Stop() error {
	client.cancel()
	if client.conn != nil {
		return client.conn.Close()
	}
	return nil
}

func (client *queryNodeClientMock) Register() error {
	return nil
}

func (client *queryNodeClientMock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return client.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
}

func (client *queryNodeClientMock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return client.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
}

func (client *queryNodeClientMock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return client.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
}

func (client *queryNodeClientMock) AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	return client.grpcClient.AddQueryChannel(ctx, req)
}

func (client *queryNodeClientMock) RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	return client.grpcClient.RemoveQueryChannel(ctx, req)
}

func (client *queryNodeClientMock) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return client.grpcClient.WatchDmChannels(ctx, req)
}

func (client *queryNodeClientMock) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	return client.grpcClient.LoadSegments(ctx, req)
}

func (client *queryNodeClientMock) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return client.grpcClient.ReleaseCollection(ctx, req)
}

func (client *queryNodeClientMock) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return client.grpcClient.ReleasePartitions(ctx, req)
}

func (client *queryNodeClientMock) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	return client.grpcClient.ReleaseSegments(ctx, req)
}

func (client *queryNodeClientMock) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return client.grpcClient.GetSegmentInfo(ctx, req)
}

func (client *queryNodeClientMock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return client.grpcClient.GetMetrics(ctx, req)
}
