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

package mock

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
)

var _ types.QueryNode = &QueryNodeClient{}

type QueryNodeClient struct {
	grpcClient *GrpcQueryNodeClient
	Err        error
}

func (q QueryNodeClient) Init() error {
	return nil
}

func (q QueryNodeClient) Start() error {
	return nil
}

func (q QueryNodeClient) Stop() error {
	return nil
}

func (q QueryNodeClient) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return q.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
}

func (q QueryNodeClient) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return q.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
}

func (q QueryNodeClient) Register() error {
	return nil
}

func (q QueryNodeClient) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return q.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
}

func (q QueryNodeClient) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return q.grpcClient.WatchDmChannels(ctx, req)
}

func (q QueryNodeClient) WatchDeltaChannels(ctx context.Context, req *querypb.WatchDeltaChannelsRequest) (*commonpb.Status, error) {
	return q.grpcClient.WatchDeltaChannels(ctx, req)
}

func (q QueryNodeClient) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	return q.grpcClient.LoadSegments(ctx, req)
}

func (q QueryNodeClient) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return q.grpcClient.ReleaseCollection(ctx, req)
}

func (q QueryNodeClient) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return q.grpcClient.ReleasePartitions(ctx, req)
}

func (q QueryNodeClient) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	return q.grpcClient.ReleaseSegments(ctx, req)
}

func (q QueryNodeClient) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return q.grpcClient.GetSegmentInfo(ctx, req)
}

func (q QueryNodeClient) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	return q.grpcClient.Search(ctx, req)
}

func (q QueryNodeClient) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return q.grpcClient.Query(ctx, req)
}

func (q QueryNodeClient) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return q.grpcClient.SyncReplicaSegments(ctx, req)
}

func (q QueryNodeClient) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return q.grpcClient.GetMetrics(ctx, req)
}
