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

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

var _ querypb.QueryNodeClient = &GrpcQueryNodeClient{}

type GrpcQueryNodeClient struct {
	Err error
}

func (m *GrpcQueryNodeClient) GetStatistics(ctx context.Context, in *querypb.GetStatisticsRequest, opts ...grpc.CallOption) (*internalpb.GetStatisticsResponse, error) {
	return &internalpb.GetStatisticsResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, m.Err
}

func (m *GrpcQueryNodeClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) Search(ctx context.Context, in *querypb.SearchRequest, opts ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return &internalpb.SearchResults{}, m.Err
}

func (m *GrpcQueryNodeClient) SearchSegments(ctx context.Context, in *querypb.SearchRequest, opts ...grpc.CallOption) (*internalpb.SearchResults, error) {
	return &internalpb.SearchResults{}, m.Err
}

func (m *GrpcQueryNodeClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, m.Err
}

func (m *GrpcQueryNodeClient) QueryStream(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
	return &streamrpc.LocalQueryClient{}, m.Err
}

func (m *GrpcQueryNodeClient) QuerySegments(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, m.Err
}

func (m *GrpcQueryNodeClient) QueryStreamSegments(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamSegmentsClient, error) {
	return &streamrpc.LocalQueryClient{}, m.Err
}

func (m *GrpcQueryNodeClient) SyncReplicaSegments(ctx context.Context, in *querypb.SyncReplicaSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) GetDataDistribution(ctx context.Context, in *querypb.GetDataDistributionRequest, opts ...grpc.CallOption) (*querypb.GetDataDistributionResponse, error) {
	return &querypb.GetDataDistributionResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) SyncDistribution(ctx context.Context, in *querypb.SyncDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) Delete(ctx context.Context, in *querypb.DeleteRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) DeleteBatch(ctx context.Context, in *querypb.DeleteBatchRequest, opts ...grpc.CallOption) (*querypb.DeleteBatchResponse, error) {
	return &querypb.DeleteBatchResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) UpdateSchema(ctx context.Context, in *querypb.UpdateSchemaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) Close() error {
	return m.Err
}
