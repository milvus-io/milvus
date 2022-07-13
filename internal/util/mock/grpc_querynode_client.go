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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

var _ querypb.QueryNodeClient = &GrpcQueryNodeClient{}

type GrpcQueryNodeClient struct {
	Err error
}

func (m *GrpcQueryNodeClient) GetStatistics(ctx context.Context, in *querypb.GetStatisticsRequest, opts ...grpc.CallOption) (*internalpb.GetStatisticsResponse, error) {
	return &internalpb.GetStatisticsResponse{}, m.Err
}

func (m *GrpcQueryNodeClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.Err
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

func (m *GrpcQueryNodeClient) WatchDeltaChannels(ctx context.Context, in *querypb.WatchDeltaChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
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

func (m *GrpcQueryNodeClient) Query(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
	return &internalpb.RetrieveResults{}, m.Err
}

func (m *GrpcQueryNodeClient) SyncReplicaSegments(ctx context.Context, in *querypb.SyncReplicaSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}
