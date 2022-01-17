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

type QueryCoordClient struct {
	Err error
}

func (m *QueryCoordClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.Err
}

func (m *QueryCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *QueryCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *QueryCoordClient) ShowCollections(ctx context.Context, in *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	return &querypb.ShowCollectionsResponse{}, m.Err
}

func (m *QueryCoordClient) ShowPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	return &querypb.ShowPartitionsResponse{}, m.Err
}

func (m *QueryCoordClient) LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *QueryCoordClient) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *QueryCoordClient) LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *QueryCoordClient) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *QueryCoordClient) CreateQueryChannel(ctx context.Context, in *querypb.CreateQueryChannelRequest, opts ...grpc.CallOption) (*querypb.CreateQueryChannelResponse, error) {
	return &querypb.CreateQueryChannelResponse{}, m.Err
}

func (m *QueryCoordClient) GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	return &querypb.GetPartitionStatesResponse{}, m.Err
}

func (m *QueryCoordClient) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{}, m.Err
}

func (m *QueryCoordClient) LoadBalance(ctx context.Context, in *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *QueryCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}
