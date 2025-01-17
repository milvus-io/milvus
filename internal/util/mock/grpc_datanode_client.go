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
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
)

var _ datapb.DataNodeClient = &GrpcDataNodeClient{}

type GrpcDataNodeClient struct {
	Err error
}

func (m *GrpcDataNodeClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, m.Err
}

func (m *GrpcDataNodeClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcDataNodeClient) WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) FlushSegments(ctx context.Context, in *datapb.FlushSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{}, m.Err
}

func (m *GrpcDataNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcDataNodeClient) CompactionV2(ctx context.Context, req *datapb.CompactionPlan, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) GetCompactionState(ctx context.Context, in *datapb.CompactionStateRequest, opts ...grpc.CallOption) (*datapb.CompactionStateResponse, error) {
	return &datapb.CompactionStateResponse{}, m.Err
}

func (m *GrpcDataNodeClient) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest, opts ...grpc.CallOption) (*datapb.ResendSegmentStatsResponse, error) {
	return &datapb.ResendSegmentStatsResponse{}, m.Err
}

func (m *GrpcDataNodeClient) SyncSegments(ctx context.Context, in *datapb.SyncSegmentsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) FlushChannels(ctx context.Context, in *datapb.FlushChannelsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) NotifyChannelOperation(ctx context.Context, in *datapb.ChannelOperationsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) CheckChannelOperationProgress(ctx context.Context, req *datapb.ChannelWatchInfo, opts ...grpc.CallOption) (*datapb.ChannelOperationProgressResponse, error) {
	return &datapb.ChannelOperationProgressResponse{}, m.Err
}

func (m *GrpcDataNodeClient) PreImport(ctx context.Context, req *datapb.PreImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) ImportV2(ctx context.Context, req *datapb.ImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) QueryPreImport(ctx context.Context, req *datapb.QueryPreImportRequest, opts ...grpc.CallOption) (*datapb.QueryPreImportResponse, error) {
	return &datapb.QueryPreImportResponse{}, m.Err
}

func (m *GrpcDataNodeClient) QueryImport(ctx context.Context, req *datapb.QueryImportRequest, opts ...grpc.CallOption) (*datapb.QueryImportResponse, error) {
	return &datapb.QueryImportResponse{}, m.Err
}

func (m *GrpcDataNodeClient) DropImport(ctx context.Context, req *datapb.DropImportRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) QuerySlot(ctx context.Context, req *datapb.QuerySlotRequest, opts ...grpc.CallOption) (*datapb.QuerySlotResponse, error) {
	return &datapb.QuerySlotResponse{}, m.Err
}

func (m *GrpcDataNodeClient) DropCompactionPlan(ctx context.Context, req *datapb.DropCompactionPlanRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}
