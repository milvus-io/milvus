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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

var _ datapb.DataNodeClient = &GrpcDataNodeClient{}

type GrpcDataNodeClient struct {
	Err error
}

func (m *GrpcDataNodeClient) GetComponentStates(ctx context.Context, in *internalpb.GetComponentStatesRequest, opts ...grpc.CallOption) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{}, m.Err
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

func (m *GrpcDataNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcDataNodeClient) Compaction(ctx context.Context, req *datapb.CompactionPlan, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) GetCompactionState(ctx context.Context, in *datapb.CompactionStateRequest, opts ...grpc.CallOption) (*datapb.CompactionStateResponse, error) {
	return &datapb.CompactionStateResponse{}, m.Err
}

func (m *GrpcDataNodeClient) Import(ctx context.Context, req *datapb.ImportTaskRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcDataNodeClient) ResendSegmentStats(ctx context.Context, req *datapb.ResendSegmentStatsRequest, opts ...grpc.CallOption) (*datapb.ResendSegmentStatsResponse, error) {
	return &datapb.ResendSegmentStatsResponse{}, m.Err
}

func (m *GrpcDataNodeClient) AddSegment(ctx context.Context, in *datapb.AddSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}
