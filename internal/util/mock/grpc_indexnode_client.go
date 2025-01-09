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
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
)

var _ indexpb.IndexNodeClient = &GrpcIndexNodeClient{}

type GrpcIndexNodeClient struct {
	Err error
}

func (m *GrpcIndexNodeClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, m.Err
}

//func (m *GrpcIndexNodeClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
//	return &milvuspb.StringResponse{}, m.Err
//}

func (m *GrpcIndexNodeClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcIndexNodeClient) CreateJob(ctx context.Context, in *indexpb.CreateJobRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcIndexNodeClient) QueryJobs(ctx context.Context, in *indexpb.QueryJobsRequest, opts ...grpc.CallOption) (*indexpb.QueryJobsResponse, error) {
	return &indexpb.QueryJobsResponse{}, m.Err
}

func (m *GrpcIndexNodeClient) DropJobs(ctx context.Context, in *indexpb.DropJobsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcIndexNodeClient) GetJobStats(ctx context.Context, in *indexpb.GetJobStatsRequest, opts ...grpc.CallOption) (*indexpb.GetJobStatsResponse, error) {
	return &indexpb.GetJobStatsResponse{}, m.Err
}

func (m *GrpcIndexNodeClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcIndexNodeClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{}, m.Err
}

func (m *GrpcIndexNodeClient) CreateJobV2(ctx context.Context, in *indexpb.CreateJobV2Request, opt ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcIndexNodeClient) QueryJobsV2(ctx context.Context, in *indexpb.QueryJobsV2Request, opt ...grpc.CallOption) (*indexpb.QueryJobsV2Response, error) {
	return &indexpb.QueryJobsV2Response{}, m.Err
}

func (m *GrpcIndexNodeClient) DropJobsV2(ctx context.Context, in *indexpb.DropJobsV2Request, opt ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcIndexNodeClient) Close() error {
	return m.Err
}
