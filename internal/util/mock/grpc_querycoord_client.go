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
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

// Check if GrpcQueryCoordClient implements proto.GrpcQueryCoordClient
var _ querypb.QueryCoordClient = &GrpcQueryCoordClient{}

type GrpcQueryCoordClient struct {
	Err error
}

func (m *GrpcQueryCoordClient) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	return &milvuspb.CheckHealthResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, m.Err
}

func (m *GrpcQueryCoordClient) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) ShowLoadCollections(ctx context.Context, in *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	return &querypb.ShowCollectionsResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) ShowLoadPartitions(ctx context.Context, in *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	return &querypb.ShowPartitionsResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) LoadPartitions(ctx context.Context, in *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) LoadCollection(ctx context.Context, in *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) GetPartitionStates(ctx context.Context, in *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	return &querypb.GetPartitionStatesResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetLoadSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	return &querypb.GetSegmentInfoResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) LoadBalance(ctx context.Context, in *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetReplicas(ctx context.Context, in *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	return &milvuspb.GetReplicasResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetShardLeaders(ctx context.Context, in *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
	return &querypb.GetShardLeadersResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error) {
	return &milvuspb.ListResourceGroupsResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error) {
	return &querypb.DescribeResourceGroupResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) ListCheckers(ctx context.Context, in *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error) {
	return &querypb.ListCheckersResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) ActivateChecker(ctx context.Context, in *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) DeactivateChecker(ctx context.Context, in *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error) {
	return &querypb.ListQueryNodeResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error) {
	return &querypb.GetQueryNodeDistributionResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest, opts ...grpc.CallOption) (*querypb.CheckBalanceStatusResponse, error) {
	return &querypb.CheckBalanceStatusResponse{}, m.Err
}

func (m *GrpcQueryCoordClient) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}

func (m *GrpcQueryCoordClient) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{}, m.Err
}
