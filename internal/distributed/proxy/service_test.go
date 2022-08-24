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

package grpcproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	milvusmock "github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/health/grpc_health_v1"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockBase struct {
	mock.Mock
	isMockGetComponentStatesOn bool
}

func (m *MockBase) On(methodName string, arguments ...interface{}) *mock.Call {
	if methodName == "GetComponentStates" {
		m.isMockGetComponentStatesOn = true
	}
	return m.Mock.On(methodName, arguments...)
}

func (m *MockBase) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	if m.isMockGetComponentStatesOn {
		ret1 := &internalpb.ComponentStates{}
		var ret2 error
		args := m.Called(ctx)
		arg1 := args.Get(0)
		arg2 := args.Get(1)
		if arg1 != nil {
			ret1 = arg1.(*internalpb.ComponentStates)
		}
		if arg2 != nil {
			ret2 = arg2.(error)
		}
		return ret1, ret2
	}
	return &internalpb.ComponentStates{
		State:  &internalpb.ComponentInfo{StateCode: internalpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil
}

func (m *MockBase) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockBase) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockRootCoord struct {
	MockBase
	initErr  error
	startErr error
	regErr   error
	stopErr  error
}

func (m *MockRootCoord) Init() error {
	return m.initErr
}

func (m *MockRootCoord) Start() error {
	return m.startErr
}

func (m *MockRootCoord) Stop() error {
	return m.stopErr
}

func (m *MockRootCoord) Register() error {
	return m.regErr
}

func (m *MockRootCoord) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ReportImport(ctx context.Context, req *rootcoordpb.ImportResult) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockRootCoord) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return nil, nil
}

func (m *MockRootCoord) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockIndexCoord struct {
	MockBase
	initErr  error
	startErr error
	regErr   error
	stopErr  error
}

func (m *MockIndexCoord) Init() error {
	return m.initErr
}

func (m *MockIndexCoord) Start() error {
	return m.startErr
}

func (m *MockIndexCoord) Stop() error {
	return m.stopErr
}

func (m *MockIndexCoord) Register() error {
	return m.regErr
}

func (m *MockIndexCoord) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockIndexCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return nil, nil
}

func (m *MockIndexCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockQueryCoord struct {
	MockBase
	initErr  error
	startErr error
	stopErr  error
	regErr   error
}

func (m *MockQueryCoord) Init() error {
	return m.initErr
}

func (m *MockQueryCoord) Start() error {
	return m.startErr
}

func (m *MockQueryCoord) Stop() error {
	return m.stopErr
}

func (m *MockQueryCoord) Register() error {
	return m.regErr
}

func (m *MockQueryCoord) UpdateStateCode(code internalpb.StateCode) {
}

func (m *MockQueryCoord) SetRootCoord(types.RootCoord) error {
	return nil
}

func (m *MockQueryCoord) SetDataCoord(types.DataCoord) error {
	return nil
}

func (m *MockQueryCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
			Role:      "MockQueryCoord",
			StateCode: internalpb.StateCode_Healthy,
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil
}

func (m *MockQueryCoord) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	return nil, nil
}

func (m *MockQueryCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockDataCoord struct {
	MockBase
	err      error
	initErr  error
	startErr error
	stopErr  error
	regErr   error
}

func (m *MockDataCoord) Init() error {
	return m.initErr
}

func (m *MockDataCoord) Start() error {
	return m.startErr
}

func (m *MockDataCoord) Stop() error {
	return m.stopErr
}

func (m *MockDataCoord) Register() error {
	return m.regErr
}

func (m *MockDataCoord) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) AddSegment(ctx context.Context, req *datapb.AddSegmentRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockDataCoord) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockDataCoord) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) CompleteCompaction(ctx context.Context, req *datapb.CompactionResult) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockDataCoord) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	return &datapb.DropVirtualChannelResponse{}, nil
}

func (m *MockDataCoord) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	return &datapb.SetSegmentStateResponse{}, nil
}

func (m *MockDataCoord) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	return nil, nil
}

func (m *MockDataCoord) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockDataCoord) AcquireSegmentLock(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockDataCoord) ReleaseSegmentLock(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type MockProxy struct {
	MockBase
	err      error
	initErr  error
	startErr error
	stopErr  error
	regErr   error
	isMockOn bool
}

func (m *MockProxy) Init() error {
	return m.initErr
}

func (m *MockProxy) Start() error {
	return m.startErr
}

func (m *MockProxy) Stop() error {
	return m.stopErr
}

func (m *MockProxy) Register() error {
	return m.regErr
}

func (m *MockProxy) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockProxy) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockProxy) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return nil, nil
}

func (m *MockProxy) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return nil, nil
}

func (m *MockProxy) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	return nil, nil
}

func (m *MockProxy) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return nil, nil
}

func (m *MockProxy) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetIndexBuildProgress(ctx context.Context, request *milvuspb.GetIndexBuildProgressRequest) (*milvuspb.GetIndexBuildProgressResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	return nil, nil
}

func (m *MockProxy) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
	return nil, nil
}

func (m *MockProxy) Delete(ctx context.Context, request *milvuspb.DeleteRequest) (*milvuspb.MutationResult, error) {
	return nil, nil
}

func (m *MockProxy) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	return nil, nil
}

func (m *MockProxy) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
	return nil, nil
}

func (m *MockProxy) Query(ctx context.Context, request *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
	return nil, nil
}

func (m *MockProxy) CalcDistance(ctx context.Context, request *milvuspb.CalcDistanceRequest) (*milvuspb.CalcDistanceResults, error) {
	return nil, nil
}

func (m *MockProxy) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetPersistentSegmentInfo(ctx context.Context, request *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetQuerySegmentInfo(ctx context.Context, request *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	return nil, nil
}

func (m *MockProxy) Dummy(ctx context.Context, request *milvuspb.DummyRequest) (*milvuspb.DummyResponse, error) {
	return nil, nil
}

func (m *MockProxy) RegisterLink(ctx context.Context, request *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *MockProxy) LoadBalance(ctx context.Context, request *milvuspb.LoadBalanceRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) SetRates(ctx context.Context, request *proxypb.SetRatesRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) GetProxyMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return nil, nil
}

func (m *MockProxy) SetRootCoordClient(rootCoord types.RootCoord) {

}

func (m *MockProxy) SetDataCoordClient(dataCoord types.DataCoord) {

}

func (m *MockProxy) SetIndexCoordClient(indexCoord types.IndexCoord) {

}

func (m *MockProxy) SetQueryCoordClient(queryCoord types.QueryCoord) {

}

func (m *MockProxy) GetRateLimiter() (types.Limiter, error) {
	return nil, nil
}

func (m *MockProxy) UpdateStateCode(stateCode internalpb.StateCode) {

}

func (m *MockProxy) SetEtcdClient(etcdClient *clientv3.Client) {
}

func (m *MockProxy) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return nil, nil
}

func (m *MockProxy) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return nil, nil
}

func (m *MockProxy) Import(ctx context.Context, req *milvuspb.ImportRequest) (*milvuspb.ImportResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetImportState(ctx context.Context, req *milvuspb.GetImportStateRequest) (*milvuspb.GetImportStateResponse, error) {
	return nil, nil
}

func (m *MockProxy) ListImportTasks(ctx context.Context, in *milvuspb.ListImportTasksRequest) (*milvuspb.ListImportTasksResponse, error) {
	return nil, nil
}

func (m *MockProxy) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return nil, nil
}

func (m *MockProxy) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) CreateCredential(ctx context.Context, req *milvuspb.CreateCredentialRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) UpdateCredential(ctx context.Context, req *milvuspb.UpdateCredentialRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return nil, nil
}

func (m *MockProxy) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return nil, nil
}

func (m *MockProxy) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return nil, nil
}

func (m *MockProxy) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *MockProxy) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return nil, nil
}

func (m *MockProxy) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type WaitOption struct {
	Duration      time.Duration `json:"duration"`
	Port          int           `json:"port"`
	TLSMode       int           `json:"tls_mode"`
	ClientPemPath string        `json:"client_pem_path"`
	ClientKeyPath string        `json:"client_key_path"`
	CaPath        string        `json:"ca_path"`
}

func (opt *WaitOption) String() string {
	s, err := json.Marshal(*opt)
	if err != nil {
		return fmt.Sprintf("error: %s", err)
	}
	return string(s)
}

func newWaitOption(duration time.Duration, Port int, tlsMode int, clientPemPath, clientKeyPath, clientCaPath string) *WaitOption {
	return &WaitOption{
		Duration:      duration,
		Port:          Port,
		TLSMode:       tlsMode,
		ClientPemPath: clientPemPath,
		ClientKeyPath: clientKeyPath,
		CaPath:        clientCaPath,
	}
}

func withCredential(clientPemPath, clientKeyPath, clientCaPath string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(clientPemPath, clientKeyPath)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(clientCaPath)
	if err != nil {
		return nil, err
	}
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to AppendCertsFromPEM")
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   "localhost",
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS13,
	})
	return creds, nil
}

// waitForGrpcReady block until service available or panic after times out.
func waitForGrpcReady(opt *WaitOption) {
	ticker := time.NewTicker(opt.Duration)
	ch := make(chan error, 1)

	go func() {
		// just used in UT to self-check service is available.
		address := "localhost:" + strconv.Itoa(opt.Port)
		var err error

		if opt.TLSMode == 1 || opt.TLSMode == 2 {
			var creds credentials.TransportCredentials
			if opt.TLSMode == 1 {
				creds, err = credentials.NewClientTLSFromFile(Params.ServerPemPath, "localhost")
			} else {
				creds, err = withCredential(opt.ClientPemPath, opt.ClientKeyPath, opt.CaPath)
			}
			if err != nil {
				ch <- err
				return
			}
			_, err = grpc.Dial(address, grpc.WithBlock(), grpc.WithTransportCredentials(creds))
			ch <- err
			return
		}
		if _, err := grpc.Dial(address, grpc.WithBlock(), grpc.WithInsecure()); true {
			ch <- err
		}
	}()

	select {
	case err := <-ch:
		if err != nil {
			log.Error("grpc service not ready",
				zap.Error(err),
				zap.Any("option", opt))
			panic(err)
		}
	case <-ticker.C:
		log.Error("grpc service not ready",
			zap.Any("option", opt))
		panic("grpc service not ready")
	}
}

// TODO: should tls-related configurations be hard code here?
var waitDuration = time.Second * 1
var clientPemPath = "../../../configs/cert/client.pem"
var clientKeyPath = "../../../configs/cert/client.key"

// waitForServerReady wait for internal grpc service and external service to be ready, according to the params.
func waitForServerReady() {
	waitForGrpcReady(newWaitOption(waitDuration, Params.InternalPort, 0, "", "", ""))
	waitForGrpcReady(newWaitOption(waitDuration, Params.Port, Params.TLSMode, clientPemPath, clientKeyPath, Params.CaPemPath))
}

func runAndWaitForServerReady(server *Server) error {
	err := server.Run()
	if err != nil {
		return err
	}
	waitForServerReady()
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.Nil(t, err)

	server.proxy = &MockProxy{}
	server.rootCoordClient = &MockRootCoord{}
	server.indexCoordClient = &MockIndexCoord{}
	server.queryCoordClient = &MockQueryCoord{}
	server.dataCoordClient = &MockDataCoord{}

	t.Run("Run", func(t *testing.T) {
		err = runAndWaitForServerReady(server)
		assert.Nil(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		_, err := server.GetComponentStates(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		_, err := server.GetStatisticsChannel(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("InvalidateCollectionMetaCache", func(t *testing.T) {
		_, err := server.InvalidateCollectionMetaCache(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateCollection", func(t *testing.T) {
		_, err := server.CreateCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropCollection", func(t *testing.T) {
		_, err := server.DropCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("HasCollection", func(t *testing.T) {
		_, err := server.HasCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("LoadCollection", func(t *testing.T) {
		_, err := server.LoadCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		_, err := server.ReleaseCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DescribeCollection", func(t *testing.T) {
		_, err := server.DescribeCollection(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetCollectionStatistics", func(t *testing.T) {
		_, err := server.GetCollectionStatistics(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ShowCollections", func(t *testing.T) {
		_, err := server.ShowCollections(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreatePartition", func(t *testing.T) {
		_, err := server.CreatePartition(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropPartition", func(t *testing.T) {
		_, err := server.DropPartition(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("HasPartition", func(t *testing.T) {
		_, err := server.HasPartition(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		_, err := server.LoadPartitions(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		_, err := server.ReleasePartitions(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetPartitionStatistics", func(t *testing.T) {
		_, err := server.GetPartitionStatistics(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ShowPartitions", func(t *testing.T) {
		_, err := server.ShowPartitions(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		_, err := server.CreateIndex(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropIndex", func(t *testing.T) {
		_, err := server.DropIndex(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		_, err := server.DescribeIndex(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		_, err := server.GetIndexBuildProgress(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		_, err := server.GetIndexState(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Insert", func(t *testing.T) {
		_, err := server.Insert(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		_, err := server.Delete(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Search", func(t *testing.T) {
		_, err := server.Search(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Flush", func(t *testing.T) {
		_, err := server.Flush(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Query", func(t *testing.T) {
		_, err := server.Query(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CalcDistance", func(t *testing.T) {
		_, err := server.CalcDistance(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetDdChannel", func(t *testing.T) {
		_, err := server.GetDdChannel(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetPersistentSegmentInfo", func(t *testing.T) {
		_, err := server.GetPersistentSegmentInfo(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetQuerySegmentInfo", func(t *testing.T) {
		_, err := server.GetQuerySegmentInfo(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("Dummy", func(t *testing.T) {
		_, err := server.Dummy(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("RegisterLink", func(t *testing.T) {
		_, err := server.RegisterLink(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		_, err := server.GetMetrics(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("LoadBalance", func(t *testing.T) {
		_, err := server.LoadBalance(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateAlias", func(t *testing.T) {
		_, err := server.CreateAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropAlias", func(t *testing.T) {
		_, err := server.DropAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("AlterAlias", func(t *testing.T) {
		_, err := server.AlterAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetCompactionState", func(t *testing.T) {
		_, err := server.GetCompactionState(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ManualCompaction", func(t *testing.T) {
		_, err := server.ManualCompaction(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetCompactionStateWithPlans", func(t *testing.T) {
		_, err := server.GetCompactionStateWithPlans(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateCredential", func(t *testing.T) {
		_, err := server.CreateCredential(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("UpdateCredential", func(t *testing.T) {
		_, err := server.UpdateCredential(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DeleteCredential", func(t *testing.T) {
		_, err := server.DeleteCredential(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ListCredUsers", func(t *testing.T) {
		_, err := server.ListCredUsers(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("InvalidateCredentialCache", func(t *testing.T) {
		_, err := server.InvalidateCredentialCache(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("UpdateCredentialCache", func(t *testing.T) {
		_, err := server.UpdateCredentialCache(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("CreateRole", func(t *testing.T) {
		_, err := server.CreateRole(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropRole", func(t *testing.T) {
		_, err := server.DropRole(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("OperateUserRole", func(t *testing.T) {
		_, err := server.OperateUserRole(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("SelectRole", func(t *testing.T) {
		_, err := server.SelectRole(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("SelectUser", func(t *testing.T) {
		_, err := server.SelectUser(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("OperatePrivilege", func(t *testing.T) {
		_, err := server.OperatePrivilege(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("SelectGrant", func(t *testing.T) {
		_, err := server.SelectGrant(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("RefreshPrivilegeInfoCache", func(t *testing.T) {
		_, err := server.RefreshPolicyInfoCache(ctx, nil)
		assert.Nil(t, err)
	})

	err = server.Stop()
	assert.Nil(t, err)

	// Update config and start server again to test with different config set.
	// This works as config will be initialized only once
	proxy.Params.ProxyCfg.GinLogging = false
	err = runAndWaitForServerReady(server)
	assert.Nil(t, err)
	err = server.Stop()
	assert.Nil(t, err)
}

func TestServer_Check(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.Nil(t, err)

	mockProxy := &MockProxy{}
	server.proxy = mockProxy
	server.rootCoordClient = &MockRootCoord{}
	server.indexCoordClient = &MockIndexCoord{}
	server.queryCoordClient = &MockQueryCoord{}
	server.dataCoordClient = &MockDataCoord{}

	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	ret, err := server.Check(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)

	mockProxy.On("GetComponentStates", ctx).Return(nil, fmt.Errorf("mock grpc unexpected error")).Once()

	ret, err = server.Check(ctx, req)
	assert.NotNil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo := &internalpb.ComponentInfo{
		NodeID:    0,
		Role:      "proxy",
		StateCode: internalpb.StateCode_Abnormal,
	}
	status := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	componentState := &internalpb.ComponentStates{
		State:  componentInfo,
		Status: status,
	}
	mockProxy.On("GetComponentStates", ctx).Return(componentState, nil)

	ret, err = server.Check(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	status.ErrorCode = commonpb.ErrorCode_Success
	ret, err = server.Check(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = internalpb.StateCode_Initializing
	ret, err = server.Check(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = internalpb.StateCode_Healthy
	ret, err = server.Check(ctx, req)
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)
}

func TestServer_Watch(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.Nil(t, err)

	mockProxy := &MockProxy{}
	server.proxy = mockProxy
	server.rootCoordClient = &MockRootCoord{}
	server.indexCoordClient = &MockIndexCoord{}
	server.queryCoordClient = &MockQueryCoord{}
	server.dataCoordClient = &MockDataCoord{}

	watchServer := milvusmock.NewGrpcHealthWatchServer()
	resultChan := watchServer.Chan()
	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	//var ret *grpc_health_v1.HealthCheckResponse
	err = server.Watch(req, watchServer)
	ret := <-resultChan

	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)

	mockProxy.On("GetComponentStates", ctx).Return(nil, fmt.Errorf("mock grpc unexpected error")).Once()

	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo := &internalpb.ComponentInfo{
		NodeID:    0,
		Role:      "proxy",
		StateCode: internalpb.StateCode_Abnormal,
	}
	status := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	componentState := &internalpb.ComponentStates{
		State:  componentInfo,
		Status: status,
	}
	mockProxy.On("GetComponentStates", ctx).Return(componentState, nil)

	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	status.ErrorCode = commonpb.ErrorCode_Success
	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = internalpb.StateCode_Initializing
	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = internalpb.StateCode_Healthy
	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.Nil(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)
}

func Test_NewServer_HTTPServer_Enabled(t *testing.T) {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.Nil(t, err)

	server.proxy = &MockProxy{}
	server.rootCoordClient = &MockRootCoord{}
	server.indexCoordClient = &MockIndexCoord{}
	server.queryCoordClient = &MockQueryCoord{}
	server.dataCoordClient = &MockDataCoord{}

	HTTPParams.InitOnce()
	HTTPParams.Enabled = true

	err = runAndWaitForServerReady(server)
	assert.Nil(t, err)
	err = server.Stop()
	assert.Nil(t, err)

	defer func() {
		e := recover()
		if e == nil {
			t.Fatalf("test should have panicked but did not")
		}
	}()
	// if disable workds path not registered, so it shall not panic
	server.registerHTTPServer()
}

func getServer(t *testing.T) *Server {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.Nil(t, err)

	server.proxy = &MockProxy{}
	server.rootCoordClient = &MockRootCoord{}
	server.indexCoordClient = &MockIndexCoord{}
	server.queryCoordClient = &MockQueryCoord{}
	server.dataCoordClient = &MockDataCoord{}
	return server
}

func Test_NewServer_TLS_TwoWay(t *testing.T) {
	server := getServer(t)

	Params.InitOnce("proxy")
	Params.TLSMode = 2
	Params.ServerPemPath = "../../../configs/cert/server.pem"
	Params.ServerKeyPath = "../../../configs/cert/server.key"
	Params.CaPemPath = "../../../configs/cert/ca.pem"
	HTTPParams.Enabled = false

	err := runAndWaitForServerReady(server)
	assert.Nil(t, err)
	assert.NotNil(t, server.grpcExternalServer)
	err = server.Stop()
	assert.Nil(t, err)
}

func Test_NewServer_TLS_OneWay(t *testing.T) {
	server := getServer(t)

	Params.InitOnce("proxy")
	Params.TLSMode = 1
	Params.ServerPemPath = "../../../configs/cert/server.pem"
	Params.ServerKeyPath = "../../../configs/cert/server.key"
	HTTPParams.Enabled = false

	err := runAndWaitForServerReady(server)
	assert.Nil(t, err)
	assert.NotNil(t, server.grpcExternalServer)
	err = server.Stop()
	assert.Nil(t, err)
}

func Test_NewServer_TLS_FileNotExisted(t *testing.T) {
	server := getServer(t)

	Params.InitOnce("proxy")
	Params.TLSMode = 1
	Params.ServerPemPath = "../not/existed/server.pem"
	Params.ServerKeyPath = "../../../configs/cert/server.key"
	HTTPParams.Enabled = false
	err := runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()

	Params.TLSMode = 2
	Params.ServerPemPath = "../not/existed/server.pem"
	Params.CaPemPath = "../../../configs/cert/ca.pem"
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()

	Params.ServerPemPath = "../../../configs/cert/server.pem"
	Params.CaPemPath = "../not/existed/ca.pem"
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()

	Params.ServerPemPath = "../../../configs/cert/server.pem"
	Params.CaPemPath = "service.go"
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()
}
