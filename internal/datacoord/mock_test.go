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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ kv.MetaKv = &metaMemoryKV{}

type metaMemoryKV struct {
	memkv.MemoryKV
}

func NewMetaMemoryKV() *metaMemoryKV {
	return &metaMemoryKV{MemoryKV: *memkv.NewMemoryKV()}
}

func (mm *metaMemoryKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	keys, values, err := mm.MemoryKV.LoadWithPrefix(context.TODO(), prefix)
	if err != nil {
		return err
	}

	for i, k := range keys {
		if err := fn([]byte(k), []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func (mm *metaMemoryKV) GetPath(key string) string {
	panic("implement me")
}

func (mm *metaMemoryKV) Watch(ctx context.Context, key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithPrefix(ctx context.Context, key string) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) WatchWithRevision(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	panic("implement me")
}

func (mm *metaMemoryKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	panic("implement me")
}

func newMemoryMeta(t *testing.T) (*meta, error) {
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	return newMeta(context.TODO(), catalog, nil, broker)
}

func newMockAllocator(t *testing.T) *allocator.MockAllocator {
	counter := atomic.NewInt64(0)
	mockAllocator := allocator.NewMockAllocator(t)
	mockAllocator.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		return counter.Inc(), nil
	}).Maybe()
	mockAllocator.EXPECT().AllocTimestamp(mock.Anything).RunAndReturn(func(ctx context.Context) (uint64, error) {
		return uint64(counter.Inc()), nil
	}).Maybe()
	mockAllocator.EXPECT().AllocN(mock.Anything).RunAndReturn(func(i int64) (int64, int64, error) {
		v := counter.Add(i)
		return v, v + i, nil
	}).Maybe()
	return mockAllocator
}

func newMock0Allocator(t *testing.T) *allocator.MockAllocator {
	mock0Allocator := allocator.NewMockAllocator(t)
	mock0Allocator.EXPECT().AllocID(mock.Anything).Return(100, nil).Maybe()
	mock0Allocator.EXPECT().AllocTimestamp(mock.Anything).Return(1000, nil).Maybe()
	mock0Allocator.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Maybe()
	return mock0Allocator
}

func newTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "field1", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}},
			{FieldID: 2, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}

func newTestScalarClusteringKeySchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test_scalar_clustering",
		Description: "schema for test scalar clustering compaction",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "field1", IsPrimaryKey: true, IsClusteringKey: true, Description: "field no.1", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "field2", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
		},
	}
}

type mockMixCoord struct {
	state commonpb.StateCode
	cnt   atomic.Int64
}

func (m *mockMixCoord) DescribeDatabase(ctx context.Context, in *rootcoordpb.DescribeDatabaseRequest) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return &rootcoordpb.DescribeDatabaseResponse{
		Status:           merr.Success(),
		DbID:             1,
		DbName:           "default",
		CreatedTimestamp: 1,
	}, nil
}

func (m *mockMixCoord) Close() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockMixCoord) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	// TODO implement me
	panic("implement me")
}

func (m *mockMixCoord) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) AddCollectionField(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func newMockMixCoord() *mockMixCoord {
	return &mockMixCoord{state: commonpb.StateCode_Healthy}
}

func (m *mockMixCoord) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *mockMixCoord) Stop() error {
	m.state = commonpb.StateCode_Abnormal
	return nil
}

func (m *mockMixCoord) Register() error {
	return nil
}

func (m *mockMixCoord) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    0,
			Role:      "",
			StateCode: m.state,
			ExtraInfo: []*commonpb.KeyValuePair{},
		},
		SubcomponentStates: []*milvuspb.ComponentInfo{},
		Status:             merr.Success(),
	}, nil
}

func (m *mockMixCoord) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	panic("not implemented") // TODO: Implement
}

// DDL request
func (m *mockMixCoord) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	// return not exist
	if req.CollectionID == -1 {
		err := merr.WrapErrCollectionNotFound(req.GetCollectionID())
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Success(),
		Schema: &schemapb.CollectionSchema{
			Name: "test",
		},
		CollectionID:        1314,
		VirtualChannelNames: []string{"vchan1"},
	}, nil
}

func (m *mockMixCoord) DescribeCollectionInternal(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return m.DescribeCollection(ctx, req)
}

func (m *mockMixCoord) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status:          merr.Success(),
		CollectionNames: []string{"test"},
	}, nil
}

func (m *mockMixCoord) ShowCollectionIDs(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return &rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return &milvuspb.ListDatabasesResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return &milvuspb.ShowPartitionsResponse{
		Status:         merr.Success(),
		PartitionNames: []string{"_default"},
		PartitionIDs:   []int64{0},
	}, nil
}

func (m *mockMixCoord) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return m.ShowPartitions(ctx, req)
}

// global timestamp allocator
func (m *mockMixCoord) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocTimestampResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}

	val := m.cnt.Add(int64(req.Count))
	phy := time.Now().UnixNano() / int64(time.Millisecond)
	ts := tsoutil.ComposeTS(phy, val)
	return &rootcoordpb.AllocTimestampResponse{
		Status:    merr.Success(),
		Timestamp: ts,
		Count:     req.Count,
	}, nil
}

func (m *mockMixCoord) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	if m.state != commonpb.StateCode_Healthy {
		return &rootcoordpb.AllocIDResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil
	}
	val := m.cnt.Add(int64(req.Count))
	return &rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     val,
		Count:  req.Count,
	}, nil
}

// segment
func (m *mockMixCoord) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetPChannelInfo(ctx context.Context, req *rootcoordpb.GetPChannelInfoRequest) (*rootcoordpb.GetPChannelInfoResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  "ddchannel",
	}, nil
}

func (m *mockMixCoord) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) SegmentFlushCompleted(ctx context.Context, in *datapb.SegmentFlushCompletedMsg) (*commonpb.Status, error) {
	return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
}

func (m *mockMixCoord) AddNewSegment(ctx context.Context, in *datapb.SegmentMsg) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return &internalpb.ShowConfigurationsResponse{
		Status: merr.Success(),
	}, nil
}

func (m *mockMixCoord) Init() error {
	return nil
}

func (m *mockMixCoord) Start() error {
	return nil
}

func (m *mockMixCoord) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): change the id, though it's not important in ut
	nodeID := UniqueID(20210901)

	rootCoordTopology := metricsinfo.RootCoordTopology{
		Self: metricsinfo.RootCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
				ID:   nodeID,
			},
		},
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp, err := metricsinfo.MarshalTopology(rootCoordTopology)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, nodeID),
	}, nil
}

func (m *mockMixCoord) GetDcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) GetQcMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) BackupRBAC(ctx context.Context, req *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	panic("not implemented") // TODO: Implement
}

func (m *mockMixCoord) RestoreRBAC(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	panic("not implemented") // TODO: Implement
}

type mockCompactionTrigger struct {
	methods map[string]interface{}
}

// triggerSingleCompaction trigerr a compaction bundled with collection-partiiton-channel-segment
func (t *mockCompactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, blockToSendSignal bool) error {
	if f, ok := t.methods["triggerSingleCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64, partitionID int64, segmentID int64, channel string) error); ok {
			return ff(collectionID, partitionID, segmentID, channel)
		}
	}
	panic("not implemented")
}

// triggerManualCompaction force to start a compaction
func (t *mockCompactionTrigger) triggerManualCompaction(collectionID int64) (UniqueID, error) {
	if f, ok := t.methods["triggerManualCompaction"]; ok {
		if ff, ok := f.(func(collectionID int64) (UniqueID, error)); ok {
			return ff(collectionID)
		}
	}
	panic("not implemented")
}

func (t *mockCompactionTrigger) start() {
	if f, ok := t.methods["start"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

func (t *mockCompactionTrigger) stop() {
	if f, ok := t.methods["stop"]; ok {
		if ff, ok := f.(func()); ok {
			ff()
			return
		}
	}
	panic("not implemented")
}

func (m *mockMixCoord) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return &internalpb.ListPolicyResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
}

func (m *mockMixCoord) CreatePrivilegeGroup(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) DropPrivilegeGroup(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (m *mockMixCoord) ListPrivilegeGroups(ctx context.Context, req *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	panic("implement me")
}

func (m *mockMixCoord) OperatePrivilegeGroup(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	panic("implement me")
}

// DataCoordServer
func (s *mockMixCoord) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest) (*datapb.GetChannelRecoveryInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) GcControl(ctx context.Context, req *datapb.GcControlRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (s *mockMixCoord) ImportV2(ctx context.Context, req *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) GetImportProgress(ctx context.Context, req *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListImports(ctx context.Context, req *internalpb.ListImportsRequestInternal) (*internalpb.ListImportsResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) ListIndexes(ctx context.Context, req *indexpb.ListIndexesRequest) (*indexpb.ListIndexesResponse, error) {
	panic("implement me")
}

func (s *mockMixCoord) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
	panic("implement me")
}

// RegisterStreamingCoordGRPCService registers the grpc service of streaming coordinator.
func (s *mockMixCoord) RegisterStreamingCoordGRPCService(server *grpc.Server) {
	panic("implement me")
}

type mockHandler struct {
	meta *meta
}

func newMockHandler() *mockHandler {
	return &mockHandler{}
}

func (h *mockHandler) GetQueryVChanPositions(channel RWChannel, partitionID ...UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetName(),
	}
}

func (h *mockHandler) GetDataVChanPositions(channel RWChannel, partitionID UniqueID) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetName(),
	}
}

func (h *mockHandler) CheckShouldDropChannel(channel string) bool {
	return false
}

func (h *mockHandler) FinishDropChannel(channel string, collectionID int64) error {
	return nil
}

func (h *mockHandler) GetCollection(_ context.Context, collectionID UniqueID) (*collectionInfo, error) {
	// empty schema
	if h.meta != nil {
		return h.meta.GetCollection(collectionID), nil
	}
	return &collectionInfo{ID: collectionID}, nil
}

func (h *mockHandler) GetCurrentSegmentsView(ctx context.Context, channel RWChannel, partitionIDs ...UniqueID) *SegmentsView {
	return nil
}

func newMockHandlerWithMeta(meta *meta) *mockHandler {
	return &mockHandler{
		meta: meta,
	}
}
