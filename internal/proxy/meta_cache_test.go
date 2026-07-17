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

package proxy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var dbName = GetCurDBNameFromContextOrDefault(context.Background())

type MockMixCoordClientInterface struct {
	types.MixCoordClient
	Error       bool
	AccessCount int32

	listPolicy            func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error)
	showLoadCollections   func(ctx context.Context, in *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)
	getShardLeaders       func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error)
	listResourceGroups    func(ctx context.Context, in *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error)
	describeResourceGroup func(ctx context.Context, in *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error)
}

func EqualSchema(t *testing.T, expect, actual *schemapb.CollectionSchema) {
	assert.Equal(t, expect.AutoID, actual.AutoID)
	assert.Equal(t, expect.Description, actual.Description)
	assert.Equal(t, expect.Name, actual.Name)
	assert.Equal(t, expect.EnableDynamicField, actual.EnableDynamicField)
	assert.Equal(t, len(expect.Fields), len(actual.Fields))
	for i := range expect.Fields {
		assert.Equal(t, expect.Fields[i], actual.Fields[i])
	}
	assert.Equal(t, len(expect.Functions), len(actual.Functions))
	for i := range expect.Functions {
		assert.Equal(t, expect.Functions[i], actual.Functions[i])
	}
	assert.Equal(t, len(expect.Properties), len(actual.Properties))
	for i := range expect.Properties {
		assert.Equal(t, expect.Properties[i], actual.Properties[i])
	}
}

func (m *MockMixCoordClientInterface) IncAccessCount() {
	atomic.AddInt32(&m.AccessCount, 1)
}

func (m *MockMixCoordClientInterface) GetAccessCount() int {
	ret := atomic.LoadInt32(&m.AccessCount)
	return int(ret)
}

func (m *MockMixCoordClientInterface) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	if in.CollectionName == "collection1" || in.CollectionName == "collection1_alias" || in.CollectionID == 1 {
		return &milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{1, 2},
			CreatedTimestamps:    []uint64{100, 200},
			CreatedUtcTimestamps: []uint64{100, 200},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "collection2" || in.CollectionID == 2 {
		return &milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{3, 4},
			CreatedTimestamps:    []uint64{201, 202},
			CreatedUtcTimestamps: []uint64{201, 202},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{5, 6},
			CreatedTimestamps:    []uint64{201},
			CreatedUtcTimestamps: []uint64{201},
			PartitionNames:       []string{"par1", "par2"},
		}, nil
	}
	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		PartitionIDs:         []typeutil.UniqueID{},
		CreatedTimestamps:    []uint64{},
		CreatedUtcTimestamps: []uint64{},
		PartitionNames:       []string{},
	}, nil
}

func (m *MockMixCoordClientInterface) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.IncAccessCount()
	if in.CollectionName == "collection1" || in.CollectionName == "collection1_alias" || in.CollectionID == 1 {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: typeutil.UniqueID(1),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
				Name:   "collection1",
			},
			DbName:      dbName,
			RequestTime: 100,
		}, nil
	}
	if in.CollectionName == "collection2" || in.CollectionID == 2 {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: typeutil.UniqueID(2),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
				Name:   "collection2",
			},
			DbName:      dbName,
			RequestTime: 100,
		}, nil
	}
	if in.CollectionName == "errorCollection" {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: typeutil.UniqueID(3),
			Schema: &schemapb.CollectionSchema{
				AutoID: true,
			},
			DbName:      dbName,
			RequestTime: 100,
		}, nil
	}

	err := merr.WrapErrCollectionNotFound(in.CollectionName)
	return &milvuspb.DescribeCollectionResponse{
		Status: merr.Status(err),
		Schema: nil,
	}, nil
}

func (m *MockMixCoordClientInterface) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}
	m.IncAccessCount()
	if req.Username == "mockUser" {
		encryptedPassword, _ := crypto.PasswordEncrypt("mockPass")
		return &rootcoordpb.GetCredentialResponse{
			Status:   merr.Success(),
			Username: "mockUser",
			Password: encryptedPassword,
		}, nil
	}

	err := fmt.Errorf("can't find credential: %s", req.Username)
	return nil, err
}

func (m *MockMixCoordClientInterface) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	if m.Error {
		return nil, errors.New("mocked error")
	}

	return &milvuspb.ListCredUsersResponse{
		Status:    merr.Success(),
		Usernames: []string{"mockUser"},
	}, nil
}

func (m *MockMixCoordClientInterface) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	if m.listPolicy != nil {
		return m.listPolicy(ctx, in)
	}
	return &internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil
}

func (c *MockMixCoordClientInterface) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	panic("implement me")
}

// GetTimeTickChannel get timetick channel name
func (c *MockMixCoordClientInterface) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

// GetStatisticsChannel just define a channel, not used currently
func (c *MockMixCoordClientInterface) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	panic("implement me")
}

// ShowConfigurations gets specified configurations para of RootCoord
func (c *MockMixCoordClientInterface) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	panic("implement me")
}

// CreateCollection create collection
func (c *MockMixCoordClientInterface) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DropCollection drop collection
func (c *MockMixCoordClientInterface) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// HasCollection check collection existence
func (c *MockMixCoordClientInterface) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	panic("implement me")
}

// CreatePartition create partition
func (c *MockMixCoordClientInterface) AddCollectionField(ctx context.Context, in *milvuspb.AddCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AddCollectionStructField(ctx context.Context, in *milvuspb.AddCollectionStructFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// describeCollectionInternal return collection info
func (c *MockMixCoordClientInterface) describeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	panic("implement me")
}

// ShowCollections list all collection names
func (c *MockMixCoordClientInterface) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	return &milvuspb.ShowCollectionsResponse{
		Status: merr.Success(),
	}, nil
}

// ShowCollectionIDs returns all collection IDs.
func (c *MockMixCoordClientInterface) ShowCollectionIDs(ctx context.Context, in *rootcoordpb.ShowCollectionIDsRequest, opts ...grpc.CallOption) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterCollection(ctx context.Context, request *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterCollectionField(ctx context.Context, request *milvuspb.AlterCollectionFieldRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// CreatePartition create partition
func (c *MockMixCoordClientInterface) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DropPartition drop partition
func (c *MockMixCoordClientInterface) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// HasPartition check partition existence
func (c *MockMixCoordClientInterface) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	panic("implement me")
}

// showPartitionsInternal list all partitions in collection
func (c *MockMixCoordClientInterface) showPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	panic("implement me")
}

// AllocTimestamp global timestamp allocator
func (c *MockMixCoordClientInterface) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	panic("implement me")
}

// AllocID global ID allocator
func (c *MockMixCoordClientInterface) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	panic("implement me")
}

// UpdateChannelTimeTick used to handle ChannelTimeTickMsg
func (c *MockMixCoordClientInterface) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ShowSegments list all segments
func (c *MockMixCoordClientInterface) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	panic("implement me")
}

// GetVChannels returns all vchannels belonging to the pchannel.
func (c *MockMixCoordClientInterface) GetPChannelInfo(ctx context.Context, in *rootcoordpb.GetPChannelInfoRequest, opts ...grpc.CallOption) (*rootcoordpb.GetPChannelInfoResponse, error) {
	panic("implement me")
}

// InvalidateCollectionMetaCache notifies RootCoord to release the collection cache in Proxies.
func (c *MockMixCoordClientInterface) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// CreateAlias create collection alias
func (c *MockMixCoordClientInterface) CreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DropAlias drop collection alias
func (c *MockMixCoordClientInterface) DropAlias(ctx context.Context, req *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// AlterAlias alter collection alias
func (c *MockMixCoordClientInterface) AlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// DescribeAlias describe alias
func (c *MockMixCoordClientInterface) DescribeAlias(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	return &milvuspb.DescribeAliasResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_CollectionNotExists,
			Reason:    "alias not found",
		},
	}, nil
}

// ListAliases list all aliases of db or collection
func (c *MockMixCoordClientInterface) ListAliases(ctx context.Context, req *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterRole(ctx context.Context, req *milvuspb.AlterRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DescribeDatabase(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AlterDatabase(ctx context.Context, request *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// Flush flushes a collection's data
func (c *MockMixCoordClientInterface) Flush(ctx context.Context, req *datapb.FlushRequest, opts ...grpc.CallOption) (*datapb.FlushResponse, error) {
	panic("implement me")
}

// AssignSegmentID applies allocations for specified Coolection/Partition and related Channel Name(Virtial Channel)
//
// ctx is the context to control request deadline and cancellation
// req contains the requester's info(id and role) and the list of Assignment Request,
// which contains the specified collection, partitaion id, the related VChannel Name and row count it needs
//
// response struct `AssignSegmentIDResponse` contains the assignment result for each request
// error is returned only when some communication issue occurs
// if some error occurs in the process of `AssignSegmentID`, it will be recorded and returned in `Status` field of response
//
// `AssignSegmentID` will applies current configured allocation policies for each request
// if the VChannel is newly used, `WatchDmlChannels` will be invoked to notify a `DataNode`(selected by policy) to watch it
// if there is anything make the allocation impossible, the response will not contain the corresponding result
func (c *MockMixCoordClientInterface) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest, opts ...grpc.CallOption) (*datapb.AssignSegmentIDResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) AllocSegment(ctx context.Context, in *datapb.AllocSegmentRequest, opts ...grpc.CallOption) (*datapb.AllocSegmentResponse, error) {
	panic("implement me")
}

// GetSegmentStates requests segment state information
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment id to query
//
// response struct `GetSegmentStatesResponse` contains the list of each state query result
//
//	when the segment is not found, the state entry will has the field `Status`  to identify failure
//	otherwise the Segment State and Start position information will be returned
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentStatesResponse, error) {
	panic("implement me")
}

// GetInsertBinlogPaths requests binlog paths for specified segment
//
// ctx is the context to control request deadline and cancellation
// req contains the segment id to query
//
// response struct `GetInsertBinlogPathsResponse` contains the fields list
//
//	and corresponding binlog path list
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest, opts ...grpc.CallOption) (*datapb.GetInsertBinlogPathsResponse, error) {
	panic("implement me")
}

// GetCollectionStatistics requests collection statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection id to query
//
// response struct `GetCollectionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetCollectionStatisticsResponse, error) {
	panic("implement me")
}

// GetPartitionStatistics requests partition statistics
//
// ctx is the context to control request deadline and cancellation
// req contains the collection and partition id to query
//
// response struct `GetPartitionStatisticsResponse` contains the key-value list fields returning related data
//
//	only row count for now
//
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest, opts ...grpc.CallOption) (*datapb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

// GetSegmentInfoChannel DEPRECATED
// legacy api to get SegmentInfo Channel name
func (c *MockMixCoordClientInterface) GetSegmentInfoChannel(ctx context.Context, _ *datapb.GetSegmentInfoChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

// GetSegmentInfo requests segment info
//
// ctx is the context to control request deadline and cancellation
// req contains the list of segment ids to query
//
// response struct `GetSegmentInfoResponse` contains the list of segment info
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*datapb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

// SaveBinlogPaths updates segments binlogs(including insert binlogs, stats logs and delta logs)
//
//	and related message stream positions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response status contains the status/error code and failing reason if any
// error is returned only when some communication issue occurs
//
// there is a constraint that the `SaveBinlogPaths` requests of same segment shall be passed in sequence
//
//		the root reason is each `SaveBinlogPaths` will overwrite the checkpoint position
//	 if the constraint is broken, the checkpoint position will not be monotonically increasing and the integrity will be compromised
func (c *MockMixCoordClientInterface) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// GetRecoveryInfo request segment recovery info of collection/partition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
// response struct `GetRecoveryInfoResponse` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponse, error) {
	panic("implement me")
}

// GetRecoveryInfoV2 request segment recovery info of collection/partitions
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partitions id to query
//
// response struct `GetRecoveryInfoResponseV2` contains the list of segments info and corresponding vchannel info
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2, opts ...grpc.CallOption) (*datapb.GetRecoveryInfoResponseV2, error) {
	panic("implement me")
}

// GetChannelRecoveryInfo returns the corresponding vchannel info.
func (c *MockMixCoordClientInterface) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, opts ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
	panic("implement me")
}

// GetFlushedSegments returns flushed segment list of requested collection/parition
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id to query
//
//	when partition is lesser or equal to 0, all flushed segments of collection will be returned
//
// response struct `GetFlushedSegmentsResponse` contains flushed segment id list
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest, opts ...grpc.CallOption) (*datapb.GetFlushedSegmentsResponse, error) {
	panic("implement me")
}

// GetSegmentsByStates returns segment list of requested collection/partition and segment states
//
// ctx is the context to control request deadline and cancellation
// req contains the collection/partition id and segment states to query
// when partition is lesser or equal to 0, all segments of collection will be returned
//
// response struct `GetSegmentsByStatesResponse` contains segment id list
// error is returned only when some communication issue occurs
func (c *MockMixCoordClientInterface) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest, opts ...grpc.CallOption) (*datapb.GetSegmentsByStatesResponse, error) {
	panic("implement me")
}

// ManualCompaction triggers a compaction for a collection
func (c *MockMixCoordClientInterface) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest, opts ...grpc.CallOption) (*milvuspb.ManualCompactionResponse, error) {
	panic("implement me")
}

// GetCompactionState gets the state of a compaction
func (c *MockMixCoordClientInterface) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionStateResponse, error) {
	panic("implement me")
}

// GetCompactionStateWithPlans gets the state of a compaction by plan
func (c *MockMixCoordClientInterface) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest, opts ...grpc.CallOption) (*milvuspb.GetCompactionPlansResponse, error) {
	panic("implement me")
}

// WatchChannels notifies DataCoord to watch vchannels of a collection
func (c *MockMixCoordClientInterface) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest, opts ...grpc.CallOption) (*datapb.WatchChannelsResponse, error) {
	panic("implement me")
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (c *MockMixCoordClientInterface) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushStateResponse, error) {
	panic("implement me")
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (c *MockMixCoordClientInterface) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest, opts ...grpc.CallOption) (*milvuspb.GetFlushAllStateResponse, error) {
	panic("implement me")
}

// DropVirtualChannel drops virtual channel in datacoord.
func (c *MockMixCoordClientInterface) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
	panic("implement me")
}

// SetSegmentState sets the state of a given segment.
func (c *MockMixCoordClientInterface) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest, opts ...grpc.CallOption) (*datapb.SetSegmentStateResponse, error) {
	panic("implement me")
}

// UpdateSegmentStatistics is the client side caller of UpdateSegmentStatistics.
func (c *MockMixCoordClientInterface) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (c *MockMixCoordClientInterface) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// BroadcastAlteredCollection is the DataCoord client side code for BroadcastAlteredCollection call.
func (c *MockMixCoordClientInterface) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest, opts ...grpc.CallOption) (*datapb.GcConfirmResponse, error) {
	panic("implement me")
}

// CreateIndex sends the build index request to IndexCoord.
func (c *MockMixCoordClientInterface) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// AlterIndex sends the alter index request to IndexCoord.
func (c *MockMixCoordClientInterface) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// GetIndexState gets the index states from IndexCoord.
func (c *MockMixCoordClientInterface) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStateResponse, error) {
	panic("implement me")
}

// GetSegmentIndexState gets the index states from IndexCoord.
func (c *MockMixCoordClientInterface) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest, opts ...grpc.CallOption) (*indexpb.GetSegmentIndexStateResponse, error) {
	panic("implement me")
}

// GetIndexInfos gets the index file paths from IndexCoord.
func (c *MockMixCoordClientInterface) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest, opts ...grpc.CallOption) (*indexpb.GetIndexInfoResponse, error) {
	panic("implement me")
}

// DescribeIndex describe the index info of the collection.
func (c *MockMixCoordClientInterface) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest, opts ...grpc.CallOption) (*indexpb.DescribeIndexResponse, error) {
	panic("implement me")
}

// GetIndexStatistics get the statistics of the index.
func (c *MockMixCoordClientInterface) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest, opts ...grpc.CallOption) (*indexpb.GetIndexStatisticsResponse, error) {
	panic("implement me")
}

// GetIndexBuildProgress describe the progress of the index.
func (c *MockMixCoordClientInterface) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest, opts ...grpc.CallOption) (*indexpb.GetIndexBuildProgressResponse, error) {
	panic("implement me")
}

// DropIndex sends the drop index request to IndexCoord.
func (c *MockMixCoordClientInterface) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GcControl(ctx context.Context, req *datapb.GcControlRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal, opts ...grpc.CallOption) (*internalpb.ImportResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest, opts ...grpc.CallOption) (*internalpb.GetImportProgressResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListImports(ctx context.Context, in *internalpb.ListImportsRequestInternal, opts ...grpc.CallOption) (*internalpb.ListImportsResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListIndexes(ctx context.Context, in *indexpb.ListIndexesRequest, opts ...grpc.CallOption) (*indexpb.ListIndexesResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest, opts ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
	if c.showLoadCollections != nil {
		return c.showLoadCollections(ctx, req)
	}
	return &querypb.ShowCollectionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

// LoadCollection loads the data of the specified collections in the QueryCoord.
func (c *MockMixCoordClientInterface) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ReleaseCollection release the data of the specified collections in the QueryCoord.
func (c *MockMixCoordClientInterface) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ShowPartitions shows the partitions in the QueryCoord.
func (c *MockMixCoordClientInterface) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest, opts ...grpc.CallOption) (*querypb.ShowPartitionsResponse, error) {
	panic("implement me")
}

// LoadPartitions loads the data of the specified partitions in the QueryCoord.
func (c *MockMixCoordClientInterface) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ReleasePartitions release the data of the specified partitions in the QueryCoord.
func (c *MockMixCoordClientInterface) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// SyncNewCreatedPartition notifies QueryCoord to sync new created partition if collection is loaded.
func (c *MockMixCoordClientInterface) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// GetPartitionStates gets the states of the specified partition.
func (c *MockMixCoordClientInterface) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest, opts ...grpc.CallOption) (*querypb.GetPartitionStatesResponse, error) {
	panic("implement me")
}

// GetSegmentInfo gets the information of the specified segment from QueryCoord.
func (c *MockMixCoordClientInterface) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest, opts ...grpc.CallOption) (*querypb.GetSegmentInfoResponse, error) {
	panic("implement me")
}

// LoadBalance migrate the sealed segments on the source node to the dst nodes.
func (c *MockMixCoordClientInterface) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

// ShowConfigurations gets specified configurations para of QueryCoord
// func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
// 	req = typeutil.Clone(req)
// 	commonpbutil.UpdateMsgBase(
// 		req.GetBase(),
// 		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID(), commonpbutil.WithTargetID(c.grpcClient.GetNodeID())),
// 	)
// 	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*internalpb.ShowConfigurationsResponse, error) {
// 		return client.ShowConfigurations(ctx, req)
// 	})
// }

// GetReplicas gets the replicas of a certain collection.
func (c *MockMixCoordClientInterface) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicasResponse, error) {
	panic("implement me")
}

// GetShardLeaders gets the shard leaders of a certain collection.
func (c *MockMixCoordClientInterface) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
	if c.getShardLeaders != nil {
		return c.getShardLeaders(ctx, req)
	}
	return &querypb.GetShardLeadersResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "not implemented",
		},
	}, nil
}

func (c *MockMixCoordClientInterface) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest, opts ...grpc.CallOption) (*querypb.DescribeResourceGroupResponse, error) {
	if c.describeResourceGroup != nil {
		return c.describeResourceGroup(ctx, req)
	}
	return &querypb.DescribeResourceGroupResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (c *MockMixCoordClientInterface) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (c *MockMixCoordClientInterface) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListResourceGroupsResponse, error) {
	if c.listResourceGroups != nil {
		return c.listResourceGroups(ctx, req)
	}

	return &milvuspb.ListResourceGroupsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}

func (c *MockMixCoordClientInterface) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest, opts ...grpc.CallOption) (*querypb.ListCheckersResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest, opts ...grpc.CallOption) (*querypb.ListQueryNodeResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest, opts ...grpc.CallOption) (*querypb.GetQueryNodeDistributionResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest, opts ...grpc.CallOption) (*querypb.CheckBalanceStatusResponse, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	panic("implement me")
}

func (c *MockMixCoordClientInterface) Close() error {
	panic("implement me")
}

// Simulate the cache path and the
func TestMetaCache_GetCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
	id, err = globalMetaCache.GetCollectionID(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection2",
	})

	// test to get from cache, this should trigger root request
	id, err = globalMetaCache.GetCollectionID(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
}

func TestMetaCache_GetCollectionByAliasHitsCache(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection1_alias")
	assert.NoError(t, err)
	assert.Equal(t, typeutil.UniqueID(1), id)
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1_alias")
	assert.NoError(t, err)
	assert.Equal(t, "collection1", schema.GetName())
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1_alias", 0)
	assert.NoError(t, err)
	assert.Equal(t, typeutil.UniqueID(1), info.collID)
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	metaCache := globalMetaCache.(*MetaCache)
	metaCache.mu.RLock()
	defer metaCache.mu.RUnlock()
	_, aliasCachedAsCollection := metaCache.nameIdx[dbName]["collection1_alias"]
	assert.False(t, aliasCachedAsCollection, "the alias must not be registered as a collection-name hint")
	assert.Equal(t, "collection1", metaCache.aliasInfo[dbName]["collection1_alias"].collectionName)
}

func TestMetaCache_GetBasicCollectionInfo(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	// should be no data race.
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
		assert.NoError(t, err)
		assert.Equal(t, info.collID, int64(1))
		_ = info.consistencyLevel
		_ = info.createdTimestamp
		_ = info.createdUtcTimestamp
	}()
	go func() {
		defer wg.Done()
		info, err := globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
		assert.NoError(t, err)
		assert.Equal(t, info.collID, int64(1))
		_ = info.consistencyLevel
		_ = info.createdTimestamp
		_ = info.createdUtcTimestamp
	}()
	wg.Wait()
}

func TestMetaCacheGetCollectionWithUpdate(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil)
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)
	t.Run("update with name", func(t *testing.T) {
		rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
			Schema: &schemapb.CollectionSchema{
				Name: "bar",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: 1,
						Name:    "p",
					},
					{
						FieldID: 100,
						Name:    "pk",
					},
				},
			},
			ShardsNum:            1,
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1"},
			VirtualChannelNames:  []string{"by-dev-rootcoord-dml_1_1v0"},
		}, nil).Once()
		c, err := globalMetaCache.GetCollectionInfo(ctx, "foo", "bar", 1)
		assert.NoError(t, err)
		assert.Equal(t, c.collID, int64(1))
		assert.Equal(t, c.schema.Name, "bar")
	})

	t.Run("update with name", func(t *testing.T) {
		rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
			Schema: &schemapb.CollectionSchema{
				Name: "bar",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID: 1,
						Name:    "p",
					},
					{
						FieldID: 100,
						Name:    "pk",
					},
				},
			},
			ShardsNum:            1,
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1"},
			VirtualChannelNames:  []string{"by-dev-rootcoord-dml_1_1v0"},
		}, nil).Once()
		c, err := globalMetaCache.GetCollectionInfo(ctx, "foo", "hoo", 0)
		assert.NoError(t, err)
		assert.Equal(t, c.collID, int64(1))
		assert.Equal(t, c.schema.Name, "bar")
	})
}

func TestMetaCache_InitCache(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil).Once()
		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
	})

	t.Run("failed to list policy", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(
			&internalpb.ListPolicyResponse{Status: merr.Status(errors.New("mock list policy error"))},
			nil).Once()
		err := InitMetaCache(ctx, rootCoord)
		assert.Error(t, err)
	})

	t.Run("rpc error", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(
			nil, errors.New("mock list policy rpc errorr")).Once()
		err := InitMetaCache(ctx, rootCoord)
		assert.Error(t, err)
	})
}

// TestMetaCache_ByIDIndexRealDBBucket verifies that by-id lookups are served
// from the cluster-wide id index and that fills land under the collection's
// actual database: same-name collections in different databases no longer
// evict each other, and a by-id fill is shared with later by-name lookups.
func TestMetaCache_ByIDIndexRealDBBucket(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		resp := &milvuspb.DescribeCollectionResponse{Status: merr.Success(), RequestTime: 100}
		switch {
		case req.GetCollectionID() == 101 || (req.GetDbName() == "db1" && req.GetCollectionName() == "foo"):
			resp.CollectionID = 101
			resp.DbName = "db1"
			resp.DbId = 1
			resp.Schema = &schemapb.CollectionSchema{Name: "foo"}
		case req.GetCollectionID() == 202 || (req.GetDbName() == "db2" && req.GetCollectionName() == "foo"):
			resp.CollectionID = 202
			resp.DbName = "db2"
			resp.DbId = 2
			resp.Schema = &schemapb.CollectionSchema{Name: "foo"}
		default:
			return nil, merr.WrapErrCollectionNotFound(req.GetCollectionName())
		}
		return resp, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	// two same-name collections in different databases, both filled by id only
	name, err := cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	name, err = cache.GetCollectionName(ctx, "", 202)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// repeated by-id lookups hit the id index; the entries do not evict each
	// other even though the names collide
	for i := 0; i < 3; i++ {
		_, err = cache.GetCollectionName(ctx, "", 101)
		assert.NoError(t, err)
		_, err = cache.GetCollectionName(ctx, "", 202)
		assert.NoError(t, err)
	}
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// entries live under their real databases; no "" bucket exists
	cache.mu.RLock()
	assert.Nil(t, cache.nameIdx[""])
	assert.NotNil(t, cachedEntryLocked(cache, "db1", "foo"))
	assert.NotNil(t, cachedEntryLocked(cache, "db2", "foo"))
	assert.Same(t, cachedEntryLocked(cache, "db1", "foo"), collByIDLive(cache, 101))
	assert.Same(t, cachedEntryLocked(cache, "db2", "foo"), collByIDLive(cache, 202))
	cache.mu.RUnlock()

	// a by-name lookup reuses the by-id fill, no extra RPC
	id, err := cache.GetCollectionID(ctx, "db1", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(101), id)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))

	// removal by id drops the index entry, next by-id lookup goes remote
	cache.RemoveCollectionsByID(ctx, 101, 0)
	name, err = cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(3), atomic.LoadInt32(&describeCount))

	// dropping a database drops its entries from the index too
	cache.RemoveDatabase(ctx, "db2", 0)
	cache.mu.RLock()
	assert.Nil(t, collByIDLive(cache, 202))
	assert.NotNil(t, collByIDLive(cache, 101))
	cache.mu.RUnlock()
}

// TestMetaCache_ByIDEmptyDBNameCachedWithoutNameHint verifies that when an
// id-only describe comes back without a real db name (e.g. an older rootcoord
// during a rolling upgrade), the entry IS cached in the id-keyed primary store
// (ids are cluster-unique, so serving it to by-id lookups is always safe), but
// gets NO name hint: a later default.<name> by-name lookup must still miss and
// issue its own describe, preserving the cross-db mis-hit protection.
func TestMetaCache_ByIDEmptyDBNameCachedWithoutNameHint(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		// older rootcoord: resolves the name but does not report the real db
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 101,
			DbName:       "",
			Schema:       &schemapb.CollectionSchema{Name: "foo"},
			RequestTime:  100,
		}, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	// the id-only lookup resolves the name from the response
	name, err := cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount))

	// repeat by-id lookups are cache HITS (the primary store is id-keyed, an
	// empty response db cannot make an id ambiguous)
	name, err = cache.GetCollectionName(ctx, "", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount))

	// the entry is live by id, but no name hint was planted under a guessed
	// bucket — a by-name resolution of ""/default.foo would miss
	cache.mu.RLock()
	assert.NotNil(t, collByIDLive(cache, 101))
	assert.Nil(t, cachedEntryLocked(cache, "", "foo"))
	assert.Nil(t, cachedEntryLocked(cache, "default", "foo"))
	cache.mu.RUnlock()

	// a later default.foo BY-NAME lookup must not hit the id-cached entry: it
	// issues its own describe (cross-db mis-hit protection preserved)
	_, err = cache.GetCollectionID(ctx, "default", "foo")
	assert.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))
}

// TestMetaCache_ByIDDefaultedRequestDBNameCachedWithoutNameHint covers the case
// where the request db is not empty but was defaulted by database_interceptor:
// an external id-only lookup has its empty DbName filled with "default" before
// it reaches the cache, so the request db that arrives here is "default", not
// "". That db is still NOT authoritative for an id lookup: the id-only describe
// that comes back without a real db name IS cached in the id-keyed primary
// (safe: served only to by-id lookups) but gets NO name hint under
// default.<name> — a later default.<name> by-name lookup must miss and issue
// its own describe, so it cannot mis-hit a collection that actually lives in
// another database.
func TestMetaCache_ByIDDefaultedRequestDBNameCachedWithoutNameHint(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		// older rootcoord resolves the name but does not report the real db; the
		// collection actually lives in another database (unknown to the proxy)
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 101,
			DbName:       "",
			Schema:       &schemapb.CollectionSchema{Name: "foo"},
			RequestTime:  100,
		}, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	// id-only lookup, but the request db arrives as the interceptor-defaulted
	// "default" (not "") — the name still resolves from the response
	name, err := cache.GetCollectionName(ctx, "default", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount))

	// repeat by-id lookups are cache HITS from the id-keyed primary
	name, err = cache.GetCollectionName(ctx, "default", 101)
	assert.NoError(t, err)
	assert.Equal(t, "foo", name)
	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount))

	// the defaulted request db was not trusted to key a name hint: a by-name
	// resolution of default.foo misses even though the entry is live by id
	cache.mu.RLock()
	assert.NotNil(t, collByIDLive(cache, 101))
	assert.Nil(t, cachedEntryLocked(cache, "default", "foo"))
	cache.mu.RUnlock()

	// a later default.foo BY-NAME lookup issues its own describe instead of
	// silently mis-hitting the id-cached entry
	_, err = cache.GetCollectionID(ctx, "default", "foo")
	assert.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&describeCount))
}

// TestMetaCache_GetCollectionInfoByIDCacheHit verifies GetCollectionInfo serves
// an id-only lookup straight from the cluster-wide by-id index as a cache HIT,
// instead of probing id 0 (an unconditional miss) and routing through
// UpdateByID with a spurious cache-miss metric.
func TestMetaCache_GetCollectionInfoByIDCacheHit(t *testing.T) {
	ctx := context.Background()
	mix := NewMixCoordMock()
	describeCount := int32(0)
	mix.SetDescribeCollectionFunc(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		atomic.AddInt32(&describeCount, 1)
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: 1,
			DbName:       "db1",
			DbId:         3,
			Schema:       &schemapb.CollectionSchema{Name: "foo"},
			RequestTime:  100,
		}, nil
	})
	cache, err := NewMetaCache(mix)
	assert.NoError(t, err)
	defer cache.Close()

	hitCounter := metrics.ProxyCacheStatsCounter.WithLabelValues(paramtable.GetStringNodeID(), "GetCollectionInfo", metrics.CacheHitLabel)
	hitBefore := testutil.ToFloat64(hitCounter)

	// first id-only lookup primes the by-id index (one describe, recorded as a miss)
	info, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, "foo", info.schema.GetName())

	// a second id-only lookup must be served from the by-id index as a HIT, with
	// no additional describe and no cache-miss metric
	info, err = cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), info.collID)

	assert.Equal(t, int32(1), atomic.LoadInt32(&describeCount), "cached id-only lookup should not re-describe")
	assert.Equal(t, float64(1), testutil.ToFloat64(hitCounter)-hitBefore,
		"id-only GetCollectionInfo should record exactly one cache hit, not a miss")
}

func TestMetaCache_EmptyDBNameSharesDefaultEntry(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, "", "collection1")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)
	assert.Equal(t, 1, rootCoord.GetAccessCount())

	// explicit default db, by-id with empty db, and by-id with an unrelated db
	// (ids are cluster-unique, rootcoord ignores the db for by-id describes)
	// are all served by the same entry without further RPCs
	id, err = globalMetaCache.GetCollectionID(ctx, "default", "collection1")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)
	name, err := globalMetaCache.GetCollectionName(ctx, "", 1)
	assert.NoError(t, err)
	assert.Equal(t, "collection1", name)
	name, err = globalMetaCache.GetCollectionName(ctx, "some_other_db", 1)
	assert.NoError(t, err)
	assert.Equal(t, "collection1", name)
	assert.Equal(t, 1, rootCoord.GetAccessCount())
}

func TestMetaCache_GetCollectionName(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	collection, err := globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	// should'nt be accessed to remote root coord.
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
	collection, err = globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 1)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection2")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection2",
	})

	// test to get from cache, this should trigger root request
	collection, err = globalMetaCache.GetCollectionName(ctx, GetCurDBNameFromContextOrDefault(ctx), 1)
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	assert.Equal(t, collection, "collection1")
	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Equal(t, rootCoord.GetAccessCount(), 2)
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
}

func TestMetaCache_GetCollectionFailure(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)
	rootCoord.Error = true

	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.Error(t, err)
	assert.Nil(t, schema)

	rootCoord.Error = false

	schema, err = globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})

	rootCoord.Error = true
	// should be cached with no error
	assert.NoError(t, err)
	EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
		AutoID:    true,
		Fields:    []*schemapb.FieldSchema{},
		Functions: []*schemapb.FunctionSchema{},
		Name:      "collection1",
	})
}

func TestMetaCache_GetNonExistCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetCollectionID(ctx, dbName, "collection3")
	assert.Error(t, err)
	assert.Equal(t, id, int64(0))
	schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection3")
	assert.Error(t, err)
	assert.Nil(t, schema)
}

func TestMetaCache_GetPartitionID(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	id, err := globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(1))
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par2")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(2))
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection2", "par1")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(3))
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection2", "par2")
	assert.NoError(t, err)
	assert.Equal(t, id, typeutil.UniqueID(4))
}

func TestMetaCache_ConcurrentTest1(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	cnt := 100
	getCollectionCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			// GetCollectionSchema will never fail
			schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, "collection1")
			assert.NoError(t, err)
			EqualSchema(t, schema.CollectionSchema, &schemapb.CollectionSchema{
				AutoID:    true,
				Fields:    []*schemapb.FieldSchema{},
				Functions: []*schemapb.FunctionSchema{},
				Name:      "collection1",
			})
			time.Sleep(10 * time.Millisecond)
		}
	}

	getPartitionCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			// GetPartitions may fail
			globalMetaCache.GetPartitions(ctx, dbName, "collection1")
			time.Sleep(10 * time.Millisecond)
		}
	}

	invalidCacheFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			// periodically invalid collection cache
			globalMetaCache.RemoveCollection(ctx, dbName, "collection1", 0)
			time.Sleep(10 * time.Millisecond)
		}
	}

	wg.Add(1)
	go getCollectionCacheFunc(&wg)

	wg.Add(1)
	go invalidCacheFunc(&wg)

	wg.Add(1)
	go getPartitionCacheFunc(&wg)
	wg.Wait()
}

func TestMetaCache_GetPartitionError(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	// Test the case where ShowPartitionsResponse is not aligned
	id, err := globalMetaCache.GetPartitionID(ctx, dbName, "errorCollection", "par1")
	assert.Error(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))

	partitions, err2 := globalMetaCache.GetPartitions(ctx, dbName, "errorCollection")
	assert.NotNil(t, err2)
	assert.Equal(t, len(partitions), 0)

	// Test non existed tables
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "nonExisted", "par1")
	assert.Error(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))

	// Test non existed partition
	id, err = globalMetaCache.GetPartitionID(ctx, dbName, "collection1", "par3")
	assert.Error(t, err)
	assert.Equal(t, id, typeutil.UniqueID(0))
}

func TestMetaCache_GetShard(t *testing.T) {
	t.Skip("GetShard has been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_ClearShards(t *testing.T) {
	t.Skip("DeprecateShardCache has been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_PolicyInfo(t *testing.T) {
	client := &MockMixCoordClientInterface{}

	t.Run("InitMetaCache", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, errors.New("mock error")
		}
		err := InitMetaCache(context.Background(), client)
		assert.Error(t, err)

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
			}, nil
		}
		err = InitMetaCache(context.Background(), client)
		assert.NoError(t, err)
	})

	t.Run("GetPrivilegeInfo", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2")},
			}, nil
		}
		err := InitMetaCache(context.Background(), client)
		assert.NoError(t, err)
		policyInfos := privilege.GetPrivilegeCache().GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))
		roles := privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Equal(t, 2, len(roles))
	})

	t.Run("GetPrivilegeInfo", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2")},
			}, nil
		}
		err := InitMetaCache(context.Background(), client)
		assert.NoError(t, err)

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos := privilege.GetPrivilegeCache().GetPrivilegeInfo(context.Background())
		assert.Equal(t, 4, len(policyInfos))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRevokePrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos = privilege.GetPrivilegeCache().GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles := privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Equal(t, 3, len(roles))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles = privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Equal(t, 2, len(roles))

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: ""})
		assert.Error(t, err)
		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: 100, OpKey: "policyX"})
		assert.Error(t, err)
	})

	t.Run("Delete user or drop role", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role2", "Collection", "collection1", "read", "default"),
					"policy2",
					"policy3",
				},
				UserRoles: []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2"), funcutil.EncodeUserRoleCache("foo2", "role3")},
			}, nil
		}
		err := InitMetaCache(context.Background(), client)
		assert.NoError(t, err)

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDeleteUser, OpKey: "foo"})
		assert.NoError(t, err)

		roles := privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Len(t, roles, 0)

		roles = privilege.GetPrivilegeCache().GetUserRole("foo2")
		assert.Len(t, roles, 2)

		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDropRole, OpKey: "role2"})
		assert.NoError(t, err)
		roles = privilege.GetPrivilegeCache().GetUserRole("foo2")
		assert.Len(t, roles, 1)
		assert.Equal(t, "role3", roles[0])

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2"), funcutil.EncodeUserRoleCache("foo2", "role3")},
			}, nil
		}
		err = privilege.GetPrivilegeCache().RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh})
		assert.NoError(t, err)
		roles = privilege.GetPrivilegeCache().GetUserRole("foo")
		assert.Len(t, roles, 2)
	})
}

func TestMetaCache_RemoveCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	rootCoord.showLoadCollections = func(ctx context.Context, in *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status:              merr.Success(),
			CollectionIDs:       []UniqueID{1, 2},
			InMemoryPercentages: []int64{100, 50},
		}, nil
	}

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 1)

	globalMetaCache.RemoveCollection(ctx, dbName, "collection1", 0)
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 2)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1), 100)
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 3)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1), 100)
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.GetAccessCount(), 4)
}

func TestGlobalMetaCache_ShuffleShardLeaders(t *testing.T) {
	t.Skip("shardLeaders and nodeInfo have been moved to shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestMetaCache_Database(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)
	assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	_, err = GetCachedCollectionSchema(ctx, dbName, "collection1")
	assert.NoError(t, err)
	assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), true)
	assert.Equal(t, CheckDatabase(ctx, dbName), true)
}

func TestGetDatabaseInfo(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		cache, err := NewMetaCache(rootCoord)
		assert.NoError(t, err)

		rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{
			Status: merr.Success(),
			DbID:   1,
			DbName: "default",
		}, nil).Once()
		{
			dbInfo, err := cache.GetDatabaseInfo(ctx, "default")
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(1), dbInfo.dbID)
		}

		{
			dbInfo, err := cache.GetDatabaseInfo(ctx, "default")
			assert.NoError(t, err)
			assert.Equal(t, UniqueID(1), dbInfo.dbID)
		}
	})

	t.Run("error", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		cache, err := NewMetaCache(rootCoord)
		assert.NoError(t, err)

		rootCoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{
			Status: merr.Status(errors.New("mock error: describe database")),
		}, nil).Once()
		_, err = cache.GetDatabaseInfo(ctx, "default")
		assert.Error(t, err)
	})
}

func TestMetaCache_AllocID(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(nil),
			ID:     11198,
			Count:  10,
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.NoError(t, err)
		assert.Equal(t, id, int64(11198))
	})

	t.Run("error", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(nil),
		}, errors.New("mock error"))
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})

	t.Run("failed", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(errors.New("mock failed")),
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})
}

func TestMetaCache_InvalidateShardLeaderCache(t *testing.T) {
	t.Skip("GetShard and InvalidateShardLeaderCache have been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestSchemaInfo_GetLoadFieldIDs(t *testing.T) {
	type testCase struct {
		tag              string
		schema           *schemapb.CollectionSchema
		loadFields       []string
		skipDynamicField bool
		expectResult     []int64
		expectErr        bool
	}

	rowIDField := &schemapb.FieldSchema{
		FieldID:  common.RowIDField,
		Name:     common.RowIDFieldName,
		DataType: schemapb.DataType_Int64,
	}
	timestampField := &schemapb.FieldSchema{
		FieldID:  common.TimeStampField,
		Name:     common.TimeStampFieldName,
		DataType: schemapb.DataType_Int64,
	}
	pkField := &schemapb.FieldSchema{
		FieldID:      common.StartOfUserFieldID,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	scalarField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
	}
	scalarFieldSkipLoad := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "text",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.FieldSkipLoadKey, Value: "true"},
		},
	}
	partitionKeyField := &schemapb.FieldSchema{
		FieldID:        common.StartOfUserFieldID + 2,
		Name:           "part_key",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	vectorField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 3,
		Name:     "vector",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "768"},
		},
	}
	dynamicField := &schemapb.FieldSchema{
		FieldID:   common.StartOfUserFieldID + 4,
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}
	clusteringKeyField := &schemapb.FieldSchema{
		FieldID:         common.StartOfUserFieldID + 5,
		Name:            "clustering_key",
		DataType:        schemapb.DataType_Int32,
		IsClusteringKey: true,
	}

	subIntField := &schemapb.FieldSchema{
		FieldID:     common.StartOfUserFieldID + 7,
		Name:        "sub_int",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int32,
	}
	subFloatVectorField := &schemapb.FieldSchema{
		FieldID:     common.StartOfUserFieldID + 8,
		Name:        "sub_float_vector",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "768"},
		},
	}
	structArrayField := &schemapb.StructArrayFieldSchema{
		FieldID: common.StartOfUserFieldID + 6,
		Name:    "struct_array",
		Fields: []*schemapb.FieldSchema{
			subIntField,
			subFloatVectorField,
		},
	}

	testCases := []testCase{
		{
			tag: "default",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{},
			expectErr:        false,
		},
		{
			tag: "default_from_schema",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarFieldSkipLoad,
					partitionKeyField,
					vectorField,
					dynamicField,
					clusteringKeyField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4, common.StartOfUserFieldID + 5},
			expectErr:        false,
		},
		{
			tag: "load_fields",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
					clusteringKeyField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"pk", "part_key", "vector", "clustering_key"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 4, common.StartOfUserFieldID + 5},
			expectErr:        false,
		},
		{
			tag: "load_fields_skip_dynamic",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"pk", "part_key", "vector"},
			skipDynamicField: true,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3},
			expectErr:        false,
		},
		{
			tag: "pk_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"part_key", "vector"},
			skipDynamicField: true,
			expectErr:        true,
		},
		{
			tag: "part_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"pk", "vector"},
			skipDynamicField: true,
			expectErr:        true,
		},
		{
			tag: "vector_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					dynamicField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields:       []string{"pk", "part_key"},
			skipDynamicField: true,
			expectErr:        true,
		},
		{
			tag: "clustering_key_not_loaded",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				Functions: []*schemapb.FunctionSchema{},
			},
			loadFields: []string{"pk", "part_key", "vector"},
			expectErr:  true,
		},
		{
			tag: "struct_array_field_default",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{
					structArrayField,
				},
			},
			loadFields:       nil,
			skipDynamicField: false,
			expectResult:     []int64{},
			expectErr:        false,
		},
		{
			tag: "load_struct_array_field",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{
					structArrayField,
				},
			},
			loadFields:       []string{"pk", "part_key", "clustering_key", "struct_array"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 5, common.StartOfUserFieldID + 7, common.StartOfUserFieldID + 8},
			expectErr:        false,
		},
		{
			tag: "load_struct_array_field_with_vector",
			schema: &schemapb.CollectionSchema{
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					rowIDField,
					timestampField,
					pkField,
					scalarField,
					partitionKeyField,
					vectorField,
					clusteringKeyField,
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{
					structArrayField,
				},
			},
			loadFields:       []string{"pk", "part_key", "clustering_key", "vector", "struct_array"},
			skipDynamicField: false,
			expectResult:     []int64{common.StartOfUserFieldID, common.StartOfUserFieldID + 2, common.StartOfUserFieldID + 3, common.StartOfUserFieldID + 5, common.StartOfUserFieldID + 7, common.StartOfUserFieldID + 8},
			expectErr:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			info := newSchemaInfo(tc.schema)

			result, err := info.GetLoadFieldIDs(tc.loadFields, tc.skipDynamicField)
			if tc.expectErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.expectResult, result)
		})
	}
}

// TestMetaCache_FillInvalidateOrdering replaces the old version-floor test
// (TestMetaCache_Parallel): cache fills are ordered against invalidations by
// fillMu instead of a per-collection version comparison. A fill whose describe
// RPC is in flight when an invalidation arrives must be DRAINED by the
// invalidation — its (possibly pre-DDL) write-back lands before the eviction
// runs, so the eviction cleans it and a stale snapshot can never outlive the
// DDL that invalidated it.
func TestMetaCache_FillInvalidateOrdering(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			// first call parks in the "RPC in flight" state until released
			once.Do(func() {
				close(entered)
				<-gate
			})
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 111,
				DbName:       dbName,
				Schema:       &schemapb.CollectionSchema{Name: "collection1", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  100, // pre-DDL snapshot
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// start a fill and park it inside the describe RPC
	fillDone := make(chan struct{})
	go func() {
		defer close(fillDone)
		_, err := cache.GetCollectionInfo(ctx, dbName, "collection1", 0)
		assert.NoError(t, err)
	}()
	<-entered

	// the invalidation must WAIT for the in-flight fill
	invalDone := make(chan struct{})
	go func() {
		defer close(invalDone)
		cache.RemoveCollectionsByID(ctx, 111, 150)
	}()
	select {
	case <-invalDone:
		t.Fatal("invalidation must drain the in-flight fill before evicting")
	case <-time.After(100 * time.Millisecond):
	}

	// release the fill: its write-back lands first, then the eviction runs
	close(gate)
	<-fillDone
	<-invalDone

	cache.mu.RLock()
	assert.Nil(t, cachedEntryLocked(cache, dbName, "collection1"), "the drained fill's pre-DDL write-back must not survive the eviction")
	assert.Nil(t, collByIDLive(cache, 111))
	cache.mu.RUnlock()
	assertMetaCacheByIDConsistent(t, cache)
}

func TestMetaCache_GetShardLeaderList(t *testing.T) {
	t.Skip("GetShardLeaderList has been moved to ShardClientMgr in shardclient package")
	// Test body removed - functionality moved to shardclient package
}

func TestVersionCache(t *testing.T) {
	t.Run("Lookup_Miss", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()
		entry, ok, release := cache.Lookup("key1")
		assert.False(t, ok)
		assert.Nil(t, entry)
		release(entry)
	})

	t.Run("Insert_And_Lookup", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()
		value := 100
		entry, release := cache.Insert("key1", &value, 1000)
		assert.NotNil(t, entry)
		assert.Equal(t, 100, *entry.value)
		release(entry)

		entry2, ok, release2 := cache.Lookup("key1")
		assert.True(t, ok)
		assert.Equal(t, 100, *entry2.value)
		release2(entry2)
	})

	t.Run("Insert_Higher_Version_Overwrites", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()
		value1 := 100
		entry1, release1 := cache.Insert("key1", &value1, 1000)
		release1(entry1)

		value2 := 200
		entry2, release2 := cache.Insert("key1", &value2, 2000)
		assert.Equal(t, 200, *entry2.value)
		release2(entry2)

		entry3, ok, release3 := cache.Lookup("key1")
		assert.True(t, ok)
		assert.Equal(t, 200, *entry3.value)
		release3(entry3)
	})

	t.Run("Insert_Lower_Version_Ignored", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()
		value1 := 100
		entry1, release1 := cache.Insert("key1", &value1, 2000)
		release1(entry1)

		value2 := 200
		entry2, release2 := cache.Insert("key1", &value2, 1000)
		assert.Equal(t, 100, *entry2.value)
		release2(entry2)
	})

	t.Run("Stale_Erase_When_No_Refs", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()
		value := 100
		entry, release := cache.Insert("key1", &value, 1000)
		release(entry)

		cache.Stale("key1", 2000)

		_, ok, release2 := cache.Lookup("key1")
		assert.False(t, ok)
		release2(nil)
	})

	t.Run("Stale_Marks_Entry_With_Active_Refs", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()
		value := 100
		entry, release := cache.Insert("key1", &value, 1000)
		assert.Equal(t, EntryStateActive, entry.state)

		cache.Stale("key1", 2000)

		// Entry should be marked as stale
		entry2, ok, release2 := cache.Lookup("key1")
		assert.True(t, ok)
		assert.Equal(t, EntryStateStale, entry2.state)
		release2(entry2)

		release(entry)

		// After release, Stale should erase
		cache.Stale("key1", 3000)
		_, ok, release3 := cache.Lookup("key1")
		assert.False(t, ok)
		release3(nil)
	})

	t.Run("Prune_Only_Stale_Entries", func(t *testing.T) {
		cache := NewVersionCache[string, *int]()

		// key1: Active, ref=0 - should NOT be pruned
		value1 := 100
		entry1, release1 := cache.Insert("key1", &value1, 1000)
		release1(entry1)

		// key2: Stale, ref=0 - should be pruned
		value2 := 200
		entry2, release2 := cache.Insert("key2", &value2, 2000)
		release2(entry2)
		cache.Stale("key2", 2500)

		// key3: Stale, ref=1 - should NOT be pruned
		value3 := 300
		entry3, _ := cache.Insert("key3", &value3, 3000)
		cache.Stale("key3", 3500)

		cache.Prune()

		// key1 should still exist (Active, ref=0)
		entry1Found, ok1, release1Found := cache.Lookup("key1")
		assert.True(t, ok1)
		assert.Equal(t, 100, *entry1Found.value)
		release1Found(entry1Found)

		// key2 should be gone (Stale, ref=0)
		_, ok2, release2Found := cache.Lookup("key2")
		assert.False(t, ok2)
		release2Found(nil)

		// key3 should still exist (Stale, ref=1)
		entry3Found, ok3, release3Found := cache.Lookup("key3")
		assert.True(t, ok3)
		assert.Equal(t, EntryStateStale, entry3Found.state)
		release3Found(entry3Found)

		// Release the ref on entry3
		release1(entry3)
	})
}

func TestMetaCache_GetPartitionInfo_CacheHit(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// the partition getter resolves the collection (real name + canonical db)
	// before keying the partition cache; the second lookup hits the collection cache
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Once()

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info1, err := cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), info1.partitionID)
	assert.Equal(t, "par1", info1.name)

	info2, err := cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, info1, info2)
}

func TestMetaCache_GetPartitionInfo_DefaultPartition(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Once()

	defaultPartitionName := Params.CommonCfg.DefaultPartitionName.GetValue()
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.MatchedBy(func(req *milvuspb.ShowPartitionsRequest) bool {
		return len(req.PartitionNames) == 0
	})).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{1},
		PartitionNames:       []string{defaultPartitionName},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info, err := cache.GetPartitionInfo(ctx, "db", "collection", "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.partitionID)
	assert.Equal(t, defaultPartitionName, info.name)
}

func TestMetaCache_GetPartitionInfo_Error(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Once()

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(nil, errors.New("connection failed")).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.Error(t, err)
}

func TestMetaCache_GetPartitionInfos_CacheHit(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// one describe for the name resolution (fills the collection cache) and one inside the
	// collection-level partition fill; the second GetPartitionInfos hits both caches
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema: &schemapb.CollectionSchema{
			Name:   "collection",
			Fields: []*schemapb.FieldSchema{},
		},
		RequestTime: 1000,
	}, nil).Times(2)

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100, 101},
		PartitionNames:       []string{"par1", "par2"},
		CreatedTimestamps:    []uint64{1000, 1001},
		CreatedUtcTimestamps: []uint64{1000, 1001},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	infos1, err := cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(infos1.partitionInfos))

	infos2, err := cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	assert.Equal(t, infos1, infos2)
}

func TestMetaCache_RemovePartition(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// Two describes: the initial resolution, and the re-resolution after the
	// partition DDL staled the collection entry.
	describeCalls := 0
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCalls++
			requestTime := uint64(1000)
			if describeCalls > 1 {
				requestTime = 3000
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				// A real rootcoord always reports the collection's db, so this
				// exercises the normal (cached-collection) invalidation path. The
				// older-rootcoord empty-DbName upgrade path is covered by
				// TestMetaCache_RemovePartitionViaAliasInvalidatesRealNameOnEmptyDBName.
				DbName: "db",
				Schema: &schemapb.CollectionSchema{
					Name:   "collection",
					Fields: []*schemapb.FieldSchema{},
				},
				Aliases:     []string{"alias"},
				RequestTime: requestTime,
			}, nil
		}).Times(2)

	// Two fills: the initial one, and the refetch after the partition DDL staled
	// the exact real-name key. The alias lookups resolve to the real name and hit
	// the same entries, so they trigger no extra RPC.
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Times(2)

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	_, err = cache.GetPartitionInfo(ctx, "db", "alias", "par1")
	assert.NoError(t, err)

	cache.RemovePartition(ctx, "db", 1, "alias", "par1", 2000)

	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	_, err = cache.GetPartitionInfo(ctx, "db", "alias", "par1")
	assert.NoError(t, err)
}

// TestMetaCache_RemovePartitionViaAliasInvalidatesRealNameOnEmptyDBName is a
// regression test for the rolling-upgrade case: an older rootcoord omits DbName
// from describe responses. A partition DDL issued via an alias must still
// invalidate the partition entries populated through the real name — under the
// id-keyed partition cache this holds because both spellings resolve to the
// same collection id, no matter how the entry was primed.
func TestMetaCache_RemovePartitionViaAliasInvalidatesRealNameOnEmptyDBName(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// older rootcoord: the describe resolves the real name + aliases but reports
	// no db name (for a by-name fill the request db is authoritative instead).
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "",
		Schema: &schemapb.CollectionSchema{
			Name:   "collection",
			Fields: []*schemapb.FieldSchema{},
		},
		Aliases:     []string{"alias"},
		RequestTime: 1000,
	}, nil).Maybe()

	var mu sync.Mutex
	showByName := map[string]int{}
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			mu.Lock()
			showByName[req.GetCollectionName()]++
			mu.Unlock()
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{"par1"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// a data-plane access primed the partition cache via the REAL collection
	// name (keyed by the resolved collection id)
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	// a DropPartition issued via the alias arrives: collectionID is the real id,
	// collectionName is the alias
	cache.RemovePartition(ctx, "db", 1, "alias", "par1", 2000)

	// the real-name partition entry must have been invalidated too, so this
	// re-fetches (a second ShowPartitions for "collection") rather than serving
	// the stale cached partition
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, showByName["collection"],
		"real-name partition cache should be invalidated by an alias-based partition DDL even when the collection is uncached (empty DbName)")
}

func TestMetaCache_RemovePartitionInvalidatesCollectionInfo(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	describeCalls := 0
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCalls++
			numPartitions := int64(1)
			requestTime := uint64(1000)
			if describeCalls > 1 {
				numPartitions = 2
				requestTime = 3000
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:        merr.Success(),
				CollectionID:  1,
				DbName:        "db",
				NumPartitions: numPartitions,
				RequestTime:   requestTime,
				Schema: &schemapb.CollectionSchema{
					Name:   "collection",
					Fields: []*schemapb.FieldSchema{},
				},
			}, nil
		}).Times(2)

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info, err := cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.numPartitions)

	cache.RemovePartition(ctx, "db", 1, "collection", "par1", 2000)

	info, err = cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), info.numPartitions)
}

// TestMetaCache_RemovePartitionStalesCollectionInfoUnconditionally: with fills
// serialized against invalidations by fillMu, partition DDL no longer compares a
// version floor — it always stales the collection entry. A duplicate or late
// broadcast at worst causes one extra re-describe (over-invalidation), never a
// stale read. (This intentionally replaces the old KeepsCollectionInfoForStale-
// Version behavior, whose floor was removed together with collectionCacheVersion.)
func TestMetaCache_RemovePartitionStalesCollectionInfoUnconditionally(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	describeCalls := 0
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCalls++
			return &milvuspb.DescribeCollectionResponse{
				Status:        merr.Success(),
				CollectionID:  1,
				DbName:        "db",
				NumPartitions: 1,
				RequestTime:   1000,
				Schema: &schemapb.CollectionSchema{
					Name:   "collection",
					Fields: []*schemapb.FieldSchema{},
				},
			}, nil
		}).Times(2)

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	info, err := cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.numPartitions)

	// even an older-timestamp partition broadcast evicts the collection entry
	cache.RemovePartition(ctx, "db", 1, "collection", "par1", 500)

	info, err = cache.GetCollectionInfo(ctx, "db", "collection", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.numPartitions)
	assert.Equal(t, 2, describeCalls, "the entry was staled, so the lookup re-describes")
}

func TestMetaCache_PartitionCache_Concurrent(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// name resolution before keying; concurrent resolvers merge via the
	// collection singleflight, so no fixed count
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "db",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Maybe()

	var callCount atomic.Int32
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			callCount.Add(1)
			time.Sleep(50 * time.Millisecond)
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{"par1"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	numGoroutines := 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.GetPartitionInfo(ctx, "db", "collection", "par1")
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(1), callCount.Load(), "Singleflight should merge concurrent requests")
}

func TestMetaCache_GetPartitionInfos_SingleflightKeyIncludesDatabase(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			time.Sleep(50 * time.Millisecond)
			// distinct collections in distinct dbs => distinct cluster-unique ids
			id := int64(1)
			if req.GetDbName() == "db2" {
				id = 2
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       req.GetDbName(),
				Schema: &schemapb.CollectionSchema{
					Name:   req.GetCollectionName(),
					Fields: []*schemapb.FieldSchema{},
				},
				RequestTime: 1000,
			}, nil
		}).Times(4) // per db: one name resolution + one inside the partition fill

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{req.GetDbName() + "_par"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		}).Twice()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	type result struct {
		db    string
		infos *partitionInfos
		err   error
	}

	results := make(chan result, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for _, db := range []string{"db1", "db2"} {
		wg.Add(1)
		go func(db string) {
			defer wg.Done()
			<-start
			infos, err := cache.GetPartitionInfos(ctx, db, "collection")
			results <- result{
				db:    db,
				infos: infos,
				err:   err,
			}
		}(db)
	}

	close(start)
	wg.Wait()
	close(results)

	for result := range results {
		assert.NoError(t, result.err)
		if assert.NotNil(t, result.infos) && assert.Len(t, result.infos.partitionInfos, 1) {
			assert.Equal(t, result.db+"_par", result.infos.partitionInfos[0].name)
		}
	}
}

// TestMetaCache_GetPartitionInfosNormalizesEmptyDB is a regression test for the
// collection/partition cache asymmetry: the collection cache normalizes an empty
// db to "default", so the partition cache must too. Otherwise an empty-db request
// would key partitions under ""/<coll> while a drop/recreate invalidation keyed
// on the canonical "default" bucket never sweeps them, leaving stale entries. The
// Once() expectations assert both lookups share one normalized cache bucket.
func TestMetaCache_GetPartitionInfosNormalizesEmptyDB(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// two describes on the first call (name resolution + partition fill); the
	// explicit-default call hits the caches, proving both requests share the
	// canonical bucket
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "default",
		Schema: &schemapb.CollectionSchema{
			Name:   "collection",
			Fields: []*schemapb.FieldSchema{},
		},
		RequestTime: 1000,
	}, nil).Times(2)

	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// populate the collection-level partition cache via an empty-db request
	infos1, err := cache.GetPartitionInfos(ctx, "", "collection")
	assert.NoError(t, err)
	assert.Len(t, infos1.partitionInfos, 1)

	// the explicit-default request must hit the same canonical bucket with no
	// extra RPC; without normalization it would miss and exceed the Once() mocks
	infos2, err := cache.GetPartitionInfos(ctx, "default", "collection")
	assert.NoError(t, err)
	assert.Equal(t, infos1, infos2)
}

// TestMetaCache_GetPartitionInfoNormalizesEmptyDB is the partition-name-level
// counterpart: GetPartitionInfo must key its cache under the canonical database
// too, so an empty-db and an explicit-default lookup share one entry.
func TestMetaCache_GetPartitionInfoNormalizesEmptyDB(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	// one name resolution; the explicit-default lookup hits the same cached entry
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 1,
		DbName:       "default",
		Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
		RequestTime:  1000,
	}, nil).Once()

	// ShowPartitions must fire exactly once across both lookups when the cache
	// key is normalized.
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100, 101},
		PartitionNames:       []string{"par1", "par2"},
		CreatedTimestamps:    []uint64{1000, 1001},
		CreatedUtcTimestamps: []uint64{1000, 1001},
	}, nil).Once()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	p1, err := cache.GetPartitionInfo(ctx, "", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), p1.partitionID)

	// explicit default resolves from the same normalized bucket — no extra RPC
	p2, err := cache.GetPartitionInfo(ctx, "default", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, p1, p2)
}

// collByIDLive returns the live primary entry for id (nil when absent or its
// database generation moved on). Callers must hold cache.mu (tests take RLock).
func collByIDLive(cache *MetaCache, id UniqueID) *collectionInfo {
	if e, ok := cache.liveLocked(id); ok {
		return e
	}
	return nil
}

// cachedEntryLocked resolves (db, name) exactly like a read would: name hint ->
// primary, validated (id live, name still matches). nil = a lookup would miss.
// Callers must hold cache.mu.
func cachedEntryLocked(cache *MetaCache, db, name string) *collectionInfo {
	db = normalizeDBName(db)
	ids, ok := cache.nameIdx[db]
	if !ok {
		return nil
	}
	id, ok := ids[name]
	if !ok {
		return nil
	}
	e, ok := cache.liveLocked(id)
	if !ok || e.schema.GetName() != name || normalizeDBName(e.dbName) != db {
		return nil
	}
	return e
}

// seedCollection plants a collection into the primary store + name hint the way
// update() would, for tests that build MetaCache literals. The literal must have
// collections/nameIdx/dbGen maps initialized.
func seedCollection(cache *MetaCache, db, name string, id UniqueID) *collectionInfo {
	info := &collectionInfo{
		collID: id,
		dbName: db,
		schema: newSchemaInfo(&schemapb.CollectionSchema{Name: name}),
		dbGen:  cache.dbGen[db],
	}
	cache.collections[id] = info
	ids, ok := cache.nameIdx[db]
	if !ok {
		ids = make(map[string]UniqueID)
		cache.nameIdx[db] = ids
	}
	ids[name] = id
	return info
}

// assertMetaCacheByIDConsistent verifies primary-store coherence: every primary
// entry's map key equals its collID, and every name hint that VALIDATES resolves
// to a live entry of the hint's database. (Hints may legally dangle -- they are
// lazily validated on read and swept by the GC -- so a dangling hint is not a
// violation.)
func assertMetaCacheByIDConsistent(t *testing.T, cache *MetaCache) {
	t.Helper()
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	for id, e := range cache.collections {
		if !assert.NotNil(t, e) {
			continue
		}
		assert.Equalf(t, id, e.collID, "primary key %d must equal entry.collID", id)
	}
	for db, ids := range cache.nameIdx {
		for name, id := range ids {
			e, ok := cache.collections[id]
			if !ok || e.dbGen != cache.dbGen[e.dbName] {
				continue // dangling hint: legal, lazily swept
			}
			if e.schema.GetName() == name {
				assert.Equalf(t, db, normalizeDBName(e.dbName),
					"hint %q/%q resolves to a live entry of database %q", db, name, e.dbName)
			}
		}
	}
}

// TestMetaCache_RenameOldNameStopsResolving locks in the rename observable:
// after foo -> bar, caching the new name makes the pre-rename name stop
// resolving IMMEDIATELY. There is no eager hint eviction anymore — the old
// nameIdx hint may physically linger, but it fails validation on read (the
// primary entry's real name is now "bar"), so a "foo" lookup misses and
// re-describes. The rename broadcast then only needs to evict the primary
// entry by id.
func TestMetaCache_RenameOldNameStopsResolving(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var renamed atomic.Bool
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			name := "foo"
			rt := uint64(100)
			if renamed.Load() {
				name = "bar"
				rt = 200
			}
			if req.GetCollectionID() == 1 || req.GetCollectionName() == name {
				return &milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 1,
					DbName:       "db",
					Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
					RequestTime:  rt,
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName())),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	id, err := cache.GetCollectionID(ctx, "db", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	renamed.Store(true)
	id, err = cache.GetCollectionID(ctx, "db", "bar")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	// the old name must no longer resolve (the dangling hint fails validation);
	// note: we deliberately do NOT assert on the internal absence of the hint —
	// it may legally dangle until the GC sweeps it
	cache.mu.RLock()
	hasFoo := cachedEntryLocked(cache, "db", "foo") != nil
	hasBar := cachedEntryLocked(cache, "db", "bar") != nil
	cache.mu.RUnlock()
	assert.False(t, hasFoo, "the old name must stop resolving the moment the rename is observed")
	assert.True(t, hasBar)
	assertMetaCacheByIDConsistent(t, cache)

	// PRODUCTION-path check in the pre-broadcast window: a lookup of the old name
	// must go through the real read path, fail the hint validation, re-describe
	// and report not-found -- NOT serve id 1 through the dangling hint. (The
	// helper assertions above re-implement the validation, so only this catches
	// a production regression in getCollection's hint validation.)
	_, err = cache.GetCollectionID(ctx, "db", "foo")
	assert.Error(t, err, "the pre-rename name must not resolve through the dangling hint")

	// the rename broadcast evicts the primary entry by id
	cache.RemoveCollectionsByID(ctx, 1, 0)

	cache.mu.RLock()
	hasFoo = cachedEntryLocked(cache, "db", "foo") != nil
	hasBar = cachedEntryLocked(cache, "db", "bar") != nil
	hasID := collByIDLive(cache, 1) != nil
	cache.mu.RUnlock()
	assert.False(t, hasFoo, "old name must not resolve (no stale read of the pre-rename name)")
	assert.False(t, hasBar, "new name must not resolve either after the eviction")
	assert.False(t, hasID, "the primary entry must be gone")
	assertMetaCacheByIDConsistent(t, cache)

	// real stale-read check: the pre-rename name must not resolve any more -- it
	// re-describes, and since the collection was renamed it is now not found
	// (rather than returning the stale id 1 from a lingering cache entry)
	_, err = cache.GetCollectionID(ctx, "db", "foo")
	assert.Error(t, err, "the pre-rename name must not resolve after invalidation")
	// the new name still resolves correctly
	id, err = cache.GetCollectionID(ctx, "db", "bar")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)
}

// TestMetaCache_AliasRepointResolvesToNewTarget verifies an alias re-point is
// reflected after invalidation: alias -> collA is cached, then AlterAlias points
// it at collB and broadcasts RemoveAlias; the next resolution must return collB,
// not the stale collA. (Guards the alias-cache correctness the RBAC path depends
// on for its object-name resolution.)
func TestMetaCache_AliasRepointResolvesToNewTarget(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	// ResolveCollectionAlias L1 (getCollection) never RPCs; a not-found describe
	// keeps any accidental probe harmless.
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Status(merr.WrapErrCollectionNotFound("")),
	}, nil).Maybe()

	var repointed atomic.Bool
	rootCoord.EXPECT().DescribeAlias(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
			target := "collA"
			if repointed.Load() {
				target = "collB"
			}
			return &milvuspb.DescribeAliasResponse{
				Status:     merr.Success(),
				Alias:      req.GetAlias(),
				Collection: target,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	got, err := cache.ResolveCollectionAlias(ctx, "db", "myalias")
	assert.NoError(t, err)
	assert.Equal(t, "collA", got)

	repointed.Store(true)
	cache.RemoveAlias(ctx, "db", "myalias")

	got, err = cache.ResolveCollectionAlias(ctx, "db", "myalias")
	assert.NoError(t, err)
	assert.Equal(t, "collB", got, "alias must re-resolve to the new target after RemoveAlias")
}

// TestMetaCache_DropCollectionClearsAllCaches verifies a drop makes the
// collection unreachable through every access path. Under the id-primary
// architecture the drop deletes ONLY the primary entry: name/alias hints and
// partition entries survive physically but dangle — hints fail validation on
// read, and partition entries are keyed by the (never reused) collection id, so
// a recreated collection gets a NEW id and refetches everything.
func TestMetaCache_DropCollectionClearsAllCaches(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	// ids are never reused in production: after the drop, the "recreated"
	// collection must be described with a NEW id
	var collID atomic.Int64
	collID.Store(1)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: collID.Load(),
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "collection", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  1000,
			}, nil
		}).Maybe()
	rootCoord.EXPECT().DescribeAlias(mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status:     merr.Success(),
		Alias:      "myalias",
		Collection: "collection",
	}, nil).Maybe()

	var showCount atomic.Int32
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			showCount.Add(1)
			return &milvuspb.ShowPartitionsResponse{
				Status:               merr.Success(),
				PartitionIDs:         []int64{100},
				PartitionNames:       []string{"par1"},
				CreatedTimestamps:    []uint64{1000},
				CreatedUtcTimestamps: []uint64{1000},
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetCollectionID(ctx, "db", "collection")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	target, err := cache.ResolveCollectionAlias(ctx, "db", "myalias")
	assert.NoError(t, err)
	assert.Equal(t, "collection", target)
	assert.Equal(t, int32(2), showCount.Load(), "collection-level and partition-level caches each fetched once")

	cache.mu.RLock()
	hasColl := cachedEntryLocked(cache, "db", "collection") != nil
	hasID := collByIDLive(cache, 1) != nil
	aliasEntry, hasAlias := cache.aliasInfo["db"]["myalias"]
	cache.mu.RUnlock()
	assert.True(t, hasColl)
	assert.True(t, hasID)
	assert.True(t, hasAlias)
	assert.Equal(t, "collection", aliasEntry.collectionName)

	cache.RemoveCollectionsByID(ctx, 1, 0)

	cache.mu.RLock()
	hasColl = cachedEntryLocked(cache, "db", "collection") != nil
	hasID = collByIDLive(cache, 1) != nil
	_, hasAlias = cache.aliasInfo["db"]["myalias"]
	cache.mu.RUnlock()
	assert.False(t, hasColl, "a by-name lookup must miss after the drop")
	assert.False(t, hasID, "the primary entry must be deleted on drop")
	// hints are lazy under the new contract: the alias entry survives physically
	// (the GC sweeps it), but resolving the dropped collection through it misses
	assert.True(t, hasAlias, "alias hints are not cleaned synchronously anymore")
	_, ok := cache.getCollection("db", "myalias", 0)
	assert.False(t, ok, "a lookup through the alias must not reach the dropped collection")
	assertMetaCacheByIDConsistent(t, cache)

	// the recreated collection has a NEW id, so its partition caches miss and
	// re-fetch (the old id's entries are unreachable and swept by the GC)
	collID.Store(2)
	_, err = cache.GetPartitionInfos(ctx, "db", "collection")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfo(ctx, "db", "collection", "par1")
	assert.NoError(t, err)
	assert.Equal(t, int32(4), showCount.Load(), "the recreated collection's partition info must be re-fetched, proving the old entries are unreachable")
}

// TestMetaCache_ConcurrentIDPartitionInvalidateConsistency hammers the cache with
// id-only reads, by-name reads, partition reads, and both invalidation paths
// concurrently. Run under -race it flags any data race; the final consistency
// check flags any primary-store/name-hint desync left behind.
func TestMetaCache_ConcurrentIDPartitionInvalidateConsistency(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			if req.GetCollectionName() != "" {
				fmt.Sscanf(req.GetCollectionName(), "coll%d", &id)
			} else {
				id = req.GetCollectionID()
			}
			if id < 1 || id > 5 {
				return &milvuspb.DescribeCollectionResponse{
					Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName())),
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: fmt.Sprintf("coll%d", id), Fields: []*schemapb.FieldSchema{}},
				RequestTime:  1000,
			}, nil
		})
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status:               merr.Success(),
		PartitionIDs:         []int64{100},
		PartitionNames:       []string{"par1"},
		CreatedTimestamps:    []uint64{1000},
		CreatedUtcTimestamps: []uint64{1000},
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := int64((i % 5) + 1)
				name := fmt.Sprintf("coll%d", id)
				switch (i + g) % 5 {
				case 0:
					// id-only read must never return another collection's name
					if got, err := cache.GetCollectionName(ctx, "", id); err == nil {
						assert.Equalf(t, name, got, "by-id read returned wrong name for id %d", id)
					}
				case 1:
					// by-name read must never return another collection's id
					if got, err := cache.GetCollectionID(ctx, "db", name); err == nil {
						assert.Equalf(t, id, got, "by-name read returned wrong id for %s", name)
					}
				case 2:
					if infos, err := cache.GetPartitionInfos(ctx, "db", name); err == nil {
						assert.NotNil(t, infos)
					}
				case 3:
					cache.RemovePartition(ctx, "db", id, name, "par1", uint64(2000+i))
				case 4:
					cache.RemoveCollectionsByID(ctx, id, 0)
				}
			}
		}(g)
	}
	wg.Wait()

	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_CrossDBSameNameDropIsolation verifies that dropping a collection
// in one database never disturbs a same-name collection in another database: the
// primary entry, name hint, and partition cache of the untouched db must all
// survive.
func TestMetaCache_CrossDBSameNameDropIsolation(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			switch {
			case req.GetCollectionID() == 1 || (req.GetDbName() == "db1" && req.GetCollectionName() == "foo"):
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 1, DbName: "db1", Schema: &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}}, RequestTime: 100}, nil
			case req.GetCollectionID() == 2 || (req.GetDbName() == "db2" && req.GetCollectionName() == "foo"):
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success(), CollectionID: 2, DbName: "db2", Schema: &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}}, RequestTime: 100}, nil
			default:
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
		})
	var mu sync.Mutex
	showByDB := map[string]int{}
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			mu.Lock()
			showByDB[req.GetDbName()]++
			mu.Unlock()
			return &milvuspb.ShowPartitionsResponse{Status: merr.Success(), PartitionIDs: []int64{100}, PartitionNames: []string{"par1"}, CreatedTimestamps: []uint64{100}, CreatedUtcTimestamps: []uint64{100}}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	for _, db := range []string{"db1", "db2"} {
		_, err = cache.GetCollectionID(ctx, db, "foo")
		assert.NoError(t, err)
		_, err = cache.GetPartitionInfos(ctx, db, "foo")
		assert.NoError(t, err)
	}
	mu.Lock()
	assert.Equal(t, 1, showByDB["db1"])
	assert.Equal(t, 1, showByDB["db2"])
	mu.Unlock()

	// drop only db1/foo
	cache.RemoveCollectionsByID(ctx, 1, 0)

	cache.mu.RLock()
	hasDB1 := cachedEntryLocked(cache, "db1", "foo") != nil
	coll2 := cachedEntryLocked(cache, "db2", "foo")
	live1 := collByIDLive(cache, 1)
	live2 := collByIDLive(cache, 2)
	cache.mu.RUnlock()
	assert.False(t, hasDB1, "dropped db1/foo must be gone")
	assert.Nil(t, live1, "dropped id 1 must leave the primary store")
	assert.NotNil(t, coll2, "db2/foo must be untouched")
	assert.NotNil(t, live2)
	assert.Same(t, coll2, live2)
	assertMetaCacheByIDConsistent(t, cache)

	// db2/foo partitions must still be a cache hit (not swept by db1's drop)
	_, err = cache.GetPartitionInfos(ctx, "db2", "foo")
	assert.NoError(t, err)
	mu.Lock()
	assert.Equal(t, 1, showByDB["db2"], "db2 partitions must not be re-fetched after dropping db1/foo")
	mu.Unlock()
}

// TestMetaCache_EmptyDBSharesDefaultForPartitionInvalidation verifies the
// empty-db/default sharing observable end-to-end: partition entries are keyed
// by the collection id, so an empty-db request and an explicit-default request
// resolve to the same collection and share ONE partition cache entry. After a
// drop, the recreated collection carries a NEW id (ids are never reused), so
// the partition lookup misses and re-fetches — the old id's entries are
// unreachable regardless of which db-name spelling populated them.
func TestMetaCache_EmptyDBSharesDefaultForPartitionInvalidation(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	// ids are never reused in production: after the drop, the "recreated"
	// collection is described with a NEW id
	var collID atomic.Int64
	collID.Store(1)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Success(), CollectionID: collID.Load(), DbName: "default", Schema: &schemapb.CollectionSchema{Name: "coll", Fields: []*schemapb.FieldSchema{}}, RequestTime: 100,
			}, nil
		}).Maybe()
	var showCount atomic.Int32
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
			showCount.Add(1)
			return &milvuspb.ShowPartitionsResponse{Status: merr.Success(), PartitionIDs: []int64{100}, PartitionNames: []string{"par1"}, CreatedTimestamps: []uint64{100}, CreatedUtcTimestamps: []uint64{100}}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache the collection (so a later drop can find it) and its partitions via
	// the EMPTY db name; both normalize to "default"
	_, err = cache.GetCollectionID(ctx, "", "coll")
	assert.NoError(t, err)
	_, err = cache.GetPartitionInfos(ctx, "", "coll")
	assert.NoError(t, err)
	assert.Equal(t, int32(1), showCount.Load())

	// an explicit "default" request hits the SAME normalized entry (no re-fetch)
	_, err = cache.GetPartitionInfos(ctx, "default", "coll")
	assert.NoError(t, err)
	assert.Equal(t, int32(1), showCount.Load(), "empty-db and default must share one partition cache entry")

	// a drop broadcast evicts the primary entry by id; the recreate gets a new id
	cache.RemoveCollectionsByID(ctx, 1, 0)
	collID.Store(2)

	// the empty-db lookup must now re-fetch: the recreated collection's new id
	// misses the partition cache, and the old id's entry is unreachable
	_, err = cache.GetPartitionInfos(ctx, "", "coll")
	assert.NoError(t, err)
	assert.Equal(t, int32(2), showCount.Load(), "the recreated collection's partition info must be re-fetched under its new id")
}

// TestMetaCache_ConcurrentAliasResolveInvalidate exercises the alias cache under
// concurrent resolution and invalidation. Run under -race it flags any data race
// or map corruption in the alias path, and asserts resolution never returns a
// garbage value. NOTE: this does NOT prove the C1 stale-write race (a late
// DescribeAlias overwriting a concurrent RemoveAlias) is fixed -- that is a
// separate, deferred correctness issue; this only guards low-level safety.
func TestMetaCache_ConcurrentAliasResolveInvalidate(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Status(merr.WrapErrCollectionNotFound("")),
	}, nil).Maybe()
	var flip atomic.Int64
	rootCoord.EXPECT().DescribeAlias(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
			target := "collA"
			if flip.Add(1)%2 == 0 {
				target = "collB"
			}
			return &milvuspb.DescribeAliasResponse{Status: merr.Success(), Alias: req.GetAlias(), Collection: target}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	valid := []string{"collA", "collB", "myalias"}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if i%3 == 0 {
					cache.RemoveAlias(ctx, "db", "myalias")
				} else {
					got, err := cache.ResolveCollectionAlias(ctx, "db", "myalias")
					if err == nil {
						assert.Containsf(t, valid, got, "alias resolved to a corrupted value %q", got)
					}
				}
			}
		}()
	}
	wg.Wait()
}

// TestMetaCache_DropAliasInvalidatesCanonicalCollectionAndByIDIndex is a
// regression test: an alias DDL broadcasts only the alias name with
// CollectionID=0, so the proxy sees RemoveCollection(db, alias). The collection
// is stored under its real name, so the direct lookup misses. Before the fix the
// canonical entry and its by-id index kept a stale Aliases list that an id-only
// Describe (via the cluster-wide by-id index) would then serve. The proxy must
// resolve the alias to its target and evict it.
func TestMetaCache_DropAliasInvalidatesCanonicalCollectionAndByIDIndex(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var dropped atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var aliases []string
			if !dropped.Load() {
				aliases = []string{"a"}
			}
			if req.GetCollectionID() == 1 || (req.GetDbName() == "db1" && req.GetCollectionName() == "A") {
				return &milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 1,
					DbName:       "db1",
					Schema:       &schemapb.CollectionSchema{Name: "A", Fields: []*schemapb.FieldSchema{}},
					Aliases:      aliases,
					RequestTime:  uint64(reqTime.Add(1)),
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache collection A in the non-default db1 by its real name; its Aliases=[a]
	// must populate the reverse alias index
	info, err := cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, info.aliases)

	cache.mu.RLock()
	hasID := collByIDLive(cache, 1) != nil
	aliasEntry, hasAlias := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	assert.True(t, hasID, "collection must be live in the primary store")
	if assert.True(t, hasAlias, "the collection's alias must be hinted") {
		assert.Equal(t, "A", aliasEntry.collectionName)
	}

	// DropAlias(a) -> the proxy sees RemoveCollection(db1, "a") with the alias name
	dropped.Store(true)
	cache.RemoveCollection(ctx, "db1", "a", 0)

	cache.mu.RLock()
	hasID = collByIDLive(cache, 1) != nil
	hasA := cachedEntryLocked(cache, "db1", "A") != nil
	cache.mu.RUnlock()
	assert.False(t, hasID, "DropAlias must evict the target's primary entry")
	assert.False(t, hasA, "the target must no longer resolve by name")

	// an id-only Describe now re-queries rootcoord and must get the fresh aliases
	// (without the dropped alias), not the stale cached [a]
	info2, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Empty(t, info2.aliases, "id-only describe must not serve the dropped alias")
}

// TestMetaCache_AlterAliasInvalidatesBothOldAndNewTarget verifies AlterAlias
// evicts BOTH the old target (via the proxy resolving the alias name to it) and
// the new target (via the collection id the rootcoord callback now forwards), so
// neither serves a stale Aliases list afterwards.
func TestMetaCache_AlterAliasInvalidatesBothOldAndNewTarget(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var altered atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			name := req.GetCollectionName()
			if req.GetCollectionID() != 0 {
				id = req.GetCollectionID()
				switch id {
				case 1:
					name = "A"
				case 2:
					name = "B"
				}
			} else if name == "A" {
				id = 1
			} else if name == "B" {
				id = 2
			}
			if id != 1 && id != 2 {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			var aliases []string
			// before alter: alias "a" -> A; after: alias "a" -> B
			if (id == 1 && !altered.Load()) || (id == 2 && altered.Load()) {
				aliases = []string{"a"}
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db1",
				Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  uint64(reqTime.Add(1)),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	_, err = cache.GetCollectionInfo(ctx, "db1", "B", 0)
	assert.NoError(t, err)
	cache.mu.RLock()
	aliasEntry, hasAlias := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	if assert.True(t, hasAlias, "alias of the old target must be reverse-indexed") {
		assert.Equal(t, "A", aliasEntry.collectionName)
	}

	// AlterAlias(a: A -> B): the proxy sees RemoveCollection(db1, "a") for the old
	// target plus RemoveCollectionsByID(2) for the new target (id forwarded by the
	// rootcoord callback).
	altered.Store(true)
	cache.RemoveCollection(ctx, "db1", "a", 0)
	cache.RemoveCollectionsByID(ctx, 2, 0)

	cache.mu.RLock()
	hasA := collByIDLive(cache, 1) != nil
	hasB := collByIDLive(cache, 2) != nil
	cache.mu.RUnlock()
	assert.False(t, hasA, "old target A must be evicted")
	assert.False(t, hasB, "new target B must be evicted")
	assertMetaCacheByIDConsistent(t, cache)

	// fresh describes: A no longer reports the alias, B now does
	infoA, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Empty(t, infoA.aliases, "old target must no longer report the moved alias")
	infoB, err := cache.GetCollectionInfo(ctx, "", "", 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, infoB.aliases, "new target must report the moved alias")
}

// TestMetaCache_CreateAliasInvalidatesTargetViaForwardedID verifies CreateAlias
// evicts the target collection even though the proxy's reverse alias index has no
// entry for the brand-new alias (the collection was cached before it existed) --
// it relies on the collection id forwarded by the rootcoord callback.
func TestMetaCache_CreateAliasInvalidatesTargetViaForwardedID(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var created atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var aliases []string
			if created.Load() {
				aliases = []string{"a"}
			}
			if req.GetCollectionID() == 1 || (req.GetDbName() == "db1" && req.GetCollectionName() == "A") {
				return &milvuspb.DescribeCollectionResponse{
					Status:       merr.Success(),
					CollectionID: 1,
					DbName:       "db1",
					Schema:       &schemapb.CollectionSchema{Name: "A", Fields: []*schemapb.FieldSchema{}},
					Aliases:      aliases,
					RequestTime:  uint64(reqTime.Add(1)),
				}, nil
			}
			return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// cache A with no alias yet -> the reverse index has no entry for "a"
	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)
	cache.mu.RLock()
	_, hasAliasRev := cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	assert.False(t, hasAliasRev, "A had no alias, so the reverse index has no entry for a")

	// CreateAlias(a -> A): the proxy's RemoveCollection(db1, "a") cannot resolve a
	// (not reverse-indexed); eviction relies on RemoveCollectionsByID(1) with the
	// forwarded id.
	created.Store(true)
	cache.RemoveCollection(ctx, "db1", "a", 0)
	cache.RemoveCollectionsByID(ctx, 1, 0)

	cache.mu.RLock()
	hasID := collByIDLive(cache, 1) != nil
	cache.mu.RUnlock()
	assert.False(t, hasID, "CreateAlias must evict the target via the forwarded id")

	// id-only Describe now reports the newly created alias
	info, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"a"}, info.aliases, "target must report the newly created alias")
}

// TestMetaCache_DropAliasCleansDanglingReverseIndexWhenTargetUncached covers the
// case where an alias was only ever resolved into aliasInfo (via DescribeAlias)
// without its target collection ever entering the primary store. A DropAlias
// must still clean that dangling alias hint, so a later ResolveCollectionAlias
// (whose Level-2 hit is returned as-is and feeds RBAC object resolution) cannot
// return the stale target. The proxy must not rely on the caller's compensating
// RemoveAlias to do this.
func TestMetaCache_DropAliasCleansDanglingReverseIndexWhenTargetUncached(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Status(merr.WrapErrCollectionNotFound("")),
	}, nil).Maybe()
	rootCoord.EXPECT().DescribeAlias(mock.Anything, mock.Anything).Return(&milvuspb.DescribeAliasResponse{
		Status:     merr.Success(),
		Alias:      "a",
		Collection: "foo",
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// resolving the alias populates aliasInfo[db1][a] -> foo WITHOUT caching
	// the target collection (ResolveCollectionAlias only resolves the name)
	got, err := cache.ResolveCollectionAlias(ctx, "db1", "a")
	assert.NoError(t, err)
	assert.Equal(t, "foo", got)

	cache.mu.RLock()
	_, hasRev := cache.aliasInfo["db1"]["a"]
	hasColl := cachedEntryLocked(cache, "db1", "foo") != nil
	cache.mu.RUnlock()
	assert.True(t, hasRev, "resolve populated the alias hint")
	assert.False(t, hasColl, "the target collection was never cached")

	// DropAlias(a): the proxy sees RemoveCollection(db1, "a"); the target is not
	// in the primary store, so removeCollectionByAliasLocked's dangling branch
	// must delete the alias hint itself.
	cache.RemoveCollection(ctx, "db1", "a", 0)

	cache.mu.RLock()
	_, hasRev = cache.aliasInfo["db1"]["a"]
	cache.mu.RUnlock()
	assert.False(t, hasRev, "the dangling alias hint must be cleaned even when the target is uncached")
}

// TestMetaCache_ConcurrentAlterAliasRefreshEvictsOldTarget is the P2-1 regression:
// under a concurrent AlterAlias(a: A -> B), a Describe of the new target B lands on
// this proxy AFTER the alter commits (so B reports Aliases=[a]) but BEFORE the
// alter's expiration arrives. That Describe overwrites the single-valued alias
// resolution aliasInfo[a] from A to B, while A is still cached holding the stale
// Aliases=[a] -- so the proxy alone cannot resolve the OLD target from the alias
// name. The fix is at the source: rootcoord resolves the pre-alter target under
// its database lock and forwards BOTH ids in the expiration
// (AlterAliasMessageHeader.old_collection_id), so the proxy evicts A and B by id
// regardless of its own alias-resolution state. This test simulates that contract:
// the expiration is RemoveCollection(db, "a") + RemoveCollectionsByID(B) +
// RemoveCollectionsByID(A).
func TestMetaCache_ConcurrentAlterAliasRefreshEvictsOldTarget(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var altered atomic.Bool
	var reqTime atomic.Int64
	reqTime.Store(100)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			var id int64
			name := req.GetCollectionName()
			if req.GetCollectionID() != 0 {
				id = req.GetCollectionID()
				switch id {
				case 1:
					name = "A"
				case 2:
					name = "B"
				}
			} else if name == "A" {
				id = 1
			} else if name == "B" {
				id = 2
			}
			if id != 1 && id != 2 {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound(req.GetCollectionName()))}, nil
			}
			var aliases []string
			// before alter: alias "a" -> A; after: alias "a" -> B
			if (id == 1 && !altered.Load()) || (id == 2 && altered.Load()) {
				aliases = []string{"a"}
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: id,
				DbName:       "db1",
				Schema:       &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{}},
				Aliases:      aliases,
				RequestTime:  uint64(reqTime.Add(1)),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// (1) cache the OLD target A holding alias "a"
	_, err = cache.GetCollectionInfo(ctx, "db1", "A", 0)
	assert.NoError(t, err)

	// (2) the alter commits at rootcoord, then a concurrent Describe(B) lands here
	// BEFORE the expiration and refreshes B with the moved alias. This overwrites the
	// single-valued reverse index aliasInfo[a] from A to B; A stays cached, stale.
	altered.Store(true)
	_, err = cache.GetCollectionInfo(ctx, "db1", "B", 0)
	assert.NoError(t, err)

	cache.mu.RLock()
	singleVal, hasSingle := cache.aliasInfo["db1"]["a"]
	staleA := collByIDLive(cache, 1)
	freshB := collByIDLive(cache, 2)
	cache.mu.RUnlock()
	// the race state: the single-valued alias resolution lost the old target A
	// (the concurrent Describe of B overwrote it to B), yet BOTH A and B are
	// cached, each holding "a" in its own Aliases list -- name-based resolution
	// alone can no longer reach A, which is why rootcoord must forward its id.
	if assert.True(t, hasSingle) {
		assert.Equal(t, "B", singleVal.collectionName, "the concurrent refresh overwrote the single-valued alias resolution to the new target")
	}
	if assert.NotNil(t, staleA, "old target A is still cached at this point") {
		assert.Contains(t, staleA.aliases, "a", "old target A still holds the stale alias in its own Aliases list")
	}
	if assert.NotNil(t, freshB, "new target B is cached") {
		assert.Contains(t, freshB.aliases, "a", "new target B holds the refreshed alias")
	}

	// (3) the AlterAlias expiration arrives with the rootcoord-forwarded ids:
	// RemoveCollection(db1, "a") for the alias name, RemoveCollectionsByID(2) for
	// the new target, and RemoveCollectionsByID(1) for the OLD target (from the
	// header's old_collection_id).
	cache.RemoveCollection(ctx, "db1", "a", 0)
	cache.RemoveCollectionsByID(ctx, 2, 0)
	cache.RemoveCollectionsByID(ctx, 1, 0)

	cache.mu.RLock()
	hasA := collByIDLive(cache, 1) != nil
	hasB := collByIDLive(cache, 2) != nil
	cache.mu.RUnlock()
	assert.False(t, hasA, "old target A must be evicted via the forwarded old-target id (P2-1)")
	assert.False(t, hasB, "new target B must be evicted too")
	assertMetaCacheByIDConsistent(t, cache)

	// a fresh id-only Describe of A must no longer report the moved alias
	infoA, err := cache.GetCollectionInfo(ctx, "", "", 1)
	assert.NoError(t, err)
	assert.Empty(t, infoA.aliases, "old target must no longer serve the stale moved alias")
}

// TestMetaCache_RemoveDatabaseBlocksStaleResurrection: RemoveDatabase must drain
// an in-flight describe (fillMu) before evicting, so a describe issued BEFORE a
// DropDatabase/AlterDatabase broadcast cannot write its pre-DDL response back
// afterwards and resurrect an entry of a dropped/altered database — with the id
// index, such a resurrected entry would even serve id-only lookups cluster-wide.
func TestMetaCache_RemoveDatabaseBlocksStaleResurrection(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	entered := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	var describeCount atomic.Int32
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCount.Add(1)
			// first call parks in flight until released; it is served from a
			// pre-DDL snapshot
			once.Do(func() {
				close(entered)
				<-gate
			})
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: 1,
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  1000,
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	// a fill is in flight when the DropDatabase/AlterDatabase broadcast arrives
	fillDone := make(chan struct{})
	go func() {
		defer close(fillDone)
		_, err := cache.GetCollectionInfo(ctx, "db", "foo", 0)
		assert.NoError(t, err)
	}()
	<-entered

	invalDone := make(chan struct{})
	go func() {
		defer close(invalDone)
		cache.RemoveDatabase(ctx, "db", 2000)
	}()
	select {
	case <-invalDone:
		t.Fatal("RemoveDatabase must drain the in-flight fill before evicting")
	case <-time.After(100 * time.Millisecond):
	}

	// release: the pre-DDL write-back lands first, the db eviction then cleans it
	close(gate)
	<-fillDone
	<-invalDone

	// assert via the read-path helpers: RemoveDatabase bumps the db generation,
	// so the entry is dead to reads even if it physically lingers until the GC
	cache.mu.RLock()
	hasFoo := cachedEntryLocked(cache, "db", "foo") != nil
	hasID := collByIDLive(cache, 1) != nil
	cache.mu.RUnlock()
	assert.False(t, hasFoo, "a pre-DDL describe must not resurrect the evicted db entry")
	assert.False(t, hasID, "a by-id lookup must not serve the resurrected entry")

	// PRODUCTION-path check: the helpers above re-implement the dbGen validation,
	// so additionally prove the real read path does not serve the dead entry --
	// a post-DDL lookup must RE-DESCRIBE (count goes up), not hit the cache.
	countBefore := describeCount.Load()
	_, err = cache.GetCollectionInfo(ctx, "db", "foo", 0)
	assert.NoError(t, err)
	assert.Greater(t, describeCount.Load(), countBefore,
		"the post-DDL lookup must re-describe instead of serving the generation-dead entry")
	cache.mu.RLock()
	hasFoo = cachedEntryLocked(cache, "db", "foo") != nil
	cache.mu.RUnlock()
	assert.True(t, hasFoo, "a post-DDL describe re-caches")
	assertMetaCacheByIDConsistent(t, cache)
}

// TestMetaCache_DropRecreateReusingNameRelinksIDIndex: a drop + recreate reusing
// the collection name replaces the same name-key with a NEW id. The id index
// must unlink the old id and serve the new one (invariant I0 holds for both).
func TestMetaCache_DropRecreateReusingNameRelinksIDIndex(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	var collID atomic.Int64
	collID.Store(1)
	var reqTime atomic.Int64
	reqTime.Store(1000)
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: collID.Load(),
				DbName:       "db",
				Schema:       &schemapb.CollectionSchema{Name: "foo", Fields: []*schemapb.FieldSchema{}},
				RequestTime:  uint64(reqTime.Add(1)),
			}, nil
		})

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)
	defer cache.Close()

	id, err := cache.GetCollectionID(ctx, "db", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(1), id)

	// drop (broadcast evicts by id) + recreate under the same name with a new id
	cache.RemoveCollectionsByID(ctx, 1, 0)
	collID.Store(2)

	id, err = cache.GetCollectionID(ctx, "db", "foo")
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(2), id)

	cache.mu.RLock()
	hasOld := collByIDLive(cache, 1) != nil
	newLive := collByIDLive(cache, 2)
	sameKey := cachedEntryLocked(cache, "db", "foo")
	cache.mu.RUnlock()
	assert.False(t, hasOld, "the old id must not be live after the drop")
	assert.Same(t, sameKey, newLive, "the reused name must resolve to the recreated collection's new id")
	assertMetaCacheByIDConsistent(t, cache)
}

func TestMetaCache_Close(t *testing.T) {
	rootCoord := mocks.NewMockMixCoordClient(t)

	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(),
	}, nil).Maybe()

	cache, err := NewMetaCache(rootCoord)
	assert.NoError(t, err)

	cache.Close()
	cache.Close()
}
