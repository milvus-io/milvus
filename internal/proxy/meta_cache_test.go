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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	uatomic "go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	if in.CollectionName == "collection1" || in.CollectionID == 1 {
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
	if in.CollectionName == "collection1" || in.CollectionID == 1 {
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
	panic("implement me")
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

	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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

func TestMetaCache_GetBasicCollectionInfo(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}

	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
		rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{11},
			PartitionNames:       []string{"p1"},
			CreatedTimestamps:    []uint64{11},
			CreatedUtcTimestamps: []uint64{11},
		}, nil).Once()
		rootCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Once()
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
		rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status:               merr.Success(),
			PartitionIDs:         []typeutil.UniqueID{11},
			PartitionNames:       []string{"p1"},
			CreatedTimestamps:    []uint64{11},
			CreatedUtcTimestamps: []uint64{11},
		}, nil).Once()
		rootCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Once()
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
		mgr := newShardClientMgr()
		err := InitMetaCache(ctx, rootCoord, mgr)
		assert.NoError(t, err)
	})

	t.Run("failed to list policy", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(
			&internalpb.ListPolicyResponse{Status: merr.Status(errors.New("mock list policy error"))},
			nil).Once()
		mgr := newShardClientMgr()
		err := InitMetaCache(ctx, rootCoord, mgr)
		assert.Error(t, err)
	})

	t.Run("rpc error", func(t *testing.T) {
		ctx := context.Background()
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(
			nil, errors.New("mock list policy rpc errorr")).Once()
		mgr := newShardClientMgr()
		err := InitMetaCache(ctx, rootCoord, mgr)
		assert.Error(t, err)
	})
}

func TestMetaCache_GetCollectionName(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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
			globalMetaCache.RemoveCollection(ctx, dbName, "collection1")
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
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
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

func TestMetaCache_GetShards(t *testing.T) {
	var (
		ctx            = context.Background()
		collectionName = "collection1"
		collectionID   = int64(1)
	)

	rootCoord := &MockMixCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, mgr)
	require.Nil(t, err)

	t.Run("No collection in meta cache", func(t *testing.T) {
		shards, err := globalMetaCache.GetShards(ctx, true, dbName, "non-exists", 0)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info invalid shardLeaders", func(t *testing.T) {
		shards, err := globalMetaCache.GetShards(ctx, false, dbName, collectionName, collectionID)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})

	t.Run("without shardLeaders in collection info", func(t *testing.T) {
		rootCoord.getShardLeaders = func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
			return &querypb.GetShardLeadersResponse{
				Status: merr.Success(),
				Shards: []*querypb.ShardLeadersList{
					{
						ChannelName: "channel-1",
						NodeIds:     []int64{1, 2, 3},
						NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					},
				},
			}, nil
		}

		shards, err := globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)
		assert.NoError(t, err)
		assert.NotEmpty(t, shards)
		assert.Equal(t, 1, len(shards))
		assert.Equal(t, 3, len(shards["channel-1"]))

		// get from cache

		rootCoord.getShardLeaders = func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
			return &querypb.GetShardLeadersResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "not implemented",
				},
			}, nil
		}

		shards, err = globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)

		assert.NoError(t, err)
		assert.NotEmpty(t, shards)
		assert.Equal(t, 1, len(shards))
		assert.Equal(t, 3, len(shards["channel-1"]))
	})
}

func TestMetaCache_ClearShards(t *testing.T) {
	var (
		ctx            = context.TODO()
		collectionName = "collection1"
		collectionID   = int64(1)
	)

	qc := &MockMixCoordClientInterface{}
	mgr := newShardClientMgr()
	err := InitMetaCache(ctx, qc, mgr)
	require.Nil(t, err)

	t.Run("Clear with no collection info", func(t *testing.T) {
		globalMetaCache.DeprecateShardCache(dbName, "collection_not_exist")
	})

	t.Run("Clear valid collection empty cache", func(t *testing.T) {
		globalMetaCache.DeprecateShardCache(dbName, collectionName)
	})

	t.Run("Clear valid collection valid cache", func(t *testing.T) {
		qc.getShardLeaders = func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
			return &querypb.GetShardLeadersResponse{
				Status: merr.Success(),
				Shards: []*querypb.ShardLeadersList{
					{
						ChannelName: "channel-1",
						NodeIds:     []int64{1, 2, 3},
						NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
					},
				},
			}, nil
		}

		shards, err := globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)
		require.NoError(t, err)
		require.NotEmpty(t, shards)
		require.Equal(t, 1, len(shards))
		require.Equal(t, 3, len(shards["channel-1"]))

		globalMetaCache.DeprecateShardCache(dbName, collectionName)

		qc.getShardLeaders = func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
			return &querypb.GetShardLeadersResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "not implemented",
				},
			}, nil
		}

		shards, err = globalMetaCache.GetShards(ctx, true, dbName, collectionName, collectionID)
		assert.Error(t, err)
		assert.Empty(t, shards)
	})
}

func TestMetaCache_PolicyInfo(t *testing.T) {
	client := &MockMixCoordClientInterface{}
	mgr := newShardClientMgr()

	t.Run("InitMetaCache", func(t *testing.T) {
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return nil, fmt.Errorf("mock error")
		}
		err := InitMetaCache(context.Background(), client, mgr)
		assert.Error(t, err)

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
			}, nil
		}
		err = InitMetaCache(context.Background(), client, mgr)
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
		err := InitMetaCache(context.Background(), client, mgr)
		assert.NoError(t, err)
		policyInfos := globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))
		roles := globalMetaCache.GetUserRole("foo")
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
		err := InitMetaCache(context.Background(), client, mgr)
		assert.NoError(t, err)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos := globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 4, len(policyInfos))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRevokePrivilege, OpKey: "policyX"})
		assert.NoError(t, err)
		policyInfos = globalMetaCache.GetPrivilegeInfo(context.Background())
		assert.Equal(t, 3, len(policyInfos))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles := globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 3, len(roles))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("foo", "role3")})
		assert.NoError(t, err)
		roles = globalMetaCache.GetUserRole("foo")
		assert.Equal(t, 2, len(roles))

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: ""})
		assert.Error(t, err)
		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: 100, OpKey: "policyX"})
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
		err := InitMetaCache(context.Background(), client, mgr)
		assert.NoError(t, err)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDeleteUser, OpKey: "foo"})
		assert.NoError(t, err)

		roles := globalMetaCache.GetUserRole("foo")
		assert.Len(t, roles, 0)

		roles = globalMetaCache.GetUserRole("foo2")
		assert.Len(t, roles, 2)

		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheDropRole, OpKey: "role2"})
		assert.NoError(t, err)
		roles = globalMetaCache.GetUserRole("foo2")
		assert.Len(t, roles, 1)
		assert.Equal(t, "role3", roles[0])

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: []string{"policy1", "policy2", "policy3"},
				UserRoles:   []string{funcutil.EncodeUserRoleCache("foo", "role1"), funcutil.EncodeUserRoleCache("foo", "role2"), funcutil.EncodeUserRoleCache("foo2", "role2"), funcutil.EncodeUserRoleCache("foo2", "role3")},
			}, nil
		}
		err = globalMetaCache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh})
		assert.NoError(t, err)
		roles = globalMetaCache.GetUserRole("foo")
		assert.Len(t, roles, 2)
	})
}

func TestMetaCache_RemoveCollection(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, shardMgr)
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

	globalMetaCache.RemoveCollection(ctx, dbName, "collection1")
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 2)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1), 100, false)
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// shouldn't access RootCoord again
	assert.Equal(t, rootCoord.GetAccessCount(), 3)

	globalMetaCache.RemoveCollectionsByID(ctx, UniqueID(1), 100, false)
	// no collectionInfo of collection2, should access RootCoord
	_, err = globalMetaCache.GetCollectionInfo(ctx, dbName, "collection1", 1)
	assert.NoError(t, err)
	// no collectionInfo of collection1, should access RootCoord
	assert.Equal(t, rootCoord.GetAccessCount(), 4)
}

func TestGlobalMetaCache_ShuffleShardLeaders(t *testing.T) {
	shards := map[string][]nodeInfo{
		"channel-1": {
			{
				nodeID:  1,
				address: "localhost:9000",
			},
			{
				nodeID:  2,
				address: "localhost:9000",
			},
			{
				nodeID:  3,
				address: "localhost:9000",
			},
		},
	}
	sl := &shardLeaders{
		idx:          uatomic.NewInt64(5),
		shardLeaders: shards,
	}

	reader := sl.GetReader()
	result := reader.Shuffle()
	assert.Len(t, result["channel-1"], 3)
	assert.Equal(t, int64(1), result["channel-1"][0].nodeID)

	reader = sl.GetReader()
	result = reader.Shuffle()
	assert.Len(t, result["channel-1"], 3)
	assert.Equal(t, int64(2), result["channel-1"][0].nodeID)

	reader = sl.GetReader()
	result = reader.Shuffle()
	assert.Len(t, result["channel-1"], 3)
	assert.Equal(t, int64(3), result["channel-1"][0].nodeID)
}

func TestMetaCache_Database(t *testing.T) {
	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, shardMgr)
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
		shardMgr := newShardClientMgr()
		cache, err := NewMetaCache(rootCoord, shardMgr)
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
		shardMgr := newShardClientMgr()
		cache, err := NewMetaCache(rootCoord, shardMgr)
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
	shardMgr := newShardClientMgr()

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

		err := InitMetaCache(ctx, rootCoord, shardMgr)
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
		}, fmt.Errorf("mock error"))
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord, shardMgr)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})

	t.Run("failed", func(t *testing.T) {
		rootCoord := mocks.NewMockMixCoordClient(t)
		rootCoord.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Status(fmt.Errorf("mock failed")),
		}, nil)
		rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
			Status:      merr.Success(),
			PolicyInfos: []string{"policy1", "policy2", "policy3"},
		}, nil)

		err := InitMetaCache(ctx, rootCoord, shardMgr)
		assert.NoError(t, err)
		assert.Equal(t, globalMetaCache.HasDatabase(ctx, dbName), false)

		id, err := globalMetaCache.AllocID(ctx)
		assert.Error(t, err)
		assert.Equal(t, id, int64(0))
	})
}

func TestMetaCache_InvalidateShardLeaderCache(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(Params.ProxyCfg.ShardLeaderCacheInterval.Key, "1")

	ctx := context.Background()
	rootCoord := &MockMixCoordClientInterface{}
	shardMgr := newShardClientMgr()
	err := InitMetaCache(ctx, rootCoord, shardMgr)
	assert.NoError(t, err)

	rootCoord.showLoadCollections = func(ctx context.Context, in *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
		return &querypb.ShowCollectionsResponse{
			Status:              merr.Success(),
			CollectionIDs:       []UniqueID{1},
			InMemoryPercentages: []int64{100},
		}, nil
	}

	called := uatomic.NewInt32(0)

	rootCoord.getShardLeaders = func(ctx context.Context, in *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
		called.Inc()
		return &querypb.GetShardLeadersResponse{
			Status: merr.Success(),
			Shards: []*querypb.ShardLeadersList{
				{
					ChannelName: "channel-1",
					NodeIds:     []int64{1, 2, 3},
					NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002"},
				},
			},
		}, nil
	}
	nodeInfos, err := globalMetaCache.GetShards(ctx, true, dbName, "collection1", 1)
	assert.NoError(t, err)
	assert.Len(t, nodeInfos["channel-1"], 3)
	assert.Equal(t, called.Load(), int32(1))

	globalMetaCache.GetShards(ctx, true, dbName, "collection1", 1)
	assert.Equal(t, called.Load(), int32(1))

	globalMetaCache.InvalidateShardLeaderCache([]int64{1})
	nodeInfos, err = globalMetaCache.GetShards(ctx, true, dbName, "collection1", 1)
	assert.NoError(t, err)
	assert.Len(t, nodeInfos["channel-1"], 3)
	assert.Equal(t, called.Load(), int32(2))
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

func TestMetaCache_Parallel(t *testing.T) {
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ShowLoadCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()
	rootCoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
		Status: merr.Success(),
	}, nil).Maybe()
	mgr := newShardClientMgr()
	cache, err := NewMetaCache(rootCoord, mgr)
	assert.NoError(t, err)

	cacheVersion := uint64(100)
	// clean cache
	cache.RemoveCollectionsByID(ctx, 111, cacheVersion+2, false)

	// update cache, but version is smaller
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, option ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "collection1",
			},
			CollectionID: 111,
			DbName:       dbName,
			RequestTime:  cacheVersion,
		}, nil
	}).Once()

	collInfo, err := cache.update(ctx, dbName, "collection1", 111)
	assert.NoError(t, err)
	assert.Equal(t, "collection1", collInfo.schema.Name)
	assert.Equal(t, int64(111), collInfo.collID)
	_, ok := cache.collInfo[dbName]["collection1"]
	assert.False(t, ok)

	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, request *milvuspb.DescribeCollectionRequest, option ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		cacheVersion++
		return &milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "collection1",
			},
			CollectionID: 111,
			DbName:       dbName,
			RequestTime:  cacheVersion + 5,
		}, nil
	}).Once()

	collInfo, err = cache.update(ctx, dbName, "collection1", 111)
	assert.NoError(t, err)
	assert.Equal(t, "collection1", collInfo.schema.Name)
	assert.Equal(t, int64(111), collInfo.collID)
	_, ok = cache.collInfo[dbName]["collection1"]
	assert.True(t, ok)
}
