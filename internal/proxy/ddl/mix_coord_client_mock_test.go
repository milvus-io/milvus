package ddl

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
			Aliases:     []string{"collection1_alias"},
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
