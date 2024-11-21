package coordclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var _ types.RootCoordClient = &rootCoordLocalClientImpl{}

// newRootCoordLocalClient creates a new local client for root coordinator server.
func newRootCoordLocalClient() *rootCoordLocalClientImpl {
	return &rootCoordLocalClientImpl{
		localRootCoordServer: syncutil.NewFuture[rootcoordpb.RootCoordServer](),
	}
}

// rootCoordLocalClientImpl is used to implement a local client for root coordinator server.
// We need to merge all the coordinator into one server, so use those client to erase the rpc layer between different coord.
type rootCoordLocalClientImpl struct {
	localRootCoordServer *syncutil.Future[rootcoordpb.RootCoordServer]
}

func (r *rootCoordLocalClientImpl) waitForReady(ctx context.Context) (rootcoordpb.RootCoordServer, error) {
	return r.localRootCoordServer.GetWithContext(ctx)
}

func (r *rootCoordLocalClientImpl) setReadyServer(server rootcoordpb.RootCoordServer) {
	r.localRootCoordServer.Set(server)
}

func (r *rootCoordLocalClientImpl) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetComponentStates(ctx, in)
}

func (r *rootCoordLocalClientImpl) GetTimeTickChannel(ctx context.Context, in *internalpb.GetTimeTickChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetTimeTickChannel(ctx, in)
}

func (r *rootCoordLocalClientImpl) GetStatisticsChannel(ctx context.Context, in *internalpb.GetStatisticsChannelRequest, opts ...grpc.CallOption) (*milvuspb.StringResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetStatisticsChannel(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateCollection(ctx, in)
}

func (r *rootCoordLocalClientImpl) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropCollection(ctx, in)
}

func (r *rootCoordLocalClientImpl) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.HasCollection(ctx, in)
}

func (r *rootCoordLocalClientImpl) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DescribeCollection(ctx, in)
}

func (r *rootCoordLocalClientImpl) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DescribeCollectionInternal(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateAlias(ctx, in)
}

func (r *rootCoordLocalClientImpl) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropAlias(ctx, in)
}

func (r *rootCoordLocalClientImpl) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AlterAlias(ctx, in)
}

func (r *rootCoordLocalClientImpl) DescribeAlias(ctx context.Context, in *milvuspb.DescribeAliasRequest, opts ...grpc.CallOption) (*milvuspb.DescribeAliasResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DescribeAlias(ctx, in)
}

func (r *rootCoordLocalClientImpl) ListAliases(ctx context.Context, in *milvuspb.ListAliasesRequest, opts ...grpc.CallOption) (*milvuspb.ListAliasesResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListAliases(ctx, in)
}

func (r *rootCoordLocalClientImpl) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowCollectionsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowCollections(ctx, in)
}

func (r *rootCoordLocalClientImpl) AlterCollection(ctx context.Context, in *milvuspb.AlterCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AlterCollection(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreatePartition(ctx, in)
}

func (r *rootCoordLocalClientImpl) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropPartition(ctx, in)
}

func (r *rootCoordLocalClientImpl) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest, opts ...grpc.CallOption) (*milvuspb.BoolResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.HasPartition(ctx, in)
}

func (r *rootCoordLocalClientImpl) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowPartitions(ctx, in)
}

func (r *rootCoordLocalClientImpl) ShowPartitionsInternal(ctx context.Context, in *milvuspb.ShowPartitionsRequest, opts ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowPartitionsInternal(ctx, in)
}

func (r *rootCoordLocalClientImpl) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest, opts ...grpc.CallOption) (*milvuspb.ShowSegmentsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowSegments(ctx, in)
}

func (r *rootCoordLocalClientImpl) AllocTimestamp(ctx context.Context, in *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AllocTimestamp(ctx, in)
}

func (r *rootCoordLocalClientImpl) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AllocID(ctx, in)
}

func (r *rootCoordLocalClientImpl) UpdateChannelTimeTick(ctx context.Context, in *internalpb.ChannelTimeTickMsg, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.UpdateChannelTimeTick(ctx, in)
}

func (r *rootCoordLocalClientImpl) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.InvalidateCollectionMetaCache(ctx, in)
}

func (r *rootCoordLocalClientImpl) ShowConfigurations(ctx context.Context, in *internalpb.ShowConfigurationsRequest, opts ...grpc.CallOption) (*internalpb.ShowConfigurationsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ShowConfigurations(ctx, in)
}

func (r *rootCoordLocalClientImpl) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetMetrics(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateCredential(ctx, in)
}

func (r *rootCoordLocalClientImpl) UpdateCredential(ctx context.Context, in *internalpb.CredentialInfo, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.UpdateCredential(ctx, in)
}

func (r *rootCoordLocalClientImpl) DeleteCredential(ctx context.Context, in *milvuspb.DeleteCredentialRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DeleteCredential(ctx, in)
}

func (r *rootCoordLocalClientImpl) ListCredUsers(ctx context.Context, in *milvuspb.ListCredUsersRequest, opts ...grpc.CallOption) (*milvuspb.ListCredUsersResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListCredUsers(ctx, in)
}

func (r *rootCoordLocalClientImpl) GetCredential(ctx context.Context, in *rootcoordpb.GetCredentialRequest, opts ...grpc.CallOption) (*rootcoordpb.GetCredentialResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.GetCredential(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateRole(ctx, in)
}

func (r *rootCoordLocalClientImpl) DropRole(ctx context.Context, in *milvuspb.DropRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropRole(ctx, in)
}

func (r *rootCoordLocalClientImpl) OperateUserRole(ctx context.Context, in *milvuspb.OperateUserRoleRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.OperateUserRole(ctx, in)
}

func (r *rootCoordLocalClientImpl) SelectRole(ctx context.Context, in *milvuspb.SelectRoleRequest, opts ...grpc.CallOption) (*milvuspb.SelectRoleResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SelectRole(ctx, in)
}

func (r *rootCoordLocalClientImpl) SelectUser(ctx context.Context, in *milvuspb.SelectUserRequest, opts ...grpc.CallOption) (*milvuspb.SelectUserResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SelectUser(ctx, in)
}

func (r *rootCoordLocalClientImpl) OperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.OperatePrivilege(ctx, in)
}

func (r *rootCoordLocalClientImpl) SelectGrant(ctx context.Context, in *milvuspb.SelectGrantRequest, opts ...grpc.CallOption) (*milvuspb.SelectGrantResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.SelectGrant(ctx, in)
}

func (r *rootCoordLocalClientImpl) ListPolicy(ctx context.Context, in *internalpb.ListPolicyRequest, opts ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListPolicy(ctx, in)
}

func (r *rootCoordLocalClientImpl) BackupRBAC(ctx context.Context, in *milvuspb.BackupRBACMetaRequest, opts ...grpc.CallOption) (*milvuspb.BackupRBACMetaResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.BackupRBAC(ctx, in)
}

func (r *rootCoordLocalClientImpl) RestoreRBAC(ctx context.Context, in *milvuspb.RestoreRBACMetaRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.RestoreRBAC(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreatePrivilegeGroup(ctx, in)
}

func (r *rootCoordLocalClientImpl) DropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropPrivilegeGroup(ctx, in)
}

func (r *rootCoordLocalClientImpl) ListPrivilegeGroups(ctx context.Context, in *milvuspb.ListPrivilegeGroupsRequest, opts ...grpc.CallOption) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListPrivilegeGroups(ctx, in)
}

func (r *rootCoordLocalClientImpl) OperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.OperatePrivilegeGroup(ctx, in)
}

func (r *rootCoordLocalClientImpl) CheckHealth(ctx context.Context, in *milvuspb.CheckHealthRequest, opts ...grpc.CallOption) (*milvuspb.CheckHealthResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CheckHealth(ctx, in)
}

func (r *rootCoordLocalClientImpl) RenameCollection(ctx context.Context, in *milvuspb.RenameCollectionRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.RenameCollection(ctx, in)
}

func (r *rootCoordLocalClientImpl) CreateDatabase(ctx context.Context, in *milvuspb.CreateDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.CreateDatabase(ctx, in)
}

func (r *rootCoordLocalClientImpl) DropDatabase(ctx context.Context, in *milvuspb.DropDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DropDatabase(ctx, in)
}

func (r *rootCoordLocalClientImpl) ListDatabases(ctx context.Context, in *milvuspb.ListDatabasesRequest, opts ...grpc.CallOption) (*milvuspb.ListDatabasesResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.ListDatabases(ctx, in)
}

func (r *rootCoordLocalClientImpl) DescribeDatabase(ctx context.Context, in *rootcoordpb.DescribeDatabaseRequest, opts ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.DescribeDatabase(ctx, in)
}

func (r *rootCoordLocalClientImpl) AlterDatabase(ctx context.Context, in *rootcoordpb.AlterDatabaseRequest, opts ...grpc.CallOption) (*commonpb.Status, error) {
	s, err := r.waitForReady(ctx)
	if err != nil {
		return nil, err
	}
	return s.AlterDatabase(ctx, in)
}

func (r *rootCoordLocalClientImpl) Close() error {
	return nil
}
