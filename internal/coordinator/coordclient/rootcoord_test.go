package coordclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

func TestRootCoordLocalClient(t *testing.T) {
	c := newRootCoordLocalClient()
	c.setReadyServer(rootcoordpb.UnimplementedRootCoordServer{})

	ctx := context.Background()

	_, err := c.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Error(t, err)

	_, err = c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
	assert.Error(t, err)

	_, err = c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
	assert.Error(t, err)

	_, err = c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
	assert.Error(t, err)

	_, err = c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
	assert.Error(t, err)

	_, err = c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
	assert.Error(t, err)

	_, err = c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
	assert.Error(t, err)

	_, err = c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
	assert.Error(t, err)

	_, err = c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{})
	assert.Error(t, err)

	_, err = c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
	assert.Error(t, err)

	_, err = c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.Error(t, err)

	_, err = c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
	assert.Error(t, err)

	_, err = c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
	assert.Error(t, err)

	_, err = c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
	assert.Error(t, err)

	_, err = c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
	assert.Error(t, err)

	_, err = c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
	assert.Error(t, err)

	_, err = c.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{})
	assert.Error(t, err)

	_, err = c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
	assert.Error(t, err)

	_, err = c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
	assert.Error(t, err)

	_, err = c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{})
	assert.Error(t, err)

	_, err = c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
	assert.Error(t, err)

	_, err = c.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.Error(t, err)

	_, err = c.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Error(t, err)

	_, err = c.CreateCredential(ctx, &internalpb.CredentialInfo{})
	assert.Error(t, err)

	_, err = c.UpdateCredential(ctx, &internalpb.CredentialInfo{})
	assert.Error(t, err)

	_, err = c.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{})
	assert.Error(t, err)

	_, err = c.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
	assert.Error(t, err)

	_, err = c.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{})
	assert.Error(t, err)

	_, err = c.CreateRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.OperateUserRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.SelectRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.SelectUser(ctx, nil)
	assert.Error(t, err)

	_, err = c.OperatePrivilege(ctx, nil)
	assert.Error(t, err)

	_, err = c.SelectGrant(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListPolicy(ctx, nil)
	assert.Error(t, err)

	_, err = c.BackupRBAC(ctx, nil)
	assert.Error(t, err)

	_, err = c.RestoreRBAC(ctx, nil)
	assert.Error(t, err)

	_, err = c.CreatePrivilegeGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropPrivilegeGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListPrivilegeGroups(ctx, nil)
	assert.Error(t, err)

	_, err = c.OperatePrivilegeGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.CheckHealth(ctx, nil)
	assert.Error(t, err)

	_, err = c.RenameCollection(ctx, nil)
	assert.Error(t, err)

	_, err = c.CreateDatabase(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropDatabase(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListDatabases(ctx, nil)
	assert.Error(t, err)

	_, err = c.DescribeDatabase(ctx, nil)
	assert.Error(t, err)

	_, err = c.AlterDatabase(ctx, nil)
	assert.Error(t, err)

	c.Close()
}

func TestRootCoordLocalClientWithTimeout(t *testing.T) {
	c := newRootCoordLocalClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	assert.Error(t, err)

	_, err = c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
	assert.Error(t, err)

	_, err = c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
	assert.Error(t, err)

	_, err = c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
	assert.Error(t, err)

	_, err = c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
	assert.Error(t, err)

	_, err = c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
	assert.Error(t, err)

	_, err = c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
	assert.Error(t, err)

	_, err = c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
	assert.Error(t, err)

	_, err = c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{})
	assert.Error(t, err)

	_, err = c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
	assert.Error(t, err)

	_, err = c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.Error(t, err)

	_, err = c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
	assert.Error(t, err)

	_, err = c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
	assert.Error(t, err)

	_, err = c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
	assert.Error(t, err)

	_, err = c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
	assert.Error(t, err)

	_, err = c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
	assert.Error(t, err)

	_, err = c.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{})
	assert.Error(t, err)

	_, err = c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
	assert.Error(t, err)

	_, err = c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
	assert.Error(t, err)

	_, err = c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{})
	assert.Error(t, err)

	_, err = c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
	assert.Error(t, err)

	_, err = c.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
	assert.Error(t, err)

	_, err = c.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
	assert.Error(t, err)

	_, err = c.CreateCredential(ctx, &internalpb.CredentialInfo{})
	assert.Error(t, err)

	_, err = c.UpdateCredential(ctx, &internalpb.CredentialInfo{})
	assert.Error(t, err)

	_, err = c.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{})
	assert.Error(t, err)

	_, err = c.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
	assert.Error(t, err)

	_, err = c.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{})
	assert.Error(t, err)

	_, err = c.CreateRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.OperateUserRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.SelectRole(ctx, nil)
	assert.Error(t, err)

	_, err = c.SelectUser(ctx, nil)
	assert.Error(t, err)

	_, err = c.OperatePrivilege(ctx, nil)
	assert.Error(t, err)

	_, err = c.SelectGrant(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListPolicy(ctx, nil)
	assert.Error(t, err)

	_, err = c.BackupRBAC(ctx, nil)
	assert.Error(t, err)

	_, err = c.RestoreRBAC(ctx, nil)
	assert.Error(t, err)

	_, err = c.CreatePrivilegeGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropPrivilegeGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListPrivilegeGroups(ctx, nil)
	assert.Error(t, err)

	_, err = c.OperatePrivilegeGroup(ctx, nil)
	assert.Error(t, err)

	_, err = c.CheckHealth(ctx, nil)
	assert.Error(t, err)

	_, err = c.RenameCollection(ctx, nil)
	assert.Error(t, err)

	_, err = c.CreateDatabase(ctx, nil)
	assert.Error(t, err)

	_, err = c.DropDatabase(ctx, nil)
	assert.Error(t, err)

	_, err = c.ListDatabases(ctx, nil)
	assert.Error(t, err)

	_, err = c.DescribeDatabase(ctx, nil)
	assert.Error(t, err)

	_, err = c.AlterDatabase(ctx, nil)
	assert.Error(t, err)
}
