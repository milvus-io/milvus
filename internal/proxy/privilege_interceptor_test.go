package proxy

import (
	"context"
	"sync"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestUnaryServerInterceptor(t *testing.T) {
	interceptor := UnaryServerInterceptor(PrivilegeInterceptor)
	assert.NotNil(t, interceptor)
}

func TestPrivilegeInterceptor(t *testing.T) {
	ctx := context.Background()

	t.Run("Authorization Disabled", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		_, err := PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NoError(t, err)
	})

	t.Run("Authorization Enabled", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")

		_, err := PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Error(t, err)

		ctx = GetContext(context.Background(), "alice:123456")
		client := &MockRootCoordClientInterface{}
		queryCoord := &types.MockQueryCoord{}
		mgr := newShardClientMgr()

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeLoad.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeGetLoadState.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeGetLoadingProgress.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeFlush.String(), "default"),
					funcutil.PolicyForPrivilege("role2", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAll.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("alice", "role1"),
					funcutil.EncodeUserRoleCache("fooo", "role2"),
				},
			}, nil
		}

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "foo:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "root:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NoError(t, err)

		err = InitMetaCache(ctx, client, queryCoord, mgr)
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.GetLoadStateRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)

		fooCtx := GetContext(context.Background(), "foo:123456")
		_, err = PrivilegeInterceptor(fooCtx, &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Error(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.InsertRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Error(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.UpsertRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.Error(t, err)
		_, err = PrivilegeInterceptor(fooCtx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: "col1",
		})
		assert.Error(t, err)
		_, err = PrivilegeInterceptor(fooCtx, &milvuspb.GetLoadStateRequest{
			CollectionName: "col1",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(ctx, &milvuspb.FlushRequest{
			DbName:          "db_test",
			CollectionNames: []string{"col1"},
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContextWithDB(context.Background(), "fooo:123456", "foo"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_test",
			CollectionName: "col1",
		})
		assert.NotNil(t, err)

		g := sync.WaitGroup{}
		for i := 0; i < 20; i++ {
			g.Add(1)
			go func() {
				defer g.Done()
				assert.NotPanics(t, func() {
					PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.LoadCollectionRequest{
						DbName:         "db_test",
						CollectionName: "col1",
					})
				})
			}()
		}
		g.Wait()

		assert.Panics(t, func() {
			getPolicyModel("foo")
		})
	})

}

func TestResourceGroupPrivilege(t *testing.T) {
	ctx := context.Background()

	t.Run("Resource Group Privilege", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")

		_, err := PrivilegeInterceptor(ctx, &milvuspb.ListResourceGroupsRequest{})
		assert.Error(t, err)

		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockRootCoordClientInterface{}
		queryCoord := &types.MockQueryCoord{}
		mgr := newShardClientMgr()

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeCreateResourceGroup.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeDropResourceGroup.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeDescribeResourceGroup.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeListResourceGroups.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeTransferNode.String(), "default"),
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeTransferReplica.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client, queryCoord, mgr)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: "rg",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DropResourceGroupRequest{
			ResourceGroup: "rg",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DescribeResourceGroupRequest{
			ResourceGroup: "rg",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.ListResourceGroupsRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.TransferNodeRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.TransferReplicaRequest{})
		assert.NoError(t, err)
	})

}
