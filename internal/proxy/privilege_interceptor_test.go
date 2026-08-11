package proxy

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
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

		err = InitMetaCache(ctx, client)
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
		{
			_, err = PrivilegeInterceptor(GetContext(context.Background(), "foo:"+util.PasswordHolder), &milvuspb.LoadCollectionRequest{
				DbName:         "db_test",
				CollectionName: "col1",
			})
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "apikey user"))
		}
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
			privilege.GetPolicyModel("foo")
		})
	})
}

func TestRootShouldBindRole(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	rootCtx := GetContext(context.Background(), "root:Milvus")
	t.Run("not bind role", func(t *testing.T) {
		Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "false")
		defer Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)

		InitEmptyGlobalCache()
		_, err := PrivilegeInterceptor(rootCtx, &milvuspb.LoadCollectionRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)
	})

	t.Run("bind role", func(t *testing.T) {
		Params.Save(Params.CommonCfg.RootShouldBindRole.Key, "true")
		defer Params.Reset(Params.CommonCfg.RootShouldBindRole.Key)

		InitEmptyGlobalCache()
		_, err := PrivilegeInterceptor(rootCtx, &milvuspb.LoadCollectionRequest{
			CollectionName: "col1",
		})
		assert.Error(t, err)

		AddRootUserToAdminRole()
		_, err = PrivilegeInterceptor(rootCtx, &milvuspb.LoadCollectionRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)
	})
}

func TestResourceGroupPrivilege(t *testing.T) {
	ctx := context.Background()

	t.Run("Resource Group Privilege", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")

		_, err := PrivilegeInterceptor(ctx, &milvuspb.ListResourceGroupsRequest{})
		assert.Error(t, err)

		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
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
		InitMetaCache(ctx, client)

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

func TestPrivilegeGroup(t *testing.T) {
	ctx := context.Background()

	t.Run("grant ReadOnly to single collection", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		privilege.InitPrivilegeGroups()

		var err error
		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "coll1", commonpb.ObjectPrivilege_PrivilegeGroupReadOnly.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client)
		defer privilege.CleanPrivilegeCache()

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll1",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
	})

	t.Run("grant ReadOnly to all collection", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		privilege.InitPrivilegeGroups()

		var err error
		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "*", commonpb.ObjectPrivilege_PrivilegeGroupReadOnly.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client)
		defer privilege.CleanPrivilegeCache()

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll1",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll2",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll2",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
	})

	t.Run("grant ReadWrite to single collection", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		privilege.InitPrivilegeGroups()

		var err error
		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "coll1", commonpb.ObjectPrivilege_PrivilegeGroupReadWrite.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client)
		defer privilege.CleanPrivilegeCache()

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DeleteRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DeleteRequest{
			CollectionName: "coll2",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateResourceGroupRequest{})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DescribeAliasRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateDatabaseRequest{})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DropDatabaseRequest{})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateCollectionRequest{})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DropCollectionRequest{})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.RenameCollectionRequest{
			OldName: "coll1",
			NewName: "newName",
		})
		assert.NoError(t, err)
	})

	t.Run("grant ReadWrite to all collection", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		privilege.InitPrivilegeGroups()

		var err error
		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "*", commonpb.ObjectPrivilege_PrivilegeGroupReadWrite.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client)
		defer privilege.CleanPrivilegeCache()

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DeleteRequest{
			CollectionName: "coll1",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{
			CollectionName: "coll2",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{
			CollectionName: "coll2",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{
			CollectionName: "coll2",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DeleteRequest{
			CollectionName: "coll2",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateResourceGroupRequest{})
		assert.Error(t, err)
	})

	t.Run("Admin", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		privilege.InitPrivilegeGroups()

		var err error
		ctx = GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status: merr.Success(),
				PolicyInfos: []string{
					funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeGroupAdmin.String(), "default"),
				},
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client)
		defer privilege.CleanPrivilegeCache()

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.QueryRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SearchRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.InsertRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DeleteRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateResourceGroupRequest{})
		assert.NoError(t, err)
	})
}

func TestBuiltinPrivilegeGroup(t *testing.T) {
	t.Run("ClusterAdmin", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		privilege.InitPrivilegeGroups()

		var err error
		ctx := GetContext(context.Background(), "fooo:123456")
		client := &MockMixCoordClientInterface{}

		policies := []string{}
		for _, priv := range Params.RbacConfig.GetDefaultPrivilegeGroup("ClusterReadOnly").Privileges {
			objectType := util.GetObjectType(priv.Name)
			policies = append(policies, funcutil.PolicyForPrivilege("role1", objectType, "*", util.PrivilegeNameForMetastore(priv.Name), "default"))
		}
		client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
			return &internalpb.ListPolicyResponse{
				Status:      merr.Success(),
				PolicyInfos: policies,
				UserRoles: []string{
					funcutil.EncodeUserRoleCache("fooo", "role1"),
				},
			}, nil
		}
		InitMetaCache(ctx, client)
		defer privilege.CleanPrivilegeCache()

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.SelectUserRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.DescribeResourceGroupRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.ListResourceGroupsRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.CreateResourceGroupRequest{})
		assert.Error(t, err)
	})
}

// grantReplicateToRole builds a mock policy client that grants the given role the
// cluster-level replicate privilege (PrivilegeUpdateReplicateConfiguration), which is
// what StreamPrivilegeInterceptor requires for CreateReplicateStream.
func grantReplicateToRole(user, role string) *MockMixCoordClientInterface {
	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				funcutil.PolicyForPrivilege(role, commonpb.ObjectType_Global.String(), "*",
					commonpb.ObjectPrivilege_PrivilegeUpdateReplicateConfiguration.String(), "default"),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache(user, role),
			},
		}, nil
	}
	return client
}

// TestPrivilegeStreamInterceptor verifies the high-order wrapper mirrors
// UnaryServerInterceptor: it wraps a StreamPrivilegeFunc, blocks the handler on auth
// failure, and propagates the authorized context to the handler on success.
func TestPrivilegeStreamInterceptor(t *testing.T) {
	interceptor := PrivilegeStreamInterceptor(StreamPrivilegeInterceptor)
	assert.NotNil(t, interceptor)

	info := &grpc.StreamServerInfo{FullMethod: milvuspb.MilvusService_CreateReplicateStream_FullMethodName}

	t.Run("deny blocks handler", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		InitEmptyGlobalCache()

		called := false
		handler := func(srv interface{}, ss grpc.ServerStream) error {
			called = true
			return nil
		}
		ss := &mockServerStream{ctx: GetContext(context.Background(), "foo:123456")}
		err := interceptor(nil, ss, info, handler)
		assert.Error(t, err)
		assert.False(t, called, "handler must not run when authorization fails")
	})

	t.Run("permit propagates authorized context", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		privilege.InitPrivilegeGroups()
		InitMetaCache(context.Background(), grantReplicateToRole("alice", "role1"))
		defer privilege.CleanPrivilegeCache()

		called := false
		var handlerCtx context.Context
		handler := func(srv interface{}, ss grpc.ServerStream) error {
			called = true
			handlerCtx = ss.Context()
			return nil
		}
		ss := &mockServerStream{ctx: GetContext(context.Background(), "alice:123456")}
		err := interceptor(nil, ss, info, handler)
		assert.NoError(t, err)
		assert.True(t, called, "handler must run for an authorized stream")
		assert.NotNil(t, handlerCtx, "authorized context must be propagated to the handler")
	})
}

// TestStreamPrivilegeInterceptor exercises the authorization core: authorization
// disabled, unregistered method (fail-closed), missing privilege, granted privilege,
// and the root bypass.
func TestStreamPrivilegeInterceptor(t *testing.T) {
	createStream := milvuspb.MilvusService_CreateReplicateStream_FullMethodName

	t.Run("authorization disabled passes", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "false")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)

		_, err := StreamPrivilegeInterceptor(context.Background(), createStream)
		assert.NoError(t, err)
	})

	t.Run("unregistered method denied (fail-closed)", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		InitEmptyGlobalCache()

		ctx := GetContext(context.Background(), "root:123456")
		_, err := StreamPrivilegeInterceptor(ctx, "/milvus.proto.milvus.MilvusService/SomeUnknownStream")
		assert.Error(t, err)
	})

	t.Run("missing privilege denied", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		privilege.InitPrivilegeGroups()
		// grant replicate to role1/alice only; bob has no such privilege
		InitMetaCache(context.Background(), grantReplicateToRole("alice", "role1"))
		defer privilege.CleanPrivilegeCache()

		_, err := StreamPrivilegeInterceptor(GetContext(context.Background(), "bob:123456"), createStream)
		assert.Error(t, err)
	})

	t.Run("granted privilege permitted", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		privilege.InitPrivilegeGroups()
		InitMetaCache(context.Background(), grantReplicateToRole("alice", "role1"))
		defer privilege.CleanPrivilegeCache()

		_, err := StreamPrivilegeInterceptor(GetContext(context.Background(), "alice:123456"), createStream)
		assert.NoError(t, err)
	})

	t.Run("dump messages shares the replicate privilege", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		privilege.InitPrivilegeGroups()
		InitMetaCache(context.Background(), grantReplicateToRole("alice", "role1"))
		defer privilege.CleanPrivilegeCache()

		dumpStream := milvuspb.MilvusService_DumpMessages_FullMethodName
		// the same replicate privilege authorizes DumpMessages ...
		_, err := StreamPrivilegeInterceptor(GetContext(context.Background(), "alice:123456"), dumpStream)
		assert.NoError(t, err)
		// ... while a user without it is denied.
		_, err = StreamPrivilegeInterceptor(GetContext(context.Background(), "bob:123456"), dumpStream)
		assert.Error(t, err)
	})

	t.Run("root bypass", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
		defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Save(Params.CommonCfg.RootShouldBindRole.Key, "false")
		defer paramtable.Get().Reset(Params.CommonCfg.RootShouldBindRole.Key)
		InitEmptyGlobalCache()

		_, err := StreamPrivilegeInterceptor(GetContext(context.Background(), "root:123456"), createStream)
		assert.NoError(t, err)
	})
}
