package proxy

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
			CollectionName: "col1",
		})
		assert.NoError(t, err)
	})

	t.Run("Authorization Enabled", func(t *testing.T) {
		paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")

		_, err := PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
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
			CollectionName: "col1",
		})
		assert.Error(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "root:123456"), &milvuspb.LoadCollectionRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)

		err = InitMetaCache(ctx, client)
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.HasCollectionRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
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
			CollectionName: "col1",
		})
		assert.Error(t, err)
		{
			_, err = PrivilegeInterceptor(GetContext(context.Background(), "foo:"+util.PasswordHolder), &milvuspb.LoadCollectionRequest{
				CollectionName: "col1",
			})
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "apikey user"))
		}
		_, err = PrivilegeInterceptor(ctx, &milvuspb.InsertRequest{
			CollectionName: "col1",
		})
		assert.Error(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.UpsertRequest{
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
			CollectionNames: []string{"col1"},
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.LoadCollectionRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)

		// fooo only has policies on the `default` db; targeting `foo` db must
		// be denied. With the fix, request DbName takes precedence over ctx.
		_, err = PrivilegeInterceptor(GetContextWithDB(context.Background(), "fooo:123456", "foo"), &milvuspb.LoadCollectionRequest{
			DbName:         "foo",
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

// TestPrivilegePreferRequestDBName verifies that PrivilegeInterceptor authorizes
// against the DbName carried by the request itself rather than the connection
// context. A user holding privileges on db_target should pass when calling
// DescribeDatabase("db_target") without a prior useDatabase.
func TestPrivilegePreferRequestDBName(t *testing.T) {
	paramtable.Get().Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(Params.CommonCfg.AuthorizationEnabled.Key)
	privilege.InitPrivilegeGroups()

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				// role_db_target only owns DescribeDatabase + Load on db_target.
				// The Load privilege verifies that the "request DbName takes
				// precedence" rule applies to other request types as well.
				funcutil.PolicyForPrivilege("role_db_target", commonpb.ObjectType_Global.String(), "*",
					commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String(), "db_target"),
				funcutil.PolicyForPrivilege("role_db_target", commonpb.ObjectType_Collection.String(), "*",
					commonpb.ObjectPrivilege_PrivilegeLoad.String(), "db_target"),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("user_db_target", "role_db_target"),
			},
		}, nil
	}
	assert.NoError(t, InitMetaCache(context.Background(), client))
	defer privilege.CleanPrivilegeCache()

	// Caller uses an empty connection context (no useDatabase), but explicitly
	// passes DbName=db_target in the request. With the fix, auth is enforced
	// against db_target and should succeed.
	ctx := GetContext(context.Background(), "user_db_target:123456")
	_, err := PrivilegeInterceptor(ctx, &milvuspb.DescribeDatabaseRequest{DbName: "db_target"})
	assert.NoError(t, err)

	// Sanity: when the request carries no DbName and the context has no db
	// either, it falls back to the cluster default and should be denied.
	_, err = PrivilegeInterceptor(ctx, &milvuspb.DescribeDatabaseRequest{})
	assert.Error(t, err)

	// Sanity: a request explicitly targeting a different db (default) must
	// still be denied because the user only has privileges on db_target.
	_, err = PrivilegeInterceptor(ctx, &milvuspb.DescribeDatabaseRequest{DbName: util.DefaultDBName})
	assert.Error(t, err)

	// Cross-check: when the connection context is on db_other and the request
	// explicitly targets db_target, the request DbName takes precedence and
	// the call should succeed.
	ctxOther := GetContextWithDB(context.Background(), "user_db_target:123456", "db_other")
	_, err = PrivilegeInterceptor(ctxOther, &milvuspb.DescribeDatabaseRequest{DbName: "db_target"})
	assert.NoError(t, err)

	// Boundary regression: even when the connection context is on the
	// privileged db (db_target), a request that explicitly targets a different
	// db (default) must be denied.
	ctxTarget := GetContextWithDB(context.Background(), "user_db_target:123456", "db_target")
	_, err = PrivilegeInterceptor(ctxTarget, &milvuspb.DescribeDatabaseRequest{DbName: util.DefaultDBName})
	assert.Error(t, err)

	// Generality regression: the request-DbName preference is duck-typed via
	// GetDbName(), so it must apply to every privileged request type. Verify
	// with LoadCollectionRequest.
	_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
		DbName: "db_target", CollectionName: "c1",
	})
	assert.NoError(t, err)
	_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
		DbName: util.DefaultDBName, CollectionName: "c1",
	})
	assert.Error(t, err)
	_, err = PrivilegeInterceptor(ctxOther, &milvuspb.LoadCollectionRequest{
		DbName: "db_target", CollectionName: "c1",
	})
	assert.NoError(t, err)

	// Escalation regression: the connection context is on the privileged db
	// (db_target), but the request explicitly targets db_other where the user
	// has no privileges. With the fix, auth is enforced against req.DbName and
	// must be denied.
	_, err = PrivilegeInterceptor(ctxTarget, &milvuspb.LoadCollectionRequest{
		DbName: "db_other", CollectionName: "c1",
	})
	assert.Error(t, err)
}
