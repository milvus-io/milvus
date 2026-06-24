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
			DbName:         "default",
			CollectionName: "col1",
		})
		assert.NoError(t, err)
		_, err = PrivilegeInterceptor(ctx, &milvuspb.LoadCollectionRequest{
			DbName:         "default",
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
			DbName:          "default",
			CollectionNames: []string{"col1"},
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "fooo:123456"), &milvuspb.LoadCollectionRequest{
			DbName:         "default",
			CollectionName: "col1",
		})
		assert.NoError(t, err)

		// fooo holds Global-All only on "default"; a request explicitly targeting
		// another db must be denied regardless of the connection-context db.
		_, err = PrivilegeInterceptor(GetContextWithDB(context.Background(), "fooo:123456", "default"), &milvuspb.LoadCollectionRequest{
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

// TestPrivilegeInterceptorRequestDBName guards against milvus-io/milvus#50678:
// the privilege check must run against the db the request actually operates on
// (request-body DbName takes precedence), not merely the connection-context db.
func TestPrivilegeInterceptorRequestDBName(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				// scenario A: alice is granted Load on col1 ONLY in db_target.
				funcutil.PolicyForPrivilege("role_a", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeLoad.String(), "db_target"),
				// scenario B: bob is granted Load on col1 ONLY in default.
				funcutil.PolicyForPrivilege("role_b", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeLoad.String(), "default"),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("alice", "role_a"),
				funcutil.EncodeUserRoleCache("bob", "role_b"),
			},
		}, nil
	}
	err := InitMetaCache(context.Background(), client)
	assert.NoError(t, err)

	// Both users connect WITHOUT useDatabase, so the connection-context db is
	// "default"; they target a database purely via the request body DbName.
	t.Run("false denial fixed: granted on db_target, request db_target -> allowed", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "alice:pwd"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_target",
			CollectionName: "col1",
		})
		assert.NoError(t, err)
	})

	t.Run("escalation blocked: granted on default, request db_escalate -> denied", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "bob:pwd"), &milvuspb.LoadCollectionRequest{
			DbName:         "db_escalate",
			CollectionName: "col1",
		})
		assert.Error(t, err)
	})

	t.Run("fallback to context db when request carries no db_name", func(t *testing.T) {
		// bob is granted on default; request omits db_name, context db is default.
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "bob:pwd"), &milvuspb.LoadCollectionRequest{
			CollectionName: "col1",
		})
		assert.NoError(t, err)
	})
}

// TestPrivilegeInterceptorClusterLevel guards the cluster-level half of the
// #50678 fix: cluster-level privileges (e.g. CreateDatabase/DropDatabase) are
// not scoped to a specific database, so they must be authorized against the
// connection-context db rather than the request's DbName (which for
// CreateDatabase is the brand-new database name and would never match the
// cluster grant). Without the level-aware resolution this denies every
// pre-existing cluster grant.
func TestPrivilegeInterceptorClusterLevel(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				// cluster admin: granted PrivilegeAll on the connection-context db
				// (default). carol below also connects on the default db, so the
				// cluster check (context-scoped) matches this grant.
				funcutil.PolicyForPrivilege("role_cluster", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAll.String(), "default"),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("carol", "role_cluster"),
			},
		}, nil
	}
	err := InitMetaCache(context.Background(), client)
	assert.NoError(t, err)

	t.Run("cluster grant authorizes CreateDatabase for any new db name", func(t *testing.T) {
		// carol connects without useDatabase (context db = default); the request
		// targets a brand-new db. Authorization must use AnyWord, not "brand_new_db".
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "carol:pwd"), &milvuspb.CreateDatabaseRequest{
			DbName: "brand_new_db",
		})
		assert.NoError(t, err)
	})

	t.Run("cluster grant authorizes DropDatabase regardless of target db", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "carol:pwd"), &milvuspb.DropDatabaseRequest{
			DbName: "some_other_db",
		})
		assert.NoError(t, err)
	})

	t.Run("no cluster grant -> CreateDatabase denied", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "dave:pwd"), &milvuspb.CreateDatabaseRequest{
			DbName: "brand_new_db",
		})
		assert.Error(t, err)
	})
}

// TestPrivilegeInterceptorDatabaseLevel covers the database-level half of the
// #50678 fix using the issue's original reproducer (AlterDatabase). A
// database-level privilege is scoped to the db the request targets, so a grant
// on db_target authorizes the op even without a prior useDatabase (false denial
// fixed), while a grant on another db does not (cross-db escalation blocked).
func TestPrivilegeInterceptorDatabaseLevel(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	client := &MockMixCoordClientInterface{}
	client.listPolicy = func(ctx context.Context, in *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
		return &internalpb.ListPolicyResponse{
			Status: merr.Success(),
			PolicyInfos: []string{
				// AlterDatabase granted ONLY on db_target (database-level: objectName=*).
				funcutil.PolicyForPrivilege("role_alter", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAlterDatabase.String(), "db_target"),
				// DescribeDatabase granted ONLY on default.
				funcutil.PolicyForPrivilege("role_desc", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeDescribeDatabase.String(), "default"),
			},
			UserRoles: []string{
				funcutil.EncodeUserRoleCache("erin", "role_alter"),
				funcutil.EncodeUserRoleCache("frank", "role_desc"),
			},
		}, nil
	}
	err := InitMetaCache(context.Background(), client)
	assert.NoError(t, err)

	// erin connects without useDatabase (context db = default); grant is on db_target.
	t.Run("false denial fixed: AlterDatabase granted on db_target, request db_target -> allowed", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "erin:pwd"), &milvuspb.AlterDatabaseRequest{
			DbName: "db_target",
		})
		assert.NoError(t, err)
	})

	t.Run("escalation blocked: AlterDatabase granted on db_target, request another db -> denied", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "erin:pwd"), &milvuspb.AlterDatabaseRequest{
			DbName: "db_other",
		})
		assert.Error(t, err)
	})

	// frank is granted DescribeDatabase only on default.
	t.Run("DescribeDatabase scoped to granted db", func(t *testing.T) {
		_, err := PrivilegeInterceptor(GetContext(context.Background(), "frank:pwd"), &milvuspb.DescribeDatabaseRequest{
			DbName: "default",
		})
		assert.NoError(t, err)

		_, err = PrivilegeInterceptor(GetContext(context.Background(), "frank:pwd"), &milvuspb.DescribeDatabaseRequest{
			DbName: "db_secret",
		})
		assert.Error(t, err)
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
