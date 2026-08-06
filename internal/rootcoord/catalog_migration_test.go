package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMigrateRootCoordCatalogCopiesSemanticSnapshot(t *testing.T) {
	ctx := context.Background()
	source := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	target := kvrootcoord.NewCatalog(memkv.NewMemoryKV())

	db := &model.Database{ID: 10, Name: "db1"}
	require.NoError(t, source.CreateDatabase(ctx, db, 100))
	coll := &model.Collection{
		CollectionID: 1000,
		DBID:         10,
		DBName:       "db1",
		Name:         "coll1",
		Fields: []*model.Field{{
			FieldID:  100,
			Name:     "pk",
			DataType: schemapb.DataType_Int64,
		}},
		Partitions: []*model.Partition{{
			PartitionID:   2000,
			PartitionName: "_default",
			CollectionID:  1000,
			State:         etcdpb.PartitionState_PartitionCreated,
		}},
		State: etcdpb.CollectionState_CollectionCreated,
	}
	require.NoError(t, source.CreateCollection(ctx, coll, 101))
	require.NoError(t, source.CreateAlias(ctx, &model.Alias{
		Name:         "alias1",
		CollectionID: 1000,
		DbID:         10,
		State:        etcdpb.AliasState_AliasCreated,
	}, 102))
	require.NoError(t, source.SaveFileResource(ctx, &internalpb.FileResourceInfo{Id: 99, Name: "resource1"}, 1))

	result, err := migrateRootCoordCatalogSnapshot(ctx, source, target, 200)
	require.NoError(t, err)
	require.Equal(t, 1, result.Databases)
	require.Equal(t, 1, result.Collections)
	require.Equal(t, 1, result.Aliases)
	require.Equal(t, 1, result.FileResources)

	got, err := target.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, int64(1000), got.CollectionID)
	require.Len(t, got.Partitions, 1)

	aliases, err := target.ListAliases(ctx, 10, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, aliases, 1)
	require.Equal(t, "alias1", aliases[0].Name)

	resources, version, err := target.ListFileResource(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Len(t, resources, 1)
}

func TestMigrateRootCoordCatalogCopiesRBACAndCredentials(t *testing.T) {
	ctx := context.Background()
	source := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	target := kvrootcoord.NewCatalog(memkv.NewMemoryKV())

	require.NoError(t, source.AlterCredential(ctx, &model.Credential{
		Username:          "root",
		EncryptedPassword: "root-secret",
		Tenant:            util.DefaultTenant,
	}))
	require.NoError(t, source.AlterCredential(ctx, &model.Credential{
		Username:          "user1",
		EncryptedPassword: "user-secret",
		Tenant:            util.DefaultTenant,
	}))
	require.NoError(t, source.CreateRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: "role1"}))
	require.NoError(t, source.AlterUserRole(ctx, util.DefaultTenant,
		&milvuspb.UserEntity{Name: "user1"},
		&milvuspb.RoleEntity{Name: "role1"},
		milvuspb.OperateUserRoleType_AddUserToRole,
	))
	require.NoError(t, source.AlterGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "role1"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "coll1",
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: "root"},
			Privilege: &milvuspb.PrivilegeEntity{Name: util.PrivilegeNameForMetastore("Load")},
		},
		DbName: util.DefaultDBName,
	}, milvuspb.OperatePrivilegeType_Grant))
	require.NoError(t, source.SavePrivilegeGroup(ctx, &milvuspb.PrivilegeGroupInfo{
		GroupName:  "custom_group",
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "CreateCollection"}},
	}))

	_, err := migrateRootCoordCatalogSnapshot(ctx, source, target, 200)
	require.NoError(t, err)

	rootCred, err := target.GetCredential(ctx, "root")
	require.NoError(t, err)
	require.Equal(t, "root-secret", rootCred.EncryptedPassword)

	userCred, err := target.GetCredential(ctx, "user1")
	require.NoError(t, err)
	require.Equal(t, "user-secret", userCred.EncryptedPassword)

	users, err := target.ListUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: "user1"}, true)
	require.NoError(t, err)
	require.Len(t, users, 1)
	require.Len(t, users[0].Roles, 1)
	require.Equal(t, "role1", users[0].Roles[0].Name)

	grants, err := target.ListGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "role1"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "coll1",
		DbName:     util.DefaultDBName,
	})
	require.NoError(t, err)
	require.Len(t, grants, 1)
	require.Equal(t, "Load", grants[0].Grantor.Privilege.Name)

	groups, err := target.ListPrivilegeGroups(ctx)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, "custom_group", groups[0].GroupName)
}

func TestMetaTableCutoverCatalogMigratesAndReloadsWithoutReplacingMetaTable(t *testing.T) {
	ctx := context.Background()
	source := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	target := kvrootcoord.NewCatalog(memkv.NewMemoryKV())

	require.NoError(t, source.CreateDatabase(ctx, model.NewDefaultDatabase(nil), 99))
	db := &model.Database{ID: 10, Name: "db1"}
	require.NoError(t, source.CreateDatabase(ctx, db, 100))
	coll := &model.Collection{
		CollectionID: 1000,
		DBID:         10,
		DBName:       "db1",
		Name:         "coll1",
		Fields: []*model.Field{{
			FieldID:  100,
			Name:     "pk",
			DataType: schemapb.DataType_Int64,
		}},
		Partitions: []*model.Partition{{
			PartitionID:   2000,
			PartitionName: "_default",
			CollectionID:  1000,
			State:         etcdpb.PartitionState_PartitionCreated,
		}},
		State: etcdpb.CollectionState_CollectionCreated,
	}
	require.NoError(t, source.CreateCollection(ctx, coll, 101))

	meta, err := NewMetaTable(ctx, source, nil)
	require.NoError(t, err)
	metaPtr := meta

	result, err := meta.CutoverCatalog(ctx, target, 200)
	require.NoError(t, err)
	require.Equal(t, rootCoordCatalogMigrationResult{
		Databases:   2,
		Collections: 1,
	}, result)
	require.Same(t, metaPtr, meta)

	got, err := meta.GetCollectionByName(ctx, "db1", "coll1", typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Equal(t, int64(1000), got.CollectionID)

	require.NoError(t, meta.CreateDatabase(ctx, &model.Database{ID: 11, Name: "db2"}, 300))
	_, err = target.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	dbs, err := target.ListDatabases(ctx, typeutil.MaxTimestamp)
	require.NoError(t, err)
	dbNames := make([]string, 0, len(dbs))
	for _, db := range dbs {
		dbNames = append(dbNames, db.Name)
	}
	require.Contains(t, dbNames, "db2")
}
