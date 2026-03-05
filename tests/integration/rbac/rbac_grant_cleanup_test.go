// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rbac

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

// TestDropCollectionCleansGrants verifies that dropping a collection removes
// its associated RBAC grant entries, so re-creating a collection with the same
// name does NOT inherit stale permissions.
func (s *RBACBasicTestSuite) TestDropCollectionCleansGrants() {
	ctx := GetContext(context.Background(), defaultAuth)
	colName := "grant_cleanup_drop_col"
	roleName := "grant_cleanup_drop_role"

	// Create collection
	schema := integration.ConstructSchema(colName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: colName,
		Schema:         marshaledSchema,
	})
	s.NoError(err)
	s.True(merr.Ok(createResp))

	// Create role and grant a privilege on the collection
	resp, err := s.Cluster.MilvusClient.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(resp))

	grantResp, err := s.Cluster.MilvusClient.OperatePrivilegeV2(ctx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: roleName},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: "GetStatistics"},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         util.AnyWord,
		CollectionName: colName,
	})
	s.NoError(err)
	s.True(merr.Ok(grantResp))

	// Verify grant exists
	selectResp, err := s.Cluster.MilvusClient.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: colName,
			DbName:     util.AnyWord,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.NotEmpty(selectResp.GetEntities(), "grant should exist before drop")

	// Drop collection
	dropResp, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: colName,
	})
	s.NoError(err)
	s.True(merr.Ok(dropResp))
	time.Sleep(2 * time.Second)

	// Verify grant is cleaned up
	selectResp, err = s.Cluster.MilvusClient.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: colName,
			DbName:     util.AnyWord,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Empty(selectResp.GetEntities(), "grants should be cleaned up after drop collection")

	// Cleanup
	s.Cluster.MilvusClient.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName, ForceDrop: true}) //nolint
}

// TestRenameCollectionMigratesGrants verifies that renaming a collection
// migrates its RBAC grant entries to the new name.
func (s *RBACBasicTestSuite) TestRenameCollectionMigratesGrants() {
	ctx := GetContext(context.Background(), defaultAuth)
	oldName := "grant_migrate_old_col"
	newName := "grant_migrate_new_col"
	roleName := "grant_migrate_role"

	// Create collection
	schema := integration.ConstructSchema(oldName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: oldName,
		Schema:         marshaledSchema,
	})
	s.NoError(err)
	s.True(merr.Ok(createResp))

	// Create role and grant a privilege on the collection
	resp, err := s.Cluster.MilvusClient.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(resp))

	grantResp, err := s.Cluster.MilvusClient.OperatePrivilegeV2(ctx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: roleName},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: "GetStatistics"},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         util.AnyWord,
		CollectionName: oldName,
	})
	s.NoError(err)
	s.True(merr.Ok(grantResp))

	// Rename collection
	renameResp, err := s.Cluster.MilvusClient.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		OldName: oldName,
		NewName: newName,
	})
	s.NoError(err)
	s.True(merr.Ok(renameResp))
	time.Sleep(2 * time.Second)

	// Verify grant no longer exists under old name
	selectResp, err := s.Cluster.MilvusClient.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: oldName,
			DbName:     util.AnyWord,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Empty(selectResp.GetEntities(), "grants should not exist under old name after rename")

	// Verify grant now exists under new name
	selectResp, err = s.Cluster.MilvusClient.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: newName,
			DbName:     util.AnyWord,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.NotEmpty(selectResp.GetEntities(), "grants should be migrated to new name after rename")

	// Cleanup
	s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: newName}) //nolint
	s.Cluster.MilvusClient.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName, ForceDrop: true}) //nolint
}

// TestRenameCollectionGrantSurvivesRestart verifies that an ID-based grant
// for a renamed collection still resolves to the new name after the coord
// process restarts. This is the load-bearing property of ID-based grant
// storage: on startup, MetaTable rebuilds its in-memory name index from
// the catalog, and the grant key (dbID:X.colID:Y) must remain pointing
// at the collection regardless of any rename that happened before the
// restart. If grants were ever written back as name-based, the post-restart
// SelectGrant under the new name would return empty.
func (s *RBACBasicTestSuite) TestRenameCollectionGrantSurvivesRestart() {
	ctx := GetContext(context.Background(), defaultAuth)
	oldName := "grant_restart_old_col"
	newName := "grant_restart_new_col"
	roleName := "grant_restart_role"

	schema := integration.ConstructSchema(oldName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	createResp, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: oldName,
		Schema:         marshaledSchema,
	})
	s.NoError(err)
	s.True(merr.Ok(createResp))

	roleResp, err := s.Cluster.MilvusClient.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(roleResp))

	grantResp, err := s.Cluster.MilvusClient.OperatePrivilegeV2(ctx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: roleName},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: "GetStatistics"},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         util.AnyWord,
		CollectionName: oldName,
	})
	s.NoError(err)
	s.True(merr.Ok(grantResp))

	renameResp, err := s.Cluster.MilvusClient.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		OldName: oldName,
		NewName: newName,
	})
	s.NoError(err)
	s.True(merr.Ok(renameResp))
	time.Sleep(2 * time.Second)

	// Restart mixcoord (which hosts rootcoord in 2.6+). After restart, the
	// MetaTable must reconstruct its in-memory name→grant mapping from
	// persisted ID-based grant keys.
	s.Cluster.DefaultMixCoord().Stop()
	s.Cluster.AddMixCoord()

	// Combine readiness probe and the property-under-test into a single
	// Eventually: poll SelectGrant until it returns success AND the grant
	// resolves under the new name AND no longer resolves under the old
	// name. ShowCollections is not used as a readiness gate because it
	// could pass while MetaTable's grant reverse-index is still rebuilding,
	// producing a false-positive failure.
	s.Eventually(func() bool {
		newResp, err := s.Cluster.MilvusClient.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: roleName},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: newName,
				DbName:     util.AnyWord,
			},
		})
		if err != nil || !merr.Ok(newResp.GetStatus()) || len(newResp.GetEntities()) == 0 {
			return false
		}
		oldResp, err := s.Cluster.MilvusClient.SelectGrant(ctx, &milvuspb.SelectGrantRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: roleName},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: oldName,
				DbName:     util.AnyWord,
			},
		})
		return err == nil && merr.Ok(oldResp.GetStatus()) && len(oldResp.GetEntities()) == 0
	}, 2*time.Minute, 2*time.Second, "grant should survive rename and coord restart under new name only")

	// Cleanup
	s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: newName}) //nolint
	s.Cluster.MilvusClient.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: roleName, ForceDrop: true}) //nolint
}

// TestDropDatabaseClearsWildcardGrantAndRefreshesCache verifies both halves
// of the DropDatabase grant-cleanup path:
//  1. The catalog removes any grants of the form "<db>.*" for the dropped db.
//  2. The proxy privilege cache is refreshed, so if a database with the same
//     name is later re-created (with a fresh dbID), the old grant does NOT
//     silently re-activate against the new database.
//
// The second property is the non-trivial one. A DropDatabase that only
// clears the catalog but forgets to push a cache refresh to proxies would
// let a stale policy entry keyed on dbName survive. We make that observable
// by using a non-root user (root bypasses RBAC) and checking that the user
// is denied on the recreated <db>.<col> — if the cache held the old wildcard
// grant, the request would succeed.
func (s *RBACBasicTestSuite) TestDropDatabaseClearsWildcardGrantAndRefreshesCache() {
	rootCtx := GetContext(context.Background(), defaultAuth)
	dbName := "grant_dropdb_db"
	colName := "grant_dropdb_col"
	roleName := "grant_dropdb_role"
	userName := "grant_dropdb_user"
	userPassword := "grant_dropdb_pwd"

	defer func() {
		s.Cluster.MilvusClient.DropCollection(rootCtx, &milvuspb.DropCollectionRequest{DbName: dbName, CollectionName: colName}) //nolint
		s.Cluster.MilvusClient.DropDatabase(rootCtx, &milvuspb.DropDatabaseRequest{DbName: dbName})                              //nolint
		s.Cluster.MilvusClient.DropRole(rootCtx, &milvuspb.DropRoleRequest{RoleName: roleName, ForceDrop: true})                 //nolint
		s.Cluster.MilvusClient.DeleteCredential(rootCtx, &milvuspb.DeleteCredentialRequest{Username: userName})                  //nolint
	}()

	createDBResp, err := s.Cluster.MilvusClient.CreateDatabase(rootCtx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	s.NoError(err)
	s.True(merr.Ok(createDBResp))

	schema := integration.ConstructSchema(colName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	createCollResp, err := s.Cluster.MilvusClient.CreateCollection(rootCtx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: colName,
		Schema:         marshaledSchema,
	})
	s.NoError(err)
	s.True(merr.Ok(createCollResp))

	roleResp, err := s.Cluster.MilvusClient.CreateRole(rootCtx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(roleResp))

	credResp, err := s.Cluster.MilvusClient.CreateCredential(rootCtx, &milvuspb.CreateCredentialRequest{
		Username: userName,
		Password: crypto.Base64Encode(userPassword),
	})
	s.NoError(err)
	s.True(merr.Ok(credResp))

	bindResp, err := s.Cluster.MilvusClient.OperateUserRole(rootCtx, &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
		Type:     milvuspb.OperateUserRoleType_AddUserToRole,
	})
	s.NoError(err)
	s.True(merr.Ok(bindResp))

	// Grant wildcard privilege on <db>.* to the role.
	grantResp, err := s.Cluster.MilvusClient.OperatePrivilegeV2(rootCtx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: roleName},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: "GetStatistics"},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         dbName,
		CollectionName: util.AnyWord,
	})
	s.NoError(err)
	s.True(merr.Ok(grantResp))

	userCtx := GetContext(context.Background(), fmt.Sprintf("%s:%s", userName, userPassword))

	// Sanity: poll until the wildcard grant is reflected in the proxy
	// privilege cache and lets the user in. Cache refresh is async — sleep
	// is fragile, especially under CI load.
	s.Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.GetCollectionStatistics(userCtx, &milvuspb.GetCollectionStatisticsRequest{
			DbName:         dbName,
			CollectionName: colName,
		})
		return err == nil && merr.Ok(resp.GetStatus())
	}, 30*time.Second, 500*time.Millisecond, "user should have access via <db>.* wildcard grant before drop")

	// DropDatabase requires an empty database, so drop the collection first.
	dropCollResp, err := s.Cluster.MilvusClient.DropCollection(rootCtx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: colName,
	})
	s.NoError(err)
	s.True(merr.Ok(dropCollResp))

	dropDBResp, err := s.Cluster.MilvusClient.DropDatabase(rootCtx, &milvuspb.DropDatabaseRequest{DbName: dbName})
	s.NoError(err)
	s.True(merr.Ok(dropDBResp))

	// Catalog side: poll until the wildcard grant for the dropped db is
	// gone. Grant cleanup is async via the broadcast ack callback.
	s.Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.SelectGrant(rootCtx, &milvuspb.SelectGrantRequest{
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: roleName},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
				ObjectName: util.AnyWord,
				DbName:     dbName,
			},
		})
		return err == nil && merr.Ok(resp.GetStatus()) && len(resp.GetEntities()) == 0
	}, 30*time.Second, 500*time.Millisecond, "<db>.* grant should be cleaned up after DropDatabase")

	// Re-create database and collection with the same names but fresh IDs.
	// If the proxy privilege cache was NOT refreshed on DropDatabase, the
	// stale policy entry (keyed by dbName) would still grant access, and
	// the request below would silently succeed — leaking permissions to
	// a different logical database that just happens to share the name.
	recreateDBResp, err := s.Cluster.MilvusClient.CreateDatabase(rootCtx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	s.NoError(err)
	s.True(merr.Ok(recreateDBResp))

	recreateCollResp, err := s.Cluster.MilvusClient.CreateCollection(rootCtx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: colName,
		Schema:         marshaledSchema,
	})
	s.NoError(err)
	s.True(merr.Ok(recreateCollResp))

	// The user must be denied on the recreated database's collection.
	// Poll until denial is observed: if the proxy cache still holds the old
	// wildcard grant, the request would succeed (the bug). 30s is enough
	// margin over the cache refresh interval to fail loudly rather than flake.
	s.Eventually(func() bool {
		_, err := s.Cluster.MilvusClient.GetCollectionStatistics(userCtx, &milvuspb.GetCollectionStatisticsRequest{
			DbName:         dbName,
			CollectionName: colName,
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "user should be denied on recreated <db>.<col> — stale wildcard grant must not re-activate")
}
