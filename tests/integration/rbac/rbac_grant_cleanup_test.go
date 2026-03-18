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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
