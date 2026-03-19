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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

// TestAliasRBAC verifies that the RBAC interceptor resolves aliases to real
// collection names before checking permissions. When a user operates on a
// collection through an alias, the permission check must be against the real
// collection name, not the alias.
func (s *RBACBasicTestSuite) TestAliasRBAC() {
	rootCtx := GetContext(context.Background(), defaultAuth)

	realColName := "real_collection_rbac"
	aliasName := "alias_collection_rbac"
	roleName := "alias_role"
	userName := "alias_user"
	userPassword := "alias_passwd"

	// Setup: create collection, alias, role, user
	schema := integration.ConstructSchema(realColName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)
	createCollResp, err := s.Cluster.MilvusClient.CreateCollection(rootCtx, &milvuspb.CreateCollectionRequest{
		CollectionName: realColName,
		Schema:         marshaledSchema,
	})
	s.NoError(err)
	s.True(merr.Ok(createCollResp))

	createAliasResp, err := s.Cluster.MilvusClient.CreateAlias(rootCtx, &milvuspb.CreateAliasRequest{
		CollectionName: realColName,
		Alias:          aliasName,
	})
	s.NoError(err)
	s.True(merr.Ok(createAliasResp))

	resp, err := s.Cluster.MilvusClient.CreateRole(rootCtx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: roleName},
	})
	s.NoError(err)
	s.True(merr.Ok(resp))

	resp, err = s.Cluster.MilvusClient.CreateCredential(rootCtx, &milvuspb.CreateCredentialRequest{
		Username: userName,
		Password: crypto.Base64Encode(userPassword),
	})
	s.NoError(err)
	s.True(merr.Ok(resp))

	resp, err = s.Cluster.MilvusClient.OperateUserRole(rootCtx, &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
		Type:     milvuspb.OperateUserRoleType_AddUserToRole,
	})
	s.NoError(err)
	s.True(merr.Ok(resp))

	defer func() {
		s.Cluster.MilvusClient.DropAlias(rootCtx, &milvuspb.DropAliasRequest{Alias: aliasName})                      //nolint
		s.Cluster.MilvusClient.DropCollection(rootCtx, &milvuspb.DropCollectionRequest{CollectionName: realColName}) //nolint
		s.Cluster.MilvusClient.DropRole(rootCtx, &milvuspb.DropRoleRequest{RoleName: roleName, ForceDrop: true})     //nolint
		s.Cluster.MilvusClient.DeleteCredential(rootCtx, &milvuspb.DeleteCredentialRequest{Username: userName})      //nolint
	}()

	userCtx := GetContext(context.Background(), fmt.Sprintf("%s:%s", userName, userPassword))

	// Without GetStatistics privilege, GetCollectionStatistics via alias should fail.
	// The privilege interceptor resolves the alias to the real collection name,
	// so using an alias does NOT bypass RBAC.
	_, err = s.Cluster.MilvusClient.GetCollectionStatistics(userCtx, &milvuspb.GetCollectionStatisticsRequest{
		CollectionName: aliasName,
	})
	s.Error(err, "GetCollectionStatistics via alias should fail without permission")

	// Grant GetStatistics on the REAL collection name (not alias)
	grantResp, err := s.Cluster.MilvusClient.OperatePrivilegeV2(rootCtx, &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: roleName},
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: util.UserRoot},
			Privilege: &milvuspb.PrivilegeEntity{Name: "GetStatistics"},
		},
		Type:           milvuspb.OperatePrivilegeType_Grant,
		DbName:         util.AnyWord,
		CollectionName: realColName,
	})
	s.NoError(err)
	s.True(merr.Ok(grantResp))

	// Wait for privilege cache to refresh
	time.Sleep(3 * time.Second)

	// Now GetCollectionStatistics via alias should succeed because the interceptor
	// resolves the alias to the real collection name before checking permission.
	statsResp, err := s.Cluster.MilvusClient.GetCollectionStatistics(userCtx, &milvuspb.GetCollectionStatisticsRequest{
		CollectionName: aliasName,
	})
	s.NoError(err)
	s.True(merr.Ok(statsResp.GetStatus()), "GetCollectionStatistics via alias should succeed when user has permission on real collection")
}
