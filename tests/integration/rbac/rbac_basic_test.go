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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const defaultAuth = "root:Milvus"

type RBACBasicTestSuite struct {
	integration.MiniClusterSuite
}

func (s *RBACBasicTestSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	s.WithMilvusConfig(paramtable.Get().ProxyCfg.ResolveAliasForPrivilege.Key, "true")
	s.MiniClusterSuite.SetupSuite()
}

func (s *RBACBasicTestSuite) TestDropRole() {
	ctx := GetContext(context.Background(), defaultAuth)

	createRole := func(name string) {
		resp, err := s.Cluster.MilvusClient.CreateRole(ctx, &milvuspb.CreateRoleRequest{
			Entity: &milvuspb.RoleEntity{Name: name},
		})
		s.NoError(err)
		s.True(merr.Ok(resp))
	}

	operatePrivilege := func(role, privilege, objectName, dbName string, operateType milvuspb.OperatePrivilegeType) {
		resp, err := s.Cluster.MilvusClient.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
			Type: operateType,
			Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: role},
				Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Global.String()},
				ObjectName: collectionName,
				DbName:     dbName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: util.UserRoot},
					Privilege: &milvuspb.PrivilegeEntity{Name: privilege},
				},
			},
			Version: "v2",
		})
		s.NoError(err)
		s.True(merr.Ok(resp))
	}

	// generate some rbac content
	// create role test_role
	roleName := fmt.Sprintf("test_role_%d", rand.Int31n(1000000))
	createRole(roleName)

	// grant collection level search privilege to role test_role_2
	operatePrivilege(roleName, "Search", util.AnyWord, "db1", milvuspb.OperatePrivilegeType_Grant)

	// drop role test_role_2
	dropRoleResp, err := s.Cluster.MilvusClient.DropRole(ctx, &milvuspb.DropRoleRequest{
		RoleName: roleName,
	})
	s.NoError(err)
	s.False(merr.Ok(dropRoleResp))

	// force delete role when grants exist
	dropRoleResp, err = s.Cluster.MilvusClient.DropRole(ctx, &milvuspb.DropRoleRequest{
		RoleName:  roleName,
		ForceDrop: true,
	})
	s.NoError(err)
	s.True(merr.Ok(dropRoleResp))

	// test empty rbac content
	backupRBACResp, err := s.Cluster.MilvusClient.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	s.NoError(err)
	s.True(merr.Ok(backupRBACResp.GetStatus()))
	for _, role := range backupRBACResp.GetRBACMeta().Roles {
		s.NotEqual(roleName, role.GetName())
	}
}

func (s *RBACBasicTestSuite) TestCredentialDescriptionRoundTrip() {
	rootCtx := GetContext(context.Background(), defaultAuth)
	userName := fmt.Sprintf("desc_user_%d", rand.Int31n(1000000))
	oldPassword := "old_password"
	newPassword := "new_password"
	description := "CJK user description \xe6\x9d\x83\xe9\x99\x90\xe7\x94\xa8\xe6\x88\xb7"
	updatedDescription := "updated description \xe6\x9b\xb4\xe6\x96\xb0"

	resp, err := s.Cluster.MilvusClient.CreateCredential(rootCtx, &milvuspb.CreateCredentialRequest{
		Username:    userName,
		Password:    crypto.Base64Encode(oldPassword),
		Description: &description,
	})
	s.NoError(err)
	s.True(merr.Ok(resp))
	defer s.Cluster.MilvusClient.DeleteCredential(rootCtx, &milvuspb.DeleteCredentialRequest{Username: userName}) //nolint

	selectResp, err := s.Cluster.MilvusClient.SelectUser(rootCtx, &milvuspb.SelectUserRequest{
		User:            &milvuspb.UserEntity{Name: userName},
		IncludeRoleInfo: false,
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Len(selectResp.GetResults(), 1)
	s.Equal(description, selectResp.GetResults()[0].GetDescription())

	updateResp, err := s.Cluster.MilvusClient.UpdateCredential(rootCtx, &milvuspb.UpdateCredentialRequest{
		Username:    userName,
		Description: &updatedDescription,
	})
	s.NoError(err)
	s.True(merr.Ok(updateResp))

	oldUserCtx := GetContext(context.Background(), fmt.Sprintf("%s:%s", userName, oldPassword))
	authResp, err := s.Cluster.MilvusClient.ShowCollections(oldUserCtx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(authResp.GetStatus()))

	selectResp, err = s.Cluster.MilvusClient.SelectUser(rootCtx, &milvuspb.SelectUserRequest{
		User:            &milvuspb.UserEntity{Name: userName},
		IncludeRoleInfo: false,
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Len(selectResp.GetResults(), 1)
	s.Equal(updatedDescription, selectResp.GetResults()[0].GetDescription())

	updateResp, err = s.Cluster.MilvusClient.UpdateCredential(rootCtx, &milvuspb.UpdateCredentialRequest{
		Username:    userName,
		OldPassword: crypto.Base64Encode(oldPassword),
		NewPassword: crypto.Base64Encode(newPassword),
	})
	s.NoError(err)
	s.True(merr.Ok(updateResp))

	newUserCtx := GetContext(context.Background(), fmt.Sprintf("%s:%s", userName, newPassword))
	authResp, err = s.Cluster.MilvusClient.ShowCollections(newUserCtx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(authResp.GetStatus()))

	selectResp, err = s.Cluster.MilvusClient.SelectUser(rootCtx, &milvuspb.SelectUserRequest{
		User:            &milvuspb.UserEntity{Name: userName},
		IncludeRoleInfo: false,
	})
	s.NoError(err)
	s.True(merr.Ok(selectResp.GetStatus()))
	s.Len(selectResp.GetResults(), 1)
	s.Equal(updatedDescription, selectResp.GetResults()[0].GetDescription())
}

func TestRBAC(t *testing.T) {
	suite.Run(t, new(RBACBasicTestSuite))
}
