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

func TestRBAC(t *testing.T) {
	suite.Run(t, new(RBACBasicTestSuite))
}
