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
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim            = 128
	dbName         = ""
	collectionName = "test_load_collection"
)

type RBACBackupTestSuite struct {
	integration.MiniClusterSuite
}

func (s *RBACBackupTestSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func GetContext(ctx context.Context, originValue string) context.Context {
	authKey := strings.ToLower(util.HeaderAuthorize)
	authValue := crypto.Base64Encode(originValue)
	contextMap := map[string]string{
		authKey: authValue,
	}
	md := metadata.New(contextMap)
	return metadata.NewIncomingContext(ctx, md)
}

func (s *RBACBackupTestSuite) TestBackup() {
	ctx := GetContext(context.Background(), "root:123456")
	// test empty rbac content
	resp, err := s.Cluster.Proxy.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Equal("", resp.GetRBACMeta().String())

	// generate some rbac content
	roleName := "test_role"
	resp1, err := s.Cluster.Proxy.CreateRole(ctx, &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{
			Name: roleName,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp1))
	resp2, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Grant,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Search"},
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp2))
	s.Equal("", resp2.GetReason())
	userName := "test_user"
	passwd := "test_passwd"
	resp3, err := s.Cluster.Proxy.CreateCredential(ctx, &milvuspb.CreateCredentialRequest{
		Username: userName,
		Password: crypto.Base64Encode(passwd),
	})
	s.NoError(err)
	s.True(merr.Ok(resp3))
	resp4, err := s.Cluster.Proxy.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp4))

	// test back up rbac
	resp5, err := s.Cluster.Proxy.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp5.GetStatus()))

	// test restore, expect to failed due to role/user already exist
	resp6, err := s.Cluster.Proxy.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: resp5.GetRBACMeta(),
	})
	s.NoError(err)
	s.False(merr.Ok(resp6))

	// drop exist role/user, successful to restore
	resp7, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Revoke,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Search"},
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp7))
	resp8, err := s.Cluster.Proxy.DropRole(ctx, &milvuspb.DropRoleRequest{
		RoleName: roleName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp8))
	resp9, err := s.Cluster.Proxy.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{
		Username: userName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp9))

	resp10, err := s.Cluster.Proxy.RestoreRBAC(ctx, &milvuspb.RestoreRBACMetaRequest{
		RBACMeta: resp5.GetRBACMeta(),
	})
	s.NoError(err)
	s.True(merr.Ok(resp10))

	// check the restored rbac, should be same as the original one
	resp11, err := s.Cluster.Proxy.BackupRBAC(ctx, &milvuspb.BackupRBACMetaRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp11.GetStatus()))
	s.Equal(resp11.GetRBACMeta().String(), resp5.GetRBACMeta().String())

	// clean rbac meta
	resp12, err := s.Cluster.Proxy.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{
		Type: milvuspb.OperatePrivilegeType_Revoke,
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: util.UserRoot},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Search"},
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp12))

	resp13, err := s.Cluster.Proxy.DropRole(ctx, &milvuspb.DropRoleRequest{
		RoleName: roleName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp13))

	resp14, err := s.Cluster.Proxy.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{
		Username: userName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp14))
}

func TestRBACBackup(t *testing.T) {
	suite.Run(t, new(RBACBackupTestSuite))
}
