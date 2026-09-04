// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestExecuteOperatePrivilegeGroupTaskStepsAddPrivilegesByName(t *testing.T) {
	ctx := context.Background()
	groupName := "group1"
	roleName := "role1"
	dbName := util.DefaultDBName
	objectName := "collection1"

	meta := newMockMetaTable()
	meta.ListPrivilegeGroupsFunc = func(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
		return []*milvuspb.PrivilegeGroupInfo{
			{
				GroupName: groupName,
				Privileges: []*milvuspb.PrivilegeEntity{
					{Name: "Query"},
				},
			},
		}, nil
	}
	meta.GetPrivilegeGroupRolesFunc = func(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error) {
		return []*milvuspb.RoleEntity{{Name: roleName}}, nil
	}
	meta.SelectGrantFunc = func(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
		return []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: roleName},
				ObjectName: objectName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: "root"},
					Privilege: &milvuspb.PrivilegeEntity{Name: groupName},
				},
				DbName: dbName,
			},
		}, nil
	}
	meta.OperatePrivilegeGroupFunc = func(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
		return nil
	}

	core := newTestCore(withMeta(meta))
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)

	var (
		mu              sync.Mutex
		refreshRequests []*proxypb.RefreshPolicyInfoCacheRequest
	)
	proxy := newMockProxy()
	proxy.RefreshPolicyInfoCacheFunc = func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
		mu.Lock()
		defer mu.Unlock()
		refreshRequests = append(refreshRequests, request)
		return merr.Success(), nil
	}
	core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)

	err := executeOperatePrivilegeGroupTaskSteps(ctx, core, &milvuspb.PrivilegeGroupInfo{
		GroupName: groupName,
		Privileges: []*milvuspb.PrivilegeEntity{
			{Name: "Query"},
		},
	}, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup)

	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, refreshRequests)
}

func TestExecuteOperatePrivilegeGroupTaskStepsRemovePrivilegesByName(t *testing.T) {
	ctx := context.Background()
	groupName := "group1"
	roleName := "role1"
	dbName := util.DefaultDBName
	objectName := "collection1"

	meta := newMockMetaTable()
	meta.ListPrivilegeGroupsFunc = func(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
		return []*milvuspb.PrivilegeGroupInfo{
			{
				GroupName: groupName,
				Privileges: []*milvuspb.PrivilegeEntity{
					{Name: "Query"},
					{Name: "Load"},
				},
			},
		}, nil
	}
	meta.GetPrivilegeGroupRolesFunc = func(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error) {
		return []*milvuspb.RoleEntity{{Name: roleName}}, nil
	}
	meta.SelectGrantFunc = func(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
		return []*milvuspb.GrantEntity{
			{
				Role:       &milvuspb.RoleEntity{Name: roleName},
				ObjectName: objectName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: "root"},
					Privilege: &milvuspb.PrivilegeEntity{Name: groupName},
				},
				DbName: dbName,
			},
		}, nil
	}
	meta.OperatePrivilegeGroupFunc = func(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
		return nil
	}

	core := newTestCore(withMeta(meta))
	core.proxyClientManager = proxyutil.NewProxyClientManager(proxyutil.DefaultProxyCreator)

	var (
		mu              sync.Mutex
		refreshRequests []*proxypb.RefreshPolicyInfoCacheRequest
	)
	proxy := newMockProxy()
	proxy.RefreshPolicyInfoCacheFunc = func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
		mu.Lock()
		defer mu.Unlock()
		refreshRequests = append(refreshRequests, request)
		return merr.Success(), nil
	}
	core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)

	err := executeOperatePrivilegeGroupTaskSteps(ctx, core, &milvuspb.PrivilegeGroupInfo{
		GroupName: groupName,
		Privileges: []*milvuspb.PrivilegeEntity{
			{Name: "Query"},
		},
	}, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)

	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, refreshRequests, 1)
	require.Equal(t, int32(typeutil.CacheRevokePrivilege), refreshRequests[0].GetOpType())
	require.Equal(t,
		funcutil.PolicyForPrivilege(roleName, util.GetObjectType("Query"), objectName, util.PrivilegeNameForMetastore("Query"), dbName),
		refreshRequests[0].GetOpKey(),
	)
}
