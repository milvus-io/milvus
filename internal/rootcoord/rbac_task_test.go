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
	"strconv"
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
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableLocalCollectionVisibility.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.ProxyCfg.EnableLocalCollectionVisibility.Key) })
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
	require.EqualValues(t, 1, refreshRequests[0].GetCollectionVisibility().GetFormatVersion())
	require.Empty(t, refreshRequests[0].GetCollectionVisibility().GetRoles())
}

// A privilege the role still holds through a built-in group must not be revoked
// from the proxy cache when it is removed from a custom group: the built-in
// group is expanded into the same policy keys at grant time. The policy key has
// no grantor, so it does not matter which user granted the built-in group.
func TestExecuteOperatePrivilegeGroupTaskStepsRemoveKeepsBuiltinGroupPrivilege(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableLocalCollectionVisibility.Key, "false"))
	t.Cleanup(func() { Params.Reset(Params.ProxyCfg.EnableLocalCollectionVisibility.Key) })
	ctx := context.Background()
	groupName := "group1"
	roleName := "role1"
	dbName := util.DefaultDBName
	objectName := "collection1"

	newMeta := func(builtinObjectName, builtinGrantor string) *mockMetaTable {
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
				{
					Role:       &milvuspb.RoleEntity{Name: roleName},
					ObjectName: builtinObjectName,
					Grantor: &milvuspb.GrantorEntity{
						User:      &milvuspb.UserEntity{Name: builtinGrantor},
						Privilege: &milvuspb.PrivilegeEntity{Name: "CollectionReadOnly"},
					},
					DbName: dbName,
				},
			}, nil
		}
		meta.OperatePrivilegeGroupFunc = func(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
			return nil
		}
		return meta
	}

	run := func(t *testing.T, builtinObjectName, builtinGrantor string) []*proxypb.RefreshPolicyInfoCacheRequest {
		core := newTestCore(withMeta(newMeta(builtinObjectName, builtinGrantor)))
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
			GroupName:  groupName,
			Privileges: []*milvuspb.PrivilegeEntity{{Name: "Query"}},
		}, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		return refreshRequests
	}

	t.Run("same collection: built-in group still covers Query, nothing to revoke", func(t *testing.T) {
		require.Empty(t, run(t, objectName, "root"))
	})

	t.Run("same collection, built-in group granted by another user: nothing to revoke", func(t *testing.T) {
		require.Empty(t, run(t, objectName, "admin"))
	})

	t.Run("other collection: Query on collection1 is still revoked", func(t *testing.T) {
		refreshRequests := run(t, "collection2", "root")
		require.Len(t, refreshRequests, 1)
		require.Nil(t, refreshRequests[0].GetCollectionVisibility())
		require.Equal(t, int32(typeutil.CacheRevokePrivilege), refreshRequests[0].GetOpType())
		require.Equal(t,
			funcutil.PolicyForPrivilege(roleName, util.GetObjectType("Query"), objectName, util.PrivilegeNameForMetastore("Query"), dbName),
			refreshRequests[0].GetOpKey(),
		)
	})
}

// Revoking a privilege group from a role must keep the policy keys the role
// still holds through its remaining grants, even when those grants were made by
// another user: the policy key has no grantor.
func TestExecuteOperatePrivilegeTaskStepsRevokeKeepsKeysOfOtherGrantors(t *testing.T) {
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableLocalCollectionVisibility.Key, "true"))
	t.Cleanup(func() { Params.Reset(Params.ProxyCfg.EnableLocalCollectionVisibility.Key) })
	ctx := context.Background()
	groupName := "group1"
	roleName := "role1"
	dbName := util.DefaultDBName
	objectName := "collection1"

	run := func(t *testing.T, remainingObjectName string) []*proxypb.RefreshPolicyInfoCacheRequest {
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
		meta.IsCustomPrivilegeGroupFunc = func(ctx context.Context, name string) (bool, error) {
			return name == groupName, nil
		}
		meta.OperatePrivilegeFunc = func(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
			return nil
		}
		// after the revoke the role still holds CollectionReadOnly, granted by another user
		meta.SelectGrantFunc = func(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
			return []*milvuspb.GrantEntity{
				{
					Role:       &milvuspb.RoleEntity{Name: roleName},
					ObjectName: remainingObjectName,
					Grantor: &milvuspb.GrantorEntity{
						User:      &milvuspb.UserEntity{Name: "admin"},
						Privilege: &milvuspb.PrivilegeEntity{Name: "CollectionReadOnly"},
					},
					DbName: dbName,
				},
			}, nil
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

		err := executeOperatePrivilegeTaskSteps(ctx, core, &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: roleName},
			Object:     &milvuspb.ObjectEntity{Name: commonpb.ObjectType_Collection.String()},
			ObjectName: objectName,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: groupName},
			},
			DbName: dbName,
		}, milvuspb.OperatePrivilegeType_Revoke)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		return refreshRequests
	}

	policyKey := func(privilege string) string {
		return funcutil.PolicyForPrivilege(roleName, util.GetObjectType(privilege), objectName, util.PrivilegeNameForMetastore(privilege), dbName)
	}

	t.Run("same collection: Query stays, only Load is revoked", func(t *testing.T) {
		refreshRequests := run(t, objectName)
		require.Len(t, refreshRequests, 1)
		require.Equal(t, int32(typeutil.CacheRevokePrivilege), refreshRequests[0].GetOpType())
		require.Equal(t, policyKey("Load"), refreshRequests[0].GetOpKey())
		visibility := refreshRequests[0].GetCollectionVisibility()
		require.EqualValues(t, 1, visibility.GetFormatVersion())
		require.Len(t, visibility.GetRoles(), 1)
		require.Equal(t, roleName, visibility.GetRoles()[0].GetRole())
		require.Len(t, visibility.GetRoles()[0].GetRules(), 1)
		require.Equal(t, objectName, visibility.GetRoles()[0].GetRules()[0].GetCollectionName(), "remaining group still grants visibility")
	})

	t.Run("other collection: both Query and Load are revoked", func(t *testing.T) {
		refreshRequests := run(t, "collection2")
		require.Len(t, refreshRequests, 1)
		require.Equal(t, int32(typeutil.CacheRevokePrivilege), refreshRequests[0].GetOpType())
		require.Equal(t, policyKey("Query")+"|"+policyKey("Load"), refreshRequests[0].GetOpKey())
	})
}

func TestOperatePrivilegeLocalVisibilityGate(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, tc := range []struct {
			name           string
			operation      milvuspb.OperatePrivilegeType
			emptyGroup     bool
			overlap        bool
			failProjection bool
		}{
			{name: "grant", operation: milvuspb.OperatePrivilegeType_Grant},
			{name: "revoke", operation: milvuspb.OperatePrivilegeType_Revoke},
			{name: "empty group grant", operation: milvuspb.OperatePrivilegeType_Grant, emptyGroup: true},
			{name: "overlapping revoke", operation: milvuspb.OperatePrivilegeType_Revoke, overlap: true},
			{name: "projection read failure", operation: milvuspb.OperatePrivilegeType_Grant, failProjection: true},
		} {
			t.Run(tc.name+"/enabled="+strconv.FormatBool(enabled), func(t *testing.T) {
				require.NoError(t, Params.Save(Params.ProxyCfg.EnableLocalCollectionVisibility.Key, strconv.FormatBool(enabled)))
				t.Cleanup(func() { Params.Reset(Params.ProxyCfg.EnableLocalCollectionVisibility.Key) })
				ctx := context.Background()
				privilegeName := "Search"
				if tc.emptyGroup {
					privilegeName = "EmptyGroup"
				}
				entity := &milvuspb.GrantEntity{
					Role: &milvuspb.RoleEntity{Name: "reader"}, Object: &milvuspb.ObjectEntity{Name: "Collection"},
					DbName: "tenant_b", ObjectName: "orders",
					Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName}, User: &milvuspb.UserEntity{Name: "root"}},
				}
				meta := newMockMetaTable()
				writes := 0
				meta.OperatePrivilegeFunc = func(context.Context, string, *milvuspb.GrantEntity, milvuspb.OperatePrivilegeType) error {
					writes++
					return nil
				}
				meta.IsCustomPrivilegeGroupFunc = func(_ context.Context, name string) (bool, error) {
					return name == "EmptyGroup", nil
				}
				meta.ListPrivilegeGroupsFunc = func(context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
					return []*milvuspb.PrivilegeGroupInfo{{GroupName: "EmptyGroup"}}, nil
				}
				var reads []string
				meta.SelectGrantFunc = func(_ context.Context, _ string, request *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
					require.Equal(t, "reader", request.GetRole().GetName())
					reads = append(reads, request.GetDbName())
					if request.GetDbName() == util.AnyWord && tc.failProjection {
						return nil, merr.ErrServiceUnavailable
					}
					if tc.overlap || tc.operation == milvuspb.OperatePrivilegeType_Grant {
						return []*milvuspb.GrantEntity{entity}, nil
					}
					return nil, nil
				}
				core := newTestCore(withMeta(meta), withValidProxyManager())
				var mu sync.Mutex
				var notifications []*proxypb.RefreshPolicyInfoCacheRequest
				proxy := newMockProxy()
				proxy.RefreshPolicyInfoCacheFunc = func(_ context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
					mu.Lock()
					defer mu.Unlock()
					notifications = append(notifications, request)
					return merr.Success(), nil
				}
				core.proxyClientManager.GetProxyClients().Insert(TestProxyID, proxy)
				err := executeOperatePrivilegeTaskSteps(ctx, core, entity, tc.operation)
				require.Equal(t, 1, writes)
				var wantReads []string
				if tc.operation == milvuspb.OperatePrivilegeType_Revoke {
					wantReads = append(wantReads, "tenant_b") // Existing action-overlap check.
				}
				noActionDelta := tc.emptyGroup || tc.overlap
				if enabled && !noActionDelta {
					wantReads = append(wantReads, util.AnyWord)
				}
				require.Equal(t, wantReads, reads)
				mu.Lock()
				defer mu.Unlock()
				if enabled && tc.failProjection {
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
					require.Empty(t, notifications)
					return
				}
				require.NoError(t, err)
				if noActionDelta && !enabled {
					require.Empty(t, notifications)
					return
				}
				require.Len(t, notifications, 1)
				request := notifications[0]
				if noActionDelta {
					require.EqualValues(t, typeutil.CacheRefresh, request.GetOpType())
					return
				}
				wantOp := typeutil.CacheGrantPrivilege
				if tc.operation == milvuspb.OperatePrivilegeType_Revoke {
					wantOp = typeutil.CacheRevokePrivilege
				}
				require.EqualValues(t, wantOp, request.GetOpType())
				require.Equal(t, funcutil.PolicyForPrivilege("reader", "Collection", "orders", util.PrivilegeNameForMetastore("Search"), "tenant_b"), request.GetOpKey())
				if enabled {
					require.EqualValues(t, 1, request.GetCollectionVisibility().GetFormatVersion())
					require.Len(t, request.GetCollectionVisibility().GetRoles(), 1)
				} else {
					require.Nil(t, request.GetCollectionVisibility())
				}
			})
		}
	}
}
