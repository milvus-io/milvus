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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/rbacutil"
)

func TestCollectionVisibilityRoleReplacement(t *testing.T) {
	meta := newMockMetaTable()
	grants := []*milvuspb.GrantEntity{{
		Role:   &milvuspb.RoleEntity{Name: "reader"},
		Object: &milvuspb.ObjectEntity{Name: "Global"}, ObjectName: "orders", DbName: "tenant_b",
		Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "EmptyGroup"}},
	}}
	meta.SelectGrantFunc = func(_ context.Context, _ string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
		require.Equal(t, "reader", entity.GetRole().GetName())
		require.Equal(t, util.AnyWord, entity.GetDbName(), "replace all databases of the role")
		return grants, nil
	}
	core := newTestCore(withMeta(meta))
	groups := []*milvuspb.PrivilegeGroupInfo{{GroupName: "EmptyGroup"}}
	snapshot, err := core.collectionVisibilityForRole(context.Background(), &milvuspb.RoleEntity{Name: "reader"}, groups)
	require.NoError(t, err)
	index := make(rbacutil.CollectionVisibilityIndex)
	index.Replace(snapshot.GetRoles())
	require.True(t, index.CanSee([]string{"reader"}, "tenant_b", "orders"), "empty groups remain visible")
	grants = nil
	snapshot, err = core.collectionVisibilityForRole(context.Background(), &milvuspb.RoleEntity{Name: "reader"}, groups)
	require.NoError(t, err)
	require.Len(t, snapshot.GetRoles(), 1)
	require.Equal(t, "reader", snapshot.GetRoles()[0].GetRole())
	index.Replace(snapshot.GetRoles())
	require.False(t, index.CanSee([]string{"reader"}, "tenant_b", "orders"))
}

func TestListPolicyLocalVisibilityGate(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run("enabled="+strconv.FormatBool(enabled), func(t *testing.T) {
			require.NoError(t, Params.Save(Params.ProxyCfg.EnableLocalCollectionVisibility.Key, strconv.FormatBool(enabled)))
			t.Cleanup(func() { Params.Reset(Params.ProxyCfg.EnableLocalCollectionVisibility.Key) })
			meta := newMockMetaTable()
			meta.ListPolicyFunc = func(context.Context, string) ([]*milvuspb.GrantEntity, error) {
				return []*milvuspb.GrantEntity{{
					Role: &milvuspb.RoleEntity{Name: "reader"}, Object: &milvuspb.ObjectEntity{Name: "Collection"},
					DbName: "tenant_b", ObjectName: "orders",
					Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "Search"}},
				}}, nil
			}
			meta.ListPrivilegeGroupsFunc = func(context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) { return nil, nil }
			meta.ListUserRoleFunc = func(context.Context, string) ([]string, error) { return nil, nil }
			core := newTestCore(withMeta(meta), withHealthyCode())
			response, err := core.ListPolicy(context.Background(), &internalpb.ListPolicyRequest{})
			require.NoError(t, err)
			require.NoError(t, merr.Error(response.GetStatus()))
			require.Len(t, response.GetPolicyInfos(), 1)
			if !enabled {
				require.Nil(t, response.GetCollectionVisibility())
				return
			}
			require.EqualValues(t, 1, response.GetCollectionVisibility().GetFormatVersion())
			index := make(rbacutil.CollectionVisibilityIndex)
			index.Replace(response.GetCollectionVisibility().GetRoles())
			require.True(t, index.CanSee([]string{"reader"}, "tenant_b", "orders"))
		})
	}
}
