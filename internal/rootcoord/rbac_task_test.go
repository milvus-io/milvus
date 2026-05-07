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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExecuteOperatePrivilegeGroupTaskSteps_PersistFailSkipsCache(t *testing.T) {
	ctx := context.Background()
	events := make([]string, 0, 2)

	meta := newMockMetaTable()
	meta.ListPrivilegeGroupsFunc = func(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
		return []*milvuspb.PrivilegeGroupInfo{{
			GroupName:  "group1",
			Privileges: []*milvuspb.PrivilegeEntity{{Name: "Query"}},
		}}, nil
	}
	meta.GetPrivilegeGroupRolesFunc = func(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error) {
		return []*milvuspb.RoleEntity{{Name: "role1"}}, nil
	}
	meta.SelectGrantFunc = func(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
		return []*milvuspb.GrantEntity{{
			Role:       &milvuspb.RoleEntity{Name: "role1"},
			Object:     &milvuspb.ObjectEntity{Name: "Collection"},
			ObjectName: util.AnyWord,
			DbName:     util.AnyWord,
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "group1"},
			},
		}}, nil
	}
	meta.OperatePrivilegeGroupFunc = func(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
		events = append(events, "persist")
		return errors.New("persist failed")
	}

	core := newTestCore(withMeta(meta), withValidProxyManager())
	proxyClient, ok := core.proxyClientManager.GetProxyClients().Get(TestProxyID)
	require.True(t, ok)

	proxy, ok := proxyClient.(*mockProxy)
	require.True(t, ok)
	proxy.RefreshPolicyInfoCacheFunc = func(ctx context.Context, request *proxypb.RefreshPolicyInfoCacheRequest) (*commonpb.Status, error) {
		events = append(events, "cache")
		return merr.Success(), nil
	}

	err := executeOperatePrivilegeGroupTaskSteps(ctx, core, &milvuspb.PrivilegeGroupInfo{
		GroupName:  "group1",
		Privileges: []*milvuspb.PrivilegeEntity{{Name: "Query"}},
	}, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
	require.Error(t, err)
	require.Equal(t, []string{"persist"}, events)
}
