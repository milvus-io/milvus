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

package privilege

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type visibilityCoordinator struct {
	types.MixCoordClient
	response *internalpb.ListPolicyResponse
}

func (c *visibilityCoordinator) ListPolicy(context.Context, *internalpb.ListPolicyRequest, ...grpc.CallOption) (*internalpb.ListPolicyResponse, error) {
	return c.response, nil
}

func TestCollectionVisibilityCacheLifecycle(t *testing.T) {
	ctx := context.Background()
	projection := &internalpb.CollectionVisibilitySnapshot{FormatVersion: 1, Roles: []*internalpb.RoleCollectionVisibility{{
		Role: "reader", Rules: []*internalpb.CollectionVisibilityRule{{DbName: "tenant_b", CollectionName: "orders"}},
	}}}
	coordinator := &visibilityCoordinator{response: &internalpb.ListPolicyResponse{
		Status: merr.Success(), CollectionVisibility: projection,
		UserRoles: []string{funcutil.EncodeUserRoleCache("alice", "reader")},
	}}
	require.NoError(t, InitPrivilegeCache(ctx, coordinator))
	t.Cleanup(ResetPrivilegeCacheForTest)
	cache := GetPrivilegeCache()
	check := func(want, known bool) {
		visible, supported := cache.CanSeeCollection("alice", "tenant_b", "orders")
		require.Equal(t, known, supported)
		require.Equal(t, want, visible)
	}
	check(true, true)
	visible, known := cache.CanSeeCollection("alice", "tenant_a", "orders")
	require.True(t, known)
	require.False(t, visible)

	// Group action expansion can change without changing raw visibility.
	policy := funcutil.PolicyForPrivilege("reader", "Collection", "orders", "Query", "tenant_b")
	op := typeutil.CacheOp{OpType: typeutil.CacheGrantPrivilege, OpKey: policy}
	require.NoError(t, cache.RefreshPolicyInfoWithVisibility(op, &internalpb.CollectionVisibilitySnapshot{FormatVersion: 1}))
	check(true, true)
	require.NoError(t, cache.RefreshPolicyInfoWithVisibility(
		typeutil.CacheOp{OpType: typeutil.CacheRevokePrivilege, OpKey: policy},
		&internalpb.CollectionVisibilitySnapshot{FormatVersion: 1, Roles: []*internalpb.RoleCollectionVisibility{{Role: "reader"}}}))
	check(false, true)

	require.NoError(t, cache.RefreshPolicyInfoWithVisibility(op, projection))
	check(true, true)
	require.NoError(t, cache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRemoveUserFromRole, OpKey: funcutil.EncodeUserRoleCache("alice", "reader")}))
	check(false, true)
	require.NoError(t, cache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheAddUserToRole, OpKey: funcutil.EncodeUserRoleCache("alice", "reader")}))
	check(true, true)

	// Legacy action deltas cannot safely update raw visibility. A partial new
	// delta must not upgrade an incomplete projection into a complete snapshot.
	require.NoError(t, cache.RefreshPolicyInfo(op))
	check(false, false)
	require.NoError(t, cache.RefreshPolicyInfoWithVisibility(op, projection))
	check(false, false)
	require.NoError(t, cache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh}))
	check(true, true)

	coordinator.response.CollectionVisibility = &internalpb.CollectionVisibilitySnapshot{FormatVersion: 1}
	require.NoError(t, cache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh}))
	check(false, true)
	coordinator.response.CollectionVisibility = nil
	require.NoError(t, cache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh}))
	check(false, false)
	// Enabling RC projection production after an unsupported full snapshot
	// requires a full refresh; the next role delta alone is insufficient.
	coordinator.response.CollectionVisibility = projection
	require.NoError(t, cache.RefreshPolicyInfoWithVisibility(op, projection))
	check(false, false)
	require.NoError(t, cache.RefreshPolicyInfo(typeutil.CacheOp{OpType: typeutil.CacheRefresh}))
	check(true, true)
}
