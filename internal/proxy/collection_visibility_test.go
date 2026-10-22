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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDescribeCollectionLocalVisibility(t *testing.T) {
	for key, value := range map[string]string{
		Params.CommonCfg.AuthorizationEnabled.Key:           "true",
		Params.CommonCfg.RootShouldBindRole.Key:             "false",
		Params.ProxyCfg.EnableCachedServiceProvider.Key:     "true",
		Params.ProxyCfg.EnableLocalCollectionVisibility.Key: "true",
	} {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}
	t.Cleanup(privilege.ResetPrivilegeCacheForTest)
	ctx := NewContextWithMetadata(context.Background(), "alice", "tenant_a")
	projection := &internalpb.CollectionVisibilitySnapshot{FormatVersion: 1, Roles: []*internalpb.RoleCollectionVisibility{{
		Role: "reader", Rules: []*internalpb.CollectionVisibilityRule{{DbName: "tenant_b", CollectionName: "orders"}},
	}}}
	mix := mocks.NewMockMixCoordClient(t)
	mix.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status: merr.Success(), CollectionVisibility: projection,
		UserRoles: []string{funcutil.EncodeUserRoleCache("alice", "reader")},
	}, nil).Once()
	require.NoError(t, privilege.InitPrivilegeCache(ctx, mix))
	collection := &collectionInfo{CollID: 77, DBName: "tenant_b", Schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Name: "orders"})}
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionInfo(mock.Anything, "tenant_a", "", int64(77)).Return(collection, nil)
	node := &Proxy{mixCoord: mix, metaCache: cache}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	req := &milvuspb.DescribeCollectionRequest{DbName: "tenant_a", CollectionID: 77}
	for i := 0; i < 3; i++ {
		resp, err := node.DescribeCollection(ctx, req)
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Equal(t, "tenant_b", resp.GetDbName())
	}
	// No DescribeCollection RPC is configured: hot metadata authorization must
	// succeed locally, and a subsequent revoke must deny on that same metadata.
	_, err := node.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheRevokePrivilege),
		OpKey:  funcutil.PolicyForPrivilege("reader", "Collection", "orders", "Query", "tenant_b"),
		CollectionVisibility: &internalpb.CollectionVisibilitySnapshot{FormatVersion: 1,
			Roles: []*internalpb.RoleCollectionVisibility{{Role: "reader"}}},
	})
	require.NoError(t, err)
	resp, err := node.DescribeCollection(ctx, req)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
	require.Nil(t, resp.GetSchema())
	require.Zero(t, resp.GetCollectionID())

	// Disable the experimental path: retain the existing authoritative check.
	require.NoError(t, Params.Save(Params.ProxyCfg.EnableLocalCollectionVisibility.Key, "false"))
	mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(
		&milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.ErrPrivilegeNotPermitted)}, nil).Once()
	resp, err = node.DescribeCollection(ctx, req)
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrPrivilegeNotPermitted)
}
