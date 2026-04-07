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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestRLSListRowPolicies_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.ListRowPolicies(context.Background(), &messagespb.ListRowPoliciesRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp.GetStatus())
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
}

func TestRLSListRowPolicies_Success(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, "default", "test_collection", mock.Anything).
		Return(&model.Collection{DBID: 1, CollectionID: 100}, nil)
	meta.EXPECT().ListRLSPolicies(mock.Anything, int64(1), int64(100)).
		Return([]*model.RLSPolicy{
			{
				PolicyName:   "policy1",
				CollectionID: 100,
				PolicyType:   0,
				Actions:      []string{"query"},
				Roles:        []string{"PUBLIC"},
				UsingExpr:    "tenant_id == $current_user_tags[tenant]",
				Description:  "test policy",
			},
		}, nil)

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.ListRowPolicies(context.Background(), &messagespb.ListRowPoliciesRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.Len(t, resp.GetPolicies(), 1)
	assert.Equal(t, "policy1", resp.GetPolicies()[0].GetPolicyName())
	assert.Equal(t, messagespb.RLSPolicyType_PERMISSIVE, resp.GetPolicies()[0].GetPolicyType())
}

func TestRLSListRowPolicies_CollectionNotFound(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, "default", "nonexistent", mock.Anything).
		Return(nil, errors.New("collection not found"))

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.ListRowPolicies(context.Background(), &messagespb.ListRowPoliciesRequest{
		DbName:         "default",
		CollectionName: "nonexistent",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
}

func TestRLSGetUserTags_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.GetUserTags(context.Background(), &messagespb.GetUserTagsRequest{
		UserName: "testuser",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
}

func TestRLSGetUserTags_Success(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetUserTags(mock.Anything, "testuser").
		Return(&model.RLSUserTags{
			UserName: "testuser",
			Tags:     map[string]string{"tenant": "acme", "role": "admin"},
		}, nil)

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.GetUserTags(context.Background(), &messagespb.GetUserTagsRequest{
		UserName: "testuser",
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.Equal(t, "acme", resp.GetTags()["tenant"])
	assert.Equal(t, "admin", resp.GetTags()["role"])
}

func TestRLSGetUserTags_NotFound(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetUserTags(mock.Anything, "newuser").
		Return(nil, merr.ErrIoKeyNotFound)

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.GetUserTags(context.Background(), &messagespb.GetUserTagsRequest{
		UserName: "newuser",
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.NotNil(t, resp.GetTags())
	assert.Empty(t, resp.GetTags())
}

func TestRLSGetUserTags_Error(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetUserTags(mock.Anything, "testuser").
		Return(nil, errors.New("etcd connection failed"))

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.GetUserTags(context.Background(), &messagespb.GetUserTagsRequest{
		UserName: "testuser",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
}

func TestRLSListUsersWithTag_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.ListUsersWithTag(context.Background(), &messagespb.ListUsersWithTagRequest{
		TagKey:   "tenant",
		TagValue: "acme",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
}

func TestRLSListUsersWithTag_Success(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().ListUsersWithTag(mock.Anything, "tenant", "acme").
		Return([]string{"user1", "user2"}, nil)

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.ListUsersWithTag(context.Background(), &messagespb.ListUsersWithTagRequest{
		TagKey:   "tenant",
		TagValue: "acme",
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.GetStatus().GetCode())
	assert.Equal(t, []string{"user1", "user2"}, resp.GetUsers())
}

func TestRLSListUsersWithTag_Error(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().ListUsersWithTag(mock.Anything, "tenant", "acme").
		Return(nil, errors.New("storage error"))

	c := newTestCore(withHealthyCode(), withMeta(meta))
	resp, err := c.ListUsersWithTag(context.Background(), &messagespb.ListUsersWithTagRequest{
		TagKey:   "tenant",
		TagValue: "acme",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetStatus().GetCode())
}

func TestRLSCreateRowPolicy_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.CreateRowPolicy(context.Background(), &messagespb.CreateRowPolicyRequest{
		DbName: "default",
		Policy: &messagespb.RLSPolicy{
			PolicyName:     "test_policy",
			CollectionName: "test_collection",
		},
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetCode())
}

func TestRLSCreateRowPolicy_NilPolicy(t *testing.T) {
	c := newTestCore(withHealthyCode())
	resp, err := c.CreateRowPolicy(context.Background(), &messagespb.CreateRowPolicyRequest{
		DbName: "default",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetCode())
}

func TestRLSDropRowPolicy_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.DropRowPolicy(context.Background(), &messagespb.DropRowPolicyRequest{
		DbName:         "default",
		CollectionName: "test_collection",
		PolicyName:     "test_policy",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetCode())
}

func TestRLSSetUserTags_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.SetUserTags(context.Background(), &messagespb.SetUserTagsRequest{
		UserName: "testuser",
		Tags:     map[string]string{"tenant": "acme"},
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetCode())
}

func TestRLSDeleteUserTag_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.DeleteUserTag(context.Background(), &messagespb.DeleteUserTagRequest{
		UserName: "testuser",
		TagKey:   "tenant",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetCode())
}

func TestRLSRefreshRLSCache_Unhealthy(t *testing.T) {
	c := newTestCore(withAbnormalCode())
	resp, err := c.RefreshRLSCache(context.Background(), &messagespb.RefreshRLSCacheRequest{
		OpType: messagespb.RLSCacheOpType_CreatePolicy,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.GetCode())
}

func TestRLSBroadcastRLSCacheRefresh_NilManager(t *testing.T) {
	c := newTestCore(withHealthyCode())
	c.proxyClientManager = nil
	err := c.broadcastRLSCacheRefresh(context.Background(), &messagespb.RefreshRLSCacheRequest{
		OpType: messagespb.RLSCacheOpType_CreatePolicy,
	})
	assert.NoError(t, err)
}

func TestRLSBroadcastRLSCacheRefresh_NilRequest(t *testing.T) {
	c := newTestCore(withHealthyCode())
	err := c.broadcastRLSCacheRefresh(context.Background(), nil)
	assert.NoError(t, err)
}
