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

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/stretchr/testify/assert"
)

func TestRLSCacheRefreshHandlerCreatePolicy(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)

	// Setup initial cache state
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"test policy",
		),
	}
	cache.UpdatePolicies(456, 123, policies)

	// Verify policy is cached
	retrieved := cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 1)

	// Handle cache refresh for new policy creation
	req := &messagespb.RefreshRLSCacheRequest{
		OpType:         messagespb.RLSCacheOpType_CreatePolicy,
		CollectionName: "test_collection",
		PolicyName:     "policy2",
	}

	err := handler.HandleCacheRefresh(context.Background(), req)
	assert.NoError(t, err)

	// Cache should be invalidated
	retrieved = cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 0)
}

func TestRLSCacheRefreshHandlerDropPolicy(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)

	// Setup cache
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"test policy",
		),
	}
	cache.UpdatePolicies(456, 123, policies)

	// Handle cache refresh for policy drop
	req := &messagespb.RefreshRLSCacheRequest{
		OpType:         messagespb.RLSCacheOpType_DropPolicy,
		CollectionName: "test_collection",
		PolicyName:     "policy1",
	}

	err := handler.HandleCacheRefresh(context.Background(), req)
	assert.NoError(t, err)

	// Cache should be cleared
	retrieved := cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 0)
}

func TestRLSCacheRefreshHandlerUpdateUserTags(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)

	// Setup cache
	tags := map[string]string{"department": "engineering"}
	cache.UpdateUserTags("alice", tags)

	// Verify tags are cached
	retrieved := cache.GetUserTags("alice")
	assert.Equal(t, "engineering", retrieved["department"])

	// Handle cache refresh for user tags update
	req := &messagespb.RefreshRLSCacheRequest{
		OpType:   messagespb.RLSCacheOpType_UpdateUserTags,
		UserName: "alice",
	}

	err := handler.HandleCacheRefresh(context.Background(), req)
	assert.NoError(t, err)

	// Cache should be cleared
	retrieved = cache.GetUserTags("alice")
	assert.Empty(t, retrieved)
}

func TestRLSCacheRefreshHandlerDeleteUserTag(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)

	// Setup cache
	cache.UpdateUserTag("alice", "department", "engineering")
	cache.UpdateUserTag("alice", "region", "us-west")

	// Verify tags are cached
	retrieved := cache.GetUserTags("alice")
	assert.Len(t, retrieved, 2)

	// Handle cache refresh for user tag deletion
	req := &messagespb.RefreshRLSCacheRequest{
		OpType:   messagespb.RLSCacheOpType_DeleteUserTag,
		UserName: "alice",
	}

	err := handler.HandleCacheRefresh(context.Background(), req)
	assert.NoError(t, err)

	// Cache should be cleared
	retrieved = cache.GetUserTags("alice")
	assert.Empty(t, retrieved)
}

func TestRLSCacheRefreshHandlerUpdateCollectionConfig(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)

	// Setup cache
	cache.UpdateCollectionConfig(456, 123, true, false)

	// Verify config is cached
	config := cache.GetCollectionConfig(456, 123)
	assert.NotNil(t, config)
	assert.True(t, config.Enabled)

	// Handle cache refresh for collection config update
	req := &messagespb.RefreshRLSCacheRequest{
		OpType:         messagespb.RLSCacheOpType_UpdateCollectionConfig,
		CollectionName: "test_collection",
	}

	err := handler.HandleCacheRefresh(context.Background(), req)
	assert.NoError(t, err)

	// Cache should be cleared
	config = cache.GetCollectionConfig(456, 123)
	assert.Nil(t, config)
}

func TestRLSCacheRefreshHandlerNilRequest(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)

	// Handle nil request should not panic
	err := handler.HandleCacheRefresh(context.Background(), nil)
	assert.NoError(t, err)
}

func TestRLSCacheRefreshHandlerNilCache(t *testing.T) {
	handler := NewRLSCacheRefreshHandler(nil)

	req := &messagespb.RefreshRLSCacheRequest{
		OpType:         messagespb.RLSCacheOpType_CreatePolicy,
		CollectionName: "test_collection",
	}

	// Handle request with nil cache should not panic
	err := handler.HandleCacheRefresh(context.Background(), req)
	assert.NoError(t, err)
}

func TestCacheRefreshInterceptor(t *testing.T) {
	cache := NewRLSCache()
	handler := NewRLSCacheRefreshHandler(cache)
	interceptor := NewCacheRefreshInterceptor(handler)

	// Setup cache
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"test policy",
		),
	}
	cache.UpdatePolicies(456, 123, policies)

	// Use interceptor to handle refresh
	req := &messagespb.RefreshRLSCacheRequest{
		OpType:         messagespb.RLSCacheOpType_CreatePolicy,
		CollectionName: "test_collection",
		PolicyName:     "policy2",
	}

	err := interceptor.OnCacheRefresh(context.Background(), req)
	assert.NoError(t, err)

	// Cache should be invalidated
	retrieved := cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 0)
}
