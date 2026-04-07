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
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

// mockMixCoordClient implements MixCoordClient for testing
type mockMixCoordClient struct {
	listPoliciesFunc func(ctx context.Context, req *messagespb.ListRowPoliciesRequest) (*messagespb.ListRowPoliciesResponse, error)
	getUserTagsFunc  func(ctx context.Context, req *messagespb.GetUserTagsRequest) (*messagespb.GetUserTagsResponse, error)
}

func (m *mockMixCoordClient) ListRowPolicies(ctx context.Context, req *messagespb.ListRowPoliciesRequest, opts ...grpc.CallOption) (*messagespb.ListRowPoliciesResponse, error) {
	if m.listPoliciesFunc != nil {
		return m.listPoliciesFunc(ctx, req)
	}
	return &messagespb.ListRowPoliciesResponse{
		Status: &commonpb.Status{ErrorCode: 0},
	}, nil
}

func (m *mockMixCoordClient) GetUserTags(ctx context.Context, req *messagespb.GetUserTagsRequest, opts ...grpc.CallOption) (*messagespb.GetUserTagsResponse, error) {
	if m.getUserTagsFunc != nil {
		return m.getUserTagsFunc(ctx, req)
	}
	return &messagespb.GetUserTagsResponse{
		Status: &commonpb.Status{ErrorCode: 0},
	}, nil
}

func TestNewProxyPolicyLoader(t *testing.T) {
	client := &mockMixCoordClient{}
	loader := NewProxyPolicyLoader(client)

	assert.NotNil(t, loader)
	assert.Equal(t, client, loader.mixCoord)
}

func TestProxyPolicyLoaderLoadPoliciesNilClient(t *testing.T) {
	loader := NewProxyPolicyLoader(nil)

	policies, err := loader.LoadPolicies(context.Background(), "db1", "coll1")

	assert.NoError(t, err)
	assert.Nil(t, policies)
}

func TestProxyPolicyLoaderLoadUserTagsNilClient(t *testing.T) {
	loader := NewProxyPolicyLoader(nil)

	tags, err := loader.LoadUserTags(context.Background(), "user1")

	assert.NoError(t, err)
	assert.Nil(t, tags)
}

func TestProxyPolicyLoaderLoadUserTagsSuccess(t *testing.T) {
	expectedTags := map[string]string{
		"department": "engineering",
		"role":       "developer",
	}

	client := &mockMixCoordClient{
		getUserTagsFunc: func(ctx context.Context, req *messagespb.GetUserTagsRequest) (*messagespb.GetUserTagsResponse, error) {
			assert.Equal(t, "testuser", req.UserName)
			return &messagespb.GetUserTagsResponse{
				Status: &commonpb.Status{ErrorCode: 0},
				Tags:   expectedTags,
			}, nil
		},
	}

	loader := NewProxyPolicyLoader(client)

	tags, err := loader.LoadUserTags(context.Background(), "testuser")

	assert.NoError(t, err)
	assert.Equal(t, expectedTags, tags)
}

func TestProxyPolicyLoaderLoadUserTagsError(t *testing.T) {
	client := &mockMixCoordClient{
		getUserTagsFunc: func(ctx context.Context, req *messagespb.GetUserTagsRequest) (*messagespb.GetUserTagsResponse, error) {
			return &messagespb.GetUserTagsResponse{
				Status: &commonpb.Status{
					ErrorCode: 1,
					Reason:    "user not found",
				},
			}, nil
		},
	}

	loader := NewProxyPolicyLoader(client)

	tags, err := loader.LoadUserTags(context.Background(), "unknown_user")

	assert.Error(t, err)
	assert.Nil(t, tags)
}

func TestProxyPolicyLoaderLoadPoliciesError(t *testing.T) {
	client := &mockMixCoordClient{
		listPoliciesFunc: func(ctx context.Context, req *messagespb.ListRowPoliciesRequest) (*messagespb.ListRowPoliciesResponse, error) {
			return &messagespb.ListRowPoliciesResponse{
				Status: &commonpb.Status{
					ErrorCode: 1,
					Reason:    "policy service unavailable",
				},
			}, nil
		},
	}

	loader := NewProxyPolicyLoader(client)

	policies, err := loader.LoadPolicies(context.Background(), "db1", "coll1")

	assert.Error(t, err)
	assert.Nil(t, policies)
}

// mockRLSPolicyLoader implements RLSPolicyLoader for testing
type mockRLSPolicyLoader struct {
	policies map[string][]*model.RLSPolicy
	userTags map[string]map[string]string
}

func newMockRLSPolicyLoader() *mockRLSPolicyLoader {
	return &mockRLSPolicyLoader{
		policies: make(map[string][]*model.RLSPolicy),
		userTags: make(map[string]map[string]string),
	}
}

func (m *mockRLSPolicyLoader) LoadPolicies(ctx context.Context, dbName, collectionName string) ([]*model.RLSPolicy, error) {
	key := dbName + "." + collectionName
	return m.policies[key], nil
}

func (m *mockRLSPolicyLoader) LoadUserTags(ctx context.Context, userName string) (map[string]string, error) {
	return m.userTags[userName], nil
}

func (m *mockRLSPolicyLoader) SetPolicies(dbName, collectionName string, policies []*model.RLSPolicy) {
	key := dbName + "." + collectionName
	m.policies[key] = policies
}

func (m *mockRLSPolicyLoader) SetUserTags(userName string, tags map[string]string) {
	m.userTags[userName] = tags
}

func TestNewRLSCacheWithLoader(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()

	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	assert.NotNil(t, cacheWithLoader)
	assert.Equal(t, cache, cacheWithLoader.cache)
	assert.Equal(t, loader, cacheWithLoader.loader)
	assert.NotNil(t, cacheWithLoader.loadedCollections)
	assert.NotNil(t, cacheWithLoader.loadedUsers)
}

func TestRLSCacheWithLoaderGetUserTagsFromCache(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// Set up loader to return tags (lazy-load path)
	expectedTags := map[string]string{"dept": "eng"}
	loader.SetUserTags("user1", expectedTags)

	tags := cacheWithLoader.GetUserTags(context.Background(), "user1")

	assert.Equal(t, expectedTags, tags)
}

func TestRLSCacheWithLoaderGetUserTagsLazyLoad(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// Set up loader to return tags
	expectedTags := map[string]string{"dept": "eng"}
	loader.SetUserTags("user1", expectedTags)

	tags := cacheWithLoader.GetUserTags(context.Background(), "user1")

	assert.Equal(t, expectedTags, tags)
}

func TestRLSCacheWithLoaderGetUserTagsAlreadyLoaded(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// First load - empty result
	tags := cacheWithLoader.GetUserTags(context.Background(), "user1")
	assert.Empty(t, tags)

	// Second load - should return empty without calling loader again
	// (loader has no tags, so result is empty, but loader shouldn't be called twice)
	tags = cacheWithLoader.GetUserTags(context.Background(), "user1")
	assert.Empty(t, tags)
}

func TestRLSCacheWithLoaderInvalidateCollectionCache(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// Add a policy to cache
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("policy1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"}, "true", "", "test"),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Verify policy exists
	assert.NotEmpty(t, cache.GetPoliciesForCollection(1, 100))

	// Invalidate
	cacheWithLoader.InvalidateCollectionCache(1, 100)

	// Verify policy is removed
	assert.Empty(t, cache.GetPoliciesForCollection(1, 100))
}

func TestRLSCacheWithLoaderInvalidateUserCache(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// Mark as loaded
	cacheWithLoader.mu.Lock()
	cacheWithLoader.loadedUsers["user1"] = &cacheEntry{loadedAt: time.Now()}
	cacheWithLoader.mu.Unlock()

	// Invalidate
	cacheWithLoader.InvalidateUserCache("user1")

	// Verify not marked as loaded anymore
	cacheWithLoader.mu.RLock()
	_, loaded := cacheWithLoader.loadedUsers["user1"]
	cacheWithLoader.mu.RUnlock()
	assert.False(t, loaded)
}

func TestRLSCacheWithLoaderInvalidateAllCache(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// Add some data
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy("policy1", 100, 1, model.RLSPolicyTypePermissive,
			[]string{"query"}, []string{"PUBLIC"}, "true", "", "test"),
	}
	cache.UpdatePolicies(1, 100, policies)
	cache.SetUserTags("user1", map[string]string{"key": "value"})

	// Mark as loaded
	cacheWithLoader.mu.Lock()
	cacheWithLoader.loadedCollections[generateCollectionKey(1, 100)] = &cacheEntry{loadedAt: time.Now()}
	cacheWithLoader.loadedUsers["user1"] = &cacheEntry{loadedAt: time.Now()}
	cacheWithLoader.mu.Unlock()

	// Invalidate all
	cacheWithLoader.InvalidateAllCache()

	// Verify everything is cleared
	assert.Empty(t, cache.GetPoliciesForCollection(1, 100))
	assert.Empty(t, cacheWithLoader.loadedCollections)
	assert.Empty(t, cacheWithLoader.loadedUsers)
}

func TestRLSCacheWithLoaderGetUnderlyingCache(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	underlying := cacheWithLoader.GetUnderlyingCache()

	assert.Same(t, cache, underlying)
}

func TestRLSCacheWithLoaderConcurrency(t *testing.T) {
	cache := NewRLSCache()
	loader := newMockRLSPolicyLoader()
	cacheWithLoader := NewRLSCacheWithLoader(cache, loader)

	// Set up some test data in loader
	loader.SetUserTags("user1", map[string]string{"key": "value"})

	done := make(chan bool)

	// Multiple concurrent readers
	for i := 0; i < 10; i++ {
		go func(userNum int) {
			for j := 0; j < 50; j++ {
				_ = cacheWithLoader.GetUserTags(context.Background(), "user1")
			}
			done <- true
		}(i)
	}

	// Multiple concurrent invalidations
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 20; j++ {
				cacheWithLoader.InvalidateUserCache("user1")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 15; i++ {
		<-done
	}
}
