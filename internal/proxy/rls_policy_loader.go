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
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// RLSPolicyLoader defines the interface for loading RLS policies
type RLSPolicyLoader interface {
	// LoadPolicies loads policies for a collection from the source of truth
	LoadPolicies(ctx context.Context, dbName, collectionName string) ([]*model.RLSPolicy, error)
	// LoadUserTags loads tags for a user from the source of truth
	LoadUserTags(ctx context.Context, userName string) (map[string]string, error)
}

// MixCoordClient defines the minimal interface needed to load RLS data
// This matches the gRPC client interface from rootcoordpb.RootCoordClient
type MixCoordClient interface {
	ListRowPolicies(ctx context.Context, req *messagespb.ListRowPoliciesRequest, opts ...grpc.CallOption) (*messagespb.ListRowPoliciesResponse, error)
	GetUserTags(ctx context.Context, req *messagespb.GetUserTagsRequest, opts ...grpc.CallOption) (*messagespb.GetUserTagsResponse, error)
}

// ProxyPolicyLoader implements RLSPolicyLoader using mixCoord
type ProxyPolicyLoader struct {
	mixCoord MixCoordClient
}

// NewProxyPolicyLoader creates a new policy loader
func NewProxyPolicyLoader(mixCoord MixCoordClient) *ProxyPolicyLoader {
	return &ProxyPolicyLoader{
		mixCoord: mixCoord,
	}
}

// LoadPolicies loads policies for a collection from rootcoord
func (l *ProxyPolicyLoader) LoadPolicies(ctx context.Context, dbName, collectionName string) ([]*model.RLSPolicy, error) {
	if l.mixCoord == nil {
		return nil, nil
	}

	req := &messagespb.ListRowPoliciesRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}

	resp, err := l.mixCoord.ListRowPolicies(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, merr.WrapErrServiceInternal("ListRowPolicies returned nil response")
	}
	if resp.GetStatus() == nil {
		return nil, merr.WrapErrServiceInternal("ListRowPolicies returned nil status")
	}

	if statusErr := merr.Error(resp.GetStatus()); statusErr != nil {
		log.Ctx(ctx).Warn("ListRowPolicies returned error",
			zap.Error(statusErr))
		return nil, statusErr
	}

	// Get collection ID for the policies
	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get collection ID for policy loader",
			zap.String("dbName", dbName),
			zap.String("collectionName", collectionName),
			zap.Error(err))
		return nil, err
	}

	dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, dbName)
	if err != nil {
		log.Ctx(ctx).Warn("failed to get database info for policy loader",
			zap.String("dbName", dbName),
			zap.Error(err))
		return nil, err
	}

	// Convert proto policies to model policies
	policies := make([]*model.RLSPolicy, 0, len(resp.GetPolicies()))
	for _, proto := range resp.GetPolicies() {
		policy := convertProtoToModelRLSPolicy(proto, collectionID, dbInfo.dbID)
		if policy != nil {
			policies = append(policies, policy)
		}
	}

	return policies, nil
}

// LoadUserTags loads tags for a user from rootcoord
func (l *ProxyPolicyLoader) LoadUserTags(ctx context.Context, userName string) (map[string]string, error) {
	if l.mixCoord == nil {
		return nil, nil
	}

	req := &messagespb.GetUserTagsRequest{
		UserName: userName,
	}

	resp, err := l.mixCoord.GetUserTags(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, merr.WrapErrServiceInternal("GetUserTags returned nil response")
	}
	if resp.GetStatus() == nil {
		return nil, merr.WrapErrServiceInternal("GetUserTags returned nil status")
	}

	if statusErr := merr.Error(resp.GetStatus()); statusErr != nil {
		log.Ctx(ctx).Warn("GetUserTags returned error",
			zap.Error(statusErr))
		return nil, statusErr
	}

	return resp.GetTags(), nil
}

// cacheEntry tracks when a cache entry was loaded for TTL expiration
type cacheEntry struct {
	loadedAt time.Time
	loading  bool // true while a goroutine is actively loading this entry
}

// RLSCacheWithLoader wraps RLSCache with lazy loading and TTL-based expiration
type RLSCacheWithLoader struct {
	cache  *RLSCache
	loader RLSPolicyLoader
	mu     sync.RWMutex

	// Track when collections/users were loaded for TTL expiration
	loadedCollections map[string]*cacheEntry
	loadedUsers       map[string]*cacheEntry
}

// NewRLSCacheWithLoader creates a cache with lazy loading
func NewRLSCacheWithLoader(cache *RLSCache, loader RLSPolicyLoader) *RLSCacheWithLoader {
	return &RLSCacheWithLoader{
		cache:             cache,
		loader:            loader,
		loadedCollections: make(map[string]*cacheEntry),
		loadedUsers:       make(map[string]*cacheEntry),
	}
}

// getCacheTTL returns the cache TTL from config
func getCacheTTL() time.Duration {
	config := GetRLSConfig()
	if config != nil {
		return config.GetCacheExpirationDuration()
	}
	return time.Hour
}

// isExpired checks if a cache entry has expired
func (e *cacheEntry) isExpired(ttl time.Duration) bool {
	return time.Since(e.loadedAt) > ttl
}

// GetPoliciesForCollection returns policies, loading on-demand if needed.
// Uses double-checked locking to prevent thundering herd when multiple goroutines
// hit a cache miss simultaneously.
func (c *RLSCacheWithLoader) GetPoliciesForCollection(ctx context.Context, dbID int64, collectionID int64, dbName, collectionName string) []*CompiledRLSPolicy {
	key := generateCollectionKey(dbID, collectionID)
	ttl := getCacheTTL()

	// Fast path: check if we have a valid (non-expired) cache entry under read lock
	c.mu.RLock()
	entry := c.loadedCollections[key]
	c.mu.RUnlock()

	if entry != nil && !entry.isExpired(ttl) {
		policies := c.cache.GetPoliciesForCollection(dbID, collectionID)
		GetRLSMetrics().RecordCacheHit()
		return policies
	}

	// Slow path: acquire write lock and re-check to prevent thundering herd
	c.mu.Lock()
	entry = c.loadedCollections[key]
	if entry != nil && !entry.isExpired(ttl) && !entry.loading {
		// Another goroutine loaded while we waited for the lock
		c.mu.Unlock()
		GetRLSMetrics().RecordCacheHit()
		return c.cache.GetPoliciesForCollection(dbID, collectionID)
	}
	if entry != nil && entry.loading {
		// Another goroutine is currently loading — return stale/empty cache data
		// rather than starting a duplicate load
		c.mu.Unlock()
		return c.cache.GetPoliciesForCollection(dbID, collectionID)
	}
	// Mark as loading to prevent other goroutines from also loading
	c.loadedCollections[key] = &cacheEntry{loading: true}
	c.mu.Unlock()

	GetRLSMetrics().RecordCacheMiss()

	if c.loader != nil && dbName != "" && collectionName != "" {
		modelPolicies, err := c.loader.LoadPolicies(ctx, dbName, collectionName)
		if err != nil {
			log.Ctx(ctx).Warn("failed to load policies on-demand",
				zap.String("dbName", dbName),
				zap.String("collectionName", collectionName),
				zap.Error(err))
			GetRLSMetrics().RecordPolicyLoadFailure("on_demand")

			// On failure, clear the entry so next request retries
			c.mu.Lock()
			delete(c.loadedCollections, key)
			c.mu.Unlock()

			// If we have stale data, return it rather than nothing
			if entry != nil {
				return c.cache.GetPoliciesForCollection(dbID, collectionID)
			}
			return nil
		}

		if len(modelPolicies) > 0 {
			c.cache.UpdatePolicies(dbID, collectionID, modelPolicies)
		} else {
			c.cache.UpdatePolicies(dbID, collectionID, nil)
		}
		GetRLSMetrics().SetActivePolicies(dbName, collectionName, len(modelPolicies))
	}

	// Mark as loaded with current time (after data is populated)
	c.mu.Lock()
	c.loadedCollections[key] = &cacheEntry{loadedAt: time.Now()}
	c.mu.Unlock()

	return c.cache.GetPoliciesForCollection(dbID, collectionID)
}

// GetUserTags returns user tags, loading on-demand if needed.
// Uses double-checked locking to prevent thundering herd.
func (c *RLSCacheWithLoader) GetUserTags(ctx context.Context, userName string) map[string]string {
	ttl := getCacheTTL()

	// Fast path: check under read lock
	c.mu.RLock()
	entry := c.loadedUsers[userName]
	c.mu.RUnlock()

	if entry != nil && !entry.isExpired(ttl) {
		return c.cache.GetUserTags(userName)
	}

	// Slow path: acquire write lock and re-check
	c.mu.Lock()
	entry = c.loadedUsers[userName]
	if entry != nil && !entry.isExpired(ttl) && !entry.loading {
		c.mu.Unlock()
		return c.cache.GetUserTags(userName)
	}
	if entry != nil && entry.loading {
		// Another goroutine is loading — return stale/empty data
		c.mu.Unlock()
		return c.cache.GetUserTags(userName)
	}
	c.loadedUsers[userName] = &cacheEntry{loading: true}
	c.mu.Unlock()

	if c.loader != nil {
		loadedTags, err := c.loader.LoadUserTags(ctx, userName)
		if err != nil {
			log.Ctx(ctx).Warn("failed to load user tags on-demand",
				zap.String("userName", userName),
				zap.Error(err))
			GetRLSMetrics().RecordUserTagsLoadFailure()

			// On failure, clear so next request retries
			c.mu.Lock()
			delete(c.loadedUsers, userName)
			c.mu.Unlock()

			// Return stale data if available
			if entry != nil {
				return c.cache.GetUserTags(userName)
			}
			return make(map[string]string)
		}

		if len(loadedTags) > 0 {
			c.cache.SetUserTags(userName, loadedTags)
		} else {
			c.cache.DeleteAllUserTags(userName)
		}
	}

	// Mark as loaded after data is populated
	c.mu.Lock()
	c.loadedUsers[userName] = &cacheEntry{loadedAt: time.Now()}
	c.mu.Unlock()

	return c.cache.GetUserTags(userName)
}

// InvalidateCollectionCache invalidates cache and marks as not loaded
func (c *RLSCacheWithLoader) InvalidateCollectionCache(dbID int64, collectionID int64) {
	c.cache.InvalidateCollectionCache(dbID, collectionID)

	key := generateCollectionKey(dbID, collectionID)
	c.mu.Lock()
	delete(c.loadedCollections, key)
	c.mu.Unlock()
}

// InvalidateUserCache invalidates user cache and marks as not loaded
func (c *RLSCacheWithLoader) InvalidateUserCache(userName string) {
	c.cache.DeleteAllUserTags(userName)

	c.mu.Lock()
	delete(c.loadedUsers, userName)
	c.mu.Unlock()
}

// InvalidateAllCache invalidates all caches
func (c *RLSCacheWithLoader) InvalidateAllCache() {
	c.cache.InvalidateAllCache()

	c.mu.Lock()
	c.loadedCollections = make(map[string]*cacheEntry)
	c.loadedUsers = make(map[string]*cacheEntry)
	c.mu.Unlock()
}

// GetUnderlyingCache returns the underlying cache
func (c *RLSCacheWithLoader) GetUnderlyingCache() *RLSCache {
	return c.cache
}
