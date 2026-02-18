package proxy

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/util/cache"
)

// CompiledRLSPolicy is a policy with pre-compiled/parsed expressions
type CompiledRLSPolicy struct {
	*model.RLSPolicy
	// UsingExprAST and CheckExprAST would be added after implementing expression parser integration
	// For now, storing raw expressions and parsing on-demand
}

// RLSCollectionConfig represents RLS configuration for a collection
type RLSCollectionConfig struct {
	CollectionID int64
	Enabled      bool
	Force        bool
}

// RLSCache manages RLS policies and user tags in the proxy
type RLSCache struct {
	mu sync.RWMutex

	// L1: Compiled policies per collection
	// Key: dbID/collectionID format
	policyCache map[string][]*CompiledRLSPolicy

	// L1: Collection RLS config
	collectionConfig map[string]*RLSCollectionConfig

	// L1: User tags (per user)
	userTags map[string]map[string]string

	// L2: Merged expressions LRU
	// Key: hash(user+collection+action)
	mergedExprCache *cache.Cache
}

// NewRLSCache creates a new RLS cache instance
func NewRLSCache() *RLSCache {
	return &RLSCache{
		policyCache:      make(map[string][]*CompiledRLSPolicy),
		collectionConfig: make(map[string]*RLSCollectionConfig),
		userTags:         make(map[string]map[string]string),
		mergedExprCache:  cache.NewCache(10000), // 10k entries LRU
	}
}

// GetPoliciesForCollection returns all policies for a collection
func (rc *RLSCache) GetPoliciesForCollection(dbID int64, collectionID int64) []*CompiledRLSPolicy {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	key := generateCollectionKey(dbID, collectionID)
	return rc.policyCache[key]
}

// GetCollectionConfig returns RLS configuration for a collection
func (rc *RLSCache) GetCollectionConfig(dbID int64, collectionID int64) *RLSCollectionConfig {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	key := generateCollectionKey(dbID, collectionID)
	return rc.collectionConfig[key]
}

// GetUserTags returns tags for a user
func (rc *RLSCache) GetUserTags(userName string) map[string]string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if tags, ok := rc.userTags[userName]; ok {
		// Return copy to prevent external modification
		tagsCopy := make(map[string]string)
		for k, v := range tags {
			tagsCopy[k] = v
		}
		return tagsCopy
	}
	return make(map[string]string)
}

// UpdatePolicies updates policies for a collection
func (rc *RLSCache) UpdatePolicies(dbID int64, collectionID int64, policies []*model.RLSPolicy) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	key := generateCollectionKey(dbID, collectionID)

	compiledPolicies := make([]*CompiledRLSPolicy, 0, len(policies))
	for _, policy := range policies {
		compiled := &CompiledRLSPolicy{
			RLSPolicy: policy,
		}
		compiledPolicies = append(compiledPolicies, compiled)
	}

	if len(compiledPolicies) > 0 {
		rc.policyCache[key] = compiledPolicies
	} else {
		delete(rc.policyCache, key)
	}

	// Invalidate L2 cache
	rc.mergedExprCache.Clear()
}

// UpdateCollectionConfig updates RLS configuration for a collection
func (rc *RLSCache) UpdateCollectionConfig(dbID int64, collectionID int64, enabled bool, force bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	key := generateCollectionKey(dbID, collectionID)

	if enabled || force {
		rc.collectionConfig[key] = &RLSCollectionConfig{
			CollectionID: collectionID,
			Enabled:      enabled,
			Force:        force,
		}
	} else {
		delete(rc.collectionConfig, key)
	}

	// Invalidate L2 cache
	rc.mergedExprCache.Clear()
}

// UpdateUserTags updates or replaces user tags
func (rc *RLSCache) UpdateUserTags(userName string, tags map[string]string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if len(tags) > 0 {
		rc.userTags[userName] = tags
	} else {
		delete(rc.userTags, userName)
	}

	// Invalidate L2 cache
	rc.mergedExprCache.Clear()
}

// UpdateUserTag updates a single tag for a user
func (rc *RLSCache) UpdateUserTag(userName string, key string, value string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if _, ok := rc.userTags[userName]; !ok {
		rc.userTags[userName] = make(map[string]string)
	}
	rc.userTags[userName][key] = value

	// Invalidate L2 cache
	rc.mergedExprCache.Clear()
}

// DeleteUserTag deletes a specific tag for a user
func (rc *RLSCache) DeleteUserTag(userName string, key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if tags, ok := rc.userTags[userName]; ok {
		delete(tags, key)
		if len(tags) == 0 {
			delete(rc.userTags, userName)
		}
	}

	// Invalidate L2 cache
	rc.mergedExprCache.Clear()
}

// InvalidateCollectionCache removes all cache entries for a collection
func (rc *RLSCache) InvalidateCollectionCache(dbID int64, collectionID int64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	key := generateCollectionKey(dbID, collectionID)
	delete(rc.policyCache, key)
	delete(rc.collectionConfig, key)

	// Invalidate L2 cache
	rc.mergedExprCache.Clear()
}

// InvalidateAllCache clears all caches
func (rc *RLSCache) InvalidateAllCache() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.policyCache = make(map[string][]*CompiledRLSPolicy)
	rc.collectionConfig = make(map[string]*RLSCollectionConfig)
	rc.userTags = make(map[string]map[string]string)
	rc.mergedExprCache.Clear()
}

// Helper functions

func generateCollectionKey(dbID int64, collectionID int64) string {
	return fmt.Sprintf("%d/%d", dbID, collectionID)
}
