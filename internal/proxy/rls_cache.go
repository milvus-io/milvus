package proxy

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

// CompiledRLSPolicy is a policy with pre-compiled/parsed expressions
type CompiledRLSPolicy struct {
	*model.RLSPolicy
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

	// L2: Merged expressions cache
	// Key: collectionKey/user/action → merged expression
	// Uses a bounded map with per-collection prefix for targeted invalidation
	mergedExprCache map[string]string
}

// NewRLSCache creates a new RLS cache instance
func NewRLSCache() *RLSCache {
	return &RLSCache{
		policyCache:      make(map[string][]*CompiledRLSPolicy),
		collectionConfig: make(map[string]*RLSCollectionConfig),
		userTags:         make(map[string]map[string]string),
		mergedExprCache:  make(map[string]string),
	}
}

// GetPoliciesForCollection returns all policies for a collection
func (rc *RLSCache) GetPoliciesForCollection(dbID int64, collectionID int64) []*CompiledRLSPolicy {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	key := generateCollectionKey(dbID, collectionID)
	policies := rc.policyCache[key]
	if len(policies) == 0 {
		return nil
	}
	result := make([]*CompiledRLSPolicy, len(policies))
	copy(result, policies)
	return result
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

// GetMergedExpr returns a cached merged expression, if available
func (rc *RLSCache) GetMergedExpr(collectionKey, userName, action string) (string, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	l2Key := generateL2Key(collectionKey, userName, action)
	expr, ok := rc.mergedExprCache[l2Key]
	return expr, ok
}

// SetMergedExpr caches a merged expression
func (rc *RLSCache) SetMergedExpr(collectionKey, userName, action, expr string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Enforce cache size limit from config
	config := GetRLSConfig()
	maxEntries := 10000
	if config != nil {
		maxEntries = config.GetMaxCacheEntries()
	}
	if len(rc.mergedExprCache) >= maxEntries {
		// Evict all entries when limit is reached (simple strategy)
		rc.mergedExprCache = make(map[string]string)
	}

	l2Key := generateL2Key(collectionKey, userName, action)
	rc.mergedExprCache[l2Key] = expr
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

	// Invalidate L2 cache entries for this collection only
	rc.invalidateL2ForCollectionLocked(key)
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

	// Invalidate L2 cache entries for this collection only
	rc.invalidateL2ForCollectionLocked(key)
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

	// Invalidate L2 cache entries for this user
	rc.invalidateL2ForUserLocked(userName)
}

// UpdateUserTag updates a single tag for a user
func (rc *RLSCache) UpdateUserTag(userName string, key string, value string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if _, ok := rc.userTags[userName]; !ok {
		rc.userTags[userName] = make(map[string]string)
	}
	rc.userTags[userName][key] = value

	// Invalidate L2 cache entries for this user
	rc.invalidateL2ForUserLocked(userName)
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

	// Invalidate L2 cache entries for this user
	rc.invalidateL2ForUserLocked(userName)
}

// InvalidateCollectionCache removes all cache entries for a collection
func (rc *RLSCache) InvalidateCollectionCache(dbID int64, collectionID int64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	key := generateCollectionKey(dbID, collectionID)
	delete(rc.policyCache, key)
	delete(rc.collectionConfig, key)

	// Invalidate L2 cache entries for this collection only
	rc.invalidateL2ForCollectionLocked(key)
}

// InvalidateAllCache clears all caches
func (rc *RLSCache) InvalidateAllCache() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.policyCache = make(map[string][]*CompiledRLSPolicy)
	rc.collectionConfig = make(map[string]*RLSCollectionConfig)
	rc.userTags = make(map[string]map[string]string)
	rc.mergedExprCache = make(map[string]string)
}

// SetUserTags sets all tags for a user (alias for UpdateUserTags)
func (rc *RLSCache) SetUserTags(userName string, tags map[string]string) {
	rc.UpdateUserTags(userName, tags)
}

// DeleteAllUserTags removes all tags for a user
func (rc *RLSCache) DeleteAllUserTags(userName string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	delete(rc.userTags, userName)
	rc.invalidateL2ForUserLocked(userName)
}

// invalidateL2ForCollectionLocked removes L2 entries matching the collection key prefix.
// Must be called with rc.mu held.
func (rc *RLSCache) invalidateL2ForCollectionLocked(collectionKey string) {
	prefix := collectionKey + "/"
	for k := range rc.mergedExprCache {
		if strings.HasPrefix(k, prefix) {
			delete(rc.mergedExprCache, k)
		}
	}
}

// invalidateL2ForUserLocked removes L2 entries matching the user name.
// Must be called with rc.mu held.
func (rc *RLSCache) invalidateL2ForUserLocked(userName string) {
	// L2 key format: collectionKey/encodedUserName/encodedAction
	// We need to scan for entries containing /encodedUserName/
	needle := "/" + url.QueryEscape(userName) + "/"
	for k := range rc.mergedExprCache {
		if strings.Contains(k, needle) {
			delete(rc.mergedExprCache, k)
		}
	}
}

func generateCollectionKey(dbID int64, collectionID int64) string {
	return fmt.Sprintf("%d/%d", dbID, collectionID)
}

func generateL2Key(collectionKey, userName, action string) string {
	return collectionKey + "/" + url.QueryEscape(userName) + "/" + url.QueryEscape(action)
}
