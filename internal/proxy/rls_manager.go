package proxy

import (
	"context"
	"sync"
)

// RLSManager manages RLS components for the proxy
type RLSManager struct {
	mu              sync.RWMutex
	cache           *RLSCache
	cacheWithLoader *RLSCacheWithLoader

	queryInterceptor  *RLSQueryInterceptor
	searchInterceptor *RLSSearchInterceptor
	insertInterceptor *RLSInsertInterceptor
	deleteInterceptor *RLSDeleteInterceptor
	upsertInterceptor *RLSUpsertInterceptor

	contextProvider ContextProvider
	loader          RLSPolicyLoader
}

var (
	globalRLSManager   *RLSManager
	globalRLSManagerMu sync.RWMutex
)

// InitRLSManager initializes the global RLS manager
func InitRLSManager() *RLSManager {
	mgr := &RLSManager{
		cache: NewRLSCache(),
	}
	globalRLSManagerMu.Lock()
	globalRLSManager = mgr
	globalRLSManagerMu.Unlock()
	return mgr
}

// GetRLSManager returns the global RLS manager
func GetRLSManager() *RLSManager {
	globalRLSManagerMu.RLock()
	defer globalRLSManagerMu.RUnlock()
	return globalRLSManager
}

// GetCache returns the RLS cache
func (m *RLSManager) GetCache() *RLSCache {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cache
}

// SetPolicyLoader sets the policy loader
func (m *RLSManager) SetPolicyLoader(loader RLSPolicyLoader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loader = loader
	if loader != nil && m.cache != nil {
		m.cacheWithLoader = NewRLSCacheWithLoader(m.cache, loader)
	} else {
		m.cacheWithLoader = nil
	}
}

// GetCacheWithLoader returns the cache wrapper with lazy loader.
func (m *RLSManager) GetCacheWithLoader() *RLSCacheWithLoader {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cacheWithLoader
}

// GetQueryInterceptor returns the query interceptor
func (m *RLSManager) GetQueryInterceptor() *RLSQueryInterceptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.queryInterceptor
}

// GetSearchInterceptor returns the search interceptor
func (m *RLSManager) GetSearchInterceptor() *RLSSearchInterceptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.searchInterceptor
}

// GetInsertInterceptor returns the insert interceptor
func (m *RLSManager) GetInsertInterceptor() *RLSInsertInterceptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.insertInterceptor
}

// GetDeleteInterceptor returns the delete interceptor
func (m *RLSManager) GetDeleteInterceptor() *RLSDeleteInterceptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.deleteInterceptor
}

// GetUpsertInterceptor returns the upsert interceptor
func (m *RLSManager) GetUpsertInterceptor() *RLSUpsertInterceptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.upsertInterceptor
}

// SetContextProvider sets the context provider and initializes interceptors
func (m *RLSManager) SetContextProvider(provider ContextProvider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.contextProvider = provider
	m.queryInterceptor = NewRLSQueryInterceptor(m.cache, provider)
	m.searchInterceptor = NewRLSSearchInterceptor(m.cache, provider)
	m.insertInterceptor = NewRLSInsertInterceptor(m.cache, provider)
	m.deleteInterceptor = NewRLSDeleteInterceptor(m.cache, provider)
	m.upsertInterceptor = NewRLSUpsertInterceptor(m.cache, provider)
}

// EnsurePoliciesLoaded preloads policies from source-of-truth if a loader is configured.
func (m *RLSManager) EnsurePoliciesLoaded(ctx context.Context, dbID int64, collectionID int64, dbName, collectionName string) {
	m.mu.RLock()
	cacheWithLoader := m.cacheWithLoader
	m.mu.RUnlock()

	if cacheWithLoader == nil {
		return
	}
	cacheWithLoader.GetPoliciesForCollection(ctx, dbID, collectionID, dbName, collectionName)
}

// EnsureUserTagsLoaded preloads user tags from source-of-truth if a loader is configured.
func (m *RLSManager) EnsureUserTagsLoaded(ctx context.Context, userName string) {
	if userName == "" {
		return
	}

	m.mu.RLock()
	cacheWithLoader := m.cacheWithLoader
	m.mu.RUnlock()

	if cacheWithLoader == nil {
		return
	}
	cacheWithLoader.GetUserTags(ctx, userName)
}
