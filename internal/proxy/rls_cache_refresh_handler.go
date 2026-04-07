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

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"go.uber.org/zap"
)

// RLSCacheRefreshHandler manages cache refresh operations from RootCoord
type RLSCacheRefreshHandler struct {
	cache *RLSCache
	mu    sync.RWMutex
}

// NewRLSCacheRefreshHandler creates a new cache refresh handler
func NewRLSCacheRefreshHandler(cache *RLSCache) *RLSCacheRefreshHandler {
	return &RLSCacheRefreshHandler{
		cache: cache,
	}
}

// HandleCacheRefresh processes a cache refresh request from RootCoord
func (h *RLSCacheRefreshHandler) HandleCacheRefresh(ctx context.Context, req *messagespb.RefreshRLSCacheRequest) error {
	if h.cache == nil {
		log.Warn("RLS cache is nil, skipping refresh")
		return nil
	}

	if req == nil {
		return nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("opType", req.GetOpType().String()),
		zap.String("dbName", req.GetDbName()),
		zap.String("collectionName", req.GetCollectionName()),
	)

	switch req.GetOpType() {
	case messagespb.RLSCacheOpType_CreatePolicy:
		return h.handleCreatePolicy(ctx, req, logger)
	case messagespb.RLSCacheOpType_DropPolicy:
		return h.handleDropPolicy(ctx, req, logger)
	case messagespb.RLSCacheOpType_UpdateUserTags:
		return h.handleUpdateUserTags(ctx, req, logger)
	case messagespb.RLSCacheOpType_DeleteUserTag:
		return h.handleDeleteUserTag(ctx, req, logger)
	case messagespb.RLSCacheOpType_UpdateCollectionConfig:
		return h.handleUpdateCollectionConfig(ctx, req, logger)
	default:
		logger.Warn("unknown cache refresh operation type")
	}

	return nil
}

// handleCreatePolicy handles policy creation cache refresh
func (h *RLSCacheRefreshHandler) handleCreatePolicy(ctx context.Context, req *messagespb.RefreshRLSCacheRequest, logger *log.MLogger) error {
	logger.Info("handling create policy cache refresh",
		zap.String("policyName", req.GetPolicyName()),
	)

	// Invalidate collection cache to force reload on next access
	// In a full implementation, we would fetch the policy from RootCoord and cache it
	// For now, we just invalidate so it will be loaded on demand
	h.invalidateCollectionCache(ctx, req.GetDbName(), req.GetCollectionName())

	logger.Debug("policy cache invalidated for refresh")
	return nil
}

// handleDropPolicy handles policy deletion cache refresh
func (h *RLSCacheRefreshHandler) handleDropPolicy(ctx context.Context, req *messagespb.RefreshRLSCacheRequest, logger *log.MLogger) error {
	logger.Info("handling drop policy cache refresh",
		zap.String("policyName", req.GetPolicyName()),
	)

	// Invalidate collection cache when policy is dropped
	h.invalidateCollectionCache(ctx, req.GetDbName(), req.GetCollectionName())

	logger.Debug("policy cache invalidated after drop")
	return nil
}

// handleUpdateUserTags handles user tags update cache refresh
func (h *RLSCacheRefreshHandler) handleUpdateUserTags(ctx context.Context, req *messagespb.RefreshRLSCacheRequest, logger *log.MLogger) error {
	logger.Info("handling update user tags cache refresh",
		zap.String("userName", req.GetUserName()),
	)

	// Invalidate user-specific cache entries (tags + L2 expressions for this user)
	h.invalidateUserCache(req.GetUserName())

	logger.Debug("user cache invalidated due to user tag update")
	return nil
}

// handleDeleteUserTag handles user tag deletion cache refresh
func (h *RLSCacheRefreshHandler) handleDeleteUserTag(ctx context.Context, req *messagespb.RefreshRLSCacheRequest, logger *log.MLogger) error {
	logger.Info("handling delete user tag cache refresh",
		zap.String("userName", req.GetUserName()),
	)

	// Invalidate user-specific cache entries
	h.invalidateUserCache(req.GetUserName())

	logger.Debug("user cache invalidated due to user tag deletion")
	return nil
}

// handleUpdateCollectionConfig handles collection config update cache refresh
func (h *RLSCacheRefreshHandler) handleUpdateCollectionConfig(ctx context.Context, req *messagespb.RefreshRLSCacheRequest, logger *log.MLogger) error {
	logger.Info("handling update collection config cache refresh",
		zap.String("collectionName", req.GetCollectionName()),
	)

	// Invalidate collection cache for config changes
	h.invalidateCollectionCache(ctx, req.GetDbName(), req.GetCollectionName())

	logger.Debug("collection cache invalidated due to config update")
	return nil
}

// invalidateCollectionCache clears cache for a specific collection.
// Meta cache lookups are done outside the lock to avoid blocking other operations.
func (h *RLSCacheRefreshHandler) invalidateCollectionCache(ctx context.Context, dbName, collectionName string) {
	// Resolve IDs outside the lock — these may do remote calls
	var dbID int64
	var collectionID int64
	resolved := false
	if globalMetaCache != nil && dbName != "" && collectionName != "" {
		dbInfo, dbErr := globalMetaCache.GetDatabaseInfo(ctx, dbName)
		collID, collErr := globalMetaCache.GetCollectionID(ctx, dbName, collectionName)
		if dbErr == nil && collErr == nil && dbInfo != nil {
			dbID = dbInfo.dbID
			collectionID = collID
			resolved = true
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cache == nil {
		return
	}

	if resolved {
		if rlsMgr := GetRLSManager(); rlsMgr != nil {
			if cacheWithLoader := rlsMgr.GetCacheWithLoader(); cacheWithLoader != nil {
				cacheWithLoader.InvalidateCollectionCache(dbID, collectionID)
				return
			}
		}
		h.cache.InvalidateCollectionCache(dbID, collectionID)
		return
	}

	// Fallback when collection id cannot be resolved.
	if rlsMgr := GetRLSManager(); rlsMgr != nil {
		if cacheWithLoader := rlsMgr.GetCacheWithLoader(); cacheWithLoader != nil {
			cacheWithLoader.InvalidateAllCache()
			return
		}
	}
	h.cache.InvalidateAllCache()
}

// invalidateUserCache clears cache entries for a specific user
func (h *RLSCacheRefreshHandler) invalidateUserCache(userName string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cache == nil || userName == "" {
		return
	}

	h.cache.DeleteAllUserTags(userName)
	if rlsMgr := GetRLSManager(); rlsMgr != nil {
		if cacheWithLoader := rlsMgr.GetCacheWithLoader(); cacheWithLoader != nil {
			cacheWithLoader.InvalidateUserCache(userName)
		}
	}
}

// CacheRefreshInterceptor wraps a request to handle RLS cache refresh
// This can be used in the proxy's request handling pipeline
type CacheRefreshInterceptor struct {
	handler *RLSCacheRefreshHandler
}

// NewCacheRefreshInterceptor creates a new cache refresh interceptor
func NewCacheRefreshInterceptor(handler *RLSCacheRefreshHandler) *CacheRefreshInterceptor {
	return &CacheRefreshInterceptor{
		handler: handler,
	}
}

// OnCacheRefresh is called when a cache refresh request is received
func (i *CacheRefreshInterceptor) OnCacheRefresh(ctx context.Context, req *messagespb.RefreshRLSCacheRequest) error {
	if i.handler == nil {
		return nil
	}
	return i.handler.HandleCacheRefresh(ctx, req)
}
