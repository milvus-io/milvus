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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
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
func (h *RLSCacheRefreshHandler) HandleCacheRefresh(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest) error {
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
	case milvuspb.RLSCacheOpType_CreatePolicy:
		return h.handleCreatePolicy(ctx, req, logger)
	case milvuspb.RLSCacheOpType_DropPolicy:
		return h.handleDropPolicy(ctx, req, logger)
	case milvuspb.RLSCacheOpType_UpdateUserTags:
		return h.handleUpdateUserTags(ctx, req, logger)
	case milvuspb.RLSCacheOpType_DeleteUserTag:
		return h.handleDeleteUserTag(ctx, req, logger)
	case milvuspb.RLSCacheOpType_UpdateCollectionConfig:
		return h.handleUpdateCollectionConfig(ctx, req, logger)
	default:
		logger.Warn("unknown cache refresh operation type")
	}

	return nil
}

// handleCreatePolicy handles policy creation cache refresh
func (h *RLSCacheRefreshHandler) handleCreatePolicy(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest, logger *zap.Logger) error {
	logger.Info("handling create policy cache refresh",
		zap.String("policyName", req.GetPolicyName()),
	)

	// Invalidate collection cache to force reload on next access
	// In a full implementation, we would fetch the policy from RootCoord and cache it
	// For now, we just invalidate so it will be loaded on demand
	h.invalidateCollectionCache(req.GetDbName(), req.GetCollectionName())

	logger.Debug("policy cache invalidated for refresh")
	return nil
}

// handleDropPolicy handles policy deletion cache refresh
func (h *RLSCacheRefreshHandler) handleDropPolicy(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest, logger *zap.Logger) error {
	logger.Info("handling drop policy cache refresh",
		zap.String("policyName", req.GetPolicyName()),
	)

	// Invalidate collection cache when policy is dropped
	h.invalidateCollectionCache(req.GetDbName(), req.GetCollectionName())

	logger.Debug("policy cache invalidated after drop")
	return nil
}

// handleUpdateUserTags handles user tags update cache refresh
func (h *RLSCacheRefreshHandler) handleUpdateUserTags(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest, logger *zap.Logger) error {
	logger.Info("handling update user tags cache refresh",
		zap.String("userName", req.GetUserName()),
	)

	// For user tags, we need to invalidate expression cache but keep user tags
	// In a full implementation, we would fetch updated tags and cache them
	h.invalidateExpressionCache()

	logger.Debug("expression cache invalidated due to user tag update")
	return nil
}

// handleDeleteUserTag handles user tag deletion cache refresh
func (h *RLSCacheRefreshHandler) handleDeleteUserTag(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest, logger *zap.Logger) error {
	logger.Info("handling delete user tag cache refresh",
		zap.String("userName", req.GetUserName()),
	)

	// Invalidate expression cache when user tag is deleted
	h.invalidateExpressionCache()

	logger.Debug("expression cache invalidated due to user tag deletion")
	return nil
}

// handleUpdateCollectionConfig handles collection config update cache refresh
func (h *RLSCacheRefreshHandler) handleUpdateCollectionConfig(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest, logger *zap.Logger) error {
	logger.Info("handling update collection config cache refresh",
		zap.String("collectionName", req.GetCollectionName()),
	)

	// Invalidate collection cache for config changes
	h.invalidateCollectionCache(req.GetDbName(), req.GetCollectionName())

	logger.Debug("collection cache invalidated due to config update")
	return nil
}

// invalidateCollectionCache clears cache for a specific collection
func (h *RLSCacheRefreshHandler) invalidateCollectionCache(dbName, collectionName string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cache == nil {
		return
	}

	// Note: RLSCache uses dbID/collectionID, not names
	// In a full implementation, we would look up the IDs from the metastore
	// For now, we'll do a full cache clear as a safe fallback
	h.cache.InvalidateAllCache()
}

// invalidateExpressionCache clears the expression cache (L2 cache)
func (h *RLSCacheRefreshHandler) invalidateExpressionCache() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cache != nil {
		// The expression cache in RLSCache is cleared by invalidating all
		h.cache.InvalidateAllCache()
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
func (i *CacheRefreshInterceptor) OnCacheRefresh(ctx context.Context, req *milvuspb.RefreshRLSCacheRequest) error {
	if i.handler == nil {
		return nil
	}
	return i.handler.HandleCacheRefresh(ctx, req)
}
