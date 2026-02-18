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
	"fmt"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"go.uber.org/zap"
)

// RLSSearchInterceptor applies RLS filtering to search/hybrid search operations
type RLSSearchInterceptor struct {
	cache           *RLSCache
	exprBuilder     *RLSExpressionBuilder
	contextProvider ContextProvider
}

// NewRLSSearchInterceptor creates a new search interceptor with RLS support
func NewRLSSearchInterceptor(
	cache *RLSCache,
	contextProvider ContextProvider,
) *RLSSearchInterceptor {
	return &RLSSearchInterceptor{
		cache:           cache,
		exprBuilder:     NewRLSExpressionBuilder(),
		contextProvider: contextProvider,
	}
}

// InterceptSearch applies RLS filtering to search filter expression
// Search operations combine vector search with optional filter expression
// RLS is applied as an additional filter constraint
func (i *RLSSearchInterceptor) InterceptSearch(
	ctx context.Context,
	dbID int64,
	collectionID int64,
	searchFilter string,
	action string,
) (string, error) {
	// Skip RLS if cache is not available
	if i.cache == nil {
		return searchFilter, nil
	}

	// Get user context
	userContext, err := i.contextProvider.GetUserContext(ctx)
	if err != nil {
		log.Warn("failed to get user context for RLS search", zap.Error(err))
		return searchFilter, nil
	}

	if userContext == nil {
		return searchFilter, nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("userName", userContext.UserName),
		zap.Int64("collectionID", collectionID),
		zap.String("action", action),
	)

	// Get RLS policies for the collection
	policies := i.cache.GetPoliciesForCollection(dbID, collectionID)
	if len(policies) == 0 {
		logger.Debug("no RLS policies found for collection")
		return searchFilter, nil
	}

	// Convert CompiledRLSPolicy to model.RLSPolicy
	modelPolicies := make([]*model.RLSPolicy, 0, len(policies))
	for _, compiledPolicy := range policies {
		if compiledPolicy.RLSPolicy != nil {
			modelPolicies = append(modelPolicies, compiledPolicy.RLSPolicy)
		}
	}

	// Get user tags
	userTags := i.cache.GetUserTags(userContext.UserName)

	// Build RLS context
	rlsContext := &RLSContext{
		CurrentUserName: userContext.UserName,
		CurrentUserTags: userTags,
		CurrentRoles:    userContext.UserRoles,
	}

	// Build RLS expression for search
	rlsExpr, err := i.exprBuilder.BuildExpression(
		modelPolicies,
		action,
		rlsContext,
		userContext.UserRoles,
	)
	if err != nil {
		logger.Error("failed to build RLS expression for search", zap.Error(err))
		// On error, deny search by returning false
		return "false", nil
	}

	logger.Debug("built RLS expression for search",
		zap.String("rlsExpr", rlsExpr),
		zap.String("searchFilter", searchFilter),
	)

	// Merge search filter with RLS expression
	mergedExpr := i.mergeExpressions(searchFilter, rlsExpr)

	logger.Debug("merged RLS and search filter",
		zap.String("mergedExpr", mergedExpr),
	)

	return mergedExpr, nil
}

// InterceptHybridSearch applies RLS filtering to hybrid search operations
// Hybrid search combines multiple search operations with different methods
// RLS constraints must be applied to all sub-searches
func (i *RLSSearchInterceptor) InterceptHybridSearch(
	ctx context.Context,
	dbID int64,
	collectionID int64,
	searchFilters []string,
) ([]string, error) {
	// Skip RLS if cache is not available
	if i.cache == nil {
		return searchFilters, nil
	}

	// Get user context
	userContext, err := i.contextProvider.GetUserContext(ctx)
	if err != nil {
		log.Warn("failed to get user context for RLS hybrid search", zap.Error(err))
		return searchFilters, nil
	}

	if userContext == nil {
		return searchFilters, nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("userName", userContext.UserName),
		zap.Int64("collectionID", collectionID),
	)

	// Get RLS policies
	policies := i.cache.GetPoliciesForCollection(dbID, collectionID)
	if len(policies) == 0 {
		logger.Debug("no RLS policies found for hybrid search")
		return searchFilters, nil
	}

	// Convert CompiledRLSPolicy to model.RLSPolicy
	modelPolicies := make([]*model.RLSPolicy, 0, len(policies))
	for _, compiledPolicy := range policies {
		if compiledPolicy.RLSPolicy != nil {
			modelPolicies = append(modelPolicies, compiledPolicy.RLSPolicy)
		}
	}

	// Get user tags
	userTags := i.cache.GetUserTags(userContext.UserName)

	// Build RLS context
	rlsContext := &RLSContext{
		CurrentUserName: userContext.UserName,
		CurrentUserTags: userTags,
		CurrentRoles:    userContext.UserRoles,
	}

	// Build RLS expression for search
	rlsExpr, err := i.exprBuilder.BuildExpression(
		modelPolicies,
		"search",
		rlsContext,
		userContext.UserRoles,
	)
	if err != nil {
		logger.Error("failed to build RLS expression for hybrid search", zap.Error(err))
		// On error, deny all searches by returning false for all
		result := make([]string, len(searchFilters))
		for j := range result {
			result[j] = "false"
		}
		return result, nil
	}

	// Apply RLS to all sub-search filters
	result := make([]string, 0, len(searchFilters))
	for idx, searchFilter := range searchFilters {
		mergedExpr := i.mergeExpressions(searchFilter, rlsExpr)
		result = append(result, mergedExpr)

		logger.Debug("merged RLS in hybrid search",
			zap.Int("subSearchIndex", idx),
			zap.String("mergedExpr", mergedExpr),
		)
	}

	return result, nil
}

// mergeExpressions combines search filter and RLS expression
func (i *RLSSearchInterceptor) mergeExpressions(searchFilter, rlsExpr string) string {
	if searchFilter == "" && rlsExpr == "" {
		return "true"
	} else if searchFilter == "" {
		return rlsExpr
	} else if rlsExpr == "" {
		return searchFilter
	} else if rlsExpr == "false" {
		return "false"
	}

	// Both exist: AND them together
	return fmt.Sprintf("(%s) AND (%s)", searchFilter, rlsExpr)
}

// UpsertInterceptor applies RLS filtering to upsert operations
type RLSUpsertInterceptor struct {
	cache           *RLSCache
	exprBuilder     *RLSExpressionBuilder
	contextProvider ContextProvider
}

// NewRLSUpsertInterceptor creates a new upsert interceptor with RLS support
func NewRLSUpsertInterceptor(
	cache *RLSCache,
	contextProvider ContextProvider,
) *RLSUpsertInterceptor {
	return &RLSUpsertInterceptor{
		cache:           cache,
		exprBuilder:     NewRLSExpressionBuilder(),
		contextProvider: contextProvider,
	}
}

// InterceptUpsert validates upsert against RLS constraints
// Upsert combines insert and update semantics, so RLS validation is similar to insert
func (i *RLSUpsertInterceptor) InterceptUpsert(
	ctx context.Context,
	dbID int64,
	collectionID int64,
) error {
	// Skip RLS if cache is not available
	if i.cache == nil {
		return nil
	}

	// Get user context
	userContext, err := i.contextProvider.GetUserContext(ctx)
	if err != nil {
		log.Warn("failed to get user context for RLS upsert", zap.Error(err))
		return nil
	}

	if userContext == nil {
		return nil
	}

	// Get RLS policies for the collection
	policies := i.cache.GetPoliciesForCollection(dbID, collectionID)
	if len(policies) == 0 {
		return nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("userName", userContext.UserName),
		zap.Int64("collectionID", collectionID),
	)

	// Get user tags
	userTags := i.cache.GetUserTags(userContext.UserName)

	// Build RLS context
	rlsContext := &RLSContext{
		CurrentUserName: userContext.UserName,
		CurrentUserTags: userTags,
		CurrentRoles:    userContext.UserRoles,
	}

	// Convert CompiledRLSPolicy to model.RLSPolicy
	modelPolicies := make([]*model.RLSPolicy, 0, len(policies))
	for _, compiledPolicy := range policies {
		if compiledPolicy.RLSPolicy != nil {
			modelPolicies = append(modelPolicies, compiledPolicy.RLSPolicy)
		}
	}

	// Build check expression for upsert (similar to insert)
	checkExpr, err := i.exprBuilder.BuildExpression(
		modelPolicies,
		"upsert",
		rlsContext,
		userContext.UserRoles,
	)
	if err != nil {
		logger.Error("failed to build RLS check expression for upsert", zap.Error(err))
		// On error, deny upsert
		return fmt.Errorf("RLS check expression validation failed: %w", err)
	}

	// If expression evaluates to false, deny upsert
	if checkExpr == "false" {
		logger.Warn("upsert denied by RLS policies")
		return fmt.Errorf("upsert operation denied by RLS policies")
	}

	logger.Debug("upsert validated against RLS policies",
		zap.String("checkExpr", checkExpr),
	)

	return nil
}
