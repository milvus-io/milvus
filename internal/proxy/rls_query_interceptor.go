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

// RLSQueryContext holds user context for RLS evaluation in queries
type RLSQueryContext struct {
	UserName  string
	UserRoles []string
}

// RLSQueryInterceptor intercepts query requests to apply RLS filtering
type RLSQueryInterceptor struct {
	cache           *RLSCache
	exprBuilder     *RLSExpressionBuilder
	contextProvider ContextProvider
}

// ContextProvider provides user context for RLS evaluation
type ContextProvider interface {
	// GetUserContext returns the RLS context for the current request
	GetUserContext(ctx context.Context) (*RLSQueryContext, error)
}

// NewRLSQueryInterceptor creates a new query interceptor with RLS support
func NewRLSQueryInterceptor(
	cache *RLSCache,
	contextProvider ContextProvider,
) *RLSQueryInterceptor {
	return &RLSQueryInterceptor{
		cache:           cache,
		exprBuilder:     NewRLSExpressionBuilder(),
		contextProvider: contextProvider,
	}
}

// InterceptQuery applies RLS filtering to a query expression
// Returns the merged expression combining user filter and RLS constraints
func (i *RLSQueryInterceptor) InterceptQuery(
	ctx context.Context,
	dbID int64,
	collectionID int64,
	userFilter string,
	action string,
) (string, error) {
	// Skip RLS if cache is not available
	if i.cache == nil {
		return userFilter, nil
	}

	// Get user context
	userContext, err := i.contextProvider.GetUserContext(ctx)
	if err != nil {
		log.Warn("failed to get user context for RLS", zap.Error(err))
		return userFilter, nil
	}

	if userContext == nil {
		return userFilter, nil
	}

	logger := log.Ctx(ctx).With(
		zap.String("userName", userContext.UserName),
		zap.Int64("collectionID", collectionID),
		zap.String("action", action),
	)

	// Get RLS policies for the collection
	policies := i.cache.GetPoliciesForCollection(dbID, collectionID)
	if len(policies) == 0 {
		// No policies, return user filter as-is
		logger.Debug("no RLS policies found for collection")
		return userFilter, nil
	}

	// Convert CompiledRLSPolicy to model.RLSPolicy for expression builder
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

	// Build RLS expression
	rlsExpr, err := i.exprBuilder.BuildExpression(
		modelPolicies,
		action,
		rlsContext,
		userContext.UserRoles,
	)
	if err != nil {
		logger.Error("failed to build RLS expression", zap.Error(err))
		// On error, deny access by returning false
		return "false", nil
	}

	logger.Debug("built RLS expression",
		zap.String("rlsExpr", rlsExpr),
		zap.String("userFilter", userFilter),
	)

	// Merge RLS expression with user filter
	mergedExpr := i.mergeExpressions(userFilter, rlsExpr)

	logger.Debug("merged RLS and user filter",
		zap.String("mergedExpr", mergedExpr),
	)

	return mergedExpr, nil
}

// mergeExpressions combines user filter and RLS expression
// Logic: (userFilter) AND (rlsExpr)
func (i *RLSQueryInterceptor) mergeExpressions(userFilter, rlsExpr string) string {
	if userFilter == "" && rlsExpr == "" {
		return "true" // No constraints
	} else if userFilter == "" {
		return rlsExpr // Only RLS
	} else if rlsExpr == "" {
		return userFilter // Only user filter
	} else if rlsExpr == "false" {
		return "false" // RLS denies all
	}

	// Both exist: AND them together
	return fmt.Sprintf("(%s) AND (%s)", userFilter, rlsExpr)
}

// SimpleContextProvider is a basic implementation of ContextProvider
// It can be extended to read user context from request headers or tokens
type SimpleContextProvider struct {
	userName string
	roles    []string
}

// NewSimpleContextProvider creates a simple context provider
func NewSimpleContextProvider(userName string, roles []string) *SimpleContextProvider {
	return &SimpleContextProvider{
		userName: userName,
		roles:    roles,
	}
}

// GetUserContext returns the stored user context
func (p *SimpleContextProvider) GetUserContext(ctx context.Context) (*RLSQueryContext, error) {
	if p.userName == "" {
		return nil, nil
	}
	return &RLSQueryContext{
		UserName:  p.userName,
		UserRoles: p.roles,
	}, nil
}

// InsertInterceptor applies RLS filtering to insert operations
type RLSInsertInterceptor struct {
	cache           *RLSCache
	exprBuilder     *RLSExpressionBuilder
	contextProvider ContextProvider
}

// NewRLSInsertInterceptor creates a new insert interceptor with RLS support
func NewRLSInsertInterceptor(
	cache *RLSCache,
	contextProvider ContextProvider,
) *RLSInsertInterceptor {
	return &RLSInsertInterceptor{
		cache:           cache,
		exprBuilder:     NewRLSExpressionBuilder(),
		contextProvider: contextProvider,
	}
}

// InterceptInsert validates insert against RLS CHECK expressions
// Returns error if insert violates RLS constraints
func (i *RLSInsertInterceptor) InterceptInsert(
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
		log.Warn("failed to get user context for RLS insert validation", zap.Error(err))
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

	// Build check expression for insert
	checkExpr, err := i.exprBuilder.BuildExpression(
		modelPolicies,
		"insert",
		rlsContext,
		userContext.UserRoles,
	)
	if err != nil {
		logger.Error("failed to build RLS check expression", zap.Error(err))
		// On error, deny insert
		return fmt.Errorf("RLS check expression validation failed: %w", err)
	}

	// If expression evaluates to false, deny insert
	if checkExpr == "false" {
		logger.Warn("insert denied by RLS policies")
		return fmt.Errorf("insert operation denied by RLS policies")
	}

	logger.Debug("insert validated against RLS policies",
		zap.String("checkExpr", checkExpr),
	)

	return nil
}

// DeleteInterceptor applies RLS filtering to delete operations
type RLSDeleteInterceptor struct {
	cache           *RLSCache
	exprBuilder     *RLSExpressionBuilder
	contextProvider ContextProvider
}

// NewRLSDeleteInterceptor creates a new delete interceptor with RLS support
func NewRLSDeleteInterceptor(
	cache *RLSCache,
	contextProvider ContextProvider,
) *RLSDeleteInterceptor {
	return &RLSDeleteInterceptor{
		cache:           cache,
		exprBuilder:     NewRLSExpressionBuilder(),
		contextProvider: contextProvider,
	}
}

// InterceptDelete applies RLS filtering to delete expressions
// Only allows deleting rows that match RLS policies
func (i *RLSDeleteInterceptor) InterceptDelete(
	ctx context.Context,
	dbID int64,
	collectionID int64,
	deleteFilter string,
) (string, error) {
	// Skip RLS if cache is not available
	if i.cache == nil {
		return deleteFilter, nil
	}

	// Get user context
	userContext, err := i.contextProvider.GetUserContext(ctx)
	if err != nil {
		log.Warn("failed to get user context for RLS delete", zap.Error(err))
		return deleteFilter, nil
	}

	if userContext == nil {
		return deleteFilter, nil
	}

	// Get RLS policies for the collection
	policies := i.cache.GetPoliciesForCollection(dbID, collectionID)
	if len(policies) == 0 {
		return deleteFilter, nil
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

	// Build RLS expression for delete
	rlsExpr, err := i.exprBuilder.BuildExpression(
		modelPolicies,
		"delete",
		rlsContext,
		userContext.UserRoles,
	)
	if err != nil {
		logger.Error("failed to build RLS expression for delete", zap.Error(err))
		return "false", nil
	}

	logger.Debug("built RLS expression for delete",
		zap.String("rlsExpr", rlsExpr),
		zap.String("deleteFilter", deleteFilter),
	)

	// Merge delete filter with RLS expression
	mergedExpr := i.mergeExpressions(deleteFilter, rlsExpr)

	logger.Debug("merged RLS and delete filter",
		zap.String("mergedExpr", mergedExpr),
	)

	return mergedExpr, nil
}

// mergeExpressions combines filters with RLS expression
func (i *RLSDeleteInterceptor) mergeExpressions(filter, rlsExpr string) string {
	if filter == "" && rlsExpr == "" {
		return "true"
	} else if filter == "" {
		return rlsExpr
	} else if rlsExpr == "" {
		return filter
	} else if rlsExpr == "false" {
		return "false"
	}
	return fmt.Sprintf("(%s) AND (%s)", filter, rlsExpr)
}
