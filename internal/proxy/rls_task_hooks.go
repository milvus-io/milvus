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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// applyRLSFilter applies RLS filter to a query/delete expression.
// Returns the original expression if RLS is not enabled or not applicable.
func applyRLSFilter(ctx context.Context, dbName string, collectionName string, collectionID int64, originalExpr string) (string, error) {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil {
		return originalExpr, nil
	}

	config := GetRLSConfig()
	if config == nil || !config.IsEnabled() {
		return originalExpr, nil
	}

	interceptor := rlsMgr.GetQueryInterceptor()
	if interceptor == nil {
		return originalExpr, nil
	}

	dbID := getDBIDForRLS(ctx, dbName)
	rlsMgr.EnsurePoliciesLoaded(ctx, dbID, collectionID, dbName, collectionName)
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		rlsMgr.EnsureUserTagsLoaded(ctx, username)
	}

	result, err := interceptor.InterceptQuery(ctx, dbID, collectionID, originalExpr, "query")
	if err != nil {
		log.Warn("RLS filter application failed",
			zap.String("collection", collectionName),
			zap.Error(err))
		if config.IsStrictMode() {
			return "", err
		}
		// In permissive mode, log the error and return original expression
		return originalExpr, nil
	}

	return result, nil
}

// applyRLSDeleteFilter applies RLS filter to a delete expression.
// Returns the original expression if RLS is not enabled or not applicable.
func applyRLSDeleteFilter(ctx context.Context, dbName string, collectionName string, collectionID int64, originalExpr string) (string, error) {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil {
		return originalExpr, nil
	}

	config := GetRLSConfig()
	if config == nil || !config.IsEnabled() {
		return originalExpr, nil
	}

	interceptor := rlsMgr.GetDeleteInterceptor()
	if interceptor == nil {
		return originalExpr, nil
	}

	dbID := getDBIDForRLS(ctx, dbName)
	rlsMgr.EnsurePoliciesLoaded(ctx, dbID, collectionID, dbName, collectionName)
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		rlsMgr.EnsureUserTagsLoaded(ctx, username)
	}

	result, err := interceptor.InterceptDelete(ctx, dbID, collectionID, originalExpr)
	if err != nil {
		log.Warn("RLS delete filter application failed",
			zap.String("collection", collectionName),
			zap.Error(err))
		if config.IsStrictMode() {
			return "", err
		}
		return originalExpr, nil
	}

	return result, nil
}

// applyRLSSearchFilter applies RLS filter to a search expression.
// Returns the original expression if RLS is not enabled or not applicable.
func applyRLSSearchFilter(ctx context.Context, dbName string, collectionName string, collectionID int64, originalExpr string) (string, error) {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil {
		return originalExpr, nil
	}

	config := GetRLSConfig()
	if config == nil || !config.IsEnabled() {
		return originalExpr, nil
	}

	interceptor := rlsMgr.GetSearchInterceptor()
	if interceptor == nil {
		return originalExpr, nil
	}

	dbID := getDBIDForRLS(ctx, dbName)
	rlsMgr.EnsurePoliciesLoaded(ctx, dbID, collectionID, dbName, collectionName)
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		rlsMgr.EnsureUserTagsLoaded(ctx, username)
	}

	result, err := interceptor.InterceptSearch(ctx, dbID, collectionID, originalExpr, "search")
	if err != nil {
		log.Warn("RLS search filter application failed",
			zap.String("collection", collectionName),
			zap.Error(err))
		if config.IsStrictMode() {
			return "", err
		}
		return originalExpr, nil
	}

	return result, nil
}

// applyRLSInsertCheck validates insert operations against RLS policies.
// Returns an error if the insert is denied by RLS policies.
func applyRLSInsertCheck(ctx context.Context, dbName string, collectionName string, collectionID int64) error {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil {
		return nil
	}

	config := GetRLSConfig()
	if config == nil || !config.IsEnabled() {
		return nil
	}

	interceptor := rlsMgr.GetInsertInterceptor()
	if interceptor == nil {
		return nil
	}

	dbID := getDBIDForRLS(ctx, dbName)
	rlsMgr.EnsurePoliciesLoaded(ctx, dbID, collectionID, dbName, collectionName)
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		rlsMgr.EnsureUserTagsLoaded(ctx, username)
	}

	_, err := interceptor.InterceptInsert(ctx, dbID, collectionID)
	if err != nil {
		log.Warn("RLS insert check failed",
			zap.String("collection", collectionName),
			zap.Error(err))
		return err
	}

	return nil
}

// applyRLSUpsertCheck validates upsert operations against RLS policies.
// Returns an error if the upsert is denied by RLS policies.
func applyRLSUpsertCheck(ctx context.Context, dbName string, collectionName string, collectionID int64) error {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil {
		return nil
	}

	config := GetRLSConfig()
	if config == nil || !config.IsEnabled() {
		return nil
	}

	interceptor := rlsMgr.GetUpsertInterceptor()
	if interceptor == nil {
		return nil
	}

	dbID := getDBIDForRLS(ctx, dbName)
	rlsMgr.EnsurePoliciesLoaded(ctx, dbID, collectionID, dbName, collectionName)
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		rlsMgr.EnsureUserTagsLoaded(ctx, username)
	}

	_, err := interceptor.InterceptUpsert(ctx, dbID, collectionID)
	if err != nil {
		log.Warn("RLS upsert check failed",
			zap.String("collection", collectionName),
			zap.Error(err))
		return err
	}

	return nil
}

// applyRLSHybridSearchFilters applies RLS to each sub-request DSL in a hybrid search.
// In permissive mode, on error the original filters are returned (fail-open — intentional).
// In strict mode, returns error.
func applyRLSHybridSearchFilters(ctx context.Context, dbName string, collectionName string, collectionID int64, subFilters []string) ([]string, error) {
	rlsMgr := GetRLSManager()
	if rlsMgr == nil {
		return subFilters, nil
	}

	config := GetRLSConfig()
	if config == nil || !config.IsEnabled() {
		return subFilters, nil
	}

	interceptor := rlsMgr.GetSearchInterceptor()
	if interceptor == nil {
		return subFilters, nil
	}

	dbID := getDBIDForRLS(ctx, dbName)
	rlsMgr.EnsurePoliciesLoaded(ctx, dbID, collectionID, dbName, collectionName)
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		rlsMgr.EnsureUserTagsLoaded(ctx, username)
	}

	result, err := interceptor.InterceptHybridSearch(ctx, dbID, collectionID, subFilters)
	if err != nil {
		log.Warn("RLS hybrid search filter application failed",
			zap.String("collection", collectionName),
			zap.Error(err))
		if config.IsStrictMode() {
			return nil, err
		}
		// Permissive mode: intentionally fail-open. RLS is NOT enforced for this request.
		log.Warn("RLS hybrid search filter bypassed in permissive mode (fail-open)",
			zap.String("collection", collectionName))
		return subFilters, nil
	}

	return result, nil
}

// getDBIDForRLS retrieves the database ID from the meta cache for RLS evaluation.
// Returns 0 if the database info cannot be found (e.g., default database).
func getDBIDForRLS(ctx context.Context, dbName string) int64 {
	if globalMetaCache == nil {
		return 0
	}
	dbInfo, err := globalMetaCache.GetDatabaseInfo(ctx, dbName)
	if err != nil {
		return 0
	}
	return dbInfo.dbID
}
