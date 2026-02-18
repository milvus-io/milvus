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

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/stretchr/testify/assert"
)

func TestRLSSearchInterceptorWithoutPolicies(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSSearchInterceptor(cache, contextProvider)

	// Search without policies should return filter unchanged
	searchFilter := "score > 0.8"
	result, err := interceptor.InterceptSearch(context.Background(), 1, 100, searchFilter, "search")

	assert.NoError(t, err)
	assert.Equal(t, searchFilter, result)
}

func TestRLSSearchInterceptorWithPermissivePolicy(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"PUBLIC"})
	interceptor := NewRLSSearchInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"user_own_data",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"search"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"user own data policy",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Search with RLS policy
	searchFilter := "score > 0.8"
	result, err := interceptor.InterceptSearch(context.Background(), 1, 100, searchFilter, "search")

	assert.NoError(t, err)
	assert.Contains(t, result, "score > 0.8")
	assert.Contains(t, result, "owner_id == 'alice'")
	assert.Contains(t, result, "AND")
}

func TestRLSSearchInterceptorWithRestrictivePolicy(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("bob", []string{"employee"})
	interceptor := NewRLSSearchInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"restrict_archived",
			100, 1,
			model.RLSPolicyTypeRestrictive,
			[]string{"search"},
			[]string{"PUBLIC"},
			"archived == false",
			"",
			"restrict archived records",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Search should include restrictive constraint
	searchFilter := "category == 'electronics'"
	result, err := interceptor.InterceptSearch(context.Background(), 1, 100, searchFilter, "search")

	assert.NoError(t, err)
	assert.Contains(t, result, "category == 'electronics'")
	assert.Contains(t, result, "archived == false")
}

func TestRLSSearchInterceptorNilCache(t *testing.T) {
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSSearchInterceptor(nil, contextProvider)

	// Search with nil cache should return filter unchanged
	searchFilter := "score > 0.8"
	result, err := interceptor.InterceptSearch(context.Background(), 1, 100, searchFilter, "search")

	assert.NoError(t, err)
	assert.Equal(t, searchFilter, result)
}

func TestRLSSearchInterceptorMergeExpressions(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSSearchInterceptor(cache, contextProvider)

	tests := []struct {
		name         string
		searchFilter string
		rlsExpr      string
		expected     string
	}{
		{"both empty", "", "", "true"},
		{"only search filter", "score > 0.8", "", "score > 0.8"},
		{"only rls expr", "", "owner_id == 'alice'", "owner_id == 'alice'"},
		{"both set", "score > 0.8", "owner_id == 'alice'", "(score > 0.8) AND (owner_id == 'alice')"},
		{"rls denies", "score > 0.8", "false", "false"},
	}

	for _, tt := range tests {
		result := interceptor.mergeExpressions(tt.searchFilter, tt.rlsExpr)
		assert.Equal(t, tt.expected, result, "test case: %s", tt.name)
	}
}

func TestRLSHybridSearchInterceptor(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"PUBLIC"})
	interceptor := NewRLSSearchInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"user_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"search"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"user policy",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Hybrid search with multiple filters
	searchFilters := []string{"score > 0.8", "category == 'electronics'"}
	results, err := interceptor.InterceptHybridSearch(context.Background(), 1, 100, searchFilters)

	assert.NoError(t, err)
	assert.Len(t, results, 2)
	for _, result := range results {
		assert.Contains(t, result, "owner_id == 'alice'")
		assert.Contains(t, result, "AND")
	}
}

func TestRLSHybridSearchInterceptorNoPolicies(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSSearchInterceptor(cache, contextProvider)

	// Hybrid search without policies
	searchFilters := []string{"score > 0.8", "category == 'electronics'"}
	results, err := interceptor.InterceptHybridSearch(context.Background(), 1, 100, searchFilters)

	assert.NoError(t, err)
	assert.Equal(t, searchFilters, results)
}

func TestRLSUpsertInterceptor(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"PUBLIC"})
	interceptor := NewRLSUpsertInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"upsert_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"upsert"},
			[]string{"PUBLIC"},
			"",
			"owner_id == $current_user_name",
			"upsert check policy",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Upsert should pass validation
	err := interceptor.InterceptUpsert(context.Background(), 1, 100)
	assert.NoError(t, err)
}

func TestRLSUpsertInterceptorDenied(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSUpsertInterceptor(cache, contextProvider)

	// Setup policy that denies upsert for employees
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"admin_only_upsert",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"upsert"},
			[]string{"admin"},
			"",
			"true",
			"admin only upsert",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Upsert should be denied
	err := interceptor.InterceptUpsert(context.Background(), 1, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "denied")
}

func TestRLSUpsertInterceptorNoPolicies(t *testing.T) {
	cache := NewRLSCache(nil)
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSUpsertInterceptor(cache, contextProvider)

	// Upsert without policies should pass
	err := interceptor.InterceptUpsert(context.Background(), 1, 100)
	assert.NoError(t, err)
}

func TestRLSUpsertInterceptorNilCache(t *testing.T) {
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSUpsertInterceptor(nil, contextProvider)

	// Upsert with nil cache should pass
	err := interceptor.InterceptUpsert(context.Background(), 1, 100)
	assert.NoError(t, err)
}
