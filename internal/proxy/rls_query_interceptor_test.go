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

func TestRLSQueryInterceptorWithoutPolicies(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSQueryInterceptor(cache, contextProvider)

	// Query without policies should deny by default.
	userFilter := "age > 18"
	result, err := interceptor.InterceptQuery(context.Background(), 1, 100, userFilter, "query")

	assert.NoError(t, err)
	assert.Equal(t, "false", result)
}

func TestRLSQueryInterceptorWithPermissivePolicy(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("alice", []string{"PUBLIC"})
	interceptor := NewRLSQueryInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"user_own_data",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"user own data policy",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Query with RLS policy
	userFilter := "age > 18"
	result, err := interceptor.InterceptQuery(context.Background(), 1, 100, userFilter, "query")

	assert.NoError(t, err)
	assert.Contains(t, result, "age > 18")
	assert.Contains(t, result, "owner_id == 'alice'")
	assert.Contains(t, result, "AND")
}

func TestRLSQueryInterceptorWithRestrictivePolicy(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("bob", []string{"employee"})
	interceptor := NewRLSQueryInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"restrict_archived",
			100, 1,
			model.RLSPolicyTypeRestrictive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"archived == false",
			"",
			"restrict archived records",
		),
	}
	cache.UpdatePolicies(1, 100, policies)
	// Setup user tags
	cache.UpdateUserTags("bob", map[string]string{"department": "engineering"})

	// Query should include restrictive constraint
	userFilter := "status == 'active'"
	result, err := interceptor.InterceptQuery(context.Background(), 1, 100, userFilter, "query")

	assert.NoError(t, err)
	assert.Contains(t, result, "status == 'active'")
	assert.Contains(t, result, "archived == false")
}

func TestRLSQueryInterceptorWithUserTags(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("charlie", []string{"employee"})
	interceptor := NewRLSQueryInterceptor(cache, contextProvider)

	// Setup policies that use user tags
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"department_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"department == $current_user_tags['department']",
			"",
			"view own department",
		),
	}
	cache.UpdatePolicies(1, 100, policies)
	// Setup user tags
	cache.UpdateUserTags("charlie", map[string]string{"department": "sales"})

	// Query should substitute user tags
	userFilter := ""
	result, err := interceptor.InterceptQuery(context.Background(), 1, 100, userFilter, "query")

	assert.NoError(t, err)
	assert.Contains(t, result, "department == 'sales'")
}

func TestRLSQueryInterceptorNilCache(t *testing.T) {
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSQueryInterceptor(nil, contextProvider)

	// Query with nil cache should return user filter unchanged
	userFilter := "age > 18"
	result, err := interceptor.InterceptQuery(context.Background(), 1, 100, userFilter, "query")

	assert.NoError(t, err)
	assert.Equal(t, userFilter, result)
}

func TestRLSQueryInterceptorMergeExpressions(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSQueryInterceptor(cache, contextProvider)

	tests := []struct {
		name       string
		userFilter string
		rlsExpr    string
		expected   string
	}{
		{"both empty", "", "", "true"},
		{"only user filter", "age > 18", "", "age > 18"},
		{"only rls expr", "", "owner_id == 'alice'", "owner_id == 'alice'"},
		{"both set", "age > 18", "owner_id == 'alice'", "(age > 18) AND (owner_id == 'alice')"},
		{"rls denies", "age > 18", "false", "false"},
	}

	for _, tt := range tests {
		result := interceptor.mergeExpressions(tt.userFilter, tt.rlsExpr)
		assert.Equal(t, tt.expected, result, "test case: %s", tt.name)
	}
}

func TestRLSInsertInterceptor(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("alice", []string{"PUBLIC"})
	interceptor := NewRLSInsertInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"insert_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"insert"},
			[]string{"PUBLIC"},
			"",
			"owner_id == $current_user_name",
			"insert check policy",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Insert should pass validation
	_, err := interceptor.InterceptInsert(context.Background(), 1, 100)
	assert.NoError(t, err)
}

func TestRLSInsertInterceptorDenied(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("alice", []string{"employee"})
	interceptor := NewRLSInsertInterceptor(cache, contextProvider)

	// Setup policy that denies insert for employees
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"admin_only_insert",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"insert"},
			[]string{"admin"},
			"",
			"true",
			"admin only insert",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Insert should be denied
	_, err := interceptor.InterceptInsert(context.Background(), 1, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "denied")
}

func TestRLSDeleteInterceptor(t *testing.T) {
	cache := NewRLSCache()
	contextProvider := NewSimpleContextProvider("alice", []string{"PUBLIC"})
	interceptor := NewRLSDeleteInterceptor(cache, contextProvider)

	// Setup policies
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"delete_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"delete"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"delete own data",
		),
	}
	cache.UpdatePolicies(1, 100, policies)

	// Delete should apply RLS filter
	deleteFilter := "status == 'inactive'"
	result, err := interceptor.InterceptDelete(context.Background(), 1, 100, deleteFilter)

	assert.NoError(t, err)
	assert.Contains(t, result, "status == 'inactive'")
	assert.Contains(t, result, "owner_id == 'alice'")
	assert.Contains(t, result, "AND")
}

func TestSimpleContextProvider(t *testing.T) {
	provider := NewSimpleContextProvider("alice", []string{"employee", "admin"})

	ctx := context.Background()
	userContext, err := provider.GetUserContext(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, userContext)
	assert.Equal(t, "alice", userContext.UserName)
	assert.Len(t, userContext.UserRoles, 2)
	assert.Contains(t, userContext.UserRoles, "employee")
	assert.Contains(t, userContext.UserRoles, "admin")
}

func TestSimpleContextProviderEmpty(t *testing.T) {
	provider := NewSimpleContextProvider("", []string{})

	ctx := context.Background()
	userContext, err := provider.GetUserContext(ctx)

	assert.NoError(t, err)
	assert.Nil(t, userContext)
}
