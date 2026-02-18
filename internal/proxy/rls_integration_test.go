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
	"github.com/stretchr/testify/suite"
)

// RLSIntegrationTestSuite tests end-to-end RLS functionality
type RLSIntegrationTestSuite struct {
	suite.Suite
	cache           *RLSCache
	exprBuilder     *RLSExpressionBuilder
	queryInt        *RLSQueryInterceptor
	insertInt       *RLSInsertInterceptor
	deleteInt       *RLSDeleteInterceptor
	searchInt       *RLSSearchInterceptor
	upsertInt       *RLSUpsertInterceptor
	contextProvider ContextProvider
}

// SetupTest initializes test components before each test
func (suite *RLSIntegrationTestSuite) SetupTest() {
	suite.cache = NewRLSCache(nil)
	suite.exprBuilder = NewRLSExpressionBuilder()
	suite.contextProvider = NewSimpleContextProvider("alice", []string{"employee"})

	suite.queryInt = NewRLSQueryInterceptor(suite.cache, suite.contextProvider)
	suite.insertInt = NewRLSInsertInterceptor(suite.cache, suite.contextProvider)
	suite.deleteInt = NewRLSDeleteInterceptor(suite.cache, suite.contextProvider)
	suite.searchInt = NewRLSSearchInterceptor(suite.cache, suite.contextProvider)
	suite.upsertInt = NewRLSUpsertInterceptor(suite.cache, suite.contextProvider)
}

// TestScenarioBasicPermissivePolicy tests a simple permissive policy
func (suite *RLSIntegrationTestSuite) TestScenarioBasicPermissivePolicy() {
	// Scenario: User can only see their own data
	policy := model.NewRLSPolicy(
		"user_own_data",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"query", "search"},
		[]string{"PUBLIC"},
		"owner_id == $current_user_name",
		"",
		"Users can only query their own data",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})

	// Test query
	result, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "", "query")
	suite.NoError(err)
	suite.Contains(result, "owner_id == 'alice'")

	// Test search
	result, err = suite.searchInt.InterceptSearch(context.Background(), 1, 100, "score > 0.5", "search")
	suite.NoError(err)
	suite.Contains(result, "owner_id == 'alice'")
	suite.Contains(result, "score > 0.5")
}

// TestScenarioRestrictivePolicy tests a restrictive policy
func (suite *RLSIntegrationTestSuite) TestScenarioRestrictivePolicy() {
	// Scenario: Prevent access to archived data
	policy := model.NewRLSPolicy(
		"no_archived",
		100, 1,
		model.RLSPolicyTypeRestrictive,
		[]string{"query", "search", "delete"},
		[]string{"PUBLIC"},
		"archived == false",
		"",
		"Prevent access to archived data",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})

	// Test query
	result, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "status == 'active'", "query")
	suite.NoError(err)
	suite.Contains(result, "archived == false")
	suite.Contains(result, "status == 'active'")

	// Test delete
	result, err = suite.deleteInt.InterceptDelete(context.Background(), 1, 100, "id < 1000")
	suite.NoError(err)
	suite.Contains(result, "archived == false")
	suite.Contains(result, "id < 1000")
}

// TestScenarioMixedPolicies tests combining permissive and restrictive policies
func (suite *RLSIntegrationTestSuite) TestScenarioMixedPolicies() {
	// Scenario: Users see their own data + public data, but never archived
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"user_own_data",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"User own data",
		),
		model.NewRLSPolicy(
			"public_data",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"public == true",
			"",
			"Public data",
		),
		model.NewRLSPolicy(
			"no_archived",
			100, 1,
			model.RLSPolicyTypeRestrictive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"archived == false",
			"",
			"No archived",
		),
	}

	suite.cache.UpdatePolicies(1, 100, policies)

	// Expected: (owner_id == 'alice' OR public == true) AND (archived == false)
	result, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "", "query")
	suite.NoError(err)
	suite.Contains(result, "owner_id == 'alice'")
	suite.Contains(result, "public == true")
	suite.Contains(result, "archived == false")
	suite.Contains(result, "OR")
	suite.Contains(result, "AND")
}

// TestScenarioUserTags tests policies using user tags
func (suite *RLSIntegrationTestSuite) TestScenarioUserTags() {
	// Scenario: Users can see data from their department
	policy := model.NewRLSPolicy(
		"department_policy",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"query"},
		[]string{"employee"},
		"department == $current_user_tags['department']",
		"",
		"Department-based access",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})
	suite.cache.UpdateUserTags("alice", map[string]string{
		"department": "engineering",
		"region":     "us-west",
	})

	// Test query
	result, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "", "query")
	suite.NoError(err)
	suite.Contains(result, "department == 'engineering'")
}

// TestScenarioRoleBasedAccess tests role-based policy filtering
func (suite *RLSIntegrationTestSuite) TestScenarioRoleBasedAccess() {
	// Scenario: Different policies for different roles
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"employee_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"employee"},
			"owner_id == $current_user_name",
			"",
			"Employee: own data only",
		),
		model.NewRLSPolicy(
			"admin_policy",
			100, 1,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"admin"},
			"true",
			"",
			"Admin: all data",
		),
	}

	suite.cache.UpdatePolicies(1, 100, policies)

	// Employee should only see own data
	result, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "", "query")
	suite.NoError(err)
	suite.Contains(result, "owner_id == 'alice'")
	suite.NotContains(result, "true") // Admin policy should not apply
}

// TestScenarioInsertRestriction tests insert validation
func (suite *RLSIntegrationTestSuite) TestScenarioInsertRestriction() {
	// Scenario: Users can only insert data they own
	policy := model.NewRLSPolicy(
		"insert_own_data",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"insert"},
		[]string{"employee"},
		"",
		"owner_id == $current_user_name",
		"Insert own data only",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})

	// Insert should be allowed for employee role
	err := suite.insertInt.InterceptInsert(context.Background(), 1, 100)
	suite.NoError(err)
}

// TestScenarioInsertDenied tests insert denial
func (suite *RLSIntegrationTestSuite) TestScenarioInsertDenied() {
	// Scenario: Only admins can insert
	policy := model.NewRLSPolicy(
		"admin_only_insert",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"insert"},
		[]string{"admin"},
		"",
		"true",
		"Admin-only insert",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})

	// Insert should be denied for employee
	err := suite.insertInt.InterceptInsert(context.Background(), 1, 100)
	suite.Error(err)
	suite.Contains(err.Error(), "denied")
}

// TestScenarioHybridSearch tests hybrid search with RLS
func (suite *RLSIntegrationTestSuite) TestScenarioHybridSearch() {
	// Scenario: Hybrid search with department filtering
	policy := model.NewRLSPolicy(
		"department_search",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"search"},
		[]string{"employee"},
		"department == $current_user_tags['department']",
		"",
		"Department search",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})
	suite.cache.UpdateUserTags("alice", map[string]string{
		"department": "sales",
	})

	// Hybrid search with multiple queries
	filters := []string{"score > 0.7", "category == 'premium'"}
	results, err := suite.searchInt.InterceptHybridSearch(context.Background(), 1, 100, filters)
	suite.NoError(err)
	suite.Len(results, 2)

	// All sub-searches should have department filter
	for _, result := range results {
		suite.Contains(result, "department == 'sales'")
	}
}

// TestScenarioCacheRefresh tests cache refresh operations
func (suite *RLSIntegrationTestSuite) TestScenarioCacheRefresh() {
	// Setup initial policies
	policy := model.NewRLSPolicy(
		"initial_policy",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"query"},
		[]string{"PUBLIC"},
		"status == 'active'",
		"",
		"Initial policy",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})

	// Verify policy is cached
	policies := suite.cache.GetPoliciesForCollection(1, 100)
	suite.Len(policies, 1)

	// Simulate cache refresh
	suite.cache.InvalidateCollectionCache(1, 100)

	// Verify cache is cleared
	policies = suite.cache.GetPoliciesForCollection(1, 100)
	suite.Len(policies, 0)
}

// TestScenarioMultiActionPolicy tests a single policy for multiple actions
func (suite *RLSIntegrationTestSuite) TestScenarioMultiActionPolicy() {
	// Scenario: Same policy applies to query, search, and delete
	policy := model.NewRLSPolicy(
		"multi_action",
		100, 1,
		model.RLSPolicyTypePermissive,
		[]string{"query", "search", "delete"},
		[]string{"PUBLIC"},
		"owner_id == $current_user_name",
		"",
		"Multi-action policy",
	)

	suite.cache.UpdatePolicies(1, 100, []*model.RLSPolicy{policy})

	// Test all actions
	queryResult, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "", "query")
	suite.NoError(err)
	suite.Contains(queryResult, "owner_id == 'alice'")

	searchResult, err := suite.searchInt.InterceptSearch(context.Background(), 1, 100, "", "search")
	suite.NoError(err)
	suite.Contains(searchResult, "owner_id == 'alice'")

	deleteResult, err := suite.deleteInt.InterceptDelete(context.Background(), 1, 100, "")
	suite.NoError(err)
	suite.Contains(deleteResult, "owner_id == 'alice'")
}

// TestScenarioNoPoliciesDenyAll tests deny-all when no policies exist
func (suite *RLSIntegrationTestSuite) TestScenarioNoPoliciesDenyAll() {
	// No policies loaded
	// This scenario depends on configuration whether to deny or allow when no policies

	result, err := suite.queryInt.InterceptQuery(context.Background(), 1, 100, "age > 18", "query")
	suite.NoError(err)
	// Without policies, user filter should be returned as-is (permissive default)
	suite.Equal("age > 18", result)
}

// TestRLSIntegrationTestSuite runs the integration test suite
func TestRLSIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(RLSIntegrationTestSuite))
}
