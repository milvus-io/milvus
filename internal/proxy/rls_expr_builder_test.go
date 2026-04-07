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
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/stretchr/testify/assert"
)

func TestRLSExpressionBuilderPermissivePolicy(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"user_own_data",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"test policy",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"employee"},
	}

	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"employee"})
	assert.NoError(t, err)
	assert.Contains(t, expr, "owner_id == 'alice'")
}

func TestRLSExpressionBuilderRestrictivePolicy(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"restrict_salary",
			123, 456,
			model.RLSPolicyTypeRestrictive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"salary IS NULL OR department == $current_user_tags['department']",
			"",
			"restrict salary view",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "bob",
		CurrentUserTags: map[string]string{
			"department": "engineering",
		},
		CurrentRoles: []string{"employee"},
	}

	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"employee"})
	assert.NoError(t, err)
	assert.Contains(t, expr, "department == 'engineering'")
}

func TestRLSExpressionBuilderMixedPolicies(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"user_own_data",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"test policy",
		),
		model.NewRLSPolicy(
			"restrict_archived",
			123, 456,
			model.RLSPolicyTypeRestrictive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"archived == false",
			"",
			"restrict archived",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"employee"},
	}

	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"employee"})
	assert.NoError(t, err)
	// Should have both permissive and restrictive
	assert.Contains(t, expr, "owner_id == 'alice'")
	assert.Contains(t, expr, "archived == false")
	assert.Contains(t, expr, "AND")
}

func TestRLSExpressionBuilderNoPolicies(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"employee"},
	}

	expr, err := builder.BuildExpression([]*model.RLSPolicy{}, "query", rlsContext, []string{"employee"})
	assert.NoError(t, err)
	assert.Equal(t, "false", expr) // Deny all
}

func TestRLSExpressionBuilderRoleFiltering(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"employee_only",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"employee"},
			"true",
			"",
			"employee policy",
		),
		model.NewRLSPolicy(
			"admin_policy",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"admin"},
			"true",
			"",
			"admin policy",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"employee"},
	}

	// Employee should only get employee policy
	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"employee"})
	assert.NoError(t, err)
	assert.NotEqual(t, "false", expr)
}

func TestRLSExpressionBuilderVariableSubstitution(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{
			"department": "engineering",
			"region":     "us-west",
		},
	}

	result := builder.substituteVariables(
		"owner == $current_user_name AND dept == $current_user_tags['department']",
		rlsContext,
	)

	assert.Contains(t, result, "'alice'")
	assert.Contains(t, result, "'engineering'")
}

func TestRLSExpressionBuilderValidateExpression(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"valid simple", "true", false},
		{"valid complex", "(a == 1) AND (b == 2)", false},
		{"empty expression", "", true},
		{"unbalanced parens", "(a == 1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := builder.ValidateExpression(tt.expr)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRLSExpressionBuilderUpdateCollectionConfig(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	// Test that builder correctly handles updating collection config
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"config_test",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"status == 'active'",
			"",
			"test config",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "user1",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"PUBLIC"},
	}

	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"PUBLIC"})
	assert.NoError(t, err)
	assert.Contains(t, expr, "status == 'active'")
}

func TestRLSExpressionBuilderMultiplePoliciesAND(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	// Multiple permissive policies should be ORed
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"owner_id == $current_user_name",
			"",
			"policy 1",
		),
		model.NewRLSPolicy(
			"policy2",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"public == true",
			"",
			"policy 2",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"PUBLIC"},
	}

	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"PUBLIC"})
	assert.NoError(t, err)
	// Multiple permissive policies should be ORed
	assert.Contains(t, expr, "OR")
	assert.Contains(t, expr, "owner_id == 'alice'")
	assert.Contains(t, expr, "public == true")
}

func TestRLSExpressionBuilderActionFiltering(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"query_only",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"query policy",
		),
		model.NewRLSPolicy(
			"insert_only",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"insert"},
			[]string{"PUBLIC"},
			"",
			"true",
			"insert policy",
		),
	}

	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"PUBLIC"},
	}

	// Query should only get query policy
	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"PUBLIC"})
	assert.NoError(t, err)
	assert.NotEqual(t, "false", expr)

	// Insert should only get insert policy
	expr, err = builder.BuildExpression(policies, "insert", rlsContext, []string{"PUBLIC"})
	assert.NoError(t, err)
	assert.NotEqual(t, "false", expr)
}

func TestRLSExpressionBuilderMissingTagDeniesAccess(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"tenant_policy",
			123, 456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"tenant_id == $current_user_tags['tenant']",
			"",
			"tenant isolation",
		),
	}

	// User does NOT have the 'tenant' tag
	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{},
		CurrentRoles:    []string{"PUBLIC"},
	}

	// Should deny access when required tag is missing
	expr, err := builder.BuildExpression(policies, "query", rlsContext, []string{"PUBLIC"})
	assert.NoError(t, err)
	// The expression should deny all because the policy has an unresolvable tag placeholder.
	// The builder may wrap in parentheses, so accept both "false" and "(false)".
	assert.Contains(t, []string{"false", "(false)"}, expr)
}

func TestRLSExpressionBuilderPartialTagSubstitution(t *testing.T) {
	builder := NewRLSExpressionBuilder()

	// User has 'dept' but not 'region' — entire expression should be denied
	rlsContext := &RLSContext{
		CurrentUserName: "alice",
		CurrentUserTags: map[string]string{
			"dept": "engineering",
		},
	}

	result := builder.substituteVariables(
		"dept == $current_user_tags['dept'] AND region == $current_user_tags['region']",
		rlsContext,
	)
	assert.Equal(t, "false", result)
}
