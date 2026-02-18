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
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

// RLSContext holds the runtime context for RLS evaluation
type RLSContext struct {
	CurrentUserName string
	CurrentUserTags map[string]string
	CurrentRoles    []string
}

// RLSExpressionBuilder builds RLS expressions from policies
type RLSExpressionBuilder struct {
}

// NewRLSExpressionBuilder creates a new expression builder
func NewRLSExpressionBuilder() *RLSExpressionBuilder {
	return &RLSExpressionBuilder{}
}

// BuildExpression builds the final RLS expression for a given action and user context
func (b *RLSExpressionBuilder) BuildExpression(policies []*model.RLSPolicy, action string, rlsContext *RLSContext, userRoles []string) (string, error) {
	if len(policies) == 0 {
		return "false", nil // Deny all if no policies
	}

	// Filter policies applicable to this user
	applicablePolicies := b.filterApplicablePolicies(policies, action, userRoles)
	if len(applicablePolicies) == 0 {
		return "false", nil // Deny all if no applicable policies
	}

	// Split by policy type
	permissive := b.filterByType(applicablePolicies, model.RLSPolicyTypePermissive)
	restrictive := b.filterByType(applicablePolicies, model.RLSPolicyTypeRestrictive)

	// Build expressions
	permExpr, err := b.buildPermissiveExpression(permissive, action, rlsContext)
	if err != nil {
		return "", fmt.Errorf("failed to build permissive expression: %w", err)
	}

	restrictExpr, err := b.buildRestrictiveExpression(restrictive, action, rlsContext)
	if err != nil {
		return "", fmt.Errorf("failed to build restrictive expression: %w", err)
	}

	// Merge: (PERMISSIVE) AND (RESTRICTIVE)
	return b.mergeExpressions(permExpr, restrictExpr), nil
}

// filterApplicablePolicies returns policies that apply to the current user
func (b *RLSExpressionBuilder) filterApplicablePolicies(policies []*model.RLSPolicy, action string, userRoles []string) []*model.RLSPolicy {
	applicable := []*model.RLSPolicy{}

	for _, policy := range policies {
		// Check if policy applies to this action
		if !policy.AppliesTo(action) {
			continue
		}

		// Check if policy applies to user's roles
		if b.policyAppliesToUser(policy, userRoles) {
			applicable = append(applicable, policy)
		}
	}

	return applicable
}

// policyAppliesToUser checks if a policy applies to the current user
func (b *RLSExpressionBuilder) policyAppliesToUser(policy *model.RLSPolicy, userRoles []string) bool {
	for _, policyRole := range policy.Roles {
		if policyRole == "PUBLIC" {
			return true
		}

		for _, userRole := range userRoles {
			if policyRole == userRole {
				return true
			}
		}
	}

	return false
}

// filterByType filters policies by type
func (b *RLSExpressionBuilder) filterByType(policies []*model.RLSPolicy, policyType model.RLSPolicyType) []*model.RLSPolicy {
	filtered := []*model.RLSPolicy{}
	for _, policy := range policies {
		if policy.PolicyType == policyType {
			filtered = append(filtered, policy)
		}
	}
	return filtered
}

// buildPermissiveExpression builds OR expression from permissive policies
func (b *RLSExpressionBuilder) buildPermissiveExpression(policies []*model.RLSPolicy, action string, rlsContext *RLSContext) (string, error) {
	if len(policies) == 0 {
		return "", nil // No permissive policies
	}

	expressions := []string{}
	for _, policy := range policies {
		expr := b.selectPolicyExpression(policy, action)
		if expr == "" {
			continue
		}

		// Variable substitution
		substituted := b.substituteVariables(expr, rlsContext)
		expressions = append(expressions, fmt.Sprintf("(%s)", substituted))
	}

	if len(expressions) == 0 {
		return "", nil
	}

	// OR all expressions together
	return strings.Join(expressions, " OR "), nil
}

// buildRestrictiveExpression builds AND expression from restrictive policies
func (b *RLSExpressionBuilder) buildRestrictiveExpression(policies []*model.RLSPolicy, action string, rlsContext *RLSContext) (string, error) {
	if len(policies) == 0 {
		return "", nil // No restrictive policies
	}

	expressions := []string{}
	for _, policy := range policies {
		expr := b.selectPolicyExpression(policy, action)
		if expr == "" {
			continue
		}

		// Variable substitution
		substituted := b.substituteVariables(expr, rlsContext)
		expressions = append(expressions, fmt.Sprintf("(%s)", substituted))
	}

	if len(expressions) == 0 {
		return "", nil
	}

	// AND all expressions together
	return strings.Join(expressions, " AND "), nil
}

// mergeExpressions merges permissive and restrictive expressions
func (b *RLSExpressionBuilder) mergeExpressions(permExpr string, restrictExpr string) string {
	if permExpr == "" && restrictExpr == "" {
		return "false" // Deny all
	} else if permExpr == "" {
		return restrictExpr // Only restrictive
	} else if restrictExpr == "" {
		return permExpr // Only permissive
	}

	// Both exist
	return fmt.Sprintf("(%s) AND (%s)", permExpr, restrictExpr)
}

func (b *RLSExpressionBuilder) selectPolicyExpression(policy *model.RLSPolicy, action string) string {
	if policy == nil {
		return ""
	}

	switch strings.ToLower(action) {
	case string(model.RLSActionInsert), string(model.RLSActionUpsert):
		return strings.TrimSpace(policy.CheckExpr)
	default:
		return strings.TrimSpace(policy.UsingExpr)
	}
}

// substituteVariables performs variable substitution in expressions
func (b *RLSExpressionBuilder) substituteVariables(expr string, rlsContext *RLSContext) string {
	result := expr

	// Replace $current_user_name
	result = strings.ReplaceAll(result, "$current_user_name", fmt.Sprintf("'%s'", escapeString(rlsContext.CurrentUserName)))

	// Replace $current_user_tags['key']
	for key, value := range rlsContext.CurrentUserTags {
		placeholder := fmt.Sprintf("$current_user_tags['%s']", key)
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("'%s'", escapeString(value)))
	}

	// Replace any remaining $current_user_tags[...] placeholders (tags not present on user)
	// with a sentinel that never matches real data — policy should deny access for missing tags.
	for strings.Contains(result, "$current_user_tags[") {
		start := strings.Index(result, "$current_user_tags[")
		end := strings.Index(result[start:], "]")
		if end < 0 {
			break // malformed placeholder, leave as-is (expression parser will reject it)
		}
		placeholder := result[start : start+end+1]
		result = strings.Replace(result, placeholder, "'__rls_no_tag__'", 1)
	}

	return result
}

// escapeString escapes special characters in string values
func escapeString(s string) string {
	// Escape backslashes first, then single quotes
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	// Also escape double quotes and special chars that could affect expression parsing
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

// ValidateExpression performs basic validation on an expression
func (b *RLSExpressionBuilder) ValidateExpression(expr string) error {
	if expr == "" {
		return fmt.Errorf("expression cannot be empty")
	}

	// Check for obviously invalid patterns
	if !isValidExpressionFormat(expr) {
		return fmt.Errorf("expression has invalid format")
	}

	return nil
}

// isValidExpressionFormat performs basic format validation
func isValidExpressionFormat(expr string) bool {
	// Check for balanced parentheses
	count := 0
	for _, ch := range expr {
		if ch == '(' {
			count++
		} else if ch == ')' {
			count--
		}
		if count < 0 {
			return false
		}
	}
	return count == 0
}
