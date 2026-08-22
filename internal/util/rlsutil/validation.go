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

package rlsutil

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	maxSupportedPolicyActions = 8

	// MaxTransportIdentifierLength is the absolute safety bound for RLS
	// locator and identifier strings before an internal request is cloned.
	// It is intentionally much larger than the configurable creation limits.
	MaxTransportIdentifierLength = 64 * 1024
	// MaxTransportTagKeys bounds raw deletion work before deduplication. The
	// configurable semantic quota is applied to the distinct keys afterward.
	MaxTransportTagKeys = 4096
)

func validateTransportIdentifier(name, value string) error {
	if len(value) > MaxTransportIdentifierLength {
		return merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"RLS %s exceeds transport max length %d",
			name,
			MaxTransportIdentifierLength,
		))
	}
	return nil
}

// ValidateRequestTarget bounds collection locator fields before Proxy clones
// and forwards an RLS request. This fixed transport limit is deliberately
// separate from refreshable creation limits, so existing objects remain
// addressable after those limits are lowered.
func ValidateRequestTarget(dbName, collectionName string) error {
	if err := validateTransportIdentifier("database name", dbName); err != nil {
		return err
	}
	return validateTransportIdentifier("collection name", collectionName)
}

// ValidatePolicyName validates the required policy name without applying the
// creation limit, so existing policies remain addressable after a limit change.
func ValidatePolicyName(policyName string) error {
	if funcutil.IsEmptyString(policyName) {
		return merr.WrapErrParameterInvalidMsg("RLS policy name is empty")
	}
	return validateTransportIdentifier("policy name", policyName)
}

// ValidatePolicyNameWithLimit validates a policy name for creation.
func ValidatePolicyNameWithLimit(policyName string) error {
	if err := ValidatePolicyName(policyName); err != nil {
		return err
	}
	maxPolicyNameLength := paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.GetAsInt()
	if len(policyName) > maxPolicyNameLength {
		return merr.WrapErrParameterInvalidMsg("RLS policy name exceeds max length %d", maxPolicyNameLength)
	}
	return nil
}

// ValidatePolicy validates the structural fields of a policy definition for creation.
func ValidatePolicy(policyName string, policyType PolicyType, actions []PolicyAction, usingExpr string, checkExpr string) error {
	return validatePolicy(policyName, policyType, actions, usingExpr, checkExpr, ValidatePolicyNameWithLimit)
}

// ValidatePolicyForUpdate validates the structural fields of an existing policy.
// The refreshable creation-name limit is intentionally not reapplied because
// policy names are immutable and must remain addressable after the limit changes.
func ValidatePolicyForUpdate(policyName string, policyType PolicyType, actions []PolicyAction, usingExpr string, checkExpr string) error {
	return validatePolicy(policyName, policyType, actions, usingExpr, checkExpr, ValidatePolicyName)
}

func validatePolicy(policyName string, policyType PolicyType, actions []PolicyAction, usingExpr string, checkExpr string, validateName func(string) error) error {
	if err := validateName(policyName); err != nil {
		return err
	}
	switch policyType {
	case PolicyTypePermissive, PolicyTypeRestrictive:
	default:
		return merr.WrapErrParameterInvalidMsg("invalid RLS policy type: %s", policyType.String())
	}
	if len(actions) == 0 {
		return merr.WrapErrParameterInvalidMsg("RLS policy actions is empty")
	}
	if len(actions) > maxSupportedPolicyActions {
		return merr.WrapErrParameterInvalidMsg("RLS policy actions exceeds max count %d", maxSupportedPolicyActions)
	}
	usingExprEmpty := strings.TrimSpace(usingExpr) == ""
	checkExprEmpty := strings.TrimSpace(checkExpr) == ""
	if usingExprEmpty && checkExprEmpty {
		return merr.WrapErrParameterInvalidMsg("RLS policy must define using_expr or check_expr")
	}
	maxExpressionLength := paramtable.Get().ProxyCfg.RLSMaxExpressionLength.GetAsInt()
	if len(usingExpr) > maxExpressionLength {
		return merr.WrapErrParameterInvalidMsg("RLS using_expr exceeds max length %d", maxExpressionLength)
	}
	if len(checkExpr) > maxExpressionLength {
		return merr.WrapErrParameterInvalidMsg("RLS check_expr exceeds max length %d", maxExpressionLength)
	}

	seen := make(map[PolicyAction]struct{}, len(actions))
	needUsingExpr := false
	needCheckExpr := false
	for _, action := range actions {
		if _, ok := seen[action]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicated RLS policy action: %s", action.String())
		}
		seen[action] = struct{}{}

		switch action {
		case PolicyActionQuery,
			PolicyActionQueryIterator,
			PolicyActionSearch,
			PolicyActionSearchIterator,
			PolicyActionHybridSearch,
			PolicyActionDelete:
			needUsingExpr = true
		case PolicyActionInsert:
			needCheckExpr = true
		case PolicyActionUpsert:
			needUsingExpr = true
			needCheckExpr = true
		default:
			return merr.WrapErrParameterInvalidMsg("invalid RLS policy action: %s", action.String())
		}
	}
	if needUsingExpr && usingExprEmpty {
		return merr.WrapErrParameterInvalidMsg("RLS policy using_expr is required by selected actions")
	}
	if needCheckExpr && checkExprEmpty {
		return merr.WrapErrParameterInvalidMsg("RLS policy check_expr is required by selected actions")
	}
	if !needUsingExpr && !usingExprEmpty {
		return merr.WrapErrParameterInvalidMsg("RLS policy using_expr is not used by selected actions")
	}
	if !needCheckExpr && !checkExprEmpty {
		return merr.WrapErrParameterInvalidMsg("RLS policy check_expr is not used by selected actions")
	}
	return nil
}

// ValidatePolicyDescription validates a policy description length.
func ValidatePolicyDescription(description string) error {
	maxDescriptionLength := paramtable.Get().ProxyCfg.RLSMaxPolicyDescriptionLength.GetAsInt()
	if len(description) > maxDescriptionLength {
		return merr.WrapErrParameterInvalidMsg("RLS policy description exceeds max length %d", maxDescriptionLength)
	}
	return nil
}

// ValidatePrincipalName validates the required principal name without applying
// the creation limit, so existing principals remain addressable after a limit change.
func ValidatePrincipalName(principalName string) error {
	if funcutil.IsEmptyString(principalName) {
		return merr.WrapErrParameterInvalidMsg("RLS principal name is empty")
	}
	return validateTransportIdentifier("principal name", principalName)
}

// ValidatePrincipalNameWithLimit validates a principal name for create or update.
func ValidatePrincipalNameWithLimit(principalName string) error {
	if err := ValidatePrincipalName(principalName); err != nil {
		return err
	}
	maxPrincipalNameLength := paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.GetAsInt()
	if len(principalName) > maxPrincipalNameLength {
		return merr.WrapErrParameterInvalidMsg("RLS principal name exceeds max length %d", maxPrincipalNameLength)
	}
	return nil
}

// ValidateTagKey validates an existing RLS principal tag key without applying
// the creation limit, so existing keys remain addressable after a limit change.
func ValidateTagKey(tagKey string) error {
	if funcutil.IsEmptyString(tagKey) {
		return merr.WrapErrParameterInvalidMsg("RLS principal tag key is empty")
	}
	if err := validateTransportIdentifier("principal tag key", tagKey); err != nil {
		return err
	}
	if strings.ContainsRune(tagKey, '\'') {
		return merr.WrapErrParameterInvalidMsg("RLS principal tag key contains reserved character \"'\"")
	}
	return nil
}

// ValidateTagKeyWithLimit validates a tag key for creation or replacement.
func ValidateTagKeyWithLimit(tagKey string) error {
	if err := ValidateTagKey(tagKey); err != nil {
		return err
	}
	maxTagKeyLength := paramtable.Get().ProxyCfg.RLSMaxTagKeyLength.GetAsInt()
	if len(tagKey) > maxTagKeyLength {
		return merr.WrapErrParameterInvalidMsg("RLS principal tag key exceeds max length %d", maxTagKeyLength)
	}
	return nil
}

// ValidateTags validates a complete principal tag map.
func ValidateTags(tags map[string]string) error {
	if len(tags) == 0 {
		return merr.WrapErrParameterInvalidMsg("RLS principal tags are empty")
	}
	if len(tags) > paramtable.Get().ProxyCfg.RLSMaxTagsPerPrincipal.GetAsInt() {
		return merr.WrapErrServiceQuotaExceeded("unable to set RLS principal tags because the number of tags has reached the limit")
	}
	for key, value := range tags {
		if err := ValidateTagKeyWithLimit(key); err != nil {
			return err
		}
		maxTagValueLength := paramtable.Get().ProxyCfg.RLSMaxTagValueLength.GetAsInt()
		if len(value) > maxTagValueLength {
			return merr.WrapErrParameterInvalidMsg("RLS principal tag value exceeds max length %d", maxTagValueLength)
		}
	}
	return nil
}

// ValidateAndDeduplicateTagKeys bounds and normalizes a tag-key deletion list.
func ValidateAndDeduplicateTagKeys(tagKeys []string) ([]string, error) {
	if len(tagKeys) > MaxTransportTagKeys {
		return nil, merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"number of raw RLS principal tag keys to delete exceeds transport max limit %d",
			MaxTransportTagKeys,
		))
	}
	seen := make(map[string]struct{}, len(tagKeys))
	uniqueTagKeys := make([]string, 0, len(tagKeys))
	for _, key := range tagKeys {
		if err := ValidateTagKey(key); err != nil {
			return nil, err
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniqueTagKeys = append(uniqueTagKeys, key)
	}
	maxTagKeys := paramtable.Get().ProxyCfg.RLSMaxTagsPerPrincipal.GetAsInt()
	if len(uniqueTagKeys) > maxTagKeys {
		return nil, merr.WrapErrServiceQuotaExceededMsg(
			"number of distinct RLS principal tag keys to delete exceeds max limit %d",
			maxTagKeys,
		)
	}
	return uniqueTagKeys, nil
}
