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
	"math"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	maxSupportedPolicyActions       = 8
	maxJSONEscapeBytesPerByte int64 = int64(len(`\u0000`))
	maxJSONNumberLength             = len(`-1.7976931348623157e+308`)
	// maxRLSPrincipalMetadataBytes leaves headroom for one unchunked WAL
	// message and one metastore record while bounding JSON decoding and plan
	// template expansion.
	maxRLSPrincipalMetadataBytes int64 = 1 << 20
	// One array must fit the existing materialized-tag budget even before
	// string payloads. This structural ceiling is not a refreshable quota.
	maxRLSArrayTagElements = int(maxRLSPrincipalMetadataBytes/tagValueRetainedSize) - 1

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

// ValidatePolicyRoles rejects the deprecated role-scoped policy contract.
// RLS principals, rather than Milvus RBAC roles, are the only runtime policy identity.
func ValidatePolicyRoles(roles []string) error {
	if len(roles) > 0 {
		return merr.WrapErrParameterInvalidMsg("role-scoped RLS policies are not supported; roles must be empty")
	}
	return nil
}

// ValidatePolicyActionCount bounds the raw action list before conversion.
func ValidatePolicyActionCount(actionCount int) error {
	if actionCount == 0 {
		return merr.WrapErrParameterInvalidMsg("RLS policy actions is empty")
	}
	if actionCount > maxSupportedPolicyActions {
		return merr.WrapErrParameterInvalidMsg("RLS policy actions exceeds max count %d", maxSupportedPolicyActions)
	}
	return nil
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
	return validatePolicy(policyName, policyType, actions, usingExpr, checkExpr, ValidatePolicyNameWithLimit, true)
}

// ValidatePolicyForUpdate validates the structural fields of an existing policy.
// The refreshable creation-name limit is intentionally not reapplied because
// policy names are immutable and must remain addressable after the limit changes.
func ValidatePolicyForUpdate(policyName string, policyType PolicyType, actions []PolicyAction, usingExpr string, checkExpr string) error {
	return validatePolicy(policyName, policyType, actions, usingExpr, checkExpr, ValidatePolicyName, true)
}

// ValidateStoredPolicy validates persisted policy metadata without reapplying
// refreshable write-admission limits.
func ValidateStoredPolicy(policyName string, policyType PolicyType, actions []PolicyAction, usingExpr string, checkExpr string) error {
	return validatePolicy(policyName, policyType, actions, usingExpr, checkExpr, ValidatePolicyName, false)
}

func validatePolicy(policyName string, policyType PolicyType, actions []PolicyAction, usingExpr string, checkExpr string, validateName func(string) error, enforceExpressionLength bool) error {
	if err := validateName(policyName); err != nil {
		return err
	}
	switch policyType {
	case PolicyTypePermissive, PolicyTypeRestrictive:
	default:
		return merr.WrapErrParameterInvalidMsg("invalid RLS policy type: %s", policyType.String())
	}
	if err := ValidatePolicyActionCount(len(actions)); err != nil {
		return err
	}
	usingExprEmpty := strings.TrimSpace(usingExpr) == ""
	checkExprEmpty := strings.TrimSpace(checkExpr) == ""
	if usingExprEmpty && checkExprEmpty {
		return merr.WrapErrParameterInvalidMsg("RLS policy must define using_expr or check_expr")
	}
	if enforceExpressionLength {
		maxExpressionLength := paramtable.Get().ProxyCfg.RLSMaxExpressionLength.GetAsInt()
		if len(usingExpr) > maxExpressionLength {
			return merr.WrapErrParameterInvalidMsg("RLS using_expr exceeds max length %d", maxExpressionLength)
		}
		if len(checkExpr) > maxExpressionLength {
			return merr.WrapErrParameterInvalidMsg("RLS check_expr exceeds max length %d", maxExpressionLength)
		}
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

func validatePrincipalTagsJSONTransportSize(payload string, maxTags int) error {
	maxPayloadBytes := maxRLSPrincipalMetadataBytes
	if maxTags > 0 {
		maxPayloadBytes = min(maxPrincipalTagsJSONLength(maxTags), maxPayloadBytes)
	}
	if int64(len(payload)) > maxPayloadBytes {
		return merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"RLS principal tags JSON exceeds transport max length %d",
			maxPayloadBytes,
		))
	}
	return nil
}

// ValidatePrincipalTagsRecordSize bounds the complete canonical principal
// record after incremental tag updates have been merged.
func ValidatePrincipalTagsRecordSize(principalName string, tags map[string]TagValue) error {
	payload, err := TagsToJSON(tags)
	if err != nil {
		return err
	}
	payloadBytes := int64(len(payload))
	if payloadBytes > maxRLSPrincipalMetadataBytes ||
		int64(len(principalName)) > maxRLSPrincipalMetadataBytes-payloadBytes {
		return merr.WrapErrParameterTooLarge(fmt.Sprintf(
			"RLS principal name and tags exceed max length %d",
			maxRLSPrincipalMetadataBytes,
		))
	}
	return nil
}

func maxPrincipalTagsJSONLength(maxTags int) int64 {
	maxTagKeyLength := int64(paramtable.Get().ProxyCfg.RLSMaxTagKeyLength.GetAsInt())
	maxTagValueLength := int64(paramtable.Get().ProxyCfg.RLSMaxTagValueLength.GetAsInt())
	maxArrayElements := int64(paramtable.Get().ProxyCfg.RLSMaxArrayLiteralElements.GetAsInt())
	if maxTagKeyLength > math.MaxInt64/maxJSONEscapeBytesPerByte ||
		maxTagValueLength > (math.MaxInt64-2)/maxJSONEscapeBytesPerByte {
		return math.MaxInt64
	}

	maxKeyBytes := maxTagKeyLength * maxJSONEscapeBytesPerByte
	maxScalarBytes := max(maxTagValueLength*maxJSONEscapeBytesPerByte+2, int64(maxJSONNumberLength))
	if maxScalarBytes == math.MaxInt64 || maxArrayElements > (math.MaxInt64-2)/(maxScalarBytes+1) {
		return math.MaxInt64
	}
	// Array elements use the scalar bound plus one conservative comma each.
	maxValueBytes := max(maxScalarBytes, 2+maxArrayElements*(maxScalarBytes+1))
	// Two key quotes, one colon, and one conservative comma per member.
	if maxKeyBytes > math.MaxInt64-maxValueBytes-4 {
		return math.MaxInt64
	}
	maxMemberBytes := maxKeyBytes + maxValueBytes + 4
	if int64(maxTags) > (math.MaxInt64-2)/maxMemberBytes {
		return math.MaxInt64
	}
	return 2 + int64(maxTags)*maxMemberBytes // object braces plus members
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
func ValidateTags(tags map[string]TagValue) error {
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
		if err := validateTagValue(key, value); err != nil {
			return err
		}
	}
	return nil
}

func validateTagValue(key string, value TagValue) error {
	switch value.Kind {
	case TagValueKindString:
		maxTagValueLength := paramtable.Get().ProxyCfg.RLSMaxTagValueLength.GetAsInt()
		if len(value.StringValue) > maxTagValueLength {
			return merr.WrapErrParameterInvalidMsg("RLS principal tag value exceeds max length %d", maxTagValueLength)
		}
	case TagValueKindInt64:
	case TagValueKindDouble:
		if math.IsNaN(value.DoubleValue) || math.IsInf(value.DoubleValue, 0) {
			return merr.WrapErrParameterInvalidMsg("RLS principal tag %q has a non-finite double value", key)
		}
	case TagValueKindArray:
		if value.arrayValue == nil {
			return merr.WrapErrParameterInvalidMsg("RLS principal tag %q has an invalid array value", key)
		}
		maxElements := min(paramtable.Get().ProxyCfg.RLSMaxArrayLiteralElements.GetAsInt(), maxRLSArrayTagElements)
		if len(value.arrayValue) > maxElements {
			return merr.WrapErrParameterInvalidMsg("RLS principal tag %q exceeds max array elements %d", key, maxElements)
		}
		for _, element := range value.arrayValue {
			if element.Kind == TagValueKindArray {
				return merr.WrapErrParameterInvalidMsg("RLS principal tag %q does not support nested arrays", key)
			}
			if element.Kind != value.arrayValue[0].Kind {
				return merr.WrapErrParameterInvalidMsg("RLS principal tag %q array elements must have the same type", key)
			}
			if err := validateTagValue(key, element); err != nil {
				return err
			}
		}
	default:
		return merr.WrapErrParameterInvalidMsg("RLS principal tag %q has unsupported value type", key)
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
