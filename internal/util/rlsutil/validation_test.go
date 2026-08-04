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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidatePayloadBounds(t *testing.T) {
	paramtable.Init()

	t.Run("policy action count", func(t *testing.T) {
		actions := make([]PolicyAction, maxSupportedPolicyActions+1)
		err := ValidatePolicy(
			"policy",
			PolicyTypePermissive,
			actions,
			"true",
			"",
		)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("raw tag key transport count", func(t *testing.T) {
		_, err := ValidateAndDeduplicateTagKeys(make([]string, MaxTransportTagKeys+1))
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("distinct tag key semantic count", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxTagsPerPrincipal.Key, "1")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxTagsPerPrincipal.Key)

		keys, err := ValidateAndDeduplicateTagKeys([]string{"key", "key"})
		require.NoError(t, err)
		require.Equal(t, []string{"key"}, keys)

		_, err = ValidateAndDeduplicateTagKeys([]string{"key1", "key2"})
		require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
	})

	t.Run("bounded creation names", func(t *testing.T) {
		maxPolicyNameLength := paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.GetAsInt()
		err := ValidatePolicyNameWithLimit(strings.Repeat("p", maxPolicyNameLength+1))
		require.ErrorIs(t, err, merr.ErrParameterInvalid)

		maxPrincipalNameLength := paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.GetAsInt()
		err = ValidatePrincipalNameWithLimit(strings.Repeat("p", maxPrincipalNameLength+1))
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("existing policy names remain updatable", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.Key, "1")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxPolicyNameLength.Key)

		err := ValidatePolicy(
			"existing-policy",
			PolicyTypePermissive,
			[]PolicyAction{PolicyActionQuery},
			"true",
			"",
		)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.NoError(t, ValidatePolicyForUpdate(
			"existing-policy",
			PolicyTypePermissive,
			[]PolicyAction{PolicyActionQuery},
			"true",
			"",
		))
	})

	t.Run("unused policy expressions are rejected", func(t *testing.T) {
		for _, test := range []struct {
			name      string
			actions   []PolicyAction
			usingExpr string
			checkExpr string
			unused    string
		}{
			{
				name:      "check expression for query",
				actions:   []PolicyAction{PolicyActionQuery},
				usingExpr: "true",
				checkExpr: "true",
				unused:    "check_expr is not used",
			},
			{
				name:      "using expression for insert",
				actions:   []PolicyAction{PolicyActionInsert},
				usingExpr: "true",
				checkExpr: "true",
				unused:    "using_expr is not used",
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				for _, validate := range []func(string, PolicyType, []PolicyAction, string, string) error{
					ValidatePolicy,
					ValidatePolicyForUpdate,
				} {
					err := validate("policy", PolicyTypePermissive, test.actions, test.usingExpr, test.checkExpr)
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					require.Contains(t, err.Error(), test.unused)
				}
			})
		}
	})

	t.Run("existing tag keys remain addressable", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxTagKeyLength.Key, "1")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxTagKeyLength.Key)

		_, err := ValidateAndDeduplicateTagKeys([]string{"existing-key"})
		require.NoError(t, err)
		err = ValidateTags(map[string]string{"new-key": "value"})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("quoted tag keys are rejected", func(t *testing.T) {
		require.ErrorIs(t, ValidateTagKey("x'y"), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]string{"x'y": "value"}), merr.ErrParameterInvalid)
	})

	t.Run("transport identifier bounds", func(t *testing.T) {
		oversized := strings.Repeat("x", MaxTransportIdentifierLength+1)
		require.ErrorIs(t, ValidatePolicyName(oversized), merr.ErrParameterTooLarge)
		require.ErrorIs(t, ValidatePrincipalName(oversized), merr.ErrParameterTooLarge)
		require.ErrorIs(t, ValidateTagKey(oversized), merr.ErrParameterTooLarge)
		require.ErrorIs(t, ValidateRequestTarget(oversized, "collection"), merr.ErrParameterTooLarge)
		require.ErrorIs(t, ValidateRequestTarget("database", oversized), merr.ErrParameterTooLarge)
	})
}
