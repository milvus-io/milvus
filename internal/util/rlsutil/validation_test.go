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
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestValidatePayloadBounds(t *testing.T) {
	paramtable.Init()

	t.Run("policy action count", func(t *testing.T) {
		require.ErrorIs(t, ValidatePolicyActionCount(0), merr.ErrParameterInvalid)
		require.NoError(t, ValidatePolicyActionCount(maxSupportedPolicyActions))
		require.ErrorIs(t, ValidatePolicyActionCount(maxSupportedPolicyActions+1), merr.ErrParameterInvalid)

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

	t.Run("deprecated policy roles", func(t *testing.T) {
		require.NoError(t, ValidatePolicyRoles(nil))
		require.ErrorIs(t, ValidatePolicyRoles([]string{"reader"}), merr.ErrParameterInvalid)
	})

	t.Run("raw tag key transport count", func(t *testing.T) {
		_, err := ValidateAndDeduplicateTagKeys(make([]string, MaxTransportTagKeys+1))
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("raw principal tags transport bytes", func(t *testing.T) {
		params := &paramtable.Get().ProxyCfg
		require.NoError(t, paramtable.Get().Save(params.RLSMaxTagsPerPrincipal.Key, "1"))
		require.NoError(t, paramtable.Get().Save(params.RLSMaxTagKeyLength.Key, "1"))
		require.NoError(t, paramtable.Get().Save(params.RLSMaxTagValueLength.Key, "1"))
		require.NoError(t, paramtable.Get().Save(params.RLSMaxArrayLiteralElements.Key, "1"))
		require.NoError(t, paramtable.Get().Save(params.RLSMaxPrincipalCacheBytes.Key, "1"))
		defer func() {
			require.NoError(t, paramtable.Get().Reset(params.RLSMaxTagsPerPrincipal.Key))
			require.NoError(t, paramtable.Get().Reset(params.RLSMaxTagKeyLength.Key))
			require.NoError(t, paramtable.Get().Reset(params.RLSMaxTagValueLength.Key))
			require.NoError(t, paramtable.Get().Reset(params.RLSMaxArrayLiteralElements.Key))
			require.NoError(t, paramtable.Get().Reset(params.RLSMaxPrincipalCacheBytes.Key))
		}()

		maxPayloadBytes := maxPrincipalTagsJSONLength(1)
		_, err := TagsFromJSONWithLimit(`{"k":"`+strings.Repeat("x", int(maxPayloadBytes))+`"}`, 1)
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
		_, err = TagsFromJSONWithLimit(`{"k":`+strings.Repeat("1", int(maxPayloadBytes))+`}`, 1)
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
		tags, err := TagsFromJSONWithLimit(`{"k":`+strings.Repeat("1", maxJSONNumberLength+1)+`}`, 1)
		require.NoError(t, err)
		require.Equal(t, TagValueKindDouble, tags["k"].Kind)
		_, err = TagsFromJSONWithLimit(`{"kk":"x"}`, 1)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		_, err = TagsFromJSONWithLimit(`{"k":"xx"}`, 1)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)

		storedTags, err := TagsFromJSON(`{"kk":"xx"}`)
		require.NoError(t, err)
		require.Equal(t, NewStringTagValue("xx"), storedTags["kk"])
		_, err = TagsFromJSON(`{"k":` + strings.Repeat("1", maxJSONNumberLength+1) + `}`)
		require.NoError(t, err)

		tags, err = TagsFromJSONWithLimit(`{"k":-9223372036854775808}`, 1)
		require.NoError(t, err)
		require.Equal(t, NewInt64TagValue(math.MinInt64), tags["k"])
		tags, err = TagsFromJSONWithLimit(`{"k":-1.7976931348623157e+308}`, 1)
		require.NoError(t, err)
		require.Equal(t, NewDoubleTagValue(-math.MaxFloat64), tags["k"])
	})

	t.Run("absolute principal tags transport bytes", func(t *testing.T) {
		_, err := TagsFromJSONWithLimit(
			strings.Repeat(" ", int(maxRLSPrincipalMetadataBytes)+1),
			paramtable.Get().ProxyCfg.RLSMaxTagsPerPrincipal.GetAsInt(),
		)
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

	t.Run("stored policies ignore refreshable expression limits", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxExpressionLength.Key, "1")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxExpressionLength.Key)

		err := ValidatePolicyForUpdate(
			"policy",
			PolicyTypePermissive,
			[]PolicyAction{PolicyActionQuery},
			"true",
			"",
		)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.NoError(t, ValidateStoredPolicy(
			"policy",
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
		err = ValidateTags(map[string]TagValue{"new-key": NewStringTagValue("value")})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("quoted tag keys are rejected", func(t *testing.T) {
		require.ErrorIs(t, ValidateTagKey("x'y"), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"x'y": NewStringTagValue("value")}), merr.ErrParameterInvalid)
	})

	t.Run("typed tag values", func(t *testing.T) {
		require.NoError(t, ValidateTags(map[string]TagValue{
			"string": NewStringTagValue("value"),
			"int":    NewInt64TagValue(3),
			"double": NewDoubleTagValue(0.75),
			"array":  NewArrayTagValue([]TagValue{NewStringTagValue("one"), NewStringTagValue("two")}),
		}))
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"double": NewDoubleTagValue(math.NaN())}), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"double": NewDoubleTagValue(math.Inf(1))}), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"array": NewArrayTagValue([]TagValue{NewDoubleTagValue(math.NaN())})}), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"array": {Kind: TagValueKindArray}}), merr.ErrParameterInvalid)
	})

	t.Run("principal tag logical size", func(t *testing.T) {
		tags := map[string]TagValue{
			"s": NewStringTagValue("abc"),
			"i": NewInt64TagValue(1),
			"d": NewDoubleTagValue(1.5),
			"a": NewArrayTagValue([]TagValue{NewStringTagValue(""), NewStringTagValue("")}),
		}
		size, err := PrincipalTagsSize("alice", tags)
		require.NoError(t, err)
		require.Equal(t, int64(len("alice")+len("s")+len("abc")+len("i")+len("d")+
			len("a"))+6*tagValueRetainedSize, size)

		_, err = PrincipalTagsSize("alice", map[string]TagValue{"unsupported": {Kind: TagValueKindUnknown}})
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("principal tag metadata bytes", func(t *testing.T) {
		require.NoError(t, ValidatePrincipalTagsRecordSize("alice", map[string]TagValue{
			"tenant": NewStringTagValue("acme"),
		}))
		err := ValidatePrincipalTagsRecordSize("alice", map[string]TagValue{
			"tenant": NewStringTagValue(strings.Repeat("x", int(maxRLSPrincipalMetadataBytes))),
		})
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	})

	t.Run("JSON tag payload", func(t *testing.T) {
		tags, err := TagsFromJSON(`{"tenant":"acme","level":3,"score":0.75,"groups":["sales","ops"]}`)
		require.NoError(t, err)
		require.Equal(t, NewStringTagValue("acme"), tags["tenant"])
		require.Equal(t, NewInt64TagValue(3), tags["level"])
		require.Equal(t, NewDoubleTagValue(0.75), tags["score"])
		require.Equal(t, NewArrayTagValue([]TagValue{NewStringTagValue("sales"), NewStringTagValue("ops")}), tags["groups"])
		payload, err := TagsToJSON(tags)
		require.NoError(t, err)
		require.JSONEq(t, `{"tenant":"acme","level":3,"score":0.75,"groups":["sales","ops"]}`, payload)
		for _, value := range []TagValue{
			NewDoubleTagValue(3),
			NewDoubleTagValue(9223372036854774784),
		} {
			payload, err := TagsToJSON(map[string]TagValue{"value": value})
			require.NoError(t, err)
			roundTrip, err := TagsFromJSON(payload)
			require.NoError(t, err)
			require.Equal(t, value, roundTrip["value"])
		}
		largeDoublePayload, err := TagsToJSON(map[string]TagValue{"value": NewDoubleTagValue(1e20)})
		require.NoError(t, err)
		largeDoubleTags, err := TagsFromJSON(largeDoublePayload)
		require.NoError(t, err)
		require.Equal(t, NewDoubleTagValue(1e20), largeDoubleTags["value"])
		for _, invalid := range []string{`[]`, `{"nested":{"x":1}}`, `{"nested":[[1]]}`, `{"flag":true}`, `{"flags":[true]}`, `{"none":[null]}`, `{"x":1} trailing`} {
			_, err = TagsFromJSON(invalid)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		}

		source := []TagValue{NewStringTagValue("original")}
		arrayTag := NewArrayTagValue(source)
		source[0] = NewStringTagValue("mutated")
		returned := arrayTag.ArrayValues()
		returned[0] = NewStringTagValue("also-mutated")
		require.Equal(t, []TagValue{NewStringTagValue("original")}, arrayTag.ArrayValues())

		numbers := []TagValue{NewInt64TagValue(1), NewDoubleTagValue(2)}
		arrayTag = NewArrayTagValue(numbers)
		require.Equal(t, []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2)}, arrayTag.ArrayValues())
		require.Equal(t, NewInt64TagValue(1), numbers[0])
	})

	t.Run("bounded JSON tag payload", func(t *testing.T) {
		tags, err := TagsFromJSONWithLimit(`{"tenant":"acme"}`, 1)
		require.NoError(t, err)
		require.Equal(t, map[string]TagValue{"tenant": NewStringTagValue("acme")}, tags)

		_, err = TagsFromJSONWithLimit(`{"tenant":"acme","level":3}`, 1)
		require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
		_, err = TagsFromJSONWithLimit(`{"tenant":"acme","tenant":"other"}`, 1)
		require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)

		oldLimit := paramtable.Get().ProxyCfg.RLSMaxArrayLiteralElements.SwapTempValue("1")
		defer paramtable.Get().ProxyCfg.RLSMaxArrayLiteralElements.SwapTempValue(oldLimit)
		_, err = TagsFromJSONWithLimit(`{"groups":["one","two"]}`, 1)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
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

func TestArrayTagElementTypes(t *testing.T) {
	paramtable.Init()
	for _, test := range []struct {
		name    string
		payload string
		values  []TagValue
		valid   bool
	}{
		{name: "empty", payload: `[]`, valid: true},
		{name: "strings", payload: `["sales","ops"]`, values: []TagValue{NewStringTagValue("sales"), NewStringTagValue("ops")}, valid: true},
		{name: "integers", payload: `[1,2]`, values: []TagValue{NewInt64TagValue(1), NewInt64TagValue(2)}, valid: true},
		{name: "doubles", payload: `[1.0,2e0]`, values: []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2)}, valid: true},
		{name: "string then integer", payload: `["sales",1]`, values: []TagValue{NewStringTagValue("sales"), NewInt64TagValue(1)}},
		{name: "integer then string", payload: `[1,"sales"]`, values: []TagValue{NewInt64TagValue(1), NewStringTagValue("sales")}},
		{name: "string then double", payload: `["sales",1.0]`, values: []TagValue{NewStringTagValue("sales"), NewDoubleTagValue(1)}},
		{name: "double then string", payload: `[1.0,"sales"]`, values: []TagValue{NewDoubleTagValue(1), NewStringTagValue("sales")}},
		{name: "integer then double", payload: `[1,2.0]`, values: []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2)}, valid: true},
		{name: "double then integer", payload: `[1.0,2]`, values: []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2)}, valid: true},
		{name: "exponent promotes integers", payload: `[1,2e0]`, values: []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2)}, valid: true},
		{name: "fractional number promotes integers", payload: `[1,2.5]`, values: []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2.5)}, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			tags := map[string]TagValue{"value": NewArrayTagValue(test.values)}
			for _, maxTags := range []int{0, 1} {
				decoded, err := TagsFromJSONWithLimit(`{"value":`+test.payload+`}`, maxTags)
				if test.valid {
					require.NoError(t, err)
					require.Equal(t, tags, decoded)
				} else {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					require.Nil(t, decoded)
				}
			}

			validationErr := ValidateTags(tags)
			encoded, encodeErr := TagsToJSON(tags)
			if !test.valid {
				require.ErrorIs(t, validationErr, merr.ErrParameterInvalid)
				require.ErrorIs(t, encodeErr, merr.ErrServiceInternal)
				return
			}
			require.NoError(t, validationErr)
			require.NoError(t, encodeErr)
			roundTrip, err := TagsFromJSON(encoded)
			require.NoError(t, err)
			require.Equal(t, tags, roundTrip)
		})
	}
}
