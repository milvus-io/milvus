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
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newArrayTagValueForTest(t testing.TB, values []TagValue) TagValue {
	t.Helper()
	value, err := arrayTagValueForTest(values)
	require.NoError(t, err)
	return value
}

func arrayTagValueForTest(values []TagValue) (TagValue, error) {
	array := &tagArray{}
	for _, value := range values {
		if err := array.appendDecoded(value); err != nil {
			return TagValue{}, err
		}
	}
	return TagValue{Kind: TagValueKindArray, arrayValue: array}, nil
}

func TestPolicyEnumCompatibility(t *testing.T) {
	paramtable.Init()
	for _, policyType := range []PolicyType{PolicyTypeUnknown, -1, 100} {
		err := ValidateStoredPolicy("policy", policyType, []PolicyAction{PolicyActionQuery}, "true", "")
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.Contains(t, err.Error(), "invalid RLS policy type: RowPolicyTypeUnknown")
	}
	for _, action := range []PolicyAction{-1, 100} {
		err := ValidateStoredPolicy("policy", PolicyTypePermissive, []PolicyAction{action}, "true", "")
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.Contains(t, err.Error(), "invalid RLS policy action: Unknown")
		require.Equal(t, "unknown", PolicyActionOperation(action))
	}
	for action, operation := range map[PolicyAction]string{
		PolicyActionQuery: "query", PolicyActionSearch: "search", PolicyActionInsert: "insert",
		PolicyActionDelete: "delete", PolicyActionUpsert: "upsert", PolicyActionQueryIterator: "query iterator",
		PolicyActionSearchIterator: "search iterator", PolicyActionHybridSearch: "hybrid search",
	} {
		require.Equal(t, operation, PolicyActionOperation(action))
	}
}

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
		tags, err = TagsFromJSONWithLimit(`{"k":"xx"}`, 1)
		require.NoError(t, err)
		require.ErrorIs(t, ValidateTags(tags), merr.ErrParameterInvalid)

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
			"array":  newArrayTagValueForTest(t, []TagValue{NewStringTagValue("one"), NewStringTagValue("two")}),
		}))
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"double": NewDoubleTagValue(math.NaN())}), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"double": NewDoubleTagValue(math.Inf(1))}), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"array": newArrayTagValueForTest(t, []TagValue{NewDoubleTagValue(math.NaN())})}), merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidateTags(map[string]TagValue{"array": {Kind: TagValueKindArray}}), merr.ErrParameterInvalid)
	})

	t.Run("principal tag logical size", func(t *testing.T) {
		tags := map[string]TagValue{
			"s": NewStringTagValue("abc"),
			"i": NewInt64TagValue(1),
			"d": NewDoubleTagValue(1.5),
			"a": newArrayTagValueForTest(t, []TagValue{NewStringTagValue(""), NewStringTagValue("")}),
		}
		size, err := PrincipalTagsSize("alice", tags)
		require.NoError(t, err)
		require.Equal(t, int64(len("alice")+len("s")+len("abc")+len("i")+len("d")+
			len("a"))+4*tagValueRetainedSize+tagArrayRetainedSize+2*int64(unsafe.Sizeof("")), size)

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
		require.Equal(t, newArrayTagValueForTest(t, []TagValue{NewStringTagValue("sales"), NewStringTagValue("ops")}), tags["groups"])
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
		arrayTag := newArrayTagValueForTest(t, source)
		source[0] = NewStringTagValue("mutated")
		require.Equal(t, []string{"original"}, arrayTag.arrayValue.strings)

		numbers := []TagValue{NewInt64TagValue(1), NewDoubleTagValue(2)}
		arrayTag = newArrayTagValueForTest(t, numbers)
		require.Equal(t, []float64{1, 2}, arrayTag.arrayValue.doubles)
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

func TestDecodeArrayTagsUsesOnlyFinalTypedBuffer(t *testing.T) {
	for _, payload := range []string{`[1,2,3.5,4,5.0]`, `[3.5,1,2,4,5.0]`} {
		tags, err := TagsFromJSON(`{"groups":` + payload + `}`)
		require.NoError(t, err)
		array := tags["groups"].arrayValue
		require.Equal(t, TagValueKindDouble, array.kind)
		require.Nil(t, array.integers)
		require.Nil(t, array.strings)
		require.Len(t, array.doubles, 5)
		require.ElementsMatch(t, []float64{1, 2, 3.5, 4, 5}, array.doubles)
	}
	for _, payload := range []string{`[1,9007199254740993,2.0]`, `[1.0,2,9007199254740993]`} {
		tags, err := TagsFromJSON(`{"groups":` + payload + `}`)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.Nil(t, tags)
	}
}

func TestCompactArrayTagRetainedSize(t *testing.T) {
	for _, test := range []struct {
		name         string
		value        TagValue
		elementBytes int64
	}{
		{"integer", NewInt64TagValue(7), 8},
		{"double", NewDoubleTagValue(1.5), 8},
		{"string", NewStringTagValue("abc"), int64(unsafe.Sizeof(""))},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := make([]TagValue, 1024)
			for i := range values {
				values[i] = test.value
			}
			array := newArrayTagValueForTest(t, values)
			size, err := PrincipalTagsSize("alice", map[string]TagValue{"groups": array})
			require.NoError(t, err)
			capacity := cap(array.arrayValue.integers) + cap(array.arrayValue.doubles) + cap(array.arrayValue.strings)
			backingBytes := int64(capacity)*test.elementBytes + 1024*int64(len(test.value.StringValue))
			require.Equal(t, int64(len("alicegroups"))+tagValueRetainedSize+tagArrayRetainedSize+backingBytes, size)
			values[0] = NewStringTagValue("changed")
			require.Equal(t, test.value, array.arrayValue.at(0))
			encoded, err := TagsToJSON(map[string]TagValue{"groups": array})
			require.NoError(t, err)
			decoded, err := TagsFromJSON(encoded)
			require.NoError(t, err)
			require.Equal(t, array, decoded["groups"])
		})
	}
	// Compact retention must not silently increase template expansion limits.
	require.Equal(t, 16383, maxRLSArrayTagElements)
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
			array, arrayErr := arrayTagValueForTest(test.values)
			tags := map[string]TagValue{"value": array}
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

			if !test.valid {
				require.ErrorIs(t, arrayErr, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, arrayErr)
			validationErr := ValidateTags(tags)
			encoded, encodeErr := TagsToJSON(tags)
			require.NoError(t, validationErr)
			require.NoError(t, encodeErr)
			roundTrip, err := TagsFromJSON(encoded)
			require.NoError(t, err)
			require.Equal(t, tags, roundTrip)
		})
	}
}

func TestArrayTagNumericPromotionPrecision(t *testing.T) {
	paramtable.Init()
	for _, test := range []struct {
		integer int64
		exact   bool
	}{
		{integer: 1<<53 - 1, exact: true},
		{integer: 1 << 53, exact: true},
		{integer: 1<<53 + 1},
		{integer: 1<<53 + 2, exact: true},
		{integer: -(1<<53 + 1)},
		{integer: -(1<<53 + 2), exact: true},
		{integer: math.MaxInt64},
		{integer: math.MaxInt64 - 1023, exact: true},
		{integer: math.MinInt64, exact: true},
		{integer: math.MinInt64 + 1},
	} {
		t.Run(strconv.FormatInt(test.integer, 10), func(t *testing.T) {
			integer := NewInt64TagValue(test.integer)
			// Pure integer arrays never require promotion.
			array, err := arrayTagValueForTest([]TagValue{integer})
			require.NoError(t, err)
			require.Equal(t, []int64{test.integer}, array.arrayValue.integers)
			payload, err := TagsToJSON(map[string]TagValue{"value": array})
			require.NoError(t, err)
			roundTrip, err := TagsFromJSON(payload)
			require.NoError(t, err)
			require.Equal(t, array, roundTrip["value"])

			for _, doubleFirst := range []bool{false, true} {
				values := []TagValue{integer, NewDoubleTagValue(1)}
				tokens := []string{strconv.FormatInt(test.integer, 10), "1.0"}
				if doubleFirst {
					values[0], values[1] = values[1], values[0]
					tokens[0], tokens[1] = tokens[1], tokens[0]
				}
				array, err := arrayTagValueForTest(values)
				if test.exact {
					require.NoError(t, err)
					for i, source := range values {
						require.Equal(t, TagValueKindDouble, array.arrayValue.kind)
						if source.Kind == TagValueKindInt64 {
							require.Equal(t, test.integer, int64(array.arrayValue.doubles[i]))
						}
					}
				} else {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					require.Equal(t, TagValue{}, array)
				}
				// Neither successful nor failed promotion changes caller-owned input.
				if doubleFirst {
					require.Equal(t, integer, values[1])
				} else {
					require.Equal(t, integer, values[0])
				}
				for _, maxTags := range []int{0, 1} {
					decoded, err := TagsFromJSONWithLimit(`{"value":[`+strings.Join(tokens, ",")+`]}`, maxTags)
					if test.exact {
						require.NoError(t, err)
						require.Equal(t, array, decoded["value"])
					} else {
						require.ErrorIs(t, err, merr.ErrParameterInvalid)
						require.Nil(t, decoded)
					}
				}
			}
		})
	}
}

func TestStoredArrayTagStructuralBounds(t *testing.T) {
	paramtable.Init()
	limit := &paramtable.Get().ProxyCfg.RLSMaxArrayLiteralElements
	previous := limit.SwapTempValue("1")
	defer limit.SwapTempValue(previous)

	// Refreshable admission limits must not make existing metadata unreadable.
	stored, err := TagsFromJSON(`{"groups":[1,2]}`)
	require.NoError(t, err)
	require.Equal(t, 2, stored["groups"].arrayValue.len())
	_, err = TagsFromJSONWithLimit(`{"groups":[1,2]}`, 1)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	stringLimit := &paramtable.Get().ProxyCfg.RLSMaxTagValueLength
	previousStringLimit := stringLimit.SwapTempValue("1")
	defer stringLimit.SwapTempValue(previousStringLimit)
	stored, err = TagsFromJSON(`{"groups":["existing"]}`)
	require.NoError(t, err)
	require.Equal(t, "existing", stored["groups"].arrayValue.strings[0])
	decoded, err := TagsFromJSONWithLimit(`{"groups":["existing"]}`, 1)
	require.NoError(t, err)
	require.ErrorIs(t, ValidateTags(decoded), merr.ErrParameterInvalid)

	// Fixed bounds still apply before a compact JSON array expands in memory.
	limit.SwapTempValue(strconv.Itoa(maxRLSArrayTagElements + 1))
	payload := `{"groups":[` + strings.Repeat("0,", maxRLSArrayTagElements-1) + `0]}`
	_, err = TagsFromJSON(payload)
	require.NoError(t, err)
	payload = `{"groups":[` + strings.Repeat("0,", maxRLSArrayTagElements) + `0]}`
	_, err = TagsFromJSON(payload)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = TagsFromJSONWithLimit(payload, 1)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	tooMany := make([]TagValue, maxRLSArrayTagElements+1)
	for i := range tooMany {
		tooMany[i] = NewInt64TagValue(0)
	}
	require.ErrorIs(t, ValidateTags(map[string]TagValue{"groups": newArrayTagValueForTest(t, tooMany)}), merr.ErrParameterInvalid)

	_, err = TagsFromJSON(`{}` + strings.Repeat(" ", int(maxRLSPrincipalMetadataBytes)-2))
	require.NoError(t, err)
	_, err = TagsFromJSON(`{}` + strings.Repeat(" ", int(maxRLSPrincipalMetadataBytes)-1))
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}
