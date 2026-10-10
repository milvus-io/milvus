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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestNewRowDataOnlyBuildsReferencedReaders(t *testing.T) {
	rows := newRowData(managerTestFieldsData("sales"), []int64{101})
	require.Len(t, rows.fields, 1)
	require.Contains(t, rows.fields, int64(101))
}

func TestCompilePolicyExprCachesTagVariableTypes(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
	}})
	require.NoError(t, err)

	for _, test := range []struct {
		name         string
		expr         string
		expected     schemapb.DataType
		expectsArray bool
	}{
		{name: "scalar", expr: "age == $current_principal_tags['value']", expected: schemapb.DataType_Int64},
		{name: "array element", expr: "array_contains(scores, $current_principal_tags['value'])", expected: schemapb.DataType_Float},
		{name: "array values", expr: "array_contains_any(scores, $current_principal_tags['value'])", expected: schemapb.DataType_Float, expectsArray: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			templates, _ := preparePolicyExprTemplates([]*RowPolicy{{
				PolicyName: "typed",
				PolicyType: PolicyTypePermissive,
				Actions:    []PolicyAction{PolicyActionQuery},
				UsingExpr:  test.expr,
			}}, PolicyActionQuery, usingExpression)
			compiled, err := compileExprTemplates(helper, templates, "", usingExpression)
			require.NoError(t, err)
			require.Len(t, compiled.permissive, 1)

			policy := compiled.permissive[0]
			require.NotNil(t, policy.tagBinding)
			require.Equal(t, "value", policy.tagBinding.key)
			require.NotEmpty(t, policy.tagBinding.variable)
			require.Equal(t, []schemapb.DataType{test.expected}, policy.tagBinding.dataTypes)
			require.Equal(t, test.expectsArray, policy.tagBinding.expectsArray)
		})
	}
}

func TestInstantiateNotAndArrayTags(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
	}})
	require.NoError(t, err)
	fields := []*schemapb.FieldData{
		{
			FieldId: 100, FieldName: "age", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}}}},
		},
		{
			FieldId: 101, FieldName: "scores", Type: schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_Int64,
				Data:        []*schemapb.ScalarField{{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}},
			}}}},
		},
	}

	for _, test := range []struct {
		name     string
		expr     string
		tags     map[string]TagValue
		expected truthValue
	}{
		{name: "not equality match", expr: "not (age == $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewInt64TagValue(18)}, expected: truthFalse},
		{name: "not equality mismatch", expr: "not (age == $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewInt64TagValue(19)}, expected: truthTrue},
		{name: "not in list", expr: "not (age in [17, 19])", expected: truthTrue},
		{name: "array any with exact numeric conversion", expr: "array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": newArrayTagValueForTest(t, []TagValue{NewDoubleTagValue(2), NewInt64TagValue(4)})}, expected: truthTrue},
		{name: "not array any", expr: "not array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": newArrayTagValueForTest(t, []TagValue{NewInt64TagValue(4), NewInt64TagValue(5)})}, expected: truthTrue},
		{name: "missing tag under not stays false", expr: "not (age == $current_principal_tags['value'])", expected: truthFalse},
		{name: "scalar tag cannot fill array", expr: "array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewInt64TagValue(2)}, expected: truthFalse},
		{name: "lossy array element stays false", expr: "array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": newArrayTagValueForTest(t, []TagValue{NewDoubleTagValue(2.5)})}, expected: truthFalse},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled, err := CompileCheckExpression([]*RowPolicy{{
				PolicyName: "check",
				PolicyType: PolicyTypePermissive,
				Actions:    []PolicyAction{PolicyActionInsert},
				CheckExpr:  test.expr,
			}}, PolicyActionInsert, helper, 4096)
			require.NoError(t, err)
			for _, optimize := range []bool{false, true} {
				expr, err := compiled.instantiate("alice", test.tags, optimize)
				require.NoError(t, err)
				rows := newRowData(fields, ReferencedFieldIDs(expr))
				actual, err := evalExpr(expr, rows, 0)
				require.NoError(t, err)
				require.Equal(t, test.expected, actual, "optimize=%v", optimize)
			}
		})
	}
}

func TestInstantiateArrayTagsHasAggregateBudget(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
	}}})
	require.NoError(t, err)

	elements := make([]TagValue, 1024)
	for i := range elements {
		elements[i] = NewInt64TagValue(int64(i))
	}
	arrayTag := newArrayTagValueForTest(t, elements)
	arrayBytes, ok := normalizedRLSTagValueSize([]schemapb.DataType{schemapb.DataType_Int64}, true, arrayTag)
	require.True(t, ok)
	require.Equal(t, int64(1025*64), arrayBytes)
	policyCount := int(maxRLSPrincipalMetadataBytes/arrayBytes) + 1
	policies := make([]*RowPolicy, policyCount)
	for i := range policies {
		policies[i] = &RowPolicy{
			PolicyName: fmt.Sprintf("policy-%d", i),
			PolicyType: PolicyTypePermissive,
			Actions:    []PolicyAction{PolicyActionInsert},
			CheckExpr:  "array_contains_any(scores, $current_principal_tags['groups'])",
		}
	}

	compiled, err := CompileCheckExpression(policies[:1], PolicyActionInsert, helper, 4096)
	require.NoError(t, err)
	_, err = compiled.Instantiate("alice", map[string]TagValue{"groups": arrayTag})
	require.NoError(t, err)

	compiled, err = CompileCheckExpression(policies, PolicyActionInsert, helper, 4096)
	require.NoError(t, err)
	_, err = compiled.Instantiate("alice", map[string]TagValue{"groups": arrayTag})
	require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
}

func TestArrayTagTemplateNormalizationPreservesSnapshot(t *testing.T) {
	tag := newArrayTagValueForTest(t, []TagValue{NewDoubleTagValue(1), NewDoubleTagValue(2)})
	values, ok := rlsArrayTagElements([]schemapb.DataType{schemapb.DataType_Int64}, tag.arrayValue)
	require.True(t, ok)
	require.Len(t, values, 2)
	require.Equal(t, planparserv2.NewInt(1), values[0])
	require.Equal(t, planparserv2.NewInt(2), values[1])
	require.Equal(t, []float64{1, 2}, tag.arrayValue.doubles)

	values, ok = rlsArrayTagElements([]schemapb.DataType{schemapb.DataType_Double}, tag.arrayValue)
	require.True(t, ok)
	require.Equal(t, planparserv2.NewFloat(1), values[0])
	require.Equal(t, planparserv2.NewFloat(2), values[1])
}

func TestArrayTagDirectFillMatchesTemplateFill(t *testing.T) {
	for _, test := range []struct {
		name     string
		dataType schemapb.DataType
		payload  string
		values   []*planpb.GenericValue
	}{
		{"integer", schemapb.DataType_Int64, `[1.0,2.0]`, []*planpb.GenericValue{planparserv2.NewInt(1), planparserv2.NewInt(2)}},
		{"float", schemapb.DataType_Float, `[1,2]`, []*planpb.GenericValue{planparserv2.NewFloat(1), planparserv2.NewFloat(2)}},
		{"double", schemapb.DataType_Double, `[1,2.5]`, []*planpb.GenericValue{planparserv2.NewFloat(1), planparserv2.NewFloat(2.5)}},
		{"string", schemapb.DataType_VarChar, `["sales","ops"]`, []*planpb.GenericValue{planparserv2.NewString("sales"), planparserv2.NewString("ops")}},
	} {
		for _, op := range []string{"array_contains_any", "array_contains_all", "not array_contains_any", "not array_contains_all"} {
			for _, empty := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/empty=%v", test.name, op, empty), func(t *testing.T) {
					helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
						FieldID: 101, Name: "values", DataType: schemapb.DataType_Array, ElementType: test.dataType,
					}}})
					require.NoError(t, err)
					compiled, err := CompileCheckExpression([]*RowPolicy{{
						PolicyName: "check", PolicyType: PolicyTypePermissive, Actions: []PolicyAction{PolicyActionInsert},
						CheckExpr: op + "(values, $current_principal_tags['groups'])",
					}}, PolicyActionInsert, helper, 4096)
					require.NoError(t, err)
					policy := compiled.permissive[0]
					payload, values := test.payload, test.values
					if empty {
						payload, values = `[]`, nil
					}
					tags, err := TagsFromJSON(`{"groups":` + payload + `}`)
					require.NoError(t, err)
					expected := proto.Clone(policy.expr).(*planpb.Expr)
					require.NoError(t, planparserv2.FillExpressionValue(expected, map[string]*planpb.GenericValue{
						policy.tagBinding.variable: {Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{Array: values, SameType: true}}},
					}))
					before := proto.Clone(policy.expr)
					actual, err := policy.instantiate("alice", tags, nil)
					require.NoError(t, err)
					require.True(t, proto.Equal(expected, actual))
					require.True(t, proto.Equal(before, policy.expr), "compiled template must remain immutable")
				})
			}
		}
	}
}

func TestNestedNotIsRejected(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64,
	}}})
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, "not (not (age == 18))", nil)
	require.NoError(t, err)
	require.ErrorIs(t, ValidateParsedExpression(expr, nil), merr.ErrParameterInvalid)
}

func TestNotPreservesUnknownForNull(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64, Nullable: true,
	}}})
	require.NoError(t, err)
	compiled, err := CompileCheckExpression([]*RowPolicy{{
		PolicyName: "check",
		PolicyType: PolicyTypePermissive,
		Actions:    []PolicyAction{PolicyActionInsert},
		CheckExpr:  "not (age == 18)",
	}}, PolicyActionInsert, helper, 4096)
	require.NoError(t, err)
	fields := []*schemapb.FieldData{{
		FieldId: 100, FieldName: "age", Type: schemapb.DataType_Int64,
		ValidData: []bool{false},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}},
		}},
	}}
	for _, optimize := range []bool{false, true} {
		expr, err := compiled.instantiate("alice", nil, optimize)
		require.NoError(t, err)
		actual, err := evalExpr(expr, newRowData(fields, ReferencedFieldIDs(expr)), 0)
		require.NoError(t, err)
		require.Equal(t, truthUnknown, actual, "optimize=%v", optimize)
	}
}

func TestPreparePolicyExprTemplatesCombinedLength(t *testing.T) {
	tests := []struct {
		name     string
		policies []*RowPolicy
		expected string
	}{
		{name: "empty"},
		{
			name: "restrictive only",
			policies: []*RowPolicy{{
				PolicyType: PolicyTypeRestrictive,
				Actions:    []PolicyAction{PolicyActionQuery},
				UsingExpr:  "active == true",
			}},
			expected: "false",
		},
		{
			name: "permissive and restrictive groups",
			policies: []*RowPolicy{
				{PolicyType: PolicyTypePermissive, Actions: []PolicyAction{PolicyActionQuery}, UsingExpr: "a == 1"},
				{PolicyType: PolicyTypePermissive, Actions: []PolicyAction{PolicyActionQuery}, UsingExpr: "b == 2"},
				{PolicyType: PolicyTypeRestrictive, Actions: []PolicyAction{PolicyActionQuery}, UsingExpr: "c == 3"},
				{PolicyType: PolicyTypeRestrictive, Actions: []PolicyAction{PolicyActionQuery}, UsingExpr: "d == 4"},
			},
			expected: "((a == 1) or (b == 2)) and ((c == 3) and (d == 4))",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, combinedLength := preparePolicyExprTemplates(test.policies, PolicyActionQuery, usingExpression)
			require.Equal(t, len(test.expected), combinedLength)
		})
	}
}
