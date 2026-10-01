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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
			variable := policy.tagVariables["value"]
			require.Equal(t, []schemapb.DataType{test.expected}, policy.tagVariableDataTypes[variable])
			require.Equal(t, test.expectsArray, policy.tagVariableArrays[variable])
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
		{name: "array any", expr: "array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewArrayTagValue([]TagValue{NewDoubleTagValue(2), NewInt64TagValue(4)})}, expected: truthTrue},
		{name: "not array any", expr: "not array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewArrayTagValue([]TagValue{NewInt64TagValue(4), NewInt64TagValue(5)})}, expected: truthTrue},
		{name: "missing tag under not stays false", expr: "not (age == $current_principal_tags['value'])", expected: truthFalse},
		{name: "scalar tag cannot fill array", expr: "array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewInt64TagValue(2)}, expected: truthFalse},
		{name: "lossy array element stays false", expr: "array_contains_any(scores, $current_principal_tags['value'])", tags: map[string]TagValue{"value": NewArrayTagValue([]TagValue{NewDoubleTagValue(2.5)})}, expected: truthFalse},
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
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			ValidData: []bool{false},
			Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}},
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
