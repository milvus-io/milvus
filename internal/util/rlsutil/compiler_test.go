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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestNewRowDataOnlyBuildsReferencedReaders(t *testing.T) {
	rows := newRowData(managerTestFieldsData("sales"), []int64{101})
	require.Len(t, rows.fields, 1)
	require.Contains(t, rows.fields, int64(101))
}

func TestCompilePolicyExprCachesTagVariableDataTypes(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
	}})
	require.NoError(t, err)

	for _, test := range []struct {
		name     string
		expr     string
		expected schemapb.DataType
	}{
		{name: "scalar", expr: "age == $current_principal_tags['value']", expected: schemapb.DataType_Int64},
		{name: "array element", expr: "array_contains(scores, $current_principal_tags['value'])", expected: schemapb.DataType_Float},
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
		})
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
