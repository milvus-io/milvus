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

package rls

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newManagerWithAlice() *manager {
	return newManagerWithPrincipal(100, "alice")
}

func newManagerWithPrincipal(collectionID UniqueID, principalName string) *manager {
	m := newManager()
	if err := m.init(context.Background(), &metadataTestCoord{}); err != nil {
		panic("failed to initialize manager test dependencies")
	}
	if !setManagerTestPrincipalTags(m, collectionID, principalName, nil) {
		panic("failed to initialize manager test principal")
	}
	return m
}

func validateRows(ctx context.Context, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, expr string, operation string, exprKind string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" || rowNum == 0 {
		return nil
	}
	parsedExpr, err := planparserv2.ParseExpr(schemaHelper, expr, nil)
	if err != nil {
		return merr.Wrapf(err, "failed to parse RLS %s expression for %s", exprKind, operation)
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, parsedExpr, operation, exprKind)
}

func TestReferencedFieldIDs(t *testing.T) {
	helper := newManagerTestPrincipalSchemaHelper(t)
	expr, err := planparserv2.ParseExpr(helper, `dept in ["sales", "support"] and owner == "alice"`, nil)
	require.NoError(t, err)
	assert.Equal(t, []int64{101, 102}, ReferencedFieldIDs(expr))
}

func TestNewRowDataOnlyBuildsReferencedReaders(t *testing.T) {
	rows := newRowData(managerTestFieldsDataWithID(1, "sales"), []int64{101})
	require.Len(t, rows.fields, 1)
	require.Contains(t, rows.fields, int64(101))
}

func TestManagerRejectsInvalidPredicateStateAsInternal(t *testing.T) {
	helper := newManagerTestSchemaHelper(t)
	var nilManager *manager

	_, err := nilManager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	_, err = newManager().resolveUsingPredicate(context.Background(), 0, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestManagerPolicyCombination(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "engineering"`,
			},
			{
				PolicyName: "sales",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
			{
				PolicyName: "existing_ids",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `id in [1, 2]`,
			},
			{
				PolicyName: "search_only",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionSearch},
				UsingExpr:  `dept == "ignored"`,
			},
		},
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(1, "sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(2, "engineering"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(3, "sales"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsDataWithID(1, "product"), 1, expr, "query", "using"))

	_, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionDelete, helper)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestManagerRestrictiveOnlyIsFalse(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	coord := &metadataTestCoord{principalErr: merr.WrapErrServiceUnavailableMsg("principal metadata unavailable")}
	require.NoError(t, manager.init(ctx, coord))
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "restrictive_only",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == $current_principal_tags['dept']`,
			},
		},
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
	require.Zero(t, coord.principalCalls.Load())
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerPolicyTagsAndPrincipal(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestPrincipalSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
			{
				PolicyName: "owner",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "owner == $current_principal",
			},
			{
				PolicyName: "region",
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "region == $current_principal_tags['region']",
			},
		},
	}))
	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{
		"dept":   rlsutil.NewStringTagValue("sales"),
		"region": rlsutil.NewStringTagValue("us"),
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "alice", "us"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "bob", "us"), 1, expr, "query", "using"))

	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{
		"dept": rlsutil.NewStringTagValue("sales"),
	}))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "alice", "us"), 1, expr, "query", "using"))
}

func TestManagerMissingTagOnlyDeniesReferencingPolicy(t *testing.T) {
	oldOptimize := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue("false")
	defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(oldOptimize)

	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "principal_dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
			{
				PolicyName: "public_dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "public"`,
			},
		},
	}))
	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", nil))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	_, combined := expr.GetExpr().(*planpb.Expr_BinaryExpr)
	require.False(t, combined)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("public"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerMissingTagsShortCircuitPolicyGroups(t *testing.T) {
	oldOptimize := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue("false")
	defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(oldOptimize)

	policy := func(name string, policyType rlsutil.PolicyType, expr string) *rlsutil.RowPolicy {
		return &rlsutil.RowPolicy{
			PolicyName: name,
			PolicyType: policyType,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  expr,
		}
	}
	tests := []struct {
		name     string
		policies []*rlsutil.RowPolicy
	}{
		{
			name: "all permissive policies are false",
			policies: []*rlsutil.RowPolicy{
				policy("dept", rlsutil.PolicyTypePermissive, "dept == $current_principal_tags['dept']"),
				policy("team", rlsutil.PolicyTypePermissive, "dept == $current_principal_tags['team']"),
			},
		},
		{
			name: "restrictive false dominates",
			policies: []*rlsutil.RowPolicy{
				policy("public", rlsutil.PolicyTypePermissive, `dept == "public"`),
				policy("region", rlsutil.PolicyTypeRestrictive, "dept == $current_principal_tags['region']"),
				policy("sales", rlsutil.PolicyTypeRestrictive, `dept == "sales"`),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := newManagerWithAlice()
			require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: test.policies}))

			expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, newManagerTestSchemaHelper(t))
			require.NoError(t, err)
			require.True(t, rewriter.IsAlwaysFalseExpr(expr))
		})
	}
}

func TestManagerTaglessPrincipalDoesNotLoadTags(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	coord := &metadataTestCoord{principalTags: map[string]map[string]string{}}
	require.NoError(t, manager.init(ctx, coord))
	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{{
			PolicyName: "owner",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  "owner == $current_principal",
		}},
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, newManagerTestPrincipalSchemaHelper(t))
	require.NoError(t, err)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestPrincipalFieldsData("sales", "alice", "us"), 1, expr, "query", "using"))
	require.Zero(t, coord.principalCalls.Load())
}

func TestManagerTypedPrincipalTagMatching(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	testCases := []struct {
		name       string
		field      string
		tag        rlsutil.TagValue
		fieldsData []*schemapb.FieldData
		allowed    bool
	}{
		{name: "matching string", field: "dept", tag: rlsutil.NewStringTagValue("sales"), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0.75), allowed: true},
		{name: "matching int", field: "age", tag: rlsutil.NewInt64TagValue(18), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0.75), allowed: true},
		{name: "matching double", field: "score", tag: rlsutil.NewDoubleTagValue(0.75), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0.75), allowed: true},
		{name: "string mismatch", field: "age", tag: rlsutil.NewStringTagValue("18"), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0.75)},
		{name: "integral double on integer", field: "age", tag: rlsutil.NewDoubleTagValue(18), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0.75), allowed: true},
		{name: "int on floating", field: "score", tag: rlsutil.NewInt64TagValue(0), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0), allowed: true},
		{name: "fractional double on integer", field: "age", tag: rlsutil.NewDoubleTagValue(18.5), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 0.75)},
		{name: "large int loses floating precision", field: "score", tag: rlsutil.NewInt64TagValue(9007199254740993), fieldsData: managerTestFieldsDataWithAgeAndScore("sales", 18, 9007199254740992)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
				Policies: []*rlsutil.RowPolicy{{
					PolicyName: "typed",
					PolicyType: rlsutil.PolicyTypePermissive,
					Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
					UsingExpr:  testCase.field + " == $current_principal_tags['value']",
				}},
			}))
			require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{"value": testCase.tag}))
			expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
			require.NoError(t, err)
			require.NotNil(t, expr)
			err = ValidateRowsByPredicate(ctx, testCase.fieldsData, 1, expr, "query", "using")
			if testCase.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
			}
		})
	}
}

func TestManagerRejectsInexactDoubleToFloatTag(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{
		Name: "rls_float_tag_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "score", DataType: schemapb.DataType_Float},
			{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	for _, usingExpr := range []string{
		"score == $current_principal_tags['value']",
		"array_contains(scores, $current_principal_tags['value'])",
	} {
		t.Run(usingExpr, func(t *testing.T) {
			manager := newManagerWithAlice()
			require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
				Policies: []*rlsutil.RowPolicy{{
					PolicyName: "typed",
					PolicyType: rlsutil.PolicyTypePermissive,
					Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
					UsingExpr:  usingExpr,
				}},
			}))
			require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{
				"value": rlsutil.NewDoubleTagValue(16777217),
			}))

			expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
			require.NoError(t, err)
			require.True(t, rewriter.IsAlwaysFalseExpr(expr))
		})
	}
}

func TestManagerRejectsOutOfRangeIntegerTag(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "level", DataType: schemapb.DataType_Int8},
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	manager := newManagerWithAlice()
	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
		PolicyName: "narrow_integer",
		PolicyType: rlsutil.PolicyTypePermissive,
		Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
		UsingExpr:  "level == $current_principal_tags['level']",
	}}}))
	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{
		"level": rlsutil.NewInt64TagValue(256),
	}))

	expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.True(t, rewriter.IsAlwaysFalseExpr(expr))
}

func TestManagerRejectsPersistedPoliciesOutsideRuntimeContract(t *testing.T) {
	t.Run("deprecated string field", func(t *testing.T) {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID:  100,
			Name:     "legacy",
			DataType: schemapb.DataType_String,
		}}})
		require.NoError(t, err)
		manager := newManagerWithAlice()
		require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
			PolicyName: "legacy_string",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
			CheckExpr:  `legacy == "value"`,
		}}}))

		expr, err := manager.resolveCheckPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionInsert, helper)
		require.Nil(t, expr)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("unsupported operator", func(t *testing.T) {
		manager := newManagerWithAlice()
		require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
			PolicyName: "not_equal",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  `dept != "blocked"`,
		}}}))

		expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, newManagerTestSchemaHelper(t))
		require.Nil(t, expr)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		state := manager.getCollectionState(100)
		state.mu.RLock()
		entry := state.compiled[compiledKey{action: rlsutil.PolicyActionQuery, kind: usingExprKind}]
		state.mu.RUnlock()
		require.NotNil(t, entry)
		require.ErrorIs(t, entry.err, merr.ErrDataIntegrity)

		require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
			PolicyName: "equal",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  `dept == "sales"`,
		}}}))
		expr, err = manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, newManagerTestSchemaHelper(t))
		require.NoError(t, err)
		require.NotNil(t, expr)
	})

	t.Run("element nullable array using expression", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID:         100,
			Name:            "tags",
			DataType:        schemapb.DataType_Array,
			ElementType:     schemapb.DataType_VarChar,
			ElementNullable: true,
		}}}
		helper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)
		manager := newManagerWithAlice()
		require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
			PolicyName: "nullable_array",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  `array_contains(tags, "red")`,
		}}}))

		expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, helper)
		require.Nil(t, expr)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("floating literal on integer array", func(t *testing.T) {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID:     100,
			Name:        "values",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
		}}})
		require.NoError(t, err)
		manager := newManagerWithAlice()
		require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
			PolicyName: "mixed_numeric_array",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  "array_contains_any(values, [1, 1.5])",
		}}}))

		expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, helper)
		require.Nil(t, expr)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("internal compiler error is not cached", func(t *testing.T) {
		manager := newManagerWithAlice()
		require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
			PolicyName: "equal",
			PolicyType: rlsutil.PolicyTypePermissive,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  `dept == "sales"`,
		}}}))

		expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, nil)
		require.Nil(t, expr)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		state := manager.getCollectionState(100)
		state.mu.RLock()
		_, cached := state.compiled[compiledKey{action: rlsutil.PolicyActionQuery, kind: usingExprKind}]
		state.mu.RUnlock()
		require.False(t, cached)

		expr, err = manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, newManagerTestSchemaHelper(t))
		require.NoError(t, err)
		require.NotNil(t, expr)
	})
}

func TestManagerSnapshotReplace(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)
	policy := &rlsutil.RowPolicy{
		PolicyName: "p1",
		PolicyType: rlsutil.PolicyTypePermissive,
		Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
		UsingExpr:  "dept == $current_principal_tags['dept']",
	}
	tags := map[string]rlsutil.TagValue{"dept": rlsutil.NewStringTagValue("sales")}

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{policy},
	}))
	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", tags))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", nil))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: nil,
	}))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Nil(t, expr)
}

func TestManagerEmptyPolicySnapshotFailsClosed(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	require.Nil(t, expr)
}

func TestPreparePolicyExprTemplatesCombinedLength(t *testing.T) {
	tests := []struct {
		name     string
		policies []*rlsutil.RowPolicy
		expected string
	}{
		{name: "empty"},
		{
			name: "restrictive only",
			policies: []*rlsutil.RowPolicy{{
				PolicyType: rlsutil.PolicyTypeRestrictive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "active == true",
			}},
			expected: "false",
		},
		{
			name: "permissive and restrictive groups",
			policies: []*rlsutil.RowPolicy{
				{PolicyType: rlsutil.PolicyTypePermissive, Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionQuery}, UsingExpr: "a == 1"},
				{PolicyType: rlsutil.PolicyTypePermissive, Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionQuery}, UsingExpr: "b == 2"},
				{PolicyType: rlsutil.PolicyTypeRestrictive, Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionQuery}, UsingExpr: "c == 3"},
				{PolicyType: rlsutil.PolicyTypeRestrictive, Actions: []rlsutil.PolicyAction{rlsutil.PolicyActionQuery}, UsingExpr: "d == 4"},
			},
			expected: "((a == 1) or (b == 2)) and ((c == 3) and (d == 4))",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, combinedLength := preparePolicyExprTemplates(test.policies, rlsutil.PolicyActionQuery, usingExprKind)
			require.Equal(t, len(test.expected), combinedLength)
		})
	}
}

func TestManagerCombinedExpressionLengthLimit(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.Key, "1024")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.Key)
	})

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "p1",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == 'sales'",
			},
		},
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.Key, "8")
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrServiceQuotaExceeded)
	require.Nil(t, expr)
	state := manager.getCollectionState(100)
	state.mu.RLock()
	entry := state.compiled[compiledKey{action: rlsutil.PolicyActionQuery, kind: usingExprKind}]
	state.mu.RUnlock()
	require.NotNil(t, entry)
	require.ErrorIs(t, entry.err, merr.ErrServiceQuotaExceeded)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.Key, "1024")
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	state.mu.RLock()
	entry = state.compiled[compiledKey{action: rlsutil.PolicyActionQuery, kind: usingExprKind}]
	state.mu.RUnlock()
	require.NotNil(t, entry)
	require.NoError(t, entry.err)
}

func TestManagerAlwaysTruePredicateReturnsNil(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "full_access",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "true",
			},
		},
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.Nil(t, expr)
}

func TestManagerConstantPoliciesShortCircuitTagRefresh(t *testing.T) {
	oldOptimize := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue("false")
	defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(oldOptimize)

	policy := func(name string, policyType rlsutil.PolicyType, expr string) *rlsutil.RowPolicy {
		return &rlsutil.RowPolicy{
			PolicyName: name,
			PolicyType: policyType,
			Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
			UsingExpr:  expr,
		}
	}
	tests := []struct {
		name            string
		policies        []*rlsutil.RowPolicy
		wantError       bool
		wantAlwaysFalse bool
	}{
		{
			name: "permissive true",
			policies: []*rlsutil.RowPolicy{
				policy("all", rlsutil.PolicyTypePermissive, "true"),
				policy("public", rlsutil.PolicyTypePermissive, `dept == "public"`),
				policy("tag", rlsutil.PolicyTypePermissive, `dept == $current_principal_tags['dept']`),
			},
		},
		{
			name: "restrictive false",
			policies: []*rlsutil.RowPolicy{
				policy("tag", rlsutil.PolicyTypePermissive, `dept == $current_principal_tags['dept']`),
				policy("blocked", rlsutil.PolicyTypeRestrictive, `dept == "blocked"`),
				policy("none", rlsutil.PolicyTypeRestrictive, "false"),
			},
			wantAlwaysFalse: true,
		},
		{
			name: "restrictive tag remains required",
			policies: []*rlsutil.RowPolicy{
				policy("all", rlsutil.PolicyTypePermissive, "true"),
				policy("tag", rlsutil.PolicyTypeRestrictive, `dept == $current_principal_tags['dept']`),
			},
			wantError: true,
		},
		{
			name: "permissive tag remains required",
			policies: []*rlsutil.RowPolicy{
				policy("none", rlsutil.PolicyTypePermissive, "false"),
				policy("tag", rlsutil.PolicyTypePermissive, `dept == $current_principal_tags['dept']`),
			},
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coord := &metadataTestCoord{principalErr: merr.WrapErrServiceUnavailableMsg("principal metadata unavailable")}
			manager := newManager()
			require.NoError(t, manager.init(context.Background(), coord))
			require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: test.policies}))

			expr, err := manager.resolveUsingPredicate(context.Background(), 100, "alice", rlsutil.PolicyActionQuery, newManagerTestSchemaHelper(t))
			if test.wantError {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Equal(t, int32(1), coord.principalCalls.Load())
				return
			}
			require.NoError(t, err)
			require.Zero(t, coord.principalCalls.Load())
			if test.wantAlwaysFalse {
				require.True(t, rewriter.IsAlwaysFalseExpr(expr))
			} else {
				require.Nil(t, expr)
			}
		})
	}
}

func TestManagerRequestPathLoadsMissingStartupState(t *testing.T) {
	ctx := context.Background()
	const collectionID = UniqueID(104)

	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	coord := &managerTestCoordClient{
		getRLSMetadata: func(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
			require.NotNil(t, req)
			require.Equal(t, collectionID, req.GetCollectionId())
			if req.GetPrincipalName() != "" {
				require.Equal(t, "alice", req.GetPrincipalName())
				payload, err := rlsutil.TagsToJSON(map[string]rlsutil.TagValue{"dept": rlsutil.NewStringTagValue("sales")})
				require.NoError(t, err)
				return &rootcoordpb.GetRLSMetadataResponse{
					Status:       merr.Success(),
					CollectionId: collectionID,
					Principals: []*rootcoordpb.RLSPrincipalInfo{{
						CollectionId:  collectionID,
						PrincipalName: "alice",
						Tags:          payload,
					}},
				}, nil
			}
			return &rootcoordpb.GetRLSMetadataResponse{
				Status:       merr.Success(),
				CollectionId: collectionID,
				Policies: []*rootcoordpb.RLSPolicyInfo{
					{
						CollectionId: collectionID,
						PolicyId:     1,
						PolicyName:   "dept",
						PolicyType:   milvuspb.RowPolicyType(rlsutil.PolicyTypePermissive),
						Actions:      []milvuspb.RowPolicyAction{milvuspb.RowPolicyAction(rlsutil.PolicyActionQuery)},
						UsingExpr:    "dept == $current_principal_tags['dept']",
					},
				},
			}, nil
		},
	}

	require.NoError(t, manager.init(ctx, coord))

	expr, err := manager.resolveUsingPredicate(ctx, collectionID, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerRequestPathRefreshFailsClosed(t *testing.T) {
	ctx := context.Background()
	const collectionID = UniqueID(105)

	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	staleRefresh := time.Now().Add(-2 * time.Hour)
	require.True(t, setPolicySnapshotForTest(manager, collectionID, policySnapshot{
		RefreshedAt: staleRefresh,
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "stale-allow",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))
	coord := &managerTestCoordClient{
		getRLSMetadata: func(ctx context.Context, req *rootcoordpb.GetRLSMetadataRequest) (*rootcoordpb.GetRLSMetadataResponse, error) {
			return &rootcoordpb.GetRLSMetadataResponse{
				Status: merr.Status(merr.WrapErrServiceUnavailableMsg("rootcoord unavailable")),
			}, nil
		},
	}

	require.NoError(t, manager.init(ctx, coord))
	expr, err := manager.resolveUsingPredicate(ctx, collectionID, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Nil(t, expr)
}

func TestManagerCollectionStateIsScopedByCollectionID(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))
	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)

	require.True(t, setPolicySnapshotForTest(manager, 101, policySnapshot{}))
	expr, err = manager.resolveUsingPredicate(ctx, 101, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	require.Nil(t, expr)
}

func TestManagerCollectionPredicateLocksAreIndependent(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)
	for _, collectionID := range []UniqueID{100, 200} {
		require.True(t, setPolicySnapshotForTest(manager, collectionID, policySnapshot{
			Policies: []*rlsutil.RowPolicy{
				{
					PolicyName: "dept",
					PolicyType: rlsutil.PolicyTypePermissive,
					Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
					UsingExpr:  `dept == "sales"`,
				},
			},
		}))
		require.True(t, setManagerTestPrincipalTags(manager, collectionID, "alice", nil))
	}

	state := manager.getCollectionState(100)
	require.NotNil(t, state)
	state.mu.Lock()
	firstDone := make(chan error, 1)
	go func() {
		_, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
		firstDone <- err
	}()
	select {
	case err := <-firstDone:
		state.mu.Unlock()
		require.Failf(t, "predicate unexpectedly bypassed its collection lock", "error: %v", err)
		return
	case <-time.After(20 * time.Millisecond):
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := manager.resolveUsingPredicate(ctx, 200, "alice", rlsutil.PolicyActionQuery, helper)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		state.mu.Unlock()
		t.Fatal("predicate for one collection waited for another collection's lock")
	}

	state.mu.Unlock()
	require.NoError(t, <-firstDone)
}

func TestManagerDefaultDatabaseNameDoesNotAffectLookup(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
		},
	}))
	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerMissingEntriesFailClosed(t *testing.T) {
	ctx := context.Background()
	manager := newManager()
	helper := newManagerTestSchemaHelper(t)
	require.NoError(t, manager.init(ctx, &metadataTestCoord{}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Nil(t, expr)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "p1",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "dept == $current_principal_tags['dept']",
			},
		},
	}))
	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", nil))

	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{
		"region": rlsutil.NewStringTagValue("us"),
	}))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))

	require.True(t, setManagerTestPrincipalTags(manager, 100, "alice", map[string]rlsutil.TagValue{
		"dept": rlsutil.NewStringTagValue("sales"),
	}))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
}

func TestManagerPolicySnapshotReplacesByName(t *testing.T) {
	ctx := context.Background()
	manager := newManagerWithAlice()
	helper := newManagerTestSchemaHelper(t)

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "sales",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "engineering"`,
			},
		},
	}))

	expr, err := manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "sales",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "sales"`,
			},
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "product"`,
			},
		},
	}))

	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("engineering"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("product"), 1, expr, "query", "using"))

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "engineering",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  `dept == "product"`,
			},
		},
	}))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.NoError(t, err)
	require.NotNil(t, expr)
	require.Error(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("sales"), 1, expr, "query", "using"))
	require.NoError(t, ValidateRowsByPredicate(ctx, managerTestFieldsData("product"), 1, expr, "query", "using"))

	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{
		Policies: nil,
	}))
	expr, err = manager.resolveUsingPredicate(ctx, 100, "alice", rlsutil.PolicyActionQuery, helper)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Nil(t, expr)
}

func TestResolveRuntimePrincipal(t *testing.T) {
	principal, enforce, err := ResolveRuntimePrincipal(false, "", "query")
	require.NoError(t, err)
	assert.Empty(t, principal)
	assert.False(t, enforce)

	_, _, err = ResolveRuntimePrincipal(true, "", "query")
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "rls_principal")
	_, _, err = ResolveRuntimePrincipal(true, " \t ", "query")
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "rls_principal")

	principal, enforce, err = ResolveRuntimePrincipal(true, "alice", "query")
	require.NoError(t, err)
	assert.Equal(t, "alice", principal)
	assert.True(t, enforce)

	paramtable.Get().Save(paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.Key, "3")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().ProxyCfg.RLSMaxPrincipalNameLength.Key)
	})
	principal, enforce, err = ResolveRuntimePrincipal(true, "alice", "query")
	require.NoError(t, err)
	assert.Equal(t, "alice", principal)
	assert.True(t, enforce)

	_, _, err = ResolveRuntimePrincipal(true, strings.Repeat("a", rlsutil.MaxTransportIdentifierLength+1), "query")
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
}

func TestResolvePredicateRequiresPrincipal(t *testing.T) {
	manager := newManagerWithAlice()
	require.True(t, setPolicySnapshotForTest(manager, 100, policySnapshot{Policies: []*rlsutil.RowPolicy{{
		PolicyName: "allow_all",
		PolicyType: rlsutil.PolicyTypePermissive,
		Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
		UsingExpr:  "true",
	}}}))

	expr, err := manager.resolveUsingPredicate(context.Background(), 100, "", rlsutil.PolicyActionQuery, newManagerTestSchemaHelper(t))
	require.Nil(t, expr)
	require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
}

func TestValidateCheckForWriteUsesSchemaTimezone(t *testing.T) {
	ctx := context.Background()
	const collectionID = int64(987654322)
	manager := newManagerWithPrincipal(collectionID, "alice")

	schema := &schemapb.CollectionSchema{
		Name:       "rls_timestamptz_test",
		Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	require.True(t, setPolicySnapshotForTest(manager, collectionID, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "insert_at_midnight",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
				CheckExpr:  "ts == ISO '2025-01-01 00:00:00'",
			},
		},
	}))
	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
		{
			FieldId:   101,
			FieldName: "ts",
			Type:      schemapb.DataType_Timestamptz,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{
				TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1735660800000000}},
			}}},
		},
	}
	require.NoError(t, validateCheckForWrite(ctx, manager, collectionID, "alice", rlsutil.PolicyActionInsert, fieldsData, helper, 1, "insert"))
}

func TestManagerReadPredicateUsesSchemaTimezone(t *testing.T) {
	ctx := context.Background()
	const collectionID = int64(987654323)
	manager := newManagerWithPrincipal(collectionID, "alice")

	schema := &schemapb.CollectionSchema{
		Name:       "rls_read_timestamptz_test",
		Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	require.True(t, setPolicySnapshotForTest(manager, collectionID, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "read_at_midnight",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "ts == ISO '2025-01-01 00:00:00'",
			},
		},
	}))

	expr, err := manager.resolveUsingPredicate(
		ctx,
		collectionID,
		"alice",
		rlsutil.PolicyActionQuery,
		helper,
	)
	require.NoError(t, err)

	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   101,
			FieldName: "ts",
			Type:      schemapb.DataType_Timestamptz,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{
				TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{1735660800000000}},
			}}},
		},
	}
	require.NoError(t, ValidateRowsByPredicate(ctx, fieldsData, 1, expr, "query", "using"))
}

func TestManagerCompiledPredicateCacheUsesSchemaContext(t *testing.T) {
	ctx := context.Background()
	const collectionID = int64(987654324)
	manager := newManagerWithPrincipal(collectionID, "alice")

	newHelper := func(version int32, timezone string) *typeutil.SchemaHelper {
		helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
			Name:       "rls_compiled_cache_schema_context_test",
			Version:    version,
			Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: timezone}},
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz},
			},
		})
		require.NoError(t, err)
		return helper
	}

	require.True(t, setPolicySnapshotForTest(manager, collectionID, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "read_at_midnight",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
				UsingExpr:  "ts == ISO '2025-01-01 00:00:00'",
			},
		},
	}))

	getPredicateValue := func(helper *typeutil.SchemaHelper) int64 {
		expr, err := manager.resolveUsingPredicate(
			ctx,
			collectionID,
			"alice",
			rlsutil.PolicyActionQuery,
			helper,
		)
		require.NoError(t, err)
		require.NotNil(t, expr.GetUnaryRangeExpr())
		return expr.GetUnaryRangeExpr().GetValue().GetInt64Val()
	}

	shanghaiV1 := newHelper(1, "Asia/Shanghai")
	assert.Equal(t, int64(1735660800000000), getPredicateValue(shanghaiV1))

	// A real schema evolution must use a distinct compiled entry even when the
	// collection timezone remains unchanged.
	shanghaiV2 := newHelper(2, "Asia/Shanghai")
	assert.Equal(t, int64(1735660800000000), getPredicateValue(shanghaiV2))

	// Collection property alters, including timezone changes, do not increment
	// schema version. Timezone therefore has to be part of the cache identity.
	utcV2 := newHelper(2, "UTC")
	assert.Equal(t, int64(1735689600000000), getPredicateValue(utcV2))

	state := manager.getCollectionState(collectionID)
	require.NotNil(t, state)
	state.mu.RLock()
	defer state.mu.RUnlock()
	require.Len(t, state.compiled, 1)
	entry := state.compiled[compiledKey{action: rlsutil.PolicyActionQuery, kind: usingExprKind}]
	require.NotNil(t, entry)
	assert.Equal(t, int32(2), entry.schemaVersion)
	assert.Equal(t, "UTC", entry.timezone)
	require.NotNil(t, entry.expression)
}

func TestValidateRowsUsesFieldPrecisionForFloat(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_float_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "score", DataType: schemapb.DataType_Float},
			{FieldID: 101, Name: "scores", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	t.Run("scalar", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{{
			FieldId:   100,
			FieldName: "score",
			Type:      schemapb.DataType_Float,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: []float32{0.1}},
			}}},
		}}
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "score == 0.1", "insert", "check"))
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "score in [0.1, 0.2]", "insert", "check"))
	})

	t.Run("array", func(t *testing.T) {
		fieldsData := []*schemapb.FieldData{{
			FieldId:   101,
			FieldName: "scores",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Float,
					Data: []*schemapb.ScalarField{{
						Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{0.1}}},
					}},
				},
			}}},
		}}
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "array_contains(scores, 0.1)", "insert", "check"))
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "array_contains_all(scores, [0.1])", "insert", "check"))
		require.NoError(t, validateRows(context.Background(), fieldsData, helper, 1, "array_contains_any(scores, [0.1, 0.2])", "insert", "check"))
	})
}

func TestValidateRowsTreatsEmptyValidDataAsDense(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_default_value_test",
		Fields: []*schemapb.FieldSchema{{
			FieldID:  100,
			Name:     "score",
			DataType: schemapb.DataType_Float,
			DefaultValue: &schemapb.ValueField{
				Data: &schemapb.ValueField_FloatData{FloatData: 0.1},
			},
		}},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	fieldsData := []*schemapb.FieldData{{
		FieldId:   100,
		FieldName: "score",
		Type:      schemapb.DataType_Float,
		ValidData: []bool{},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: []float32{0.1}},
		}}},
	}}

	require.NotPanics(t, func() {
		err = validateRows(context.Background(), fieldsData, helper, 1, "score == 0.1", "insert", "check")
	})
	require.NoError(t, err)
}

func newManagerTestSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()

	schema := &schemapb.CollectionSchema{
		Name: "rls_manager_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "score", DataType: schemapb.DataType_Double},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func managerTestFieldsDataWithAgeAndScore(dept string, age int64, score float64) []*schemapb.FieldData {
	fields := managerTestFieldsData(dept)
	return append(fields,
		&schemapb.FieldData{
			FieldId:   102,
			FieldName: "age",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{age}}}}},
		},
		&schemapb.FieldData{
			FieldId:   103,
			FieldName: "score",
			Type:      schemapb.DataType_Double,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{Data: []float64{score}},
			}}},
		},
	)
}

func newManagerTestPrincipalSchemaHelper(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()

	schema := &schemapb.CollectionSchema{
		Name: "rls_manager_principal_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 103, Name: "region", DataType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func managerTestFieldsData(dept string) []*schemapb.FieldData {
	return managerTestFieldsDataWithID(1, dept)
}

func managerTestFieldsDataWithID(id int64, dept string) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{id}}}}},
		},
		{
			FieldId:   101,
			FieldName: "dept",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{dept}}}}},
		},
	}
}

func managerTestPrincipalFieldsData(dept string, owner string, region string) []*schemapb.FieldData {
	return []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
		},
		{
			FieldId:   101,
			FieldName: "dept",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{dept}}}}},
		},
		{
			FieldId:   102,
			FieldName: "owner",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{owner}}}}},
		},
		{
			FieldId:   103,
			FieldName: "region",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{region}}}}},
		},
	}
}

func TestMergePredicateToPlan(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_plan_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{
				FieldID:  103,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	visitorArgs := &planparserv2.ParserVisitorArgs{}

	retrievePlan, err := planparserv2.CreateRetrievePlanArgs(helper, "age > 18", nil, visitorArgs)
	require.NoError(t, err)
	rlsPredicate, err := planparserv2.ParseExpr(helper, `owner == "alice"`, nil)
	require.NoError(t, err)
	require.NoError(t, MergePredicateToPlan(retrievePlan, rlsPredicate))
	assertPredicateMerged(t, retrievePlan.GetQuery().GetPredicates())

	searchPlan, err := planparserv2.CreateSearchPlanArgs(helper, "age > 18", "vec", &planpb.QueryInfo{
		Topk:           10,
		MetricType:     "L2",
		SearchParams:   "{}",
		GroupByFieldId: -1,
	}, nil, nil, visitorArgs)
	require.NoError(t, err)
	rlsPredicate, err = planparserv2.ParseExpr(helper, `owner == "alice"`, nil)
	require.NoError(t, err)
	require.NoError(t, MergePredicateToPlan(searchPlan, rlsPredicate))
	assertPredicateMerged(t, searchPlan.GetVectorAnns().GetPredicates())
}

func assertPredicateMerged(t *testing.T, expr *planpb.Expr) {
	t.Helper()

	binaryExpr := expr.GetBinaryExpr()
	require.NotNil(t, binaryExpr)
	assert.Equal(t, planpb.BinaryExpr_LogicalAnd, binaryExpr.GetOp())
	assert.NotNil(t, binaryExpr.GetLeft())
	assert.NotNil(t, binaryExpr.GetRight())
}

func TestValidateRowsByParsedExpression(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "owner", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 103, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	fieldsData := []*schemapb.FieldData{
		{
			FieldId:   100,
			FieldName: "id",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}},
		},
		{
			FieldId:   101,
			FieldName: "owner",
			Type:      schemapb.DataType_VarChar,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"alice", "alice"}}}}},
		},
		{
			FieldId:   102,
			FieldName: "age",
			Type:      schemapb.DataType_Int64,
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18, 19}}}}},
		},
		{
			FieldId:   103,
			FieldName: "tags",
			Type:      schemapb.DataType_Array,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red", "blue"}}}},
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}},
				},
			}}}},
		},
	}

	allowedExpr := `owner == "alice" and age in [18, 19] and array_contains(tags, "red")`
	err = validateRows(context.Background(), fieldsData, helper, 2, allowedExpr, "insert", "check")
	require.NoError(t, err)
	parsedExpr, err := planparserv2.ParseExpr(helper, allowedExpr, nil)
	require.NoError(t, err)
	rows := newRowData(fieldsData, ReferencedFieldIDs(parsedExpr))
	result, err := evalExpr(parsedExpr, rows, 0)
	require.NoError(t, err)
	require.Equal(t, truthTrue, result)
	require.Len(t, rows.termMatchers, 1)

	err = validateRows(context.Background(), fieldsData, helper, 2, `age == 18`, "insert", "check")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
	assert.Contains(t, err.Error(), "row 1")
}

func TestLiteralMatcherRejectsMalformedExpression(t *testing.T) {
	_, err := newLiteralMatcher(schemapb.DataType_Int64, []*planpb.GenericValue{planparserv2.NewString("not an integer")})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestValidateWritePredicatesUseThreeValuedLogic(t *testing.T) {
	ctx := context.Background()
	const collectionID = int64(987654323)
	manager := newManagerWithPrincipal(collectionID, "alice")

	schema := &schemapb.CollectionSchema{
		Name: "rls_three_valued_logic_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "dept", DataType: schemapb.DataType_VarChar, Nullable: true},
			{FieldID: 102, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	require.True(t, setPolicySnapshotForTest(manager, collectionID, policySnapshot{
		Policies: []*rlsutil.RowPolicy{
			{
				PolicyName: "allowed_dept",
				PolicyType: rlsutil.PolicyTypePermissive,
				Actions:    []rlsutil.PolicyAction{rlsutil.PolicyActionInsert},
				CheckExpr:  `dept == "allowed"`,
			},
		},
	}))

	newFieldsData := func(fullSize bool) []*schemapb.FieldData {
		deptData := []string(nil)
		arrayData := []*schemapb.ScalarField(nil)
		if fullSize {
			deptData = []string{""}
			arrayData = []*schemapb.ScalarField{{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}},
			}}
		}
		return []*schemapb.FieldData{
			{
				FieldId:   100,
				FieldName: "id",
				Type:      schemapb.DataType_Int64,
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
			},
			{
				FieldId:   101,
				FieldName: "dept",
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{false},
				Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: deptData}}}},
			},
			{
				FieldId:   102,
				FieldName: "tags",
				Type:      schemapb.DataType_Array,
				ValidData: []bool{false},
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_VarChar,
					Data:        arrayData,
				}}}},
			},
		}
	}

	for _, fullSize := range []bool{false, true} {
		name := "compact"
		if fullSize {
			name = "full_size"
		}
		t.Run(name, func(t *testing.T) {
			fieldsData := newFieldsData(fullSize)
			err := validateCheckForWrite(ctx, manager, collectionID, "alice", rlsutil.PolicyActionInsert, fieldsData, helper, 1, "insert")
			require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		})
	}

	fieldsData := newFieldsData(false)
	rowData := newRowData(fieldsData, []int64{101, 102})
	tests := []struct {
		name     string
		expr     string
		expected truthValue
	}{
		{name: "unknown and false", expr: `dept == "blocked" and false`, expected: truthFalse},
		{name: "unknown and true", expr: `dept == "blocked" and true`, expected: truthUnknown},
		{name: "unknown or true", expr: `dept == "blocked" or true`, expected: truthTrue},
		{name: "unknown or false", expr: `dept == "blocked" or false`, expected: truthUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsedExpr, err := planparserv2.ParseExpr(helper, test.expr, nil)
			require.NoError(t, err)
			result, err := evalExpr(parsedExpr, rowData, 0)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)

			err = ValidateRowsByPredicate(ctx, fieldsData, 1, parsedExpr, "upsert", "using")
			if test.expected == truthTrue {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		})
	}
}

func TestNullableArrayUsesFieldSpecificValidData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_nullable_array_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, `array_contains(tags, "blue")`, nil)
	require.NoError(t, err)
	red := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}}
	blue := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"blue"}}}}
	for _, storage := range []struct {
		name       string
		values     []*schemapb.ScalarField
		mappedRows bool
	}{
		{name: "dense", values: []*schemapb.ScalarField{red, {}, blue}},
		{name: "compact", values: []*schemapb.ScalarField{red, blue}, mappedRows: true},
	} {
		t.Run(storage.name, func(t *testing.T) {
			fieldData := &schemapb.FieldData{
				FieldId:   101,
				FieldName: "tags",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					ValidData: []bool{true, false, true},
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_VarChar,
						Data:        storage.values,
					}},
				}},
			}
			rows := newRowData([]*schemapb.FieldData{fieldData}, []int64{101})
			if storage.mappedRows {
				require.NotEmpty(t, rows.fields[101].arrayDataIndices)
			} else {
				require.Empty(t, rows.fields[101].arrayDataIndices)
			}
			for rowIdx, expected := range []truthValue{truthFalse, truthUnknown, truthTrue} {
				actual, err := evalExpr(expr, rows, rowIdx)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
			err := ValidateRowsByPredicate(context.Background(), []*schemapb.FieldData{fieldData}, 3, expr, "upsert", "check")
			require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
		})
	}
}

func TestArrayMatcherSkipsNullElements(t *testing.T) {
	tests := []struct {
		name        string
		dataType    schemapb.DataType
		array       *schemapb.ScalarField
		nullTarget  *planpb.GenericValue
		validTarget *planpb.GenericValue
	}{
		{
			name:     "bool",
			dataType: schemapb.DataType_Bool,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{Data: []bool{false, true}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: false}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
		},
		{
			name:     "int",
			dataType: schemapb.DataType_Int64,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{0, 7}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}},
		},
		{
			name:     "long",
			dataType: schemapb.DataType_Int64,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{0, 7}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}},
		},
		{
			name:     "float",
			dataType: schemapb.DataType_Float,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: []float32{0, 1.5}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 1.5}},
		},
		{
			name:     "double",
			dataType: schemapb.DataType_Double,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{Data: []float64{0, 2.5}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 0}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 2.5}},
		},
		{
			name:     "string",
			dataType: schemapb.DataType_VarChar,
			array: &schemapb.ScalarField{ValidData: []bool{false, true}, Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: []string{"", "present"}},
			}},
			nullTarget:  &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: ""}},
			validTarget: &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "present"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := &rowData{}
			newMatcher := func(target *planpb.GenericValue) *arrayLiteralMatcher {
				literals, err := newLiteralMatcher(test.dataType, []*planpb.GenericValue{target})
				require.NoError(t, err)
				return &arrayLiteralMatcher{literalMatcher: literals, op: planpb.JSONContainsExpr_Contains, seen: make([]uint32, len(literals.values))}
			}

			contains, err := newMatcher(test.nullTarget).matches(test.array, rows)
			require.NoError(t, err)
			require.False(t, contains)

			contains, err = newMatcher(test.validTarget).matches(test.array, rows)
			require.NoError(t, err)
			require.True(t, contains)
			require.Len(t, rows.arrayElementLayouts, 1)
		})
	}

	tests[0].array.ValidData = []bool{false}
	literals, err := newLiteralMatcher(tests[0].dataType, []*planpb.GenericValue{tests[0].validTarget})
	require.NoError(t, err)
	matcher := &arrayLiteralMatcher{literalMatcher: literals, op: planpb.JSONContainsExpr_Contains, seen: make([]uint32, len(literals.values))}
	rows := &rowData{}
	_, err = matcher.matches(tests[0].array, rows)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = matcher.matches(tests[0].array, rows)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Len(t, rows.arrayElementLayouts, 1)
}

func TestArrayContainsOperationsSkipNullElements(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_nullable_array_element_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	tests := []struct {
		expr     string
		expected truthValue
	}{
		{expr: "array_contains(values, 0)", expected: truthFalse},
		{expr: "array_contains(values, 7)", expected: truthTrue},
		{expr: "array_contains_any(values, [0, 8])", expected: truthFalse},
		{expr: "array_contains_any(values, [0, 7])", expected: truthTrue},
		{expr: "array_contains_all(values, [7, 0])", expected: truthFalse},
		{expr: "array_contains_all(values, [7])", expected: truthTrue},
		{expr: "array_contains_all(values, [7, 7])", expected: truthTrue},
		{expr: "array_contains_all(values, [])", expected: truthTrue},
		{expr: "array_contains_any(values, [])", expected: truthFalse},
	}
	for _, storage := range []struct {
		name   string
		values []int64
	}{
		{name: "dense", values: []int64{0, 7}},
		{name: "compact", values: []int64{7}},
	} {
		t.Run(storage.name, func(t *testing.T) {
			rows := newRowData([]*schemapb.FieldData{{
				FieldId:   100,
				FieldName: "values",
				Type:      schemapb.DataType_Array,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
					ElementType: schemapb.DataType_Int64,
					Data: []*schemapb.ScalarField{{
						ValidData: []bool{false, true},
						Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: storage.values}},
					}},
				}}}},
			}}, []int64{100})

			for _, test := range tests {
				t.Run(test.expr, func(t *testing.T) {
					expr, err := planparserv2.ParseExpr(helper, test.expr, nil)
					require.NoError(t, err)
					actual, err := evalExpr(expr, rows, 0)
					require.NoError(t, err)
					require.Equal(t, test.expected, actual)
					if contains := expr.GetJsonContainsExpr(); contains != nil {
						require.NotNil(t, rows.arrayMatchers[contains])
					}
				})
			}
			require.Len(t, rows.arrayElementLayouts, 1)
		})
	}
}

func TestArrayContainsAllMatcherDoesNotLeakAcrossRows(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
	}}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, "array_contains_all(values, [7, 8])", nil)
	require.NoError(t, err)
	rows := newRowData([]*schemapb.FieldData{{
		FieldId:   100,
		FieldName: "values",
		Type:      schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_Int64,
			Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7, 8}}}},
				{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}},
			},
		}}}},
	}}, []int64{100})

	result, err := evalExpr(expr, rows, 0)
	require.NoError(t, err)
	require.Equal(t, truthTrue, result)
	require.NotNil(t, rows.arrayMatchers[expr.GetJsonContainsExpr()])
	result, err = evalExpr(expr, rows, 1)
	require.NoError(t, err)
	require.Equal(t, truthFalse, result)
}

func TestValidateRowsInternalRowShapeErrorsAreSystemErrors(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "rls_test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "tags", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	assertSystemError := func(fieldsData []*schemapb.FieldData, rowNum int, expr string) {
		t.Helper()
		err := validateRows(context.Background(), fieldsData, helper, rowNum, expr, "insert", "check")
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.NotErrorIs(t, err, merr.ErrParameterInvalid)
	}

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   100,
		FieldName: "id",
		Type:      schemapb.DataType_Int64,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
	}}, 1, `age == 18`)

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   101,
		FieldName: "age",
		Type:      schemapb.DataType_Int64,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}}},
	}}, 1, `age == 18`)

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   101,
		FieldName: "age",
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			ValidData: []bool{true, true},
			Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}},
		}},
	}}, 2, `age == 18`)

	assertSystemError([]*schemapb.FieldData{{
		FieldId:   102,
		FieldName: "tags",
		Type:      schemapb.DataType_Array,
		ValidData: []bool{true, false, false},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_VarChar,
			Data: []*schemapb.ScalarField{
				{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"red"}}}},
				{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"ignored"}}}},
			},
		}}}},
	}}, 3, `array_contains(tags, "red")`)
}

func TestValidateRowsByPredicateValidatesReferencedFieldRowCount(t *testing.T) {
	helper := newManagerTestSchemaHelper(t)
	expr, err := planparserv2.ParseExpr(helper, `dept == "sales"`, nil)
	require.NoError(t, err)

	twoRows := []*schemapb.FieldData{{
		FieldId:   101,
		FieldName: "dept",
		Type:      schemapb.DataType_VarChar,
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"sales", "engineering"}}}}},
	}}

	for _, test := range []struct {
		name       string
		fieldsData []*schemapb.FieldData
		rowNum     int
	}{
		{name: "negative", fieldsData: twoRows, rowNum: -1},
		{name: "zero with data", fieldsData: twoRows, rowNum: 0},
		{name: "trailing row", fieldsData: twoRows, rowNum: 1},
		{name: "count exceeds data", fieldsData: twoRows, rowNum: 3},
		{name: "missing data", rowNum: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateRowsByPredicate(context.Background(), test.fieldsData, test.rowNum, expr, "insert", "check")
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}

	require.NoError(t, ValidateRowsByPredicate(context.Background(), nil, 0, expr, "insert", "check"))
	require.ErrorIs(t, ValidateRowsByPredicate(context.Background(), twoRows, 0, alwaysFalsePredicate(), "insert", "check"), merr.ErrServiceInternal)
	require.NoError(t, ValidateRowsByPredicate(context.Background(), []*schemapb.FieldData{
		managerTestFieldsData("sales")[1],
		{
			FieldId:   200,
			FieldName: "location",
			Type:      schemapb.DataType_Geometry,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryWktData{
				GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT (1 2)", "POINT (3 4)"}},
			}}},
		},
	}, 1, expr, "insert", "check"))
}

func TestValidateRowsRejectsUnsupportedComparisonOperator(t *testing.T) {
	helper := newManagerTestSchemaHelper(t)
	expr, err := planparserv2.ParseExpr(helper, `age > 17`, nil)
	require.NoError(t, err)
	err = ValidateRowsByPredicate(
		context.Background(),
		managerTestFieldsDataWithAgeAndScore("sales", 18, 0),
		1,
		expr,
		"insert",
		"check",
	)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestValidateRowsStopsOnCanceledContext(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "age", DataType: schemapb.DataType_Int64},
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := planparserv2.ParseExpr(helper, `age == 18`, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = ValidateRowsByPredicate(ctx, []*schemapb.FieldData{{
		FieldId: 100,
		Type:    schemapb.DataType_Int64,
		Field:   &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{18}}}}},
	}}, 1, expr, "insert", "check")
	require.ErrorIs(t, err, context.Canceled)
}
