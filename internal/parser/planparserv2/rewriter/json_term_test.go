package rewriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func TestRewriteJSONMixedInSplitsHomogeneousTerms(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	for _, exprStr := range []string{
		`JSONField["v"] in [1, 2, "3", "4", true, 2.5]`,
		`JSONField["v"] in ["4", 2.5, true, "3", 2, 1]`,
	} {
		expr, err := parser.ParseExpr(helper, exprStr, nil)
		require.NoError(t, err, exprStr)

		assertHomogeneousTerms(t, expr)
		require.Equal(t, map[string]int{
			"bool": 1, "int64": 2, "float": 1, "string": 2,
		}, collectMembershipKinds(expr), exprStr)
	}
}

func TestRewriteJSONMixedNotInNegatesSplitMembership(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper,
		`JSONField["v"] not in [1, 2, "3"]`, nil)
	require.NoError(t, err)

	unary := expr.GetUnaryExpr()
	require.NotNil(t, unary)
	require.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
	require.NotNil(t, unary.GetChild().GetBinaryExpr())
	assertHomogeneousTerms(t, unary.GetChild())
	require.Equal(t, map[string]int{"int64": 2, "string": 1},
		collectMembershipKinds(unary.GetChild()))
}

func TestRewriteJSONMixedOrEqualsDoesNotRecombineTypes(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper,
		`JSONField["v"] == 1 or JSONField["v"] == "1" or JSONField["v"] == 2 or JSONField["v"] == "2"`, nil)
	require.NoError(t, err)

	assertHomogeneousTerms(t, expr)
	require.Equal(t, map[string]int{"int64": 2, "string": 2},
		collectMembershipKinds(expr))
}

func TestRewriteJSONCrossTypeInWithNotEqualKeepsBothPredicates(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	for _, exprStr := range []string{
		`JSONField["v"] in [1, 2] and JSONField["v"] != "3"`,
		`JSONField["v"] in [1, 2] or JSONField["v"] != "3"`,
	} {
		expr, err := parser.ParseExpr(helper, exprStr, nil)
		require.NoError(t, err, exprStr)
		require.NotNil(t, expr.GetBinaryExpr(), exprStr)
		require.NotNil(t, findTermExpr(expr), exprStr)
		require.NotNil(t, findUnaryRangeExpr(expr, planpb.OpType_NotEqual), exprStr)
		assertHomogeneousTerms(t, expr)
	}
}

func TestRewriteJSONMixedNotEqualsMergeOnlyWithinType(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper,
		`JSONField["v"] != 1 and JSONField["v"] != 2 and JSONField["v"] != "3" and JSONField["v"] != "4"`, nil)
	require.NoError(t, err)

	assertHomogeneousTerms(t, expr)
	require.Equal(t, map[string]int{"int64": 2, "string": 2},
		collectMembershipKinds(expr))
}

func TestRewriteJSONMixedInRunsWhenOptimizationDisabled(t *testing.T) {
	col := &planpb.ColumnInfo{
		FieldId:    102,
		DataType:   schemapb.DataType_JSON,
		NestedPath: []string{"v"},
	}
	input := &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
		ColumnInfo: col,
		Values: []*planpb.GenericValue{
			{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			{Val: &planpb.GenericValue_StringVal{StringVal: "1"}},
		},
	}}}

	expr := rewriter.RewriteExprWithConfig(input, false)
	require.NotNil(t, expr.GetBinaryExpr())
	assertHomogeneousTerms(t, expr)
	require.Equal(t, map[string]int{"int64": 1, "string": 1},
		collectMembershipKinds(expr))
}

func TestRewriteJSONMixedTermInsideCallExpr(t *testing.T) {
	col := &planpb.ColumnInfo{
		FieldId:    102,
		DataType:   schemapb.DataType_JSON,
		NestedPath: []string{"v"},
	}
	input := &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{
		FunctionName: "test",
		FunctionParameters: []*planpb.Expr{{
			Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
				ColumnInfo: col,
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
					{Val: &planpb.GenericValue_StringVal{StringVal: "1"}},
				},
			}},
		}},
	}}}

	expr := rewriter.RewriteExprWithConfig(input, false)
	parameter := expr.GetCallExpr().GetFunctionParameters()[0]
	require.NotNil(t, parameter.GetBinaryExpr())
	assertHomogeneousTerms(t, parameter)
	require.Equal(t, map[string]int{"int64": 1, "string": 1},
		collectMembershipKinds(parameter))
}

func TestRewriteJSONMixedNumericPreservesLargeInteger(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper,
		`JSONField["v"] in [9007199254740993, 1.5]`, nil)
	require.NoError(t, err)

	var integerValues []int64
	walkExpr(expr, func(current *planpb.Expr) {
		if term := current.GetTermExpr(); term != nil {
			for _, value := range term.GetValues() {
				if testValueKind(value) == "int64" {
					integerValues = append(integerValues, value.GetInt64Val())
				}
			}
		}
		if unary := current.GetUnaryRangeExpr(); unary != nil &&
			unary.GetOp() == planpb.OpType_Equal && testValueKind(unary.GetValue()) == "int64" {
			integerValues = append(integerValues, unary.GetValue().GetInt64Val())
		}
	})
	require.Equal(t, []int64{9007199254740993}, integerValues)
}

func TestRewriteJSONArrayInUsesEqualityBranches(t *testing.T) {
	helper := buildSchemaHelperWithJSON(t)
	expr, err := parser.ParseExpr(helper,
		`JSONField["v"] in [[1, 2], [3, 4]]`, nil)
	require.NoError(t, err)
	require.Nil(t, findTermExpr(expr))
	require.Equal(t, map[string]int{"array": 2}, collectMembershipKinds(expr))
}

func assertHomogeneousTerms(t *testing.T, expr *planpb.Expr) {
	t.Helper()
	walkExpr(expr, func(current *planpb.Expr) {
		term := current.GetTermExpr()
		if term == nil || len(term.GetValues()) == 0 {
			return
		}
		kind := testValueKind(term.GetValues()[0])
		for _, value := range term.GetValues()[1:] {
			require.Equal(t, kind, testValueKind(value),
				"TermExpr contains mixed value types")
		}
	})
}

func collectMembershipKinds(expr *planpb.Expr) map[string]int {
	result := make(map[string]int)
	walkExpr(expr, func(current *planpb.Expr) {
		if term := current.GetTermExpr(); term != nil {
			for _, value := range term.GetValues() {
				result[testValueKind(value)]++
			}
			return
		}
		if unary := current.GetUnaryRangeExpr(); unary != nil &&
			unary.GetOp() == planpb.OpType_Equal {
			result[testValueKind(unary.GetValue())]++
		}
	})
	return result
}

func walkExpr(expr *planpb.Expr, visit func(*planpb.Expr)) {
	if expr == nil {
		return
	}
	visit(expr)
	if binary := expr.GetBinaryExpr(); binary != nil {
		walkExpr(binary.GetLeft(), visit)
		walkExpr(binary.GetRight(), visit)
	}
	if unary := expr.GetUnaryExpr(); unary != nil {
		walkExpr(unary.GetChild(), visit)
	}
}

func testValueKind(value *planpb.GenericValue) string {
	switch value.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return "bool"
	case *planpb.GenericValue_Int64Val:
		return "int64"
	case *planpb.GenericValue_FloatVal:
		return "float"
	case *planpb.GenericValue_StringVal:
		return "string"
	case *planpb.GenericValue_ArrayVal:
		return "array"
	default:
		return "other"
	}
}
