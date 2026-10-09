package rewriter_test

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// false < unknown < true gives SQL three-valued AND/OR as min/max.
// This oracle evaluates plan nodes independently of the rewrite helpers.
func evalInNotEqualPlan(t *testing.T, expr *planpb.Expr, value *int64, fieldID int64, nullable bool) int {
	t.Helper()
	checkColumn := func(col *planpb.ColumnInfo) {
		require.NotNil(t, col)
		require.Equal(t, fieldID, col.GetFieldId())
		require.Equal(t, schemapb.DataType_Int64, col.GetDataType())
		require.Equal(t, nullable, col.GetNullable())
	}
	truth := func(matched bool) int {
		if value == nil {
			return 1
		}
		if matched {
			return 2
		}
		return 0
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return 2
	case *planpb.Expr_UnaryExpr:
		require.Equal(t, planpb.UnaryExpr_Not, node.UnaryExpr.GetOp())
		return 2 - evalInNotEqualPlan(t, node.UnaryExpr.GetChild(), value, fieldID, nullable)
	case *planpb.Expr_BinaryExpr:
		left := evalInNotEqualPlan(t, node.BinaryExpr.GetLeft(), value, fieldID, nullable)
		right := evalInNotEqualPlan(t, node.BinaryExpr.GetRight(), value, fieldID, nullable)
		if node.BinaryExpr.GetOp() == planpb.BinaryExpr_LogicalAnd {
			return min(left, right)
		}
		require.Equal(t, planpb.BinaryExpr_LogicalOr, node.BinaryExpr.GetOp())
		return max(left, right)
	case *planpb.Expr_TermExpr:
		checkColumn(node.TermExpr.GetColumnInfo())
		require.False(t, node.TermExpr.GetIsInField())
		require.NotEmpty(t, node.TermExpr.GetValues(), "NULL-sensitive contradictions must retain a nonempty predicate")
		matched := false
		for _, item := range node.TermExpr.GetValues() {
			require.IsType(t, &planpb.GenericValue_Int64Val{}, item.GetVal())
			matched = matched || value != nil && *value == item.GetInt64Val()
		}
		return truth(matched)
	case *planpb.Expr_UnaryRangeExpr:
		checkColumn(node.UnaryRangeExpr.GetColumnInfo())
		require.IsType(t, &planpb.GenericValue_Int64Val{}, node.UnaryRangeExpr.GetValue().GetVal())
		matched := value != nil && *value == node.UnaryRangeExpr.GetValue().GetInt64Val()
		if node.UnaryRangeExpr.GetOp() == planpb.OpType_NotEqual {
			matched = !matched
		} else {
			require.Equal(t, planpb.OpType_Equal, node.UnaryRangeExpr.GetOp())
		}
		return truth(matched)
	default:
		t.Fatalf("unexpected executable predicate: %T", node)
		return 0
	}
}

func TestRewriteInNotEqualThreeValuedSemantics(t *testing.T) {
	cases := []struct {
		name, filter string
		matches      func(int64) bool
	}{
		{"and_removes_one", "%s IN [1,2,3] AND %s != 2", func(x int64) bool { return (x == 1 || x == 2 || x == 3) && x != 2 }},
		{"and_excludes_all", "%s IN [1,2] AND %s != 1 AND %s != 2", func(x int64) bool { return (x == 1 || x == 2) && x != 1 && x != 2 }},
		{"or_hit", "%s IN [1,2] OR %s != 1", func(x int64) bool { return x == 1 || x == 2 || x != 1 }},
		{"or_miss", "%s IN [1,2] OR %s != 3", func(x int64) bool { return x == 1 || x == 2 || x != 3 }},
		{"multiple_terms_keep_constraints", "%s IN [1,2] AND %s IN [3,4] AND %s != 1", func(x int64) bool { return (x == 1 || x == 2) && (x == 3 || x == 4) && x != 1 }},
		{"multiple_terms_partial_exclusion", "%s IN [1,2,3] AND %s IN [2,3,4] AND %s != 2", func(x int64) bool { return (x == 1 || x == 2 || x == 3) && (x == 2 || x == 3 || x == 4) && x != 2 }},
		{"or_two_exclusions", "%s IN [1,2] OR %s != 3 OR %s != 4", func(int64) bool { return true }},
	}
	for _, nullable := range []bool{false, true} {
		helper := buildSchemaHelperForRewriteNullableT(t)
		fieldName := "Int64Field"
		if nullable {
			fieldName = "NullableInt64Field"
		}
		field, err := helper.GetFieldFromName(fieldName)
		require.NoError(t, err)
		for _, tc := range cases {
			for _, negate := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/nullable=%t/not=%t", tc.name, nullable, negate), func(t *testing.T) {
					// Substitute each source operand without building expectations from the plan.
					filter := strings.ReplaceAll(tc.filter, "%s", fieldName)
					if negate {
						filter = "NOT (" + filter + ")"
					}
					expr, err := parser.ParseExpr(helper, filter, nil)
					require.NoError(t, err)
					for x := int64(0); x <= 5; x++ {
						want := 0
						if tc.matches(x) {
							want = 2
						}
						if negate {
							want = 2 - want
						}
						require.Equal(t, want, evalInNotEqualPlan(t, expr, &x, field.GetFieldID(), nullable), "row=%d filter=%s plan=%v", x, filter, expr)
					}
					if nullable {
						require.Equal(t, 1, evalInNotEqualPlan(t, expr, nil, field.GetFieldID(), true), "NULL filter=%s plan=%v", filter, expr)
					}
				})
			}
		}
	}
}

func TestRewriteInNotEqualMixedJSONTypesStaySeparate(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 109, Name: "JSONField", DataType: schemapb.DataType_JSON},
	}})
	require.NoError(t, err)
	for _, op := range []string{"AND", "OR"} {
		filter := fmt.Sprintf(`JSONField["key"] IN [1,2] %s JSONField["key"] != "1"`, op)
		expr, err := parser.ParseExpr(helper, filter, nil)
		require.NoError(t, err)
		require.NotNil(t, expr.GetBinaryExpr(), filter)
		require.NotNil(t, findTermExpr(expr), filter)
		notEqual := findUnaryRangeExpr(expr, planpb.OpType_NotEqual)
		require.NotNil(t, notEqual, filter)
		require.IsType(t, &planpb.GenericValue_StringVal{}, notEqual.GetValue().GetVal())
		require.Equal(t, "1", notEqual.GetValue().GetStringVal())
		require.False(t, rewriter.IsAlwaysTrueExpr(expr), filter)
	}
}

func TestRewriteInNotEqualMissingArrayElementKeepsPredicate(t *testing.T) {
	helper := buildSchemaHelperWithArraysT(t)
	for _, field := range []string{"ArrayInt[0]", "NullableArrayInt[0]"} {
		for _, source := range []string{
			"%s IN [1,2] AND %s != 1 AND %s != 2",
			"%s IN [1,2] OR %s != 1",
		} {
			for _, negate := range []bool{false, true} {
				filter := strings.ReplaceAll(source, "%s", field)
				if negate {
					filter = "NOT (" + filter + ")"
				}
				expr, err := parser.ParseExpr(helper, filter, nil)
				require.NoError(t, err, filter)
				require.False(t, rewriter.IsAlwaysTrueExpr(expr), "missing elements prevent a valid constant: %s", filter)
				require.False(t, rewriter.IsAlwaysFalseExpr(expr), "missing elements prevent a valid constant: %s", filter)
				require.NotNil(t, findTermExpr(expr), "preserve element-level IN semantics: %s", filter)
			}
		}
	}
}

func TestRewriteInNotEqualFloat32KeepsNarrowingSemantics(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 110, Name: "Float32Field", DataType: schemapb.DataType_Float},
	}})
	require.NoError(t, err)
	for _, filter := range []string{
		`Float32Field IN [1.0,1.00000001] AND Float32Field != 1.0`,
		`Float32Field IN [1.00000001,2.00000001] OR Float32Field != 1.0`,
	} {
		// Segcore compares float32 values: distinct float64 literals above narrow
		// to the same value, so dropping one using float64 equality is unsafe.
		expr, err := parser.ParseExpr(helper, filter, nil)
		require.NoError(t, err)
		require.NotNil(t, expr.GetBinaryExpr(), filter)
		require.NotNil(t, findTermExpr(expr), filter)
		require.NotNil(t, findUnaryRangeExpr(expr, planpb.OpType_NotEqual), filter)
	}
}

func TestRewriteInNotEqualNaNAndReverseInKeepPredicates(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		column := &planpb.ColumnInfo{FieldId: 111, DataType: schemapb.DataType_Double}
		values := []*planpb.GenericValue{
			{Val: &planpb.GenericValue_FloatVal{FloatVal: 1}},
			{Val: &planpb.GenericValue_FloatVal{FloatVal: math.NaN()}},
		}
		if reverse {
			values[1] = &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 2}}
		}
		for _, op := range []planpb.BinaryExpr_BinaryOp{planpb.BinaryExpr_LogicalAnd, planpb.BinaryExpr_LogicalOr} {
			term := &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
				ColumnInfo: column, Values: values, IsInField: reverse,
			}}}
			notEqual := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: column, Op: planpb.OpType_NotEqual, Value: values[0],
			}}}
			expr := rewriter.RewriteExprWithConfig(&planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
				Left: term, Right: notEqual, Op: op,
			}}}, true)
			require.NotNil(t, expr.GetBinaryExpr(), "reverse=%t op=%v", reverse, op)
			require.NotNil(t, findTermExpr(expr))
			require.NotNil(t, findUnaryRangeExpr(expr, planpb.OpType_NotEqual))
		}
	}
}
