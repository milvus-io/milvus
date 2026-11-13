package rewriter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func budgetTestCol(id int64) *planpb.ColumnInfo {
	return &planpb.ColumnInfo{FieldId: id, DataType: schemapb.DataType_Int64, Nullable: true}
}

func budgetTestGt(id, v int64) *planpb.Expr {
	return newUnaryRangeExpr(budgetTestCol(id), planpb.OpType_GreaterThan, &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: v}})
}

func budgetTestIn(id int64, n int) *planpb.Expr {
	values := make([]*planpb.GenericValue, n)
	for i := range values {
		values[i] = &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: int64(i)}}
	}
	return newTermExpr(budgetTestCol(id), values)
}

// budgetTestOr builds OR over branches, each branch AND(factor, residuals...).
func budgetTestOr(factor *planpb.Expr, residuals [][]*planpb.Expr) *planpb.Expr {
	branches := make([]*planpb.Expr, len(residuals))
	for i, rs := range residuals {
		conj := append([]*planpb.Expr{proto.Clone(factor).(*planpb.Expr)}, rs...)
		branches[i] = buildBalancedBinary(planpb.BinaryExpr_LogicalAnd, conj)
	}
	return buildBalancedBinary(planpb.BinaryExpr_LogicalOr, branches)
}

func countFactor(e, factor *planpb.Expr) int {
	if e == nil {
		return 0
	}
	if proto.Equal(e, factor) {
		return 1
	}
	if be := e.GetBinaryExpr(); be != nil {
		return countFactor(be.GetLeft(), factor) + countFactor(be.GetRight(), factor)
	}
	return 0
}

func TestCommonFactorBudget_Branches(t *testing.T) {
	factor := budgetTestGt(1, 0)
	build := func(n int) *planpb.Expr {
		residuals := make([][]*planpb.Expr, n)
		for i := range residuals {
			residuals[i] = []*planpb.Expr{budgetTestGt(int64(100+i), 0)}
		}
		return budgetTestOr(factor, residuals)
	}

	got := RewriteExprWithConfig(build(maxCommonFactorBranches), true)
	require.Equal(t, 1, countFactor(got, factor))
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp())

	got = RewriteExprWithConfig(build(maxCommonFactorBranches+1), true)
	require.Equal(t, maxCommonFactorBranches+1, countFactor(got, factor), "over the branch budget the OR is kept")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp())
}

func TestCommonFactorBudget_Nodes(t *testing.T) {
	factor := budgetTestGt(1, 0)
	build := func(perBranch int) *planpb.Expr {
		residuals := make([][]*planpb.Expr, 2)
		field := int64(100)
		for i := range residuals {
			for j := 0; j < perBranch; j++ {
				residuals[i] = append(residuals[i], budgetTestGt(field, 0))
				field++
			}
		}
		return budgetTestOr(factor, residuals)
	}

	// Each branch of k conjuncts has k leaves and k-1 AND nodes.
	within := (maxCommonFactorNodes/2+1)/2 - 1
	got := RewriteExprWithConfig(build(within), true)
	require.Equal(t, 1, countFactor(got, factor))

	got = RewriteExprWithConfig(build(maxCommonFactorNodes/2), true)
	require.Equal(t, 2, countFactor(got, factor), "over the node budget the OR is kept")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp())
}

func TestCommonFactorBudget_Work(t *testing.T) {
	residuals := [][]*planpb.Expr{{budgetTestGt(100, 0)}, {budgetTestGt(101, 0)}}

	small := budgetTestIn(1, 100)
	got := RewriteExprWithConfig(budgetTestOr(small, residuals), true)
	require.Equal(t, 1, countFactor(got, small))

	large := budgetTestIn(1, maxCommonFactorWork/2)
	got = RewriteExprWithConfig(budgetTestOr(large, residuals), true)
	require.Equal(t, 2, countFactor(got, large), "over the work budget the OR is kept")
	require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp())
}

func TestCommonFactorBudget_DisabledOptimizer(t *testing.T) {
	factor := budgetTestGt(1, 0)
	input := budgetTestOr(factor, [][]*planpb.Expr{{budgetTestGt(100, 0)}, {budgetTestGt(101, 0)}})
	got := RewriteExprWithConfig(input, false)
	require.Equal(t, 2, countFactor(got, factor))
}

func TestCommonFactorKey(t *testing.T) {
	budget := &commonFactorBudget{nodes: maxCommonFactorNodes, work: maxCommonFactorWork}
	col := budgetTestCol(1)
	i7 := &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 7}}
	f7 := &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 7}}
	s7 := &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "7"}}

	keyOf := func(e *planpb.Expr) string { return commonFactorKey(e, budget) }
	require.NotEmpty(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)))
	require.NotEqual(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)), keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, f7)))
	require.NotEqual(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)), keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, s7)))
	require.NotEqual(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)), keyOf(newUnaryRangeExpr(col, planpb.OpType_NotEqual, i7)))
	require.NotEqual(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)), keyOf(newUnaryRangeExpr(budgetTestCol(2), planpb.OpType_Equal, i7)))
	// A term on the same column with the same single value is a different node.
	require.NotEqual(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)), keyOf(newTermExpr(col, []*planpb.GenericValue{i7})))
	// Template names do not change the key once values are bound.
	tmpl := newUnaryRangeExpr(col, planpb.OpType_Equal, i7)
	tmpl.GetUnaryRangeExpr().TemplateVariableName = "x"
	require.Equal(t, keyOf(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)), keyOf(tmpl))

	// String length prefixes keep concatenations apart.
	a := newTermExpr(&planpb.ColumnInfo{FieldId: 3, DataType: schemapb.DataType_VarChar}, []*planpb.GenericValue{
		{Val: &planpb.GenericValue_StringVal{StringVal: "a|sb"}},
	})
	b := newTermExpr(&planpb.ColumnInfo{FieldId: 3, DataType: schemapb.DataType_VarChar}, []*planpb.GenericValue{
		{Val: &planpb.GenericValue_StringVal{StringVal: "a"}}, {Val: &planpb.GenericValue_StringVal{StringVal: "b"}},
	})
	require.NotEqual(t, keyOf(a), keyOf(b))

	unsupported := []*planpb.Expr{
		newUnaryRangeExpr(col, planpb.OpType_PrefixMatch, s7),
		newUnaryRangeExpr(col, planpb.OpType_Equal, nil),
		newUnaryRangeExpr(col, planpb.OpType_Equal, &planpb.GenericValue{Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{}}}),
		newUnaryRangeExpr(&planpb.ColumnInfo{FieldId: 4, DataType: schemapb.DataType_JSON, NestedPath: []string{"a"}}, planpb.OpType_Equal, i7),
		newUnaryRangeExpr(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, NestedPath: []string{"0"}}, planpb.OpType_Equal, i7),
		newUnaryRangeExpr(&planpb.ColumnInfo{FieldId: 6, DataType: schemapb.DataType_Int64, IsElementLevel: true}, planpb.OpType_Equal, i7),
		newTermExpr(col, nil),
		{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{ColumnInfo: col, Values: []*planpb.GenericValue{i7}, IsInField: true}}},
		newNullExpr(col, planpb.NullExpr_IsNull),
		notExpr(newUnaryRangeExpr(col, planpb.OpType_Equal, i7)),
		newAlwaysTrueExpr(),
	}
	for _, e := range unsupported {
		require.Empty(t, keyOf(e), "%v", e)
	}
}
