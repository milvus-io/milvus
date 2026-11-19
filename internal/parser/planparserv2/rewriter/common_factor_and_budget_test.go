package rewriter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// budgetTestAnd builds AND over clauses, each clause OR(factor, residuals...).
func budgetTestAnd(factor *planpb.Expr, residuals [][]*planpb.Expr) *planpb.Expr {
	clauses := make([]*planpb.Expr, len(residuals))
	for i, rs := range residuals {
		disj := append([]*planpb.Expr{proto.Clone(factor).(*planpb.Expr)}, rs...)
		clauses[i] = buildBalancedBinary(planpb.BinaryExpr_LogicalOr, disj)
	}
	return buildBalancedBinary(planpb.BinaryExpr_LogicalAnd, clauses)
}

func TestCommonFactorAndBudget_Clauses(t *testing.T) {
	factor := budgetTestGt(1, 0)
	build := func(n int) *planpb.Expr {
		residuals := make([][]*planpb.Expr, n)
		for i := range residuals {
			residuals[i] = []*planpb.Expr{budgetTestGt(int64(100+i), 0)}
		}
		return budgetTestAnd(factor, residuals)
	}

	got := RewriteExprWithConfig(build(maxCommonFactorBranches), true)
	require.Equal(t, 1, countFactor(got, factor))
	require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp())

	got = RewriteExprWithConfig(build(maxCommonFactorBranches+1), true)
	require.Equal(t, maxCommonFactorBranches+1, countFactor(got, factor), "over the clause budget the AND is kept")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp())
}

func TestCommonFactorAndBudget_Nodes(t *testing.T) {
	factor := budgetTestGt(1, 0)
	build := func(perClause int) *planpb.Expr {
		residuals := make([][]*planpb.Expr, 2)
		field := int64(100)
		for i := range residuals {
			for j := 0; j < perClause; j++ {
				residuals[i] = append(residuals[i], budgetTestGt(field, 0))
				field++
			}
		}
		return budgetTestAnd(factor, residuals)
	}

	// Each clause of k disjuncts has k leaves and k-1 OR nodes.
	within := (maxCommonFactorNodes/2+1)/2 - 1
	got := RewriteExprWithConfig(build(within), true)
	require.Equal(t, 1, countFactor(got, factor))

	got = RewriteExprWithConfig(build(maxCommonFactorNodes/2), true)
	require.Equal(t, 2, countFactor(got, factor), "over the node budget the AND is kept")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp())
}

func TestCommonFactorAndBudget_Work(t *testing.T) {
	residuals := [][]*planpb.Expr{{budgetTestGt(100, 0)}, {budgetTestGt(101, 0)}}

	small := budgetTestIn(1, 100)
	got := RewriteExprWithConfig(budgetTestAnd(small, residuals), true)
	require.Equal(t, 1, countFactor(got, small))

	large := budgetTestIn(1, maxCommonFactorWork/2)
	got = RewriteExprWithConfig(budgetTestAnd(large, residuals), true)
	require.Equal(t, 2, countFactor(got, large), "over the work budget the AND is kept")
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp())
}

func TestCommonFactorAndBudget_DisabledOptimizer(t *testing.T) {
	factor := budgetTestGt(1, 0)
	input := budgetTestAnd(factor, [][]*planpb.Expr{{budgetTestGt(100, 0)}, {budgetTestGt(101, 0)}})
	got := RewriteExprWithConfig(input, false)
	require.Equal(t, 2, countFactor(got, factor))
}
