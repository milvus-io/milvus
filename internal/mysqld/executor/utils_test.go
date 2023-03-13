package executor

import "github.com/milvus-io/milvus/internal/mysqld/planner"

func GenNodeExpression(field string, c int64, op planner.ComparisonOperator) *planner.NodeExpression {
	n := planner.NewNodeExpression("", planner.WithPredicate(
		planner.NewNodePredicate("", planner.WithNodeBinaryComparisonPredicate(
			planner.NewNodeBinaryComparisonPredicate("",
				planner.NewNodePredicate("", planner.WithNodeExpressionAtomPredicate(
					planner.NewNodeExpressionAtomPredicate("",
						planner.NewNodeExpressionAtom("", planner.ExpressionAtomWithFullColumnName(
							planner.NewNodeFullColumnName("", field)))))),
				planner.NewNodePredicate("", planner.WithNodeExpressionAtomPredicate(
					planner.NewNodeExpressionAtomPredicate("",
						planner.NewNodeExpressionAtom("", planner.ExpressionAtomWithConstant(
							planner.NewNodeConstant("", planner.WithDecimalLiteral(c))))))),
				op),
		),
		)),
	)
	return n
}
