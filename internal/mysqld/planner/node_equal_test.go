package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEqual(t *testing.T) {
	// a in ["100", false, 666.666]
	expr1 := NewNodeExpression("", WithPredicate(
		NewNodePredicate("", WithInPredicate(
			NewNodeInPredicate("", NewNodePredicate("", WithNodeExpressionAtomPredicate(
				NewNodeExpressionAtomPredicate("",
					NewNodeExpressionAtom("", ExpressionAtomWithFullColumnName(
						NewNodeFullColumnName("", "a")))))), NewNodeExpressions("", []*NodeExpression{
				NewNodeExpression("", WithPredicate(
					NewNodePredicate("", WithNodeExpressionAtomPredicate(
						NewNodeExpressionAtomPredicate("",
							NewNodeExpressionAtom("", ExpressionAtomWithConstant(
								NewNodeConstant("", WithStringLiteral("100"))))))))),
				NewNodeExpression("", WithPredicate(
					NewNodePredicate("", WithNodeExpressionAtomPredicate(
						NewNodeExpressionAtomPredicate("",
							NewNodeExpressionAtom("", ExpressionAtomWithConstant(
								NewNodeConstant("", WithBooleanLiteral(false))))))))),
				NewNodeExpression("", WithPredicate(
					NewNodePredicate("", WithNodeExpressionAtomPredicate(
						NewNodeExpressionAtomPredicate("",
							NewNodeExpressionAtom("", ExpressionAtomWithConstant(
								NewNodeConstant("", WithRealLiteral(666.666))))))))),
			}), InOperatorIn)))))

	// b >= (+100)
	expr2 := NewNodeExpression("", WithPredicate(
		NewNodePredicate("", WithNodeBinaryComparisonPredicate(
			NewNodeBinaryComparisonPredicate("", NewNodePredicate("", WithNodeExpressionAtomPredicate(
				NewNodeExpressionAtomPredicate("",
					NewNodeExpressionAtom("", ExpressionAtomWithFullColumnName(
						NewNodeFullColumnName("", "b")))))), NewNodePredicate("", WithNodeExpressionAtomPredicate(
				NewNodeExpressionAtomPredicate("",
					NewNodeExpressionAtom("", ExpressionAtomWithNestedExpr(
						NewNodeNestedExpressionAtom("", []*NodeExpression{
							NewNodeExpression("", WithPredicate(
								NewNodePredicate("", WithNodeExpressionAtomPredicate(
									NewNodeExpressionAtomPredicate("",
										NewNodeExpressionAtom("", ExpressionAtomWithUnaryExpr(
											NewNodeUnaryExpressionAtom("", NewNodeExpressionAtom("", ExpressionAtomWithConstant(
												NewNodeConstant("", WithDecimalLiteral(100)))), UnaryOperatorPositive)))))))),
						})))))),
				ComparisonOperatorGreaterEqual)))))

	// c is true
	expr3 := NewNodeExpression("", WithIsExpr(
		NewNodeIsExpression("", NewNodePredicate("", WithNodeExpressionAtomPredicate(
			NewNodeExpressionAtomPredicate("",
				NewNodeExpressionAtom("", ExpressionAtomWithFullColumnName(
					NewNodeFullColumnName("", "c")))))), TestValueTrue, IsOperatorIs)))

	// ((not expr1) & (expr2)) | expr3
	expr := NewNodeExpression("", WithLogicalExpr(
		NewNodeLogicalExpression("",
			NewNodeExpression("", WithLogicalExpr(
				NewNodeLogicalExpression("",
					NewNodeExpression("", WithNotExpr(
						NewNodeNotExpression("", expr1))),
					expr2,
					LogicalOperatorAnd))),
			expr3,
			LogicalOperatorOr)))

	n := NewNodeSqlStatements("", []*NodeSqlStatement{
		NewNodeSqlStatement("", WithDmlStatement(
			NewNodeDmlStatement("", WithSelectStatement(
				NewNodeSelectStatement("", WithSimpleSelect(
					NewNodeSimpleSelect("", WithLockClause(
						NewNodeLockClause("", LockClauseOptionForUpdate)), WithQuery(
						NewNodeQuerySpecification("", []*NodeSelectSpec{
							NewNodeSelectSpec(""),
						}, []*NodeSelectElement{
							NewNodeSelectElement("", WithStar()),
							NewNodeSelectElement("", WithFullColumnName(
								NewNodeFullColumnName("", "a", FullColumnNameWithAlias("alias1")))),
							NewNodeSelectElement("", WithFunctionCall(
								NewNodeFunctionCall("", FunctionCallWithAlias("alias2"), WithAgg(
									NewNodeAggregateWindowedFunction("", WithAggCount(
										NewNodeCount(""))))))),
						}, WithLimit(
							NewNodeLimitClause("", 100, 0)), WithFrom(
							NewNodeFromClause("", []*NodeTableSource{
								NewNodeTableSource("", WithTableName("t")),
							}, WithWhere(expr)))))))))))),
	})

	assert.True(t, Equal(n, n))
}
