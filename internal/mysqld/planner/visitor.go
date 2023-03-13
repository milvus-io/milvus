package planner

type Visitor interface {
	VisitSqlStatements(n *NodeSqlStatements) interface{}
	VisitSqlStatement(n *NodeSqlStatement) interface{}
	VisitDmlStatement(n *NodeDmlStatement) interface{}
	VisitSelectStatement(n *NodeSelectStatement) interface{}
	VisitSimpleSelect(n *NodeSimpleSelect) interface{}
	VisitQuerySpecification(n *NodeQuerySpecification) interface{}
	VisitLockClause(n *NodeLockClause) interface{}
	VisitSelectSpec(n *NodeSelectSpec) interface{}
	VisitSelectElement(n *NodeSelectElement) interface{}
	VisitFromClause(n *NodeFromClause) interface{}
	VisitLimitClause(n *NodeLimitClause) interface{}
	VisitSelectElementStar(n *NodeSelectElementStar) interface{}
	VisitFullColumnName(n *NodeFullColumnName) interface{}
	VisitFunctionCall(n *NodeFunctionCall) interface{}
	VisitAggregateWindowedFunction(n *NodeAggregateWindowedFunction) interface{}
	VisitCount(n *NodeCount) interface{}
	VisitTableSource(n *NodeTableSource) interface{}
	VisitExpression(n *NodeExpression) interface{}
	VisitExpressions(n *NodeExpressions) interface{}
	VisitNotExpression(n *NodeNotExpression) interface{}
	VisitLogicalExpression(n *NodeLogicalExpression) interface{}
	VisitIsExpression(n *NodeIsExpression) interface{}
	VisitPredicate(n *NodePredicate) interface{}
	VisitInPredicate(n *NodeInPredicate) interface{}
	VisitBinaryComparisonPredicate(n *NodeBinaryComparisonPredicate) interface{}
	VisitExpressionAtomPredicate(n *NodeExpressionAtomPredicate) interface{}
	VisitExpressionAtom(n *NodeExpressionAtom) interface{}
	VisitUnaryExpressionAtom(n *NodeUnaryExpressionAtom) interface{}
	VisitNestedExpressionAtom(n *NodeNestedExpressionAtom) interface{}
	VisitConstant(n *NodeConstant) interface{}
}
