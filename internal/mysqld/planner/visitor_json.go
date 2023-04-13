package planner

import (
	"strconv"

	"github.com/milvus-io/milvus/internal/mysqld/sqlutil"
)

type jsonVisitor struct {
}

func (v jsonVisitor) VisitDmlStatement(n *NodeDmlStatement) interface{} {
	j := map[string]interface{}{}
	if n.SelectStatement.IsSome() {
		j["dml_statement"] = n.SelectStatement.Unwrap().Accept(v)
	}
	return j
}

func (v jsonVisitor) VisitSelectStatement(n *NodeSelectStatement) interface{} {
	j := map[string]interface{}{}
	if n.SimpleSelect.IsSome() {
		j["simple_select"] = n.SimpleSelect.Unwrap().Accept(v)
	}
	return j
}

func (v jsonVisitor) VisitSimpleSelect(n *NodeSimpleSelect) interface{} {
	j := map[string]interface{}{}

	if n.Query.IsSome() {
		j["query"] = n.Query.Unwrap().Accept(v)
	}

	if n.LockClause.IsSome() {
		j["lock_clause"] = n.LockClause.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitQuerySpecification(n *NodeQuerySpecification) interface{} {
	var selectSpecs []interface{}
	var selectElements []interface{}

	for _, selectSpec := range n.SelectSpecs {
		selectSpecs = append(selectSpecs, selectSpec.Accept(v))
	}

	for _, selectElement := range n.SelectElements {
		selectElements = append(selectElements, selectElement.Accept(v))
	}

	j := map[string]interface{}{
		"select_specs":    selectSpecs,
		"select_elements": selectElements,
	}

	if n.From.IsSome() {
		j["from_clause"] = n.From.Unwrap().Accept(v)
	}

	if n.Limit.IsSome() {
		j["limit_clause"] = n.Limit.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitLockClause(n *NodeLockClause) interface{} {
	// leaf node.
	return n.Option.String()
}

func (v jsonVisitor) VisitSelectSpec(n *NodeSelectSpec) interface{} {
	// leaf node.
	return ""
}

func (v jsonVisitor) VisitSelectElement(n *NodeSelectElement) interface{} {
	j := map[string]interface{}{}

	if n.Star.IsSome() {
		j["star"] = n.Star.Unwrap().Accept(v)
	}

	if n.FullColumnName.IsSome() {
		j["full_column_name"] = n.FullColumnName.Unwrap().Accept(v)
	}

	if n.FunctionCall.IsSome() {
		j["function_call"] = n.FunctionCall.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitFromClause(n *NodeFromClause) interface{} {
	var tableSources []interface{}

	for _, tableSource := range n.TableSources {
		tableSources = append(tableSources, tableSource.Accept(v))
	}

	j := map[string]interface{}{
		"table_sources": tableSources,
	}

	if n.Where.IsSome() {
		j["where"] = n.Where.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitLimitClause(n *NodeLimitClause) interface{} {
	// leaf node.
	j := map[string]interface{}{
		"limit":  n.Limit,
		"offset": n.Offset,
	}
	return j
}

func (v jsonVisitor) VisitSelectElementStar(n *NodeSelectElementStar) interface{} {
	// leaf node.
	return "*"
}

func (v jsonVisitor) VisitFullColumnName(n *NodeFullColumnName) interface{} {
	// leaf node.

	j := map[string]interface{}{
		"name": n.Name,
	}

	if n.Alias.IsSome() {
		j["alias"] = n.Alias.Unwrap()
	}

	return j
}

func (v jsonVisitor) VisitFunctionCall(n *NodeFunctionCall) interface{} {
	j := map[string]interface{}{}

	if n.Agg.IsSome() {
		j["agg"] = n.Agg.Unwrap().Accept(v)
	}

	if n.Alias.IsSome() {
		j["alias"] = n.Alias.Unwrap()
	}

	return j
}

func (v jsonVisitor) VisitAggregateWindowedFunction(n *NodeAggregateWindowedFunction) interface{} {
	j := map[string]interface{}{}

	if n.AggCount.IsSome() {
		j["agg_count"] = n.AggCount.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitCount(n *NodeCount) interface{} {
	// leaf node.
	return "count"
}

func (v jsonVisitor) VisitTableSource(n *NodeTableSource) interface{} {
	j := map[string]interface{}{}

	if n.TableName.IsSome() {
		j["name"] = n.TableName.Unwrap()
	}

	return j
}

func (v jsonVisitor) VisitExpression(n *NodeExpression) interface{} {

	j := map[string]interface{}{}

	if n.NotExpr.IsSome() {
		j["not_expr"] = n.NotExpr.Unwrap().Accept(v)
	}

	if n.LogicalExpr.IsSome() {
		j["logical_expr"] = n.LogicalExpr.Unwrap().Accept(v)
	}

	if n.IsExpr.IsSome() {
		j["is_expr"] = n.IsExpr.Unwrap().Accept(v)
	}

	if n.Predicate.IsSome() {
		j["predicate"] = n.Predicate.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitExpressions(n *NodeExpressions) interface{} {
	j := map[string]interface{}{}

	var expressions []interface{}

	for _, expression := range n.Expressions {
		expressions = append(expressions, expression.Accept(v))
	}

	j["expressions"] = expressions

	return j
}

func (v jsonVisitor) VisitNotExpression(n *NodeNotExpression) interface{} {
	j := map[string]interface{}{}

	j["expression"] = n.Expression.Accept(v)

	return j
}

func (v jsonVisitor) VisitLogicalExpression(n *NodeLogicalExpression) interface{} {
	j := map[string]interface{}{}

	j["op"] = n.Op.String()

	j["left"] = n.Left.Accept(v)

	j["right"] = n.Right.Accept(v)

	return j
}

func (v jsonVisitor) VisitIsExpression(n *NodeIsExpression) interface{} {
	j := map[string]interface{}{}

	j["predicate"] = n.Predicate.Accept(v)

	j["op"] = n.Op.String()

	j["test_value"] = n.TV.String()

	return j
}

func (v jsonVisitor) VisitPredicate(n *NodePredicate) interface{} {
	j := map[string]interface{}{}

	if n.InPredicate.IsSome() {
		j["in_predicate"] = n.InPredicate.Unwrap().Accept(v)
	}

	if n.BinaryComparisonPredicate.IsSome() {
		j["binary_comparison_predicate"] = n.BinaryComparisonPredicate.Unwrap().Accept(v)
	}

	if n.ExpressionAtomPredicate.IsSome() {
		j["expression_atom_predicate"] = n.ExpressionAtomPredicate.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitInPredicate(n *NodeInPredicate) interface{} {
	j := map[string]interface{}{}

	j["predicate"] = n.Predicate.Accept(v)

	j["op"] = n.Op.String()

	j["expressions"] = n.Expressions.Accept(v)

	return j
}

func (v jsonVisitor) VisitBinaryComparisonPredicate(n *NodeBinaryComparisonPredicate) interface{} {
	j := map[string]interface{}{}

	j["left"] = n.Left.Accept(v)

	j["op"] = n.Op.String()

	j["right"] = n.Right.Accept(v)

	return j
}

func (v jsonVisitor) VisitExpressionAtomPredicate(n *NodeExpressionAtomPredicate) interface{} {
	j := map[string]interface{}{}

	j["expression_atom"] = n.ExpressionAtom.Accept(v)

	return j
}

func (v jsonVisitor) VisitExpressionAtom(n *NodeExpressionAtom) interface{} {

	j := map[string]interface{}{}

	if n.Constant.IsSome() {
		j["constant"] = n.Constant.Unwrap().Accept(v)
	}

	if n.FullColumnName.IsSome() {
		j["full_column_name"] = n.FullColumnName.Unwrap().Accept(v)
	}

	if n.UnaryExpr.IsSome() {
		j["unary_expr"] = n.UnaryExpr.Unwrap().Accept(v)
	}

	if n.NestedExpr.IsSome() {
		j["nested_expr"] = n.NestedExpr.Unwrap().Accept(v)
	}

	return j
}

func (v jsonVisitor) VisitUnaryExpressionAtom(n *NodeUnaryExpressionAtom) interface{} {

	j := map[string]interface{}{}

	j["op"] = n.Op.String()

	j["expr"] = n.Expr.Accept(v)

	return j
}

func (v jsonVisitor) VisitNestedExpressionAtom(n *NodeNestedExpressionAtom) interface{} {

	j := map[string]interface{}{}

	var expressions []interface{}

	for _, expression := range n.Expressions {
		expressions = append(expressions, expression.Accept(v))
	}

	j["expressions"] = expressions

	return j
}

func (v jsonVisitor) VisitConstant(n *NodeConstant) interface{} {
	// leaf node.

	j := map[string]interface{}{}

	if n.StringLiteral.IsSome() {
		j["string_literal"] = n.StringLiteral.Unwrap()
	}

	if n.DecimalLiteral.IsSome() {
		j["decimal_literal"] = strconv.FormatInt(n.DecimalLiteral.Unwrap(), 10)
	}

	if n.BooleanLiteral.IsSome() {
		j["boolean_literal"] = strconv.FormatBool(n.BooleanLiteral.Unwrap())
	}

	if n.RealLiteral.IsSome() {
		j["real_literal"] = sqlutil.Float64ToString(n.RealLiteral.Unwrap())
	}

	return j
}

func (v jsonVisitor) VisitANNSClause(n *NodeANNSClause) interface{} {
	// leaf node.

	j := map[string]interface{}{}

	j["anns"] = n.String()

	return j
}

func NewJSONVisitor() Visitor {
	return &jsonVisitor{}
}
