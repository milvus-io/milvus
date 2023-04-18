package planner

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/mysqld/sqlutil"
)

// TODO: remove this after execution engine is ready.
type exprTextRestorer struct {
	Visitor
}

func (v *exprTextRestorer) VisitFullColumnName(n *NodeFullColumnName) interface{} {
	return n.Name
}

func (v *exprTextRestorer) VisitExpression(n *NodeExpression) interface{} {
	if n.NotExpr.IsSome() {
		return n.NotExpr.Unwrap().Accept(v)
	}
	if n.LogicalExpr.IsSome() {
		return n.LogicalExpr.Unwrap().Accept(v)
	}
	if n.IsExpr.IsSome() {
		return n.IsExpr.Unwrap().Accept(v)
	}
	if n.Predicate.IsSome() {
		return n.Predicate.Unwrap().Accept(v)
	}
	return ""
}

func (v *exprTextRestorer) VisitExpressions(n *NodeExpressions) interface{} {
	l := len(n.Expressions)
	if l == 0 {
		return ""
	}
	s := n.Expressions[0].Accept(v).(string)
	for i := 1; i < l; i++ {
		s += fmt.Sprintf(", %s", n.Expressions[i].Accept(v))
	}
	return s
}

func (v *exprTextRestorer) VisitNotExpression(n *NodeNotExpression) interface{} {
	r := n.Expression.Accept(v)
	return fmt.Sprintf("not (%s)", r)
}

func (v *exprTextRestorer) VisitLogicalExpression(n *NodeLogicalExpression) interface{} {
	l := n.Left.Accept(v)
	r := n.Right.Accept(v)
	return fmt.Sprintf("(%s) %s (%s)", l, n.Op, r)
}

func (v *exprTextRestorer) VisitIsExpression(n *NodeIsExpression) interface{} {
	r := n.Predicate.Accept(v)
	return fmt.Sprintf("%s (%s)", n.Op, r)
}

func (v *exprTextRestorer) VisitPredicate(n *NodePredicate) interface{} {
	if n.InPredicate.IsSome() {
		return n.InPredicate.Unwrap().Accept(v)
	}
	if n.BinaryComparisonPredicate.IsSome() {
		return n.BinaryComparisonPredicate.Unwrap().Accept(v)
	}
	if n.ExpressionAtomPredicate.IsSome() {
		return n.ExpressionAtomPredicate.Unwrap().Accept(v)
	}
	return ""
}

func (v *exprTextRestorer) VisitInPredicate(n *NodeInPredicate) interface{} {
	// TODO: consider () instead of []
	predicate := n.Predicate.Accept(v)
	exprs := n.Expressions.Accept(v)
	return fmt.Sprintf("(%s) %s [%s]", predicate, n.Op, exprs)
}

func (v *exprTextRestorer) VisitBinaryComparisonPredicate(n *NodeBinaryComparisonPredicate) interface{} {
	l := n.Left.Accept(v)
	r := n.Right.Accept(v)
	return fmt.Sprintf("(%s) %s (%s)", l, n.Op, r)
}

func (v *exprTextRestorer) VisitExpressionAtomPredicate(n *NodeExpressionAtomPredicate) interface{} {
	return n.ExpressionAtom.Accept(v)
}

func (v *exprTextRestorer) VisitExpressionAtom(n *NodeExpressionAtom) interface{} {
	if n.Constant.IsSome() {
		return n.Constant.Unwrap().Accept(v)
	}
	if n.FullColumnName.IsSome() {
		return n.FullColumnName.Unwrap().Accept(v)
	}
	if n.UnaryExpr.IsSome() {
		return n.UnaryExpr.Unwrap().Accept(v)
	}
	if n.NestedExpr.IsSome() {
		return n.NestedExpr.Unwrap().Accept(v)
	}
	return ""
}

func (v *exprTextRestorer) VisitUnaryExpressionAtom(n *NodeUnaryExpressionAtom) interface{} {
	expr := n.Expr.Accept(v)
	return fmt.Sprintf("%s (%s)", n.Op, expr)
}

func (v *exprTextRestorer) VisitConstant(n *NodeConstant) interface{} {
	if n.StringLiteral.IsSome() {
		return n.StringLiteral.Unwrap()
	}
	if n.DecimalLiteral.IsSome() {
		return strconv.Itoa(int(n.DecimalLiteral.Unwrap()))
	}
	if n.BooleanLiteral.IsSome() {
		return strconv.FormatBool(n.BooleanLiteral.Unwrap())
	}
	if n.RealLiteral.IsSome() {
		return sqlutil.Float64ToString(n.RealLiteral.Unwrap())
	}
	return ""
}

func (v *exprTextRestorer) RestoreExprText(n Node) string {
	return n.Accept(v).(string)
}

func NewExprTextRestorer() *exprTextRestorer {
	return &exprTextRestorer{}
}
