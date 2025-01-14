package planparserv2

import "github.com/milvus-io/milvus/pkg/proto/planpb"

type LogicalExprVisitor interface {
	VisitExpr(expr *planpb.Expr) interface{}
	VisitTermExpr(expr *planpb.TermExpr) interface{}
	VisitUnaryExpr(expr *planpb.UnaryExpr) interface{}
	VisitBinaryExpr(expr *planpb.BinaryExpr) interface{}
	VisitCompareExpr(expr *planpb.CompareExpr) interface{}
	VisitUnaryRangeExpr(expr *planpb.UnaryRangeExpr) interface{}
	VisitBinaryRangeExpr(expr *planpb.BinaryRangeExpr) interface{}
	VisitBinaryArithOpEvalRangeExpr(expr *planpb.BinaryArithOpEvalRangeExpr) interface{}
	VisitBinaryArithExpr(expr *planpb.BinaryArithExpr) interface{}
	VisitValueExpr(expr *planpb.ValueExpr) interface{}
	VisitColumnExpr(expr *planpb.ColumnExpr) interface{}
}
