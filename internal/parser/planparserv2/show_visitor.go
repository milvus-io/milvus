package planparserv2

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/log"
)

type ShowExprVisitor struct{}

func extractColumnInfo(info *planpb.ColumnInfo) interface{} {
	js := make(map[string]interface{})
	js["field_id"] = info.GetFieldId()
	js["data_type"] = info.GetDataType().String()
	js["auto_id"] = info.GetIsAutoID()
	js["is_pk"] = info.GetIsPrimaryKey()
	return js
}

func extractGenericValue(value *planpb.GenericValue) interface{} {
	if value == nil {
		return nil
	}
	switch realValue := value.Val.(type) {
	case *planpb.GenericValue_BoolVal:
		return realValue.BoolVal
	case *planpb.GenericValue_Int64Val:
		return realValue.Int64Val
	case *planpb.GenericValue_FloatVal:
		// // floating point value is not convenient to compare.
		// return strconv.FormatFloat(realValue.FloatVal, 'g', 10, 64)
		return realValue.FloatVal
	case *planpb.GenericValue_StringVal:
		return realValue.StringVal
	default:
		return nil
	}
}

func (v *ShowExprVisitor) VisitExpr(expr *planpb.Expr) interface{} {
	js := make(map[string]interface{})
	switch realExpr := expr.Expr.(type) {
	case *planpb.Expr_TermExpr:
		js["expr"] = v.VisitTermExpr(realExpr.TermExpr)
	case *planpb.Expr_UnaryExpr:
		js["expr"] = v.VisitUnaryExpr(realExpr.UnaryExpr)
	case *planpb.Expr_BinaryExpr:
		js["expr"] = v.VisitBinaryExpr(realExpr.BinaryExpr)
	case *planpb.Expr_CallExpr:
		js["expr"] = v.VisitCallExpr(realExpr.CallExpr)
	case *planpb.Expr_CompareExpr:
		js["expr"] = v.VisitCompareExpr(realExpr.CompareExpr)
	case *planpb.Expr_UnaryRangeExpr:
		js["expr"] = v.VisitUnaryRangeExpr(realExpr.UnaryRangeExpr)
	case *planpb.Expr_BinaryRangeExpr:
		js["expr"] = v.VisitBinaryRangeExpr(realExpr.BinaryRangeExpr)
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		js["expr"] = v.VisitBinaryArithOpEvalRangeExpr(realExpr.BinaryArithOpEvalRangeExpr)
	case *planpb.Expr_BinaryArithExpr:
		js["expr"] = v.VisitBinaryArithExpr(realExpr.BinaryArithExpr)
	case *planpb.Expr_ValueExpr:
		js["expr"] = v.VisitValueExpr(realExpr.ValueExpr)
	case *planpb.Expr_ColumnExpr:
		js["expr"] = v.VisitColumnExpr(realExpr.ColumnExpr)
	default:
		js["expr"] = ""
	}
	return js
}

func (v *ShowExprVisitor) VisitTermExpr(expr *planpb.TermExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "term"
	js["column_info"] = extractColumnInfo(expr.ColumnInfo)
	terms := make([]interface{}, 0, len(expr.Values))
	for _, v := range expr.Values {
		terms = append(terms, extractGenericValue(v))
	}
	js["terms"] = terms
	return js
}

func (v *ShowExprVisitor) VisitUnaryExpr(expr *planpb.UnaryExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = expr.Op.String()
	js["child"] = v.VisitExpr(expr.Child)
	return js
}

func (v *ShowExprVisitor) VisitBinaryExpr(expr *planpb.BinaryExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = expr.Op.String()
	js["left_child"] = v.VisitExpr(expr.Left)
	js["right_child"] = v.VisitExpr(expr.Right)
	return js
}

func (v *ShowExprVisitor) VisitCallExpr(expr *planpb.CallExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "call"
	js["func_name"] = expr.FunctionName
	params := make([]interface{}, 0, len(expr.FunctionParameters))
	for _, p := range expr.FunctionParameters {
		params = append(params, v.VisitExpr(p))
	}
	js["func_parameters"] = params
	return js
}

func (v *ShowExprVisitor) VisitCompareExpr(expr *planpb.CompareExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "compare"
	js["op"] = expr.Op.String()
	js["left_column_info"] = extractColumnInfo(expr.LeftColumnInfo)
	js["right_column_info"] = extractColumnInfo(expr.RightColumnInfo)
	return js
}

func (v *ShowExprVisitor) VisitUnaryRangeExpr(expr *planpb.UnaryRangeExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "unary_range"
	js["op"] = expr.Op.String()
	js["column_info"] = extractColumnInfo(expr.GetColumnInfo())
	js["operand"] = extractGenericValue(expr.Value)
	var extraValues []interface{}
	for _, v := range expr.ExtraValues {
		extraValues = append(extraValues, extractGenericValue(v))
	}
	js["extra_values"] = extraValues
	return js
}

func (v *ShowExprVisitor) VisitBinaryRangeExpr(expr *planpb.BinaryRangeExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "binary_range"
	js["column_info"] = extractColumnInfo(expr.GetColumnInfo())
	js["lower_value"] = extractGenericValue(expr.GetLowerValue())
	js["upper_value"] = extractGenericValue(expr.GetUpperValue())
	js["lower_inclusive"] = expr.GetLowerInclusive()
	js["upper_inclusive"] = expr.GetUpperInclusive()
	return js
}

func (v *ShowExprVisitor) VisitBinaryArithOpEvalRangeExpr(expr *planpb.BinaryArithOpEvalRangeExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "binary_arith_op_eval_range"
	js["column_info"] = extractColumnInfo(expr.ColumnInfo)
	js["arith_op"] = expr.ArithOp.String()
	js["right_operand"] = extractGenericValue(expr.GetRightOperand())
	js["op"] = expr.Op.String()
	js["value"] = extractGenericValue(expr.GetValue())
	return js
}

func (v *ShowExprVisitor) VisitBinaryArithExpr(expr *planpb.BinaryArithExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "binary_arith"
	js["left_expr"] = v.VisitExpr(expr.GetLeft())
	js["right_expr"] = v.VisitExpr(expr.GetRight())
	js["op"] = expr.Op.String()
	return js
}

func (v *ShowExprVisitor) VisitValueExpr(expr *planpb.ValueExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "value_expr"
	js["value"] = extractGenericValue(expr.GetValue())
	return js
}

func (v *ShowExprVisitor) VisitColumnExpr(expr *planpb.ColumnExpr) interface{} {
	js := make(map[string]interface{})
	js["expr_type"] = "column"
	js["column_info"] = extractColumnInfo(expr.GetInfo())
	return js
}

func NewShowExprVisitor() LogicalExprVisitor {
	return &ShowExprVisitor{}
}

// ShowExpr print the expr tree, used for debugging, not safe.
func ShowExpr(expr *planpb.Expr) {
	v := NewShowExprVisitor()
	js := v.VisitExpr(expr)
	b, _ := json.Marshal(js)
	log.Info("[ShowExpr]", zap.String("expr", string(b)))
}
