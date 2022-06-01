package planparserv2

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func IsBool(n *planpb.GenericValue) bool {
	switch n.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return true
	}
	return false
}

func IsInteger(n *planpb.GenericValue) bool {
	switch n.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		return true
	}
	return false
}

func IsFloating(n *planpb.GenericValue) bool {
	switch n.GetVal().(type) {
	case *planpb.GenericValue_FloatVal:
		return true
	}
	return false
}

func IsNumber(n *planpb.GenericValue) bool {
	return IsInteger(n) || IsFloating(n)
}

func IsString(n *planpb.GenericValue) bool {
	switch n.GetVal().(type) {
	case *planpb.GenericValue_StringVal:
		return true
	}
	return false
}

func NewBool(value bool) *planpb.GenericValue {
	return &planpb.GenericValue{
		Val: &planpb.GenericValue_BoolVal{
			BoolVal: value,
		},
	}
}

func NewInt(value int64) *planpb.GenericValue {
	return &planpb.GenericValue{
		Val: &planpb.GenericValue_Int64Val{
			Int64Val: value,
		},
	}
}

func NewFloat(value float64) *planpb.GenericValue {
	return &planpb.GenericValue{
		Val: &planpb.GenericValue_FloatVal{
			FloatVal: value,
		},
	}
}

func NewString(value string) *planpb.GenericValue {
	return &planpb.GenericValue{
		Val: &planpb.GenericValue_StringVal{
			StringVal: value,
		},
	}
}

func toValueExpr(n *planpb.GenericValue) *ExprWithType {
	expr := &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: n,
			},
		},
	}

	switch n.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return &ExprWithType{
			expr:     expr,
			dataType: schemapb.DataType_Bool,
		}
	case *planpb.GenericValue_Int64Val:
		return &ExprWithType{
			expr:     expr,
			dataType: schemapb.DataType_Int64,
		}
	case *planpb.GenericValue_FloatVal:
		return &ExprWithType{
			expr:     expr,
			dataType: schemapb.DataType_Double,
		}
	case *planpb.GenericValue_StringVal:
		return &ExprWithType{
			expr:     expr,
			dataType: schemapb.DataType_VarChar,
		}
	default:
		return nil
	}
}

func getSameType(a, b schemapb.DataType) (schemapb.DataType, error) {
	if typeutil.IsFloatingType(a) && typeutil.IsArithmetic(b) {
		return schemapb.DataType_Double, nil
	}

	if typeutil.IsIntegerType(a) && typeutil.IsIntegerType(b) {
		return schemapb.DataType_Int64, nil
	}

	return schemapb.DataType_None, fmt.Errorf("incompatible data type, %s, %s", a.String(), b.String())
}

func calcDataType(left, right *ExprWithType, reverse bool) (schemapb.DataType, error) {
	if reverse {
		return getSameType(right.dataType, left.dataType)
	}
	return getSameType(left.dataType, right.dataType)
}

func reverseOrder(op planpb.OpType) (planpb.OpType, error) {
	switch op {
	case planpb.OpType_LessThan:
		return planpb.OpType_GreaterThan, nil
	case planpb.OpType_LessEqual:
		return planpb.OpType_GreaterEqual, nil
	case planpb.OpType_GreaterThan:
		return planpb.OpType_LessThan, nil
	case planpb.OpType_GreaterEqual:
		return planpb.OpType_LessEqual, nil
	case planpb.OpType_Equal:
		return planpb.OpType_Equal, nil
	case planpb.OpType_NotEqual:
		return planpb.OpType_NotEqual, nil
	default:
		return planpb.OpType_Invalid, fmt.Errorf("cannot reverse order: %s", op)
	}
}

func toColumnInfo(left *ExprWithType) *planpb.ColumnInfo {
	return left.expr.GetColumnExpr().GetInfo()
}

func castValue(dataType schemapb.DataType, value *planpb.GenericValue) (*planpb.GenericValue, error) {
	if typeutil.IsStringType(dataType) && IsString(value) {
		return value, nil
	}

	if typeutil.IsBoolType(dataType) && IsBool(value) {
		return value, nil
	}

	if typeutil.IsFloatingType(dataType) {
		if IsFloating(value) {
			return value, nil
		}
		if IsInteger(value) {
			return NewFloat(float64(value.GetInt64Val())), nil
		}
	}

	if typeutil.IsIntegerType(dataType) {
		if IsInteger(value) {
			return value, nil
		}
	}

	return nil, fmt.Errorf("cannot cast value to %s, value: %s", dataType.String(), value)
}

func combineBinaryArithExpr(op planpb.OpType, arithOp planpb.ArithOpType, columnInfo *planpb.ColumnInfo, operand *planpb.GenericValue, value *planpb.GenericValue) *planpb.Expr {
	castedValue, err := castValue(columnInfo.GetDataType(), operand)
	if err != nil {
		return nil
	}
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithOpEvalRangeExpr{
			BinaryArithOpEvalRangeExpr: &planpb.BinaryArithOpEvalRangeExpr{
				ColumnInfo:   columnInfo,
				ArithOp:      arithOp,
				RightOperand: castedValue,
				Op:           op,
				Value:        value,
			},
		},
	}
}

func handleBinaryArithExpr(op planpb.OpType, arithExpr *planpb.BinaryArithExpr, valueExpr *planpb.ValueExpr) (*planpb.Expr, error) {
	switch op {
	case planpb.OpType_Equal, planpb.OpType_NotEqual:
		break
	default:
		// TODO: enable this after execution is ready.
		return nil, fmt.Errorf("%s is not supported in execution backend", op)
	}

	leftExpr, leftValue := arithExpr.Left.GetColumnExpr(), arithExpr.Left.GetValueExpr()
	rightExpr, rightValue := arithExpr.Right.GetColumnExpr(), arithExpr.Right.GetValueExpr()

	if leftExpr != nil && rightExpr != nil {
		// a + b == 3
		return nil, fmt.Errorf("not supported to do arithmetic operations between multiple fields")
	}

	if leftValue != nil && rightValue != nil {
		// 2 + 1 == 3
		return nil, fmt.Errorf("unexpected, should be optimized already")
	}

	if leftExpr != nil && rightValue != nil {
		// a + 2 == 3
		// a - 2 == 3
		// a * 2 == 3
		// a / 2 == 3
		// a % 2 == 3
		return combineBinaryArithExpr(op, arithExpr.GetOp(), leftExpr.GetInfo(), rightValue.GetValue(), valueExpr.GetValue()), nil
	} else if rightExpr != nil && leftValue != nil {
		// 2 + a == 3
		// 2 - a == 3
		// 2 * a == 3
		// 2 / a == 3
		// 2 % a == 3

		switch arithExpr.GetOp() {
		case planpb.ArithOpType_Add, planpb.ArithOpType_Mul:
			return combineBinaryArithExpr(op, arithExpr.GetOp(), rightExpr.GetInfo(), leftValue.GetValue(), valueExpr.GetValue()), nil
		default:
			return nil, fmt.Errorf("todo")
		}
	} else {
		// (a + b) / 2 == 3
		return nil, fmt.Errorf("complicated arithmetic operations are not supported")
	}
}

func handleCompareRightValue(op planpb.OpType, left *ExprWithType, right *planpb.ValueExpr) (*planpb.Expr, error) {
	castedValue, err := castValue(left.dataType, right.GetValue())
	if err != nil {
		return nil, err
	}

	if leftArithExpr := left.expr.GetBinaryArithExpr(); leftArithExpr != nil {
		return handleBinaryArithExpr(op, leftArithExpr, &planpb.ValueExpr{Value: castedValue})
	}

	columnInfo := toColumnInfo(left)
	if columnInfo == nil {
		return nil, fmt.Errorf("not supported to combine multiple fields")
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: columnInfo,
				Op:         op,
				Value:      castedValue,
			},
		},
	}

	switch op {
	case planpb.OpType_Invalid:
		return nil, fmt.Errorf("unsupported op type: %s", op)
	default:
		return expr, nil
	}
}

func handleCompare(op planpb.OpType, left *ExprWithType, right *ExprWithType) (*planpb.Expr, error) {
	leftColumnInfo := toColumnInfo(left)
	rightColumnInfo := toColumnInfo(right)

	if leftColumnInfo == nil || rightColumnInfo == nil {
		return nil, fmt.Errorf("only comparison between two fields is supported")
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_CompareExpr{
			CompareExpr: &planpb.CompareExpr{
				LeftColumnInfo:  leftColumnInfo,
				RightColumnInfo: rightColumnInfo,
				Op:              op,
			},
		},
	}

	switch op {
	case planpb.OpType_Invalid:
		return nil, fmt.Errorf("unsupported op type: %s", op)
	default:
		return expr, nil
	}
}

func relationalCompatible(t1, t2 schemapb.DataType) bool {
	both := typeutil.IsStringType(t1) && typeutil.IsStringType(t2)
	neither := !typeutil.IsStringType(t1) && !typeutil.IsStringType(t2)
	return both || neither
}

func HandleCompare(op int, left, right *ExprWithType) (*planpb.Expr, error) {
	if !relationalCompatible(left.dataType, right.dataType) {
		return nil, fmt.Errorf("comparisons between string and non-string are not supported")
	}

	cmpOp := cmpOpMap[op]
	if valueExpr := left.expr.GetValueExpr(); valueExpr != nil {
		op, err := reverseOrder(cmpOp)
		if err != nil {
			return nil, err
		}
		return handleCompareRightValue(op, right, valueExpr)
	} else if valueExpr := right.expr.GetValueExpr(); valueExpr != nil {
		return handleCompareRightValue(cmpOp, left, valueExpr)
	} else {
		return handleCompare(cmpOp, left, right)
	}
}
