package planparserv2

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

func IsArray(n *planpb.GenericValue) bool {
	switch n.GetVal().(type) {
	case *planpb.GenericValue_ArrayVal:
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
	case *planpb.GenericValue_ArrayVal:
		return &ExprWithType{
			expr:     expr,
			dataType: schemapb.DataType_Array,
		}
	default:
		return nil
	}
}

func getSameType(left, right *ExprWithType) (schemapb.DataType, error) {
	lDataType, rDataType := left.dataType, right.dataType
	if typeutil.IsArrayType(lDataType) {
		lDataType = toColumnInfo(left).GetElementType()
	}
	if typeutil.IsArrayType(rDataType) {
		rDataType = toColumnInfo(right).GetElementType()
	}
	if typeutil.IsJSONType(lDataType) {
		if typeutil.IsJSONType(rDataType) {
			return schemapb.DataType_JSON, nil
		}
		if typeutil.IsFloatingType(rDataType) {
			return schemapb.DataType_Double, nil
		}
		if typeutil.IsIntegerType(rDataType) {
			return schemapb.DataType_Int64, nil
		}
	}
	if typeutil.IsFloatingType(lDataType) {
		if typeutil.IsJSONType(rDataType) || typeutil.IsArithmetic(rDataType) {
			return schemapb.DataType_Double, nil
		}
	}
	if typeutil.IsIntegerType(lDataType) {
		if typeutil.IsFloatingType(rDataType) {
			return schemapb.DataType_Double, nil
		}
		if typeutil.IsIntegerType(rDataType) || typeutil.IsJSONType(rDataType) {
			return schemapb.DataType_Int64, nil
		}
	}

	return schemapb.DataType_None, fmt.Errorf("incompatible data type, %s, %s", lDataType.String(), rDataType.String())
}

func calcDataType(left, right *ExprWithType, reverse bool) (schemapb.DataType, error) {
	if reverse {
		return getSameType(right, left)
	}
	return getSameType(left, right)
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
	if typeutil.IsJSONType(dataType) {
		return value, nil
	}
	if typeutil.IsArrayType(dataType) && IsArray(value) {
		return value, nil
	}
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

	if typeutil.IsIntegerType(dataType) && IsInteger(value) {
		return value, nil
	}

	return nil, fmt.Errorf("cannot cast value to %s, value: %s", dataType.String(), value)
}

func combineBinaryArithExpr(op planpb.OpType, arithOp planpb.ArithOpType, columnInfo *planpb.ColumnInfo, operand *planpb.GenericValue, value *planpb.GenericValue) *planpb.Expr {
	dataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(dataType) && len(columnInfo.GetNestedPath()) != 0 {
		dataType = columnInfo.GetElementType()
	}
	castedValue, err := castValue(dataType, operand)
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

func combineArrayLengthExpr(op planpb.OpType, arithOp planpb.ArithOpType, columnInfo *planpb.ColumnInfo, value *planpb.GenericValue) (*planpb.Expr, error) {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithOpEvalRangeExpr{
			BinaryArithOpEvalRangeExpr: &planpb.BinaryArithOpEvalRangeExpr{
				ColumnInfo: columnInfo,
				ArithOp:    arithOp,
				Op:         op,
				Value:      value,
			},
		},
	}, nil
}

func handleBinaryArithExpr(op planpb.OpType, arithExpr *planpb.BinaryArithExpr, valueExpr *planpb.ValueExpr) (*planpb.Expr, error) {
	leftExpr, leftValue := arithExpr.Left.GetColumnExpr(), arithExpr.Left.GetValueExpr()
	rightExpr, rightValue := arithExpr.Right.GetColumnExpr(), arithExpr.Right.GetValueExpr()
	arithOp := arithExpr.GetOp()
	if arithOp == planpb.ArithOpType_ArrayLength {
		return combineArrayLengthExpr(op, arithOp, leftExpr.GetInfo(), valueExpr.GetValue())
	}

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
		return combineBinaryArithExpr(op, arithOp, leftExpr.GetInfo(), rightValue.GetValue(), valueExpr.GetValue()), nil
	} else if rightExpr != nil && leftValue != nil {
		// 2 + a == 3
		// 2 - a == 3
		// 2 * a == 3
		// 2 / a == 3
		// 2 % a == 3

		switch arithExpr.GetOp() {
		case planpb.ArithOpType_Add, planpb.ArithOpType_Mul:
			return combineBinaryArithExpr(op, arithOp, rightExpr.GetInfo(), leftValue.GetValue(), valueExpr.GetValue()), nil
		default:
			return nil, fmt.Errorf("module field is not yet supported")
		}
	} else {
		// (a + b) / 2 == 3
		return nil, fmt.Errorf("complicated arithmetic operations are not supported")
	}
}

func handleCompareRightValue(op planpb.OpType, left *ExprWithType, right *planpb.ValueExpr) (*planpb.Expr, error) {
	dataType := left.dataType
	if typeutil.IsArrayType(dataType) && len(toColumnInfo(left).GetNestedPath()) != 0 {
		dataType = toColumnInfo(left).GetElementType()
	}
	castedValue, err := castValue(dataType, right.GetValue())
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
	both := (typeutil.IsStringType(t1) || typeutil.IsJSONType(t1)) && (typeutil.IsStringType(t2) || typeutil.IsJSONType(t2))
	neither := !typeutil.IsStringType(t1) && !typeutil.IsStringType(t2)
	return both || neither
}

func canBeComparedDataType(left, right schemapb.DataType) bool {
	switch left {
	case schemapb.DataType_Bool:
		return typeutil.IsBoolType(right) || typeutil.IsJSONType(right)
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double:
		return typeutil.IsArithmetic(right) || typeutil.IsJSONType(right)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return typeutil.IsStringType(right) || typeutil.IsJSONType(right)
	case schemapb.DataType_JSON:
		return true
	default:
		return false
	}
}

func getArrayElementType(expr *ExprWithType) schemapb.DataType {
	if columnInfo := toColumnInfo(expr); columnInfo != nil {
		return columnInfo.GetElementType()
	}
	if valueExpr := expr.expr.GetValueExpr(); valueExpr != nil {
		return valueExpr.GetValue().GetArrayVal().GetElementType()
	}
	return schemapb.DataType_None
}

func canBeCompared(left, right *ExprWithType) bool {
	if !typeutil.IsArrayType(left.dataType) && !typeutil.IsArrayType(right.dataType) {
		return canBeComparedDataType(left.dataType, right.dataType)
	}
	if typeutil.IsArrayType(left.dataType) && typeutil.IsArrayType(right.dataType) {
		return canBeComparedDataType(getArrayElementType(left), getArrayElementType(right))
	}
	if typeutil.IsArrayType(left.dataType) {
		return canBeComparedDataType(getArrayElementType(left), right.dataType)
	}
	return canBeComparedDataType(left.dataType, getArrayElementType(right))
}

func getDataType(expr *ExprWithType) string {
	if typeutil.IsArrayType(expr.dataType) {
		return fmt.Sprintf("%s[%s]", expr.dataType, getArrayElementType(expr))
	}
	return expr.dataType.String()
}

func HandleCompare(op int, left, right *ExprWithType) (*planpb.Expr, error) {
	if !canBeCompared(left, right) {
		return nil, fmt.Errorf("comparisons between %s and %s are not supported",
			getDataType(left), getDataType(right))
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
	}
	return handleCompare(cmpOp, left, right)
}

func isEmptyExpression(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

func isAlwaysTrueExpr(e *planpb.Expr) bool {
	return e.GetAlwaysTrueExpr() != nil
}

func alwaysTrueExpr() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_AlwaysTrueExpr{
			AlwaysTrueExpr: &planpb.AlwaysTrueExpr{},
		},
	}
}

func IsAlwaysTruePlan(plan *planpb.PlanNode) bool {
	switch realPlan := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		return isAlwaysTrueExpr(realPlan.VectorAnns.GetPredicates())
	case *planpb.PlanNode_Predicates:
		return isAlwaysTrueExpr(realPlan.Predicates)
	case *planpb.PlanNode_Query:
		return !realPlan.Query.GetIsCount() && isAlwaysTrueExpr(realPlan.Query.GetPredicates())
	}
	return false
}

func canBeExecuted(e *ExprWithType) bool {
	return typeutil.IsBoolType(e.dataType) && !e.nodeDependent
}

func convertEscapeSingle(literal string) (string, error) {
	needReplaceIndex := make([]int, 0)
	escapeChCount := 0
	stringLength := len(literal)
	newStringLength := 2
	for i := 1; i < stringLength-1; i++ {
		newStringLength++
		if literal[i] == '\\' {
			escapeChCount++
			continue
		}
		if literal[i] == '"' && escapeChCount%2 == 0 {
			needReplaceIndex = append(needReplaceIndex, i)
			newStringLength++
		}
		if literal[i] == '\'' && escapeChCount%2 != 0 {
			needReplaceIndex = append(needReplaceIndex, i)
			newStringLength--
		}
		escapeChCount = 0
	}
	var b strings.Builder
	b.Grow(newStringLength)
	b.WriteString(`"`)
	needReplaceIndexLength := len(needReplaceIndex)
	start, end := 1, 0
	for i := 0; i < needReplaceIndexLength; i++ {
		end = needReplaceIndex[i]
		if literal[end] == '"' {
			b.WriteString(literal[start:end])
			b.WriteString(`\"`)
		} else {
			b.WriteString(literal[start : end-1])
			b.WriteString(`'`)
		}
		start = end + 1
	}
	b.WriteString(literal[end+1 : len(literal)-1])
	b.WriteString(`"`)
	return strconv.Unquote(b.String())
}

func canArithmeticDataType(left, right schemapb.DataType) bool {
	switch left {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double:
		return typeutil.IsArithmetic(right) || typeutil.IsJSONType(right)
	case schemapb.DataType_JSON:
		return typeutil.IsArithmetic(right)
	default:
		return false
	}
}

func canArithmetic(left *ExprWithType, right *ExprWithType) bool {
	if !typeutil.IsArrayType(left.dataType) && !typeutil.IsArrayType(right.dataType) {
		return canArithmeticDataType(left.dataType, right.dataType)
	}
	if typeutil.IsArrayType(left.dataType) && typeutil.IsArrayType(right.dataType) {
		return canArithmeticDataType(getArrayElementType(left), getArrayElementType(right))
	}
	if typeutil.IsArrayType(left.dataType) {
		return canArithmeticDataType(getArrayElementType(left), right.dataType)
	}
	return canArithmeticDataType(left.dataType, getArrayElementType(right))
}

func isIntegerColumn(col *planpb.ColumnInfo) bool {
	return typeutil.IsIntegerType(col.GetDataType()) ||
		(typeutil.IsArrayType(col.GetDataType()) && typeutil.IsIntegerType(col.GetElementType())) ||
		typeutil.IsJSONType(col.GetDataType())
}
