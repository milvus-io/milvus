package planparserv2

import (
	"fmt"
	"strconv"

	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type ParserVisitor struct {
	parser.BasePlanVisitor
	schema *typeutil.SchemaHelper
}

func NewParserVisitor(schema *typeutil.SchemaHelper) *ParserVisitor {
	return &ParserVisitor{schema: schema}
}

// VisitParens unpack the parentheses.
func (v *ParserVisitor) VisitParens(ctx *parser.ParensContext) interface{} {
	return ctx.Expr().Accept(v)
}

func (v *ParserVisitor) translateIdentifier(identifier string) (*ExprWithType, error) {
	field, err := v.schema.GetFieldFromName(identifier)
	if err != nil {
		return nil, err
	}
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ColumnExpr{
				ColumnExpr: &planpb.ColumnExpr{
					Info: &planpb.ColumnInfo{
						FieldId:      field.FieldID,
						DataType:     field.DataType,
						IsPrimaryKey: field.IsPrimaryKey,
						IsAutoID:     field.AutoID,
					},
				},
			},
		},
		dataType: field.DataType,
	}, nil
}

// VisitIdentifier translates expr to column plan.
func (v *ParserVisitor) VisitIdentifier(ctx *parser.IdentifierContext) interface{} {
	identifier := ctx.Identifier().GetText()
	expr, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}
	return expr
}

// VisitBoolean translates expr to GenericValue.
func (v *ParserVisitor) VisitBoolean(ctx *parser.BooleanContext) interface{} {
	literal := ctx.BooleanConstant().GetText()
	b, err := strconv.ParseBool(literal)
	if err != nil {
		return err
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Bool,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewBool(b),
				},
			},
		},
	}
}

// VisitInteger translates expr to GenericValue.
func (v *ParserVisitor) VisitInteger(ctx *parser.IntegerContext) interface{} {
	literal := ctx.IntegerConstant().GetText()
	i, err := strconv.ParseInt(literal, 0, 64)
	if err != nil {
		return err
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Int64,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewInt(i),
				},
			},
		},
	}
}

// VisitFloating translates expr to GenericValue.
func (v *ParserVisitor) VisitFloating(ctx *parser.FloatingContext) interface{} {
	literal := ctx.FloatingConstant().GetText()
	f, err := strconv.ParseFloat(literal, 64)
	if err != nil {
		return err
	}
	return &ExprWithType{
		dataType: schemapb.DataType_Double,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewFloat(f),
				},
			},
		},
	}
}

// VisitString translates expr to GenericValue.
func (v *ParserVisitor) VisitString(ctx *parser.StringContext) interface{} {
	literal, err := strconv.Unquote(ctx.StringLiteral().GetText())
	if err != nil {
		return err
	}
	return &ExprWithType{
		dataType: schemapb.DataType_VarChar,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewString(literal),
				},
			},
		},
	}
}

// VisitAddSub translates expr to arithmetic plan.
func (v *ParserVisitor) VisitAddSub(ctx *parser.AddSubContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserADD:
			return Add(leftValue, rightValue)
		case parser.PlanParserSUB:
			return Subtract(leftValue, rightValue)
		default:
			return fmt.Errorf("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	reverse := true

	if leftValue != nil {
		leftExpr = toValueExpr(leftValue)
	} else {
		reverse = false
		leftExpr = getExpr(left)
	}
	if rightValue != nil {
		rightExpr = toValueExpr(rightValue)
	} else {
		rightExpr = getExpr(right)
	}

	if leftExpr == nil || rightExpr == nil {
		return fmt.Errorf("invalid arithmetic expression, left: %s, op: %s, right: %s", ctx.Expr(0).GetText(), ctx.GetOp(), ctx.Expr(1).GetText())
	}

	if !typeutil.IsArithmetic(leftExpr.dataType) || !typeutil.IsArithmetic(rightExpr.dataType) {
		return fmt.Errorf("'%s' can only be used between integer or floating expressions", arithNameMap[ctx.GetOp().GetTokenType()])
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    arithExprMap[ctx.GetOp().GetTokenType()],
			},
		},
	}
	dataType, err := calcDataType(leftExpr, rightExpr, reverse)
	if err != nil {
		return err
	}
	return &ExprWithType{
		expr:     expr,
		dataType: dataType,
	}
}

// VisitMulDivMod translates expr to arithmetic plan.
func (v *ParserVisitor) VisitMulDivMod(ctx *parser.MulDivModContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserMUL:
			return Multiply(leftValue, rightValue)
		case parser.PlanParserDIV:
			n, err := Divide(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserMOD:
			n, err := Modulo(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		default:
			return fmt.Errorf("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	reverse := true

	if leftValue != nil {
		leftExpr = toValueExpr(leftValue)
	} else {
		leftExpr = getExpr(left)
		reverse = false
	}
	if rightValue != nil {
		rightExpr = toValueExpr(rightValue)
	} else {
		rightExpr = getExpr(right)
	}

	if leftExpr == nil || rightExpr == nil {
		return fmt.Errorf("invalid arithmetic expression, left: %s, op: %s, right: %s", ctx.Expr(0).GetText(), ctx.GetOp(), ctx.Expr(1).GetText())
	}

	if !typeutil.IsArithmetic(leftExpr.dataType) || !typeutil.IsArithmetic(rightExpr.dataType) {
		return fmt.Errorf("'%s' can only be used between integer or floating expressions", arithNameMap[ctx.GetOp().GetTokenType()])
	}

	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserMOD:
		if !typeutil.IsIntegerType(leftExpr.dataType) || !typeutil.IsIntegerType(rightExpr.dataType) {
			return fmt.Errorf("modulo can only apply on integer types")
		}
	default:
		break
	}
	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    arithExprMap[ctx.GetOp().GetTokenType()],
			},
		},
	}
	dataType, err := calcDataType(leftExpr, rightExpr, reverse)
	if err != nil {
		return err
	}
	return &ExprWithType{
		expr:     expr,
		dataType: dataType,
	}
}

// VisitEquality translates expr to compare/range plan.
func (v *ParserVisitor) VisitEquality(ctx *parser.EqualityContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserEQ:
			return Equal(leftValue, rightValue)
		case parser.PlanParserNE:
			return NotEqual(leftValue, rightValue)
		default:
			return fmt.Errorf("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	if leftValue != nil {
		leftExpr = toValueExpr(leftValue)
	} else {
		leftExpr = getExpr(left)
	}
	if rightValue != nil {
		rightExpr = toValueExpr(rightValue)
	} else {
		rightExpr = getExpr(right)
	}

	expr, err := HandleCompare(ctx.GetOp().GetTokenType(), leftExpr, rightExpr)
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitRelational translates expr to range/compare plan.
func (v *ParserVisitor) VisitRelational(ctx *parser.RelationalContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserLT:
			return Less(leftValue, rightValue)
		case parser.PlanParserLE:
			return LessEqual(leftValue, rightValue)
		case parser.PlanParserGT:
			return Greater(leftValue, rightValue)
		case parser.PlanParserGE:
			return GreaterEqual(leftValue, rightValue)
		default:
			return fmt.Errorf("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	if leftValue != nil {
		leftExpr = toValueExpr(leftValue)
	} else {
		leftExpr = getExpr(left)
	}
	if rightValue != nil {
		rightExpr = toValueExpr(rightValue)
	} else {
		rightExpr = getExpr(right)
	}

	expr, err := HandleCompare(ctx.GetOp().GetTokenType(), leftExpr, rightExpr)
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitLike handles match operations.
func (v *ParserVisitor) VisitLike(ctx *parser.LikeContext) interface{} {
	left := ctx.Expr().Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	leftExpr := getExpr(left)
	if leftExpr == nil {
		return fmt.Errorf("the left operand of like is invalid")
	}

	if !typeutil.IsStringType(leftExpr.dataType) {
		return fmt.Errorf("like operation on non-string field is unsupported")
	}

	column := toColumnInfo(leftExpr)
	if column == nil {
		return fmt.Errorf("like operation on complicated expr is unsupported")
	}

	pattern, err := strconv.Unquote(ctx.StringLiteral().GetText())
	if err != nil {
		return err
	}

	op, operand, err := translatePatternMatch(pattern)
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_UnaryRangeExpr{
				UnaryRangeExpr: &planpb.UnaryRangeExpr{
					ColumnInfo: column,
					Op:         op,
					Value:      NewString(operand),
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

// VisitTerm translates expr to term plan.
func (v *ParserVisitor) VisitTerm(ctx *parser.TermContext) interface{} {
	child := ctx.Expr(0).Accept(v)
	if err := getError(child); err != nil {
		return err
	}

	if childValue := getGenericValue(child); childValue != nil {
		return fmt.Errorf("'term' can only be used on non-const expression, but got: %s", ctx.Expr(0).GetText())
	}

	childExpr := getExpr(child)
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return fmt.Errorf("'term' can only be used on single field, but got: %s", ctx.Expr(0).GetText())
	}

	allExpr := ctx.AllExpr()
	lenOfAllExpr := len(allExpr)
	values := make([]*planpb.GenericValue, 0, lenOfAllExpr)
	for i := 1; i < lenOfAllExpr; i++ {
		term := allExpr[i].Accept(v)
		if getError(term) != nil {
			return term
		}
		n := getGenericValue(term)
		if n == nil {
			return fmt.Errorf("value '%s' in list cannot be a non-const expression", ctx.Expr(i).GetText())
		}
		castedValue, err := castValue(childExpr.dataType, n)
		if err != nil {
			return fmt.Errorf("value '%s' in list cannot be casted to %s", ctx.Expr(i).GetText(), childExpr.dataType.String())
		}
		values = append(values, castedValue)
	}
	if len(values) <= 0 {
		return fmt.Errorf("'term' has empty value list")
	}
	expr := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: columnInfo,
				Values:     values,
			},
		},
	}
	if ctx.GetOp().GetTokenType() == parser.PlanParserNIN {
		expr = &planpb.Expr{
			Expr: &planpb.Expr_UnaryExpr{
				UnaryExpr: &planpb.UnaryExpr{
					Op:    planpb.UnaryExpr_Not,
					Child: expr,
				},
			},
		}
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitEmptyTerm translates expr to term plan.
func (v *ParserVisitor) VisitEmptyTerm(ctx *parser.EmptyTermContext) interface{} {
	child := ctx.Expr().Accept(v)
	if err := getError(child); err != nil {
		return err
	}

	if childValue := getGenericValue(child); childValue != nil {
		return fmt.Errorf("'term' can only be used on non-const expression, but got: %s", ctx.Expr().GetText())
	}

	childExpr := getExpr(child)
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return fmt.Errorf("'term' can only be used on single field, but got: %s", ctx.Expr().GetText())
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: columnInfo,
				Values:     nil,
			},
		},
	}
	if ctx.GetOp().GetTokenType() == parser.PlanParserNIN {
		expr = &planpb.Expr{
			Expr: &planpb.Expr_UnaryExpr{
				UnaryExpr: &planpb.UnaryExpr{
					Op:    planpb.UnaryExpr_Not,
					Child: expr,
				},
			},
		}
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitRange translates expr to range plan.
func (v *ParserVisitor) VisitRange(ctx *parser.RangeContext) interface{} {
	identifier := ctx.Identifier().GetText()
	childExpr, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}

	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return fmt.Errorf("range operations are only supported on single fields now, got: %s", ctx.Expr(1).GetText())
	}

	lower := ctx.Expr(0).Accept(v)
	upper := ctx.Expr(1).Accept(v)
	if err := getError(lower); err != nil {
		return err
	}
	if err := getError(upper); err != nil {
		return err
	}

	lowerValue := getGenericValue(lower)
	upperValue := getGenericValue(upper)
	if lowerValue == nil {
		return fmt.Errorf("lowerbound cannot be a non-const expression: %s", ctx.Expr(0).GetText())
	}
	if upperValue == nil {
		return fmt.Errorf("upperbound cannot be a non-const expression: %s", ctx.Expr(1).GetText())
	}

	switch childExpr.dataType {
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		if !IsString(lowerValue) || !IsString(upperValue) {
			return fmt.Errorf("invalid range operations")
		}
	case schemapb.DataType_Bool:
		return fmt.Errorf("invalid range operations on boolean expr")
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		if !IsInteger(lowerValue) || !IsInteger(upperValue) {
			return fmt.Errorf("invalid range operations")
		}
	case schemapb.DataType_Float, schemapb.DataType_Double:
		if !IsNumber(lowerValue) || !IsNumber(upperValue) {
			return fmt.Errorf("invalid range operations")
		}
		if IsInteger(lowerValue) {
			lowerValue = NewFloat(float64(lowerValue.GetInt64Val()))
		}
		if IsInteger(upperValue) {
			upperValue = NewFloat(float64(upperValue.GetInt64Val()))
		}
	}

	lowerInclusive := ctx.GetOp1().GetTokenType() == parser.PlanParserLE
	upperInclusive := ctx.GetOp2().GetTokenType() == parser.PlanParserLE

	// if !(lowerInclusive && upperInclusive) {
	// 	if getGenericValue(GreaterEqual(lowerValue, upperValue)).GetBoolVal() {
	// 		return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
	// 	}
	// } else {
	// 	if getGenericValue(Greater(lowerValue, upperValue)).GetBoolVal() {
	// 		return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
	// 	}
	// }

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:     columnInfo,
				LowerInclusive: lowerInclusive,
				UpperInclusive: upperInclusive,
				LowerValue:     lowerValue,
				UpperValue:     upperValue,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitReverseRange parses the expression like "1 > a > 0".
func (v *ParserVisitor) VisitReverseRange(ctx *parser.ReverseRangeContext) interface{} {
	identifier := ctx.Identifier().GetText()
	childExpr, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}

	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return fmt.Errorf("range operations are only supported on single fields now, got: %s", ctx.Expr(1).GetText())
	}

	lower := ctx.Expr(1).Accept(v)
	upper := ctx.Expr(0).Accept(v)
	if err := getError(lower); err != nil {
		return err
	}
	if err := getError(upper); err != nil {
		return err
	}

	lowerValue := getGenericValue(lower)
	upperValue := getGenericValue(upper)
	if lowerValue == nil {
		return fmt.Errorf("lowerbound cannot be a non-const expression: %s", ctx.Expr(0).GetText())
	}
	if upperValue == nil {
		return fmt.Errorf("upperbound cannot be a non-const expression: %s", ctx.Expr(1).GetText())
	}

	switch childExpr.dataType {
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		if !IsString(lowerValue) || !IsString(upperValue) {
			return fmt.Errorf("invalid range operations")
		}
	case schemapb.DataType_Bool:
		return fmt.Errorf("invalid range operations on boolean expr")
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		if !IsInteger(lowerValue) || !IsInteger(upperValue) {
			return fmt.Errorf("invalid range operations")
		}
	case schemapb.DataType_Float, schemapb.DataType_Double:
		if !IsNumber(lowerValue) || !IsNumber(upperValue) {
			return fmt.Errorf("invalid range operations")
		}
		if IsInteger(lowerValue) {
			lowerValue = NewFloat(float64(lowerValue.GetInt64Val()))
		}
		if IsInteger(upperValue) {
			upperValue = NewFloat(float64(upperValue.GetInt64Val()))
		}
	}

	lowerInclusive := ctx.GetOp2().GetTokenType() == parser.PlanParserGE
	upperInclusive := ctx.GetOp1().GetTokenType() == parser.PlanParserGE

	// if !(lowerInclusive && upperInclusive) {
	// 	if getGenericValue(GreaterEqual(lowerValue, upperValue)).GetBoolVal() {
	// 		return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
	// 	}
	// } else {
	// 	if getGenericValue(Greater(lowerValue, upperValue)).GetBoolVal() {
	// 		return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
	// 	}
	// }

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:     columnInfo,
				LowerInclusive: lowerInclusive,
				UpperInclusive: upperInclusive,
				LowerValue:     lowerValue,
				UpperValue:     upperValue,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitUnary unpack the +expr to expr.
func (v *ParserVisitor) VisitUnary(ctx *parser.UnaryContext) interface{} {
	child := ctx.Expr().Accept(v)
	if err := getError(child); err != nil {
		return err
	}

	childValue := getGenericValue(child)
	if childValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserADD:
			return child
		case parser.PlanParserSUB:
			return Negative(childValue)
		case parser.PlanParserNOT:
			return Not(childValue)
		default:
			return fmt.Errorf("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	childExpr := getExpr(child)
	if childExpr == nil {
		return fmt.Errorf("failed to parse unary expressions")
	}
	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserADD:
		return childExpr
	case parser.PlanParserNOT:
		if !typeutil.IsBoolType(childExpr.dataType) {
			return fmt.Errorf("%s op can only be applied on boolean expression", unaryLogicalNameMap[parser.PlanParserNOT])
		}
		return &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_UnaryExpr{
					UnaryExpr: &planpb.UnaryExpr{
						Op:    unaryLogicalOpMap[parser.PlanParserNOT],
						Child: childExpr.expr,
					},
				},
			},
			dataType: schemapb.DataType_Bool,
		}
	default:
		return fmt.Errorf("unexpected op: %s", ctx.GetOp().GetText())
	}
}

// VisitLogicalOr apply logical or to two boolean expressions.
func (v *ParserVisitor) VisitLogicalOr(ctx *parser.LogicalOrContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}
	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		n, err := Or(leftValue, rightValue)
		if err != nil {
			return err
		}
		return n
	}

	if leftValue != nil || rightValue != nil {
		return fmt.Errorf("'or' can only be used between boolean expressions")
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	leftExpr = getExpr(left)
	rightExpr = getExpr(right)

	if !typeutil.IsBoolType(leftExpr.dataType) || !typeutil.IsBoolType(rightExpr.dataType) {
		return fmt.Errorf("'or' can only be used between boolean expressions")
	}
	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    planpb.BinaryExpr_LogicalOr,
			},
		},
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitLogicalAnd apply logical and to two boolean expressions.
func (v *ParserVisitor) VisitLogicalAnd(ctx *parser.LogicalAndContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}
	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		n, err := And(leftValue, rightValue)
		if err != nil {
			return err
		}
		return n
	}

	if leftValue != nil || rightValue != nil {
		return fmt.Errorf("'and' can only be used between boolean expressions")
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	leftExpr = getExpr(left)
	rightExpr = getExpr(right)

	if !typeutil.IsBoolType(leftExpr.dataType) || !typeutil.IsBoolType(rightExpr.dataType) {
		return fmt.Errorf("'and' can only be used between boolean expressions")
	}
	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    planpb.BinaryExpr_LogicalAnd,
			},
		},
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitBitXor not supported.
func (v *ParserVisitor) VisitBitXor(ctx *parser.BitXorContext) interface{} {
	return fmt.Errorf("BitXor is not supported: %s", ctx.GetText())
}

// VisitBitAnd not supported.
func (v *ParserVisitor) VisitBitAnd(ctx *parser.BitAndContext) interface{} {
	return fmt.Errorf("BitAnd is not supported: %s", ctx.GetText())
}

// VisitPower parses power expression.
func (v *ParserVisitor) VisitPower(ctx *parser.PowerContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		return Power(leftValue, rightValue)
	}

	return fmt.Errorf("power can only apply on constants: %s", ctx.GetText())
}

// VisitShift unsupported.
func (v *ParserVisitor) VisitShift(ctx *parser.ShiftContext) interface{} {
	return fmt.Errorf("shift is not supported: %s", ctx.GetText())
}

// VisitBitOr unsupported.
func (v *ParserVisitor) VisitBitOr(ctx *parser.BitOrContext) interface{} {
	return fmt.Errorf("BitOr is not supported: %s", ctx.GetText())
}
