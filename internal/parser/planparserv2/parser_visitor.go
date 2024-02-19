package planparserv2

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	field, err := v.schema.GetFieldFromNameDefaultJSON(identifier)
	if err != nil {
		return nil, merr.WrapErrParseExprFailed(nil, identifier, err.Error())
	}
	var nestedPath []string
	if identifier != field.Name {
		nestedPath = append(nestedPath, identifier)
	}
	if typeutil.IsJSONType(field.DataType) && len(nestedPath) == 0 {
		return nil, merr.WrapErrParseExprFailed(nil, identifier, "JSON field cannot be used for comparison directly, only their specific keys can be compared")
	}
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ColumnExpr{
				ColumnExpr: &planpb.ColumnExpr{
					Info: &planpb.ColumnInfo{
						FieldId:        field.FieldID,
						DataType:       field.DataType,
						IsPrimaryKey:   field.IsPrimaryKey,
						IsAutoID:       field.AutoID,
						NestedPath:     nestedPath,
						IsPartitionKey: field.IsPartitionKey,
						ElementType:    field.GetElementType(),
					},
				},
			},
		},
		dataType:      field.DataType,
		nodeDependent: true,
	}, nil
}

// VisitIdentifier translates expr to column plan.
func (v *ParserVisitor) VisitIdentifier(ctx *parser.IdentifierContext) interface{} {
	identifier := ctx.Identifier().GetText()
	expr, err := v.translateIdentifier(identifier)
	if err != nil {
		return merr.WrapErrParseExprFailed(err, identifier, err.Error())
	}
	return expr
}

// VisitBoolean translates expr to GenericValue.
func (v *ParserVisitor) VisitBoolean(ctx *parser.BooleanContext) interface{} {
	literal := ctx.BooleanConstant().GetText()
	b, err := strconv.ParseBool(literal)
	if err != nil {
		return merr.WrapErrParseExprFailed(err, literal, err.Error())
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
		nodeDependent: true,
	}
}

// VisitInteger translates expr to GenericValue.
func (v *ParserVisitor) VisitInteger(ctx *parser.IntegerContext) interface{} {
	literal := ctx.IntegerConstant().GetText()
	i, err := strconv.ParseInt(literal, 0, 64)
	if err != nil {
		return merr.WrapErrParseExprFailed(err, literal, err.Error())
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
		nodeDependent: true,
	}
}

// VisitFloating translates expr to GenericValue.
func (v *ParserVisitor) VisitFloating(ctx *parser.FloatingContext) interface{} {
	literal := ctx.FloatingConstant().GetText()
	f, err := strconv.ParseFloat(literal, 64)
	if err != nil {
		return merr.WrapErrParseExprFailed(err, literal, err.Error())
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
		nodeDependent: true,
	}
}

// VisitString translates expr to GenericValue.
func (v *ParserVisitor) VisitString(ctx *parser.StringContext) interface{} {
	literal := ctx.StringLiteral().GetText()
	pattern, err := convertEscapeSingle(literal)
	if err != nil {
		return merr.WrapErrParseExprFailed(err, literal, err.Error())
	}
	return &ExprWithType{
		dataType: schemapb.DataType_VarChar,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewString(pattern),
				},
			},
		},
		nodeDependent: true,
	}
}

func checkDirectComparisonBinaryField(columnInfo *planpb.ColumnInfo, expr string) error {
	if typeutil.IsArrayType(columnInfo.GetDataType()) && len(columnInfo.GetNestedPath()) == 0 {
		return merr.WrapErrParseExprFailed(nil, expr, "Array field cannot be used for comparison directly, only the elements corresponding to their specific index can be compared.")
	}
	return nil
}

// VisitAddSub translates expr to arithmetic plan.
func (v *ParserVisitor) VisitAddSub(ctx *parser.AddSubContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(1).GetText(), err.Error())
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserADD:
			n, err := Add(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserSUB:
			n, err := Subtract(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		default:
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(), fmt.Sprintf("unexpected op: %s", ctx.GetOp().GetText()))
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
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), ctx.Expr(0).GetText(), ctx.Expr(1).GetText(), ctx.GetOp().GetText())
	}

	if err := checkDirectComparisonBinaryField(toColumnInfo(leftExpr), ctx.GetText()); err != nil {
		return err
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(rightExpr), ctx.GetText()); err != nil {
		return err
	}
	if !canArithmetic(leftExpr, rightExpr) {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), ctx.GetOp().GetText())
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
	dataType, compatible := calcDataType(leftExpr, rightExpr, reverse)
	if !compatible {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), ctx.GetOp().GetText())
	}
	return &ExprWithType{
		expr:          expr,
		dataType:      dataType,
		nodeDependent: true,
	}
}

// VisitMulDivMod translates expr to arithmetic plan.
func (v *ParserVisitor) VisitMulDivMod(ctx *parser.MulDivModContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(1).GetText(), err.Error())
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserMUL:
			n, err := Multiply(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
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
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(), fmt.Sprintf("unexpected op: %s", ctx.GetOp().GetText()))
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

	if rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserDIV, parser.PlanParserMOD:
			if rightValue.GetFloatVal() == 0 && rightValue.GetInt64Val() == 0 {
				return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), "Zero", ctx.GetOp().GetText())
			}
		default:
			break
		}
	}

	if leftExpr == nil || rightExpr == nil {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), ctx.Expr(0).GetText(), ctx.Expr(1).GetText(), ctx.GetOp().GetText())
	}

	if err := checkDirectComparisonBinaryField(toColumnInfo(leftExpr), ctx.GetText()); err != nil {
		return err
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(rightExpr), ctx.GetText()); err != nil {
		return err
	}
	if !canArithmetic(leftExpr, rightExpr) {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), ctx.GetOp().GetText())
	}

	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserMOD:
		if !isIntegerColumn(toColumnInfo(leftExpr)) && !isIntegerColumn(toColumnInfo(rightExpr)) {
			return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), ctx.GetOp().GetText())
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
	dataType, compatible := calcDataType(leftExpr, rightExpr, reverse)
	if !compatible {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), ctx.GetOp().GetText())
	}
	return &ExprWithType{
		expr:          expr,
		dataType:      dataType,
		nodeDependent: true,
	}
}

// VisitEquality translates expr to compare/range plan.
func (v *ParserVisitor) VisitEquality(ctx *parser.EqualityContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(1).GetText(), err.Error())
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserEQ:
			n, err := Equal(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserNE:
			n, err := NotEqual(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		default:
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(), fmt.Sprintf("unexpected op: %s", ctx.GetOp().GetText()))
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
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(1).GetText(), err.Error())
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)

	if leftValue != nil && rightValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserLT:
			n, err := Less(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserLE:
			n, err := LessEqual(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserGT:
			n, err := Greater(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserGE:
			n, err := GreaterEqual(leftValue, rightValue)
			if err != nil {
				return err
			}
			return n
		default:
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(), fmt.Sprintf("unexpected op: %s", ctx.GetOp().GetText()))
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
	if err := checkDirectComparisonBinaryField(toColumnInfo(leftExpr), ctx.GetText()); err != nil {
		return err
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(rightExpr), ctx.GetText()); err != nil {
		return err
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
		return merr.WrapErrParseExprFailed(err, ctx.Expr().GetText(), err.Error())
	}

	leftExpr := getExpr(left)
	if leftExpr == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "the left operand of like is invalid")
	}

	column := toColumnInfo(leftExpr)
	if column == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "like operation on complicated expr is unsupported")
	}
	if err := checkDirectComparisonBinaryField(column, ctx.GetText()); err != nil {
		return err
	}

	if !typeutil.IsStringType(leftExpr.dataType) && !typeutil.IsJSONType(leftExpr.dataType) &&
		!(typeutil.IsArrayType(leftExpr.dataType) && typeutil.IsStringType(column.GetElementType())) {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), "Varchar", "Like")
	}

	pattern, err := convertEscapeSingle(ctx.StringLiteral().GetText())
	if err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}

	op, operand, err := translatePatternMatch(pattern)
	if err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
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
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}

	if childValue := getGenericValue(child); childValue != nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "'term' can only be used on non-const expression")
	}

	childExpr := getExpr(child)
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "'term' can only be used on single field")
	}

	dataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(dataType) && len(columnInfo.GetNestedPath()) != 0 {
		dataType = columnInfo.GetElementType()
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
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(), fmt.Sprintf("value '%s' in list cannot be a non-const expression", ctx.Expr(i).GetText()))
		}
		castedValue, err := castValue(dataType, n)
		if err != nil {
			return merr.WrapErrParseExprFailed(err, ctx.GetText(), fmt.Sprintf("value '%s' in list cannot be casted to %s", ctx.Expr(i).GetText(), dataType.String()))
		}
		values = append(values, castedValue)
	}
	if len(values) <= 0 {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "'term' has empty value list")
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
		return merr.WrapErrParseExprFailed(err, ctx.Expr().GetText(), err.Error())
	}

	if childValue := getGenericValue(child); childValue != nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "'term' can only be used on non-const expression")
	}

	childExpr := getExpr(child)
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "'term' can only be used on single field")
	}
	if err := checkDirectComparisonBinaryField(columnInfo, ctx.GetText()); err != nil {
		return err
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

func (v *ParserVisitor) getChildColumnInfo(identifier, child antlr.TerminalNode) (*planpb.ColumnInfo, error) {
	if identifier != nil {
		childExpr, err := v.translateIdentifier(identifier.GetText())
		if err != nil {
			return nil, err
		}
		return toColumnInfo(childExpr), nil
	}

	return v.getColumnInfoFromJSONIdentifier(child.GetText())
}

func checkBoundValue(lowerValue, upperValue *planpb.GenericValue, columnInfo *planpb.ColumnInfo, expr, op1, op2 string) (*planpb.GenericValue, *planpb.GenericValue, error) {
	if lowerValue == nil {
		return lowerValue, upperValue, merr.WrapErrParseExprFailed(nil, expr, "lower bound cannot be a non-const expression")
	}
	if upperValue == nil {
		return lowerValue, upperValue, merr.WrapErrParseExprFailed(nil, expr, "upper bound cannot be a non-const expression")
	}

	fieldDataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		fieldDataType = columnInfo.GetElementType()
	}

	switch fieldDataType {
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		if !IsString(lowerValue) {
			return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, GetGenericValueType(lowerValue), fieldDataType.String(), op1)
		}
		if !IsString(upperValue) {
			return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, fieldDataType.String(), GetGenericValueType(upperValue), op2)
		}
	case schemapb.DataType_Bool:
		return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, GetGenericValueType(lowerValue), fieldDataType.String(), op1)
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		if !IsInteger(lowerValue) {
			return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, GetGenericValueType(lowerValue), fieldDataType.String(), op1)
		}
		if !IsInteger(upperValue) {
			return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, fieldDataType.String(), GetGenericValueType(upperValue), op2)
		}
	case schemapb.DataType_Float, schemapb.DataType_Double:
		if !IsNumber(lowerValue) {
			return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, GetGenericValueType(lowerValue), fieldDataType.String(), op1)
		}
		if !IsNumber(upperValue) {
			return lowerValue, upperValue, merr.WrapErrParseExprUnsupported(nil, expr, fieldDataType.String(), GetGenericValueType(upperValue), op2)
		}
		if IsInteger(lowerValue) {
			lowerValue = NewFloat(float64(lowerValue.GetInt64Val()))
		}
		if IsInteger(upperValue) {
			upperValue = NewFloat(float64(upperValue.GetInt64Val()))
		}
	}
	return lowerValue, upperValue, nil
}

// VisitRange translates expr to range plan.
func (v *ParserVisitor) VisitRange(ctx *parser.RangeContext) interface{} {
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier())
	if err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	if columnInfo == nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), "range operations are only supported on single fields now")
	}
	if err := checkDirectComparisonBinaryField(columnInfo, ctx.GetText()); err != nil {
		return err
	}

	lower := ctx.Expr(0).Accept(v)
	upper := ctx.Expr(1).Accept(v)
	if err := getError(lower); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	if err := getError(upper); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}

	lowerValue := getGenericValue(lower)
	upperValue := getGenericValue(upper)
	lowerValue, upperValue, err = checkBoundValue(lowerValue, upperValue, columnInfo, ctx.GetText(), ctx.GetOp1().GetText(), ctx.GetOp2().GetText())
	if err != nil {
		return err
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
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier())
	if err != nil {
		return err
	}
	if columnInfo == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "range operations are only supported on single fields now")
	}

	if err := checkDirectComparisonBinaryField(columnInfo, ctx.GetText()); err != nil {
		return err
	}

	lower := ctx.Expr(1).Accept(v)
	upper := ctx.Expr(0).Accept(v)
	if err := getError(lower); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	if err := getError(upper); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}

	lowerValue := getGenericValue(lower)
	upperValue := getGenericValue(upper)
	lowerValue, upperValue, err = checkBoundValue(lowerValue, upperValue, columnInfo, ctx.GetText(), ctx.GetOp1().GetText(), ctx.GetOp2().GetText())
	if err != nil {
		return err
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
		return merr.WrapErrParseExprFailed(err, ctx.Expr().GetText(), err.Error())
	}

	childValue := getGenericValue(child)
	if childValue != nil {
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserADD:
			return child
		case parser.PlanParserSUB:
			n, err := Negative(childValue)
			if err != nil {
				return err
			}
			return n
		case parser.PlanParserNOT:
			b, err := Not(childValue)
			if err != nil {
				return err
			}
			return b
		default:
			return merr.WrapErrParseExprFailed(nil, ctx.Expr().GetText(), "unexpected op")
		}
	}

	childExpr := getExpr(child)
	if childExpr == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.Expr().GetText(), "failed to parse unary expressions")
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(childExpr), ctx.GetText()); err != nil {
		return err
	}
	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserADD:
		return childExpr
	case parser.PlanParserNOT:
		if !canBeExecuted(childExpr) {
			return merr.WrapErrParseExprUnsupported(nil, ctx.Expr().GetText(), "not", childExpr.dataType.String(), ctx.GetOp().GetText())
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
		return merr.WrapErrParseExprFailed(nil, ctx.Expr().GetText(), "unexpected op")
	}
}

// VisitLogicalOr apply logical or to two boolean expressions.
func (v *ParserVisitor) VisitLogicalOr(ctx *parser.LogicalOrContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}
	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(1).GetText(), err.Error())
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
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), GetGenericValueType(leftValue), GetGenericValueType(rightValue), "or")
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	leftExpr = getExpr(left)
	rightExpr = getExpr(right)

	if !canBeExecuted(leftExpr) || !canBeExecuted(rightExpr) {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), "or")
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
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}
	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
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
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), GetGenericValueType(leftValue), GetGenericValueType(rightValue), "and")
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	leftExpr = getExpr(left)
	rightExpr = getExpr(right)

	if !canBeExecuted(leftExpr) || !canBeExecuted(rightExpr) {
		return merr.WrapErrParseExprUnsupported(nil, ctx.GetText(), leftExpr.dataType.String(), rightExpr.dataType.String(), "and")
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
	return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "BitXor is not supported")
}

// VisitBitAnd not supported.
func (v *ParserVisitor) VisitBitAnd(ctx *parser.BitAndContext) interface{} {
	return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "BitAnd is not supported")
}

// VisitPower parses power expression.
func (v *ParserVisitor) VisitPower(ctx *parser.PowerContext) interface{} {
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(0).GetText(), err.Error())
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.Expr(1).GetText(), err.Error())
	}

	leftValue, rightValue := getGenericValue(left), getGenericValue(right)
	if leftValue != nil && rightValue != nil {
		n, err := Power(leftValue, rightValue)
		if err != nil {
			return err
		}
		return n
	}

	return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "power can only apply on constants")
}

// VisitShift unsupported.
func (v *ParserVisitor) VisitShift(ctx *parser.ShiftContext) interface{} {
	return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "Shift is not supported")
}

// VisitBitOr unsupported.
func (v *ParserVisitor) VisitBitOr(ctx *parser.BitOrContext) interface{} {
	return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "BitOr is not supported")
}

// getColumnInfoFromJSONIdentifier parse JSON field name and JSON nested path.
// input: user["name"]["first"],
// output: if user is JSON field name, and fieldID is 102
/*
&planpb.ColumnInfo{
	FieldId:    102,
	DataType:   JSON,
	NestedPath: []string{"name", "first"},
}, nil
*/
// if user is not JSON field name, and $SYS_META fieldID is 102:
/*
&planpb.ColumnInfo{
	FieldId:    102,
	DataType:   JSON,
	NestedPath: []string{"user", "name", "first"},
}, nil
*/
// input: user,
// output: if user is JSON field name, return error.
// if user is not JSON field name, and $SYS_META fieldID is 102:
/*
&planpb.ColumnInfo{
	FieldId:    102,
	DataType:   JSON,
	NestedPath: []string{"user"},
}, nil
*/
// More tests refer to plan_parser_v2_test.go::Test_JSONExpr
func (v *ParserVisitor) getColumnInfoFromJSONIdentifier(identifier string) (*planpb.ColumnInfo, error) {
	fieldName := strings.Split(identifier, "[")[0]
	nestedPath := make([]string, 0)
	field, err := v.schema.GetFieldFromNameDefaultJSON(fieldName)
	if err != nil {
		return nil, err
	}
	if field.GetDataType() != schemapb.DataType_JSON &&
		field.GetDataType() != schemapb.DataType_Array {
		errMsg := fmt.Sprintf("%s data type not supported accessed with []", field.GetDataType())
		return nil, fmt.Errorf(errMsg)
	}
	if fieldName != field.Name {
		nestedPath = append(nestedPath, fieldName)
	}
	jsonKeyStr := identifier[len(fieldName):]
	ss := strings.Split(jsonKeyStr, "][")
	for i := 0; i < len(ss); i++ {
		path := strings.Trim(ss[i], "[]")
		if path == "" {
			return nil, fmt.Errorf("invalid identifier: %s", identifier)
		}
		if (strings.HasPrefix(path, "\"") && strings.HasSuffix(path, "\"")) ||
			(strings.HasPrefix(path, "'") && strings.HasSuffix(path, "'")) {
			path = path[1 : len(path)-1]
			if path == "" {
				return nil, fmt.Errorf("invalid identifier: %s", identifier)
			}
			if typeutil.IsArrayType(field.DataType) {
				return nil, fmt.Errorf("can only access array field with integer index")
			}
		} else if _, err := strconv.ParseInt(path, 10, 64); err != nil {
			return nil, fmt.Errorf("json key must be enclosed in double quotes or single quotes: \"%s\"", path)
		}
		nestedPath = append(nestedPath, path)
	}

	return &planpb.ColumnInfo{
		FieldId:     field.FieldID,
		DataType:    field.DataType,
		NestedPath:  nestedPath,
		ElementType: field.GetElementType(),
	}, nil
}

func (v *ParserVisitor) VisitJSONIdentifier(ctx *parser.JSONIdentifierContext) interface{} {
	field, err := v.getColumnInfoFromJSONIdentifier(ctx.JSONIdentifier().GetText())
	if err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ColumnExpr{
				ColumnExpr: &planpb.ColumnExpr{
					Info: &planpb.ColumnInfo{
						FieldId:     field.GetFieldId(),
						DataType:    field.GetDataType(),
						NestedPath:  field.GetNestedPath(),
						ElementType: field.GetElementType(),
					},
				},
			},
		},
		dataType:      field.GetDataType(),
		nodeDependent: true,
	}
}

func (v *ParserVisitor) VisitExists(ctx *parser.ExistsContext) interface{} {
	child := ctx.Expr().Accept(v)
	if err := getError(child); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	columnInfo := toColumnInfo(child.(*ExprWithType))
	if columnInfo == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "exists operations are only supported on single fields now")
	}

	if columnInfo.GetDataType() != schemapb.DataType_JSON {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "exists operations are only supported on json field")
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ExistsExpr{
				ExistsExpr: &planpb.ExistsExpr{
					Info: &planpb.ColumnInfo{
						FieldId:    columnInfo.GetFieldId(),
						DataType:   columnInfo.GetDataType(),
						NestedPath: columnInfo.GetNestedPath(),
					},
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitArray(ctx *parser.ArrayContext) interface{} {
	allExpr := ctx.AllExpr()
	array := make([]*planpb.GenericValue, 0, len(allExpr))
	dType := schemapb.DataType_None
	sameType := true
	for i := 0; i < len(allExpr); i++ {
		element := allExpr[i].Accept(v)
		if err := getError(element); err != nil {
			return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
		}
		elementValue := getGenericValue(element)
		if elementValue == nil {
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "array element type must be generic value")
		}
		array = append(array, elementValue)

		if dType == schemapb.DataType_None {
			dType = element.(*ExprWithType).dataType
		} else if dType != element.(*ExprWithType).dataType {
			sameType = false
		}
	}
	if !sameType {
		dType = schemapb.DataType_None
	}

	return &ExprWithType{
		dataType: schemapb.DataType_Array,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: &planpb.GenericValue{
						Val: &planpb.GenericValue_ArrayVal{
							ArrayVal: &planpb.Array{
								Array:       array,
								SameType:    sameType,
								ElementType: dType,
							},
						},
					},
				},
			},
		},
		nodeDependent: true,
	}
}

func (v *ParserVisitor) VisitJSONContains(ctx *parser.JSONContainsContext) interface{} {
	field := ctx.Expr(0).Accept(v)
	if err := getError(field); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}

	columnInfo := toColumnInfo(field.(*ExprWithType))
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains operation are only supported on json or array fields now")
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	elementValue := getGenericValue(element)
	if elementValue == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains operation are only supported explicitly specified element")
	}
	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		valExpr := toValueExpr(elementValue)
		if !canBeCompared(field.(*ExprWithType), valExpr) {
			return merr.WrapErrParseExprFailed(nil, ctx.GetText(),
				fmt.Sprintf("contains operation can't compare between type: %s and %s",
					columnInfo.GetElementType(),
					valExpr.dataType))
		}
	}

	elements := make([]*planpb.GenericValue, 1)
	elements[0] = elementValue

	expr := &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:       columnInfo,
				Elements:         elements,
				Op:               planpb.JSONContainsExpr_Contains,
				ElementsSameType: true,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitJSONContainsAll(ctx *parser.JSONContainsAllContext) interface{} {
	field := ctx.Expr(0).Accept(v)
	if err := getError(field); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}

	columnInfo := toColumnInfo(field.(*ExprWithType))
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains_all operation are only supported on json or array fields now")
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	elementValue := getGenericValue(element)
	if elementValue == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains_all operation are only supported explicitly specified element")
	}

	if elementValue.GetArrayVal() == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains_all operation element must be an array")
	}

	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		for _, value := range elementValue.GetArrayVal().GetArray() {
			valExpr := toValueExpr(value)
			if !canBeCompared(field.(*ExprWithType), valExpr) {
				return merr.WrapErrParseExprFailed(nil, ctx.GetText(),
					fmt.Sprintf("contains_all operation can't compare between type: %s and %s",
						columnInfo.GetElementType(),
						valExpr.dataType))
			}
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:       columnInfo,
				Elements:         elementValue.GetArrayVal().GetArray(),
				Op:               planpb.JSONContainsExpr_ContainsAll,
				ElementsSameType: elementValue.GetArrayVal().GetSameType(),
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitJSONContainsAny(ctx *parser.JSONContainsAnyContext) interface{} {
	field := ctx.Expr(0).Accept(v)
	if err := getError(field); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}

	columnInfo := toColumnInfo(field.(*ExprWithType))
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains_any operation are only supported on json or array fields now")
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	elementValue := getGenericValue(element)
	if elementValue == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains_any operation are only supported explicitly specified element")
	}

	if elementValue.GetArrayVal() == nil {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "contains_any operation element must be an array")
	}

	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		for _, value := range elementValue.GetArrayVal().GetArray() {
			valExpr := toValueExpr(value)
			if !canBeCompared(field.(*ExprWithType), valExpr) {
				return merr.WrapErrParseExprFailed(nil, ctx.GetText(),
					fmt.Sprintf("contains_any operation can't compare between type: %s and %s",
						columnInfo.GetElementType(),
						valExpr.dataType))
			}
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:       columnInfo,
				Elements:         elementValue.GetArrayVal().GetArray(),
				Op:               planpb.JSONContainsExpr_ContainsAny,
				ElementsSameType: elementValue.GetArrayVal().GetSameType(),
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitArrayLength(ctx *parser.ArrayLengthContext) interface{} {
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier())
	if err != nil {
		return merr.WrapErrParseExprFailed(err, ctx.GetText(), err.Error())
	}
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return merr.WrapErrParseExprFailed(nil, ctx.GetText(), "array_length operation are only supported on json or array fields now")
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{
				Left: &planpb.Expr{
					Expr: &planpb.Expr_ColumnExpr{
						ColumnExpr: &planpb.ColumnExpr{
							Info: columnInfo,
						},
					},
				},
				Right: nil,
				Op:    planpb.ArithOpType_ArrayLength,
			},
		},
	}
	return &ExprWithType{
		expr:          expr,
		dataType:      schemapb.DataType_Int64,
		nodeDependent: true,
	}
}
