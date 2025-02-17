package planparserv2

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ParserVisitor struct {
	parser.BasePlanVisitor
	schema *typeutil.SchemaHelper
}

func NewParserVisitor(schema *typeutil.SchemaHelper) *ParserVisitor {
	return &ParserVisitor{schema: schema}
}

func (v *ParserVisitor) translateIdentifier(identifier string) (*ExprWithType, error) {
	identifier = decodeUnicode(identifier)
	field, err := v.schema.GetFieldFromNameDefaultJSON(identifier)
	if err != nil {
		return nil, err
	}
	var nestedPath []string
	if identifier != field.Name {
		nestedPath = append(nestedPath, identifier)
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ColumnExpr{
				ColumnExpr: &planpb.ColumnExpr{
					Info: &planpb.ColumnInfo{
						FieldId:         field.FieldID,
						DataType:        field.DataType,
						IsPrimaryKey:    field.IsPrimaryKey,
						IsAutoID:        field.AutoID,
						NestedPath:      nestedPath,
						IsPartitionKey:  field.IsPartitionKey,
						IsClusteringKey: field.IsClusteringKey,
						ElementType:     field.GetElementType(),
						Nullable:        field.GetNullable(),
					},
				},
			},
		},
		dataType:      field.DataType,
		nodeDependent: true,
	}, nil
}

func checkDirectComparisonBinaryField(columnInfo *planpb.ColumnInfo) error {
	if typeutil.IsArrayType(columnInfo.GetDataType()) && len(columnInfo.GetNestedPath()) == 0 {
		return fmt.Errorf("can not comparisons array fields directly")
	}
	return nil
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
	identifier = decodeUnicode(identifier)
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

// VisitExpr a parse tree produced by PlanParser#expr.
func (v *ParserVisitor) VisitExpr(ctx *parser.ExprContext) interface{} {
	expr := ctx.LogicalOrExpr().Accept(v)
	if err := getError(expr); err != nil {
		return err
	}
	return expr
}

// VisitLogicalOrExpr handles logical OR expressions.
func (v *ParserVisitor) VisitLogicalOrExpr(ctx *parser.LogicalOrExprContext) interface{} {
	andExprs := ctx.AllLogicalAndExpr()
	first := andExprs[0].Accept(v)
	if getError(first) != nil {
		return first
	}
	var err error
	expr := getExpr(first)
	for i := 1; i < len(andExprs); i++ {
		right := andExprs[i].Accept(v)
		if getError(right) != nil {
			return right
		}
		leftValue, rightValue := getGenericValue(expr), getGenericValue(right)
		if leftValue != nil && rightValue != nil {
			expr, err = Or(leftValue, rightValue)
			if err != nil {
				return err
			}
			continue
		}
		if leftValue != nil || rightValue != nil {
			return fmt.Errorf("'or' can only be used between boolean expressions")
		}

		if !canBeExecuted(expr) {
			return fmt.Errorf("%s op can only be applied on boolean expression", binaryLogicalNameMap[parser.PlanParserOR])
		}

		rightExpr := getExpr(right)
		if !canBeExecuted(rightExpr) {
			return fmt.Errorf("%s op can only be applied on boolean expression", binaryLogicalNameMap[parser.PlanParserOR])
		}
		expr = &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_BinaryExpr{
					BinaryExpr: &planpb.BinaryExpr{
						Left:  expr.expr,
						Right: rightExpr.expr,
						Op:    planpb.BinaryExpr_LogicalOr,
					},
				},
				IsTemplate: expr.expr.GetIsTemplate() || rightExpr.expr.GetIsTemplate(),
			},
			dataType:      schemapb.DataType_Bool,
			nodeDependent: false,
		}
	}
	return expr
}

// VisitLogicalAndExpr handles logical AND expressions.
func (v *ParserVisitor) VisitLogicalAndExpr(ctx *parser.LogicalAndExprContext) interface{} {
	equalityExprs := ctx.AllEqualityExpr()
	left := equalityExprs[0].Accept(v)
	if getError(left) != nil {
		return left
	}
	var err error
	expr := getExpr(left)
	for i := 1; i < len(equalityExprs); i++ {
		right := equalityExprs[i].Accept(v).(*ExprWithType)
		leftValue, rightValue := getGenericValue(expr), getGenericValue(right)
		if leftValue != nil && rightValue != nil {
			expr, err = Or(leftValue, rightValue)
			if err != nil {
				return err
			}
			continue
		}
		if leftValue != nil || rightValue != nil {
			return fmt.Errorf("'or' can only be used between boolean expressions")
		}
		if !canBeExecuted(expr) {
			return fmt.Errorf("%s op can only be applied on boolean expression", binaryLogicalNameMap[parser.PlanParserOR])
		}
		rightExpr := getExpr(right)
		if !canBeExecuted(rightExpr) {
			return fmt.Errorf("%s op can only be applied on boolean expression", binaryLogicalNameMap[parser.PlanParserOR])
		}
		expr = &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_BinaryExpr{
					BinaryExpr: &planpb.BinaryExpr{
						Left:  expr.expr,
						Right: right.expr,
						Op:    planpb.BinaryExpr_LogicalAnd,
					},
				},
				IsTemplate: expr.expr.GetIsTemplate() || rightExpr.expr.GetIsTemplate(),
			},
			dataType: schemapb.DataType_Bool,
		}
	}
	return expr
}

// VisitEqualityExpr handles logical Equality expressions.
func (v *ParserVisitor) VisitEqualityExpr(ctx *parser.EqualityExprContext) interface{} {
	compExprs := ctx.AllComparisonExpr()
	left := compExprs[0].Accept(v)
	if getError(left) != nil {
		return left
	}
	leftExpr := getExpr(left)
	//var err error
	for i := 1; i < len(compExprs); i++ {
		opToken := ctx.GetChild(2*i - 1).(antlr.TerminalNode).GetText()
		right := compExprs[i].Accept(v)
		if getError(right) != nil {
			return right
		}

		var opType int
		switch opToken {
		case "==":
			opType = parser.PlanParserEQ
		case "!=":
			opType = parser.PlanParserNE
		default:
			return fmt.Errorf("unknown equality operator: %s", opToken)
		}

		rightExpr := getExpr(right)
		leftValueExpr, rightValueExpr := getValueExpr(left), getValueExpr(right)
		if leftValueExpr != nil && rightValueExpr != nil {
			if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
				return fmt.Errorf("placeholder was not supported between two constants with operator: %s", opToken)
			}
			leftValue, rightValue := leftValueExpr.GetValue(), rightValueExpr.GetValue()
			var ret *ExprWithType
			switch opType {
			case parser.PlanParserEQ:
				ret = Equal(leftValue, rightValue)
			case parser.PlanParserNE:
				ret = NotEqual(leftValue, rightValue)
			default:
				return fmt.Errorf("unexpected op: %s", opToken)
			}
			if ret == nil {
				return fmt.Errorf("comparison operations cannot be applied to two incompatible operands: %s", ctx.GetText())
			}
			leftExpr = ret
			continue
		}

		expr, err := HandleCompare(opType, leftExpr, rightExpr)
		if err != nil {
			return err
		}
		leftExpr = &ExprWithType{
			expr:          expr,
			dataType:      schemapb.DataType_Bool,
			nodeDependent: false,
		}
	}
	return leftExpr
}

// visitLike handles match operations.
func (v *ParserVisitor) visitLike(ctx parser.IAdditiveExprContext, pattern string) interface{} {
	left := ctx.Accept(v)
	if getError(left) != nil {
		return left
	}
	expr := getExpr(left)
	column := toColumnInfo(expr)
	if column == nil {
		return fmt.Errorf("like operation on complicated expr is unsupported")
	}
	if err := checkDirectComparisonBinaryField(column); err != nil {
		return err
	}

	if !typeutil.IsStringType(expr.dataType) && !typeutil.IsJSONType(expr.dataType) &&
		!(typeutil.IsArrayType(expr.dataType) && typeutil.IsStringType(column.GetElementType())) {
		return fmt.Errorf("like operation on non-string or no-json field is unsupported")
	}

	pattern, err := convertEscapeSingle(pattern)
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

// visitTerm translates expr to term plan.
func (v *ParserVisitor) visitTerm(leftCtx, rightCtx parser.IAdditiveExprContext, isNot bool) interface{} {
	left := leftCtx.Accept(v)
	if getError(left) != nil {
		return left
	}
	right := rightCtx.Accept(v)
	if getError(right) != nil {
		return right
	}

	leftExpr := getExpr(left)

	columnInfo := toColumnInfo(leftExpr)
	if columnInfo == nil {
		return fmt.Errorf("'term' can only be used on single field, but got: %s", leftCtx.GetText())
	}

	dataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(dataType) && len(columnInfo.GetNestedPath()) != 0 {
		dataType = columnInfo.GetElementType()
	}

	valueExpr := getValueExpr(right)
	var placeholder string
	var isTemplate bool
	var values []*planpb.GenericValue
	if valueExpr.GetValue() == nil && valueExpr.GetTemplateVariableName() != "" {
		placeholder = valueExpr.GetTemplateVariableName()
		values = nil
		isTemplate = true
	} else {
		elementValue := valueExpr.GetValue()
		if elementValue == nil {
			return fmt.Errorf("value '%s' in list cannot be a non-const expression", rightCtx.GetText())
		}

		if !IsArray(elementValue) {
			return fmt.Errorf("the right-hand side of 'in' must be a list, but got: %s", rightCtx.GetText())
		}
		array := elementValue.GetArrayVal().GetArray()
		values = make([]*planpb.GenericValue, len(array))
		for i, e := range array {
			castedValue, err := castValue(dataType, e)
			if err != nil {
				return fmt.Errorf("value '%s' in list cannot be casted to %s", e.String(), dataType.String())
			}
			values[i] = castedValue
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo:           columnInfo,
				Values:               values,
				TemplateVariableName: placeholder,
			},
		},
		IsTemplate: isTemplate,
	}
	if isNot {
		expr = &planpb.Expr{
			Expr: &planpb.Expr_UnaryExpr{
				UnaryExpr: &planpb.UnaryExpr{
					Op:    planpb.UnaryExpr_Not,
					Child: expr,
				},
			},
			IsTemplate: isTemplate,
		}
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// visitRelational translates expr to range/compare plan.
func (v *ParserVisitor) visitRelational(leftCtx, rightCtx parser.IAdditiveExprContext, op parser.ICompOpContext) interface{} {
	left := leftCtx.Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := rightCtx.Accept(v)
	if err := getError(right); err != nil {
		return err
	}
	var opType int
	if op.GE() != nil {
		opType = parser.PlanParserGE
	} else if op.GT() != nil {
		opType = parser.PlanParserGT
	} else if op.LE() != nil {
		opType = parser.PlanParserLE
	} else if op.LT() != nil {
		opType = parser.PlanParserLT
	}
	leftValueExpr, rightValueExpr := getValueExpr(left), getValueExpr(right)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return fmt.Errorf("placeholder was not supported between two constants with operator: %s", op.GetText())
		}
		leftValue, rightValue := getGenericValue(left), getGenericValue(right)
		var ret *ExprWithType
		switch opType {
		case parser.PlanParserLT:
			ret = Less(leftValue, rightValue)
		case parser.PlanParserLE:
			ret = LessEqual(leftValue, rightValue)
		case parser.PlanParserGT:
			ret = Greater(leftValue, rightValue)
		case parser.PlanParserGE:
			ret = GreaterEqual(leftValue, rightValue)
		default:
			return fmt.Errorf("unexpected op: %d", opType)
		}
		if ret == nil {
			return fmt.Errorf("comparison operations cannot be applied to two incompatible operands: %s and %s", leftCtx.GetText(), rightCtx.GetText())
		}
		return ret
	}

	leftExpr, rightExpr := getExpr(left), getExpr(right)
	if err := checkDirectComparisonBinaryField(toColumnInfo(leftExpr)); err != nil {
		return err
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(rightExpr)); err != nil {
		return err
	}

	expr, err := HandleCompare(opType, leftExpr, rightExpr)
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// visitRange translates expr to range plan.
func (v *ParserVisitor) visitRange(lowerCtx, columnCtx, upperCtx parser.IAdditiveExprContext, lowerInclusive, upperInclusive bool) interface{} {
	column := columnCtx.Accept(v)
	if getError(column) != nil {
		return column
	}
	columnExpr := getExpr(column)
	columnInfo := toColumnInfo(columnExpr)
	if columnInfo == nil {
		return fmt.Errorf("range operations are only supported on single fields now, got: %s", columnCtx.GetText())
	}
	if err := checkDirectComparisonBinaryField(columnInfo); err != nil {
		return err
	}

	lower := lowerCtx.Accept(v)
	upper := upperCtx.Accept(v)
	if err := getError(lower); err != nil {
		return err
	}
	if err := getError(upper); err != nil {
		return err
	}

	lowerValueExpr, upperValueExpr := getValueExpr(lower), getValueExpr(upper)
	if lowerValueExpr == nil {
		return fmt.Errorf("lowerbound cannot be a non-const expression: %s", lowerCtx.GetText())
	}
	if upperValueExpr == nil {
		return fmt.Errorf("upperbound cannot be a non-const expression: %s", upperCtx.GetText())
	}

	fieldDataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		fieldDataType = columnInfo.GetElementType()
	}

	var err error
	lowerValue := lowerValueExpr.GetValue()
	upperValue := upperValueExpr.GetValue()
	if !isTemplateExpr(lowerValueExpr) {
		if lowerValue, err = castRangeValue(fieldDataType, lowerValue); err != nil {
			return err
		}
	}
	if !isTemplateExpr(upperValueExpr) {
		if upperValue, err = castRangeValue(fieldDataType, upperValue); err != nil {
			return err
		}
	}

	if !isTemplateExpr(lowerValueExpr) && !isTemplateExpr(upperValueExpr) {
		if !(lowerInclusive && upperInclusive) {
			if getGenericValue(GreaterEqual(lowerValue, upperValue)).GetBoolVal() {
				return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
			}
		} else {
			if getGenericValue(Greater(lowerValue, upperValue)).GetBoolVal() {
				return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
			}
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:                columnInfo,
				LowerInclusive:            lowerInclusive,
				UpperInclusive:            upperInclusive,
				LowerValue:                lowerValue,
				UpperValue:                upperValue,
				LowerTemplateVariableName: lowerValueExpr.GetTemplateVariableName(),
				UpperTemplateVariableName: upperValueExpr.GetTemplateVariableName(),
			},
		},
		IsTemplate: isTemplateExpr(lowerValueExpr) || isTemplateExpr(upperValueExpr),
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitComparisonExpr handles comparison Equality expressions.
func (v *ParserVisitor) VisitComparisonExpr(ctx *parser.ComparisonExprContext) interface{} {
	if ctx.LIKE() != nil {
		return v.visitLike(ctx.AdditiveExpr(0), ctx.StringLiteral().GetText())
	}

	if ctx.IN() != nil {
		return v.visitTerm(ctx.AdditiveExpr(0), ctx.AdditiveExpr(1), ctx.NOT() != nil)
	}

	additiveExprs := ctx.AllAdditiveExpr()
	if len(additiveExprs) > 3 {
		return fmt.Errorf("not suppoted for comparison expression")
	}

	if len(additiveExprs) == 2 {
		return v.visitRelational(additiveExprs[0], additiveExprs[1], ctx.CompOp(0))
	}

	if len(additiveExprs) == 3 {
		allCompOps := ctx.AllCompOp()
		var lowerInclusive, upperInclusive bool
		if allCompOps[0].LE() != nil || allCompOps[0].LT() != nil {
			if allCompOps[1].LE() != nil || allCompOps[1].LT() != nil {
				lowerInclusive = allCompOps[0].LE() != nil
				upperInclusive = allCompOps[1].LE() != nil
			} else {
				return fmt.Errorf("unsupported comparison expression between less and garthar")
			}
			return v.visitRange(additiveExprs[0], additiveExprs[1], additiveExprs[2], lowerInclusive, upperInclusive)
		}
		if allCompOps[1].GE() != nil || allCompOps[1].GT() != nil {
			lowerInclusive = allCompOps[1].GE() != nil
			upperInclusive = allCompOps[0].GE() != nil
		}
		return v.visitRange(additiveExprs[2], additiveExprs[1], additiveExprs[0], lowerInclusive, upperInclusive)
	}

	return additiveExprs[0].Accept(v)
}

// visitAddSub translates expr to arithmetic plan.
func (v *ParserVisitor) visitAddSub(leftExpr, rightExpr *ExprWithType, op int) interface{} {
	var err error
	leftValueExpr, rightValueExpr := getValueExpr(leftExpr), getValueExpr(rightExpr)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return fmt.Errorf("placeholder was not supported between two constants with operator: %d", op)
		}
		leftValue, rightValue := leftValueExpr.GetValue(), rightValueExpr.GetValue()
		switch op {
		case parser.PlanParserADD:
			return Add(leftValue, rightValue)
		case parser.PlanParserSUB:
			return Subtract(leftValue, rightValue)
		default:
			return fmt.Errorf("unexpected op: %d", op)
		}
	}

	reverse := true
	if leftValueExpr == nil {
		reverse = false
	}

	if err = checkDirectComparisonBinaryField(toColumnInfo(leftExpr)); err != nil {
		return err
	}
	if err = checkDirectComparisonBinaryField(toColumnInfo(rightExpr)); err != nil {
		return err
	}
	var dataType schemapb.DataType
	if leftExpr.expr.GetIsTemplate() {
		dataType = rightExpr.dataType
	} else if rightExpr.expr.GetIsTemplate() {
		dataType = leftExpr.dataType
	} else {
		if !canArithmetic(leftExpr.dataType, getArrayElementType(leftExpr), rightExpr.dataType, getArrayElementType(rightExpr)) {
			return fmt.Errorf("'%s' can only be used between integer or floating or json field expressions", arithNameMap[op])
		}

		dataType, err = calcDataType(leftExpr, rightExpr, reverse)
		if err != nil {
			return err
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    arithExprMap[op],
			},
		},
		IsTemplate: leftExpr.expr.GetIsTemplate() || rightExpr.expr.GetIsTemplate(),
	}
	return &ExprWithType{
		expr:          expr,
		dataType:      dataType,
		nodeDependent: true,
	}
}

// VisitAdditiveExpr handles comparison (ADD | SUB) expressions.
func (v *ParserVisitor) VisitAdditiveExpr(ctx *parser.AdditiveExprContext) interface{} {
	multExprs := ctx.AllMultiplicativeExpr()
	left := multExprs[0].Accept(v)
	if getError(left) != nil {
		return left
	}
	expr := getExpr(left)
	for i := 1; i < len(multExprs); i++ {
		opToken := ctx.GetChild(2*i - 1).(antlr.TerminalNode).GetText()
		right := multExprs[i].Accept(v)
		if getError(right) != nil {
			return right
		}
		rightExpr := getExpr(right)
		if expr == nil || rightExpr == nil {
			return fmt.Errorf("invalid arithmetic expression, left: %s, op: %s, right: %s",
				multExprs[i-1].GetText(), opToken, multExprs[i].GetText())
		}
		var opType int
		switch opToken {
		case "+":
			opType = parser.PlanParserADD
		case "-":
			opType = parser.PlanParserSUB
		default:
			return fmt.Errorf("unknown additive operator: %s", opToken)
		}
		newExpr := v.visitAddSub(expr, rightExpr, opType)
		if getError(newExpr) != nil {
			return newExpr
		}
		expr = getExpr(newExpr)
	}
	return expr
}

// visitMulDivMod translates expr to arithmetic plan.
func (v *ParserVisitor) visitMulDivMod(leftExpr, rightExpr *ExprWithType, op int) interface{} {
	var err error
	leftValueExpr, rightValueExpr := getValueExpr(leftExpr), getValueExpr(rightExpr)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return fmt.Errorf("placeholder was not supported between two constants with operator: %d", op)
		}
		leftValue, rightValue := getGenericValue(leftExpr), getGenericValue(rightExpr)
		switch op {
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
			return fmt.Errorf("unexpected op: %d", op)
		}
	}

	reverse := true
	if leftValueExpr == nil {
		reverse = false
	}

	if err := checkDirectComparisonBinaryField(toColumnInfo(leftExpr)); err != nil {
		return err
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(rightExpr)); err != nil {
		return err
	}

	var dataType schemapb.DataType
	if leftExpr.expr.GetIsTemplate() {
		dataType = rightExpr.dataType
	} else if rightExpr.expr.GetIsTemplate() {
		dataType = leftExpr.dataType
	} else {
		if !canArithmetic(leftExpr.dataType, getArrayElementType(leftExpr), rightExpr.dataType, getArrayElementType(rightExpr)) {
			return fmt.Errorf("'%s' can only be used between integer or floating or json field expressions", arithNameMap[op])
		}

		if err = checkValidModArith(arithExprMap[op], leftExpr.dataType, getArrayElementType(leftExpr), rightExpr.dataType, getArrayElementType(rightExpr)); err != nil {
			return err
		}

		dataType, err = calcDataType(leftExpr, rightExpr, reverse)
		if err != nil {
			return err
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    arithExprMap[op],
			},
		},
		IsTemplate: leftExpr.expr.GetIsTemplate() || rightExpr.expr.GetIsTemplate(),
	}

	return &ExprWithType{
		expr:          expr,
		dataType:      dataType,
		nodeDependent: true,
	}
}

// VisitMultiplicativeExpr handles comparison (MUL | DIV | MOD) expressions.
func (v *ParserVisitor) VisitMultiplicativeExpr(ctx *parser.MultiplicativeExprContext) interface{} {
	powerExprs := ctx.AllPowerExpr()
	left := powerExprs[0].Accept(v)
	if getError(left) != nil {
		return left
	}
	expr := getExpr(left)
	for i := 1; i < len(powerExprs); i++ {
		opToken := ctx.GetChild(2*i - 1).(antlr.TerminalNode).GetText()
		right := powerExprs[i].Accept(v)
		if getError(right) != nil {
			return nil
		}
		rightExpr := getExpr(right)
		if expr == nil || rightExpr == nil {
			return fmt.Errorf("invalid arithmetic expression, left: %s, op: %s, right: %s",
				powerExprs[i-1].GetText(), opToken, powerExprs[i].GetText())
		}
		var opType int
		switch opToken {
		case "*":
			opType = parser.PlanParserMUL
		case "/":
			opType = parser.PlanParserDIV
		case "%":
			opType = parser.PlanParserMOD
		default:
			return fmt.Errorf("unknown multiplicative operator: %s", opToken)
		}
		newExpr := v.visitMulDivMod(expr, rightExpr, opType)
		if getError(newExpr) != nil {
			return newExpr
		}
		expr = getExpr(newExpr)
	}
	return expr
}

// VisitPowerExpr handles comparison (POW) expressions.
func (v *ParserVisitor) VisitPowerExpr(ctx *parser.PowerExprContext) interface{} {
	left := ctx.UnaryExpr().Accept(v)
	if getError(left) != nil {
		return left
	}
	if ctx.PowerExpr() != nil {
		right := ctx.PowerExpr().Accept(v)
		if getError(right) != nil {
			return right
		}

		leftValue, rightValue := getGenericValue(left), getGenericValue(right)
		if leftValue != nil && rightValue != nil {
			return Power(leftValue, rightValue)
		}

		return fmt.Errorf("power can only apply on constants: %s", ctx.GetText())
	}
	return left
}

// visitUnary unpack the +expr to expr.
func (v *ParserVisitor) visitUnary(ctx parser.IUnaryExprContext, op int) interface{} {
	child := ctx.Accept(v)
	if err := getError(child); err != nil {
		return err
	}

	childValue := getGenericValue(child)
	if childValue != nil {
		switch op {
		case parser.PlanParserADD:
			return child
		case parser.PlanParserSUB:
			return Negative(childValue)
		case parser.PlanParserNOT:
			return Not(childValue)
		default:
			return fmt.Errorf("unexpected op: %d", op)
		}
	}

	childExpr := getExpr(child)
	if childExpr == nil {
		return fmt.Errorf("failed to parse unary expressions")
	}
	if err := checkDirectComparisonBinaryField(toColumnInfo(childExpr)); err != nil {
		return err
	}
	switch op {
	case parser.PlanParserADD:
		return childExpr
	case parser.PlanParserNOT:
		if !canBeExecuted(childExpr) {
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
		return fmt.Errorf("unexpected op: %d", op)
	}
}

func (v *ParserVisitor) visitExists(ctx parser.IUnaryExprContext) interface{} {
	child := ctx.Accept(v)
	if err := getError(child); err != nil {
		return err
	}
	columnInfo := toColumnInfo(child.(*ExprWithType))
	if columnInfo == nil {
		return fmt.Errorf(
			"exists operations are only supported on single fields now, got: %s", ctx.GetText())
	}

	if columnInfo.GetDataType() != schemapb.DataType_JSON {
		return fmt.Errorf(
			"exists operations are only supportted on json field, got:%s", columnInfo.GetDataType())
	}

	if len(columnInfo.GetNestedPath()) == 0 {
		return fmt.Errorf(
			"exists operations are only supportted on json key")
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

// VisitUnaryExpr handles comparison (ADD | SUB | BNOT | NOT | EXISTS) expressions.
func (v *ParserVisitor) VisitUnaryExpr(ctx *parser.UnaryExprContext) interface{} {
	if ctx.UnaryExpr() != nil {
		if ctx.EXISTS() != nil {
			return v.visitExists(ctx.UnaryExpr())
		}
		opToken := ctx.GetChild(0).(antlr.TerminalNode).GetText()
		var opType int
		switch opToken {
		case "+":
			opType = parser.PlanParserADD
		case "-":
			opType = parser.PlanParserSUB
		case "!", "not":
			opType = parser.PlanParserNOT
		case "~":
			opType = parser.PlanParserBNOT
		default:
			return fmt.Errorf("unknown unary operator: %s", opToken)
		}
		return v.visitUnary(ctx.UnaryExpr(), opType)
	}
	return ctx.PostfixExpr().Accept(v)
}

// visitCall parses the expr to call plan.
func (v *ParserVisitor) visitCall(identifier string, argList parser.IArgumentListContext) interface{} {
	functionName := strings.ToLower(identifier)
	funcParameters := make([]*planpb.Expr, 0)

	if argList != nil {
		for _, param := range argList.AllExpr() {
			paramExpr := param.Accept(v)
			if getError(paramExpr) != nil {
				return paramExpr
			}
			funcParameters = append(funcParameters, getExpr(paramExpr).expr)
		}
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_CallExpr{
				CallExpr: &planpb.CallExpr{
					FunctionName:       functionName,
					FunctionParameters: funcParameters,
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

// visitIsNotNull parses the expr to (is not null) plan.
func (v *ParserVisitor) visitIsNotNull(identifier string) interface{} {
	column, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_NullExpr{
			NullExpr: &planpb.NullExpr{
				ColumnInfo: toColumnInfo(column),
				Op:         planpb.NullExpr_IsNotNull,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// visitIsNull parses the expr to (is null) plan.
func (v *ParserVisitor) visitIsNull(identifier string) interface{} {
	column, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_NullExpr{
			NullExpr: &planpb.NullExpr{
				ColumnInfo: toColumnInfo(column),
				Op:         planpb.NullExpr_IsNull,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitPostfixExpr handles comparison ( postfixOp ) expressions.
func (v *ParserVisitor) VisitPostfixExpr(ctx *parser.PostfixExprContext) interface{} {
	expr := ctx.PrimaryExpr().Accept(v)
	if getError(expr) != nil {
		return expr
	}
	op := ctx.PostfixOp()
	if op != nil {
		switch child := op.(type) {
		case *parser.FunctionCallContext:
			expr = v.visitCall(ctx.PrimaryExpr().GetText(), child.ArgumentList())
		case *parser.IsNullContext:
			expr = v.visitIsNull(ctx.PrimaryExpr().GetText())
		case *parser.IsNotNullContext:
			expr = v.visitIsNotNull(ctx.PrimaryExpr().GetText())
		default:
			return fmt.Errorf("unknown postfix operation")
		}
	}
	return expr
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
		nodeDependent: true,
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
		nodeDependent: true,
	}
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
		nodeDependent: true,
	}
}

// VisitString translates expr to GenericValue.
func (v *ParserVisitor) VisitString(ctx *parser.StringContext) interface{} {
	pattern, err := convertEscapeSingle(ctx.GetText())
	if err != nil {
		return err
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

// VisitIdentifier translates expr to column plan.
func (v *ParserVisitor) VisitIdentifier(ctx *parser.IdentifierContext) interface{} {
	identifier := ctx.GetText()
	expr, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}
	return expr
}

// VisitMeta translates expr to column plan.
func (v *ParserVisitor) VisitMeta(ctx *parser.MetaContext) interface{} {
	identifier := ctx.GetText()
	expr, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}
	return expr
}

// VisitJSONIdentifier translates expr to column plan.
func (v *ParserVisitor) VisitJSONIdentifier(ctx *parser.JSONIdentifierContext) interface{} {
	field, err := v.getColumnInfoFromJSONIdentifier(ctx.JSONIdentifier().GetText())
	if err != nil {
		return err
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

// VisitTemplateVariable translates expr to template plan.
func (v *ParserVisitor) VisitTemplateVariable(ctx *parser.TemplateVariableContext) interface{} {
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value:                nil,
					TemplateVariableName: ctx.Identifier().GetText(),
				},
			},
			IsTemplate: true,
		},
	}
}

// VisitParens unpack the parentheses.
func (v *ParserVisitor) VisitParens(ctx *parser.ParensContext) interface{} {
	return ctx.Expr().Accept(v)
}

// VisitArray translates expr to array plan.
func (v *ParserVisitor) VisitArray(ctx *parser.ArrayContext) interface{} {
	allExpr := ctx.AllExpr()
	array := make([]*planpb.GenericValue, len(allExpr))
	dType := schemapb.DataType_None
	sameType := true
	for i := 0; i < len(allExpr); i++ {
		element := allExpr[i].Accept(v)
		if err := getError(element); err != nil {
			return err
		}
		elementValue := getGenericValue(element)
		if elementValue == nil {
			return fmt.Errorf("array element type must be generic value, but got: %s", allExpr[i].GetText())
		}
		array[i] = elementValue

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

// VisitEmptyArray translates expr to array plan.
func (v *ParserVisitor) VisitEmptyArray(ctx *parser.EmptyArrayContext) interface{} {
	return &ExprWithType{
		dataType: schemapb.DataType_Array,
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: &planpb.GenericValue{
						Val: &planpb.GenericValue_ArrayVal{
							ArrayVal: &planpb.Array{
								Array:       nil,
								SameType:    true,
								ElementType: schemapb.DataType_None,
							},
						},
					},
				},
			},
		},
		nodeDependent: true,
	}
}

func (v *ParserVisitor) VisitTextMatch(ctx *parser.TextMatchContext) interface{} {
	column, err := v.translateIdentifier(ctx.Identifier().GetText())
	if err != nil {
		return err
	}
	columnInfo := toColumnInfo(column)
	if !v.schema.IsFieldTextMatchEnabled(columnInfo.FieldId) {
		return fmt.Errorf("field %v does not enable text match", columnInfo.FieldId)
	}
	if !typeutil.IsStringType(column.dataType) {
		return fmt.Errorf("text match operation on non-string is unsupported")
	}

	queryText, err := convertEscapeSingle(ctx.StringLiteral().GetText())
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_UnaryRangeExpr{
				UnaryRangeExpr: &planpb.UnaryRangeExpr{
					ColumnInfo: columnInfo,
					Op:         planpb.OpType_TextMatch,
					Value:      NewString(queryText),
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitPhraseMatch(ctx *parser.PhraseMatchContext) interface{} {
	column, err := v.translateIdentifier(ctx.Identifier().GetText())
	if err != nil {
		return err
	}
	if !typeutil.IsStringType(column.dataType) {
		return fmt.Errorf("phrase match operation on non-string is unsupported")
	}
	queryText, err := convertEscapeSingle(ctx.StringLiteral().GetText())
	if err != nil {
		return err
	}
	var slop int64 = 0
	if ctx.Expr() != nil {
		slopExpr := ctx.Expr().Accept(v)
		slopValueExpr := getValueExpr(slopExpr)
		if slopValueExpr == nil || slopValueExpr.GetValue() == nil {
			return fmt.Errorf("\"slop\" should be a const integer expression with \"uint32\" value. \"slop\" expression passed: %s", ctx.Expr().GetText())
		}
		slop = slopValueExpr.GetValue().GetInt64Val()
		if slop < 0 {
			return fmt.Errorf("\"slop\" should not be a negative interger. \"slop\" passed: %s", ctx.Expr().GetText())
		}

		if slop > math.MaxUint32 {
			return fmt.Errorf("\"slop\" exceeds the range of \"uint32\". \"slop\" expression passed: %s", ctx.Expr().GetText())
		}
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_UnaryRangeExpr{
				UnaryRangeExpr: &planpb.UnaryRangeExpr{
					ColumnInfo:  toColumnInfo(column),
					Op:          planpb.OpType_PhraseMatch,
					Value:       NewString(queryText),
					ExtraValues: []*planpb.GenericValue{NewInt(slop)},
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitJSONContains(ctx *parser.JSONContainsContext) interface{} {
	field := ctx.Expr(0).Accept(v)
	if err := getError(field); err != nil {
		return err
	}

	columnInfo := toColumnInfo(field.(*ExprWithType))
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return fmt.Errorf(
			"contains operation are only supported on json or array fields now, got: %s", ctx.Expr(0).GetText())
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return err
	}
	elementExpr := getValueExpr(element)
	if elementExpr == nil {
		return fmt.Errorf(
			"contains operation are only supported explicitly specified element, got: %s", ctx.Expr(1).GetText())
	}
	var elements []*planpb.GenericValue

	if !isTemplateExpr(elementExpr) {
		elements = make([]*planpb.GenericValue, 1)
		elementValue := elementExpr.GetValue()
		if err := checkContainsElement(field.(*ExprWithType), planpb.JSONContainsExpr_Contains, elementValue); err != nil {
			return err
		}
		elements[0] = elementValue
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:           columnInfo,
				Elements:             elements,
				Op:                   planpb.JSONContainsExpr_Contains,
				ElementsSameType:     true,
				TemplateVariableName: elementExpr.GetTemplateVariableName(),
			},
		},
		IsTemplate: isTemplateExpr(elementExpr),
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitJSONContainsAll(ctx *parser.JSONContainsAllContext) interface{} {
	field := ctx.Expr(0).Accept(v)
	if err := getError(field); err != nil {
		return err
	}

	columnInfo := toColumnInfo(field.(*ExprWithType))
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return fmt.Errorf(
			"contains_all operation are only supported on json or array fields now, got: %s", ctx.Expr(0).GetText())
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return err
	}

	elementExpr := getValueExpr(element)
	if elementExpr == nil {
		return fmt.Errorf(
			"contains_all operation are only supported explicitly specified element, got: %s", ctx.Expr(1).GetText())
	}

	var elements []*planpb.GenericValue
	var sameType bool
	if !isTemplateExpr(elementExpr) {
		elementValue := elementExpr.GetValue()
		if err := checkContainsElement(field.(*ExprWithType), planpb.JSONContainsExpr_ContainsAll, elementValue); err != nil {
			return err
		}
		elements = elementValue.GetArrayVal().GetArray()
		sameType = elementValue.GetArrayVal().GetSameType()
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:           columnInfo,
				Elements:             elements,
				Op:                   planpb.JSONContainsExpr_ContainsAll,
				ElementsSameType:     sameType,
				TemplateVariableName: elementExpr.GetTemplateVariableName(),
			},
		},
		IsTemplate: isTemplateExpr(elementExpr),
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitJSONContainsAny(ctx *parser.JSONContainsAnyContext) interface{} {
	field := ctx.Expr(0).Accept(v)
	if err := getError(field); err != nil {
		return err
	}

	columnInfo := toColumnInfo(field.(*ExprWithType))
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return fmt.Errorf(
			"contains_any operation are only supported on json or array fields now, got: %s", ctx.Expr(0).GetText())
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return err
	}
	valueExpr := getValueExpr(element)

	if valueExpr == nil {
		return fmt.Errorf(
			"contains_any operation are only supported explicitly specified element, got: %s", ctx.Expr(1).GetText())
	}

	var elements []*planpb.GenericValue
	var sameType bool
	if !isTemplateExpr(valueExpr) {
		elementValue := valueExpr.GetValue()
		if err := checkContainsElement(field.(*ExprWithType), planpb.JSONContainsExpr_ContainsAny, elementValue); err != nil {
			return err
		}
		elements = elementValue.GetArrayVal().GetArray()
		sameType = elementValue.GetArrayVal().GetSameType()
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_JsonContainsExpr{
			JsonContainsExpr: &planpb.JSONContainsExpr{
				ColumnInfo:           columnInfo,
				Elements:             elements,
				Op:                   planpb.JSONContainsExpr_ContainsAny,
				ElementsSameType:     sameType,
				TemplateVariableName: valueExpr.GetTemplateVariableName(),
			},
		},
		IsTemplate: isTemplateExpr(valueExpr),
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitArrayLength(ctx *parser.ArrayLengthContext) interface{} {
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier())
	if err != nil {
		return err
	}
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return fmt.Errorf(
			"array_length operation are only supported on json or array fields now, got: %s", ctx.GetText())
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
