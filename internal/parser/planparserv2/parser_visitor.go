package planparserv2

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ParserVisitorArgs struct {
	Timezone string
}

// int64OverflowError is a special error type used to handle the case where
// 9223372036854775808 (which exceeds int64 max) is used with unary minus
// to represent -9223372036854775808 (int64 minimum value).
// This happens because ANTLR parses -9223372036854775808 as Unary(SUB, Integer(9223372036854775808)),
// causing the integer literal to exceed int64 range before the unary minus is applied.
type int64OverflowError struct {
	literal string
}

func (e *int64OverflowError) Error() string {
	return fmt.Sprintf("int64 overflow: %s", e.literal)
}

func isInt64OverflowError(err error) bool {
	_, ok := err.(*int64OverflowError)
	return ok
}

type ParserVisitor struct {
	parser.BasePlanVisitor
	schema *typeutil.SchemaHelper
	args   *ParserVisitorArgs
	// currentStructArrayField stores the struct array field name when processing ElementFilter
	currentStructArrayField string
}

func NewParserVisitor(schema *typeutil.SchemaHelper, args *ParserVisitorArgs) *ParserVisitor {
	return &ParserVisitor{schema: schema, args: args}
}

// VisitParens unpack the parentheses.
func (v *ParserVisitor) VisitParens(ctx *parser.ParensContext) interface{} {
	return ctx.Expr().Accept(v)
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

	if field.DataType == schemapb.DataType_Text {
		return nil, merr.WrapErrParameterInvalidMsg("filter on text field (%s) is not supported yet", field.Name)
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

// VisitIdentifier translates expr to column plan.
func (v *ParserVisitor) VisitIdentifier(ctx *parser.IdentifierContext) interface{} {
	identifier := ctx.GetText()
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
		nodeDependent: true,
	}
}

// VisitInteger translates expr to GenericValue.
func (v *ParserVisitor) VisitInteger(ctx *parser.IntegerContext) interface{} {
	literal := ctx.IntegerConstant().GetText()
	i, err := strconv.ParseInt(literal, 0, 64)
	if err != nil {
		// Special case: 9223372036854775808 is out of int64 range,
		// but -9223372036854775808 is valid (int64 minimum value).
		// This happens because ANTLR parses -9223372036854775808 as:
		//   Unary(SUB, Integer(9223372036854775808))
		// The integer literal 9223372036854775808 exceeds int64 max (9223372036854775807)
		// before the unary minus is applied. We handle this in VisitUnary.
		if literal == "9223372036854775808" {
			return &int64OverflowError{literal: literal}
		}
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

func checkDirectComparisonBinaryField(columnInfo *planpb.ColumnInfo) error {
	if typeutil.IsArrayType(columnInfo.GetDataType()) && len(columnInfo.GetNestedPath()) == 0 && !columnInfo.GetIsElementLevel() {
		return errors.New("can not comparisons array fields directly")
	}
	return nil
}

// VisitAddSub translates expr to arithmetic plan.
func (v *ParserVisitor) VisitAddSub(ctx *parser.AddSubContext) interface{} {
	var err error
	left := ctx.Expr(0).Accept(v)
	if err = getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err = getError(right); err != nil {
		return err
	}

	leftValueExpr, rightValueExpr := getValueExpr(left), getValueExpr(right)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return merr.WrapErrParameterInvalidMsg("placeholder was not supported between two constants with operator: %s", ctx.GetOp().GetText())
		}
		leftValue, rightValue := leftValueExpr.GetValue(), rightValueExpr.GetValue()
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
			return merr.WrapErrParameterInvalidMsg("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	leftExpr, rightExpr := getExpr(left), getExpr(right)
	reverse := true

	if leftValueExpr == nil {
		reverse = false
	}

	if leftExpr == nil || rightExpr == nil {
		return merr.WrapErrParameterInvalidMsg("invalid arithmetic expression, left: %s, op: %s, right: %s", ctx.Expr(0).GetText(), ctx.GetOp(), ctx.Expr(1).GetText())
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
		if err := canArithmetic(leftExpr.dataType, getArrayElementType(leftExpr), rightExpr.dataType, getArrayElementType(rightExpr), reverse); err != nil {
			return merr.WrapErrParameterInvalidMsg("'%s' %s", arithNameMap[ctx.GetOp().GetTokenType()], err.Error())
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
				Op:    arithExprMap[ctx.GetOp().GetTokenType()],
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

// VisitMulDivMod translates expr to arithmetic plan.
func (v *ParserVisitor) VisitMulDivMod(ctx *parser.MulDivModContext) interface{} {
	var err error
	left := ctx.Expr(0).Accept(v)
	if err := getError(left); err != nil {
		return err
	}

	right := ctx.Expr(1).Accept(v)
	if err := getError(right); err != nil {
		return err
	}

	leftValueExpr, rightValueExpr := getValueExpr(left), getValueExpr(right)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return merr.WrapErrParameterInvalidMsg("placeholder was not supported between two constants with operator: %s", ctx.GetOp().GetText())
		}
		leftValue, rightValue := getGenericValue(left), getGenericValue(right)
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
			return merr.WrapErrParameterInvalidMsg("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	leftExpr, rightExpr := getExpr(left), getExpr(right)
	reverse := true

	if leftValueExpr == nil {
		reverse = false
	}

	if leftExpr == nil || rightExpr == nil {
		return merr.WrapErrParameterInvalidMsg("invalid arithmetic expression, left: %s, op: %s, right: %s", ctx.Expr(0).GetText(), ctx.GetOp(), ctx.Expr(1).GetText())
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
		if err := canArithmetic(leftExpr.dataType, getArrayElementType(leftExpr), rightExpr.dataType, getArrayElementType(rightExpr), reverse); err != nil {
			return merr.WrapErrParameterInvalidMsg("'%s' %s", arithNameMap[ctx.GetOp().GetTokenType()], err.Error())
		}

		if err = checkValidModArith(arithExprMap[ctx.GetOp().GetTokenType()], leftExpr.dataType, getArrayElementType(leftExpr), rightExpr.dataType, getArrayElementType(rightExpr)); err != nil {
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
				Op:    arithExprMap[ctx.GetOp().GetTokenType()],
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

	leftValueExpr, rightValueExpr := getValueExpr(left), getValueExpr(right)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return merr.WrapErrParameterInvalidMsg("placeholder was not supported between two constants with operator: %s", ctx.GetOp().GetText())
		}
		leftValue, rightValue := leftValueExpr.GetValue(), rightValueExpr.GetValue()
		var ret *ExprWithType
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserEQ:
			ret = Equal(leftValue, rightValue)
		case parser.PlanParserNE:
			ret = NotEqual(leftValue, rightValue)
		default:
			return merr.WrapErrParameterInvalidMsg("unexpected op: %s", ctx.GetOp().GetText())
		}
		if ret == nil {
			return merr.WrapErrParameterInvalidMsg("comparison operations cannot be applied to two incompatible operands: %s", ctx.GetText())
		}
		return ret
	}

	leftExpr, rightExpr := getExpr(left), getExpr(right)

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
	leftValueExpr, rightValueExpr := getValueExpr(left), getValueExpr(right)
	if leftValueExpr != nil && rightValueExpr != nil {
		if isTemplateExpr(leftValueExpr) || isTemplateExpr(rightValueExpr) {
			return merr.WrapErrParameterInvalidMsg("placeholder was not supported between two constants with operator: %s", ctx.GetOp().GetText())
		}
		leftValue, rightValue := getGenericValue(left), getGenericValue(right)
		var ret *ExprWithType
		switch ctx.GetOp().GetTokenType() {
		case parser.PlanParserLT:
			ret = Less(leftValue, rightValue)
		case parser.PlanParserLE:
			ret = LessEqual(leftValue, rightValue)
		case parser.PlanParserGT:
			ret = Greater(leftValue, rightValue)
		case parser.PlanParserGE:
			ret = GreaterEqual(leftValue, rightValue)
		default:
			return merr.WrapErrParameterInvalidMsg("unexpected op: %s", ctx.GetOp().GetText())
		}
		if ret == nil {
			return merr.WrapErrParameterInvalidMsg("comparison operations cannot be applied to two incompatible operands: %s", ctx.GetText())
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
		return errors.New("the left operand of like is invalid")
	}

	column := toColumnInfo(leftExpr)
	if column == nil {
		return errors.New("like operation on complicated expr is unsupported")
	}
	if err := checkDirectComparisonBinaryField(column); err != nil {
		return err
	}

	if !typeutil.IsStringType(leftExpr.dataType) && !typeutil.IsJSONType(leftExpr.dataType) &&
		!(typeutil.IsArrayType(leftExpr.dataType) && typeutil.IsStringType(column.GetElementType())) {
		return errors.New("like operation on non-string or no-json field is unsupported")
	}

	literal := ctx.StringLiteral().GetText()
	literal = prepareSpecialEscapeCharactersForConvertingEscapeSingle(literal, likeEscapeCharacters)

	pattern, err := convertEscapeSingle(literal)
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

func (v *ParserVisitor) VisitTextMatch(ctx *parser.TextMatchContext) interface{} {
	identifier := ctx.Identifier().GetText()
	column, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}
	columnInfo := toColumnInfo(column)
	if !typeutil.IsStringType(column.dataType) {
		return errors.New("text match operation on non-string is unsupported")
	}
	if column.dataType == schemapb.DataType_Text {
		return errors.New("text match operation on text field is not supported yet")
	}
	if !v.schema.IsFieldTextMatchEnabled(columnInfo.FieldId) {
		return merr.WrapErrParameterInvalidMsg("field \"%s\" does not enable match", identifier)
	}

	queryText, err := convertEscapeSingle(ctx.StringLiteral().GetText())
	if err != nil {
		return err
	}

	// Handle optional min_should_match parameter
	var extraValues []*planpb.GenericValue
	if ctx.TextMatchOption() != nil {
		minShouldMatchExpr := ctx.TextMatchOption().Accept(v)
		if err, ok := minShouldMatchExpr.(error); ok {
			return err
		}
		extraVal, err := validateAndExtractMinShouldMatch(minShouldMatchExpr)
		if err != nil {
			return err
		}
		extraValues = extraVal
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_UnaryRangeExpr{
				UnaryRangeExpr: &planpb.UnaryRangeExpr{
					ColumnInfo:  columnInfo,
					Op:          planpb.OpType_TextMatch,
					Value:       NewString(queryText),
					ExtraValues: extraValues,
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitTextMatchOption(ctx *parser.TextMatchOptionContext) interface{} {
	// Parse the integer constant for minimum_should_match
	integerConstant := ctx.IntegerConstant().GetText()
	value, err := strconv.ParseInt(integerConstant, 0, 64)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid minimum_should_match value: %s", integerConstant)
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ValueExpr{
				ValueExpr: &planpb.ValueExpr{
					Value: NewInt(value),
				},
			},
		},
		dataType: schemapb.DataType_Int64,
	}
}

func (v *ParserVisitor) VisitPhraseMatch(ctx *parser.PhraseMatchContext) interface{} {
	identifier := ctx.Identifier().GetText()
	column, err := v.translateIdentifier(identifier)
	if err != nil {
		return err
	}

	columnInfo := toColumnInfo(column)
	if !typeutil.IsStringType(column.dataType) {
		return errors.New("phrase match operation on non-string is unsupported")
	}
	if !v.schema.IsFieldTextMatchEnabled(columnInfo.FieldId) {
		return merr.WrapErrParameterInvalidMsg("field \"%s\" does not enable match", identifier)
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
			return merr.WrapErrParameterInvalidMsg("\"slop\" should be a const integer expression with \"uint32\" value. \"slop\" expression passed: %s", ctx.Expr().GetText())
		}
		slop = slopValueExpr.GetValue().GetInt64Val()
		if slop < 0 {
			return merr.WrapErrParameterInvalidMsg("\"slop\" should not be a negative interger. \"slop\" passed: %s", ctx.Expr().GetText())
		}

		if slop > math.MaxUint32 {
			return merr.WrapErrParameterInvalidMsg("\"slop\" exceeds the range of \"uint32\". \"slop\" expression passed: %s", ctx.Expr().GetText())
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

func isRandomSampleExpr(expr *ExprWithType) bool {
	return expr.expr.GetRandomSampleExpr() != nil
}

func isElementFilterExpr(expr *ExprWithType) bool {
	return expr.expr.GetElementFilterExpr() != nil
}

func isMatchExpr(expr *ExprWithType) bool {
	return expr.expr.GetMatchExpr() != nil
}

const EPSILON = 1e-10

func (v *ParserVisitor) VisitRandomSample(ctx *parser.RandomSampleContext) interface{} {
	if ctx.Expr() == nil {
		return merr.WrapErrParameterInvalidMsg("sample factor missed: %s", ctx.GetText())
	}

	floatExpr := ctx.Expr().Accept(v)
	if err := getError(floatExpr); err != nil {
		return merr.WrapErrParameterInvalidMsg("cannot parse expression: %s, error: %s", ctx.Expr().GetText(), err)
	}
	floatValueExpr := getValueExpr(floatExpr)
	if floatValueExpr == nil || floatValueExpr.GetValue() == nil {
		return merr.WrapErrParameterInvalidMsg("\"float factor\" should be a const float expression: \"float factor\" passed: %s", ctx.Expr().GetText())
	}

	sampleFactor := floatValueExpr.GetValue().GetFloatVal()
	if sampleFactor <= 0+EPSILON || sampleFactor >= 1-EPSILON {
		return merr.WrapErrParameterInvalidMsg("the sample factor should be between 0 and 1 and not too close to 0 or 1(the difference should be larger than 1e-10), but got %s", ctx.Expr().GetText())
	}
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_RandomSampleExpr{
				RandomSampleExpr: &planpb.RandomSampleExpr{
					SampleFactor: float32(sampleFactor),
					Predicate:    nil,
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
		return merr.WrapErrParameterInvalidMsg("'term' can only be used on non-const expression, but got: %s", ctx.Expr(0).GetText())
	}

	childExpr := getExpr(child)
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg("'term' can only be used on single field, but got: %s", ctx.Expr(0).GetText())
	}

	dataType := columnInfo.GetDataType()
	// Use element type for IN operation in two cases:
	// 1. Array with nested path (e.g., arr[0] IN [1, 2, 3])
	// 2. Array with element level flag (e.g., $[intField] IN [1, 2] in MATCH_ALL/ElementFilter)
	if typeutil.IsArrayType(dataType) && (len(columnInfo.GetNestedPath()) != 0 || columnInfo.GetIsElementLevel()) {
		dataType = columnInfo.GetElementType()
	}

	term := ctx.Expr(1).Accept(v)
	if getError(term) != nil {
		return term
	}

	valueExpr := getValueExpr(term)
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
			return merr.WrapErrParameterInvalidMsg("value '%s' in list cannot be a non-const expression", ctx.Expr(1).GetText())
		}

		if !IsArray(elementValue) {
			return merr.WrapErrParameterInvalidMsg("the right-hand side of 'in' must be a list, but got: %s", ctx.Expr(1).GetText())
		}
		array := elementValue.GetArrayVal().GetArray()
		values = make([]*planpb.GenericValue, len(array))
		for i, e := range array {
			castedValue, err := castValue(dataType, e)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("value '%s' in list cannot be casted to %s", e.String(), dataType.String())
			}
			values[i] = castedValue
		}

		// For JSON type, ensure all numeric values have consistent type.
		// If there's a mix of integers and floats, convert all to floats.
		if typeutil.IsJSONType(dataType) && len(values) > 0 {
			hasInt := false
			hasFloat := false
			for _, val := range values {
				if IsInteger(val) {
					hasInt = true
				} else if IsFloating(val) {
					hasFloat = true
				}
			}
			// If we have both int and float, convert all ints to floats
			if hasInt && hasFloat {
				for i, val := range values {
					if IsInteger(val) {
						values[i] = NewFloat(float64(val.GetInt64Val()))
					}
				}
			}
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
	if ctx.GetOp() != nil {
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

func isValidStructSubField(tokenText string) bool {
	return len(tokenText) >= 4 && tokenText[:2] == "$[" && tokenText[len(tokenText)-1] == ']'
}

func (v *ParserVisitor) getColumnInfoFromStructSubField(tokenText string) (*planpb.ColumnInfo, error) {
	if !isValidStructSubField(tokenText) {
		return nil, merr.WrapErrParameterInvalidMsg("invalid struct sub-field syntax: %s", tokenText)
	}
	// Remove "$[" prefix and "]" suffix
	fieldName := tokenText[2 : len(tokenText)-1]

	// Check if we're inside an ElementFilter context
	if v.currentStructArrayField == "" {
		return nil, merr.WrapErrParameterInvalidMsg("$[%s] syntax can only be used inside ElementFilter", fieldName)
	}

	// Construct full field name for struct array field
	fullFieldName := typeutil.ConcatStructFieldName(v.currentStructArrayField, fieldName)
	// Get the struct array field info
	field, err := v.schema.GetFieldFromName(fullFieldName)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("array field not found: %s, error: %s", fullFieldName, err)
	}

	// In element-level context, data_type should be the element type
	elementType := field.GetElementType()

	return &planpb.ColumnInfo{
		FieldId:         field.FieldID,
		DataType:        elementType, // Use element type, not storage type
		IsPrimaryKey:    field.IsPrimaryKey,
		IsAutoID:        field.AutoID,
		IsPartitionKey:  field.IsPartitionKey,
		IsClusteringKey: field.IsClusteringKey,
		ElementType:     elementType,
		Nullable:        field.GetNullable(),
		IsElementLevel:  true, // Mark as element-level access
	}, nil
}

func (v *ParserVisitor) getChildColumnInfo(identifier, child, structSubField antlr.TerminalNode) (*planpb.ColumnInfo, error) {
	if identifier != nil {
		childExpr, err := v.translateIdentifier(identifier.GetText())
		if err != nil {
			return nil, err
		}
		return toColumnInfo(childExpr), nil
	}

	if structSubField != nil {
		return v.getColumnInfoFromStructSubField(structSubField.GetText())
	}

	return v.getColumnInfoFromJSONIdentifier(child.GetText())
}

// VisitCall parses the expr to call plan.
func (v *ParserVisitor) VisitCall(ctx *parser.CallContext) interface{} {
	functionName := strings.ToLower(ctx.Identifier().GetText())
	numParams := len(ctx.AllExpr())
	funcParameters := make([]*planpb.Expr, 0, numParams)
	for _, param := range ctx.AllExpr() {
		paramExpr := param.Accept(v)
		if err := getError(paramExpr); err != nil {
			return err
		}
		funcParameters = append(funcParameters, getExpr(param.Accept(v)).expr)
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

// VisitRange translates expr to range plan.
func (v *ParserVisitor) VisitRange(ctx *parser.RangeContext) interface{} {
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier(), ctx.StructSubFieldIdentifier())
	if err != nil {
		return err
	}
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg("range operations are only supported on single fields now, got: %s", ctx.Expr(1).GetText())
	}
	if err := checkDirectComparisonBinaryField(columnInfo); err != nil {
		return err
	}

	lower := ctx.Expr(0).Accept(v)
	upper := ctx.Expr(1).Accept(v)
	if err := getError(lower); err != nil {
		return err
	}
	if err := getError(upper); err != nil {
		return err
	}

	lowerValueExpr, upperValueExpr := getValueExpr(lower), getValueExpr(upper)
	if lowerValueExpr == nil {
		return merr.WrapErrParameterInvalidMsg("lowerbound cannot be a non-const expression: %s", ctx.Expr(0).GetText())
	}
	if upperValueExpr == nil {
		return merr.WrapErrParameterInvalidMsg("upperbound cannot be a non-const expression: %s", ctx.Expr(1).GetText())
	}

	fieldDataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		fieldDataType = columnInfo.GetElementType()
	}

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
	lowerInclusive := ctx.GetOp1().GetTokenType() == parser.PlanParserLE
	upperInclusive := ctx.GetOp2().GetTokenType() == parser.PlanParserLE
	if !isTemplateExpr(lowerValueExpr) && !isTemplateExpr(upperValueExpr) {
		if !(lowerInclusive && upperInclusive) {
			if getGenericValue(GreaterEqual(lowerValue, upperValue)).GetBoolVal() {
				return errors.New("invalid range: lowerbound is greater than upperbound")
			}
		} else {
			if getGenericValue(Greater(lowerValue, upperValue)).GetBoolVal() {
				return errors.New("invalid range: lowerbound is greater than upperbound")
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

// VisitReverseRange parses the expression like "1 > a > 0".
func (v *ParserVisitor) VisitReverseRange(ctx *parser.ReverseRangeContext) interface{} {
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier(), ctx.StructSubFieldIdentifier())
	if err != nil {
		return err
	}
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg("range operations are only supported on single fields now, got: %s", ctx.Expr(1).GetText())
	}

	if err := checkDirectComparisonBinaryField(columnInfo); err != nil {
		return err
	}

	lower := ctx.Expr(1).Accept(v)
	upper := ctx.Expr(0).Accept(v)
	if err := getError(lower); err != nil {
		return err
	}
	if err := getError(upper); err != nil {
		return err
	}

	lowerValueExpr, upperValueExpr := getValueExpr(lower), getValueExpr(upper)
	if lowerValueExpr == nil {
		return merr.WrapErrParameterInvalidMsg("lowerbound cannot be a non-const expression: %s", ctx.Expr(0).GetText())
	}
	if upperValueExpr == nil {
		return merr.WrapErrParameterInvalidMsg("upperbound cannot be a non-const expression: %s", ctx.Expr(1).GetText())
	}

	fieldDataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(columnInfo.GetDataType()) {
		fieldDataType = columnInfo.GetElementType()
	}

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
	lowerInclusive := ctx.GetOp2().GetTokenType() == parser.PlanParserGE
	upperInclusive := ctx.GetOp1().GetTokenType() == parser.PlanParserGE
	if !isTemplateExpr(lowerValueExpr) && !isTemplateExpr(upperValueExpr) {
		if !(lowerInclusive && upperInclusive) {
			if getGenericValue(GreaterEqual(lowerValue, upperValue)).GetBoolVal() {
				return errors.New("invalid range: lowerbound is greater than upperbound")
			}
		} else {
			if getGenericValue(Greater(lowerValue, upperValue)).GetBoolVal() {
				return errors.New("invalid range: lowerbound is greater than upperbound")
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

// VisitUnary unpack the +expr to expr.
func (v *ParserVisitor) VisitUnary(ctx *parser.UnaryContext) interface{} {
	child := ctx.Expr().Accept(v)
	if err := getError(child); err != nil {
		// Special case: handle -9223372036854775808
		// ANTLR parses -9223372036854775808 as Unary(SUB, Integer(9223372036854775808)).
		// The integer literal 9223372036854775808 exceeds int64 max, but when combined
		// with unary minus, it represents the valid int64 minimum value.
		if isInt64OverflowError(err) && ctx.GetOp().GetTokenType() == parser.PlanParserSUB {
			return &ExprWithType{
				dataType: schemapb.DataType_Int64,
				expr: &planpb.Expr{
					Expr: &planpb.Expr_ValueExpr{
						ValueExpr: &planpb.ValueExpr{
							Value: NewInt(math.MinInt64),
						},
					},
				},
				nodeDependent: true,
			}
		}
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
			n, err := Not(childValue)
			if err != nil {
				return err
			}
			return n
		default:
			return merr.WrapErrParameterInvalidMsg("unexpected op: %s", ctx.GetOp().GetText())
		}
	}

	childExpr := getExpr(child)
	if childExpr == nil {
		return errors.New("failed to parse unary expressions")
	}
	if isRandomSampleExpr(childExpr) {
		return errors.New("random sample expression cannot be used in unary expression")
	}

	if err := checkDirectComparisonBinaryField(toColumnInfo(childExpr)); err != nil {
		return err
	}
	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserADD:
		return childExpr
	case parser.PlanParserNOT:
		if !canBeExecuted(childExpr) {
			return merr.WrapErrParameterInvalidMsg("%s op can only be applied on boolean expression", unaryLogicalNameMap[parser.PlanParserNOT])
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
		return merr.WrapErrParameterInvalidMsg("unexpected op: %s", ctx.GetOp().GetText())
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
		return errors.New("'or' can only be used between boolean expressions")
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	leftExpr = getExpr(left)
	rightExpr = getExpr(right)
	if isRandomSampleExpr(leftExpr) || isRandomSampleExpr(rightExpr) {
		return errors.New("random sample expression cannot be used in logical and expression")
	}

	if isElementFilterExpr(leftExpr) {
		return errors.New("element filter expression can only be the last expression in the logical or expression")
	}

	if !canBeExecuted(leftExpr) || !canBeExecuted(rightExpr) {
		return errors.New("'or' can only be used between boolean expressions")
	}
	expr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  leftExpr.expr,
				Right: rightExpr.expr,
				Op:    planpb.BinaryExpr_LogicalOr,
			},
		},
		IsTemplate: leftExpr.expr.GetIsTemplate() || rightExpr.expr.GetIsTemplate(),
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
		return errors.New("'and' can only be used between boolean expressions")
	}

	var leftExpr *ExprWithType
	var rightExpr *ExprWithType
	leftExpr = getExpr(left)
	rightExpr = getExpr(right)
	if isRandomSampleExpr(leftExpr) {
		return errors.New("random sample expression can only be the last expression in the logical and expression")
	}

	if isElementFilterExpr(leftExpr) {
		return errors.New("element filter expression can only be the last expression in the logical and expression")
	}

	if !canBeExecuted(leftExpr) || !canBeExecuted(rightExpr) {
		return errors.New("'and' can only be used between boolean expressions")
	}

	var expr *planpb.Expr
	if isRandomSampleExpr(rightExpr) {
		randomSampleExpr := rightExpr.expr.GetRandomSampleExpr()
		randomSampleExpr.Predicate = leftExpr.expr
		expr = &planpb.Expr{
			Expr: &planpb.Expr_RandomSampleExpr{
				RandomSampleExpr: randomSampleExpr,
			},
		}
	} else if isElementFilterExpr(rightExpr) {
		// Similar to RandomSampleExpr, extract doc-level predicate
		elementFilterExpr := rightExpr.expr.GetElementFilterExpr()
		elementFilterExpr.Predicate = leftExpr.expr
		expr = &planpb.Expr{
			Expr: &planpb.Expr_ElementFilterExpr{
				ElementFilterExpr: elementFilterExpr,
			},
		}
	} else {
		expr = &planpb.Expr{
			Expr: &planpb.Expr_BinaryExpr{
				BinaryExpr: &planpb.BinaryExpr{
					Left:  leftExpr.expr,
					Right: rightExpr.expr,
					Op:    planpb.BinaryExpr_LogicalAnd,
				},
			},
			IsTemplate: leftExpr.expr.GetIsTemplate() || rightExpr.expr.GetIsTemplate(),
		}
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitBitXor not supported.
func (v *ParserVisitor) VisitBitXor(ctx *parser.BitXorContext) interface{} {
	return merr.WrapErrParameterInvalidMsg("BitXor is not supported: %s", ctx.GetText())
}

// VisitBitAnd not supported.
func (v *ParserVisitor) VisitBitAnd(ctx *parser.BitAndContext) interface{} {
	return merr.WrapErrParameterInvalidMsg("BitAnd is not supported: %s", ctx.GetText())
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

	return merr.WrapErrParameterInvalidMsg("power can only apply on constants: %s", ctx.GetText())
}

// VisitShift unsupported.
func (v *ParserVisitor) VisitShift(ctx *parser.ShiftContext) interface{} {
	return merr.WrapErrParameterInvalidMsg("shift is not supported: %s", ctx.GetText())
}

// VisitBitOr unsupported.
func (v *ParserVisitor) VisitBitOr(ctx *parser.BitOrContext) interface{} {
	return merr.WrapErrParameterInvalidMsg("BitOr is not supported: %s", ctx.GetText())
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
		return nil, merr.WrapErrParameterInvalidMsg("%s", errMsg)
	}
	if fieldName != field.Name {
		nestedPath = append(nestedPath, fieldName)
	}
	jsonKeyStr := identifier[len(fieldName):]
	ss := strings.Split(jsonKeyStr, "][")
	for i := 0; i < len(ss); i++ {
		path := strings.Trim(ss[i], "[]")
		if path == "" {
			return nil, merr.WrapErrParameterInvalidMsg("invalid identifier: %s", identifier)
		}
		if (strings.HasPrefix(path, "\"") && strings.HasSuffix(path, "\"")) ||
			(strings.HasPrefix(path, "'") && strings.HasSuffix(path, "'")) {
			path = path[1 : len(path)-1]
			if path == "" {
				return nil, merr.WrapErrParameterInvalidMsg("invalid identifier: %s", identifier)
			}
			if typeutil.IsArrayType(field.DataType) {
				return nil, errors.New("can only access array field with integer index")
			}
		} else if _, err := strconv.ParseInt(path, 10, 64); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("json key must be enclosed in double quotes or single quotes: \"%s\"", path)
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

func (v *ParserVisitor) VisitExists(ctx *parser.ExistsContext) interface{} {
	child := ctx.Expr().Accept(v)
	if err := getError(child); err != nil {
		return err
	}
	columnInfo := toColumnInfo(child.(*ExprWithType))
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg(
			"exists operations are only supported on single fields now, got: %s", ctx.Expr().GetText())
	}

	if columnInfo.GetDataType() != schemapb.DataType_JSON {
		return merr.WrapErrParameterInvalidMsg(
			"exists operations are only supportted on json field, got:%s", columnInfo.GetDataType())
	}

	if len(columnInfo.GetNestedPath()) == 0 {
		return merr.WrapErrParameterInvalidMsg(
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
			return merr.WrapErrParameterInvalidMsg("array element type must be generic value, but got: %s", allExpr[i].GetText())
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

func (v *ParserVisitor) VisitIsNotNull(ctx *parser.IsNotNullContext) interface{} {
	column, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier(), nil)
	if err != nil {
		return err
	}

	if typeutil.IsVectorType(column.DataType) {
		return merr.WrapErrParameterInvalidMsg("IsNull/IsNotNull operations are not supported on vector fields")
	}

	if len(column.NestedPath) != 0 {
		// convert json not null expr to exists expr, eg: json['a'] is not null -> exists json['a']
		expr := &planpb.Expr{
			Expr: &planpb.Expr_ExistsExpr{
				ExistsExpr: &planpb.ExistsExpr{
					Info: &planpb.ColumnInfo{
						FieldId:    column.FieldId,
						DataType:   column.DataType,
						NestedPath: column.NestedPath,
					},
				},
			},
		}

		return &ExprWithType{
			expr:     expr,
			dataType: schemapb.DataType_Bool,
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_NullExpr{
			NullExpr: &planpb.NullExpr{
				ColumnInfo: column,
				Op:         planpb.NullExpr_IsNotNull,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitIsNull(ctx *parser.IsNullContext) interface{} {
	column, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier(), nil)
	if err != nil {
		return err
	}

	if typeutil.IsVectorType(column.DataType) {
		return merr.WrapErrParameterInvalidMsg("IsNull/IsNotNull operations are not supported on vector fields")
	}

	if len(column.NestedPath) != 0 {
		// convert json is null expr to not exists expr, eg: json['a'] is null -> not exists json['a']
		expr := &planpb.Expr{
			Expr: &planpb.Expr_ExistsExpr{
				ExistsExpr: &planpb.ExistsExpr{
					Info: &planpb.ColumnInfo{
						FieldId:    column.FieldId,
						DataType:   column.DataType,
						NestedPath: column.NestedPath,
					},
				},
			},
		}
		return &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_UnaryExpr{
					UnaryExpr: &planpb.UnaryExpr{
						Op:    unaryLogicalOpMap[parser.PlanParserNOT],
						Child: expr,
					},
				},
			},
			dataType: schemapb.DataType_Bool,
		}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_NullExpr{
			NullExpr: &planpb.NullExpr{
				ColumnInfo: column,
				Op:         planpb.NullExpr_IsNull,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
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
		return merr.WrapErrParameterInvalidMsg(
			"contains operation are only supported on json or array fields now, got: %s", ctx.Expr(0).GetText())
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return err
	}
	elementExpr := getValueExpr(element)
	if elementExpr == nil {
		return merr.WrapErrParameterInvalidMsg(
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
		return merr.WrapErrParameterInvalidMsg(
			"contains_all operation are only supported on json or array fields now, got: %s", ctx.Expr(0).GetText())
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return err
	}

	elementExpr := getValueExpr(element)
	if elementExpr == nil {
		return merr.WrapErrParameterInvalidMsg(
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
		return merr.WrapErrParameterInvalidMsg(
			"contains_any operation are only supported on json or array fields now, got: %s", ctx.Expr(0).GetText())
	}

	element := ctx.Expr(1).Accept(v)
	if err := getError(element); err != nil {
		return err
	}
	valueExpr := getValueExpr(element)

	if valueExpr == nil {
		return merr.WrapErrParameterInvalidMsg(
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
	columnInfo, err := v.getChildColumnInfo(ctx.Identifier(), ctx.JSONIdentifier(), nil)
	if err != nil {
		return err
	}
	if columnInfo == nil ||
		(!typeutil.IsJSONType(columnInfo.GetDataType()) && !typeutil.IsArrayType(columnInfo.GetDataType())) {
		return merr.WrapErrParameterInvalidMsg(
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

func (v *ParserVisitor) VisitSpatialBinary(ctx *parser.SpatialBinaryContext) interface{} {
	childExpr, err := v.translateIdentifier(ctx.Identifier().GetText())
	if err != nil {
		return err
	}
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil ||
		(!typeutil.IsGeometryType(columnInfo.GetDataType())) {
		return merr.WrapErrParameterInvalidMsg(
			"spatial operation are only supported on geometry fields now, got: %s", ctx.GetText())
	}
	// Process the WKT string
	element := ctx.StringLiteral().GetText()
	wktString := element[1 : len(element)-1] // Remove surrounding quotes

	if err := checkValidWKT(wktString); err != nil {
		return err
	}

	// Map token type to GIS operation
	var op planpb.GISFunctionFilterExpr_GISOp
	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserSTEuqals:
		op = planpb.GISFunctionFilterExpr_Equals
	case parser.PlanParserSTTouches:
		op = planpb.GISFunctionFilterExpr_Touches
	case parser.PlanParserSTOverlaps:
		op = planpb.GISFunctionFilterExpr_Overlaps
	case parser.PlanParserSTCrosses:
		op = planpb.GISFunctionFilterExpr_Crosses
	case parser.PlanParserSTContains:
		op = planpb.GISFunctionFilterExpr_Contains
	case parser.PlanParserSTIntersects:
		op = planpb.GISFunctionFilterExpr_Intersects
	case parser.PlanParserSTWithin:
		op = planpb.GISFunctionFilterExpr_Within
	default:
		return merr.WrapErrParameterInvalidMsg("unhandled spatial operator: %s", ctx.GetOp().GetText())
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_GisfunctionFilterExpr{
			GisfunctionFilterExpr: &planpb.GISFunctionFilterExpr{
				ColumnInfo: columnInfo,
				WktString:  wktString,
				Op:         op,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitSTIsValid(ctx *parser.STIsValidContext) interface{} {
	childExpr, err := v.translateIdentifier(ctx.Identifier().GetText())
	if err != nil {
		return err
	}
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil ||
		(!typeutil.IsGeometryType(columnInfo.GetDataType())) {
		return merr.WrapErrParameterInvalidMsg(
			"STIsValid operation are only supported on geometry fields now, got: %s", ctx.GetText())
	}
	expr := &planpb.Expr{
		Expr: &planpb.Expr_GisfunctionFilterExpr{
			GisfunctionFilterExpr: &planpb.GISFunctionFilterExpr{
				ColumnInfo: columnInfo,
				Op:         planpb.GISFunctionFilterExpr_STIsValid,
			},
		},
	}
	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

func (v *ParserVisitor) VisitSTDWithin(ctx *parser.STDWithinContext) interface{} {
	// Process the geometry field identifier
	childExpr, err := v.translateIdentifier(ctx.Identifier().GetText())
	if err != nil {
		return err
	}
	columnInfo := toColumnInfo(childExpr)
	if columnInfo == nil ||
		(!typeutil.IsGeometryType(columnInfo.GetDataType())) {
		return merr.WrapErrParameterInvalidMsg(
			"ST_DWITHIN operation are only supported on geometry fields now, got: %s", ctx.GetText())
	}

	// Process the WKT string
	element := ctx.StringLiteral().GetText()
	wktString := element[1 : len(element)-1] // Remove surrounding quotes

	if err = checkValidPoint(wktString); err != nil {
		return err
	}

	// Process the distance expression (can be int or float)
	distanceExpr := ctx.Expr().Accept(v)
	if err := getError(distanceExpr); err != nil {
		return err
	}

	// Extract distance value - must be a constant expression
	distanceValueExpr := getValueExpr(distanceExpr)
	if distanceValueExpr == nil {
		return merr.WrapErrParameterInvalidMsg("distance parameter must be a constant numeric value, got: %s", ctx.Expr().GetText())
	}

	var distance float64
	genericValue := distanceValueExpr.GetValue()
	if genericValue == nil {
		return merr.WrapErrParameterInvalidMsg("invalid distance value: %s", ctx.Expr().GetText())
	}

	// Handle both integer and floating point values using type assertion
	switch val := genericValue.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		distance = float64(val.Int64Val)
	case *planpb.GenericValue_FloatVal:
		distance = val.FloatVal
	default:
		return merr.WrapErrParameterInvalidMsg("distance parameter must be a numeric value (int or float), got: %s", ctx.Expr().GetText())
	}

	if distance < 0 {
		return merr.WrapErrParameterInvalidMsg("distance parameter must be non-negative, got: %f", distance)
	}

	// Create the GIS function expression using the bounding box
	expr := &planpb.Expr{
		Expr: &planpb.Expr_GisfunctionFilterExpr{
			GisfunctionFilterExpr: &planpb.GISFunctionFilterExpr{
				ColumnInfo: columnInfo,
				WktString:  wktString, // Use bounding box instead of original point
				Op:         planpb.GISFunctionFilterExpr_DWithin,
				Distance:   distance, // Keep distance for reference
			},
		},
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}

// VisitTimestamptzCompareForward handles comparison expressions where the column
// is on the left side of the operator.
// Syntax example: column > '2025-01-01' [ + INTERVAL 'P1D' ]
//
// Optimization Logic:
//  1. Quick Path: If no INTERVAL is provided, it generates a UnaryRangeExpr
//     to enable index-based scan performance in Milvus.
//  2. Slow Path: If an INTERVAL exists, it generates a TimestamptzArithCompareExpr
//     for specialized arithmetic evaluation.
func (v *ParserVisitor) VisitTimestamptzCompareForward(ctx *parser.TimestamptzCompareForwardContext) interface{} {
	colExpr, err := v.translateIdentifier(ctx.Identifier().GetText())
	identifier := ctx.Identifier().Accept(v)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("can not translate identifier: %s", identifier)
	}
	if colExpr.dataType != schemapb.DataType_Timestamptz {
		return merr.WrapErrParameterInvalidMsg("field '%s' is not a timestamptz datatype", identifier)
	}

	compareOp := cmpOpMap[ctx.GetOp2().GetTokenType()]
	rawCompareStr := ctx.GetCompare_string().GetText()
	unquotedCompareStr, err := convertEscapeSingle(rawCompareStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("can not convert compare string: %s", rawCompareStr)
	}
	timestamptzInt64, err := timestamptz.ValidateAndReturnUnixMicroTz(unquotedCompareStr, v.args.Timezone)
	if err != nil {
		return err
	}

	if ctx.GetOp1() == nil {
		return &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_UnaryRangeExpr{
					UnaryRangeExpr: &planpb.UnaryRangeExpr{
						ColumnInfo: toColumnInfo(colExpr),
						Op:         compareOp,
						Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: timestamptzInt64}},
					},
				},
			},
			dataType: schemapb.DataType_Bool,
		}
	}

	arithOp := arithExprMap[ctx.GetOp1().GetTokenType()]
	rawIntervalStr := ctx.GetInterval_string().GetText()
	unquotedIntervalStr, err := convertEscapeSingle(rawIntervalStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("can not convert interval string: %s", rawIntervalStr)
	}
	interval, err := parseISODuration(unquotedIntervalStr)
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_TimestamptzArithCompareExpr{
				TimestamptzArithCompareExpr: &planpb.TimestamptzArithCompareExpr{
					TimestamptzColumn: toColumnInfo(colExpr),
					ArithOp:           arithOp,
					Interval:          interval,
					CompareOp:         compareOp,
					CompareValue:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: timestamptzInt64}},
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

// VisitTimestamptzCompareReverse handles comparison expressions where the column
// is on the right side of the operator.
// Syntax example: '2025-01-01' [ + INTERVAL 'P1D' ] > column
//
// Optimization and Normalization Logic:
//  1. Operator Reversal: The comparison operator is flipped (e.g., '>' to '<')
//     to normalize the expression into a column-centric format.
//  2. Quick Path: For simple comparisons without INTERVAL, it generates a
//     UnaryRangeExpr with the reversed operator to leverage indexing.
//  3. Slow Path: For complex expressions involving INTERVAL, it produces a
//     TimestamptzArithCompareExpr with the reversed operator.
func (v *ParserVisitor) VisitTimestamptzCompareReverse(ctx *parser.TimestamptzCompareReverseContext) interface{} {
	colExpr, err := v.translateIdentifier(ctx.Identifier().GetText())
	identifier := ctx.Identifier().GetText()
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("can not translate identifier: %s", identifier)
	}
	if colExpr.dataType != schemapb.DataType_Timestamptz {
		return merr.WrapErrParameterInvalidMsg("field '%s' is not a timestamptz datatype", identifier)
	}

	rawCompareStr := ctx.GetCompare_string().GetText()
	unquotedCompareStr, err := convertEscapeSingle(rawCompareStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("can not convert compare string: %s", rawCompareStr)
	}

	originalCompareOp := cmpOpMap[ctx.GetOp2().GetTokenType()]
	compareOp := reverseCompareOp(originalCompareOp)
	if compareOp == planpb.OpType_Invalid && originalCompareOp != planpb.OpType_Invalid {
		return merr.WrapErrParameterInvalidMsg("unsupported comparison operator for reverse Timestamptz: %s", ctx.GetOp2().GetText())
	}

	timestamptzInt64, err := timestamptz.ValidateAndReturnUnixMicroTz(unquotedCompareStr, v.args.Timezone)
	if err != nil {
		return err
	}

	// Quick Path: No arithmetic operation. Use UnaryRangeExpr for index optimization.
	if ctx.GetOp1() == nil {
		return &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_UnaryRangeExpr{
					UnaryRangeExpr: &planpb.UnaryRangeExpr{
						ColumnInfo: toColumnInfo(colExpr),
						Op:         compareOp,
						Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: timestamptzInt64}},
					},
				},
			},
			dataType: schemapb.DataType_Bool,
		}
	}

	// Slow Path: Handle arithmetic with TimestamptzArithCompareExpr.
	arithOp := arithExprMap[ctx.GetOp1().GetTokenType()]
	rawIntervalStr := ctx.GetInterval_string().GetText()
	unquotedIntervalStr, err := convertEscapeSingle(rawIntervalStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("can not convert interval string: %s", rawIntervalStr)
	}
	interval, err := parseISODuration(unquotedIntervalStr)
	if err != nil {
		return err
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_TimestamptzArithCompareExpr{
				TimestamptzArithCompareExpr: &planpb.TimestamptzArithCompareExpr{
					TimestamptzColumn: toColumnInfo(colExpr),
					ArithOp:           arithOp,
					Interval:          interval,
					CompareOp:         compareOp,
					CompareValue: &planpb.GenericValue{
						Val: &planpb.GenericValue_Int64Val{Int64Val: timestamptzInt64},
					},
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

func reverseCompareOp(op planpb.OpType) planpb.OpType {
	switch op {
	case planpb.OpType_LessThan:
		return planpb.OpType_GreaterThan
	case planpb.OpType_LessEqual:
		return planpb.OpType_GreaterEqual
	case planpb.OpType_GreaterThan:
		return planpb.OpType_LessThan
	case planpb.OpType_GreaterEqual:
		return planpb.OpType_LessEqual
	case planpb.OpType_Equal:
		return planpb.OpType_Equal
	case planpb.OpType_NotEqual:
		return planpb.OpType_NotEqual
	default:
		return planpb.OpType_Invalid
	}
}

func validateAndExtractMinShouldMatch(minShouldMatchExpr interface{}) ([]*planpb.GenericValue, error) {
	if minShouldMatchValue, ok := minShouldMatchExpr.(*ExprWithType); ok {
		valueExpr := getValueExpr(minShouldMatchValue)
		if valueExpr == nil || valueExpr.GetValue() == nil {
			return nil, merr.WrapErrParameterInvalidMsg("minimum_should_match should be a const integer expression")
		}
		minShouldMatch := valueExpr.GetValue().GetInt64Val()
		if minShouldMatch < 1 {
			return nil, merr.WrapErrParameterInvalidMsg("minimum_should_match should be >= 1, got %d", minShouldMatch)
		}
		if minShouldMatch > 1000 {
			return nil, merr.WrapErrParameterInvalidMsg("minimum_should_match should be <= 1000, got %d", minShouldMatch)
		}
		return []*planpb.GenericValue{NewInt(minShouldMatch)}, nil
	}
	return nil, nil
}

// VisitElementFilter handles ElementFilter(structArrayField, elementExpr) syntax.
func (v *ParserVisitor) VisitElementFilter(ctx *parser.ElementFilterContext) interface{} {
	// Check for nested ElementFilter - not allowed
	if v.currentStructArrayField != "" {
		return merr.WrapErrParameterInvalidMsg("nested ElementFilter is not supported, already inside ElementFilter for field: %s", v.currentStructArrayField)
	}

	// Get struct array field name (first parameter)
	arrayFieldName := ctx.Identifier().GetText()

	// Set current context for element expression parsing
	v.currentStructArrayField = arrayFieldName
	defer func() { v.currentStructArrayField = "" }()

	elementExpr := ctx.Expr().Accept(v)
	if err := getError(elementExpr); err != nil {
		return merr.WrapErrParameterInvalidMsg("cannot parse element expression: %s, error: %s", ctx.Expr().GetText(), err)
	}

	exprWithType := getExpr(elementExpr)
	if exprWithType == nil {
		return merr.WrapErrParameterInvalidMsg("invalid element expression: %s", ctx.Expr().GetText())
	}

	// Build ElementFilterExpr proto
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ElementFilterExpr{
				ElementFilterExpr: &planpb.ElementFilterExpr{
					ElementExpr: exprWithType.expr,
					StructName:  arrayFieldName,
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

// VisitStructSubField handles $[fieldName] syntax within ElementFilter.
func (v *ParserVisitor) VisitStructSubField(ctx *parser.StructSubFieldContext) interface{} {
	// Extract the field name from $[fieldName]
	tokenText := ctx.StructSubFieldIdentifier().GetText()
	if !isValidStructSubField(tokenText) {
		return merr.WrapErrParameterInvalidMsg("invalid struct sub-field syntax: %s", tokenText)
	}
	// Remove "$[" prefix and "]" suffix
	fieldName := tokenText[2 : len(tokenText)-1]

	// Check if we're inside an ElementFilter or MATCH_* context
	if v.currentStructArrayField == "" {
		return merr.WrapErrParameterInvalidMsg("$[%s] syntax can only be used inside ElementFilter or MATCH_*", fieldName)
	}

	// Construct full field name for struct array field
	fullFieldName := typeutil.ConcatStructFieldName(v.currentStructArrayField, fieldName)
	// Get the struct array field info
	field, err := v.schema.GetFieldFromName(fullFieldName)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("array field not found: %s, error: %s", fullFieldName, err)
	}

	// In element-level context, use Array as storage type, element type for operations
	elementType := field.GetElementType()

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_ColumnExpr{
				ColumnExpr: &planpb.ColumnExpr{
					Info: &planpb.ColumnInfo{
						FieldId:         field.FieldID,
						DataType:        schemapb.DataType_Array, // Storage type is Array
						IsPrimaryKey:    field.IsPrimaryKey,
						IsAutoID:        field.AutoID,
						IsPartitionKey:  field.IsPartitionKey,
						IsClusteringKey: field.IsClusteringKey,
						ElementType:     elementType, // Element type for operations
						Nullable:        field.GetNullable(),
						IsElementLevel:  true, // Mark as element-level access
					},
				},
			},
		},
		dataType:      elementType, // Expression evaluates to element type
		nodeDependent: true,
	}
}

// parseMatchExpr is a helper function for parsing match expressions
// matchType: the type of match operation (MatchAll, MatchAny, MatchLeast, MatchMost)
// count: for MatchLeast/MatchMost, the count parameter (N); for MatchAll/MatchAny, this is ignored (0)
func (v *ParserVisitor) parseMatchExpr(structArrayFieldName string, exprCtx parser.IExprContext, matchType planpb.MatchType, count int64, funcName string) interface{} {
	// Check for nested match expression - not allowed
	if v.currentStructArrayField != "" {
		return merr.WrapErrParameterInvalidMsg("nested %s is not supported, already inside match expression for field: %s", funcName, v.currentStructArrayField)
	}

	// Set current context for element expression parsing
	v.currentStructArrayField = structArrayFieldName
	defer func() { v.currentStructArrayField = "" }()

	// Parse the predicate expression
	predicate := exprCtx.Accept(v)
	if err := getError(predicate); err != nil {
		return merr.WrapErrParameterInvalidMsg("cannot parse predicate expression: %s, error: %s", exprCtx.GetText(), err)
	}

	predicateExpr := getExpr(predicate)
	if predicateExpr == nil {
		return merr.WrapErrParameterInvalidMsg("invalid predicate expression in %s: %s", funcName, exprCtx.GetText())
	}

	// Build MatchExpr proto
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_MatchExpr{
				MatchExpr: &planpb.MatchExpr{
					StructName: structArrayFieldName,
					Predicate:  predicateExpr.expr,
					MatchType:  matchType,
					Count:      count,
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}

// VisitMatchSimple handles MATCH_ALL and MATCH_ANY expressions
// Syntax: MATCH_ALL/MATCH_ANY(structArrayField, $[intField] == 1 && $[strField] == "aaa")
func (v *ParserVisitor) VisitMatchSimple(ctx *parser.MatchSimpleContext) interface{} {
	structArrayFieldName := ctx.Identifier().GetText()
	var matchType planpb.MatchType
	var opName string
	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserMATCH_ALL:
		matchType = planpb.MatchType_MatchAll
		opName = "MATCH_ALL"
	case parser.PlanParserMATCH_ANY:
		matchType = planpb.MatchType_MatchAny
		opName = "MATCH_ANY"
	default:
		return merr.WrapErrParameterInvalidMsg("unhandled match operator: %s", ctx.GetOp().GetText())
	}
	return v.parseMatchExpr(structArrayFieldName, ctx.Expr(), matchType, 0, opName)
}

// VisitMatchThreshold handles MATCH_LEAST, MATCH_MOST, and MATCH_EXACT expressions
// Syntax: MATCH_LEAST/MATCH_MOST/MATCH_EXACT(structArrayField, $[intField] == 1, threshold=N)
func (v *ParserVisitor) VisitMatchThreshold(ctx *parser.MatchThresholdContext) interface{} {
	structArrayFieldName := ctx.Identifier().GetText()

	countStr := ctx.IntegerConstant().GetText()
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid count: %s", countStr)
	}

	var matchType planpb.MatchType
	var opName string
	switch ctx.GetOp().GetTokenType() {
	case parser.PlanParserMATCH_LEAST:
		matchType = planpb.MatchType_MatchLeast
		opName = "MATCH_LEAST"
		if count <= 0 {
			return merr.WrapErrParameterInvalidMsg("count in MATCH_LEAST must be positive, got: %d", count)
		}
	case parser.PlanParserMATCH_MOST:
		matchType = planpb.MatchType_MatchMost
		opName = "MATCH_MOST"
		if count < 0 {
			return merr.WrapErrParameterInvalidMsg("count in MATCH_MOST cannot be negative, got: %d", count)
		}
	case parser.PlanParserMATCH_EXACT:
		matchType = planpb.MatchType_MatchExact
		opName = "MATCH_EXACT"
		if count < 0 {
			return merr.WrapErrParameterInvalidMsg("count in MATCH_EXACT cannot be negative, got: %d", count)
		}
	default:
		return merr.WrapErrParameterInvalidMsg("unhandled match threshold operator: %s", ctx.GetOp().GetText())
	}

	return v.parseMatchExpr(structArrayFieldName, ctx.Expr(), matchType, count, opName)
}
