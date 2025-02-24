package planparserv2

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func FillExpressionValue(expr *planpb.Expr, templateValues map[string]*planpb.GenericValue) error {
	if !expr.GetIsTemplate() {
		return nil
	}

	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		return FillTermExpressionValue(e.TermExpr, templateValues)
	case *planpb.Expr_UnaryExpr:
		return FillExpressionValue(e.UnaryExpr.GetChild(), templateValues)
	case *planpb.Expr_BinaryExpr:
		if err := FillExpressionValue(e.BinaryExpr.GetLeft(), templateValues); err != nil {
			return err
		}
		return FillExpressionValue(e.BinaryExpr.GetRight(), templateValues)
	case *planpb.Expr_UnaryRangeExpr:
		return FillUnaryRangeExpressionValue(e.UnaryRangeExpr, templateValues)
	case *planpb.Expr_BinaryRangeExpr:
		return FillBinaryRangeExpressionValue(e.BinaryRangeExpr, templateValues)
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		return FillBinaryArithOpEvalRangeExpressionValue(e.BinaryArithOpEvalRangeExpr, templateValues)
	case *planpb.Expr_BinaryArithExpr:
		if err := FillExpressionValue(e.BinaryArithExpr.GetLeft(), templateValues); err != nil {
			return err
		}
		return FillExpressionValue(e.BinaryArithExpr.GetRight(), templateValues)
	case *planpb.Expr_JsonContainsExpr:
		return FillJSONContainsExpressionValue(e.JsonContainsExpr, templateValues)
	default:
		return fmt.Errorf("this expression no need to fill placeholder with expr type: %T", e)
	}
}

func FillTermExpressionValue(expr *planpb.TermExpr, templateValues map[string]*planpb.GenericValue) error {
	value, ok := templateValues[expr.GetTemplateVariableName()]
	if !ok && expr.GetValues() == nil {
		return fmt.Errorf("the value of expression template variable name {%s} is not found", expr.GetTemplateVariableName())
	}

	if value == nil || value.GetArrayVal() == nil {
		return fmt.Errorf("the value of term expression template variable {%s} is not array", expr.GetTemplateVariableName())
	}
	dataType := expr.GetColumnInfo().GetDataType()
	if typeutil.IsArrayType(dataType) {
		if len(expr.GetColumnInfo().GetNestedPath()) != 0 {
			dataType = expr.GetColumnInfo().GetElementType()
		}
	}

	array := value.GetArrayVal().GetArray()
	values := make([]*planpb.GenericValue, len(array))
	for i, e := range array {
		castedValue, err := castValue(dataType, e)
		if err != nil {
			return err
		}
		values[i] = castedValue
	}
	expr.Values = values

	return nil
}

func FillUnaryRangeExpressionValue(expr *planpb.UnaryRangeExpr, templateValues map[string]*planpb.GenericValue) error {
	value, ok := templateValues[expr.GetTemplateVariableName()]
	if !ok {
		return fmt.Errorf("the value of expression template variable name {%s} is not found", expr.GetTemplateVariableName())
	}

	dataType := expr.GetColumnInfo().GetDataType()
	if typeutil.IsArrayType(dataType) {
		if len(expr.GetColumnInfo().GetNestedPath()) != 0 {
			dataType = expr.GetColumnInfo().GetElementType()
		}
	}

	castedValue, err := castValue(dataType, value)
	if err != nil {
		return err
	}
	expr.Value = castedValue
	return nil
}

func FillBinaryRangeExpressionValue(expr *planpb.BinaryRangeExpr, templateValues map[string]*planpb.GenericValue) error {
	var ok bool
	dataType := expr.GetColumnInfo().GetDataType()
	if typeutil.IsArrayType(dataType) && len(expr.GetColumnInfo().GetNestedPath()) != 0 {
		dataType = expr.GetColumnInfo().GetElementType()
	}
	lowerValue := expr.GetLowerValue()
	if lowerValue == nil || expr.GetLowerTemplateVariableName() != "" {
		lowerValue, ok = templateValues[expr.GetLowerTemplateVariableName()]
		if !ok {
			return fmt.Errorf("the lower value of expression template variable name {%s} is not found", expr.GetLowerTemplateVariableName())
		}
		castedLowerValue, err := castValue(dataType, lowerValue)
		if err != nil {
			return err
		}
		expr.LowerValue = castedLowerValue
	}

	upperValue := expr.GetUpperValue()
	if upperValue == nil || expr.GetUpperTemplateVariableName() != "" {
		upperValue, ok = templateValues[expr.GetUpperTemplateVariableName()]
		if !ok {
			return fmt.Errorf("the upper value of expression template variable name {%s} is not found", expr.GetUpperTemplateVariableName())
		}

		castedUpperValue, err := castValue(dataType, upperValue)
		if err != nil {
			return err
		}
		expr.UpperValue = castedUpperValue
	}

	if !(expr.GetLowerInclusive() && expr.GetUpperInclusive()) {
		if getGenericValue(GreaterEqual(lowerValue, upperValue)).GetBoolVal() {
			return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
		}
	} else {
		if getGenericValue(Greater(lowerValue, upperValue)).GetBoolVal() {
			return fmt.Errorf("invalid range: lowerbound is greater than upperbound")
		}
	}

	return nil
}

func FillBinaryArithOpEvalRangeExpressionValue(expr *planpb.BinaryArithOpEvalRangeExpr, templateValues map[string]*planpb.GenericValue) error {
	var dataType schemapb.DataType
	var err error
	var ok bool

	if expr.ArithOp == planpb.ArithOpType_ArrayLength {
		dataType = schemapb.DataType_Int64
	} else {
		operand := expr.GetRightOperand()
		if operand == nil || expr.GetOperandTemplateVariableName() != "" {
			operand, ok = templateValues[expr.GetOperandTemplateVariableName()]
			if !ok {
				return fmt.Errorf("the right operand value of expression template variable name {%s} is not found", expr.GetOperandTemplateVariableName())
			}
		}

		operandExpr := toValueExpr(operand)
		lDataType, rDataType := expr.GetColumnInfo().GetDataType(), operandExpr.dataType
		if typeutil.IsArrayType(expr.GetColumnInfo().GetDataType()) {
			lDataType = expr.GetColumnInfo().GetElementType()
		}

		if err = checkValidModArith(expr.GetArithOp(), expr.GetColumnInfo().GetDataType(), expr.GetColumnInfo().GetElementType(),
			rDataType, schemapb.DataType_None); err != nil {
			return err
		}

		if operand.GetArrayVal() != nil {
			return fmt.Errorf("can not comparisons array directly")
		}

		dataType, err = getTargetType(lDataType, rDataType)
		if err != nil {
			return err
		}

		castedOperand, err := castValue(dataType, operand)
		if err != nil {
			return err
		}
		expr.RightOperand = castedOperand
	}

	value := expr.GetValue()
	if expr.GetValue() == nil || expr.GetValueTemplateVariableName() != "" {
		value, ok = templateValues[expr.GetValueTemplateVariableName()]
		if !ok {
			return fmt.Errorf("the value of expression template variable name {%s} is not found", expr.GetValueTemplateVariableName())
		}
	}
	castedValue, err := castValue(dataType, value)
	if err != nil {
		return err
	}
	expr.Value = castedValue

	return nil
}

func FillJSONContainsExpressionValue(expr *planpb.JSONContainsExpr, templateValues map[string]*planpb.GenericValue) error {
	if expr.GetElements() != nil && expr.GetTemplateVariableName() == "" {
		return nil
	}
	value, ok := templateValues[expr.GetTemplateVariableName()]
	if !ok {
		return fmt.Errorf("the value of expression template variable name {%s} is not found", expr.GetTemplateVariableName())
	}
	if err := checkContainsElement(toColumnExpr(expr.GetColumnInfo()), expr.GetOp(), value); err != nil {
		return err
	}
	dataType := expr.GetColumnInfo().GetDataType()
	if typeutil.IsArrayType(dataType) {
		dataType = expr.GetColumnInfo().GetElementType()
	}
	if expr.GetOp() == planpb.JSONContainsExpr_Contains {
		castedValue, err := castValue(dataType, value)
		if err != nil {
			return err
		}
		expr.Elements = append(expr.Elements, castedValue)
	} else {
		for _, e := range value.GetArrayVal().GetArray() {
			castedValue, err := castValue(dataType, e)
			if err != nil {
				return err
			}
			expr.Elements = append(expr.Elements, castedValue)
		}
	}
	return nil
}
