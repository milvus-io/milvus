// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rlsutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateParsedExpression enforces the RLS policy expression subset after parsing.
func ValidateParsedExpression(expr *planpb.Expr, allowedTemplateVariables map[string]struct{}) error {
	if expr == nil {
		return merr.WrapErrParameterInvalidMsg("RLS expression is empty")
	}
	if rewriter.IsAlwaysFalseExpr(expr) {
		return nil
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return nil
	case *planpb.Expr_ValueExpr:
		if _, ok := node.ValueExpr.GetValue().GetVal().(*planpb.GenericValue_BoolVal); !ok {
			return merr.WrapErrParameterInvalidMsg("RLS value expression must be boolean")
		}
		return nil
	case *planpb.Expr_UnaryExpr, *planpb.Expr_BinaryExpr:
		return merr.WrapErrParameterInvalidMsg("compound RLS expressions are not supported for RLS policy validation")
	case *planpb.Expr_UnaryRangeExpr:
		return validateUnaryRangeExpr(node.UnaryRangeExpr, allowedTemplateVariables)
	case *planpb.Expr_TermExpr:
		return validateTermExpr(node.TermExpr, allowedTemplateVariables)
	case *planpb.Expr_JsonContainsExpr:
		return validateJSONContainsExpr(node.JsonContainsExpr, allowedTemplateVariables)
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported RLS policy expression node %T", node)
	}
}

// ValidateUsingExpressionSchema rejects schema shapes that the serving path
// cannot evaluate with the same semantics as Proxy write checks.
func ValidateUsingExpressionSchema(schema *typeutil.SchemaHelper, expr *planpb.Expr) error {
	if schema == nil {
		return merr.WrapErrParameterInvalidMsg("RLS expression requires a schema")
	}
	node, ok := expr.GetExpr().(*planpb.Expr_JsonContainsExpr)
	if !ok {
		return nil
	}
	field, err := schema.GetFieldFromID(node.JsonContainsExpr.GetColumnInfo().GetFieldId())
	if err != nil {
		return merr.Wrap(err, "resolve RLS array field")
	}
	if field.GetElementNullable() {
		return merr.WrapErrParameterInvalidMsg("RLS using_expr does not support element-nullable array fields")
	}
	return nil
}

func validateUnaryRangeExpr(expr *planpb.UnaryRangeExpr, allowedTemplateVariables map[string]struct{}) error {
	if expr.GetOp() != planpb.OpType_Equal {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports equality comparison")
	}
	if expr.GetValue() == nil && expr.GetTemplateVariableName() == "" {
		return merr.WrapErrParameterInvalidMsg("RLS equality comparison requires a value or principal variable")
	}
	if err := validateTemplateVariable(expr.GetTemplateVariableName(), allowedTemplateVariables); err != nil {
		return err
	}
	if err := validateScalarColumn(expr.GetColumnInfo()); err != nil {
		return err
	}
	if expr.GetTemplateVariableName() == funcutil.RLSPrincipalTemplateName && !typeutil.IsStringType(expr.GetColumnInfo().GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS current principal can only be compared with string fields")
	}
	return validateTagTemplateType(expr.GetTemplateVariableName(), expr.GetColumnInfo().GetDataType())
}

func validateTermExpr(expr *planpb.TermExpr, allowedTemplateVariables map[string]struct{}) error {
	if expr.GetIsInField() {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support field-to-field IN")
	}
	if err := validateTemplateVariable(expr.GetTemplateVariableName(), allowedTemplateVariables); err != nil {
		return err
	}
	if expr.GetTemplateVariableName() != "" {
		return merr.WrapErrParameterInvalidMsg("RLS principal variables cannot be used as IN-list templates")
	}
	return validateScalarColumn(expr.GetColumnInfo())
}

func validateJSONContainsExpr(expr *planpb.JSONContainsExpr, allowedTemplateVariables map[string]struct{}) error {
	if err := validateTemplateVariable(expr.GetTemplateVariableName(), allowedTemplateVariables); err != nil {
		return err
	}
	column := expr.GetColumnInfo()
	if err := validateTopLevelColumn(column); err != nil {
		return err
	}
	if !typeutil.IsArrayType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports array_contains on array fields")
	}
	if !typeutil.IsPrimitiveType(column.GetElementType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports primitive array element fields")
	}
	switch expr.GetOp() {
	case planpb.JSONContainsExpr_Contains:
	case planpb.JSONContainsExpr_ContainsAll, planpb.JSONContainsExpr_ContainsAny:
		if expr.GetTemplateVariableName() != "" {
			return merr.WrapErrParameterInvalidMsg("RLS principal variables can only be used with array_contains")
		}
		if typeutil.IsIntegerType(column.GetElementType()) {
			for _, element := range expr.GetElements() {
				if _, ok := element.GetVal().(*planpb.GenericValue_FloatVal); ok {
					return merr.WrapErrParameterInvalidMsg("RLS %s does not support floating literals on integer array fields", expr.GetOp().String())
				}
			}
		}
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported RLS array_contains operator %s", expr.GetOp().String())
	}
	if expr.GetTemplateVariableName() == funcutil.RLSPrincipalTemplateName && !typeutil.IsStringType(column.GetElementType()) {
		return merr.WrapErrParameterInvalidMsg("RLS current principal can only be compared with string array fields")
	}
	return validateTagTemplateType(expr.GetTemplateVariableName(), column.GetElementType())
}

func validateTagTemplateType(templateVariable string, dataType schemapb.DataType) error {
	if templateVariable == "" || templateVariable == funcutil.RLSPrincipalTemplateName {
		return nil
	}
	if typeutil.IsStringType(dataType) || typeutil.IsIntegerType(dataType) || typeutil.IsFloatingType(dataType) {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg("RLS principal tags can only be compared with string, integer, or floating fields")
}

func validateTemplateVariable(templateVariable string, allowedTemplateVariables map[string]struct{}) error {
	if templateVariable == "" {
		return nil
	}
	if _, ok := allowedTemplateVariables[templateVariable]; !ok {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression contains unsupported template variable %q", templateVariable)
	}
	return nil
}

func validateScalarColumn(column *planpb.ColumnInfo) error {
	if err := validateTopLevelColumn(column); err != nil {
		return err
	}
	if column.GetDataType() == schemapb.DataType_String {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support deprecated String fields; use VarChar instead")
	}
	if !typeutil.IsPrimitiveType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports top-level scalar fields")
	}
	return nil
}

func validateTopLevelColumn(column *planpb.ColumnInfo) error {
	if column == nil {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression has empty column info")
	}
	if common.IsSystemField(column.GetFieldId()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support system fields")
	}
	if len(column.GetNestedPath()) > 0 || column.GetIsElementLevel() {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support nested or element-level fields")
	}
	if typeutil.IsVectorType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support vector fields")
	}
	if typeutil.IsJSONType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support JSON fields")
	}
	return nil
}
