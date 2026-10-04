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
	"context"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CompiledExpression struct {
	permissive        []*compiledPolicyExpression
	restrictive       []*compiledPolicyExpression
	needsTags         bool
	staticOptimized   *planpb.Expr
	staticUnoptimized *planpb.Expr
}

type expressionKind int

const (
	usingExpression expressionKind = iota
	checkExpression
)

func (kind expressionKind) expression(policy *RowPolicy) string {
	if kind == checkExpression {
		return policy.GetCheckExpr()
	}
	return policy.GetUsingExpr()
}

type compiledPolicyExpression struct {
	expr                 *planpb.Expr
	needsPrincipal       bool
	tagVariables         map[string]string
	tagVariableDataTypes map[string][]schemapb.DataType
	tagVariableArrays    map[string]bool
}

func compiledExpressionNeedsTags(e *CompiledExpression) bool {
	if e == nil || len(e.permissive) == 0 {
		// Without a permissive policy, RLS denies unconditionally, so tags in
		// restrictive policies cannot affect the result.
		return false
	}
	for _, policies := range [...][]*compiledPolicyExpression{e.permissive, e.restrictive} {
		for _, policy := range policies {
			if len(policy.tagVariables) > 0 {
				return true
			}
		}
	}
	return false
}

func (e *CompiledExpression) NeedsTags() bool {
	return e != nil && e.needsTags
}

func (e *compiledPolicyExpression) isStatic() bool {
	return e != nil && !e.needsPrincipal && len(e.tagVariables) == 0
}

func (e *CompiledExpression) isStatic() bool {
	if e == nil {
		return false
	}
	for _, policies := range [...][]*compiledPolicyExpression{e.permissive, e.restrictive} {
		for _, policy := range policies {
			if !policy.isStatic() {
				return false
			}
		}
	}
	return true
}

type policyExprTemplate struct {
	policyType     PolicyType
	expr           string
	needsPrincipal bool
	tagVariables   map[string]string
}

func preparePolicyExprTemplates(policies []*RowPolicy, action PolicyAction, kind expressionKind) ([]policyExprTemplate, int) {
	templates := make([]policyExprTemplate, 0)
	var permissiveCount, restrictiveCount int
	var permissiveLength, restrictiveLength int

	for _, policy := range policies {
		if !policyMatchesAction(policy, action) {
			continue
		}
		policyExpr := strings.TrimSpace(kind.expression(policy))
		if policyExpr == "" {
			continue
		}
		var policyNeedsPrincipal bool
		var tagVariables map[string]string
		policyExpr, policyNeedsPrincipal, tagVariables = funcutil.ConvertRLSTemplateVariables(policyExpr)
		templates = append(templates, policyExprTemplate{
			policyType:     policy.GetPolicyType(),
			expr:           policyExpr,
			needsPrincipal: policyNeedsPrincipal,
			tagVariables:   tagVariables,
		})
		switch policy.GetPolicyType() {
		case PolicyTypePermissive:
			if permissiveCount > 0 {
				permissiveLength += len(" or ")
			}
			permissiveCount++
			permissiveLength += len(strings.TrimSpace(policyExpr)) + 2
		case PolicyTypeRestrictive:
			if restrictiveCount > 0 {
				restrictiveLength += len(" and ")
			}
			restrictiveCount++
			restrictiveLength += len(strings.TrimSpace(policyExpr)) + 2
		}
	}

	if permissiveCount == 0 {
		if restrictiveCount > 0 {
			return templates, len("false")
		}
		return templates, 0
	}

	combinedLength := permissiveLength + 2
	if restrictiveCount > 0 {
		combinedLength += len(" and ") + restrictiveLength + 2
	}
	return templates, combinedLength
}

// CombinedExpressionLength shares admission accounting with runtime compilation.
// check selects CHECK instead of USING; neither path materializes a combined string.
func CombinedExpressionLength(policies []*RowPolicy, action PolicyAction, check bool) int {
	kind := usingExpression
	if check {
		kind = checkExpression
	}
	_, length := preparePolicyExprTemplates(policies, action, kind)
	return length
}

func CompileUsingExpression(policies []*RowPolicy, action PolicyAction, schema *typeutil.SchemaHelper, maxLength int) (*CompiledExpression, error) {
	return compileExpression(policies, action, schema, maxLength, usingExpression)
}

func CompileCheckExpression(policies []*RowPolicy, action PolicyAction, schema *typeutil.SchemaHelper, maxLength int) (*CompiledExpression, error) {
	return compileExpression(policies, action, schema, maxLength, checkExpression)
}

func compileExpression(policies []*RowPolicy, action PolicyAction, schema *typeutil.SchemaHelper, maxLength int, kind expressionKind) (*CompiledExpression, error) {
	policies = append([]*RowPolicy(nil), policies...)
	sort.Slice(policies, func(i, j int) bool {
		return policies[i].GetPolicyName() < policies[j].GetPolicyName()
	})
	templates, combinedLength := preparePolicyExprTemplates(policies, action, kind)
	if combinedLength > maxLength {
		return nil, merr.WrapErrServiceQuotaExceededMsg("RLS combined expression exceeds max length %d", maxLength)
	}
	var timezone string
	if schema != nil {
		timezone = schema.GetTimezone()
	}
	return compileExprTemplates(schema, templates, timezone, kind)
}

func compileExprTemplates(schemaHelper *typeutil.SchemaHelper, templates []policyExprTemplate, timezone string, kind expressionKind) (*CompiledExpression, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	visitorArgs := &planparserv2.ParserVisitorArgs{Timezone: timezone}
	compiled := &CompiledExpression{}
	for _, template := range templates {
		policyExpr, err := compilePolicyExprTemplate(schemaHelper, template, visitorArgs, kind)
		if err != nil {
			return nil, err
		}
		if policyExpr == nil {
			continue
		}
		switch template.policyType {
		case PolicyTypePermissive:
			compiled.permissive = append(compiled.permissive, policyExpr)
		case PolicyTypeRestrictive:
			compiled.restrictive = append(compiled.restrictive, policyExpr)
		}
	}
	compiled.permissive = simplifyPolicyGroup(compiled.permissive, planpb.BinaryExpr_LogicalOr)
	compiled.restrictive = simplifyPolicyGroup(compiled.restrictive, planpb.BinaryExpr_LogicalAnd)
	if len(compiled.permissive) == 1 && compiled.permissive[0].isStatic() && rewriter.IsAlwaysFalseExpr(compiled.permissive[0].expr) {
		compiled.restrictive = nil
	}
	if len(compiled.restrictive) == 1 && compiled.restrictive[0].isStatic() && rewriter.IsAlwaysFalseExpr(compiled.restrictive[0].expr) {
		compiled.permissive = compiled.restrictive
		compiled.restrictive = nil
	}
	if len(compiled.permissive) == 0 && len(compiled.restrictive) == 0 {
		return nil, nil
	}
	compiled.needsTags = compiledExpressionNeedsTags(compiled)
	if compiled.isStatic() {
		var err error
		compiled.staticUnoptimized, err = compiled.instantiate("", nil, false)
		if err != nil {
			return nil, err
		}
		compiled.staticOptimized, err = compiled.instantiate("", nil, true)
		if err != nil {
			return nil, err
		}
	}
	return compiled, nil
}

func simplifyPolicyGroup(policies []*compiledPolicyExpression, op planpb.BinaryExpr_BinaryOp) []*compiledPolicyExpression {
	staticExprs := make([]*planpb.Expr, 0, len(policies))
	dynamic := make([]*compiledPolicyExpression, 0, len(policies))
	var identity *compiledPolicyExpression
	for _, policy := range policies {
		if !policy.isStatic() {
			dynamic = append(dynamic, policy)
			continue
		}
		switch op {
		case planpb.BinaryExpr_LogicalOr:
			if rewriter.IsAlwaysTrueExpr(policy.expr) {
				return []*compiledPolicyExpression{policy}
			}
			if rewriter.IsAlwaysFalseExpr(policy.expr) {
				identity = policy
				continue
			}
		case planpb.BinaryExpr_LogicalAnd:
			if rewriter.IsAlwaysFalseExpr(policy.expr) {
				return []*compiledPolicyExpression{policy}
			}
			if rewriter.IsAlwaysTrueExpr(policy.expr) {
				identity = policy
				continue
			}
		}
		staticExprs = append(staticExprs, policy.expr)
	}
	if len(staticExprs) == 0 {
		if len(dynamic) > 0 || identity == nil {
			return dynamic
		}
		return []*compiledPolicyExpression{identity}
	}

	static := &compiledPolicyExpression{expr: combinePredicates(staticExprs, op)}
	return append(dynamic, static)
}

func compilePolicyExprTemplate(schemaHelper *typeutil.SchemaHelper, template policyExprTemplate, visitorArgs *planparserv2.ParserVisitorArgs, kind expressionKind) (*compiledPolicyExpression, error) {
	expr := strings.TrimSpace(template.expr)
	if expr == "" {
		return nil, nil
	}
	if schemaHelper == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to compile RLS expression template with nil schema helper")
	}
	parsedExpr, err := planparserv2.ParseExprTemplate(schemaHelper, expr, visitorArgs)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "failed to parse persisted RLS policy expression template")
	}
	allowedTemplateVariables := make(map[string]struct{}, len(template.tagVariables)+1)
	if template.needsPrincipal {
		allowedTemplateVariables[funcutil.RLSPrincipalTemplateName] = struct{}{}
	}
	for _, variable := range template.tagVariables {
		allowedTemplateVariables[variable] = struct{}{}
	}
	if err := ValidateParsedExpression(parsedExpr, allowedTemplateVariables); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid persisted RLS policy expression")
	}
	if kind == usingExpression {
		if err := ValidateUsingExpressionSchema(schemaHelper, parsedExpr); err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "invalid persisted RLS using expression")
		}
	}
	var tagVariableDataTypes map[string][]schemapb.DataType
	var tagVariableArrays map[string]bool
	if len(template.tagVariables) > 0 {
		tagVariableDataTypes = make(map[string][]schemapb.DataType, len(template.tagVariables))
		tagVariableArrays = make(map[string]bool, len(template.tagVariables))
		for _, variable := range template.tagVariables {
			dataTypes := make([]schemapb.DataType, 0, 1)
			expectsArray := false
			collectRLSTemplateDataTypes(parsedExpr, variable, &dataTypes, &expectsArray)
			tagVariableDataTypes[variable] = dataTypes
			tagVariableArrays[variable] = expectsArray
		}
	}
	return &compiledPolicyExpression{
		expr:                 parsedExpr,
		needsPrincipal:       template.needsPrincipal,
		tagVariables:         template.tagVariables,
		tagVariableDataTypes: tagVariableDataTypes,
		tagVariableArrays:    tagVariableArrays,
	}, nil
}

func (e *CompiledExpression) Instantiate(principalName string, principalTags map[string]TagValue) (*planpb.Expr, error) {
	if e == nil {
		return nil, nil
	}
	optimizeEnabled := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
	if e.staticUnoptimized != nil {
		if optimizeEnabled {
			return e.staticOptimized, nil
		}
		return e.staticUnoptimized, nil
	}
	return e.instantiate(principalName, principalTags, optimizeEnabled)
}

// BuildCheckPredicate compiles and instantiates a write CHECK predicate from
// one authoritative metadata snapshot. It loads principal tags only when the
// compiled predicate references them.
func BuildCheckPredicate(
	policies []*RowPolicy,
	principalName string,
	loadPrincipalTags func() (map[string]TagValue, error),
	action PolicyAction,
	schema *typeutil.SchemaHelper,
	maxLength int,
) (*planpb.Expr, error) {
	compiled, err := CompileCheckExpression(policies, action, schema, maxLength)
	if err != nil {
		return nil, err
	}
	if compiled == nil {
		return nil, denyNoApplicableCheckPolicy(action)
	}
	var principalTags map[string]TagValue
	if compiled.NeedsTags() {
		if loadPrincipalTags == nil {
			return nil, merr.WrapErrServiceInternalMsg("RLS check predicate requires principal tags without a tag loader")
		}
		principalTags, err = loadPrincipalTags()
		if err != nil {
			return nil, err
		}
	}
	expr, err := compiled.Instantiate(principalName, principalTags)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, denyNoApplicableCheckPolicy(action)
	}
	if err := ValidateStaticCheckPredicate(expr, PolicyActionOperation(action)); err != nil {
		return nil, err
	}
	if rewriter.IsAlwaysTrueExpr(expr) {
		return nil, nil
	}
	return expr, nil
}

func denyNoApplicableCheckPolicy(action PolicyAction) error {
	return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: no applicable check policies", PolicyActionOperation(action))
}

func (e *CompiledExpression) instantiate(principalName string, principalTags map[string]TagValue, optimizeEnabled bool) (*planpb.Expr, error) {
	if len(e.permissive) == 0 {
		if len(e.restrictive) > 0 {
			return alwaysFalsePredicate(), nil
		}
		return nil, nil
	}

	remainingTemplateBytes := maxRLSPrincipalMetadataBytes
	restrictiveExprs, restrictiveFalse, err := instantiatePolicyExprs(e.restrictive, principalName, principalTags, planpb.BinaryExpr_LogicalAnd, &remainingTemplateBytes)
	if err != nil {
		return nil, err
	}
	if restrictiveFalse {
		return alwaysFalsePredicate(), nil
	}

	permissiveExprs, permissiveFalse, err := instantiatePolicyExprs(e.permissive, principalName, principalTags, planpb.BinaryExpr_LogicalOr, &remainingTemplateBytes)
	if err != nil {
		return nil, err
	}
	if permissiveFalse {
		return alwaysFalsePredicate(), nil
	}

	finalExpr := combinePredicates(permissiveExprs, planpb.BinaryExpr_LogicalOr)
	if len(restrictiveExprs) > 0 {
		finalExpr = combinePredicate(finalExpr, combinePredicates(restrictiveExprs, planpb.BinaryExpr_LogicalAnd), planpb.BinaryExpr_LogicalAnd)
	}
	return rewriter.RewriteExprWithConfig(finalExpr, optimizeEnabled), nil
}

func instantiatePolicyExprs(
	policies []*compiledPolicyExpression,
	principalName string,
	principalTags map[string]TagValue,
	op planpb.BinaryExpr_BinaryOp,
	remainingTemplateBytes *int64,
) ([]*planpb.Expr, bool, error) {
	exprs := make([]*planpb.Expr, 0, len(policies))
	for _, policy := range policies {
		expr, err := policy.instantiate(principalName, principalTags, remainingTemplateBytes)
		if err != nil {
			return nil, false, err
		}
		if expr == nil {
			if op == planpb.BinaryExpr_LogicalAnd {
				return nil, true, nil
			}
			continue
		}
		exprs = append(exprs, expr)
	}
	return exprs, len(policies) > 0 && len(exprs) == 0, nil
}

func (e *compiledPolicyExpression) instantiate(principalName string, principalTags map[string]TagValue, remainingTemplateBytes *int64) (*planpb.Expr, error) {
	if e == nil || e.expr == nil {
		return nil, nil
	}
	if e.isStatic() {
		return proto.Clone(e.expr).(*planpb.Expr), nil
	}
	if e.needsPrincipal && principalName == "" {
		return nil, nil
	}

	// A policy is a single predicate, so validation guarantees at most one tag
	// variable. Validate and charge it before allocating or cloning an expression.
	var normalizedVariable string
	var normalizedTagValue *planpb.GenericValue
	var sourceTagValue TagValue
	materializedBytes := int64(0)
	if e.needsPrincipal {
		materializedBytes = int64(len(principalName))
	}
	for tagKey, variable := range e.tagVariables {
		tagValue, ok := principalTags[tagKey]
		if !ok {
			return nil, nil
		}
		valueBytes, ok := normalizedRLSTagValueSize(e.tagVariableDataTypes[variable], e.tagVariableArrays[variable], tagValue)
		if !ok {
			return nil, nil
		}
		materializedBytes += valueBytes
		normalizedVariable = variable
		sourceTagValue = tagValue
	}
	if remainingTemplateBytes != nil {
		if materializedBytes > *remainingTemplateBytes {
			return nil, merr.WrapErrServiceQuotaExceededMsg(
				"RLS materialized template values exceed max size %d",
				maxRLSPrincipalMetadataBytes,
			)
		}
		*remainingTemplateBytes -= materializedBytes
	}
	if normalizedVariable != "" {
		var ok bool
		normalizedTagValue, ok = rlsTagValueToGenericValue(
			e.tagVariableDataTypes[normalizedVariable],
			e.tagVariableArrays[normalizedVariable],
			sourceTagValue,
		)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("RLS principal tag changed while instantiating policy")
		}
	}

	values := make(map[string]*planpb.GenericValue, len(e.tagVariables)+1)
	if e.needsPrincipal {
		values[funcutil.RLSPrincipalTemplateName] = planparserv2.NewString(principalName)
	}
	if normalizedVariable != "" {
		values[normalizedVariable] = normalizedTagValue
	}

	expr := proto.Clone(e.expr).(*planpb.Expr)
	if err := planparserv2.FillExpressionValue(expr, values); err != nil {
		return nil, err
	}
	return expr, nil
}

// rlsTagValueToGenericValue normalizes directly into the final template value.
// The immutable tag snapshot is never copied into an intermediate TagValue slice.
func rlsTagValueToGenericValue(dataTypes []schemapb.DataType, expectsArray bool, value TagValue) (*planpb.GenericValue, bool) {
	if len(dataTypes) == 0 {
		return nil, false
	}
	if expectsArray {
		if value.Kind != TagValueKindArray || value.arrayValue == nil {
			return nil, false
		}
		elements := make([]*planpb.GenericValue, value.arrayValue.len())
		for i := range elements {
			element := value.arrayValue.at(i)
			var ok bool
			elements[i], ok = rlsTagValueToGenericValue(dataTypes, false, element)
			if !ok {
				return nil, false
			}
		}
		return &planpb.GenericValue{
			Val: &planpb.GenericValue_ArrayVal{
				ArrayVal: &planpb.Array{Array: elements, SameType: true},
			},
		}, true
	}
	normalized, ok := normalizeRLSScalarTagValue(dataTypes, value)
	if !ok {
		return nil, false
	}
	switch normalized.Kind {
	case TagValueKindString:
		return planparserv2.NewString(normalized.StringValue), true
	case TagValueKindInt64:
		return planparserv2.NewInt(normalized.Int64Value), true
	case TagValueKindDouble:
		return planparserv2.NewFloat(normalized.DoubleValue), true
	default:
		return nil, false
	}
}

func rlsTemplateColumnDataType(columnInfo *planpb.ColumnInfo) schemapb.DataType {
	if columnInfo == nil {
		return schemapb.DataType_None
	}
	dataType := columnInfo.GetDataType()
	if typeutil.IsArrayType(dataType) &&
		(len(columnInfo.GetNestedPath()) != 0 || columnInfo.GetIsElementLevel()) {
		return columnInfo.GetElementType()
	}
	return dataType
}

func normalizedRLSTagValueSize(dataTypes []schemapb.DataType, expectsArray bool, value TagValue) (int64, bool) {
	if !expectsArray {
		normalized, ok := normalizeRLSScalarTagValue(dataTypes, value)
		if !ok {
			return 0, false
		}
		size := rlsTemplateValueSize
		switch normalized.Kind {
		case TagValueKindString:
			if int64(len(normalized.StringValue)) > math.MaxInt64-size {
				return 0, false
			}
			size += int64(len(normalized.StringValue))
		case TagValueKindInt64, TagValueKindDouble:
		default:
			return 0, false
		}
		return size, true
	}
	if value.Kind != TagValueKindArray || value.arrayValue == nil {
		return 0, false
	}
	size := rlsTemplateValueSize
	for i := 0; i < value.arrayValue.len(); i++ {
		elementSize, ok := normalizedRLSTagValueSize(dataTypes, false, value.arrayValue.at(i))
		if !ok || elementSize > math.MaxInt64-size {
			return 0, false
		}
		size += elementSize
	}
	return size, true
}

// Numeric conversions must preserve the value exactly; incompatible or lossy
// bindings make the entire policy non-matching, even under NOT.
func normalizeRLSScalarTagValue(dataTypes []schemapb.DataType, value TagValue) (TagValue, bool) {
	if value.Kind == TagValueKindArray {
		return TagValue{}, false
	}
	for _, dataType := range dataTypes {
		switch {
		case typeutil.IsStringType(dataType):
			if value.Kind != TagValueKindString {
				return TagValue{}, false
			}
		case typeutil.IsIntegerType(dataType):
			switch value.Kind {
			case TagValueKindInt64:
			case TagValueKindDouble:
				if !isExactInt64(value.DoubleValue) {
					return TagValue{}, false
				}
				value = NewInt64TagValue(int64(value.DoubleValue))
			default:
				return TagValue{}, false
			}
			if !integerTagFitsDataType(value.Int64Value, dataType) {
				return TagValue{}, false
			}
		case typeutil.IsFloatingType(dataType):
			switch value.Kind {
			case TagValueKindInt64:
				if dataType == schemapb.DataType_Float {
					if int64(float64(float32(value.Int64Value))) != value.Int64Value {
						return TagValue{}, false
					}
				} else if int64(float64(value.Int64Value)) != value.Int64Value {
					return TagValue{}, false
				}
				value = NewDoubleTagValue(float64(value.Int64Value))
			case TagValueKindDouble:
				if dataType == schemapb.DataType_Float && float64(float32(value.DoubleValue)) != value.DoubleValue {
					return TagValue{}, false
				}
			default:
				return TagValue{}, false
			}
		default:
			return TagValue{}, false
		}
	}
	return value, true
}

func integerTagFitsDataType(value int64, dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Int8:
		return value >= -1<<7 && value <= 1<<7-1
	case schemapb.DataType_Int16:
		return value >= -1<<15 && value <= 1<<15-1
	case schemapb.DataType_Int32:
		return value >= -1<<31 && value <= 1<<31-1
	case schemapb.DataType_Int64:
		return true
	default:
		return false
	}
}

func collectRLSTemplateDataTypes(expr *planpb.Expr, variable string, dataTypes *[]schemapb.DataType, expectsArray *bool) {
	if expr == nil {
		return
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_UnaryExpr:
		collectRLSTemplateDataTypes(node.UnaryExpr.GetChild(), variable, dataTypes, expectsArray)
	case *planpb.Expr_BinaryExpr:
		collectRLSTemplateDataTypes(node.BinaryExpr.GetLeft(), variable, dataTypes, expectsArray)
		collectRLSTemplateDataTypes(node.BinaryExpr.GetRight(), variable, dataTypes, expectsArray)
	case *planpb.Expr_UnaryRangeExpr:
		if node.UnaryRangeExpr.GetTemplateVariableName() == variable {
			*dataTypes = append(*dataTypes, rlsTemplateColumnDataType(node.UnaryRangeExpr.GetColumnInfo()))
		}
	case *planpb.Expr_JsonContainsExpr:
		if node.JsonContainsExpr.GetTemplateVariableName() == variable {
			*dataTypes = append(*dataTypes, node.JsonContainsExpr.GetColumnInfo().GetElementType())
			*expectsArray = node.JsonContainsExpr.GetOp() != planpb.JSONContainsExpr_Contains
		}
	}
}

func isExactInt64(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) &&
		value == math.Trunc(value) &&
		value >= -9223372036854775808.0 && value < 9223372036854775808.0
}

func combinePredicates(exprs []*planpb.Expr, op planpb.BinaryExpr_BinaryOp) *planpb.Expr {
	if len(exprs) == 0 {
		return nil
	}
	if len(exprs) == 1 {
		return exprs[0]
	}
	mid := len(exprs) / 2
	return combinePredicate(
		combinePredicates(exprs[:mid], op),
		combinePredicates(exprs[mid:], op),
		op,
	)
}

func combinePredicate(left *planpb.Expr, right *planpb.Expr, op planpb.BinaryExpr_BinaryOp) *planpb.Expr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	switch op {
	case planpb.BinaryExpr_LogicalAnd:
		if rewriter.IsAlwaysTrueExpr(left) {
			return right
		}
		if rewriter.IsAlwaysTrueExpr(right) {
			return left
		}
	case planpb.BinaryExpr_LogicalOr:
		if rewriter.IsAlwaysTrueExpr(left) || rewriter.IsAlwaysTrueExpr(right) {
			return alwaysTruePredicate()
		}
	}
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    op,
				Left:  left,
				Right: right,
			},
		},
	}
}

func policyMatchesAction(policy *RowPolicy, action PolicyAction) bool {
	return policy != nil && slices.Contains(policy.GetActions(), action)
}

func ResolveRuntimePrincipal(rlsEnabled bool, principalName string, operation string) (string, bool, error) {
	if !rlsEnabled {
		return "", false, nil
	}
	if strings.TrimSpace(principalName) == "" {
		return "", false, merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: rls_principal is required", operation)
	}
	// The refreshable principal-name limit is a creation quota. Runtime
	// requests must keep existing principals addressable after that quota is
	// lowered, while still enforcing the fixed transport safety bound.
	if err := ValidatePrincipalName(principalName); err != nil {
		return "", false, err
	}
	return principalName, true, nil
}

// ReferencedFieldIDs returns the field IDs read by an instantiated RLS
// predicate. The result is sorted to keep downstream query projections stable.
func ReferencedFieldIDs(expr *planpb.Expr) []int64 {
	fieldIDs := make(map[int64]struct{})
	collectReferencedFieldIDs(expr, fieldIDs)
	result := make([]int64, 0, len(fieldIDs))
	for fieldID := range fieldIDs {
		result = append(result, fieldID)
	}
	slices.Sort(result)
	return result
}

func collectReferencedFieldIDs(expr *planpb.Expr, fieldIDs map[int64]struct{}) {
	if expr == nil {
		return
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_UnaryExpr:
		collectReferencedFieldIDs(node.UnaryExpr.GetChild(), fieldIDs)
	case *planpb.Expr_BinaryExpr:
		collectReferencedFieldIDs(node.BinaryExpr.GetLeft(), fieldIDs)
		collectReferencedFieldIDs(node.BinaryExpr.GetRight(), fieldIDs)
	case *planpb.Expr_UnaryRangeExpr:
		addReferencedFieldID(node.UnaryRangeExpr.GetColumnInfo(), fieldIDs)
	case *planpb.Expr_TermExpr:
		addReferencedFieldID(node.TermExpr.GetColumnInfo(), fieldIDs)
	case *planpb.Expr_JsonContainsExpr:
		addReferencedFieldID(node.JsonContainsExpr.GetColumnInfo(), fieldIDs)
	case *planpb.Expr_BinaryRangeExpr:
		addReferencedFieldID(node.BinaryRangeExpr.GetColumnInfo(), fieldIDs)
	}
}

func addReferencedFieldID(column *planpb.ColumnInfo, fieldIDs map[int64]struct{}) {
	if column != nil {
		fieldIDs[column.GetFieldId()] = struct{}{}
	}
}

func alwaysTruePredicate() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_AlwaysTrueExpr{
			AlwaysTrueExpr: &planpb.AlwaysTrueExpr{},
		},
	}
}

func alwaysFalsePredicate() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op: planpb.UnaryExpr_Not,
				Child: &planpb.Expr{
					Expr: &planpb.Expr_AlwaysTrueExpr{
						AlwaysTrueExpr: &planpb.AlwaysTrueExpr{},
					},
				},
			},
		},
	}
}

func ValidateStaticCheckPredicate(checkExpr *planpb.Expr, operation string) error {
	if checkExpr != nil && rewriter.IsAlwaysFalseExpr(checkExpr) {
		return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS check expression", operation)
	}
	return nil
}

func ValidateRowsByPredicate(ctx context.Context, fieldsData []*schemapb.FieldData, rowNum int, parsedExpr *planpb.Expr, operation string, exprKind string) error {
	if parsedExpr == nil {
		return nil
	}
	if rowNum < 0 {
		return merr.WrapErrServiceInternalMsg("RLS row count must not be negative: %d", rowNum)
	}
	if len(fieldsData) == 0 {
		if rowNum == 0 {
			return nil
		}
		return merr.WrapErrServiceInternalMsg("RLS row count is %d but field data is empty", rowNum)
	}
	if rowNum == 0 {
		return merr.WrapErrServiceInternalMsg("RLS row count is zero but field data is not empty")
	}

	referencedFieldIDs := ReferencedFieldIDs(parsedExpr)
	rowData := newRowData(fieldsData, referencedFieldIDs)
	return validateRowDataByPredicate(ctx, rowData, referencedFieldIDs, rowNum, parsedExpr, operation, exprKind)
}

// ValidateInsertDataByPredicate evaluates an RLS predicate directly against
// import storage columns, avoiding a full protobuf copy of narrow integers.
func ValidateInsertDataByPredicate[T StorageFieldData](ctx context.Context, fieldsData map[int64]T, rowNum int, parsedExpr *planpb.Expr, operation string, exprKind string) error {
	if parsedExpr == nil {
		return nil
	}
	if rowNum < 0 {
		return merr.WrapErrServiceInternalMsg("RLS row count must not be negative: %d", rowNum)
	}
	if len(fieldsData) == 0 {
		if rowNum == 0 {
			return nil
		}
		return merr.WrapErrServiceInternalMsg("RLS row count is %d but field data is empty", rowNum)
	}
	if rowNum == 0 {
		return merr.WrapErrServiceInternalMsg("RLS row count is zero but field data is not empty")
	}

	referencedFieldIDs := ReferencedFieldIDs(parsedExpr)
	rowData := newInsertRowData(fieldsData, referencedFieldIDs)
	return validateRowDataByPredicate(ctx, rowData, referencedFieldIDs, rowNum, parsedExpr, operation, exprKind)
}

func validateRowDataByPredicate(ctx context.Context, rowData *rowData, referencedFieldIDs []int64, rowNum int, parsedExpr *planpb.Expr, operation string, exprKind string) error {
	if err := rowData.validateRowCount(referencedFieldIDs, rowNum); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rowData.preparePredicate(parsedExpr); err != nil {
		return merr.Wrapf(err, "invalid RLS %s expression for %s", exprKind, operation)
	}

	for rowIdx := 0; rowIdx < rowNum; rowIdx++ {
		clear(rowData.arrayElementLayouts)
		if rowIdx&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		result, err := evalExpr(parsedExpr, rowData, rowIdx)
		if err != nil {
			return merr.Wrapf(err, "failed to evaluate RLS %s expression for %s at row %d", exprKind, operation, rowIdx)
		}
		if result != truthTrue {
			return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS %s expression at row %d", operation, exprKind, rowIdx)
		}
	}
	mlog.Debug(ctx, "RLS row expression validation passed",
		mlog.String("operation", operation), mlog.String("exprKind", exprKind), mlog.Int("rowNum", rowNum))
	return nil
}

type truthValue uint8

const (
	truthUnknown truthValue = iota
	truthFalse
	truthTrue
)

func truthValueFromBool(value bool) truthValue {
	if value {
		return truthTrue
	}
	return truthFalse
}

func (value truthValue) not() truthValue {
	switch value {
	case truthTrue:
		return truthFalse
	case truthFalse:
		return truthTrue
	default:
		return truthUnknown
	}
}

func (value truthValue) and(other truthValue) truthValue {
	if value == truthFalse || other == truthFalse {
		return truthFalse
	}
	if value == truthTrue && other == truthTrue {
		return truthTrue
	}
	return truthUnknown
}

func (value truthValue) or(other truthValue) truthValue {
	if value == truthTrue || other == truthTrue {
		return truthTrue
	}
	if value == truthFalse && other == truthFalse {
		return truthFalse
	}
	return truthUnknown
}

type fieldReader struct {
	valueAt      func(int) any
	typeValid    bool
	fieldName    string
	dataType     schemapb.DataType
	elementType  schemapb.DataType
	validData    []bool
	dataLen      int
	rowCount     int
	shapeValid   bool
	nextLogical  int
	nextPhysical int
	lastLogical  int
	lastPhysical int
}

// StorageFieldData is the zero-copy subset of storage.FieldData needed by the
// import RLS evaluator.
type StorageFieldData interface {
	GetDataRows() any
	GetDataType() schemapb.DataType
	GetValidData() []bool
}

type storageArrayFieldData interface {
	GetElementType() schemapb.DataType
}

type rowData struct {
	fields              map[int64]*fieldReader
	termMatchers        map[*planpb.TermExpr]*literalMatcher
	arrayMatchers       map[*planpb.JSONContainsExpr]*arrayLiteralMatcher
	arrayElementLayouts map[*schemapb.ScalarField]arrayElementLayout
}

type arrayElementLayout struct {
	validData []bool
	err       error
}

type literalMatcher struct {
	dataType schemapb.DataType
	values   map[any]int
}

// arrayLiteralMatcher reuses target ordinals across rows. seen records the
// evaluation generation for each ordinal, avoiding a per-row allocation.
type arrayLiteralMatcher struct {
	*literalMatcher
	op         planpb.JSONContainsExpr_JSONOp
	seen       []uint32
	generation uint32
}

func newRowData(fieldsData []*schemapb.FieldData, referencedFieldIDs []int64) *rowData {
	referencedFields := make(map[int64]struct{}, len(referencedFieldIDs))
	for _, fieldID := range referencedFieldIDs {
		referencedFields[fieldID] = struct{}{}
	}
	data := &rowData{fields: make(map[int64]*fieldReader, len(referencedFields))}
	for _, fieldData := range fieldsData {
		if fieldData == nil {
			continue
		}
		if _, ok := referencedFields[fieldData.GetFieldId()]; !ok {
			continue
		}
		reader := newFieldReader(fieldData.GetFieldName(), fieldData.GetType(),
			typeutil.GetFieldDataValidData(fieldData), rlsFieldDataValues(fieldData))
		reader.elementType = fieldData.GetScalars().GetArrayData().GetElementType()
		data.fields[fieldData.GetFieldId()] = reader
	}
	return data
}

func newInsertRowData[T StorageFieldData](fieldsData map[int64]T, referencedFieldIDs []int64) *rowData {
	referencedFields := make(map[int64]struct{}, len(referencedFieldIDs))
	for _, fieldID := range referencedFieldIDs {
		referencedFields[fieldID] = struct{}{}
	}
	data := &rowData{fields: make(map[int64]*fieldReader, len(referencedFields))}
	for fieldID, fieldData := range fieldsData {
		if any(fieldData) == nil {
			continue
		}
		if _, ok := referencedFields[fieldID]; !ok {
			continue
		}
		reader := newFieldReader(strconv.FormatInt(fieldID, 10), fieldData.GetDataType(),
			fieldData.GetValidData(), fieldData.GetDataRows())
		if reader.dataType == schemapb.DataType_Array {
			if arrayData, ok := any(fieldData).(storageArrayFieldData); ok {
				reader.elementType = arrayData.GetElementType()
			}
		}
		data.fields[fieldID] = reader
	}
	return data
}

func (d *rowData) validateRowCount(referencedFieldIDs []int64, expected int) error {
	for _, fieldID := range referencedFieldIDs {
		reader, ok := d.fields[fieldID]
		if !ok {
			return merr.WrapErrServiceInternalMsg("RLS expression references field id %d which is not present in row data", fieldID)
		}
		if !reader.typeValid {
			return merr.WrapErrServiceInternalMsg("RLS field %s has invalid storage data for type %s", reader.fieldName, reader.dataType.String())
		}

		var actual int
		switch {
		case isRLSScalarType(reader.dataType), reader.dataType == schemapb.DataType_Array:
			if !reader.shapeValid {
				return merr.WrapErrServiceInternalMsg("RLS field %s has inconsistent data and validity lengths", reader.fieldName)
			}
			actual = reader.rowCount
		default:
			return merr.WrapErrServiceInternalMsg("RLS expression references unsupported field %s with type %s", reader.fieldName, reader.dataType.String())
		}
		if actual != expected {
			return merr.WrapErrServiceInternalMsg("RLS field %s row count %d does not match expected row count %d", reader.fieldName, actual, expected)
		}
	}
	return nil
}

func newLiteralMatcher(dataType schemapb.DataType, values []*planpb.GenericValue) (*literalMatcher, error) {
	matcher := &literalMatcher{
		dataType: dataType,
		values:   make(map[any]int, len(values)),
	}
	for i, value := range values {
		key, ok := genericLiteralKey(dataType, value)
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("RLS expression literal %d does not match field type %s", i, dataType.String())
		}
		if _, exists := matcher.values[key]; !exists {
			matcher.values[key] = len(matcher.values)
		}
	}
	return matcher, nil
}

func (d *rowData) termMatcher(expr *planpb.TermExpr) (*literalMatcher, error) {
	if matcher, prepared := d.termMatchers[expr]; prepared {
		return matcher, nil
	}
	if d.termMatchers == nil {
		d.termMatchers = make(map[*planpb.TermExpr]*literalMatcher)
	}
	for i, value := range expr.GetValues() {
		if !canonicalScalarLiteral(expr.GetColumnInfo().GetDataType(), value) {
			return nil, merr.WrapErrDataIntegrityMsg("RLS term literal %d does not match field type %s", i, expr.GetColumnInfo().GetDataType().String())
		}
	}
	matcher, err := newLiteralMatcher(expr.GetColumnInfo().GetDataType(), expr.GetValues())
	if err != nil {
		return nil, err
	}
	d.termMatchers[expr] = matcher
	return matcher, nil
}

func (d *rowData) arrayMatcher(expr *planpb.JSONContainsExpr) (*arrayLiteralMatcher, error) {
	if matcher, prepared := d.arrayMatchers[expr]; prepared {
		return matcher, nil
	}
	if d.arrayMatchers == nil {
		d.arrayMatchers = make(map[*planpb.JSONContainsExpr]*arrayLiteralMatcher)
	}
	literals, err := newLiteralMatcher(expr.GetColumnInfo().GetElementType(), expr.GetElements())
	if err != nil {
		return nil, err
	}
	matcher := &arrayLiteralMatcher{
		literalMatcher: literals,
		op:             expr.GetOp(),
		seen:           make([]uint32, len(literals.values)),
	}
	d.arrayMatchers[expr] = matcher
	return matcher, nil
}

func genericLiteralKey(dataType schemapb.DataType, value *planpb.GenericValue) (any, bool) {
	if value == nil {
		return nil, false
	}
	switch {
	case typeutil.IsBoolType(dataType):
		typed, ok := value.GetVal().(*planpb.GenericValue_BoolVal)
		return value.GetBoolVal(), ok && typed != nil
	case typeutil.IsIntegerType(dataType):
		typed, ok := value.GetVal().(*planpb.GenericValue_Int64Val)
		return value.GetInt64Val(), ok && typed != nil
	case typeutil.IsTimestamptzType(dataType):
		typed, ok := value.GetVal().(*planpb.GenericValue_Int64Val)
		return value.GetInt64Val(), ok && typed != nil
	case dataType == schemapb.DataType_Float:
		number, ok := genericNumericValue(value)
		return float32(number), ok
	case dataType == schemapb.DataType_Double:
		return genericNumericValue(value)
	case typeutil.IsStringType(dataType):
		typed, ok := value.GetVal().(*planpb.GenericValue_StringVal)
		return value.GetStringVal(), ok && typed != nil
	default:
		return nil, false
	}
}

func scalarLiteralKey(dataType schemapb.DataType, value any) (any, bool) {
	switch {
	case typeutil.IsBoolType(dataType):
		typed, ok := value.(bool)
		return typed, ok
	case typeutil.IsIntegerType(dataType), typeutil.IsTimestamptzType(dataType):
		return integerValue(value)
	case dataType == schemapb.DataType_Float:
		typed, ok := value.(float32)
		return typed, ok
	case dataType == schemapb.DataType_Double:
		typed, ok := value.(float64)
		return typed, ok
	case typeutil.IsStringType(dataType):
		typed, ok := value.(string)
		return typed, ok
	default:
		return nil, false
	}
}

func (m *literalMatcher) index(value any) (int, bool) {
	key, ok := scalarLiteralKey(m.dataType, value)
	if !ok {
		return 0, false
	}
	index, ok := m.values[key]
	return index, ok
}

func (m *arrayLiteralMatcher) matches(arrayValue *schemapb.ScalarField, rowData *rowData) (bool, error) {
	if len(m.values) == 0 {
		_, err := visitScalarArrayElements(arrayValue, m.dataType, rowData, nil)
		return err == nil && m.op != planpb.JSONContainsExpr_ContainsAny, err
	}
	if m.op == planpb.JSONContainsExpr_ContainsAny {
		return visitScalarArrayElements(arrayValue, m.dataType, rowData, func(value any) bool {
			_, ok := m.index(value)
			return ok
		})
	}

	m.generation++
	if m.generation == 0 {
		clear(m.seen)
		m.generation = 1
	}
	matched := 0
	return visitScalarArrayElements(arrayValue, m.dataType, rowData, func(value any) bool {
		index, ok := m.index(value)
		if !ok || m.seen[index] == m.generation {
			return false
		}
		m.seen[index] = m.generation
		matched++
		return matched == len(m.values)
	})
}

// dataIndex maps a logical row to compact nullable storage with constant
// memory. Production evaluation is monotonic; the reset keeps direct test and
// diagnostic callers correct when they read rows out of order.
func (r *fieldReader) dataIndex(rowIdx int) int {
	if len(r.validData) == 0 {
		return rowIdx
	}
	if r.dataLen == len(r.validData) {
		if r.validData[rowIdx] {
			return rowIdx
		}
		return -1
	}
	if rowIdx == r.lastLogical {
		return r.lastPhysical
	}
	if rowIdx < r.nextLogical {
		r.nextLogical = 0
		r.nextPhysical = 0
	}

	physical := -1
	for r.nextLogical <= rowIdx {
		if r.validData[r.nextLogical] {
			if r.nextLogical == rowIdx {
				physical = r.nextPhysical
			}
			r.nextPhysical++
		}
		r.nextLogical++
	}
	r.lastLogical = rowIdx
	r.lastPhysical = physical
	return physical
}

func (d *rowData) validateColumn(column *planpb.ColumnInfo) error {
	if column == nil {
		return merr.WrapErrServiceInternalMsg("RLS expression has empty column info")
	}
	if len(column.GetNestedPath()) > 0 || column.GetIsElementLevel() {
		return merr.WrapErrServiceInternalMsg("RLS expression does not support nested or element-level fields")
	}
	reader, ok := d.fields[column.GetFieldId()]
	if !ok {
		return merr.WrapErrServiceInternalMsg("RLS expression references field id %d which is not present in row data", column.GetFieldId())
	}
	if reader.dataType != column.GetDataType() {
		return merr.WrapErrServiceInternalMsg(
			"RLS field %s has type %s but expression expects %s",
			reader.fieldName,
			reader.dataType.String(),
			column.GetDataType().String(),
		)
	}
	if reader.dataType == schemapb.DataType_Array && reader.elementType != column.GetElementType() {
		return merr.WrapErrServiceInternalMsg(
			"RLS array field %s has element type %s but expression expects %s",
			reader.fieldName, reader.elementType.String(), column.GetElementType().String())
	}
	return nil
}

// Batch preparation validates column/storage types and binds valueAt once.
// Reads only map the current row through its nullable layout and index it.
func (d *rowData) value(column *planpb.ColumnInfo, rowIdx int) (any, error) {
	reader, ok := d.fields[column.GetFieldId()]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("RLS expression references field id %d which is not present in row data", column.GetFieldId())
	}
	if rowIdx < 0 || rowIdx >= reader.rowCount {
		return nil, merr.WrapErrServiceInternalMsg("RLS row index %d exceeds field %s row count %d", rowIdx, reader.fieldName, reader.rowCount)
	}
	dataIdx := reader.dataIndex(rowIdx)
	if dataIdx < 0 {
		return nil, nil
	}
	return reader.valueAt(dataIdx), nil
}

func isRLSScalarType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_Timestamptz,
		schemapb.DataType_VarChar,
		schemapb.DataType_Text:
		return true
	default:
		return false
	}
}

func rlsFieldDataValues(field *schemapb.FieldData) any {
	switch field.GetType() {
	case schemapb.DataType_Bool:
		return field.GetScalars().GetBoolData().GetData()
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return field.GetScalars().GetIntData().GetData()
	case schemapb.DataType_Int64:
		return field.GetScalars().GetLongData().GetData()
	case schemapb.DataType_Float:
		return field.GetScalars().GetFloatData().GetData()
	case schemapb.DataType_Double:
		return field.GetScalars().GetDoubleData().GetData()
	case schemapb.DataType_Timestamptz:
		return field.GetScalars().GetTimestamptzData().GetData()
	case schemapb.DataType_VarChar, schemapb.DataType_Text:
		return field.GetScalars().GetStringData().GetData()
	case schemapb.DataType_Array:
		return field.GetScalars().GetArrayData().GetData()
	default:
		return nil
	}
}

func newFieldReader(name string, dataType schemapb.DataType, validData []bool, data any) *fieldReader {
	r := &fieldReader{
		fieldName: name, dataType: dataType, validData: validData,
		lastLogical: -1, lastPhysical: -1,
	}
	switch values := data.(type) {
	case []bool:
		bindScalarValues(r, values)
	case []int8:
		bindScalarValues(r, values)
	case []int16:
		bindScalarValues(r, values)
	case []int32:
		bindScalarValues(r, values)
	case []int64:
		bindScalarValues(r, values)
	case []float32:
		bindScalarValues(r, values)
	case []float64:
		bindScalarValues(r, values)
	case []string:
		bindScalarValues(r, values)
	case []*schemapb.ScalarField:
		r.typeValid = dataType == schemapb.DataType_Array
		r.dataLen = len(values)
		r.valueAt = func(idx int) any {
			if values[idx] == nil {
				return nil
			}
			return values[idx]
		}
	}
	r.rowCount = r.dataLen
	r.shapeValid = true
	if len(validData) > 0 {
		r.rowCount = len(validData)
		r.shapeValid = r.dataLen == len(validData) || r.dataLen == int(funcutil.CountValidRows(validData))
	}
	return r
}

func bindScalarValues[T any](r *fieldReader, values []T) {
	var zero T
	_, r.typeValid = scalarLiteralKey(r.dataType, zero)
	r.dataLen = len(values)
	r.valueAt = func(idx int) any { return values[idx] }
}

func evalExpr(expr *planpb.Expr, rowData *rowData, rowIdx int) (truthValue, error) {
	if expr == nil {
		return truthUnknown, merr.WrapErrServiceInternalMsg("RLS expression is empty")
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return truthTrue, nil
	case *planpb.Expr_ValueExpr:
		value := node.ValueExpr.GetValue()
		if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok {
			return truthUnknown, merr.WrapErrServiceInternalMsg("RLS value expression is not boolean")
		}
		return truthValueFromBool(value.GetBoolVal()), nil
	case *planpb.Expr_UnaryExpr:
		if node.UnaryExpr.GetOp() != planpb.UnaryExpr_Not {
			return truthUnknown, merr.WrapErrServiceInternalMsg("unsupported RLS unary operator %s", node.UnaryExpr.GetOp().String())
		}
		result, err := evalExpr(node.UnaryExpr.GetChild(), rowData, rowIdx)
		if err != nil {
			return truthUnknown, err
		}
		return result.not(), nil
	case *planpb.Expr_BinaryExpr:
		return evalBinaryExpr(node.BinaryExpr, rowData, rowIdx)
	case *planpb.Expr_UnaryRangeExpr:
		return evalUnaryRangeExpr(node.UnaryRangeExpr, rowData, rowIdx)
	case *planpb.Expr_TermExpr:
		return evalTermExpr(node.TermExpr, rowData, rowIdx)
	case *planpb.Expr_JsonContainsExpr:
		return evalJSONContainsExpr(node.JsonContainsExpr, rowData, rowIdx)
	default:
		return truthUnknown, merr.WrapErrServiceInternalMsg("unsupported RLS expression node %T", node)
	}
}

func evalBinaryExpr(expr *planpb.BinaryExpr, rowData *rowData, rowIdx int) (truthValue, error) {
	switch expr.GetOp() {
	case planpb.BinaryExpr_LogicalAnd:
		left, err := evalExpr(expr.GetLeft(), rowData, rowIdx)
		if err != nil || left == truthFalse {
			return left, err
		}
		right, err := evalExpr(expr.GetRight(), rowData, rowIdx)
		if err != nil {
			return truthUnknown, err
		}
		return left.and(right), nil
	case planpb.BinaryExpr_LogicalOr:
		left, err := evalExpr(expr.GetLeft(), rowData, rowIdx)
		if err != nil || left == truthTrue {
			return left, err
		}
		right, err := evalExpr(expr.GetRight(), rowData, rowIdx)
		if err != nil {
			return truthUnknown, err
		}
		return left.or(right), nil
	default:
		return truthUnknown, merr.WrapErrServiceInternalMsg("unsupported RLS binary operator %s", expr.GetOp().String())
	}
}

// Column types and comparison literals are immutable across rows. Check them
// once per batch, including branches that row evaluation may short-circuit.
func (d *rowData) preparePredicate(expr *planpb.Expr) error {
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_UnaryExpr:
		return d.preparePredicate(node.UnaryExpr.GetChild())
	case *planpb.Expr_BinaryExpr:
		if err := d.preparePredicate(node.BinaryExpr.GetLeft()); err != nil {
			return err
		}
		return d.preparePredicate(node.BinaryExpr.GetRight())
	case *planpb.Expr_UnaryRangeExpr:
		if err := d.validateColumn(node.UnaryRangeExpr.GetColumnInfo()); err != nil {
			return err
		}
		dataType := node.UnaryRangeExpr.GetColumnInfo().GetDataType()
		if !canonicalScalarLiteral(dataType, node.UnaryRangeExpr.GetValue()) {
			return merr.WrapErrDataIntegrityMsg("RLS comparison value does not match field type %s", dataType.String())
		}
	case *planpb.Expr_TermExpr:
		return d.validateColumn(node.TermExpr.GetColumnInfo())
	case *planpb.Expr_JsonContainsExpr:
		return d.validateColumn(node.JsonContainsExpr.GetColumnInfo())
	}
	return nil
}

func evalUnaryRangeExpr(expr *planpb.UnaryRangeExpr, rowData *rowData, rowIdx int) (truthValue, error) {
	if expr.GetOp() != planpb.OpType_Equal && expr.GetOp() != planpb.OpType_NotEqual {
		return truthUnknown, merr.WrapErrServiceInternalMsg("unsupported RLS comparison operator %s", expr.GetOp().String())
	}
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return truthUnknown, err
	}
	if rowValue == nil {
		return truthUnknown, nil
	}
	match, err := valueEqual(rowValue, expr.GetValue())
	if err != nil {
		return truthUnknown, err
	}
	if expr.GetOp() == planpb.OpType_NotEqual {
		match = !match
	}
	return truthValueFromBool(match), nil
}

func canonicalScalarLiteral(dataType schemapb.DataType, value *planpb.GenericValue) bool {
	switch {
	case typeutil.IsBoolType(dataType):
		return planparserv2.IsBool(value)
	case typeutil.IsIntegerType(dataType), typeutil.IsTimestamptzType(dataType):
		return planparserv2.IsInteger(value)
	case typeutil.IsFloatingType(dataType):
		return planparserv2.IsFloating(value)
	case typeutil.IsStringType(dataType):
		return planparserv2.IsString(value)
	default:
		return false
	}
}

func evalTermExpr(expr *planpb.TermExpr, rowData *rowData, rowIdx int) (truthValue, error) {
	if expr.GetIsInField() {
		return truthUnknown, merr.WrapErrServiceInternalMsg("RLS term expression does not support field-to-field IN")
	}
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return truthUnknown, err
	}
	if rowValue == nil {
		return truthUnknown, nil
	}
	matcher, err := rowData.termMatcher(expr)
	if err != nil {
		return truthUnknown, err
	}
	_, matched := matcher.index(rowValue)
	return truthValueFromBool(matched), nil
}

func evalJSONContainsExpr(expr *planpb.JSONContainsExpr, rowData *rowData, rowIdx int) (truthValue, error) {
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return truthUnknown, err
	}
	if rowValue == nil {
		return truthUnknown, nil
	}
	arrayValue, ok := rowValue.(*schemapb.ScalarField)
	if !ok {
		return truthUnknown, merr.WrapErrServiceInternalMsg("RLS contains expression only supports array fields")
	}
	if expr.GetOp() == planpb.JSONContainsExpr_Contains && len(expr.GetElements()) != 1 {
		return truthUnknown, merr.WrapErrServiceInternalMsg("RLS array_contains expression requires exactly one element")
	}
	switch expr.GetOp() {
	case planpb.JSONContainsExpr_Contains, planpb.JSONContainsExpr_ContainsAll, planpb.JSONContainsExpr_ContainsAny:
		matcher, err := rowData.arrayMatcher(expr)
		if err != nil {
			return truthUnknown, err
		}
		matched, err := matcher.matches(arrayValue, rowData)
		if err != nil {
			return truthUnknown, err
		}
		return truthValueFromBool(matched), nil
	default:
		return truthUnknown, merr.WrapErrServiceInternalMsg("unsupported RLS contains operator %s", expr.GetOp().String())
	}
}

func valueEqual(rowValue any, target *planpb.GenericValue) (bool, error) {
	if boolVal, ok := rowValue.(bool); ok {
		targetBool, ok := target.GetVal().(*planpb.GenericValue_BoolVal)
		if !ok {
			return false, nil
		}
		return boolVal == targetBool.BoolVal, nil
	}
	if stringVal, ok := rowValue.(string); ok {
		targetString, ok := target.GetVal().(*planpb.GenericValue_StringVal)
		if !ok {
			return false, nil
		}
		return stringVal == targetString.StringVal, nil
	}
	if rowFloat, ok := rowValue.(float32); ok {
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return false, nil
		}
		return rowFloat == float32(targetNumber), nil
	}
	if rowDouble, ok := rowValue.(float64); ok {
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return false, nil
		}
		return rowDouble == targetNumber, nil
	}
	if targetInt, ok := target.GetVal().(*planpb.GenericValue_Int64Val); ok {
		if rowInt, ok := integerValue(rowValue); ok {
			return rowInt == targetInt.Int64Val, nil
		}
		return false, nil
	}
	if targetFloat, ok := target.GetVal().(*planpb.GenericValue_FloatVal); ok {
		rowNumber, ok := numericValue(rowValue)
		if !ok {
			return false, nil
		}
		return rowNumber == targetFloat.FloatVal, nil
	}
	return false, merr.WrapErrServiceInternalMsg("unsupported RLS value type %T", rowValue)
}

func numericValue(value any) (float64, bool) {
	if value, ok := integerValue(value); ok {
		return float64(value), true
	}
	return floatValue(value)
}

func integerValue(value any) (int64, bool) {
	switch v := value.(type) {
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

func floatValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func genericNumericValue(value *planpb.GenericValue) (float64, bool) {
	switch v := value.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		return float64(v.Int64Val), true
	case *planpb.GenericValue_FloatVal:
		return v.FloatVal, true
	default:
		return 0, false
	}
}

// A nil visit validates the row without scanning elements, for empty targets.
func visitScalarArrayElements(arrayValue *schemapb.ScalarField, expected schemapb.DataType, rowData *rowData, visit func(any) bool) (bool, error) {
	switch data := arrayValue.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		if typeutil.IsBoolType(expected) {
			return containsValidArrayElement(rowData, arrayValue, data.BoolData.GetData(), visit)
		}
	case *schemapb.ScalarField_IntData:
		if expected == schemapb.DataType_Int8 || expected == schemapb.DataType_Int16 || expected == schemapb.DataType_Int32 {
			return containsValidArrayElement(rowData, arrayValue, data.IntData.GetData(), visit)
		}
	case *schemapb.ScalarField_LongData:
		if expected == schemapb.DataType_Int64 {
			return containsValidArrayElement(rowData, arrayValue, data.LongData.GetData(), visit)
		}
	case *schemapb.ScalarField_FloatData:
		if expected == schemapb.DataType_Float {
			return containsValidArrayElement(rowData, arrayValue, data.FloatData.GetData(), visit)
		}
	case *schemapb.ScalarField_DoubleData:
		if expected == schemapb.DataType_Double {
			return containsValidArrayElement(rowData, arrayValue, data.DoubleData.GetData(), visit)
		}
	case *schemapb.ScalarField_StringData:
		if typeutil.IsStringType(expected) {
			return containsValidArrayElement(rowData, arrayValue, data.StringData.GetData(), visit)
		}
	}
	return false, merr.WrapErrServiceInternalMsg("RLS array data does not match element type %s", expected.String())
}

func (d *rowData) arrayElementValidData(arrayValue *schemapb.ScalarField, dataLen int) ([]bool, error) {
	if layout, ok := d.arrayElementLayouts[arrayValue]; ok {
		return layout.validData, layout.err
	}
	validData := typeutil.GetArrayElementValidData(arrayValue)
	var err error
	if len(validData) > 0 && dataLen != len(validData) {
		validRows := int(funcutil.CountValidRows(validData))
		if dataLen != validRows {
			err = merr.WrapErrServiceInternalMsg(
				"RLS array element data length %d matches neither validity length %d nor valid row count %d",
				dataLen, len(validData), validRows,
			)
		} else {
			// Compact element data contains physical values only for valid positions.
			validData = nil
		}
	}
	if d.arrayElementLayouts == nil {
		d.arrayElementLayouts = make(map[*schemapb.ScalarField]arrayElementLayout)
	}
	d.arrayElementLayouts[arrayValue] = arrayElementLayout{validData: validData, err: err}
	return validData, err
}

func containsValidArrayElement[T any](rowData *rowData, arrayValue *schemapb.ScalarField, values []T, matches func(any) bool) (bool, error) {
	validData, err := rowData.arrayElementValidData(arrayValue, len(values))
	if err != nil {
		return false, err
	}
	if matches == nil {
		return false, nil
	}
	for index, value := range values {
		if len(validData) > 0 && !validData[index] {
			continue
		}
		if matches(value) {
			return true, nil
		}
	}
	return false, nil
}
