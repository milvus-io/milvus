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

package rls

import (
	"context"
	"math"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type compiledExpression struct {
	permissive  []*compiledPolicyExpression
	restrictive []*compiledPolicyExpression
	needsTags   bool
}

type compiledPolicyExpression struct {
	expr                 *planpb.Expr
	needsPrincipal       bool
	tagVariables         map[string]string
	tagVariableDataTypes map[string][]schemapb.DataType
}

func compiledExpressionNeedsTags(e *compiledExpression) bool {
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

func (e *compiledPolicyExpression) isStatic() bool {
	return e != nil && !e.needsPrincipal && len(e.tagVariables) == 0
}

type policyExprTemplate struct {
	policyType     rlsutil.PolicyType
	expr           string
	needsPrincipal bool
	tagVariables   map[string]string
}

func preparePolicyExprTemplates(policies []*rlsutil.RowPolicy, action rlsutil.PolicyAction, kind exprKind) ([]policyExprTemplate, int) {
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
		case rlsutil.PolicyTypePermissive:
			if permissiveCount > 0 {
				permissiveLength += len(" or ")
			}
			permissiveCount++
			permissiveLength += len(strings.TrimSpace(policyExpr)) + 2
		case rlsutil.PolicyTypeRestrictive:
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

func compileExprTemplates(schemaHelper *typeutil.SchemaHelper, templates []policyExprTemplate, timezone string, kind exprKind) (*compiledExpression, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	visitorArgs := &planparserv2.ParserVisitorArgs{Timezone: timezone}
	compiled := &compiledExpression{}
	for _, template := range templates {
		policyExpr, err := compilePolicyExprTemplate(schemaHelper, template, visitorArgs, kind)
		if err != nil {
			return nil, err
		}
		if policyExpr == nil {
			continue
		}
		switch template.policyType {
		case rlsutil.PolicyTypePermissive:
			compiled.permissive = append(compiled.permissive, policyExpr)
		case rlsutil.PolicyTypeRestrictive:
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

func compilePolicyExprTemplate(schemaHelper *typeutil.SchemaHelper, template policyExprTemplate, visitorArgs *planparserv2.ParserVisitorArgs, kind exprKind) (*compiledPolicyExpression, error) {
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
	if err := rlsutil.ValidateParsedExpression(parsedExpr, allowedTemplateVariables); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid persisted RLS policy expression")
	}
	if kind == usingExprKind {
		if err := rlsutil.ValidateUsingExpressionSchema(schemaHelper, parsedExpr); err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "invalid persisted RLS using expression")
		}
	}
	var tagVariableDataTypes map[string][]schemapb.DataType
	if len(template.tagVariables) > 0 {
		tagVariableDataTypes = make(map[string][]schemapb.DataType, len(template.tagVariables))
		for _, variable := range template.tagVariables {
			dataTypes := make([]schemapb.DataType, 0, 1)
			collectRLSTemplateDataTypes(parsedExpr, variable, &dataTypes)
			tagVariableDataTypes[variable] = dataTypes
		}
	}
	return &compiledPolicyExpression{
		expr:                 parsedExpr,
		needsPrincipal:       template.needsPrincipal,
		tagVariables:         template.tagVariables,
		tagVariableDataTypes: tagVariableDataTypes,
	}, nil
}

func (e *compiledExpression) Instantiate(principalName string, principalTags map[string]rlsutil.TagValue) (*planpb.Expr, error) {
	if e == nil {
		return nil, nil
	}
	if len(e.permissive) == 0 {
		if len(e.restrictive) > 0 {
			return alwaysFalsePredicate(), nil
		}
		return nil, nil
	}

	restrictiveExprs, restrictiveFalse, err := instantiatePolicyExprs(e.restrictive, principalName, principalTags, planpb.BinaryExpr_LogicalAnd)
	if err != nil {
		return nil, err
	}
	if restrictiveFalse {
		return alwaysFalsePredicate(), nil
	}

	permissiveExprs, permissiveFalse, err := instantiatePolicyExprs(e.permissive, principalName, principalTags, planpb.BinaryExpr_LogicalOr)
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
	return rewriter.RewriteExpr(finalExpr), nil
}

func instantiatePolicyExprs(
	policies []*compiledPolicyExpression,
	principalName string,
	principalTags map[string]rlsutil.TagValue,
	op planpb.BinaryExpr_BinaryOp,
) ([]*planpb.Expr, bool, error) {
	exprs := make([]*planpb.Expr, 0, len(policies))
	for _, policy := range policies {
		expr, err := policy.Instantiate(principalName, principalTags)
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

func (e *compiledPolicyExpression) Instantiate(principalName string, principalTags map[string]rlsutil.TagValue) (*planpb.Expr, error) {
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
	// variable. Normalize it before allocating or cloning an expression.
	var normalizedVariable string
	var normalizedTagValue rlsutil.TagValue
	for tagKey, variable := range e.tagVariables {
		tagValue, ok := principalTags[tagKey]
		if !ok {
			return nil, nil
		}
		normalizedTagValue, ok = normalizeRLSTagValue(e.tagVariableDataTypes[variable], tagValue)
		if !ok {
			return nil, nil
		}
		normalizedVariable = variable
	}

	values := make(map[string]*planpb.GenericValue, len(e.tagVariables)+1)
	if e.needsPrincipal {
		values[funcutil.RLSPrincipalTemplateName] = planparserv2.NewString(principalName)
	}
	if normalizedVariable != "" {
		values[normalizedVariable] = rlsTagValueToGenericValue(normalizedTagValue)
	}

	expr := proto.Clone(e.expr).(*planpb.Expr)
	if err := planparserv2.FillExpressionValue(expr, values); err != nil {
		return nil, err
	}
	return expr, nil
}

func rlsTagValueToGenericValue(value rlsutil.TagValue) *planpb.GenericValue {
	switch value.Kind {
	case rlsutil.TagValueKindString:
		return planparserv2.NewString(value.StringValue)
	case rlsutil.TagValueKindInt64:
		return planparserv2.NewInt(value.Int64Value)
	case rlsutil.TagValueKindDouble:
		return planparserv2.NewFloat(value.DoubleValue)
	default:
		return nil
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

// normalizeRLSTagValue chooses a representation that can be safely consumed by
// every occurrence of a tag variable in an expression. Numeric conversions are
// allowed only when they preserve the value exactly; otherwise the policy is
// treated as not matching instead of risking an over-permissive comparison.
func normalizeRLSTagValue(dataTypes []schemapb.DataType, value rlsutil.TagValue) (rlsutil.TagValue, bool) {
	if len(dataTypes) == 0 {
		return rlsutil.TagValue{}, false
	}
	for _, dataType := range dataTypes {
		switch {
		case typeutil.IsStringType(dataType):
			if value.Kind != rlsutil.TagValueKindString {
				return rlsutil.TagValue{}, false
			}
		case typeutil.IsIntegerType(dataType):
			switch value.Kind {
			case rlsutil.TagValueKindInt64:
			case rlsutil.TagValueKindDouble:
				if !isExactInt64(value.DoubleValue) {
					return rlsutil.TagValue{}, false
				}
				value = rlsutil.NewInt64TagValue(int64(value.DoubleValue))
			default:
				return rlsutil.TagValue{}, false
			}
			if !integerTagFitsDataType(value.Int64Value, dataType) {
				return rlsutil.TagValue{}, false
			}
		case typeutil.IsFloatingType(dataType):
			switch value.Kind {
			case rlsutil.TagValueKindInt64:
				if dataType == schemapb.DataType_Float {
					if int64(float64(float32(value.Int64Value))) != value.Int64Value {
						return rlsutil.TagValue{}, false
					}
				} else if int64(float64(value.Int64Value)) != value.Int64Value {
					return rlsutil.TagValue{}, false
				}
			case rlsutil.TagValueKindDouble:
				if dataType == schemapb.DataType_Float && float64(float32(value.DoubleValue)) != value.DoubleValue {
					return rlsutil.TagValue{}, false
				}
			default:
				return rlsutil.TagValue{}, false
			}
		default:
			return rlsutil.TagValue{}, false
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

func collectRLSTemplateDataTypes(expr *planpb.Expr, variable string, dataTypes *[]schemapb.DataType) {
	if expr == nil {
		return
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_UnaryExpr:
		collectRLSTemplateDataTypes(node.UnaryExpr.GetChild(), variable, dataTypes)
	case *planpb.Expr_BinaryExpr:
		collectRLSTemplateDataTypes(node.BinaryExpr.GetLeft(), variable, dataTypes)
		collectRLSTemplateDataTypes(node.BinaryExpr.GetRight(), variable, dataTypes)
	case *planpb.Expr_UnaryRangeExpr:
		if node.UnaryRangeExpr.GetTemplateVariableName() == variable {
			*dataTypes = append(*dataTypes, rlsTemplateColumnDataType(node.UnaryRangeExpr.GetColumnInfo()))
		}
	case *planpb.Expr_JsonContainsExpr:
		if node.JsonContainsExpr.GetTemplateVariableName() == variable {
			*dataTypes = append(*dataTypes, node.JsonContainsExpr.GetColumnInfo().GetElementType())
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

func policyMatchesAction(policy *rlsutil.RowPolicy, action rlsutil.PolicyAction) bool {
	return policy != nil && slices.Contains(policy.GetActions(), action)
}

func QueryAction(isIterator bool) rlsutil.PolicyAction {
	if isIterator {
		return rlsutil.PolicyActionQueryIterator
	}
	return rlsutil.PolicyActionQuery
}

func SearchAction(isAdvanced bool, isIterator bool) rlsutil.PolicyAction {
	if isAdvanced {
		return rlsutil.PolicyActionHybridSearch
	}
	if isIterator {
		return rlsutil.PolicyActionSearchIterator
	}
	return rlsutil.PolicyActionSearch
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
	if err := rlsutil.ValidatePrincipalName(principalName); err != nil {
		return "", false, err
	}
	return principalName, true, nil
}

func MergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	if rlsPredicate == nil || rewriter.IsAlwaysTrueExpr(rlsPredicate) {
		return nil
	}
	if plan == nil {
		return merr.WrapErrServiceInternalMsg("failed to merge RLS predicate into nil plan")
	}
	switch node := plan.GetNode().(type) {
	case *planpb.PlanNode_Query:
		node.Query.Predicates = mergePredicate(node.Query.GetPredicates(), rlsPredicate)
	case *planpb.PlanNode_VectorAnns:
		node.VectorAnns.Predicates = mergePredicate(node.VectorAnns.GetPredicates(), rlsPredicate)
	case *planpb.PlanNode_Predicates:
		node.Predicates = mergePredicate(node.Predicates, rlsPredicate)
	default:
		return merr.WrapErrServiceInternalMsg("failed to merge RLS predicate into unsupported plan node %T", node)
	}
	return nil
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

func mergePredicate(userPredicate *planpb.Expr, rlsPredicate *planpb.Expr) *planpb.Expr {
	if userPredicate == nil || rewriter.IsAlwaysTrueExpr(userPredicate) {
		return rlsPredicate
	}
	if rlsPredicate == nil || rewriter.IsAlwaysTrueExpr(rlsPredicate) {
		return userPredicate
	}
	switch wrapper := userPredicate.GetExpr().(type) {
	case *planpb.Expr_RandomSampleExpr:
		wrapper.RandomSampleExpr.Predicate = mergePredicate(wrapper.RandomSampleExpr.GetPredicate(), rlsPredicate)
		return userPredicate
	case *planpb.Expr_ElementFilterExpr:
		wrapper.ElementFilterExpr.Predicate = mergePredicate(wrapper.ElementFilterExpr.GetPredicate(), rlsPredicate)
		return userPredicate
	}
	return rewriter.RewriteExpr(&planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    planpb.BinaryExpr_LogicalAnd,
				Left:  userPredicate,
				Right: rlsPredicate,
			},
		},
	})
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

func ValidateCheckForWrite(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	return validateCheckForWrite(ctx, defaultManager, collectionID, principalName, action, fieldsData, schemaHelper, rowNum, operation)
}

func ResolveCheckForWrite(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, schemaHelper *typeutil.SchemaHelper, operation string) (*planpb.Expr, error) {
	checkExpr, err := defaultManager.resolveCheckPredicate(ctx, collectionID, principalName, action, schemaHelper)
	if err != nil {
		return nil, err
	}
	if err := ValidateStaticCheckPredicate(checkExpr, operation); err != nil {
		return nil, err
	}
	return checkExpr, nil
}

func ValidateStaticCheckPredicate(checkExpr *planpb.Expr, operation string) error {
	if checkExpr != nil && rewriter.IsAlwaysFalseExpr(checkExpr) {
		return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS check expression", operation)
	}
	return nil
}

func validateCheckForWrite(ctx context.Context, m *manager, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	checkExpr, err := m.resolveCheckPredicate(ctx, collectionID, principalName, action, schemaHelper)
	if err != nil {
		return err
	}
	if err := ValidateStaticCheckPredicate(checkExpr, operation); err != nil {
		return err
	}
	if checkExpr == nil {
		return nil
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, checkExpr, operation, "check")
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
	if err := rowData.validateRowCount(referencedFieldIDs, rowNum); err != nil {
		return err
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
	field            *schemapb.FieldData
	iter             func(int) any
	scalarRows       int
	scalarShapeValid bool
	arrayRows        int
	arrayShapeValid  bool
	arrayDataIndices []int
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

type neverMatchLiteral struct{}

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
		reader := &fieldReader{field: fieldData}
		if isRLSScalarType(fieldData.GetType()) {
			reader.iter = typeutil.GetDataIterator(fieldData)
			dataLen := rlsScalarDataLen(fieldData)
			reader.scalarRows = dataLen
			reader.scalarShapeValid = true
			if validData := typeutil.GetFieldDataValidData(fieldData); len(validData) > 0 {
				reader.scalarRows = len(validData)
				reader.scalarShapeValid = dataLen == len(validData) || dataLen == int(funcutil.CountValidRows(validData))
			}
		} else if fieldData.GetType() == schemapb.DataType_Array {
			validData := typeutil.GetFieldDataValidData(fieldData)
			dataLen := len(fieldData.GetScalars().GetArrayData().GetData())
			reader.arrayRows = dataLen
			reader.arrayShapeValid = true
			if len(validData) > 0 {
				reader.arrayRows = len(validData)
				validRows := int(funcutil.CountValidRows(validData))
				reader.arrayShapeValid = dataLen == len(validData) || dataLen == validRows
				if reader.arrayShapeValid && dataLen != len(validData) {
					reader.arrayDataIndices = make([]int, len(validData))
					compactIdx := 0
					for rowIdx, valid := range validData {
						if valid {
							reader.arrayDataIndices[rowIdx] = compactIdx
							compactIdx++
						}
					}
				}
			}
		}
		data.fields[fieldData.GetFieldId()] = reader
	}
	return data
}

func (d *rowData) validateRowCount(referencedFieldIDs []int64, expected int) error {
	for _, fieldID := range referencedFieldIDs {
		reader, ok := d.fields[fieldID]
		if !ok {
			return merr.WrapErrServiceInternalMsg("RLS expression references field id %d which is not present in row data", fieldID)
		}

		var actual int
		switch {
		case isRLSScalarType(reader.field.GetType()):
			if !reader.scalarShapeValid {
				return merr.WrapErrServiceInternalMsg("RLS field %s has inconsistent data and validity lengths", reader.field.GetFieldName())
			}
			actual = reader.scalarRows
		case reader.field.GetType() == schemapb.DataType_Array:
			if !reader.arrayShapeValid {
				return merr.WrapErrServiceInternalMsg("RLS field %s has inconsistent data and validity lengths", reader.field.GetFieldName())
			}
			actual = reader.arrayRows
		default:
			return merr.WrapErrServiceInternalMsg("RLS expression references unsupported field %s with type %s", reader.field.GetFieldName(), reader.field.GetType().String())
		}
		if actual != expected {
			return merr.WrapErrServiceInternalMsg("RLS field %s row count %d does not match expected row count %d", reader.field.GetFieldName(), actual, expected)
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
		switch typed := value.GetVal().(type) {
		case *planpb.GenericValue_Int64Val:
			if typed == nil {
				return nil, false
			}
			return typed.Int64Val, true
		case *planpb.GenericValue_FloatVal:
			// ARRAY all/any accepts cross-numeric literals, but integer ARRAY
			// execution currently treats floating literals as non-matching.
			return neverMatchLiteral{}, typed != nil
		default:
			return nil, false
		}
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
	if m.op == planpb.JSONContainsExpr_ContainsAny {
		if len(m.values) == 0 {
			return false, nil
		}
		return visitScalarArrayElements(arrayValue, rowData, func(value any) bool {
			_, ok := m.index(value)
			return ok
		})
	}

	if len(m.values) == 0 {
		return true, nil
	}
	m.generation++
	if m.generation == 0 {
		clear(m.seen)
		m.generation = 1
	}
	matched := 0
	return visitScalarArrayElements(arrayValue, rowData, func(value any) bool {
		index, ok := m.index(value)
		if !ok || m.seen[index] == m.generation {
			return false
		}
		m.seen[index] = m.generation
		matched++
		return matched == len(m.values)
	})
}

func (d *rowData) value(column *planpb.ColumnInfo, rowIdx int) (any, error) {
	if column == nil {
		return nil, merr.WrapErrServiceInternalMsg("RLS expression has empty column info")
	}
	if len(column.GetNestedPath()) > 0 || column.GetIsElementLevel() {
		return nil, merr.WrapErrServiceInternalMsg("RLS expression does not support nested or element-level fields")
	}
	reader, ok := d.fields[column.GetFieldId()]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("RLS expression references field id %d which is not present in row data", column.GetFieldId())
	}
	switch reader.field.GetType() {
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
		if !reader.scalarShapeValid {
			return nil, merr.WrapErrServiceInternalMsg("RLS field %s has inconsistent data and validity lengths", reader.field.GetFieldName())
		}
		if rowIdx < 0 || rowIdx >= reader.scalarRows {
			return nil, merr.WrapErrServiceInternalMsg("RLS row index %d exceeds field %s row count %d", rowIdx, reader.field.GetFieldName(), reader.scalarRows)
		}
		return reader.iter(rowIdx), nil
	case schemapb.DataType_Array:
		if !reader.arrayShapeValid {
			return nil, merr.WrapErrServiceInternalMsg("RLS field %s has inconsistent data and validity lengths", reader.field.GetFieldName())
		}
		if rowIdx < 0 || rowIdx >= reader.arrayRows {
			return nil, merr.WrapErrServiceInternalMsg("RLS row index %d exceeds field %s row count %d", rowIdx, reader.field.GetFieldName(), reader.arrayRows)
		}
		value, err := arrayValue(reader, rowIdx)
		if err != nil || value == nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("RLS expression references unsupported field %s with type %s", reader.field.GetFieldName(), reader.field.GetType().String())
	}
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

func rlsScalarDataLen(field *schemapb.FieldData) int {
	switch field.GetType() {
	case schemapb.DataType_Bool:
		return len(field.GetScalars().GetBoolData().GetData())
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return len(field.GetScalars().GetIntData().GetData())
	case schemapb.DataType_Int64:
		return len(field.GetScalars().GetLongData().GetData())
	case schemapb.DataType_Float:
		return len(field.GetScalars().GetFloatData().GetData())
	case schemapb.DataType_Double:
		return len(field.GetScalars().GetDoubleData().GetData())
	case schemapb.DataType_Timestamptz:
		return len(field.GetScalars().GetTimestamptzData().GetData())
	case schemapb.DataType_VarChar, schemapb.DataType_Text:
		return len(field.GetScalars().GetStringData().GetData())
	default:
		return 0
	}
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

func evalUnaryRangeExpr(expr *planpb.UnaryRangeExpr, rowData *rowData, rowIdx int) (truthValue, error) {
	if expr.GetOp() != planpb.OpType_Equal {
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
	return truthValueFromBool(match), nil
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
	if _, usable := scalarLiteralKey(matcher.dataType, rowValue); !usable {
		return truthUnknown, merr.WrapErrServiceInternalMsg("RLS field value type %T does not match expression type %s", rowValue, matcher.dataType.String())
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

func arrayValue(reader *fieldReader, rowIdx int) (*schemapb.ScalarField, error) {
	data := reader.field.GetScalars().GetArrayData().GetData()
	dataIdx := rowIdx
	validData := typeutil.GetFieldDataValidData(reader.field)
	if len(validData) > 0 {
		if rowIdx >= len(validData) {
			return nil, merr.WrapErrServiceInternalMsg("RLS row index %d exceeds valid data length %d", rowIdx, len(validData))
		}
		if !validData[rowIdx] {
			return nil, nil
		}
	}
	if len(reader.arrayDataIndices) > 0 {
		dataIdx = reader.arrayDataIndices[rowIdx]
	}
	if dataIdx >= len(data) {
		return nil, merr.WrapErrServiceInternalMsg("RLS row index %d maps outside data length %d", rowIdx, len(data))
	}
	return data[dataIdx], nil
}

func visitScalarArrayElements(arrayValue *schemapb.ScalarField, rowData *rowData, visit func(any) bool) (bool, error) {
	switch data := arrayValue.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		return containsValidArrayElement(rowData, arrayValue, data.BoolData.GetData(), func(value bool) bool { return visit(value) })
	case *schemapb.ScalarField_IntData:
		return containsValidArrayElement(rowData, arrayValue, data.IntData.GetData(), func(value int32) bool { return visit(value) })
	case *schemapb.ScalarField_LongData:
		return containsValidArrayElement(rowData, arrayValue, data.LongData.GetData(), func(value int64) bool { return visit(value) })
	case *schemapb.ScalarField_FloatData:
		return containsValidArrayElement(rowData, arrayValue, data.FloatData.GetData(), func(value float32) bool { return visit(value) })
	case *schemapb.ScalarField_DoubleData:
		return containsValidArrayElement(rowData, arrayValue, data.DoubleData.GetData(), func(value float64) bool { return visit(value) })
	case *schemapb.ScalarField_StringData:
		return containsValidArrayElement(rowData, arrayValue, data.StringData.GetData(), func(value string) bool { return visit(value) })
	default:
		return false, merr.WrapErrServiceInternalMsg("unsupported RLS array element type %T", data)
	}
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

func containsValidArrayElement[T any](rowData *rowData, arrayValue *schemapb.ScalarField, values []T, matches func(T) bool) (bool, error) {
	validData, err := rowData.arrayElementValidData(arrayValue, len(values))
	if err != nil {
		return false, err
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
