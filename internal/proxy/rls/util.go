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
}

type compiledPolicyExpression struct {
	expr           *planpb.Expr
	needsPrincipal bool
	tagVariables   map[string]string
}

type policyExprTemplate struct {
	policyType     rlsutil.PolicyType
	expr           string
	needsPrincipal bool
	tagVariables   map[string]string
}

func preparePolicyExprTemplates(policies []*rlsutil.RowPolicy, action rlsutil.PolicyAction, exprSelector func(*rlsutil.RowPolicy) string) ([]policyExprTemplate, string) {
	templates := make([]policyExprTemplate, 0)
	permissiveExprs := make([]string, 0)
	restrictiveExprs := make([]string, 0)

	for _, policy := range policies {
		if !PolicyMatchesAction(policy, action) {
			continue
		}
		policyExpr := strings.TrimSpace(exprSelector(policy))
		if policyExpr == "" {
			continue
		}
		var policyNeedsPrincipal bool
		var tagVariables map[string]string
		policyExpr, policyNeedsPrincipal, tagVariables = toTemplateExpr(policyExpr)
		templates = append(templates, policyExprTemplate{
			policyType:     policy.GetPolicyType(),
			expr:           policyExpr,
			needsPrincipal: policyNeedsPrincipal,
			tagVariables:   tagVariables,
		})
		switch policy.GetPolicyType() {
		case rlsutil.PolicyTypePermissive:
			permissiveExprs = append(permissiveExprs, policyExpr)
		case rlsutil.PolicyTypeRestrictive:
			restrictiveExprs = append(restrictiveExprs, policyExpr)
		}
	}

	if len(permissiveExprs) == 0 {
		if len(restrictiveExprs) > 0 {
			return templates, "false"
		}
		return templates, ""
	}

	groups := make([]string, 0, 2)
	groups = append(groups, joinExprs(permissiveExprs, "or"))
	if len(restrictiveExprs) > 0 {
		groups = append(groups, joinExprs(restrictiveExprs, "and"))
	}
	return templates, joinExprs(groups, "and")
}

func toTemplateExpr(expr string) (string, bool, map[string]string) {
	return funcutil.ConvertRLSTemplateVariables(strings.TrimSpace(expr))
}

func compileExprTemplates(schemaHelper *typeutil.SchemaHelper, templates []policyExprTemplate, visitorArgs *planparserv2.ParserVisitorArgs) (*compiledExpression, error) {
	if len(templates) == 0 {
		return nil, nil
	}
	compiled := &compiledExpression{}
	for _, template := range templates {
		policyExpr, err := compilePolicyExprTemplate(schemaHelper, template.expr, template.needsPrincipal, template.tagVariables, visitorArgs)
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
	if len(compiled.permissive) == 0 && len(compiled.restrictive) == 0 {
		return nil, nil
	}
	return compiled, nil
}

func compilePolicyExprTemplate(schemaHelper *typeutil.SchemaHelper, expr string, needsPrincipal bool, tagVariables map[string]string, visitorArgs *planparserv2.ParserVisitorArgs) (*compiledPolicyExpression, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	if schemaHelper == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to compile RLS expression template with nil schema helper")
	}
	parsedExpr, err := planparserv2.ParseExprTemplate(schemaHelper, expr, visitorArgs)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to parse RLS expression template")
	}
	return &compiledPolicyExpression{
		expr:           parsedExpr,
		needsPrincipal: needsPrincipal,
		tagVariables:   tagVariables,
	}, nil
}

func (e *compiledExpression) Instantiate(principalName string, principalTags map[string]string) (*planpb.Expr, error) {
	if e == nil {
		return nil, nil
	}
	if len(e.permissive) == 0 {
		if len(e.restrictive) > 0 {
			return alwaysFalsePredicate(), nil
		}
		return nil, nil
	}

	permissiveExprs, err := instantiatePolicyExprs(e.permissive, principalName, principalTags)
	if err != nil {
		return nil, err
	}
	restrictiveExprs, err := instantiatePolicyExprs(e.restrictive, principalName, principalTags)
	if err != nil {
		return nil, err
	}

	finalExpr := combinePredicates(permissiveExprs, planpb.BinaryExpr_LogicalOr)
	if len(restrictiveExprs) > 0 {
		finalExpr = combinePredicate(finalExpr, combinePredicates(restrictiveExprs, planpb.BinaryExpr_LogicalAnd), planpb.BinaryExpr_LogicalAnd)
	}
	return rewriter.RewriteExpr(finalExpr), nil
}

func instantiatePolicyExprs(policies []*compiledPolicyExpression, principalName string, principalTags map[string]string) ([]*planpb.Expr, error) {
	exprs := make([]*planpb.Expr, 0, len(policies))
	for _, policy := range policies {
		expr, err := policy.Instantiate(principalName, principalTags)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			exprs = append(exprs, expr)
		}
	}
	return exprs, nil
}

func (e *compiledPolicyExpression) Instantiate(principalName string, principalTags map[string]string) (*planpb.Expr, error) {
	if e == nil || e.expr == nil {
		return nil, nil
	}
	if e.needsPrincipal && principalName == "" {
		return alwaysFalsePredicate(), nil
	}

	values := make(map[string]*planpb.GenericValue, len(e.tagVariables)+1)
	if e.needsPrincipal {
		values[funcutil.RLSPrincipalTemplateName] = planparserv2.NewString(principalName)
	}
	for tagKey, variable := range e.tagVariables {
		tagValue, ok := principalTags[tagKey]
		if !ok {
			return alwaysFalsePredicate(), nil
		}
		values[variable] = planparserv2.NewString(tagValue)
	}

	expr := proto.Clone(e.expr).(*planpb.Expr)
	if err := planparserv2.FillExpressionValue(expr, values); err != nil {
		return nil, err
	}
	return rewriter.RewriteExpr(expr), nil
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
		if isAlwaysTrueExpr(left) {
			return right
		}
		if isAlwaysTrueExpr(right) {
			return left
		}
	case planpb.BinaryExpr_LogicalOr:
		if isAlwaysTrueExpr(left) || isAlwaysTrueExpr(right) {
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

func PolicyMatchesAction(policy *rlsutil.RowPolicy, action rlsutil.PolicyAction) bool {
	if policy == nil {
		return false
	}
	for _, policyAction := range policy.GetActions() {
		if policyAction == action {
			return true
		}
	}
	return false
}

func joinExprs(exprs []string, op string) string {
	nonEmpty := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr != "" {
			nonEmpty = append(nonEmpty, parenthesizeExpr(expr))
		}
	}
	if len(nonEmpty) == 0 {
		return ""
	}
	return strings.Join(nonEmpty, " "+op+" ")
}

func parenthesizeExpr(expr string) string {
	return "(" + strings.TrimSpace(expr) + ")"
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
	return mergePredicateToPlan(plan, rlsPredicate)
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

func mergePredicateToPlan(plan *planpb.PlanNode, rlsPredicate *planpb.Expr) error {
	if rlsPredicate == nil || isAlwaysTrueExpr(rlsPredicate) {
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

func mergePredicate(userPredicate *planpb.Expr, rlsPredicate *planpb.Expr) *planpb.Expr {
	if userPredicate == nil || isAlwaysTrueExpr(userPredicate) {
		return rlsPredicate
	}
	if rlsPredicate == nil || isAlwaysTrueExpr(rlsPredicate) {
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

func isAlwaysTrueExpr(expr *planpb.Expr) bool {
	return expr != nil && expr.GetAlwaysTrueExpr() != nil
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

func ValidateCheckForWrite(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, operation string) error {
	visitorArgs := &planparserv2.ParserVisitorArgs{Timezone: schemaHelper.GetTimezone()}
	checkExpr, err := DefaultManager().GetRLSCheckPredicate(ctx, collectionID, principalName, action, enforceRLS, schemaHelper, visitorArgs)
	if err != nil {
		return err
	}
	if checkExpr == nil {
		return nil
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, checkExpr, operation, "check")
}

func ValidateUsingPredicateForExistingRows(ctx context.Context, fieldsData []*schemapb.FieldData, rowNum int, operation string, usingExpr *planpb.Expr) error {
	if rowNum == 0 {
		return nil
	}
	if usingExpr == nil {
		return nil
	}
	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, usingExpr, operation, "using")
}

func validateRows(ctx context.Context, fieldsData []*schemapb.FieldData, schemaHelper *typeutil.SchemaHelper, rowNum int, expr string, operation string, exprKind string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" || rowNum == 0 {
		return nil
	}

	parsedExpr, err := planparserv2.ParseExpr(schemaHelper, expr, nil)
	if err != nil {
		return merr.Wrapf(err, "failed to parse RLS %s expression for %s", exprKind, operation)
	}

	return ValidateRowsByPredicate(ctx, fieldsData, rowNum, parsedExpr, operation, exprKind)
}

func ValidateRowsByPredicate(ctx context.Context, fieldsData []*schemapb.FieldData, rowNum int, parsedExpr *planpb.Expr, operation string, exprKind string) error {
	if parsedExpr == nil || rowNum == 0 {
		return nil
	}

	rowData := newRowData(fieldsData)
	for rowIdx := 0; rowIdx < rowNum; rowIdx++ {
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
	field *schemapb.FieldData
	iter  func(int) any
}

type rowData struct {
	fields map[int64]*fieldReader
}

func newRowData(fieldsData []*schemapb.FieldData) *rowData {
	data := &rowData{
		fields: make(map[int64]*fieldReader, len(fieldsData)),
	}
	for _, fieldData := range fieldsData {
		if fieldData == nil {
			continue
		}
		iteratorField := fieldData
		if fieldData.GetValidData() != nil && len(fieldData.GetValidData()) == 0 {
			denseField := *fieldData
			denseField.ValidData = nil
			iteratorField = &denseField
		}
		data.fields[fieldData.GetFieldId()] = &fieldReader{
			field: fieldData,
			iter:  typeutil.GetDataIterator(iteratorField),
		}
	}
	return data
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
		return reader.iter(rowIdx), nil
	case schemapb.DataType_Array:
		value, err := arrayValue(reader.field, rowIdx)
		if err != nil || value == nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("RLS expression references unsupported field %s with type %s", reader.field.GetFieldName(), reader.field.GetType().String())
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
	rowValue, err := rowData.value(expr.GetColumnInfo(), rowIdx)
	if err != nil {
		return truthUnknown, err
	}
	if rowValue == nil {
		return truthUnknown, nil
	}
	match, err := compareValue(rowValue, expr.GetValue(), expr.GetOp())
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
	for _, value := range expr.GetValues() {
		match, err := valueEqual(rowValue, value)
		if err != nil {
			return truthUnknown, err
		}
		if match {
			return truthTrue, nil
		}
	}
	return truthFalse, nil
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

	switch expr.GetOp() {
	case planpb.JSONContainsExpr_Contains:
		if len(expr.GetElements()) != 1 {
			return truthUnknown, merr.WrapErrServiceInternalMsg("RLS array_contains expression requires exactly one element")
		}
		contains, err := scalarArrayContains(arrayValue, expr.GetElements()[0])
		if err != nil {
			return truthUnknown, err
		}
		return truthValueFromBool(contains), nil
	case planpb.JSONContainsExpr_ContainsAll:
		for _, element := range expr.GetElements() {
			contains, err := scalarArrayContains(arrayValue, element)
			if err != nil {
				return truthUnknown, err
			}
			if !contains {
				return truthFalse, nil
			}
		}
		return truthTrue, nil
	case planpb.JSONContainsExpr_ContainsAny:
		for _, element := range expr.GetElements() {
			contains, err := scalarArrayContains(arrayValue, element)
			if err != nil {
				return truthUnknown, err
			}
			if contains {
				return truthTrue, nil
			}
		}
		return truthFalse, nil
	default:
		return truthUnknown, merr.WrapErrServiceInternalMsg("unsupported RLS contains operator %s", expr.GetOp().String())
	}
}

func compareValue(rowValue any, target *planpb.GenericValue, op planpb.OpType) (bool, error) {
	switch op {
	case planpb.OpType_Equal:
		return valueEqual(rowValue, target)
	case planpb.OpType_NotEqual:
		equal, err := valueEqual(rowValue, target)
		return !equal, err
	case planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual, planpb.OpType_LessThan, planpb.OpType_LessEqual:
		order, err := valueCompare(rowValue, target)
		if err != nil {
			return false, err
		}
		switch op {
		case planpb.OpType_GreaterThan:
			return order > 0, nil
		case planpb.OpType_GreaterEqual:
			return order >= 0, nil
		case planpb.OpType_LessThan:
			return order < 0, nil
		case planpb.OpType_LessEqual:
			return order <= 0, nil
		}
	default:
		return false, merr.WrapErrServiceInternalMsg("unsupported RLS comparison operator %s", op.String())
	}
	return false, merr.WrapErrServiceInternalMsg("unsupported RLS comparison operator %s", op.String())
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

func valueCompare(rowValue any, target *planpb.GenericValue) (int, error) {
	if rowString, ok := rowValue.(string); ok {
		targetString, ok := target.GetVal().(*planpb.GenericValue_StringVal)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("RLS comparison type mismatch")
		}
		switch {
		case rowString < targetString.StringVal:
			return -1, nil
		case rowString > targetString.StringVal:
			return 1, nil
		default:
			return 0, nil
		}
	}
	if rowFloat, ok := rowValue.(float32); ok {
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("RLS comparison type mismatch")
		}
		return compareFloat(float64(rowFloat), float64(float32(targetNumber))), nil
	}
	if rowDouble, ok := rowValue.(float64); ok {
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("RLS comparison type mismatch")
		}
		return compareFloat(rowDouble, targetNumber), nil
	}
	if targetInt, ok := target.GetVal().(*planpb.GenericValue_Int64Val); ok {
		if rowInt, ok := integerValue(rowValue); ok {
			return compareInt(rowInt, targetInt.Int64Val), nil
		}
		return 0, merr.WrapErrServiceInternalMsg("RLS comparison type mismatch")
	}
	if targetFloat, ok := target.GetVal().(*planpb.GenericValue_FloatVal); ok {
		rowNumber, ok := numericValue(rowValue)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("unsupported RLS ordered comparison value type %T", rowValue)
		}
		return compareFloat(rowNumber, targetFloat.FloatVal), nil
	}
	return 0, merr.WrapErrServiceInternalMsg("RLS comparison type mismatch")
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

func compareInt(left int64, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareFloat(left float64, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
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

func arrayValue(field *schemapb.FieldData, rowIdx int) (*schemapb.ScalarField, error) {
	data := field.GetScalars().GetArrayData().GetData()
	dataIdx, valid, err := logicalRowToDataIndex(field.GetValidData(), len(data), rowIdx)
	if err != nil || !valid {
		return nil, err
	}
	return data[dataIdx], nil
}

func logicalRowToDataIndex(validData []bool, dataLen int, rowIdx int) (int, bool, error) {
	if len(validData) == 0 {
		if rowIdx >= dataLen {
			return 0, false, merr.WrapErrServiceInternalMsg("RLS row index %d exceeds data length %d", rowIdx, dataLen)
		}
		return rowIdx, true, nil
	}
	if rowIdx >= len(validData) {
		return 0, false, merr.WrapErrServiceInternalMsg("RLS row index %d exceeds valid data length %d", rowIdx, len(validData))
	}
	if !validData[rowIdx] {
		return 0, false, nil
	}
	if dataLen == len(validData) {
		return rowIdx, true, nil
	}
	dataIdx := 0
	for i := 0; i < rowIdx; i++ {
		if validData[i] {
			dataIdx++
		}
	}
	if dataIdx >= dataLen {
		return 0, false, merr.WrapErrServiceInternalMsg("RLS row index %d maps outside data length %d", rowIdx, dataLen)
	}
	return dataIdx, true, nil
}

func scalarArrayContains(arrayValue *schemapb.ScalarField, target *planpb.GenericValue) (bool, error) {
	switch data := arrayValue.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_BoolVal)
		if !ok {
			return false, nil
		}
		for _, value := range data.BoolData.GetData() {
			if value == targetValue.BoolVal {
				return true, nil
			}
		}
	case *schemapb.ScalarField_IntData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_Int64Val)
		if !ok {
			return false, nil
		}
		for _, value := range data.IntData.GetData() {
			if int64(value) == targetValue.Int64Val {
				return true, nil
			}
		}
	case *schemapb.ScalarField_LongData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_Int64Val)
		if !ok {
			return false, nil
		}
		for _, value := range data.LongData.GetData() {
			if value == targetValue.Int64Val {
				return true, nil
			}
		}
	case *schemapb.ScalarField_FloatData:
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return false, nil
		}
		targetValue := float32(targetNumber)
		for _, value := range data.FloatData.GetData() {
			if value == targetValue {
				return true, nil
			}
		}
	case *schemapb.ScalarField_DoubleData:
		targetNumber, ok := genericNumericValue(target)
		if !ok {
			return false, nil
		}
		for _, value := range data.DoubleData.GetData() {
			if value == targetNumber {
				return true, nil
			}
		}
	case *schemapb.ScalarField_StringData:
		targetValue, ok := target.GetVal().(*planpb.GenericValue_StringVal)
		if !ok {
			return false, nil
		}
		for _, value := range data.StringData.GetData() {
			if value == targetValue.StringVal {
				return true, nil
			}
		}
	default:
		return false, merr.WrapErrServiceInternalMsg("unsupported RLS array element type %T", data)
	}
	return false, nil
}
