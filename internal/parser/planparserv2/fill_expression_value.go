package planparserv2

import (
	"bytes"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MembershipPreflightBudget carries the membership-filter preflight state that
// must be shared by every expression in one request: the main predicate, each
// hybrid sub-request predicate, and each function-scorer filter.
//
// Scope matters. proxy.maxMembershipFilterPlanSize is the per-request ceiling,
// but plan construction parses one expression per sub-request plus one per
// scorer. Threading one budget through the whole request rejects repeated blob
// occurrences before materialization and avoids re-validating the same Roaring
// template once per scorer or hybrid sub-request.
//
// A nil *MembershipPreflightBudget is not shared state; callers that parse a
// standalone expression can pass nil and get single-expression scope.
type MembershipPreflightBudget struct {
	maxPlanSize           int64
	budgetInitialized     bool
	occurrenceBytes       int64
	aggregateDecodedBytes uint64

	// validated caches structural validation across parses within the request.
	// Keyed by template name, but only reused when the bytes are identical:
	// hybrid sub-requests carry independent template maps, so the same name can
	// legitimately refer to different blobs.
	validated map[string]validatedRoaringBitmapBlob
}

func NewMembershipPreflightBudget() *MembershipPreflightBudget {
	return &MembershipPreflightBudget{}
}

func (b *MembershipPreflightBudget) lookup(name string, blob []byte) (validatedRoaringBitmapBlob, bool) {
	if b == nil || b.validated == nil {
		return validatedRoaringBitmapBlob{}, false
	}
	cached, ok := b.validated[name]
	if !ok || !bytes.Equal(cached.blob, blob) {
		return validatedRoaringBitmapBlob{}, false
	}
	return cached, true
}

func (b *MembershipPreflightBudget) store(name string, validated validatedRoaringBitmapBlob) {
	if b == nil {
		return
	}
	if b.validated == nil {
		b.validated = make(map[string]validatedRoaringBitmapBlob)
	}
	b.validated[name] = validated
}

func FillExpressionValue(expr *planpb.Expr, templateValues map[string]*planpb.GenericValue) error {
	return FillExpressionValueWithBudget(expr, templateValues, nil)
}

// FillExpressionValueWithBudget is FillExpressionValue with an explicit
// request-scoped budget. Pass nil for single-expression scope.
func FillExpressionValueWithBudget(
	expr *planpb.Expr,
	templateValues map[string]*planpb.GenericValue,
	budget *MembershipPreflightBudget,
) error {
	return fillExpressionValueWithBudgetAndSchema(expr, templateValues, budget, nil)
}

// fillExpressionValueWithBudgetAndSchema preserves the public fill API for
// hand-assembled plans while allowing the parser path to recover a field name
// from the schema for fill-time membership diagnostics.
func fillExpressionValueWithBudgetAndSchema(
	expr *planpb.Expr,
	templateValues map[string]*planpb.GenericValue,
	budget *MembershipPreflightBudget,
	schema *typeutil.SchemaHelper,
) error {
	if budget == nil {
		budget = NewMembershipPreflightBudget()
	}
	ctx, err := preflightMembershipFilterValues(expr, templateValues, budget)
	if err != nil {
		return err
	}
	ctx.schema = schema
	return fillExpressionValue(expr, templateValues, ctx)
}

type fillExpressionContext struct {
	validatedRoaringBlobs map[string]validatedRoaringBitmapBlob
	schema                *typeutil.SchemaHelper
}

type roaringTemplateBlob struct {
	name string
	blob []byte
}

// validatedRoaringBlob returns the request-cached structural validation for a
// roaring template when the same bytes were already validated under this name,
// or validates them now. The cache is populated by
// preflightMembershipFilterValues, which runs before any materialization; a
// miss here re-validates as a belt-and-braces fallback rather than skipping an
// admission gate.
func (c *fillExpressionContext) validatedRoaringBlob(name string, blob []byte) (validatedRoaringBitmapBlob, error) {
	if cached, ok := c.validatedRoaringBlobs[name]; ok && bytes.Equal(cached.blob, blob) {
		return cached, nil
	}
	return validateRoaringBitmapBlob(blob)
}

func (c *fillExpressionContext) membershipFieldName(columnInfo *planpb.ColumnInfo) string {
	if c != nil && c.schema != nil {
		if field, err := c.schema.GetFieldFromID(columnInfo.GetFieldId()); err == nil {
			name := field.GetName()
			nestedPath := columnInfo.GetNestedPath()
			if field.GetIsDynamic() && len(nestedPath) != 0 {
				// An implicit dynamic key is represented by the $meta field ID
				// with the caller-visible key as the first nested-path component.
				name = nestedPath[0]
				nestedPath = nestedPath[1:]
			}
			for _, path := range nestedPath {
				name += fmt.Sprintf("[%q]", path)
			}
			return name
		}
	}
	return fmt.Sprintf("field ID %d", columnInfo.GetFieldId())
}

// preflightMembershipFilterValues charges every deferred blob occurrence
// before any Roaring body is validated or materialized. This makes a repeated
// reference to one template cheap to reject and validates each unique Roaring
// template exactly once.
func preflightMembershipFilterValues(
	expr *planpb.Expr,
	templateValues map[string]*planpb.GenericValue,
	budget *MembershipPreflightBudget,
) (*fillExpressionContext, error) {
	ctx := &fillExpressionContext{}
	if expr == nil || !expr.GetIsTemplate() {
		return ctx, nil
	}

	var seenRoaringTemplates map[string]struct{}
	var roaringOccurrenceCounts map[string]uint64
	var orderedRoaringTemplates []roaringTemplateBlob
	var preflightErr error
	walkExpr(expr, func(node *planpb.Expr) bool {
		call := node.GetCallExpr()
		if call == nil || !isMembershipFunctionName(call.GetFunctionName()) {
			return false
		}
		params := call.GetFunctionParameters()
		if len(params) != 2 || params[1] == nil || !params[1].GetIsTemplate() {
			return false
		}
		templateName := params[1].GetValueExpr().GetTemplateVariableName()
		if templateName == "" {
			return false
		}
		value, ok := templateValues[templateName]
		if !ok || value == nil {
			return false
		}
		blobValue, ok := value.GetVal().(*planpb.GenericValue_BytesVal)
		if !ok {
			return false
		}

		if !budget.budgetInitialized {
			budget.maxPlanSize = paramtable.Get().ProxyCfg.MaxMembershipFilterPlanSize.GetAsInt64()
			budget.budgetInitialized = true
		}

		// The kind is static for the explicit names; the unified
		// membership_match resolves it from the blob magic. A blob whose magic
		// cannot be resolved is left to the fill path, which reports it as a
		// canonical input error.
		kind, kindKnown := fixedMembershipKind(call.GetFunctionName())
		if !kindKnown {
			kind, _ = sniffMembershipKind(blobValue.BytesVal)
		}

		// Charge the BODY, matching the per-blob gate this early check mirrors:
		// both MBF1 and MRB1 allow their fixed 32-byte header on top of the
		// body budget, so a whole-blob charge would silently halve the usable
		// tier for a maximum-sized filter (a 64 MiB SBBF body arrives as
		// 64 MiB + 32 bytes). An unresolvable kind charges the full length,
		// conservatively.
		chargeBytes := int64(len(blobValue.BytesVal))
		switch kind {
		case membershipBloom:
			if chargeBytes > mbf1HeaderSize {
				chargeBytes -= mbf1HeaderSize
			}
		case membershipRoaring:
			if chargeBytes > roaringfilter.HeaderSize {
				chargeBytes -= roaringfilter.HeaderSize
			}
		}
		if budget.occurrenceBytes > budget.maxPlanSize || chargeBytes > budget.maxPlanSize-budget.occurrenceBytes {
			preflightErr = merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate membership-filter template bytes exceed proxy.maxMembershipFilterPlanSize before plan materialization: %d + %d > %d bytes",
				budget.occurrenceBytes, chargeBytes, budget.maxPlanSize))
			return true
		}
		budget.occurrenceBytes += chargeBytes

		if kind == membershipRoaring {
			if seenRoaringTemplates == nil {
				seenRoaringTemplates = make(map[string]struct{})
				roaringOccurrenceCounts = make(map[string]uint64)
			}
			roaringOccurrenceCounts[templateName]++
			if _, seen := seenRoaringTemplates[templateName]; !seen {
				seenRoaringTemplates[templateName] = struct{}{}
				orderedRoaringTemplates = append(orderedRoaringTemplates, roaringTemplateBlob{
					name: templateName,
					blob: blobValue.BytesVal,
				})
			}
		}
		return false
	})
	if preflightErr != nil {
		return nil, preflightErr
	}

	if len(orderedRoaringTemplates) > 0 {
		ctx.validatedRoaringBlobs = make(map[string]validatedRoaringBitmapBlob, len(orderedRoaringTemplates))
	}
	for _, template := range orderedRoaringTemplates {
		// Structural validation is a pure function of the bytes, so a blob
		// already validated earlier in this request (another sub-request, or
		// another scorer filter) does not need a second linear pass.
		validated, cached := budget.lookup(template.name, template.blob)
		if !cached {
			var err error
			validated, err = validateRoaringBitmapBlob(template.blob)
			if err != nil {
				return nil, err
			}
			budget.store(template.name, validated)
		}
		occurrences := roaringOccurrenceCounts[template.name]
		cost := validated.summary.EstimatedDecodedBytes
		remaining := uint64(0)
		if budget.aggregateDecodedBytes <= roaringfilter.MaxEstimatedDecodedBytes {
			remaining = roaringfilter.MaxEstimatedDecodedBytes - budget.aggregateDecodedBytes
		}
		if cost != 0 && occurrences > remaining/cost {
			return nil, merr.WrapErrParameterTooLarge(fmt.Sprintf(
				"aggregate roaring_match estimated decoded size exceeds maximum before plan materialization: %d + %d*%d > %d bytes",
				budget.aggregateDecodedBytes, occurrences, cost, roaringfilter.MaxEstimatedDecodedBytes))
		}
		budget.aggregateDecodedBytes += occurrences * cost
		ctx.validatedRoaringBlobs[template.name] = validated
	}
	return ctx, nil
}

func fillExpressionValue(
	expr *planpb.Expr,
	templateValues map[string]*planpb.GenericValue,
	ctx *fillExpressionContext,
) error {
	if !expr.GetIsTemplate() {
		return nil
	}

	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		return FillTermExpressionValue(e.TermExpr, templateValues)
	case *planpb.Expr_UnaryExpr:
		return fillExpressionValue(e.UnaryExpr.GetChild(), templateValues, ctx)
	case *planpb.Expr_BinaryExpr:
		if err := fillExpressionValue(e.BinaryExpr.GetLeft(), templateValues, ctx); err != nil {
			return err
		}
		if err := fillExpressionValue(e.BinaryExpr.GetRight(), templateValues, ctx); err != nil {
			return err
		}
		switch e.BinaryExpr.GetOp() {
		case planpb.BinaryExpr_LogicalOr:
			if hasBoolValue(e.BinaryExpr.GetLeft(), true) || hasBoolValue(e.BinaryExpr.GetRight(), true) {
				*expr = *alwaysTrueExpr()
			}
		case planpb.BinaryExpr_LogicalAnd:
			if hasBoolValue(e.BinaryExpr.GetLeft(), false) || hasBoolValue(e.BinaryExpr.GetRight(), false) {
				*expr = *alwaysFalseExpr()
			}
		}
		return nil
	case *planpb.Expr_UnaryRangeExpr:
		return FillUnaryRangeExpressionValue(e.UnaryRangeExpr, templateValues)
	case *planpb.Expr_BinaryRangeExpr:
		return FillBinaryRangeExpressionValue(e.BinaryRangeExpr, templateValues)
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		return FillBinaryArithOpEvalRangeExpressionValue(e.BinaryArithOpEvalRangeExpr, templateValues)
	case *planpb.Expr_BinaryArithExpr:
		if err := fillExpressionValue(e.BinaryArithExpr.GetLeft(), templateValues, ctx); err != nil {
			return err
		}
		return fillExpressionValue(e.BinaryArithExpr.GetRight(), templateValues, ctx)
	case *planpb.Expr_JsonContainsExpr:
		return FillJSONContainsExpressionValue(e.JsonContainsExpr, templateValues)
	case *planpb.Expr_RandomSampleExpr:
		return fillExpressionValue(expr.GetExpr().(*planpb.Expr_RandomSampleExpr).RandomSampleExpr.GetPredicate(), templateValues, ctx)
	case *planpb.Expr_GisfunctionFilterExpr:
		return FillGISFunctionFilterExpressionValue(e.GisfunctionFilterExpr, templateValues)
	case *planpb.Expr_ElementFilterExpr:
		if err := fillExpressionValue(e.ElementFilterExpr.GetElementExpr(), templateValues, ctx); err != nil {
			return err
		}
		if e.ElementFilterExpr.GetPredicate() != nil {
			return fillExpressionValue(e.ElementFilterExpr.GetPredicate(), templateValues, ctx)
		}
		return nil
	case *planpb.Expr_MatchExpr:
		return fillExpressionValue(e.MatchExpr.GetPredicate(), templateValues, ctx)
	case *planpb.Expr_CallExpr:
		// Only the deferred membership-filter calls carry IsTemplate today; once
		// the template value is known, the client-built blob is validated and
		// the call is materialized into its dedicated plan node here —
		// BloomFilterExpr for the bloom kind, RoaringFilterExpr for roaring,
		// selected by function name or blob magic (membership_match).
		if isMembershipFunctionName(e.CallExpr.GetFunctionName()) {
			return fillMembershipMatchExpressionValue(expr, e.CallExpr, templateValues, ctx)
		}
		return merr.WrapErrQueryPlanMsg("this expression no need to fill placeholder with expr type: %T", e)
	default:
		return merr.WrapErrQueryPlanMsg("this expression no need to fill placeholder with expr type: %T", e)
	}
}

func hasBoolValue(expr *planpb.Expr, target bool) bool {
	value := expr.GetValueExpr().GetValue()
	return IsBool(value) && value.GetBoolVal() == target
}

func FillTermExpressionValue(expr *planpb.TermExpr, templateValues map[string]*planpb.GenericValue) error {
	value, ok := templateValues[expr.GetTemplateVariableName()]
	if !ok && expr.GetValues() == nil {
		return merr.WrapErrQueryPlanMsg("the value of expression template variable name {%s} is not found", expr.GetTemplateVariableName())
	}

	if value == nil || value.GetArrayVal() == nil {
		return merr.WrapErrQueryPlanMsg("the value of term expression template variable {%s} is not array", expr.GetTemplateVariableName())
	}
	dataType := expr.GetColumnInfo().GetDataType()
	if typeutil.IsArrayType(dataType) {
		// Use element type if accessing array element
		if len(expr.GetColumnInfo().GetNestedPath()) != 0 || expr.GetColumnInfo().GetIsElementLevel() {
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

func isLikeMatchOp(op planpb.OpType) bool {
	switch op {
	case planpb.OpType_Match, planpb.OpType_PrefixMatch, planpb.OpType_PostfixMatch, planpb.OpType_InnerMatch:
		return true
	default:
		return false
	}
}

func isRegexMatchOp(op planpb.OpType) bool {
	return op == planpb.OpType_RegexMatch
}

func FillUnaryRangeExpressionValue(expr *planpb.UnaryRangeExpr, templateValues map[string]*planpb.GenericValue) error {
	value, ok := templateValues[expr.GetTemplateVariableName()]
	if !ok {
		return merr.WrapErrQueryPlanMsg("the value of expression template variable name {%s} is not found", expr.GetTemplateVariableName())
	}
	if value == nil {
		return merr.WrapErrQueryPlanMsg("the value of expression template variable {%s} is nil", expr.GetTemplateVariableName())
	}

	if isLikeMatchOp(expr.GetOp()) {
		if !IsString(value) {
			return merr.WrapErrQueryPlanMsg("the value of like expression template variable {%s} is not string", expr.GetTemplateVariableName())
		}
		op, operand, err := translatePatternMatch(value.GetStringVal())
		if err != nil {
			return err
		}
		expr.Op = op
		expr.Value = NewString(operand)
		return nil
	}

	if isRegexMatchOp(expr.GetOp()) {
		if !IsString(value) {
			return merr.WrapErrQueryPlanMsg("the value of regex expression template variable {%s} is not string", expr.GetTemplateVariableName())
		}
		op, operand, err := validateAndOptimizeRegexPattern(value.GetStringVal())
		if err != nil {
			return err
		}
		expr.Op = op
		expr.Value = NewString(operand)
		return nil
	}

	dataType := expr.GetColumnInfo().GetDataType()
	if typeutil.IsArrayType(dataType) {
		// Use element type if accessing array element
		if len(expr.GetColumnInfo().GetNestedPath()) != 0 || expr.GetColumnInfo().GetIsElementLevel() {
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

func FillGISFunctionFilterExpressionValue(expr *planpb.GISFunctionFilterExpr, templateValues map[string]*planpb.GenericValue) error {
	templateVariableName := expr.GetWktString()
	value, ok := templateValues[templateVariableName]
	if !ok {
		return merr.WrapErrQueryPlanMsg("the value of expression template variable name {%s} is not found", templateVariableName)
	}
	if value == nil || !IsString(value) {
		return merr.WrapErrQueryPlanMsg("the value of GIS WKT template variable {%s} is not string", templateVariableName)
	}

	wktString := value.GetStringVal()
	if expr.GetOp() == planpb.GISFunctionFilterExpr_DWithin {
		if err := checkValidPoint(wktString); err != nil {
			return err
		}
	} else {
		if err := checkValidWKT(wktString); err != nil {
			return err
		}
	}
	expr.WktString = wktString
	return nil
}

func FillBinaryRangeExpressionValue(expr *planpb.BinaryRangeExpr, templateValues map[string]*planpb.GenericValue) error {
	var ok bool
	dataType := expr.GetColumnInfo().GetDataType()
	// Use element type if accessing array element
	if typeutil.IsArrayType(dataType) && (len(expr.GetColumnInfo().GetNestedPath()) != 0 || expr.GetColumnInfo().GetIsElementLevel()) {
		dataType = expr.GetColumnInfo().GetElementType()
	}
	lowerValue := expr.GetLowerValue()
	if lowerValue == nil || expr.GetLowerTemplateVariableName() != "" {
		lowerValue, ok = templateValues[expr.GetLowerTemplateVariableName()]
		if !ok {
			return merr.WrapErrQueryPlanMsg("the lower value of expression template variable name {%s} is not found", expr.GetLowerTemplateVariableName())
		}
		castedLowerValue, err := castValue(dataType, lowerValue)
		if err != nil {
			return err
		}
		expr.LowerValue = castedLowerValue
		lowerValue = castedLowerValue
	}

	upperValue := expr.GetUpperValue()
	if upperValue == nil || expr.GetUpperTemplateVariableName() != "" {
		upperValue, ok = templateValues[expr.GetUpperTemplateVariableName()]
		if !ok {
			return merr.WrapErrQueryPlanMsg("the upper value of expression template variable name {%s} is not found", expr.GetUpperTemplateVariableName())
		}

		castedUpperValue, err := castValue(dataType, upperValue)
		if err != nil {
			return err
		}
		expr.UpperValue = castedUpperValue
		upperValue = castedUpperValue
	}

	return validateBinaryRangeBounds(lowerValue, upperValue, expr.GetLowerInclusive(), expr.GetUpperInclusive())
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
				return merr.WrapErrQueryPlanMsg("the right operand value of expression template variable name {%s} is not found", expr.GetOperandTemplateVariableName())
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
			return merr.WrapErrQueryPlanMsg("can not comparisons array directly")
		}

		dataType, err = getTargetType(lDataType, rDataType)
		if err != nil {
			return err
		}

		castedOperand, err := castValue(dataType, operand)
		if err != nil {
			return err
		}

		// Validate divisor for division/modulo operations
		if expr.ArithOp == planpb.ArithOpType_Div || expr.ArithOp == planpb.ArithOpType_Mod {
			if (IsInteger(castedOperand) && castedOperand.GetInt64Val() == 0) ||
				(IsFloating(castedOperand) && castedOperand.GetFloatVal() == 0) {
				return merr.WrapErrQueryPlanMsg("division or modulus by zero")
			}
		}

		// Validate the shift amount for shift operations. A templated amount
		// skips the plan-time [0, 64) guard in combineBinaryArithExpr (its value
		// is unknown at parse time), so it must be re-checked here once filled.
		// A negative or >= 64 amount is undefined behavior in the C++ executor.
		if expr.ArithOp == planpb.ArithOpType_Shl || expr.ArithOp == planpb.ArithOpType_Shr {
			if !IsInteger(castedOperand) || castedOperand.GetInt64Val() < 0 || castedOperand.GetInt64Val() >= 64 {
				// The amount is not echoed: it arrived through a template,
				// and a resolved template value must not reach an error.
				return merr.WrapErrQueryPlanMsg(
					"shift amount from an expression template must be an integer in range [0, 64)")
			}
		}

		expr.RightOperand = castedOperand
	}

	value := expr.GetValue()
	if expr.GetValue() == nil || expr.GetValueTemplateVariableName() != "" {
		value, ok = templateValues[expr.GetValueTemplateVariableName()]
		if !ok {
			return merr.WrapErrQueryPlanMsg("the value of expression template variable name {%s} is not found", expr.GetValueTemplateVariableName())
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
		return merr.WrapErrQueryPlanMsg("the value of expression template variable name {%s} is not found", expr.GetTemplateVariableName())
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
	expr.ElementsSameType = jsonContainsElementsSameType(expr.GetElements())
	return nil
}

func jsonContainsElementsSameType(elements []*planpb.GenericValue) bool {
	if len(elements) == 0 {
		return true
	}

	elementType := genericValueDataType(elements[0])
	if elementType == schemapb.DataType_None {
		return false
	}
	for _, element := range elements[1:] {
		if genericValueDataType(element) != elementType {
			return false
		}
	}
	return true
}

func genericValueDataType(value *planpb.GenericValue) schemapb.DataType {
	if value == nil {
		return schemapb.DataType_None
	}
	switch value.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return schemapb.DataType_Bool
	case *planpb.GenericValue_Int64Val:
		return schemapb.DataType_Int64
	case *planpb.GenericValue_FloatVal:
		return schemapb.DataType_Double
	case *planpb.GenericValue_StringVal:
		return schemapb.DataType_VarChar
	case *planpb.GenericValue_ArrayVal:
		return schemapb.DataType_Array
	default:
		return schemapb.DataType_None
	}
}
