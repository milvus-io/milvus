package planparserv2

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// jsonMatchContext carries the JSON field id and nested path of the JSON array
// target currently being matched by a MATCH_* call. It is the lightweight local
// equivalent of pr-49581's matchContext (which this clone does not have).
type jsonMatchContext struct {
	fieldID  int64
	jsonPath []string
}

// genericValueScalarType maps a GenericValue oneof payload to its scalar
// DataType. Returns DataType_None if the value is not a scalar literal (e.g.
// array) or is nil.
func genericValueScalarType(v *planpb.GenericValue) schemapb.DataType {
	if v == nil {
		return schemapb.DataType_None
	}
	switch v.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return schemapb.DataType_Bool
	case *planpb.GenericValue_Int64Val:
		return schemapb.DataType_Int64
	case *planpb.GenericValue_FloatVal:
		return schemapb.DataType_Double
	case *planpb.GenericValue_StringVal:
		return schemapb.DataType_VarChar
	}
	return schemapb.DataType_None
}

// isJSONMatchElementColumn reports whether a ColumnInfo references the JSON
// element being matched by the current MATCH_* call.
func isJSONMatchElementColumn(ci *planpb.ColumnInfo, mctx *jsonMatchContext) bool {
	if ci == nil || mctx == nil {
		return false
	}
	nestedPath := ci.GetNestedPath()
	return ci.GetIsElementLevel() &&
		ci.GetDataType() == schemapb.DataType_JSON &&
		ci.GetFieldId() == mctx.fieldID &&
		len(nestedPath) >= len(mctx.jsonPath) &&
		slices.Equal(nestedPath[:len(mctx.jsonPath)], mctx.jsonPath)
}

// collectJSONMatchLiteralTypes walks the predicate tree and collects the
// scalar types of every literal that sits opposite a JSON element-level
// column reference produced inside the current MATCH_*. Only well-formed
// column-vs-literal shapes contribute: UnaryRange, BinaryRange, Term,
// BinaryArithOpEvalRange.
func collectJSONMatchLiteralTypes(expr *planpb.Expr, mctx *jsonMatchContext, acc *[]schemapb.DataType) {
	if expr == nil {
		return
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		collectJSONMatchLiteralTypes(e.BinaryExpr.GetLeft(), mctx, acc)
		collectJSONMatchLiteralTypes(e.BinaryExpr.GetRight(), mctx, acc)
	case *planpb.Expr_UnaryExpr:
		collectJSONMatchLiteralTypes(e.UnaryExpr.GetChild(), mctx, acc)
	case *planpb.Expr_UnaryRangeExpr:
		if isJSONMatchElementColumn(e.UnaryRangeExpr.GetColumnInfo(), mctx) {
			if t := genericValueScalarType(e.UnaryRangeExpr.GetValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
		}
	case *planpb.Expr_BinaryRangeExpr:
		if isJSONMatchElementColumn(e.BinaryRangeExpr.GetColumnInfo(), mctx) {
			if t := genericValueScalarType(e.BinaryRangeExpr.GetLowerValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
			if t := genericValueScalarType(e.BinaryRangeExpr.GetUpperValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
		}
	case *planpb.Expr_TermExpr:
		if isJSONMatchElementColumn(e.TermExpr.GetColumnInfo(), mctx) {
			for _, v := range e.TermExpr.GetValues() {
				if t := genericValueScalarType(v); t != schemapb.DataType_None {
					*acc = append(*acc, t)
				}
			}
		}
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		if isJSONMatchElementColumn(e.BinaryArithOpEvalRangeExpr.GetColumnInfo(), mctx) {
			if t := genericValueScalarType(e.BinaryArithOpEvalRangeExpr.GetValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
		}
	}
}

// reduceJSONMatchElementType collapses a set of collected literal types into
// a single element scalar type, enforcing compatibility:
//   - all string → VarChar
//   - all bool   → Bool
//   - numeric only, any float present → Double; else Int64
//   - mixing families → error
//
// Returns error if types are incompatible or the set is empty.
func reduceJSONMatchElementType(types []schemapb.DataType) (schemapb.DataType, error) {
	if len(types) == 0 {
		return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg(
			"MATCH_* on JSON requires at least one typed literal comparison " +
				"against the element accessor ($)")
	}

	family := jsonMatchLiteralFamilyNone
	result := schemapb.DataType_None
	for _, t := range types {
		curFamily, curType := jsonMatchLiteralFamilyOf(t)
		if curFamily == jsonMatchLiteralFamilyNone {
			return schemapb.DataType_None, jsonMatchLiteralTypeError()
		}

		if family == jsonMatchLiteralFamilyNone {
			family = curFamily
			result = curType
			continue
		}

		if family != curFamily {
			return schemapb.DataType_None, jsonMatchLiteralTypeError()
		}

		if family == jsonMatchLiteralFamilyNumeric {
			if curType == schemapb.DataType_Double {
				result = schemapb.DataType_Double
			}
		}
	}
	return result, nil
}

type jsonMatchLiteralFamily int

const (
	jsonMatchLiteralFamilyNone jsonMatchLiteralFamily = iota
	jsonMatchLiteralFamilyBool
	jsonMatchLiteralFamilyString
	jsonMatchLiteralFamilyNumeric
)

func jsonMatchLiteralFamilyOf(t schemapb.DataType) (jsonMatchLiteralFamily, schemapb.DataType) {
	switch t {
	case schemapb.DataType_Bool:
		return jsonMatchLiteralFamilyBool, schemapb.DataType_Bool
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		return jsonMatchLiteralFamilyString, schemapb.DataType_VarChar
	case schemapb.DataType_Int64:
		return jsonMatchLiteralFamilyNumeric, schemapb.DataType_Int64
	case schemapb.DataType_Double:
		return jsonMatchLiteralFamilyNumeric, schemapb.DataType_Double
	default:
		return jsonMatchLiteralFamilyNone, schemapb.DataType_None
	}
}

func jsonMatchLiteralTypeError() error {
	return merr.WrapErrParameterInvalidMsg(
		"MATCH_* on JSON has inconsistent literal types across element comparisons; " +
			"all literals must belong to a single scalar family (bool | string | numeric)")
}

// validateJSONMatchElementType runs the collect+reduce pass on a JSON
// MATCH_* predicate. It only validates; the inferred element_type is
// intentionally not written into the plan. C++ dispatch routes by rhs
// val_case and the JsonInvertedIndex's own T; ElementType on the $ column
// would be dead weight.
//
// If the predicate still carries template placeholders (IsTemplate), literal
// types are unknown at visitor time, so validation is deferred: parseExprInner
// always substitutes template values via FillExpressionValue before the plan
// leaves the parser, and validateFilledJSONMatchExprs re-runs the check on the
// concrete values. A placeholder with no provided value is rejected by
// FillExpressionValue itself, so no unfilled predicate can slip through.
func validateJSONMatchElementType(predicate *planpb.Expr, mctx *jsonMatchContext) error {
	if mctx == nil {
		return nil
	}
	if predicate.GetIsTemplate() {
		// Deferred to validateFilledJSONMatchExprs after template substitution.
		return nil
	}
	var types []schemapb.DataType
	collectJSONMatchLiteralTypes(predicate, mctx, &types)
	_, err := reduceJSONMatchElementType(types)
	return err
}

// validateFilledJSONMatchExprs walks a plan expression after
// FillExpressionValue has substituted template values and re-runs the JSON
// MATCH_* element-type validation for predicates that carried placeholders at
// parse time (their IsTemplate flag stays set after filling, but the values
// are concrete now). Non-template predicates were already validated at
// visitor time and are skipped.
func validateFilledJSONMatchExprs(expr *planpb.Expr) error {
	if expr == nil {
		return nil
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		if err := validateFilledJSONMatchExprs(e.BinaryExpr.GetLeft()); err != nil {
			return err
		}
		return validateFilledJSONMatchExprs(e.BinaryExpr.GetRight())
	case *planpb.Expr_UnaryExpr:
		return validateFilledJSONMatchExprs(e.UnaryExpr.GetChild())
	case *planpb.Expr_RandomSampleExpr:
		return validateFilledJSONMatchExprs(e.RandomSampleExpr.GetPredicate())
	case *planpb.Expr_MatchExpr:
		column := e.MatchExpr.GetColumn()
		if column.GetDataType() != schemapb.DataType_JSON ||
			!e.MatchExpr.GetPredicate().GetIsTemplate() {
			return nil
		}
		var types []schemapb.DataType
		collectJSONMatchLiteralTypes(e.MatchExpr.GetPredicate(),
			&jsonMatchContext{fieldID: column.GetFieldId(), jsonPath: column.GetNestedPath()}, &types)
		_, err := reduceJSONMatchElementType(types)
		return err
	}
	return nil
}
