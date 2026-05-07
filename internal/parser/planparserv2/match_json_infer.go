package planparserv2

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

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

// isJsonMatchElementColumn reports whether a ColumnInfo references the JSON
// element being matched by the current MATCH_* call.
func isJsonMatchElementColumn(ci *planpb.ColumnInfo, mctx *matchContext) bool {
	if ci == nil || mctx == nil || mctx.sourceType != matchSourceJSON {
		return false
	}
	nestedPath := ci.GetNestedPath()
	return ci.GetIsElementLevel() &&
		ci.GetDataType() == schemapb.DataType_JSON &&
		ci.GetFieldId() == mctx.fieldID &&
		len(nestedPath) >= len(mctx.jsonPath) &&
		slices.Equal(nestedPath[:len(mctx.jsonPath)], mctx.jsonPath)
}

// collectJsonMatchLiteralTypes walks the predicate tree and collects the
// scalar types of every literal that sits opposite a JSON element-level
// column reference produced inside the current MATCH_*. Only well-formed
// column-vs-literal shapes contribute: UnaryRange, BinaryRange, Term,
// BinaryArithOpEvalRange.
func collectJsonMatchLiteralTypes(expr *planpb.Expr, mctx *matchContext, acc *[]schemapb.DataType) {
	if expr == nil {
		return
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		collectJsonMatchLiteralTypes(e.BinaryExpr.GetLeft(), mctx, acc)
		collectJsonMatchLiteralTypes(e.BinaryExpr.GetRight(), mctx, acc)
	case *planpb.Expr_UnaryExpr:
		collectJsonMatchLiteralTypes(e.UnaryExpr.GetChild(), mctx, acc)
	case *planpb.Expr_UnaryRangeExpr:
		if isJsonMatchElementColumn(e.UnaryRangeExpr.GetColumnInfo(), mctx) {
			if t := genericValueScalarType(e.UnaryRangeExpr.GetValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
		}
	case *planpb.Expr_BinaryRangeExpr:
		if isJsonMatchElementColumn(e.BinaryRangeExpr.GetColumnInfo(), mctx) {
			if t := genericValueScalarType(e.BinaryRangeExpr.GetLowerValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
			if t := genericValueScalarType(e.BinaryRangeExpr.GetUpperValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
		}
	case *planpb.Expr_TermExpr:
		if isJsonMatchElementColumn(e.TermExpr.GetColumnInfo(), mctx) {
			for _, v := range e.TermExpr.GetValues() {
				if t := genericValueScalarType(v); t != schemapb.DataType_None {
					*acc = append(*acc, t)
				}
			}
		}
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		if isJsonMatchElementColumn(e.BinaryArithOpEvalRangeExpr.GetColumnInfo(), mctx) {
			if t := genericValueScalarType(e.BinaryArithOpEvalRangeExpr.GetValue()); t != schemapb.DataType_None {
				*acc = append(*acc, t)
			}
		}
	}
}

// reduceJsonMatchElementType collapses a set of collected literal types into
// a single element scalar type, enforcing compatibility:
//   - all string → VarChar
//   - all bool   → Bool
//   - numeric only, any float present → Double; else Int64
//   - mixing families → error
//
// Returns error if types are incompatible or the set is empty.
func reduceJsonMatchElementType(types []schemapb.DataType) (schemapb.DataType, error) {
	if len(types) == 0 {
		return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg(
			"MATCH_* on JSON requires at least one typed literal comparison " +
				"against the element accessor ($)")
	}
	var hasBool, hasStr, hasInt, hasFloat bool
	for _, t := range types {
		switch t {
		case schemapb.DataType_Bool:
			hasBool = true
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			hasStr = true
		case schemapb.DataType_Int64:
			hasInt = true
		case schemapb.DataType_Double:
			hasFloat = true
		}
	}
	families := 0
	if hasBool {
		families++
	}
	if hasStr {
		families++
	}
	if hasInt || hasFloat {
		families++
	}
	if families != 1 {
		return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg(
			"MATCH_* on JSON has inconsistent literal types across element comparisons; " +
				"all literals must belong to a single scalar family (bool | string | numeric)")
	}
	switch {
	case hasBool:
		return schemapb.DataType_Bool, nil
	case hasStr:
		return schemapb.DataType_VarChar, nil
	case hasFloat:
		return schemapb.DataType_Double, nil
	default:
		return schemapb.DataType_Int64, nil
	}
}

// validateJsonMatchElementType runs the collect+reduce pass on a JSON
// MATCH_* predicate. It only validates; the inferred element_type is
// intentionally not written into the plan. C++ dispatch routes by rhs
// val_case and the JsonInvertedIndex's own T; ElementType on the $ column
// would be dead weight.
func validateJsonMatchElementType(predicate *planpb.Expr, mctx *matchContext) error {
	if mctx == nil || mctx.sourceType != matchSourceJSON {
		return nil
	}
	if predicate.GetIsTemplate() {
		return merr.WrapErrParameterInvalidMsg(
			"MATCH_* on JSON does not support template placeholders in predicate")
	}
	var types []schemapb.DataType
	collectJsonMatchLiteralTypes(predicate, mctx, &types)
	_, err := reduceJsonMatchElementType(types)
	return err
}
