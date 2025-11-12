package rewriter

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func columnKey(c *planpb.ColumnInfo) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%d|%d|%d|%t|%t|%t|",
		c.GetFieldId(),
		int32(c.GetDataType()),
		int32(c.GetElementType()),
		c.GetIsPrimaryKey(),
		c.GetIsAutoID(),
		c.GetIsPartitionKey(),
	))
	for _, p := range c.GetNestedPath() {
		b.WriteString(p)
		b.WriteByte('|')
	}
	return b.String()
}

// effectiveDataType returns the real scalar type to be used for comparisons.
// For JSON/Array columns with a concrete element_type, use element_type;
// otherwise fall back to the column data_type.
func effectiveDataType(c *planpb.ColumnInfo) schemapb.DataType {
	if c == nil {
		return schemapb.DataType_None
	}
	dt := c.GetDataType()
	if dt == schemapb.DataType_JSON || dt == schemapb.DataType_Array {
		et := c.GetElementType()
		// Treat 0 (None/Invalid) as not specified; otherwise use element type.
		if et != schemapb.DataType_None {
			return et
		}
	}
	return dt
}

func valueCase(v *planpb.GenericValue) string {
	switch v.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return "bool"
	case *planpb.GenericValue_Int64Val:
		return "int64"
	case *planpb.GenericValue_FloatVal:
		return "float"
	case *planpb.GenericValue_StringVal:
		return "string"
	case *planpb.GenericValue_ArrayVal:
		return "array"
	default:
		return "other"
	}
}

func valueCaseWithNil(v *planpb.GenericValue) string {
	if v == nil || v.GetVal() == nil {
		return "nil"
	}
	return valueCase(v)
}

func isNumericCase(k string) bool {
	return k == "int64" || k == "float"
}

func areComparableCases(a, b string) bool {
	if a == "nil" || b == "nil" {
		return false
	}
	if isNumericCase(a) && isNumericCase(b) {
		return true
	}
	return a == b && (a == "bool" || a == "string")
}

func isNumericType(dt schemapb.DataType) bool {
	if typeutil.IsBoolType(dt) || typeutil.IsStringType(dt) || typeutil.IsJSONType(dt) {
		return false
	}
	return typeutil.IsArithmetic(dt)
}

const defaultConvertOrToInNumericLimit = 150

func shouldMergeToIn(dt schemapb.DataType, count int) bool {
	if isNumericType(dt) {
		return count > defaultConvertOrToInNumericLimit
	}
	return count > 1
}

func sortTermValues(term *planpb.TermExpr) {
	if term == nil || len(term.GetValues()) <= 1 {
		return
	}
	term.Values = sortGenericValues(term.Values)
}

// sort and deduplicate a list of generic values.
func sortGenericValues(values []*planpb.GenericValue) []*planpb.GenericValue {
	if len(values) <= 1 {
		return values
	}
	var kind string
	for _, v := range values {
		if v == nil || v.GetVal() == nil {
			continue
		}
		kind = valueCase(v)
		if kind != "" && kind != "other" && kind != "array" {
			break
		}
	}
	switch kind {
	case "bool":
		sort.Slice(values, func(i, j int) bool {
			return !values[i].GetBoolVal() && values[j].GetBoolVal()
		})
		values = lo.UniqBy(values, func(v *planpb.GenericValue) bool { return v.GetBoolVal() })
	case "int64":
		sort.Slice(values, func(i, j int) bool {
			return values[i].GetInt64Val() < values[j].GetInt64Val()
		})
		values = lo.UniqBy(values, func(v *planpb.GenericValue) int64 { return v.GetInt64Val() })
	case "float":
		sort.Slice(values, func(i, j int) bool {
			return values[i].GetFloatVal() < values[j].GetFloatVal()
		})
		values = lo.UniqBy(values, func(v *planpb.GenericValue) float64 { return v.GetFloatVal() })
	case "string":
		sort.Slice(values, func(i, j int) bool {
			return values[i].GetStringVal() < values[j].GetStringVal()
		})
		values = lo.UniqBy(values, func(v *planpb.GenericValue) string { return v.GetStringVal() })
	}
	return values
}

func newTermExpr(col *planpb.ColumnInfo, values []*planpb.GenericValue) *planpb.Expr {
	values = sortGenericValues(values)
	return &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: col,
				Values:     values,
			},
		},
	}
}

func newUnaryRangeExpr(col *planpb.ColumnInfo, op planpb.OpType, val *planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         op,
				Value:      val,
			},
		},
	}
}

func newBoolConstExpr(v bool) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: &planpb.GenericValue{
					Val: &planpb.GenericValue_BoolVal{
						BoolVal: v,
					},
				},
			},
		},
	}
}

func newAlwaysTrueExpr() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_AlwaysTrueExpr{
			AlwaysTrueExpr: &planpb.AlwaysTrueExpr{},
		},
	}
}

func newAlwaysFalseExpr() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op:    planpb.UnaryExpr_Not,
				Child: newAlwaysTrueExpr(),
			},
		},
	}
}

// IsAlwaysTrueExpr checks if the expression is an AlwaysTrueExpr
func IsAlwaysTrueExpr(e *planpb.Expr) bool {
	if e == nil {
		return false
	}
	return e.GetAlwaysTrueExpr() != nil
}

func IsAlwaysFalseExpr(e *planpb.Expr) bool {
	if e == nil {
		return false
	}
	ue := e.GetUnaryExpr()
	if ue == nil || ue.GetOp() != planpb.UnaryExpr_Not {
		return false
	}
	return IsAlwaysTrueExpr(ue.GetChild())
}

// equalsGeneric compares two GenericValue by content (bool/int/float/string).
func equalsGeneric(a, b *planpb.GenericValue) bool {
	if a.GetVal() == nil || b.GetVal() == nil {
		return false
	}

	switch a.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		if _, ok := b.GetVal().(*planpb.GenericValue_BoolVal); ok {
			return a.GetBoolVal() == b.GetBoolVal()
		}
	case *planpb.GenericValue_Int64Val:
		if _, ok := b.GetVal().(*planpb.GenericValue_Int64Val); ok {
			return a.GetInt64Val() == b.GetInt64Val()
		}
	case *planpb.GenericValue_FloatVal:
		if _, ok := b.GetVal().(*planpb.GenericValue_FloatVal); ok {
			return a.GetFloatVal() == b.GetFloatVal()
		}
	case *planpb.GenericValue_StringVal:
		if _, ok := b.GetVal().(*planpb.GenericValue_StringVal); ok {
			return a.GetStringVal() == b.GetStringVal()
		}
	}
	return false
}

func satisfiesLower(dt schemapb.DataType, v, lower *planpb.GenericValue, inclusive bool) bool {
	c := cmpGeneric(dt, v, lower)
	if inclusive {
		return c >= 0
	}
	return c > 0
}

func satisfiesUpper(dt schemapb.DataType, v, upper *planpb.GenericValue, inclusive bool) bool {
	c := cmpGeneric(dt, v, upper)
	if inclusive {
		return c <= 0
	}
	return c < 0
}

func filterValuesByRange(dt schemapb.DataType, values []*planpb.GenericValue, lower *planpb.GenericValue, lowerInc bool, upper *planpb.GenericValue, upperInc bool) []*planpb.GenericValue {
	out := make([]*planpb.GenericValue, 0, len(values))
	for _, v := range values {
		pass := true
		if lower != nil && !satisfiesLower(dt, v, lower, lowerInc) {
			pass = false
		}
		if pass && upper != nil && !satisfiesUpper(dt, v, upper, upperInc) {
			pass = false
		}
		if pass {
			out = append(out, v)
		}
	}
	return sortGenericValues(out)
}

func unionValues(valuesA, valuesB []*planpb.GenericValue) []*planpb.GenericValue {
	all := append([]*planpb.GenericValue{}, valuesA...)
	all = append(all, valuesB...)
	return sortGenericValues(all)
}
