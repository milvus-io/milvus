package rewriter

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Only a quantifier's complete predicate may treat UNKNOWN as a miss.
// A NOT within the element expression still needs all three truth values.
func (v *visitor) rewriteMatchPredicate(e *planpb.Expr, foldZeroHits bool) *planpb.Expr {
	if u := e.GetUnaryExpr(); u != nil && u.GetOp() == planpb.UnaryExpr_Not {
		u.Child = v.rewriteMatchPredicate(u.GetChild(), false)
		return e
	}
	b := e.GetBinaryExpr()
	if b == nil || (b.GetOp() != planpb.BinaryExpr_LogicalAnd && b.GetOp() != planpb.BinaryExpr_LogicalOr) {
		return e
	}

	parts := flattenLogicalExpr(b)
	var comparisons, others []*planpb.Expr
	for i, p := range parts {
		p = v.rewriteMatchPredicate(p, false)
		parts[i] = p
		if isMatchScalarComparison(p.GetUnaryRangeExpr()) {
			comparisons = append(comparisons, p)
		} else {
			others = append(others, p)
		}
	}
	if len(comparisons) < 2 {
		return buildBalancedBinary(b.GetOp(), parts)
	}
	before := len(comparisons)
	if b.GetOp() == planpb.BinaryExpr_LogicalOr {
		comparisons = v.combineOrEqualsToIn(comparisons)
	} else {
		if foldZeroHits && conflictingMatchEqualities(comparisons) {
			return newAlwaysFalseExpr()
		}
		comparisons = combineAndRangePredicates(comparisons, func(*planpb.ColumnInfo) bool { return foldZeroHits })
	}
	if len(comparisons) == before {
		return buildBalancedBinary(b.GetOp(), parts)
	}
	return foldBinary(b.GetOp(), append(others, comparisons...))
}

func conflictingMatchEqualities(parts []*planpb.Expr) bool {
	first := make(map[int64]*planpb.GenericValue)
	for _, p := range parts {
		u := p.GetUnaryRangeExpr()
		if u.GetOp() != planpb.OpType_Equal {
			continue
		}
		key := u.GetColumnInfo().GetFieldId()
		if previous, ok := first[key]; ok && !equalsGeneric(previous, u.GetValue()) {
			return true
		}
		first[key] = u.GetValue()
	}
	return false
}

func isMatchScalarComparison(u *planpb.UnaryRangeExpr) bool {
	col := u.GetColumnInfo()
	if !col.GetIsElementLevel() || len(col.GetNestedPath()) != 0 || col.GetDataType() == schemapb.DataType_JSON {
		return false
	}
	switch u.GetOp() {
	case planpb.OpType_Equal, planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual, planpb.OpType_LessThan, planpb.OpType_LessEqual:
	default:
		return false
	}
	// Element plans may carry ARRAY storage type plus the scalar element type.
	// Group literals only if conversion to that scalar type preserves their value.
	dt := effectiveDataType(col)
	val := u.GetValue()
	switch x := val.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		switch dt {
		case schemapb.DataType_Int8:
			return int64(int8(x.Int64Val)) == x.Int64Val
		case schemapb.DataType_Int16:
			return int64(int16(x.Int64Val)) == x.Int64Val
		case schemapb.DataType_Int32:
			return int64(int32(x.Int64Val)) == x.Int64Val
		case schemapb.DataType_Int64:
			return true
		}
	case *planpb.GenericValue_FloatVal:
		return !math.IsNaN(x.FloatVal) && (dt == schemapb.DataType_Double ||
			dt == schemapb.DataType_Float && float64(float32(x.FloatVal)) == x.FloatVal)
	case *planpb.GenericValue_StringVal:
		return dt == schemapb.DataType_VarChar
	case *planpb.GenericValue_BoolVal:
		return dt == schemapb.DataType_Bool && u.GetOp() == planpb.OpType_Equal
	}
	return false
}
