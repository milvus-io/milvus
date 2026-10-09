package rewriter

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type scalarValueKey struct {
	kind uint8
	bits uint64
	text string
}

// Concrete keys avoid boxing every IN value. Keep equalsGeneric's literal-kind
// distinctions, normalize signed zero, and leave NaN unmatchable.
func genericScalarKey(value *planpb.GenericValue) (scalarValueKey, bool) {
	if value == nil {
		return scalarValueKey{}, false
	}
	switch v := value.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		key := scalarValueKey{kind: 1}
		if v.BoolVal {
			key.bits = 1
		}
		return key, true
	case *planpb.GenericValue_Int64Val:
		return scalarValueKey{kind: 2, bits: uint64(v.Int64Val)}, true
	case *planpb.GenericValue_FloatVal:
		if math.IsNaN(v.FloatVal) {
			return scalarValueKey{}, false
		}
		bits := uint64(0)
		if v.FloatVal != 0 {
			bits = math.Float64bits(v.FloatVal)
		}
		return scalarValueKey{kind: 3, bits: bits}, true
	case *planpb.GenericValue_StringVal:
		return scalarValueKey{kind: 4, text: v.StringVal}, true
	default:
		return scalarValueKey{}, false
	}
}

type inNotEqualGroup struct {
	col      *planpb.ColumnInfo
	termIdxs []int
	term     *planpb.TermExpr
	neqIdxs  []int
	neqVals  []*planpb.GenericValue
}

func collectInNotEqualGroups(parts []*planpb.Expr) map[string]*inNotEqualGroup {
	groups := make(map[string]*inNotEqualGroup)
	for i, part := range parts {
		if term := part.GetTermExpr(); term != nil && !term.GetIsInField() {
			key, ok := termGroupKey(term)
			if !ok {
				continue
			}
			group := groups[key]
			if group == nil {
				group = &inNotEqualGroup{col: term.GetColumnInfo()}
				groups[key] = group
			}
			group.termIdxs = append(group.termIdxs, i)
			group.term = term
		} else if unary := part.GetUnaryRangeExpr(); unary != nil && unary.GetOp() == planpb.OpType_NotEqual {
			key, ok := valueGroupKey(unary.GetColumnInfo(), unary.GetValue())
			if !ok {
				continue
			}
			group := groups[key]
			if group == nil {
				group = &inNotEqualGroup{col: unary.GetColumnInfo()}
				groups[key] = group
			}
			group.neqIdxs = append(group.neqIdxs, i)
			group.neqVals = append(group.neqVals, unary.GetValue())
		}
	}
	return groups
}

func canCombineInNotEqual(group *inNotEqualGroup) bool {
	// Nullable/missing-path intersections can leave multiple IN constraints.
	// Do not consume them while inspecting only the last one's values.
	if len(group.termIdxs) != 1 || len(group.neqIdxs) == 0 {
		return false
	}
	col := group.col
	if col.GetDataType() == schemapb.DataType_Float ||
		(col.GetIsElementLevel() && effectiveDataType(col) == schemapb.DataType_Float) {
		// Segcore narrows these literals to float32. Comparing their original
		// float64 values here could remove the wrong values or predicates.
		return false
	}
	kind := valueCase(group.term.GetValues()[0])
	for _, values := range [][]*planpb.GenericValue{group.term.GetValues(), group.neqVals} {
		for _, value := range values {
			if valueCaseWithNil(value) != kind || !canRewriteNotEqual(col, value) {
				return false
			}
			if kind == "float" && math.IsNaN(value.GetFloatVal()) {
				// In particular, JSON NaN comparisons cannot use this algebra.
				return false
			}
		}
	}
	return true
}

func (v *visitor) combineAndInWithNotEqual(parts []*planpb.Expr) []*planpb.Expr {
	return combineInNotEqual(parts, planpb.BinaryExpr_LogicalAnd)
}

func (v *visitor) combineOrInWithNotEqual(parts []*planpb.Expr) []*planpb.Expr {
	return combineInNotEqual(parts, planpb.BinaryExpr_LogicalOr)
}

// Build the exclusion set once and scan each IN list once: expected O(M+K)
// membership work for M IN values and K != values, using O(K) extra space.
func combineInNotEqual(parts []*planpb.Expr, op planpb.BinaryExpr_BinaryOp) []*planpb.Expr {
	groups := collectInNotEqualGroups(parts)
	removed := make([]bool, len(parts))
	replacements := make(map[int]*planpb.Expr)
	changed := false
	for _, group := range groups {
		if !canCombineInNotEqual(group) {
			continue
		}
		// A single != already needs only one comparison per IN value.
		// Avoid hashing overhead for that common case.
		single := len(group.neqVals) == 1
		var exclusions map[scalarValueKey]struct{}
		if !single {
			exclusions = make(map[scalarValueKey]struct{}, len(group.neqVals))
			for _, value := range group.neqVals {
				key, _ := genericScalarKey(value)
				exclusions[key] = struct{}{}
			}
		}
		termIdx := group.termIdxs[0]
		values := group.term.GetValues()
		if op == planpb.BinaryExpr_LogicalAnd {
			filtered := make([]*planpb.GenericValue, 0, len(values))
			for _, value := range values {
				excluded := false
				if single {
					excluded = equalsGeneric(value, group.neqVals[0])
				} else {
					key, _ := genericScalarKey(value)
					_, excluded = exclusions[key]
				}
				if !excluded {
					filtered = append(filtered, value)
				}
			}
			if len(filtered) == 0 && !canFoldPredicateToBoolConstant(group.col) {
				continue
			}
			if len(filtered) == 0 {
				replacements[termIdx] = newAlwaysFalseExpr()
			} else if len(filtered) != len(values) {
				replacements[termIdx] = newTermExpr(group.col, filtered)
			}
		} else {
			containsAny := false
			for _, value := range values {
				found := false
				if single {
					found = equalsGeneric(value, group.neqVals[0])
				} else {
					key, _ := genericScalarKey(value)
					_, found = exclusions[key]
				}
				if found {
					containsAny = true
					break
				}
			}
			if !containsAny {
				// IN implies every surviving !=, so it is redundant under OR.
				removed[termIdx] = true
				changed = true
				continue
			}
			if !canFoldPredicateToBoolConstant(group.col) {
				continue
			}
			replacements[termIdx] = newAlwaysTrueExpr()
		}
		for _, i := range group.neqIdxs {
			removed[i] = true
		}
		changed = true
	}
	if !changed {
		return parts
	}
	// Keep unaffected predicates in source order rather than map iteration order.
	result := make([]*planpb.Expr, 0, len(parts))
	for i, part := range parts {
		if replacement := replacements[i]; replacement != nil {
			result = append(result, replacement)
		} else if !removed[i] {
			result = append(result, part)
		}
	}
	return result
}
