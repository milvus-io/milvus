package rewriter

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// combineAndEqualWithEqual detects contradictory equalities on the same column.
// (a == 1) AND (a == 2) → false
// (a == 1) AND (a == 1) → a == 1
func (v *visitor) combineAndEqualWithEqual(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col         *planpb.ColumnInfo
		values      []*planpb.GenericValue
		origIndices []int
	}
	groups := map[string]*group{}
	others := []int{}

	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || u.GetOp() != planpb.OpType_Equal || u.GetValue() == nil {
			others = append(others, idx)
			continue
		}
		col := u.GetColumnInfo()
		if col == nil {
			others = append(others, idx)
			continue
		}
		key := columnKey(col)
		g, ok := groups[key]
		if !ok {
			g = &group{col: col}
			groups[key] = g
		}
		g.values = append(g.values, u.GetValue())
		g.origIndices = append(g.origIndices, idx)
	}

	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, idx := range others {
		out = append(out, parts[idx])
		used[idx] = true
	}

	for _, g := range groups {
		if len(g.origIndices) <= 1 {
			continue
		}
		// Check if all values are the same
		first := g.values[0]
		allSame := true
		for i := 1; i < len(g.values); i++ {
			if !equalsGeneric(first, g.values[i]) {
				allSame = false
				break
			}
		}
		for _, idx := range g.origIndices {
			used[idx] = true
		}
		if allSame {
			out = append(out, newUnaryRangeExpr(g.col, planpb.OpType_Equal, first))
		} else {
			out = append(out, newAlwaysFalseExpr())
		}
	}

	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// combineAndRangeWithEqual checks equality values against range bounds.
// (a > 10) AND (a == 5) → false (5 not > 10)
// (a > 10) AND (a == 15) → a == 15
// Also handles BinaryRangeExpr: (10 < a < 50) AND (a == 30) → a == 30
func (v *visitor) combineAndRangeWithEqual(parts []*planpb.Expr) []*planpb.Expr {
	type rangeIndex struct {
		lower    *planpb.GenericValue
		lowerInc bool
		upper    *planpb.GenericValue
		upperInc bool
		indices  []int
	}
	type eqEntry struct {
		value *planpb.GenericValue
		index int
	}
	type group struct {
		col    *planpb.ColumnInfo
		ranges []rangeIndex
		eqs    []eqEntry
	}

	groups := map[string]*group{}
	others := []int{}

	for idx, e := range parts {
		if u := e.GetUnaryRangeExpr(); u != nil && u.GetColumnInfo() != nil && u.GetValue() != nil {
			col := u.GetColumnInfo()
			key := columnKey(col)
			op := u.GetOp()

			if op == planpb.OpType_Equal {
				g := groups[key]
				if g == nil {
					g = &group{col: col}
					groups[key] = g
				}
				g.eqs = append(g.eqs, eqEntry{value: u.GetValue(), index: idx})
				continue
			}

			if op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual ||
				op == planpb.OpType_LessThan || op == planpb.OpType_LessEqual {
				g := groups[key]
				if g == nil {
					g = &group{col: col}
					groups[key] = g
				}
				ri := rangeIndex{indices: []int{idx}}
				isLower := op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual
				inc := op == planpb.OpType_GreaterEqual || op == planpb.OpType_LessEqual
				if isLower {
					ri.lower = u.GetValue()
					ri.lowerInc = inc
				} else {
					ri.upper = u.GetValue()
					ri.upperInc = inc
				}
				g.ranges = append(g.ranges, ri)
				continue
			}
		}

		if br := e.GetBinaryRangeExpr(); br != nil && br.GetColumnInfo() != nil {
			col := br.GetColumnInfo()
			key := columnKey(col)
			g := groups[key]
			if g == nil {
				g = &group{col: col}
				groups[key] = g
			}
			g.ranges = append(g.ranges, rangeIndex{
				lower:    br.GetLowerValue(),
				lowerInc: br.GetLowerInclusive(),
				upper:    br.GetUpperValue(),
				upperInc: br.GetUpperInclusive(),
				indices:  []int{idx},
			})
			continue
		}

		others = append(others, idx)
	}

	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, idx := range others {
		out = append(out, parts[idx])
		used[idx] = true
	}

	for _, g := range groups {
		if len(g.eqs) == 0 || len(g.ranges) == 0 {
			continue
		}

		dt := effectiveDataType(g.col)
		if !isSupportedScalarForRange(dt) {
			continue
		}

		// Collect tightest bounds from all ranges
		var bestLower *planpb.GenericValue
		var bestLowerInc bool
		var bestUpper *planpb.GenericValue
		var bestUpperInc bool

		for _, ri := range g.ranges {
			if ri.lower != nil {
				if bestLower == nil {
					bestLower = ri.lower
					bestLowerInc = ri.lowerInc
				} else {
					c := cmpGeneric(dt, ri.lower, bestLower)
					if c > 0 || (c == 0 && !ri.lowerInc && bestLowerInc) {
						bestLower = ri.lower
						bestLowerInc = ri.lowerInc
					}
				}
			}
			if ri.upper != nil {
				if bestUpper == nil {
					bestUpper = ri.upper
					bestUpperInc = ri.upperInc
				} else {
					c := cmpGeneric(dt, ri.upper, bestUpper)
					if c < 0 || (c == 0 && !ri.upperInc && bestUpperInc) {
						bestUpper = ri.upper
						bestUpperInc = ri.upperInc
					}
				}
			}
		}

		// Check each equality value against the tightest range
		contradiction := false
		for _, eq := range g.eqs {
			if bestLower != nil && !satisfiesLower(dt, eq.value, bestLower, bestLowerInc) {
				contradiction = true
				break
			}
			if bestUpper != nil && !satisfiesUpper(dt, eq.value, bestUpper, bestUpperInc) {
				contradiction = true
				break
			}
		}

		if contradiction {
			// Mark everything as used, emit false
			for _, eq := range g.eqs {
				used[eq.index] = true
			}
			for _, ri := range g.ranges {
				for _, idx := range ri.indices {
					used[idx] = true
				}
			}
			out = append(out, newAlwaysFalseExpr())
		} else {
			// Equality is stricter than range → drop range predicates, keep equalities
			for _, ri := range g.ranges {
				for _, idx := range ri.indices {
					used[idx] = true
				}
			}
			// Equalities are not used, will be appended from the unused loop
		}
	}

	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}
