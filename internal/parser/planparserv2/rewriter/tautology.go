package rewriter

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// combineOrComplementaryRanges detects complementary range pairs in OR.
// (a > 10) OR (a <= 10) → true (for non-nullable columns)
// (a >= 10) OR (a < 10) → true (for non-nullable columns)
// For nullable columns, NULL values evaluate to false for all comparisons,
// so we cannot simplify to AlwaysTrue.
func (v *visitor) combineOrComplementaryRanges(parts []*planpb.Expr) []*planpb.Expr {
	type rangeEntry struct {
		op    planpb.OpType
		value *planpb.GenericValue
		index int
		col   *planpb.ColumnInfo
	}
	type group struct {
		col     *planpb.ColumnInfo
		entries []rangeEntry
	}
	groups := map[string]*group{}
	others := []int{}

	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || u.GetColumnInfo() == nil || u.GetValue() == nil {
			others = append(others, idx)
			continue
		}
		op := u.GetOp()
		if op != planpb.OpType_GreaterThan && op != planpb.OpType_GreaterEqual &&
			op != planpb.OpType_LessThan && op != planpb.OpType_LessEqual {
			others = append(others, idx)
			continue
		}
		col := u.GetColumnInfo()
		// Skip nullable columns: NULL comparisons evaluate to false,
		// so (a > v) OR (a <= v) is NOT true when a is NULL.
		if col.GetNullable() {
			others = append(others, idx)
			continue
		}
		key := columnKey(col)
		g := groups[key]
		if g == nil {
			g = &group{col: col}
			groups[key] = g
		}
		g.entries = append(g.entries, rangeEntry{op: op, value: u.GetValue(), index: idx, col: col})
	}

	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, idx := range others {
		out = append(out, parts[idx])
		used[idx] = true
	}

	for _, g := range groups {
		if len(g.entries) < 2 {
			continue
		}
		dt := effectiveDataType(g.col)
		if !isSupportedScalarForRange(dt) {
			continue
		}

		// Check all pairs for complementary ranges
		foundTautology := false
		for i := 0; i < len(g.entries) && !foundTautology; i++ {
			for j := i + 1; j < len(g.entries) && !foundTautology; j++ {
				a, b := g.entries[i], g.entries[j]
				if cmpGeneric(dt, a.value, b.value) != 0 {
					continue
				}
				// Same value, check if they are complementary
				if isComplement(a.op, b.op) {
					foundTautology = true
					for _, e := range g.entries {
						used[e.index] = true
					}
					out = append(out, newAlwaysTrueExpr())
				}
			}
		}
	}

	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// isComplement returns true if op1 and op2 together cover all values at the same point.
// (> v, <= v), (>= v, < v), and their reverses.
func isComplement(a, b planpb.OpType) bool {
	switch a {
	case planpb.OpType_GreaterThan:
		return b == planpb.OpType_LessEqual
	case planpb.OpType_GreaterEqual:
		return b == planpb.OpType_LessThan
	case planpb.OpType_LessThan:
		return b == planpb.OpType_GreaterEqual
	case planpb.OpType_LessEqual:
		return b == planpb.OpType_GreaterThan
	}
	return false
}
