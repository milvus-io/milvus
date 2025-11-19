package rewriter

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func (v *visitor) combineOrEqualsToIn(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col         *planpb.ColumnInfo
		values      []*planpb.GenericValue
		origIndices []int
		valCase     string
	}
	others := make([]*planpb.Expr, 0, len(parts))
	groups := make(map[string]*group)
	indexToExpr := parts
	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || u.GetOp() != planpb.OpType_Equal || u.GetValue() == nil {
			others = append(others, e)
			continue
		}
		col := u.GetColumnInfo()
		if col == nil {
			others = append(others, e)
			continue
		}
		key := columnKey(col)
		g, ok := groups[key]
		valCase := valueCase(u.GetValue())
		if !ok {
			g = &group{col: col, values: []*planpb.GenericValue{}, origIndices: []int{}, valCase: valCase}
			groups[key] = g
		}
		if g.valCase != valCase {
			others = append(others, e)
			continue
		}
		g.values = append(g.values, u.GetValue())
		g.origIndices = append(g.origIndices, idx)
	}
	out := make([]*planpb.Expr, 0, len(parts))
	out = append(out, others...)
	for _, g := range groups {
		if shouldMergeToIn(g.col.GetDataType(), len(g.values)) {
			g.values = sortGenericValues(g.values)
			out = append(out, newTermExpr(g.col, g.values))
		} else {
			for _, i := range g.origIndices {
				out = append(out, indexToExpr[i])
			}
		}
	}
	return out
}

func (v *visitor) combineAndNotEqualsToNotIn(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col         *planpb.ColumnInfo
		values      []*planpb.GenericValue
		origIndices []int
		valCase     string
	}
	others := make([]*planpb.Expr, 0, len(parts))
	groups := make(map[string]*group)
	indexToExpr := parts
	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || u.GetOp() != planpb.OpType_NotEqual || u.GetValue() == nil {
			others = append(others, e)
			continue
		}
		col := u.GetColumnInfo()
		if col == nil {
			others = append(others, e)
			continue
		}
		key := columnKey(col)
		g, ok := groups[key]
		valCase := valueCase(u.GetValue())
		if !ok {
			g = &group{col: col, values: []*planpb.GenericValue{}, origIndices: []int{}, valCase: valCase}
			groups[key] = g
		}
		if g.valCase != valCase {
			others = append(others, e)
			continue
		}
		g.values = append(g.values, u.GetValue())
		g.origIndices = append(g.origIndices, idx)
	}
	out := make([]*planpb.Expr, 0, len(parts))
	out = append(out, others...)
	for _, g := range groups {
		if shouldMergeToIn(g.col.GetDataType(), len(g.values)) {
			g.values = sortGenericValues(g.values)
			in := newTermExpr(g.col, g.values)
			out = append(out, notExpr(in))
		} else {
			for _, i := range g.origIndices {
				out = append(out, indexToExpr[i])
			}
		}
	}
	return out
}

func notExpr(child *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op:    planpb.UnaryExpr_Not,
				Child: child,
			},
		},
	}
}

// AND: (a IN S) AND (a = v) with v in S -> a = v
func (v *visitor) combineAndInWithEqual(parts []*planpb.Expr) []*planpb.Expr {
	type agg struct {
		termIdxs []int
		eqIdxs   []int
		term     *planpb.TermExpr
		eqValues []*planpb.GenericValue
		col      *planpb.ColumnInfo
	}
	groups := map[string]*agg{}
	others := []int{}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &agg{col: te.GetColumnInfo()}
			}
			g.termIdxs = append(g.termIdxs, idx)
			g.term = te
			groups[k] = g
			continue
		}
		if ue := e.GetUnaryRangeExpr(); ue != nil && ue.GetOp() == planpb.OpType_Equal && ue.GetValue() != nil && ue.GetColumnInfo() != nil {
			k := columnKey(ue.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &agg{col: ue.GetColumnInfo()}
			}
			g.eqIdxs = append(g.eqIdxs, idx)
			g.eqValues = append(g.eqValues, ue.GetValue())
			groups[k] = g
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
		if g.term == nil || len(g.eqIdxs) == 0 {
			continue
		}
		// Build set of eq values and check presence in term set.
		termVals := g.term.GetValues()
		eqUnique := []*planpb.GenericValue{}
		for _, ev := range g.eqValues {
			dup := false
			for _, u := range eqUnique {
				if equalsGeneric(u, ev) {
					dup = true
					break
				}
			}
			if !dup {
				eqUnique = append(eqUnique, ev)
			}
		}
		// If multiple different equals present, AND implies contradiction unless identical.
		if len(eqUnique) > 1 {
			for _, ti := range g.termIdxs {
				used[ti] = true
			}
			for _, ei := range g.eqIdxs {
				used[ei] = true
			}
			// emit constant false
			out = append(out, newAlwaysFalseExpr())
			continue
		}
		// Single equal value
		ev := eqUnique[0]
		inSet := false
		for _, tv := range termVals {
			if equalsGeneric(tv, ev) {
				inSet = true
				break
			}
		}
		for _, ti := range g.termIdxs {
			used[ti] = true
		}
		for _, ei := range g.eqIdxs {
			used[ei] = true
		}
		if inSet {
			// reduce to equality
			out = append(out, newUnaryRangeExpr(g.col, planpb.OpType_Equal, ev))
		} else {
			// contradiction -> false
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

// OR: (a IN S) OR (a = v) with v in S -> keep a IN S (drop equal)
// Optional extension (not enabled here): if v not in S, could union.
func (v *visitor) combineOrInWithEqual(parts []*planpb.Expr) []*planpb.Expr {
	type agg struct {
		termIdx int
		term    *planpb.TermExpr
		col     *planpb.ColumnInfo
		eqIdxs  []int
		eqVals  []*planpb.GenericValue
	}
	groups := map[string]*agg{}
	others := []int{}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &agg{col: te.GetColumnInfo()}
				groups[k] = g
			}
			g.termIdx = idx
			g.term = te
			continue
		}
		if ue := e.GetUnaryRangeExpr(); ue != nil && ue.GetOp() == planpb.OpType_Equal && ue.GetValue() != nil && ue.GetColumnInfo() != nil {
			k := columnKey(ue.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &agg{col: ue.GetColumnInfo()}
				groups[k] = g
			}
			g.eqIdxs = append(g.eqIdxs, idx)
			g.eqVals = append(g.eqVals, ue.GetValue())
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
		if g.term == nil || len(g.eqIdxs) == 0 {
			continue
		}
		// union all equal values into term set
		union := g.term.GetValues()
		for i, ev := range g.eqVals {
			union = append(union, ev)
			used[g.eqIdxs[i]] = true
		}
		union = sortGenericValues(union)
		used[g.termIdx] = true
		out = append(out, newTermExpr(g.col, union))
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// AND: (a IN S) AND (range) -> filter S by range
func (v *visitor) combineAndInWithRange(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col       *planpb.ColumnInfo
		termIdx   int
		term      *planpb.TermExpr
		lower     *planpb.GenericValue
		lowerInc  bool
		upper     *planpb.GenericValue
		upperInc  bool
		rangeIdxs []int
	}
	groups := map[string]*group{}
	others := []int{}
	isRange := func(op planpb.OpType) bool {
		return op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual || op == planpb.OpType_LessThan || op == planpb.OpType_LessEqual
	}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &group{col: te.GetColumnInfo()}
				groups[k] = g
			}
			g.term = te
			g.termIdx = idx
			continue
		}
		if ue := e.GetUnaryRangeExpr(); ue != nil && isRange(ue.GetOp()) && ue.GetValue() != nil && ue.GetColumnInfo() != nil {
			k := columnKey(ue.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &group{col: ue.GetColumnInfo()}
				groups[k] = g
			}
			if ue.GetOp() == planpb.OpType_GreaterThan || ue.GetOp() == planpb.OpType_GreaterEqual {
				if g.lower == nil || cmpGeneric(effectiveDataType(g.col), ue.GetValue(), g.lower) > 0 || (cmpGeneric(effectiveDataType(g.col), ue.GetValue(), g.lower) == 0 && ue.GetOp() == planpb.OpType_GreaterThan && g.lowerInc) {
					g.lower = ue.GetValue()
					g.lowerInc = ue.GetOp() == planpb.OpType_GreaterEqual
				}
			} else {
				if g.upper == nil || cmpGeneric(effectiveDataType(g.col), ue.GetValue(), g.upper) < 0 || (cmpGeneric(effectiveDataType(g.col), ue.GetValue(), g.upper) == 0 && ue.GetOp() == planpb.OpType_LessThan && g.upperInc) {
					g.upper = ue.GetValue()
					g.upperInc = ue.GetOp() == planpb.OpType_LessEqual
				}
			}
			g.rangeIdxs = append(g.rangeIdxs, idx)
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
		if g.term == nil || (g.lower == nil && g.upper == nil) {
			continue
		}
		// Skip optimization if any term value is not comparable with the provided bounds
		termVals := g.term.GetValues()
		comparable := true
		for _, tv := range termVals {
			if g.lower != nil {
				if !(areComparableCases(valueCaseWithNil(tv), valueCaseWithNil(g.lower)) || (isNumericCase(valueCaseWithNil(tv)) && isNumericCase(valueCaseWithNil(g.lower)))) {
					comparable = false
					break
				}
			}
			if comparable && g.upper != nil {
				if !(areComparableCases(valueCaseWithNil(tv), valueCaseWithNil(g.upper)) || (isNumericCase(valueCaseWithNil(tv)) && isNumericCase(valueCaseWithNil(g.upper)))) {
					comparable = false
					break
				}
			}
		}
		if !comparable {
			continue
		}
		filtered := filterValuesByRange(effectiveDataType(g.col), termVals, g.lower, g.lowerInc, g.upper, g.upperInc)
		used[g.termIdx] = true
		for _, ri := range g.rangeIdxs {
			used[ri] = true
		}
		if len(filtered) == 0 {
			// Empty IN list after filtering → AlwaysFalse
			out = append(out, newAlwaysFalseExpr())
		} else {
			out = append(out, newTermExpr(g.col, filtered))
		}
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// OR: (a IN S1) OR (a IN S2) -> a IN union(S1, S2)
func (v *visitor) combineOrInWithIn(parts []*planpb.Expr) []*planpb.Expr {
	type agg struct {
		col    *planpb.ColumnInfo
		idxs   []int
		values [][]*planpb.GenericValue
	}
	groups := map[string]*agg{}
	others := []int{}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &agg{col: te.GetColumnInfo()}
				groups[k] = g
			}
			g.idxs = append(g.idxs, idx)
			g.values = append(g.values, te.GetValues())
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
		if len(g.idxs) <= 1 {
			continue
		}
		union := []*planpb.GenericValue{}
		for _, vs := range g.values {
			union = append(union, vs...)
		}
		union = sortGenericValues(union)
		for _, i := range g.idxs {
			used[i] = true
		}
		out = append(out, newTermExpr(g.col, union))
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// AND: (a IN S1) AND (a IN S2) ... -> a IN intersection(S1, S2, ...)
func (v *visitor) combineAndInWithIn(parts []*planpb.Expr) []*planpb.Expr {
	type agg struct {
		col    *planpb.ColumnInfo
		idxs   []int
		values [][]*planpb.GenericValue
	}
	groups := map[string]*agg{}
	others := []int{}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &agg{col: te.GetColumnInfo()}
				groups[k] = g
			}
			g.idxs = append(g.idxs, idx)
			g.values = append(g.values, te.GetValues())
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
		if len(g.idxs) <= 1 {
			continue
		}
		// compute intersection; start from first set
		inter := make([]*planpb.GenericValue, 0, len(g.values[0]))
	outer:
		for _, v := range g.values[0] {
			// check in every other set
			ok := true
			for i := 1; i < len(g.values); i++ {
				found := false
				for _, w := range g.values[i] {
					if equalsGeneric(v, w) {
						found = true
						break
					}
				}
				if !found {
					continue outer
				}
			}
			if ok {
				inter = append(inter, v)
			}
		}
		for _, i := range g.idxs {
			used[i] = true
		}
		if len(inter) == 0 {
			out = append(out, newAlwaysFalseExpr())
		} else {
			out = append(out, newTermExpr(g.col, inter))
		}
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// AND: (a IN S) AND (a != d) -> remove d from S; empty -> false
func (v *visitor) combineAndInWithNotEqual(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col     *planpb.ColumnInfo
		termIdx int
		term    *planpb.TermExpr
		neqIdxs []int
		neqVals []*planpb.GenericValue
	}
	groups := map[string]*group{}
	others := []int{}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &group{col: te.GetColumnInfo()}
				groups[k] = g
			}
			g.term = te
			g.termIdx = idx
			continue
		}
		if ue := e.GetUnaryRangeExpr(); ue != nil && ue.GetOp() == planpb.OpType_NotEqual && ue.GetValue() != nil && ue.GetColumnInfo() != nil {
			k := columnKey(ue.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &group{col: ue.GetColumnInfo()}
				groups[k] = g
			}
			g.neqIdxs = append(g.neqIdxs, idx)
			g.neqVals = append(g.neqVals, ue.GetValue())
			continue
		}
		others = append(others, idx)
	}
	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, i := range others {
		out = append(out, parts[i])
		used[i] = true
	}
	for _, g := range groups {
		if g.term == nil || len(g.neqIdxs) == 0 {
			continue
		}
		filtered := []*planpb.GenericValue{}
		for _, tv := range g.term.GetValues() {
			excluded := false
			for _, dv := range g.neqVals {
				if equalsGeneric(tv, dv) {
					excluded = true
					break
				}
			}
			if !excluded {
				filtered = append(filtered, tv)
			}
		}
		used[g.termIdx] = true
		for _, ni := range g.neqIdxs {
			used[ni] = true
		}
		if len(filtered) == 0 {
			out = append(out, newAlwaysFalseExpr())
		} else {
			out = append(out, newTermExpr(g.col, filtered))
		}
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

// OR: (a IN S) OR (a != d) -> if d ∈ S then true else (a != d)
func (v *visitor) combineOrInWithNotEqual(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col     *planpb.ColumnInfo
		termIdx int
		term    *planpb.TermExpr
		neqIdxs []int
		neqVals []*planpb.GenericValue
	}
	groups := map[string]*group{}
	others := []int{}
	for idx, e := range parts {
		if te := e.GetTermExpr(); te != nil {
			k := columnKey(te.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &group{col: te.GetColumnInfo()}
				groups[k] = g
			}
			g.term = te
			g.termIdx = idx
			continue
		}
		if ue := e.GetUnaryRangeExpr(); ue != nil && ue.GetOp() == planpb.OpType_NotEqual && ue.GetValue() != nil && ue.GetColumnInfo() != nil {
			k := columnKey(ue.GetColumnInfo())
			g := groups[k]
			if g == nil {
				g = &group{col: ue.GetColumnInfo()}
				groups[k] = g
			}
			g.neqIdxs = append(g.neqIdxs, idx)
			g.neqVals = append(g.neqVals, ue.GetValue())
			continue
		}
		others = append(others, idx)
	}
	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, i := range others {
		out = append(out, parts[i])
		used[i] = true
	}
	for _, g := range groups {
		if g.term == nil || len(g.neqIdxs) == 0 {
			continue
		}
		// if any neq value is inside IN set -> true
		containsAny := false
		for _, dv := range g.neqVals {
			for _, tv := range g.term.GetValues() {
				if equalsGeneric(tv, dv) {
					containsAny = true
					break
				}
			}
			if containsAny {
				break
			}
		}
		if containsAny {
			used[g.termIdx] = true
			for _, ni := range g.neqIdxs {
				used[ni] = true
			}
			out = append(out, newBoolConstExpr(true))
		} else {
			// drop the IN; keep != as-is
			used[g.termIdx] = true
		}
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}
