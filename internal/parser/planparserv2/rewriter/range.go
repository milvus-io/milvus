package rewriter

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

type bound struct {
	value     *planpb.GenericValue
	inclusive bool
	isLower   bool
	exprIndex int
}

func isSupportedScalarForRange(dt schemapb.DataType) bool {
	switch dt {
	case schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar:
		return true
	default:
		return false
	}
}

// resolveEffectiveType returns (dt, ok) where ok indicates this column is eligible for range optimization.
// Eligible when the column is a supported scalar, or an array whose element type is a supported scalar.
func resolveEffectiveType(col *planpb.ColumnInfo) (schemapb.DataType, bool) {
	if col == nil {
		return schemapb.DataType_None, false
	}
	dt := col.GetDataType()
	if isSupportedScalarForRange(dt) {
		return dt, true
	}
	if dt == schemapb.DataType_Array {
		et := col.GetElementType()
		if isSupportedScalarForRange(et) {
			return et, true
		}
	}
	return schemapb.DataType_None, false
}

func valueMatchesType(dt schemapb.DataType, v *planpb.GenericValue) bool {
	if v == nil || v.GetVal() == nil {
		return false
	}
	switch dt {
	case schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64:
		_, ok := v.GetVal().(*planpb.GenericValue_Int64Val)
		return ok
	case schemapb.DataType_Float, schemapb.DataType_Double:
		// For float columns, accept both float and int literal values
		switch v.GetVal().(type) {
		case *planpb.GenericValue_FloatVal, *planpb.GenericValue_Int64Val:
			return true
		default:
			return false
		}
	case schemapb.DataType_VarChar:
		_, ok := v.GetVal().(*planpb.GenericValue_StringVal)
		return ok
	default:
		return false
	}
}

func (v *visitor) combineAndRangePredicates(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col       *planpb.ColumnInfo
		lowers    []bound
		uppers    []bound
	}
	groups := map[string]*group{}
	// exprs not eligible for range optimization
	others := []int{}
	isRangeOp := func(op planpb.OpType) bool {
		return op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual ||
			op == planpb.OpType_LessThan || op == planpb.OpType_LessEqual
	}
	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || !isRangeOp(u.GetOp()) || u.GetValue() == nil {
			others = append(others, idx)
			continue
		}
		col := u.GetColumnInfo()
		if col == nil {
			others = append(others, idx)
			continue
		}
		// Only optimize for supported types and matching value type
		if effDt, ok := resolveEffectiveType(col); !ok || !valueMatchesType(effDt, u.GetValue()) {
			others = append(others, idx)
			continue
		}
		key := columnKey(col)
		g, ok := groups[key]
		if !ok {
			g = &group{col: col}
			groups[key] = g
		}
		b := bound{
			value:     u.GetValue(),
			inclusive: u.GetOp() == planpb.OpType_GreaterEqual || u.GetOp() == planpb.OpType_LessEqual,
			isLower:   u.GetOp() == planpb.OpType_GreaterThan || u.GetOp() == planpb.OpType_GreaterEqual,
			exprIndex: idx,
		}
		if b.isLower {
			g.lowers = append(g.lowers, b)
		} else {
			g.uppers = append(g.uppers, b)
		}
	}
	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, idx := range others {
		out = append(out, parts[idx])
		used[idx] = true
	}
	for _, g := range groups {
		effDt, _ := resolveEffectiveType(g.col)
		var bestLower *bound
		for i := range g.lowers {
			if bestLower == nil || cmpGeneric(effDt, g.lowers[i].value, bestLower.value) > 0 ||
				(cmpGeneric(effDt, g.lowers[i].value, bestLower.value) == 0 && !g.lowers[i].inclusive && bestLower.inclusive) {
				b := g.lowers[i]
				bestLower = &b
			}
		}
		var bestUpper *bound
		for i := range g.uppers {
			if bestUpper == nil || cmpGeneric(effDt, g.uppers[i].value, bestUpper.value) < 0 ||
				(cmpGeneric(effDt, g.uppers[i].value, bestUpper.value) == 0 && !g.uppers[i].inclusive && bestUpper.inclusive) {
				b := g.uppers[i]
				bestUpper = &b
			}
		}
		if bestLower != nil && bestUpper != nil {
			for _, b := range g.lowers {
				used[b.exprIndex] = true
			}
			for _, b := range g.uppers {
				used[b.exprIndex] = true
			}
			out = append(out, newBinaryRangeExpr(g.col, bestLower.inclusive, bestUpper.inclusive, bestLower.value, bestUpper.value))
		} else if bestLower != nil {
			for _, b := range g.lowers {
				used[b.exprIndex] = true
			}
			op := planpb.OpType_GreaterThan
			if bestLower.inclusive {
				op = planpb.OpType_GreaterEqual
			}
			out = append(out, newUnaryRangeExpr(g.col, op, bestLower.value))
		} else if bestUpper != nil {
			for _, b := range g.uppers {
				used[b.exprIndex] = true
			}
			op := planpb.OpType_LessThan
			if bestUpper.inclusive {
				op = planpb.OpType_LessEqual
			}
			out = append(out, newUnaryRangeExpr(g.col, op, bestUpper.value))
		}
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

func (v *visitor) combineOrRangePredicates(parts []*planpb.Expr) []*planpb.Expr {
	type key struct {
		colKey  string
		isLower bool
	}
	type group struct {
		col     *planpb.ColumnInfo
		dirLower bool
		bounds  []bound
	}
	groups := map[key]*group{}
	others := []int{}
	isRangeOp := func(op planpb.OpType) bool {
		return op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual ||
			op == planpb.OpType_LessThan || op == planpb.OpType_LessEqual
	}
	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || !isRangeOp(u.GetOp()) || u.GetValue() == nil {
			others = append(others, idx)
			continue
		}
		col := u.GetColumnInfo()
		if col == nil {
			others = append(others, idx)
			continue
		}
		if effDt, ok := resolveEffectiveType(col); !ok || !valueMatchesType(effDt, u.GetValue()) {
			others = append(others, idx)
			continue
		}
		isLower := u.GetOp() == planpb.OpType_GreaterThan || u.GetOp() == planpb.OpType_GreaterEqual
		k := key{colKey: columnKey(col), isLower: isLower}
		g, ok := groups[k]
		if !ok {
			g = &group{col: col, dirLower: isLower}
			groups[k] = g
		}
		g.bounds = append(g.bounds, bound{
			value:     u.GetValue(),
			inclusive: u.GetOp() == planpb.OpType_GreaterEqual || u.GetOp() == planpb.OpType_LessEqual,
			isLower:   isLower,
			exprIndex: idx,
		})
	}
	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, idx := range others {
		out = append(out, parts[idx])
		used[idx] = true
	}
	for _, g := range groups {
		if len(g.bounds) <= 1 {
			continue
		}
		if g.dirLower {
			var best *bound
			for i := range g.bounds {
				effDt, _ := resolveEffectiveType(g.col)
				if best == nil || cmpGeneric(effDt, g.bounds[i].value, best.value) < 0 ||
					(cmpGeneric(effDt, g.bounds[i].value, best.value) == 0 && g.bounds[i].inclusive && !best.inclusive) {
					b := g.bounds[i]
					best = &b
				}
			}
			for _, b := range g.bounds {
				used[b.exprIndex] = true
			}
			op := planpb.OpType_GreaterThan
			if best.inclusive {
				op = planpb.OpType_GreaterEqual
			}
			out = append(out, newUnaryRangeExpr(g.col, op, best.value))
		} else {
			var best *bound
			for i := range g.bounds {
				effDt, _ := resolveEffectiveType(g.col)
				if best == nil || cmpGeneric(effDt, g.bounds[i].value, best.value) > 0 ||
					(cmpGeneric(effDt, g.bounds[i].value, best.value) == 0 && g.bounds[i].inclusive && !best.inclusive) {
					b := g.bounds[i]
					best = &b
				}
			}
			for _, b := range g.bounds {
				used[b.exprIndex] = true
			}
			op := planpb.OpType_LessThan
			if best.inclusive {
				op = planpb.OpType_LessEqual
			}
			out = append(out, newUnaryRangeExpr(g.col, op, best.value))
		}
	}
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}
	return out
}

func newBinaryRangeExpr(col *planpb.ColumnInfo, lowerInclusive bool, upperInclusive bool, lower *planpb.GenericValue, upper *planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:     col,
				LowerInclusive: lowerInclusive,
				UpperInclusive: upperInclusive,
				LowerValue:     lower,
				UpperValue:     upper,
			},
		},
	}
}

// -1 means a < b, 0 means a == b, 1 means a > b
func cmpGeneric(dt schemapb.DataType, a, b *planpb.GenericValue) int {
	switch dt {
	case schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64:
		ai, bi := a.GetInt64Val(), b.GetInt64Val()
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	case schemapb.DataType_Float, schemapb.DataType_Double:
		// Allow comparing int and float by promoting to float
		toFloat := func(g *planpb.GenericValue) float64 {
			switch g.GetVal().(type) {
			case *planpb.GenericValue_FloatVal:
				return g.GetFloatVal()
			case *planpb.GenericValue_Int64Val:
				return float64(g.GetInt64Val())
			default:
				// Should not happen due to gate; treat as 0 deterministically
				return 0
			}
		}
		af, bf := toFloat(a), toFloat(b)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	case schemapb.DataType_String,
		schemapb.DataType_VarChar:
		as, bs := a.GetStringVal(), b.GetStringVal()
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	default:
		// Unsupported types are not optimized; callers gate with resolveEffectiveType.
		return 0
	}
}


