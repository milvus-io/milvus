package rewriter

import (
	"fmt"
	"sort"

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
// Eligible when the column is a supported scalar, or an array whose element type is a supported scalar,
// or a JSON field with a nested path (type will be determined from literal values).
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
	// JSON fields with nested paths are eligible; the effective type will be
	// determined from the literal value in the comparison.
	if dt == schemapb.DataType_JSON && len(col.GetNestedPath()) > 0 {
		// Return a placeholder type; actual type checking happens in resolveJSONEffectiveType
		return schemapb.DataType_JSON, true
	}
	return schemapb.DataType_None, false
}

// resolveJSONEffectiveType returns the effective type for a JSON field based on the literal value.
// Returns (type, ok) where ok is false if the value is not suitable for range optimization.
// For numeric types (int and float), we normalize to Double to allow mixing, similar to scalar Float/Double columns.
func resolveJSONEffectiveType(v *planpb.GenericValue) (schemapb.DataType, bool) {
	if v == nil || v.GetVal() == nil {
		return schemapb.DataType_None, false
	}
	switch v.GetVal().(type) {
	case *planpb.GenericValue_Int64Val:
		// Normalize int to Double for JSON to allow mixing with float literals
		return schemapb.DataType_Double, true
	case *planpb.GenericValue_FloatVal:
		return schemapb.DataType_Double, true
	case *planpb.GenericValue_StringVal:
		return schemapb.DataType_VarChar, true
	case *planpb.GenericValue_BoolVal:
		// Boolean comparisons don't have meaningful ranges
		return schemapb.DataType_None, false
	default:
		return schemapb.DataType_None, false
	}
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
	case schemapb.DataType_JSON:
		// For JSON, check if we can determine a valid type from the value
		_, ok := resolveJSONEffectiveType(v)
		return ok
	default:
		return false
	}
}

func (v *visitor) combineAndRangePredicates(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col    *planpb.ColumnInfo
		effDt  schemapb.DataType // effective type for comparison
		lowers []bound
		uppers []bound
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
		effDt, ok := resolveEffectiveType(col)
		if !ok || !valueMatchesType(effDt, u.GetValue()) {
			others = append(others, idx)
			continue
		}
		// For JSON fields, determine the actual effective type from the literal value
		if effDt == schemapb.DataType_JSON {
			var typeOk bool
			effDt, typeOk = resolveJSONEffectiveType(u.GetValue())
			if !typeOk {
				others = append(others, idx)
				continue
			}
		}
		// Group by column + effective type (for JSON, type depends on literal)
		key := columnKey(col) + fmt.Sprintf("|%d", effDt)
		g, ok := groups[key]
		if !ok {
			g = &group{col: col, effDt: effDt}
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
		// Use the effective type stored in the group
		var bestLower *bound
		for i := range g.lowers {
			if bestLower == nil || cmpGeneric(g.effDt, g.lowers[i].value, bestLower.value) > 0 ||
				(cmpGeneric(g.effDt, g.lowers[i].value, bestLower.value) == 0 && !g.lowers[i].inclusive && bestLower.inclusive) {
				b := g.lowers[i]
				bestLower = &b
			}
		}
		var bestUpper *bound
		for i := range g.uppers {
			if bestUpper == nil || cmpGeneric(g.effDt, g.uppers[i].value, bestUpper.value) < 0 ||
				(cmpGeneric(g.effDt, g.uppers[i].value, bestUpper.value) == 0 && !g.uppers[i].inclusive && bestUpper.inclusive) {
				b := g.uppers[i]
				bestUpper = &b
			}
		}
		if bestLower != nil && bestUpper != nil {
			// Check if the interval is valid (non-empty)
			c := cmpGeneric(g.effDt, bestLower.value, bestUpper.value)
			isEmpty := false
			if c > 0 {
				// lower > upper: always empty
				isEmpty = true
			} else if c == 0 {
				// lower == upper: only valid if both bounds are inclusive
				if !bestLower.inclusive || !bestUpper.inclusive {
					isEmpty = true
				}
			}

			for _, b := range g.lowers {
				used[b.exprIndex] = true
			}
			for _, b := range g.uppers {
				used[b.exprIndex] = true
			}

			if isEmpty {
				// Empty interval → constant false
				out = append(out, newAlwaysFalseExpr())
			} else {
				out = append(out, newBinaryRangeExpr(g.col, bestLower.inclusive, bestUpper.inclusive, bestLower.value, bestUpper.value))
			}
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
		effDt   schemapb.DataType // effective type for JSON fields
	}
	type group struct {
		col      *planpb.ColumnInfo
		effDt    schemapb.DataType
		dirLower bool
		bounds   []bound
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
		effDt, ok := resolveEffectiveType(col)
		if !ok || !valueMatchesType(effDt, u.GetValue()) {
			others = append(others, idx)
			continue
		}
		// For JSON fields, determine the actual effective type from the literal value
		if effDt == schemapb.DataType_JSON {
			var typeOk bool
			effDt, typeOk = resolveJSONEffectiveType(u.GetValue())
			if !typeOk {
				others = append(others, idx)
				continue
			}
		}
		isLower := u.GetOp() == planpb.OpType_GreaterThan || u.GetOp() == planpb.OpType_GreaterEqual
		k := key{colKey: columnKey(col), isLower: isLower, effDt: effDt}
		g, ok := groups[k]
		if !ok {
			g = &group{col: col, effDt: effDt, dirLower: isLower}
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
				// Use the effective type stored in the group
				if best == nil || cmpGeneric(g.effDt, g.bounds[i].value, best.value) < 0 ||
					(cmpGeneric(g.effDt, g.bounds[i].value, best.value) == 0 && g.bounds[i].inclusive && !best.inclusive) {
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
				// Use the effective type stored in the group
				if best == nil || cmpGeneric(g.effDt, g.bounds[i].value, best.value) > 0 ||
					(cmpGeneric(g.effDt, g.bounds[i].value, best.value) == 0 && g.bounds[i].inclusive && !best.inclusive) {
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

// combineAndBinaryRanges merges BinaryRangeExpr nodes with AND semantics (intersection).
// Also handles mixing BinaryRangeExpr with UnaryRangeExpr.
func (v *visitor) combineAndBinaryRanges(parts []*planpb.Expr) []*planpb.Expr {
	type interval struct {
		lower         *planpb.GenericValue
		lowerInc      bool
		upper         *planpb.GenericValue
		upperInc      bool
		exprIndex     int
		isBinaryRange bool
	}
	type group struct {
		col       *planpb.ColumnInfo
		effDt     schemapb.DataType
		intervals []interval
	}
	groups := map[string]*group{}
	others := []int{}

	for idx, e := range parts {
		// Try BinaryRangeExpr
		if bre := e.GetBinaryRangeExpr(); bre != nil {
			col := bre.GetColumnInfo()
			if col == nil {
				others = append(others, idx)
				continue
			}
			effDt, ok := resolveEffectiveType(col)
			if !ok {
				others = append(others, idx)
				continue
			}
			// For JSON, determine actual type from lower value
			if effDt == schemapb.DataType_JSON {
				var typeOk bool
				effDt, typeOk = resolveJSONEffectiveType(bre.GetLowerValue())
				if !typeOk {
					others = append(others, idx)
					continue
				}
			}
			key := columnKey(col) + fmt.Sprintf("|%d", effDt)
			g, exists := groups[key]
			if !exists {
				g = &group{col: col, effDt: effDt}
				groups[key] = g
			}
			g.intervals = append(g.intervals, interval{
				lower:         bre.GetLowerValue(),
				lowerInc:      bre.GetLowerInclusive(),
				upper:         bre.GetUpperValue(),
				upperInc:      bre.GetUpperInclusive(),
				exprIndex:     idx,
				isBinaryRange: true,
			})
			continue
		}

		// Try UnaryRangeExpr (range ops only)
		if ure := e.GetUnaryRangeExpr(); ure != nil {
			op := ure.GetOp()
			if op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual ||
				op == planpb.OpType_LessThan || op == planpb.OpType_LessEqual {
				col := ure.GetColumnInfo()
				if col == nil {
					others = append(others, idx)
					continue
				}
				effDt, ok := resolveEffectiveType(col)
				if !ok || !valueMatchesType(effDt, ure.GetValue()) {
					others = append(others, idx)
					continue
				}
				if effDt == schemapb.DataType_JSON {
					var typeOk bool
					effDt, typeOk = resolveJSONEffectiveType(ure.GetValue())
					if !typeOk {
						others = append(others, idx)
						continue
					}
				}
				key := columnKey(col) + fmt.Sprintf("|%d", effDt)
				g, exists := groups[key]
				if !exists {
					g = &group{col: col, effDt: effDt}
					groups[key] = g
				}
				isLower := op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual
				inc := op == planpb.OpType_GreaterEqual || op == planpb.OpType_LessEqual
				if isLower {
					g.intervals = append(g.intervals, interval{
						lower:     ure.GetValue(),
						lowerInc:  inc,
						upper:     nil,
						upperInc:  false,
						exprIndex: idx,
					})
				} else {
					g.intervals = append(g.intervals, interval{
						lower:     nil,
						lowerInc:  false,
						upper:     ure.GetValue(),
						upperInc:  inc,
						exprIndex: idx,
					})
				}
				continue
			}
		}

		// Not a range expr we can optimize
		others = append(others, idx)
	}

	used := make([]bool, len(parts))
	out := make([]*planpb.Expr, 0, len(parts))
	for _, idx := range others {
		out = append(out, parts[idx])
		used[idx] = true
	}

	for _, g := range groups {
		if len(g.intervals) == 0 {
			continue
		}
		if len(g.intervals) == 1 {
			// Single interval, keep as is
			continue
		}

		// Compute intersection: max lower, min upper
		var finalLower *planpb.GenericValue
		var finalLowerInc bool
		var finalUpper *planpb.GenericValue
		var finalUpperInc bool

		for _, iv := range g.intervals {
			if iv.lower != nil {
				if finalLower == nil {
					finalLower = iv.lower
					finalLowerInc = iv.lowerInc
				} else {
					c := cmpGeneric(g.effDt, iv.lower, finalLower)
					if c > 0 || (c == 0 && !iv.lowerInc) {
						finalLower = iv.lower
						finalLowerInc = iv.lowerInc
					}
				}
			}
			if iv.upper != nil {
				if finalUpper == nil {
					finalUpper = iv.upper
					finalUpperInc = iv.upperInc
				} else {
					c := cmpGeneric(g.effDt, iv.upper, finalUpper)
					if c < 0 || (c == 0 && !iv.upperInc) {
						finalUpper = iv.upper
						finalUpperInc = iv.upperInc
					}
				}
			}
		}

		// Check if intersection is empty
		if finalLower != nil && finalUpper != nil {
			c := cmpGeneric(g.effDt, finalLower, finalUpper)
			isEmpty := false
			if c > 0 {
				isEmpty = true
			} else if c == 0 {
				// Equal bounds: only valid if both inclusive
				if !finalLowerInc || !finalUpperInc {
					isEmpty = true
				}
			}
			if isEmpty {
				// Empty intersection → constant false
				for _, iv := range g.intervals {
					used[iv.exprIndex] = true
				}
				out = append(out, newAlwaysFalseExpr())
				continue
			}
		}

		// Mark all intervals as used
		for _, iv := range g.intervals {
			used[iv.exprIndex] = true
		}

		// Emit the merged interval
		if finalLower != nil && finalUpper != nil {
			out = append(out, newBinaryRangeExpr(g.col, finalLowerInc, finalUpperInc, finalLower, finalUpper))
		} else if finalLower != nil {
			op := planpb.OpType_GreaterThan
			if finalLowerInc {
				op = planpb.OpType_GreaterEqual
			}
			out = append(out, newUnaryRangeExpr(g.col, op, finalLower))
		} else if finalUpper != nil {
			op := planpb.OpType_LessThan
			if finalUpperInc {
				op = planpb.OpType_LessEqual
			}
			out = append(out, newUnaryRangeExpr(g.col, op, finalUpper))
		}
	}

	// Add unused parts
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}

	return out
}

// orInterval represents an interval in OR merging with original expression index.
type orInterval struct {
	lower     *planpb.GenericValue
	lowerInc  bool
	upper     *planpb.GenericValue
	upperInc  bool
	exprIndex int
}

// combineOrBinaryRanges merges BinaryRangeExpr nodes with OR semantics (union if overlapping/adjacent).
// Also handles mixing BinaryRangeExpr with UnaryRangeExpr.
// Supports N intervals (sort-and-sweep) and bounded+unbounded merging.
func (v *visitor) combineOrBinaryRanges(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col       *planpb.ColumnInfo
		effDt     schemapb.DataType
		intervals []orInterval
	}
	groups := map[string]*group{}
	others := []int{}

	for idx, e := range parts {
		// Try BinaryRangeExpr
		if bre := e.GetBinaryRangeExpr(); bre != nil {
			col := bre.GetColumnInfo()
			if col == nil {
				others = append(others, idx)
				continue
			}
			effDt, ok := resolveEffectiveType(col)
			if !ok {
				others = append(others, idx)
				continue
			}
			if effDt == schemapb.DataType_JSON {
				var typeOk bool
				effDt, typeOk = resolveJSONEffectiveType(bre.GetLowerValue())
				if !typeOk {
					others = append(others, idx)
					continue
				}
			}
			key := columnKey(col) + fmt.Sprintf("|%d", effDt)
			g, exists := groups[key]
			if !exists {
				g = &group{col: col, effDt: effDt}
				groups[key] = g
			}
			g.intervals = append(g.intervals, orInterval{
				lower:     bre.GetLowerValue(),
				lowerInc:  bre.GetLowerInclusive(),
				upper:     bre.GetUpperValue(),
				upperInc:  bre.GetUpperInclusive(),
				exprIndex: idx,
			})
			continue
		}

		// Try UnaryRangeExpr
		if ure := e.GetUnaryRangeExpr(); ure != nil {
			op := ure.GetOp()
			if op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual ||
				op == planpb.OpType_LessThan || op == planpb.OpType_LessEqual {
				col := ure.GetColumnInfo()
				if col == nil {
					others = append(others, idx)
					continue
				}
				effDt, ok := resolveEffectiveType(col)
				if !ok || !valueMatchesType(effDt, ure.GetValue()) {
					others = append(others, idx)
					continue
				}
				if effDt == schemapb.DataType_JSON {
					var typeOk bool
					effDt, typeOk = resolveJSONEffectiveType(ure.GetValue())
					if !typeOk {
						others = append(others, idx)
						continue
					}
				}
				key := columnKey(col) + fmt.Sprintf("|%d", effDt)
				g, exists := groups[key]
				if !exists {
					g = &group{col: col, effDt: effDt}
					groups[key] = g
				}
				isLower := op == planpb.OpType_GreaterThan || op == planpb.OpType_GreaterEqual
				inc := op == planpb.OpType_GreaterEqual || op == planpb.OpType_LessEqual
				if isLower {
					g.intervals = append(g.intervals, orInterval{
						lower:     ure.GetValue(),
						lowerInc:  inc,
						upper:     nil,
						upperInc:  false,
						exprIndex: idx,
					})
				} else {
					g.intervals = append(g.intervals, orInterval{
						lower:     nil,
						lowerInc:  false,
						upper:     ure.GetValue(),
						upperInc:  inc,
						exprIndex: idx,
					})
				}
				continue
			}
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
		if len(g.intervals) <= 1 {
			continue
		}

		merged := mergeOrIntervals(g.effDt, g.intervals)
		if merged == nil {
			continue
		}

		// Mark all original intervals as used
		for _, iv := range g.intervals {
			used[iv.exprIndex] = true
		}

		// Emit merged intervals
		for _, m := range merged {
			if m.lower == nil && m.upper == nil {
				// Unbounded on both sides shouldn't happen from merge, skip
				continue
			}
			if m.lower != nil && m.upper != nil {
				out = append(out, newBinaryRangeExpr(g.col, m.lowerInc, m.upperInc, m.lower, m.upper))
			} else if m.lower != nil {
				op := planpb.OpType_GreaterThan
				if m.lowerInc {
					op = planpb.OpType_GreaterEqual
				}
				out = append(out, newUnaryRangeExpr(g.col, op, m.lower))
			} else {
				op := planpb.OpType_LessThan
				if m.upperInc {
					op = planpb.OpType_LessEqual
				}
				out = append(out, newUnaryRangeExpr(g.col, op, m.upper))
			}
		}
	}

	// Add unused parts
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}

	return out
}

// mergedInterval is a result of OR interval merging.
type mergedInterval struct {
	lower    *planpb.GenericValue
	lowerInc bool
	upper    *planpb.GenericValue
	upperInc bool
}

// mergeOrIntervals performs a sort-and-sweep merge of OR intervals.
// Returns nil if no merging happened (callers should keep originals).
func mergeOrIntervals(dt schemapb.DataType, intervals []orInterval) []mergedInterval {
	if len(intervals) < 2 {
		return nil
	}

	// Separate into bounded, lower-only (x > a), and upper-only (x < b)
	type bounded struct {
		lower    *planpb.GenericValue
		lowerInc bool
		upper    *planpb.GenericValue
		upperInc bool
	}
	var boundedList []bounded
	var lowerOnlyList []bounded // lower set, upper nil
	var upperOnlyList []bounded // lower nil, upper set

	for _, iv := range intervals {
		b := bounded{
			lower: iv.lower, lowerInc: iv.lowerInc,
			upper: iv.upper, upperInc: iv.upperInc,
		}
		switch {
		case iv.lower != nil && iv.upper != nil:
			boundedList = append(boundedList, b)
		case iv.lower != nil && iv.upper == nil:
			lowerOnlyList = append(lowerOnlyList, b)
		case iv.lower == nil && iv.upper != nil:
			upperOnlyList = append(upperOnlyList, b)
		}
	}

	// Merge multiple lower-only into weakest: x > min(a1, a2, ...)
	var mergedLowerOnly *bounded
	if len(lowerOnlyList) > 0 {
		best := lowerOnlyList[0]
		for i := 1; i < len(lowerOnlyList); i++ {
			c := cmpGeneric(dt, lowerOnlyList[i].lower, best.lower)
			if c < 0 || (c == 0 && lowerOnlyList[i].lowerInc && !best.lowerInc) {
				best = lowerOnlyList[i]
			}
		}
		mergedLowerOnly = &best
	}

	// Merge multiple upper-only into weakest: x < max(b1, b2, ...)
	var mergedUpperOnly *bounded
	if len(upperOnlyList) > 0 {
		best := upperOnlyList[0]
		for i := 1; i < len(upperOnlyList); i++ {
			c := cmpGeneric(dt, upperOnlyList[i].upper, best.upper)
			if c > 0 || (c == 0 && upperOnlyList[i].upperInc && !best.upperInc) {
				best = upperOnlyList[i]
			}
		}
		mergedUpperOnly = &best
	}

	// Try to extend unbounded intervals by absorbing overlapping bounded intervals.
	// (x > a) OR (b < x < c) where b <= a → x > min(a, b) (if overlapping/adjacent)
	// (x < b) OR (a < x < c) where c >= b → x < max(b, c) (if overlapping/adjacent)

	if mergedLowerOnly != nil {
		// Absorb bounded intervals that overlap with the lower-only interval
		remaining := make([]bounded, 0, len(boundedList))
		for _, bnd := range boundedList {
			// The lower-only covers [a, +∞). The bounded covers (bnd.lower, bnd.upper).
			// They overlap/adjacent if bnd.upper >= a (considering inclusivity)
			cmpBndUpperVsLower := cmpGeneric(dt, bnd.upper, mergedLowerOnly.lower)
			overlaps := cmpBndUpperVsLower > 0 ||
				(cmpBndUpperVsLower == 0 && (bnd.upperInc || mergedLowerOnly.lowerInc))
			if overlaps {
				// Extend: lower bound = min(a, bnd.lower)
				c := cmpGeneric(dt, bnd.lower, mergedLowerOnly.lower)
				if c < 0 || (c == 0 && bnd.lowerInc && !mergedLowerOnly.lowerInc) {
					mergedLowerOnly.lower = bnd.lower
					mergedLowerOnly.lowerInc = bnd.lowerInc
				}
			} else {
				remaining = append(remaining, bnd)
			}
		}
		boundedList = remaining
	}

	if mergedUpperOnly != nil {
		remaining := make([]bounded, 0, len(boundedList))
		for _, bnd := range boundedList {
			// Upper-only covers (-∞, b). Bounded covers (bnd.lower, bnd.upper).
			// They overlap/adjacent if bnd.lower <= b (considering inclusivity)
			cmpBndLowerVsUpper := cmpGeneric(dt, bnd.lower, mergedUpperOnly.upper)
			overlaps := cmpBndLowerVsUpper < 0 ||
				(cmpBndLowerVsUpper == 0 && (bnd.lowerInc || mergedUpperOnly.upperInc))
			if overlaps {
				// Extend: upper bound = max(b, bnd.upper)
				c := cmpGeneric(dt, bnd.upper, mergedUpperOnly.upper)
				if c > 0 || (c == 0 && bnd.upperInc && !mergedUpperOnly.upperInc) {
					mergedUpperOnly.upper = bnd.upper
					mergedUpperOnly.upperInc = bnd.upperInc
				}
			} else {
				remaining = append(remaining, bnd)
			}
		}
		boundedList = remaining
	}

	// Note: if lower-only and upper-only together overlap, they form a tautology
	// for non-nullable columns. We leave that to combineOrComplementaryRanges.

	// Sort-and-sweep merge of remaining bounded intervals
	if len(boundedList) > 1 {
		// Sort by lower bound
		sort.Slice(boundedList, func(i, j int) bool {
			c := cmpGeneric(dt, boundedList[i].lower, boundedList[j].lower)
			if c != 0 {
				return c < 0
			}
			// Equal lower bounds: inclusive first (covers more)
			return boundedList[i].lowerInc && !boundedList[j].lowerInc
		})

		// Sweep merge
		merged := []bounded{boundedList[0]}
		for i := 1; i < len(boundedList); i++ {
			cur := &merged[len(merged)-1]
			next := boundedList[i]

			// Check overlap or adjacency
			cmpCurUpperNextLower := cmpGeneric(dt, cur.upper, next.lower)
			canMerge := cmpCurUpperNextLower > 0 ||
				(cmpCurUpperNextLower == 0 && (cur.upperInc || next.lowerInc))

			if canMerge {
				// Extend upper bound to max
				cmpUppers := cmpGeneric(dt, next.upper, cur.upper)
				if cmpUppers > 0 {
					cur.upper = next.upper
					cur.upperInc = next.upperInc
				} else if cmpUppers == 0 && next.upperInc {
					cur.upperInc = true
				}
			} else {
				merged = append(merged, next)
			}
		}
		boundedList = merged
	}

	// Also try to absorb remaining bounded intervals into unbounded ones (post-bounded-merge)
	if mergedLowerOnly != nil && len(boundedList) > 0 {
		remaining := make([]bounded, 0, len(boundedList))
		for _, bnd := range boundedList {
			cmpBndUpperVsLower := cmpGeneric(dt, bnd.upper, mergedLowerOnly.lower)
			overlaps := cmpBndUpperVsLower > 0 ||
				(cmpBndUpperVsLower == 0 && (bnd.upperInc || mergedLowerOnly.lowerInc))
			if overlaps {
				c := cmpGeneric(dt, bnd.lower, mergedLowerOnly.lower)
				if c < 0 || (c == 0 && bnd.lowerInc && !mergedLowerOnly.lowerInc) {
					mergedLowerOnly.lower = bnd.lower
					mergedLowerOnly.lowerInc = bnd.lowerInc
				}
			} else {
				remaining = append(remaining, bnd)
			}
		}
		boundedList = remaining
	}
	if mergedUpperOnly != nil && len(boundedList) > 0 {
		remaining := make([]bounded, 0, len(boundedList))
		for _, bnd := range boundedList {
			cmpBndLowerVsUpper := cmpGeneric(dt, bnd.lower, mergedUpperOnly.upper)
			overlaps := cmpBndLowerVsUpper < 0 ||
				(cmpBndLowerVsUpper == 0 && (bnd.lowerInc || mergedUpperOnly.upperInc))
			if overlaps {
				c := cmpGeneric(dt, bnd.upper, mergedUpperOnly.upper)
				if c > 0 || (c == 0 && bnd.upperInc && !mergedUpperOnly.upperInc) {
					mergedUpperOnly.upper = bnd.upper
					mergedUpperOnly.upperInc = bnd.upperInc
				}
			} else {
				remaining = append(remaining, bnd)
			}
		}
		boundedList = remaining
	}

	// Count total result intervals
	totalResults := len(boundedList)
	if mergedLowerOnly != nil {
		totalResults++
	}
	if mergedUpperOnly != nil {
		totalResults++
	}

	// Only return merged results if we actually reduced the count
	if totalResults >= len(intervals) {
		return nil
	}

	result := make([]mergedInterval, 0, totalResults)
	if mergedUpperOnly != nil {
		result = append(result, mergedInterval{
			upper: mergedUpperOnly.upper, upperInc: mergedUpperOnly.upperInc,
		})
	}
	for _, b := range boundedList {
		result = append(result, mergedInterval(b))
	}
	if mergedLowerOnly != nil {
		result = append(result, mergedInterval{
			lower: mergedLowerOnly.lower, lowerInc: mergedLowerOnly.lowerInc,
		})
	}

	return result
}
