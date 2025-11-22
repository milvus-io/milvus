package rewriter

import (
	"fmt"

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
						lower:         ure.GetValue(),
						lowerInc:      inc,
						upper:         nil,
						upperInc:      false,
						exprIndex:     idx,
						isBinaryRange: false,
					})
				} else {
					g.intervals = append(g.intervals, interval{
						lower:         nil,
						lowerInc:      false,
						upper:         ure.GetValue(),
						upperInc:      inc,
						exprIndex:     idx,
						isBinaryRange: false,
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

// combineOrBinaryRanges merges BinaryRangeExpr nodes with OR semantics (union if overlapping/adjacent).
// Also handles mixing BinaryRangeExpr with UnaryRangeExpr.
func (v *visitor) combineOrBinaryRanges(parts []*planpb.Expr) []*planpb.Expr {
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
					g.intervals = append(g.intervals, interval{
						lower:         ure.GetValue(),
						lowerInc:      inc,
						upper:         nil,
						upperInc:      false,
						exprIndex:     idx,
						isBinaryRange: false,
					})
				} else {
					g.intervals = append(g.intervals, interval{
						lower:         nil,
						lowerInc:      false,
						upper:         ure.GetValue(),
						upperInc:      inc,
						exprIndex:     idx,
						isBinaryRange: false,
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
		if len(g.intervals) == 0 {
			continue
		}
		if len(g.intervals) == 1 {
			// Single interval, keep as is
			continue
		}

		// For OR, try to merge overlapping/adjacent intervals
		// If any interval is unbounded on one side, check if it subsumes others
		// For simplicity, we'll handle the common cases:
		// 1. All bounded intervals: try to merge if overlapping/adjacent
		// 2. Mix of bounded/unbounded: merge unbounded with compatible bounds

		var hasUnboundedLower, hasUnboundedUpper bool
		var unboundedLowerVal *planpb.GenericValue
		var unboundedLowerInc bool
		var unboundedUpperVal *planpb.GenericValue
		var unboundedUpperInc bool

		// Check for unbounded intervals
		for _, iv := range g.intervals {
			if iv.lower != nil && iv.upper == nil {
				// Lower bound only (x > a)
				if !hasUnboundedLower {
					hasUnboundedLower = true
					unboundedLowerVal = iv.lower
					unboundedLowerInc = iv.lowerInc
				} else {
					// Multiple lower-only bounds: take weakest (minimum)
					c := cmpGeneric(g.effDt, iv.lower, unboundedLowerVal)
					if c < 0 || (c == 0 && iv.lowerInc && !unboundedLowerInc) {
						unboundedLowerVal = iv.lower
						unboundedLowerInc = iv.lowerInc
					}
				}
			}
			if iv.lower == nil && iv.upper != nil {
				// Upper bound only (x < b)
				if !hasUnboundedUpper {
					hasUnboundedUpper = true
					unboundedUpperVal = iv.upper
					unboundedUpperInc = iv.upperInc
				} else {
					// Multiple upper-only bounds: take weakest (maximum)
					c := cmpGeneric(g.effDt, iv.upper, unboundedUpperVal)
					if c > 0 || (c == 0 && iv.upperInc && !unboundedUpperInc) {
						unboundedUpperVal = iv.upper
						unboundedUpperInc = iv.upperInc
					}
				}
			}
		}

		// Case 1: Have both unbounded lower and upper → entire domain (always true, but we can't express that simply)
		// For now, keep them separate
		// Case 2: Have one unbounded side → merge with compatible bounded intervals
		// Case 3: All bounded → try to merge overlapping/adjacent

		if hasUnboundedLower && hasUnboundedUpper {
			// Both unbounded sides - this likely covers most values
			// Keep as separate predicates for now (more advanced merging could be done)
			continue
		}

		if hasUnboundedLower || hasUnboundedUpper {
			// Merge unbounded with bounded intervals where applicable
			// For unbounded lower (x > a): can merge with binary ranges that have compatible upper bounds
			// For unbounded upper (x < b): can merge with binary ranges that have compatible lower bounds
			// This is complex, so for now we'll keep it simple and just skip merging
			// In practice, unbounded intervals often dominate
			continue
		}

		// All bounded intervals: try to merge overlapping/adjacent ones
		// This requires sorting and checking overlap
		// For simplicity in this initial implementation, we'll check if there are exactly 2 intervals
		// and try to merge them if they overlap or are adjacent

		if len(g.intervals) == 2 {
			iv1, iv2 := g.intervals[0], g.intervals[1]
			if iv1.lower == nil || iv1.upper == nil || iv2.lower == nil || iv2.upper == nil {
				// One is not fully bounded, skip
				continue
			}

			// Check if they overlap or are adjacent
			// They overlap if: iv1.lower <= iv2.upper AND iv2.lower <= iv1.upper
			// They are adjacent if: iv1.upper == iv2.lower (or vice versa) with at least one inclusive

			// Determine order: which has smaller lower bound
			var first, second interval
			c := cmpGeneric(g.effDt, iv1.lower, iv2.lower)
			if c <= 0 {
				first, second = iv1, iv2
			} else {
				first, second = iv2, iv1
			}

			// Check if they can be merged
			// Overlap: first.upper >= second.lower
			cmpUpperLower := cmpGeneric(g.effDt, first.upper, second.lower)
			canMerge := false
			if cmpUpperLower > 0 {
				// Overlap
				canMerge = true
			} else if cmpUpperLower == 0 {
				// Adjacent: at least one bound must be inclusive
				if first.upperInc || second.lowerInc {
					canMerge = true
				}
			}

			if canMerge {
				// Merge: take min lower and max upper
				mergedLower := first.lower
				mergedLowerInc := first.lowerInc
				mergedUpper := second.upper
				mergedUpperInc := second.upperInc

				// Upper bound: take maximum
				cmpUppers := cmpGeneric(g.effDt, first.upper, second.upper)
				if cmpUppers > 0 {
					mergedUpper = first.upper
					mergedUpperInc = first.upperInc
				} else if cmpUppers == 0 {
					// Same value: prefer inclusive
					if first.upperInc {
						mergedUpperInc = true
					}
				}

				// Mark both as used
				used[first.exprIndex] = true
				used[second.exprIndex] = true

				// Emit merged interval
				out = append(out, newBinaryRangeExpr(g.col, mergedLowerInc, mergedUpperInc, mergedLower, mergedUpper))
			}
		}
		// For more than 2 intervals, we'd need more sophisticated merging logic
		// For now, we'll leave them separate
	}

	// Add unused parts
	for i := range parts {
		if !used[i] {
			out = append(out, parts[i])
		}
	}

	return out
}
