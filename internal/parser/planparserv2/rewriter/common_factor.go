package rewriter

import (
	"math"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Budgets for common-factor extraction. An OR or AND chain that exceeds any of
// them is kept as produced by the same-column merges.
const (
	// maxCommonFactorBranches bounds the number of operands of one OR chain
	// (or one AND chain).
	maxCommonFactorBranches = 64
	// maxCommonFactorNodes bounds the AND nodes and conjuncts visited while
	// splitting the branches (OR nodes and disjuncts for an AND chain).
	maxCommonFactorNodes = 1024
	// maxCommonFactorWork bounds the factor-key encoding work: one unit per
	// eligible conjunct plus one per IN value.
	maxCommonFactorWork = 1 << 14
)

type commonFactorBudget struct {
	nodes int
	work  int
}

// factorCommonOrPredicates extracts the predicates shared by every operand of
// an OR chain:
//
//	(F AND R1) OR (F AND R2) OR ...  →  F AND (R1 OR R2 OR ...)
//	F OR (F AND R)                   →  F
//
// parts are the visited, same-column-merged and constant-free operands of one
// OR chain. Each operand is split into its AND conjuncts; only comparisons,
// BinaryRangeExpr and IN on top-level scalar fields can be factors (see
// commonFactorKey). Both identities hold in three-valued logic, so the
// TRUE/FALSE/UNKNOWN result is unchanged for every row.
//
// It returns nil when no factor is shared or a budget is exceeded. The pass
// reuses the operand nodes in its output and does not modify them.
func (v *visitor) factorCommonOrPredicates(parts []*planpb.Expr) *planpb.Expr {
	if len(parts) < 2 || len(parts) > maxCommonFactorBranches {
		return nil
	}
	budget := &commonFactorBudget{nodes: maxCommonFactorNodes, work: maxCommonFactorWork}
	last := len(parts) - 1
	branches := make([][]*planpb.Expr, len(parts))
	keys := make([][]string, len(parts))
	// common maps a factor key to the last branch index that contained it, so
	// after branch i only keys with value i are shared by branches 0..i.
	var common map[string]int
	for i, part := range parts {
		conjuncts, ok := budget.flattenLogical(part, planpb.BinaryExpr_LogicalAnd)
		if !ok {
			return nil
		}
		branches[i] = conjuncts
		keys[i] = make([]string, len(conjuncts))
		shared := 0
		for j, c := range conjuncts {
			k := commonFactorKey(c, budget)
			if budget.work < 0 {
				return nil
			}
			keys[i][j] = k
			if k == "" {
				continue
			}
			if i == 0 {
				if common == nil {
					common = make(map[string]int, len(conjuncts))
				}
				if _, seen := common[k]; !seen {
					common[k] = 0
					shared++
				}
			} else if prev, ok := common[k]; ok && prev == i-1 {
				common[k] = i
				shared++
			}
		}
		if shared == 0 {
			return nil
		}
	}

	// Factors keep their first-branch order; a factor repeated inside a
	// branch is emitted once. Emitted keys are marked with last+1.
	factors := make([]*planpb.Expr, 0, len(branches[0]))
	for j, c := range branches[0] {
		if k := keys[0][j]; k != "" && common[k] == last {
			factors = append(factors, c)
			common[k] = last + 1
		}
	}

	residuals := make([]*planpb.Expr, 0, len(parts))
	for i, conjuncts := range branches {
		rest := make([]*planpb.Expr, 0, len(conjuncts))
		for j, c := range conjuncts {
			if k := keys[i][j]; k == "" || common[k] < last {
				rest = append(rest, c)
			}
		}
		if len(rest) == 0 {
			// The branch is exactly F, so F absorbs the OR: F OR (F AND R) = F.
			return v.mergeAndParts(factors)
		}
		// A single remaining conjunct is already normalized. A longer
		// remainder is merged again: removing a factor can change a
		// same-column group (for example leave a single IN), and the merge
		// fixes the operand order a second rewrite would produce.
		residual := rest[0]
		if len(rest) > 1 {
			residual = v.mergeAndParts(rest)
		}
		residuals = appendFlattened(residuals, residual, planpb.BinaryExpr_LogicalOr)
	}

	// The residual OR and the new AND are normalized with the same rules a
	// second rewrite would apply, which keeps the rewrite idempotent.
	residual := v.mergeOrParts(residuals)
	return v.mergeAndParts(appendFlattened(factors, residual, planpb.BinaryExpr_LogicalAnd))
}

// factorCommonAndPredicates is the dual of factorCommonOrPredicates. It
// extracts the predicates shared by several OR clauses of an AND chain:
//
//	(F OR R1) AND (F OR R2) AND X  →  (F OR (R1 AND R2)) AND X
//	F AND (F OR R)                 →  F
//
// parts are the visited, same-column-merged and constant-free operands of one
// AND chain. Each operand is split into its OR disjuncts; an operand that is
// not an OR is a clause with one disjunct. Factors are the same predicates as
// for the OR direction (see commonFactorKey). Unlike the OR direction, the
// clauses that do not contain the factor stay as they are: AND is associative
// and commutative, so grouping a subset of the conjuncts is exact. The group
// is formed around the factor contained in the most clauses (the first one in
// source order on a tie); the shared set F is every factor that all clauses
// of the group contain. Both identities hold in three-valued logic.
//
// One call forms one group and passes the new chain to mergeAndParts, which
// forms the next group, so the AND reaches the form a second rewrite would
// produce. Every group removes at least one operand, which bounds the
// recursion by the number of operands.
//
// It returns nil when no factor is shared by two clauses or a budget is
// exceeded. The pass reuses the operand nodes in its output and does not
// modify them.
func (v *visitor) factorCommonAndPredicates(parts []*planpb.Expr) *planpb.Expr {
	if len(parts) < 2 || len(parts) > maxCommonFactorBranches {
		return nil
	}
	budget := &commonFactorBudget{nodes: maxCommonFactorNodes, work: maxCommonFactorWork}
	clauses := make([][]*planpb.Expr, len(parts))
	keys := make([][]string, len(parts))
	// count is the number of clauses that contain a key; seen is the last
	// clause that counted it, so a key repeated in one clause counts once.
	type keyStat struct {
		count, seen, order int
	}
	stats := make(map[string]*keyStat)
	for i, part := range parts {
		disjuncts, ok := budget.flattenLogical(part, planpb.BinaryExpr_LogicalOr)
		if !ok {
			return nil
		}
		clauses[i] = disjuncts
		keys[i] = make([]string, len(disjuncts))
		for j, d := range disjuncts {
			k := commonFactorKey(d, budget)
			if budget.work < 0 {
				return nil
			}
			keys[i][j] = k
			if k == "" {
				continue
			}
			if s, ok := stats[k]; !ok {
				stats[k] = &keyStat{count: 1, seen: i, order: len(stats)}
			} else if s.seen != i {
				s.count++
				s.seen = i
			}
		}
	}
	pick, best := "", (*keyStat)(nil)
	for k, s := range stats {
		if s.count >= 2 && (best == nil || s.count > best.count || (s.count == best.count && s.order < best.order)) {
			pick, best = k, s
		}
	}
	if best == nil {
		return nil
	}

	group := make([]int, 0, best.count)
	for i := range clauses {
		for _, k := range keys[i] {
			if k == pick {
				group = append(group, i)
				break
			}
		}
	}
	// common maps a key to the last group position that contained it, so
	// after position g only keys with value g are shared by positions 0..g.
	last := len(group) - 1
	common := make(map[string]int, len(keys[group[0]]))
	for g, i := range group {
		for _, k := range keys[i] {
			if k == "" {
				continue
			}
			if g == 0 {
				if _, ok := common[k]; !ok {
					common[k] = 0
				}
			} else if prev, ok := common[k]; ok && prev == g-1 {
				common[k] = g
			}
		}
	}

	grouped := v.groupCommonClauses(parts, clauses, keys, group, common, last)
	out := make([]*planpb.Expr, 0, len(parts)-len(group)+1)
	for i, g := 0, 0; i < len(parts); i++ {
		if g < len(group) && group[g] == i {
			if g == 0 {
				out = appendFlattened(out, grouped, planpb.BinaryExpr_LogicalAnd)
			}
			g++
			continue
		}
		out = append(out, parts[i])
	}
	return v.mergeAndParts(out)
}

// groupCommonClauses rewrites the clauses of one group, whose shared factors
// are the keys k with common[k] == last, to F OR AND_i(R_i). A clause that is
// exactly F absorbs the group: F AND (F OR R) = F.
func (v *visitor) groupCommonClauses(parts []*planpb.Expr, clauses [][]*planpb.Expr, keys [][]string,
	group []int, common map[string]int, last int,
) *planpb.Expr {
	// Factors keep their order in the first clause of the group; a factor
	// repeated inside a clause is emitted once. Emitted keys are marked with
	// last+1.
	first := group[0]
	factors := make([]*planpb.Expr, 0, len(clauses[first]))
	for j, d := range clauses[first] {
		if k := keys[first][j]; k != "" && common[k] == last {
			factors = append(factors, d)
			common[k] = last + 1
		}
	}

	residuals := make([]*planpb.Expr, 0, len(group))
	for _, i := range group {
		rest := make([]*planpb.Expr, 0, len(clauses[i]))
		for j, d := range clauses[i] {
			if k := keys[i][j]; k == "" || common[k] < last {
				rest = append(rest, d)
			}
		}
		if len(rest) == 0 {
			// The clause is exactly F, already in merged form.
			return parts[i]
		}
		// As in the OR direction, a longer remainder is merged again.
		residual := rest[0]
		if len(rest) > 1 {
			residual = v.mergeOrParts(rest)
		}
		residuals = appendFlattened(residuals, residual, planpb.BinaryExpr_LogicalAnd)
	}

	residual := v.mergeAndParts(residuals)
	return v.mergeOrParts(appendFlattened(factors, residual, planpb.BinaryExpr_LogicalOr))
}

// flattenLogical splits e into the operands of its op chain (AND conjuncts or
// OR disjuncts), charging every visited node to the budget.
func (b *commonFactorBudget) flattenLogical(e *planpb.Expr, op planpb.BinaryExpr_BinaryOp) ([]*planpb.Expr, bool) {
	operands := make([]*planpb.Expr, 0, 4)
	stack := []*planpb.Expr{e}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == nil {
			continue
		}
		b.nodes--
		if b.nodes < 0 {
			return nil, false
		}
		if be := cur.GetBinaryExpr(); be != nil && be.GetOp() == op {
			stack = append(stack, be.GetRight(), be.GetLeft())
			continue
		}
		operands = append(operands, cur)
	}
	return operands, true
}

// appendFlattened appends e to parts, splicing in the operands of a same-op
// chain so that the result matches what flattening the rebuilt tree yields.
func appendFlattened(parts []*planpb.Expr, e *planpb.Expr, op planpb.BinaryExpr_BinaryOp) []*planpb.Expr {
	if be := e.GetBinaryExpr(); be != nil && be.GetOp() == op {
		return append(parts, flattenLogicalExpr(be)...)
	}
	return append(parts, e)
}

// spliceLogicalParts flattens operands that became same-op chains after being
// visited (for example an OR whose common factors were extracted into an
// AND). It returns parts unchanged when there is none.
func spliceLogicalParts(parts []*planpb.Expr, op planpb.BinaryExpr_BinaryOp) []*planpb.Expr {
	for i, p := range parts {
		if be := p.GetBinaryExpr(); be != nil && be.GetOp() == op {
			out := make([]*planpb.Expr, 0, len(parts)+1)
			out = append(out, parts[:i]...)
			for _, q := range parts[i:] {
				out = appendFlattened(out, q, op)
			}
			return out
		}
	}
	return parts
}

// commonFactorKey returns a structural key for a predicate that may be
// factored out of an OR, or "" when it is not eligible. Two predicates with
// the same key evaluate identically on every row: the key covers the whole
// ColumnInfo, the operator, the inclusivity flags and every literal with its
// GenericValue kind, so int64 7 and float 7.0 differ. Template variable names
// are not part of the key; after binding, the literal is what is evaluated.
//
// Eligible are comparisons (==, !=, <, <=, >, >=), BinaryRangeExpr and IN on
// top-level scalar fields with concrete scalar literals. JSON paths, dynamic
// fields, array elements, element-level struct fields, pattern matching,
// NOT, NULL checks, MATCH and all other expressions are never factors.
func commonFactorKey(e *planpb.Expr, budget *commonFactorBudget) string {
	key := make([]byte, 0, 48)
	switch real := e.GetExpr().(type) {
	case *planpb.Expr_UnaryRangeExpr:
		expr := real.UnaryRangeExpr
		if !isCommonFactorOp(expr.GetOp()) || len(expr.GetExtraValues()) > 0 || !isCommonFactorColumn(expr.GetColumnInfo()) {
			return ""
		}
		budget.work--
		key = append(key, 'u')
		key = appendFactorColumn(key, expr.GetColumnInfo())
		key = strconv.AppendInt(key, int64(expr.GetOp()), 10)
		var ok bool
		if key, ok = appendFactorValue(key, expr.GetValue()); !ok {
			return ""
		}
	case *planpb.Expr_BinaryRangeExpr:
		expr := real.BinaryRangeExpr
		if !isCommonFactorColumn(expr.GetColumnInfo()) {
			return ""
		}
		budget.work--
		key = append(key, 'r')
		key = appendFactorColumn(key, expr.GetColumnInfo())
		key = strconv.AppendBool(key, expr.GetLowerInclusive())
		key = strconv.AppendBool(key, expr.GetUpperInclusive())
		var ok bool
		if key, ok = appendFactorValue(key, expr.GetLowerValue()); !ok {
			return ""
		}
		if key, ok = appendFactorValue(key, expr.GetUpperValue()); !ok {
			return ""
		}
	case *planpb.Expr_TermExpr:
		expr := real.TermExpr
		values := expr.GetValues()
		if expr.GetIsInField() || len(values) == 0 || !isCommonFactorColumn(expr.GetColumnInfo()) {
			return ""
		}
		budget.work -= 1 + len(values)
		if budget.work < 0 {
			return ""
		}
		key = append(key, 't')
		key = appendFactorColumn(key, expr.GetColumnInfo())
		key = strconv.AppendInt(key, int64(len(values)), 10)
		// Value lists are sorted and deduplicated before this pass, so equal
		// sets have equal lists.
		var ok bool
		for _, val := range values {
			if key, ok = appendFactorValue(key, val); !ok {
				return ""
			}
		}
	default:
		return ""
	}
	return string(key)
}

func isCommonFactorOp(op planpb.OpType) bool {
	switch op {
	case planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual,
		planpb.OpType_LessThan, planpb.OpType_LessEqual,
		planpb.OpType_Equal, planpb.OpType_NotEqual:
		return true
	default:
		return false
	}
}

// isCommonFactorColumn accepts ordinary top-level scalar fields only.
func isCommonFactorColumn(col *planpb.ColumnInfo) bool {
	if col == nil || len(col.GetNestedPath()) > 0 || col.GetIsElementLevel() {
		return false
	}
	switch col.GetDataType() {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double,
		schemapb.DataType_String, schemapb.DataType_VarChar:
		return true
	default:
		return false
	}
}

func appendFactorColumn(key []byte, col *planpb.ColumnInfo) []byte {
	key = append(key, '|')
	key = strconv.AppendInt(key, col.GetFieldId(), 10)
	key = append(key, ':')
	key = strconv.AppendInt(key, int64(col.GetDataType()), 10)
	key = append(key, ':')
	key = strconv.AppendInt(key, int64(col.GetElementType()), 10)
	key = append(key, ':')
	for _, flag := range []bool{
		col.GetNullable(), col.GetIsPrimaryKey(), col.GetIsAutoID(),
		col.GetIsPartitionKey(), col.GetIsClusteringKey(),
	} {
		if flag {
			key = append(key, '1')
		} else {
			key = append(key, '0')
		}
	}
	return append(key, '|')
}

// appendFactorValue encodes a scalar literal with its kind. Floats are encoded
// by bit pattern, so 0.0 and -0.0 are distinct keys (conservative).
func appendFactorValue(key []byte, val *planpb.GenericValue) ([]byte, bool) {
	switch v := val.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		key = append(key, 'b')
		key = strconv.AppendBool(key, v.BoolVal)
	case *planpb.GenericValue_Int64Val:
		key = append(key, 'i')
		key = strconv.AppendInt(key, v.Int64Val, 10)
	case *planpb.GenericValue_FloatVal:
		key = append(key, 'f')
		key = strconv.AppendUint(key, math.Float64bits(v.FloatVal), 16)
	case *planpb.GenericValue_StringVal:
		key = append(key, 's')
		key = strconv.AppendInt(key, int64(len(v.StringVal)), 10)
		key = append(key, ':')
		key = append(key, v.StringVal...)
	default:
		return key, false
	}
	return append(key, '|'), true
}
