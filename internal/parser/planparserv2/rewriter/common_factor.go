package rewriter

import (
	"math"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Budgets for common-factor extraction. An OR chain that exceeds any of them
// is kept as produced by the same-column merges.
const (
	// maxCommonFactorBranches bounds the number of operands of one OR chain.
	maxCommonFactorBranches = 64
	// maxCommonFactorNodes bounds the AND nodes and conjuncts visited while
	// splitting the branches.
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
		conjuncts, ok := budget.flattenAnd(part)
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

// flattenAnd splits e into its AND conjuncts, charging every visited node to
// the budget.
func (b *commonFactorBudget) flattenAnd(e *planpb.Expr) ([]*planpb.Expr, bool) {
	conjuncts := make([]*planpb.Expr, 0, 4)
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
		if be := cur.GetBinaryExpr(); be != nil && be.GetOp() == planpb.BinaryExpr_LogicalAnd {
			stack = append(stack, be.GetRight(), be.GetLeft())
			continue
		}
		conjuncts = append(conjuncts, cur)
	}
	return conjuncts, true
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
