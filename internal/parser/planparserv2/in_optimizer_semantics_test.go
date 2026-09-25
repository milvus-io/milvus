package planparserv2

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type inTruth uint8

const (
	inFalse inTruth = iota
	inUnknown
	inTrue
)

func inTruthOf(value bool) inTruth {
	if value {
		return inTrue
	}
	return inFalse
}

type inSemanticRow struct {
	value int64
	null  bool
}

// Evaluate only the numeric predicate nodes used by these tests, independently
// of the rewrite helpers. Visiting both branches also detects invalid raw
// ValueExpr predicates hidden behind a constant branch.
type inPlanEvaluator struct {
	fieldID  int64
	nullable bool
}

func (e inPlanEvaluator) column(column *planpb.ColumnInfo) error {
	if column == nil || column.GetFieldId() != e.fieldID || column.GetDataType() != schemapb.DataType_Int64 || column.GetNullable() != e.nullable {
		return fmt.Errorf("column identity or nullability changed: %v", column)
	}
	return nil
}

func inIntValue(value *planpb.GenericValue) (int64, error) {
	if v, ok := value.GetVal().(*planpb.GenericValue_Int64Val); ok {
		return v.Int64Val, nil
	}
	return 0, fmt.Errorf("expected integer value, got %v", value)
}

func inCompare(left, right int64, op planpb.OpType) (bool, error) {
	switch op {
	case planpb.OpType_Equal:
		return left == right, nil
	case planpb.OpType_NotEqual:
		return left != right, nil
	case planpb.OpType_GreaterThan:
		return left > right, nil
	case planpb.OpType_GreaterEqual:
		return left >= right, nil
	case planpb.OpType_LessThan:
		return left < right, nil
	case planpb.OpType_LessEqual:
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported comparison: %v", op)
	}
}

func (e inPlanEvaluator) eval(expr *planpb.Expr, row inSemanticRow) (inTruth, error) {
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return inTrue, nil
	case *planpb.Expr_UnaryExpr:
		if node.UnaryExpr.GetOp() != planpb.UnaryExpr_Not {
			return inFalse, fmt.Errorf("unsupported unary op: %v", node.UnaryExpr.GetOp())
		}
		value, err := e.eval(node.UnaryExpr.GetChild(), row)
		return inTrue - value, err
	case *planpb.Expr_BinaryExpr:
		left, err := e.eval(node.BinaryExpr.GetLeft(), row)
		if err != nil {
			return inFalse, err
		}
		right, err := e.eval(node.BinaryExpr.GetRight(), row)
		if err != nil {
			return inFalse, err
		}
		switch node.BinaryExpr.GetOp() {
		case planpb.BinaryExpr_LogicalAnd:
			return min(left, right), nil
		case planpb.BinaryExpr_LogicalOr:
			return max(left, right), nil
		default:
			return inFalse, fmt.Errorf("unsupported binary op: %v", node.BinaryExpr.GetOp())
		}
	case *planpb.Expr_UnaryRangeExpr:
		predicate := node.UnaryRangeExpr
		if err := e.column(predicate.GetColumnInfo()); err != nil {
			return inFalse, err
		}
		value, err := inIntValue(predicate.GetValue())
		if err != nil {
			return inFalse, err
		}
		if row.null {
			return inUnknown, nil
		}
		matched, err := inCompare(row.value, value, predicate.GetOp())
		return inTruthOf(matched), err
	case *planpb.Expr_TermExpr:
		predicate := node.TermExpr
		if err := e.column(predicate.GetColumnInfo()); err != nil {
			return inFalse, err
		}
		// Empty IN has separate normalization semantics; these tests generate
		// only nonempty lists and must not guess its NULL behavior.
		if predicate.GetIsInField() || len(predicate.GetValues()) == 0 {
			return inFalse, fmt.Errorf("unsupported empty or in-field term: %v", predicate)
		}
		matched := false
		for _, item := range predicate.GetValues() {
			value, err := inIntValue(item)
			if err != nil {
				return inFalse, err
			}
			matched = matched || row.value == value
		}
		if row.null {
			return inUnknown, nil
		}
		return inTruthOf(matched), nil
	case *planpb.Expr_BinaryRangeExpr:
		predicate := node.BinaryRangeExpr
		if err := e.column(predicate.GetColumnInfo()); err != nil {
			return inFalse, err
		}
		lower, err := inIntValue(predicate.GetLowerValue())
		if err != nil {
			return inFalse, err
		}
		upper, err := inIntValue(predicate.GetUpperValue())
		if err != nil {
			return inFalse, err
		}
		if row.null {
			return inUnknown, nil
		}
		return inTruthOf((row.value > lower || row.value == lower && predicate.GetLowerInclusive()) &&
			(row.value < upper || row.value == upper && predicate.GetUpperInclusive())), nil
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		predicate := node.BinaryArithOpEvalRangeExpr
		if err := e.column(predicate.GetColumnInfo()); err != nil {
			return inFalse, err
		}
		if predicate.GetArithOp() != planpb.ArithOpType_Add {
			return inFalse, fmt.Errorf("unsupported arithmetic op: %v", predicate.GetArithOp())
		}
		offset, err := inIntValue(predicate.GetRightOperand())
		if err != nil {
			return inFalse, err
		}
		value, err := inIntValue(predicate.GetValue())
		if err != nil {
			return inFalse, err
		}
		if row.null {
			return inUnknown, nil
		}
		matched, err := inCompare(row.value+offset, value, predicate.GetOp())
		return inTruthOf(matched), err
	default:
		return inFalse, fmt.Errorf("unsupported predicate node: %T", node)
	}
}

func TestINOptimizerMultipleTermsAndEqual(t *testing.T) {
	cases := []struct {
		name    string
		filter  string
		matches bool
	}{
		{"grouped", "Int64Field IN [-1,0] AND (Int64Field IN [1,2] AND Int64Field == {value})", false},
		{"reordered", "(Int64Field IN [1,2] AND Int64Field == {value}) AND Int64Field IN [-1,0]", false},
		{"flat", "Int64Field IN [-1,0] AND Int64Field IN [1,2] AND Int64Field == {value}", false},
		{"swapped_sets", "Int64Field IN [1,2] AND (Int64Field IN [-1,0] AND Int64Field == {value})", false},
		{"three_sets", "Int64Field IN [-1,0] AND (Int64Field IN [0,1,2] AND (Int64Field IN [1,2] AND Int64Field == {value}))", false},
		{"overlapping", "Int64Field IN [0,1] AND (Int64Field IN [1,2] AND Int64Field == {value})", true},
	}
	values := map[string]*schemapb.TemplateValue{"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}}}
	for _, nullable := range []bool{false, true} {
		helper := newTestSchemaHelper(t)
		field, err := helper.GetFieldFromName("Int64Field")
		require.NoError(t, err)
		field.Nullable = nullable
		evaluator := inPlanEvaluator{fieldID: field.GetFieldID(), nullable: nullable}
		for _, optimize := range []bool{false, true} {
			t.Run(fmt.Sprintf("nullable=%t/optimize=%t", nullable, optimize), func(t *testing.T) {
				old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
				t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
				for _, tc := range cases {
					for notCount := 0; notCount < 4; notCount++ {
						t.Run(fmt.Sprintf("%s/not=%d", tc.name, notCount), func(t *testing.T) {
							filter := strings.Repeat("NOT (", notCount) + tc.filter + strings.Repeat(")", notCount)
							expr, err := ParseExpr(helper, filter, values)
							require.NoError(t, err)
							rows := []inSemanticRow{{value: -2}, {value: -1}, {value: 0}, {value: 1}, {value: 2}, {value: 3}}
							if nullable {
								rows = append(rows, inSemanticRow{null: true})
							}
							for _, row := range rows {
								want := inTruthOf(tc.matches && row.value == 1)
								if row.null {
									want = inUnknown
								}
								if notCount%2 != 0 {
									want = inTrue - want
								}
								got, err := evaluator.eval(expr, row)
								require.NoError(t, err)
								require.Equal(t, want, got, "row=%+v filter=%s plan=%v", row, filter, expr)
							}
						})
					}
				}
			})
		}
	}
}

// The input oracle retains the source operations and never invokes the parser
// or optimizer. No literal true/false leaves absorb the branches under test.
type inSemanticNode struct {
	kind        string
	a, b        int64
	nots        int
	left, right *inSemanticNode
}

func newINSemanticNode(rng *rand.Rand, depth int) *inSemanticNode {
	if depth == 0 || rng.Intn(4) == 0 {
		kinds := []string{"eq", "ne", "in", "not_in", "range", "arith", "template"}
		a := int64(rng.Intn(6) - 3)
		return &inSemanticNode{kind: kinds[rng.Intn(len(kinds))], a: a, b: a + int64(rng.Intn(3)+1)}
	}
	if rng.Intn(4) == 0 {
		return &inSemanticNode{kind: "not", nots: 1 + rng.Intn(3), left: newINSemanticNode(rng, depth-1)}
	}
	return &inSemanticNode{kind: []string{"and", "or"}[rng.Intn(2)], left: newINSemanticNode(rng, depth-1), right: newINSemanticNode(rng, depth-1)}
}

func (n *inSemanticNode) filter() string {
	switch n.kind {
	case "eq":
		return fmt.Sprintf("Int64Field == %d", n.a)
	case "ne":
		return fmt.Sprintf("Int64Field != %d", n.a)
	case "in":
		return fmt.Sprintf("Int64Field IN [%d,%d]", n.a, n.b)
	case "not_in":
		return fmt.Sprintf("Int64Field NOT IN [%d,%d]", n.a, n.b)
	case "range":
		return fmt.Sprintf("%d < Int64Field <= %d", n.a, n.b)
	case "arith":
		return fmt.Sprintf("Int64Field + 1 == %d", n.a)
	case "template":
		return "Int64Field == {value}"
	case "not":
		return strings.Repeat("NOT (", n.nots) + n.left.filter() + strings.Repeat(")", n.nots)
	default:
		return "(" + n.left.filter() + " " + strings.ToUpper(n.kind) + " " + n.right.filter() + ")"
	}
}

func (n *inSemanticNode) expected(row inSemanticRow) inTruth {
	switch n.kind {
	case "and":
		return min(n.left.expected(row), n.right.expected(row))
	case "or":
		return max(n.left.expected(row), n.right.expected(row))
	case "not":
		value := n.left.expected(row)
		if n.nots%2 != 0 {
			return inTrue - value
		}
		return value
	}
	if row.null {
		return inUnknown
	}
	switch n.kind {
	case "eq":
		return inTruthOf(row.value == n.a)
	case "ne":
		return inTruthOf(row.value != n.a)
	case "in":
		return inTruthOf(row.value == n.a || row.value == n.b)
	case "not_in":
		return inTruthOf(row.value != n.a && row.value != n.b)
	case "range":
		return inTruthOf(row.value > n.a && row.value <= n.b)
	case "arith":
		return inTruthOf(row.value+1 == n.a)
	case "template":
		return inTruthOf(row.value == 1)
	default:
		panic("unknown input oracle node")
	}
}

func TestINOptimizerMixedThreeValuedSemantics(t *testing.T) {
	const seed = 53624
	helper := newTestSchemaHelper(t)
	field, err := helper.GetFieldFromName("Int64Field")
	require.NoError(t, err)
	field.Nullable = true
	evaluator := inPlanEvaluator{fieldID: field.GetFieldID(), nullable: true}
	rng := rand.New(rand.NewSource(seed))
	nodes := make([]*inSemanticNode, 1000)
	for i := range nodes {
		node := &inSemanticNode{kind: []string{"and", "or"}[rng.Intn(2)], left: newINSemanticNode(rng, 2), right: newINSemanticNode(rng, 3)}
		if i%3 == 0 {
			node = &inSemanticNode{kind: "not", nots: 1 + (i/3)%3, left: node}
		}
		nodes[i] = node
	}
	values := map[string]*schemapb.TemplateValue{"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}}}
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			failedPlans := 0
			for i, node := range nodes {
				filter := node.filter()
				expr, err := ParseExpr(helper, filter, values)
				if err != nil {
					failedPlans++
					if failedPlans <= 8 {
						t.Logf("seed=%d case=%d parse failed: %v filter=%s", seed, i, err, filter)
					}
					continue
				}
				for sample := 0; sample < 8; sample++ {
					row := inSemanticRow{value: int64(sample - 3), null: sample == 7}
					want := node.expected(row)
					got, err := evaluator.eval(expr, row)
					if err != nil || got != want {
						failedPlans++
						if failedPlans <= 8 {
							t.Logf("seed=%d case=%d row=%+v got=%d want=%d err=%v filter=%s plan=%v", seed, i, row, got, want, err, filter, expr)
						}
						break
					}
				}
			}
			require.Zero(t, failedPlans, "%d of %d plans violated three-valued semantics (seed=%d)", failedPlans, len(nodes), seed)
		})
	}
}
