package planparserv2

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func buildLogicalChain(n int, op, predicate string) string {
	var sb strings.Builder
	for i := 1; i <= n; i++ {
		if i > 1 {
			fmt.Fprintf(&sb, " %s ", op)
		}
		fmt.Fprintf(&sb, predicate, i)
	}
	return sb.String()
}

func logicalChainDepth(root *planpb.Expr) int {
	type nodeAtDepth struct {
		expr  *planpb.Expr
		depth int
	}
	stack := []nodeAtDepth{{root, 1}}
	maxDepth := 0
	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if node.expr == nil {
			continue
		}
		maxDepth = max(maxDepth, node.depth)
		var children []*planpb.Expr
		switch e := node.expr.GetExpr().(type) {
		case *planpb.Expr_BinaryExpr:
			children = []*planpb.Expr{e.BinaryExpr.Left, e.BinaryExpr.Right}
		case *planpb.Expr_UnaryExpr:
			children = []*planpb.Expr{e.UnaryExpr.Child}
		case *planpb.Expr_RandomSampleExpr:
			children = []*planpb.Expr{e.RandomSampleExpr.Predicate}
		case *planpb.Expr_ElementFilterExpr:
			children = []*planpb.Expr{e.ElementFilterExpr.Predicate, e.ElementFilterExpr.ElementExpr}
		case *planpb.Expr_MatchExpr:
			children = []*planpb.Expr{e.MatchExpr.Predicate}
		}
		for _, child := range children {
			stack = append(stack, nodeAtDepth{child, node.depth + 1})
		}
	}
	return maxDepth
}

func requireLogicalChainRoundTrip(t *testing.T, expr *planpb.Expr, maxDepth int) {
	t.Helper()
	require.LessOrEqual(t, logicalChainDepth(expr), maxDepth)
	data, err := proto.Marshal(expr)
	require.NoError(t, err)
	var decoded planpb.Expr
	// Keep the default limit: 6,000 left-deep operands exceed it because
	// each logical node adds two protobuf message levels.
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.True(t, proto.Equal(expr, &decoded), "protobuf round trip changed the expression")
}

func TestParseLogicalChainBalanced(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const n = 1025 // Include an odd operand count in the pairwise fold.
	for _, op := range []string{"OR", "AND"} {
		for _, optimize := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optimize=%t", op, optimize), func(t *testing.T) {
				old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(strconv.FormatBool(optimize))
				t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
				// Arithmetic leaves cannot collapse into a single IN/range predicate.
				filter := buildLogicalChain(n, op, "Int64Field + 1 == %d")
				expr, err := ParseExpr(helper, filter, nil)
				require.NoError(t, err)
				requireLogicalChainRoundTrip(t, expr, bits.Len(uint(n-1))+1)
				wantOp := planpb.BinaryExpr_LogicalOr
				if op == "AND" {
					wantOp = planpb.BinaryExpr_LogicalAnd
				}
				values := make([]int64, 0, n)
				stack := []*planpb.Expr{expr}
				for len(stack) > 0 {
					cur := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					if binary := cur.GetBinaryExpr(); binary != nil {
						require.Equal(t, wantOp, binary.GetOp())
						stack = append(stack, binary.GetRight(), binary.GetLeft())
						continue
					}
					leaf := cur.GetBinaryArithOpEvalRangeExpr()
					require.NotNil(t, leaf)
					require.Equal(t, planpb.OpType_Equal, leaf.GetOp())
					values = append(values, leaf.GetValue().GetInt64Val())
				}
				want := make([]int64, n)
				for i := range want {
					want[i] = int64(i + 1)
				}
				if optimize {
					require.ElementsMatch(t, want, values)
				} else {
					require.Equal(t, want, values, "balancing must retain operand order")
				}
			})
		}
	}
}

func TestParseLogicalChainWrappers(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const n = 6000
	chain := buildLogicalChain(n, "AND", "Int64Field == %d")
	elementChain := strings.ReplaceAll(chain, "Int64Field", "$[sub_int]")
	cases := []struct {
		name   string
		filter string
	}{
		{"plain", chain},
		{"random_sample", chain + " AND random_sample(0.5)"},
		{"element_predicate", chain + " AND element_filter(struct_array, $[sub_int] > 0)"},
		{"element_body", "element_filter(struct_array, " + elementChain + ")"},
		{"match_body", "MATCH_ANY(struct_array, " + elementChain + ")"},
	}
	for _, tc := range cases {
		for _, optimize := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optimize=%t", tc.name, optimize), func(t *testing.T) {
				old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(strconv.FormatBool(optimize))
				t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
				expr, err := ParseExpr(helper, tc.filter, nil)
				require.NoError(t, err)
				requireLogicalChainRoundTrip(t, expr, bits.Len(uint(n-1))+2)
			})
		}
	}
}

func TestParseLogicalOREqualityChain(t *testing.T) {
	helper := newTestSchemaHelper(t)
	old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue("true")
	t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
	const n = 4096
	expr, err := ParseExpr(helper, buildLogicalChain(n, "OR", "Int64Field == %d"), nil)
	require.NoError(t, err)
	term := expr.GetTermExpr()
	require.NotNil(t, term, "OR equalities on one column should still combine into IN")
	require.Len(t, term.GetValues(), n)
	for i, value := range term.GetValues() {
		require.Equal(t, int64(i+1), value.GetInt64Val())
	}
	requireLogicalChainRoundTrip(t, expr, 1)
}

func TestParseLogicalChainTemplates(t *testing.T) {
	helper := newTestSchemaHelper(t)
	const n = 1025
	chain := buildLogicalChain(n, "OR", "Int64Field + %d == {value}")
	expr, err := ParseExprTemplate(helper, chain, nil)
	require.NoError(t, err)
	require.True(t, expr.GetIsTemplate())
	requireLogicalChainRoundTrip(t, expr, bits.Len(uint(n-1))+1)
	_, err = ParseExpr(helper, chain, nil)
	require.Error(t, err, "balancing must retain template flags used by filling")
	_, err = ParseExpr(helper, chain, map[string]*schemapb.TemplateValue{
		"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 10}},
	})
	require.NoError(t, err)
	for _, filter := range []string{"true OR (" + chain + ")", "false AND (" + chain + ")"} {
		_, err := ParseExpr(helper, filter, nil)
		require.Error(t, err, "constant folding must not hide a missing template value")
	}
}

func TestParseLogicalChainDeferredBoolean(t *testing.T) {
	helper := newTestSchemaHelper(t)
	for _, op := range []string{"OR", "AND"} {
		want := op == "OR"
		// Balancing groups the literal with the last non-template predicate.
		chain := fmt.Sprintf("Int64Field == {value} %s Int64Field == 2 %s %t %s Int64Field == 3", op, op, want, op)
		for _, wrapper := range []string{"plain", "random_sample", "element_predicate", "element_body"} {
			filter := chain
			switch wrapper {
			case "random_sample":
				filter = "(" + chain + ") AND random_sample(0.5)"
			case "element_predicate":
				filter = "(" + chain + ") AND element_filter(struct_array, $[sub_int] > 0)"
			case "element_body":
				filter = "element_filter(struct_array, " + strings.ReplaceAll(chain, "Int64Field", "$[sub_int]") + ")"
			}
			for _, optimize := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/optimize=%t", op, wrapper, optimize), func(t *testing.T) {
					old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(strconv.FormatBool(optimize))
					t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
					_, err := ParseExpr(helper, filter, nil)
					require.Error(t, err, "the literal must not hide a missing template value")
					_, err = ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
						"value": {Val: &schemapb.TemplateValue_StringVal{StringVal: "invalid integer"}},
					})
					require.Error(t, err, "the literal must not hide an invalid template value")
					expr, err := ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
						"value": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 1}},
					})
					require.NoError(t, err)
					switch wrapper {
					case "random_sample":
						require.NotNil(t, expr.GetRandomSampleExpr())
						expr = expr.GetRandomSampleExpr().GetPredicate()
					case "element_predicate":
						require.NotNil(t, expr.GetElementFilterExpr())
						expr = expr.GetElementFilterExpr().GetPredicate()
					case "element_body":
						require.NotNil(t, expr.GetElementFilterExpr())
						expr = expr.GetElementFilterExpr().GetElementExpr()
					}
					value, constant := executableLogicalConstant(t, expr)
					require.True(t, constant, "literal must determine the result for every row")
					require.Equal(t, want, value)
				})
			}
		}
	}
}

// executableLogicalConstant visits both branches to reject raw boolean values,
// then checks whether canonical constants determine the result for every row.
func executableLogicalConstant(t *testing.T, expr *planpb.Expr) (bool, bool) {
	t.Helper()
	require.NotNil(t, expr)
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_ValueExpr:
		_, rawBool := e.ValueExpr.GetValue().GetVal().(*planpb.GenericValue_BoolVal)
		require.False(t, rawBool, "raw boolean ValueExpr cannot execute as a predicate")
	case *planpb.Expr_AlwaysTrueExpr:
		return true, true
	case *planpb.Expr_UnaryExpr:
		require.Equal(t, planpb.UnaryExpr_Not, e.UnaryExpr.GetOp())
		value, constant := executableLogicalConstant(t, e.UnaryExpr.GetChild())
		return !value, constant
	case *planpb.Expr_BinaryExpr:
		left, leftConst := executableLogicalConstant(t, e.BinaryExpr.GetLeft())
		right, rightConst := executableLogicalConstant(t, e.BinaryExpr.GetRight())
		if e.BinaryExpr.GetOp() == planpb.BinaryExpr_LogicalOr {
			return left || right, leftConst && left || rightConst && right || leftConst && rightConst
		}
		require.Equal(t, planpb.BinaryExpr_LogicalAnd, e.BinaryExpr.GetOp())
		return left && right, leftConst && !left || rightConst && !right || leftConst && rightConst
	}
	return false, false
}

var logicalChainBenchmarkResult *planpb.Expr

// BenchmarkLogicalORChain measures uncached lexing/parsing, visiting and
// rewriting. Building the input and schema is excluded from the timed region.
func BenchmarkLogicalORChain(b *testing.B) {
	helper := getOptBenchSchemaHelper(b)
	for _, tc := range []struct {
		name      string
		predicate string
	}{
		{"equality", "Int64Field == %d"},
		{"arithmetic", "Int64Field + 1 == %d"},
	} {
		for _, n := range []int{1024, 4096, 16384} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, n), func(b *testing.B) {
				filter := buildLogicalChain(n, "OR", tc.predicate)
				b.ReportAllocs()
				b.SetBytes(int64(len(filter)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ast, err := benchLexParseTwoStage(filter)
					if err != nil {
						b.Fatal(err)
					}
					result := ast.Accept(NewParserVisitor(helper, &ParserVisitorArgs{}))
					if err := getError(result); err != nil {
						b.Fatal(err)
					}
					logicalChainBenchmarkResult = rewriter.RewriteExprWithConfig(getExpr(result).expr, true)
				}
			})
		}
	}
}
