package rewriter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func TestArrayContainsNullableAndEmptyTruthTables(t *testing.T) {
	col := arrayContainsTestColumn(201, schemapb.DataType_Int64)
	col.Nullable = true
	contains := func(op planpb.JSONContainsExpr_JSONOp, values ...int64) *planpb.Expr {
		args := make([]*planpb.GenericValue, 0, len(values))
		for _, value := range values {
			args = append(args, arrayContainsTestInt(value))
		}
		return arrayContainsTestExpr(col, op, args...)
	}
	one := func() *planpb.Expr { return contains(planpb.JSONContainsExpr_Contains, 1) }
	two := func() *planpb.Expr { return contains(planpb.JSONContainsExpr_Contains, 2) }
	for _, op := range []planpb.BinaryExpr_BinaryOp{planpb.BinaryExpr_LogicalOr, planpb.BinaryExpr_LogicalAnd} {
		target := planpb.JSONContainsExpr_ContainsAny
		if op == planpb.BinaryExpr_LogicalAnd {
			target = planpb.JSONContainsExpr_ContainsAll
		}
		cases := []*planpb.Expr{
			arrayContainsBinary(op, one(), two()),
			arrayContainsBinary(op, one(), one()),
			arrayContainsBinary(op, contains(target), one()),
			arrayContainsBinary(op, contains(target), contains(target)),
			arrayContainsBinary(op, contains(target, 1, 2), contains(target, 2, 3)),
		}
		for i, expr := range cases {
			for _, negate := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/case%d/not%t", op, i, negate), func(t *testing.T) {
					input := expr
					if negate {
						input = notExpr(input)
					}
					result := RewriteExprWithConfig(input, true)
					// Evaluate independently using nullable membership/three-valued
					// truth tables, including an invalid row and a valid empty array.
					for _, row := range []arrayContainsRow{
						{valid: false}, {valid: true}, {valid: true, values: []int64{0}},
						{valid: true, values: []int64{1}}, {valid: true, values: []int64{2}},
						{valid: true, values: []int64{1, 1, 2}}, {valid: true, values: []int64{1, 2, 3}},
					} {
						require.Equal(t, evalArrayContainsTruth(input, row), evalArrayContainsTruth(result, row))
					}
				})
			}
		}
	}
}

func TestArrayContainsDoesNotCrossNotOrOppositeOperator(t *testing.T) {
	col := arrayContainsTestColumn(201, schemapb.DataType_Int64)
	one := arrayContainsTestExpr(col, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1))
	two := arrayContainsTestExpr(col, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(2))
	three := arrayContainsTestExpr(col, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(3))
	inputs := []*planpb.Expr{
		arrayContainsBinary(planpb.BinaryExpr_LogicalOr, notExpr(one), two),
		arrayContainsBinary(planpb.BinaryExpr_LogicalOr, arrayContainsBinary(planpb.BinaryExpr_LogicalAnd, one, two), three),
		arrayContainsBinary(planpb.BinaryExpr_LogicalAnd, arrayContainsBinary(planpb.BinaryExpr_LogicalOr, one, two), three),
	}
	for i, input := range inputs {
		result := RewriteExprWithConfig(input, true)
		require.NotNil(t, result.GetBinaryExpr())
		for bits := 0; bits < 8; bits++ {
			row := arrayContainsRow{valid: true}
			for v := 1; v <= 3; v++ {
				if bits&(1<<(v-1)) != 0 {
					row.values = append(row.values, int64(v))
				}
			}
			require.Equal(t, evalArrayContainsTruth(input, row), evalArrayContainsTruth(result, row), "case %d", i)
		}
		require.Equal(t, evalArrayContainsTruth(input, arrayContainsRow{}), evalArrayContainsTruth(result, arrayContainsRow{}))
	}
}

func TestArrayContainsColumnPathIdentity(t *testing.T) {
	var input []*planpb.Expr
	for _, key := range []struct {
		id   int64
		path []string
	}{{201, nil}, {202, nil}, {201, []string{"0"}}, {201, []string{"1"}}} {
		col := arrayContainsTestColumn(key.id, schemapb.DataType_Int64)
		col.NestedPath = key.path
		input = append(input,
			arrayContainsTestExpr(col, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1)),
			arrayContainsTestExpr(col, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(2)))
	}
	result := combineArrayContains(input, planpb.JSONContainsExpr_ContainsAny)
	require.Len(t, result, 4)
	for i, expr := range result {
		require.Same(t, input[i*2].GetJsonContainsExpr().GetColumnInfo(), expr.GetJsonContainsExpr().GetColumnInfo())
		require.Equal(t, []int64{1, 2}, arrayContainsTestIntValues(expr.GetJsonContainsExpr().GetElements()))
	}
}

func TestArrayContainsLargeDistinctFieldsRemainBalanced(t *testing.T) {
	const leaves = 2216
	var input *planpb.Expr
	for i := 0; i < leaves; i++ {
		leaf := arrayContainsTestExpr(arrayContainsTestColumn(int64(201+i), schemapb.DataType_Int64),
			planpb.JSONContainsExpr_Contains, arrayContainsTestInt(int64(i)))
		leaf.IsTemplate = i == leaves-1
		if input == nil {
			input = leaf
		} else {
			input = arrayContainsBinary(planpb.BinaryExpr_LogicalOr, input, leaf)
		}
	}
	result := RewriteExprWithConfig(input, true)
	require.True(t, result.GetIsTemplate(), "unmerged operand metadata must survive balancing")
	type nodeDepth struct {
		node  *planpb.Expr
		depth int
	}
	stack := []nodeDepth{{result, 0}}
	count, maxDepth := 0, 0
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if binary := n.node.GetBinaryExpr(); binary != nil {
			stack = append(stack, nodeDepth{binary.GetRight(), n.depth + 1}, nodeDepth{binary.GetLeft(), n.depth + 1})
		} else {
			require.Equal(t, int64(201+count), n.node.GetJsonContainsExpr().GetColumnInfo().GetFieldId())
			count++
			if n.depth > maxDepth {
				maxDepth = n.depth
			}
		}
	}
	require.Equal(t, leaves, count)
	require.LessOrEqual(t, maxDepth, 12)
}

func arrayContainsBinary(op planpb.BinaryExpr_BinaryOp, left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{Op: op, Left: left, Right: right}}}
}

type arrayContainsRow struct {
	valid  bool
	values []int64
}

// -1=unknown, 0=false, 1=true. This oracle does not use rewriter helpers.
func evalArrayContainsTruth(expr *planpb.Expr, row arrayContainsRow) int {
	if contains := expr.GetJsonContainsExpr(); contains != nil {
		all := contains.GetOp() == planpb.JSONContainsExpr_ContainsAll
		if len(contains.GetElements()) == 0 {
			if all {
				return 1
			}
			return 0
		}
		if !row.valid {
			return -1
		}
		matched := 0
		for _, target := range contains.GetElements() {
			for _, value := range row.values {
				if value == target.GetInt64Val() {
					matched++
					break
				}
			}
		}
		if (all && matched == len(contains.GetElements())) || (!all && matched > 0) {
			return 1
		}
		return 0
	}
	if unary := expr.GetUnaryExpr(); unary != nil {
		value := evalArrayContainsTruth(unary.GetChild(), row)
		if value == -1 {
			return -1
		}
		return 1 - value
	}
	binary := expr.GetBinaryExpr()
	left, right := evalArrayContainsTruth(binary.GetLeft(), row), evalArrayContainsTruth(binary.GetRight(), row)
	if binary.GetOp() == planpb.BinaryExpr_LogicalAnd {
		if left == 0 || right == 0 {
			return 0
		}
		if left == -1 || right == -1 {
			return -1
		}
		return 1
	}
	if left == 1 || right == 1 {
		return 1
	}
	if left == -1 || right == -1 {
		return -1
	}
	return 0
}

func TestArrayContainsMergesNewlyExposedLogicalOperands(t *testing.T) {
	for _, op := range []planpb.BinaryExpr_BinaryOp{planpb.BinaryExpr_LogicalAnd, planpb.BinaryExpr_LogicalOr} {
		for _, sameColumn := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/sameColumn%t", op, sameColumn), func(t *testing.T) {
				colA := arrayContainsTestColumn(201, schemapb.DataType_Int64)
				colB := colA
				if !sameColumn {
					colB = arrayContainsTestColumn(202, schemapb.DataType_Int64)
				}
				one := arrayContainsTestExpr(colA, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1))
				two := arrayContainsTestExpr(colB, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(2))
				three := arrayContainsTestExpr(colA, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(3))
				opposite, identity, target := planpb.BinaryExpr_LogicalOr, newAlwaysFalseExpr(), planpb.JSONContainsExpr_ContainsAll
				if op == planpb.BinaryExpr_LogicalOr {
					opposite, identity, target = planpb.BinaryExpr_LogicalAnd, newAlwaysTrueExpr(), planpb.JSONContainsExpr_ContainsAny
				}
				input := arrayContainsBinary(op, arrayContainsBinary(opposite, arrayContainsBinary(op, one, two), identity), three)
				result := RewriteExprWithConfig(input, true)
				if sameColumn {
					require.Equal(t, target, result.GetJsonContainsExpr().GetOp())
					require.Equal(t, []int64{1, 2, 3}, arrayContainsTestIntValues(result.GetJsonContainsExpr().GetElements()))
				} else {
					parts := flattenLogicalExpr(result.GetBinaryExpr())
					require.Len(t, parts, 2)
					require.Equal(t, target, parts[0].GetJsonContainsExpr().GetOp())
					require.Equal(t, []int64{1, 3}, arrayContainsTestIntValues(parts[0].GetJsonContainsExpr().GetElements()))
					require.Equal(t, int64(202), parts[1].GetJsonContainsExpr().GetColumnInfo().GetFieldId())
				}
			})
		}
	}
}

func TestArrayContainsPreservesConflictingColumnMetadata(t *testing.T) {
	for _, change := range []string{"element_type", "nullable"} {
		t.Run(change, func(t *testing.T) {
			column := arrayContainsTestColumn(201, schemapb.DataType_Int64)
			conflict := arrayContainsTestColumn(201, schemapb.DataType_Int64)
			if change == "element_type" {
				conflict.ElementType = schemapb.DataType_Double
			} else {
				conflict.Nullable = true
			}
			first := arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(1))
			unmerged := arrayContainsTestExpr(conflict, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(2))
			third := arrayContainsTestExpr(column, planpb.JSONContainsExpr_Contains, arrayContainsTestInt(3))
			result := combineArrayContains([]*planpb.Expr{first, unmerged, third}, planpb.JSONContainsExpr_ContainsAny)
			require.Len(t, result, 2)
			require.Equal(t, []int64{1, 3}, arrayContainsTestIntValues(result[0].GetJsonContainsExpr().GetElements()))
			require.Same(t, column, result[0].GetJsonContainsExpr().GetColumnInfo())
			require.Same(t, unmerged, result[1])
		})
	}
}
