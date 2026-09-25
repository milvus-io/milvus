package rewriter

import (
	"fmt"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func treeDepth(e *planpb.Expr) int {
	if e == nil {
		return 0
	}
	if b := e.GetBinaryExpr(); b != nil {
		l, r := treeDepth(b.Left), treeDepth(b.Right)
		if l < r {
			l = r
		}
		return l + 1
	}
	return 1
}

func TestFoldBinaryBalancedDepthAndOrder(t *testing.T) {
	for _, op := range []planpb.BinaryExpr_BinaryOp{planpb.BinaryExpr_LogicalOr, planpb.BinaryExpr_LogicalAnd} {
		for _, n := range []int{1, 2, 3, 17, 6000} {
			t.Run(fmt.Sprintf("%s/%d", op, n), func(t *testing.T) {
				exprs := make([]*planpb.Expr, n)
				for i := range exprs {
					exprs[i] = newUnaryRangeExpr(
						&planpb.ColumnInfo{FieldId: 1, DataType: schemapb.DataType_Int64},
						planpb.OpType_Equal,
						&planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: int64(i)}},
					)
				}
				root := foldBinary(op, exprs)
				require.Equal(t, bits.Len(uint(n-1))+1, treeDepth(root))
				stack := []*planpb.Expr{root}
				leaves := 0
				for len(stack) > 0 {
					node := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					if binary := node.GetBinaryExpr(); binary != nil {
						require.Equal(t, op, binary.GetOp())
						stack = append(stack, binary.Right, binary.Left)
						continue
					}
					require.Less(t, leaves, len(exprs))
					require.Same(t, exprs[leaves], node, "operand %d changed position", leaves)
					leaves++
				}
				require.Equal(t, n, leaves)
				data, err := proto.Marshal(root)
				require.NoError(t, err)
				var decoded planpb.Expr
				require.NoError(t, proto.Unmarshal(data, &decoded))
				require.True(t, proto.Equal(root, &decoded), "protobuf round trip changed the expression")
			})
		}
	}
}

func TestFoldBinaryBalancedConstants(t *testing.T) {
	leaf := newUnaryRangeExpr(
		&planpb.ColumnInfo{FieldId: 1, DataType: schemapb.DataType_Int64},
		planpb.OpType_Equal,
		&planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
	)
	require.Nil(t, foldBinary(planpb.BinaryExpr_LogicalOr, nil))
	require.Same(t, leaf, foldBinary(planpb.BinaryExpr_LogicalOr, []*planpb.Expr{newAlwaysFalseExpr(), leaf, newAlwaysFalseExpr()}))
	require.Same(t, leaf, foldBinary(planpb.BinaryExpr_LogicalAnd, []*planpb.Expr{newAlwaysTrueExpr(), leaf, newAlwaysTrueExpr()}))
	require.True(t, IsAlwaysTrueExpr(foldBinary(planpb.BinaryExpr_LogicalOr, []*planpb.Expr{leaf, newAlwaysTrueExpr()})))
	require.True(t, IsAlwaysFalseExpr(foldBinary(planpb.BinaryExpr_LogicalAnd, []*planpb.Expr{leaf, newAlwaysFalseExpr()})))
	require.True(t, IsAlwaysTrueExpr(foldBinary(planpb.BinaryExpr_LogicalAnd, []*planpb.Expr{newAlwaysTrueExpr(), newAlwaysTrueExpr()})))
	require.True(t, IsAlwaysFalseExpr(foldBinary(planpb.BinaryExpr_LogicalOr, []*planpb.Expr{newAlwaysFalseExpr(), newAlwaysFalseExpr()})))
}
