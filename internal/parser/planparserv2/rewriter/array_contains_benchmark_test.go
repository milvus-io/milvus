package rewriter_test

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// This standalone benchmark also runs unchanged on the pre-backport revision.
// Construction is outside the timer; allocations measure the rewrite itself.
func BenchmarkRewriteArrayContains(b *testing.B) {
	for _, n := range []int{1000, 2164, 2216} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("leaves%d/enabled%t", n, enabled), func(b *testing.B) {
				col := &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true}
				var input *planpb.Expr
				for i := 0; i < n; i++ {
					leaf := &planpb.Expr{Expr: &planpb.Expr_JsonContainsExpr{JsonContainsExpr: &planpb.JSONContainsExpr{ColumnInfo: col, Op: planpb.JSONContainsExpr_Contains, ElementsSameType: true, Elements: []*planpb.GenericValue{{Val: &planpb.GenericValue_StringVal{StringVal: fmt.Sprintf("synthetic_%04d", i)}}}}}}
					if input == nil {
						input = leaf
					} else {
						input = &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{Op: planpb.BinaryExpr_LogicalOr, Left: input, Right: leaf}}}
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				var result *planpb.Expr
				for i := 0; i < b.N; i++ {
					result = rewriter.RewriteExprWithConfig(input, enabled)
				}
				b.StopTimer()
				count := 0
				stack := []*planpb.Expr{result}
				for len(stack) > 0 {
					e := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					if binary := e.GetBinaryExpr(); binary != nil {
						stack = append(stack, binary.GetLeft(), binary.GetRight())
					} else if e.GetJsonContainsExpr() != nil {
						count++
					}
				}
				b.ReportMetric(float64(count), "contains_leaves")
			})
		}
	}
}
