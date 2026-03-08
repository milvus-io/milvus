package rewriter

import (
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func (v *visitor) combineOrTextMatchToMerged(parts []*planpb.Expr) []*planpb.Expr {
	type group struct {
		col         *planpb.ColumnInfo
		origIndices []int
		literals    []string
	}
	others := make([]*planpb.Expr, 0, len(parts))
	groups := make(map[string]*group)
	indexToExpr := parts
	for idx, e := range parts {
		u := e.GetUnaryRangeExpr()
		if u == nil || u.GetOp() != planpb.OpType_TextMatch || u.GetValue() == nil {
			others = append(others, e)
			continue
		}
		if len(u.GetExtraValues()) > 0 {
			others = append(others, e)
			continue
		}
		col := u.GetColumnInfo()
		if col == nil {
			others = append(others, e)
			continue
		}
		key := columnKey(col)
		g, ok := groups[key]
		if !ok {
			g = &group{col: col}
			groups[key] = g
		}
		literal := u.GetValue().GetStringVal()
		g.literals = append(g.literals, literal)
		g.origIndices = append(g.origIndices, idx)
	}
	out := make([]*planpb.Expr, 0, len(parts))
	out = append(out, others...)
	for _, g := range groups {
		if len(g.origIndices) <= 1 {
			for _, i := range g.origIndices {
				out = append(out, indexToExpr[i])
			}
			continue
		}
		if len(g.literals) == 0 {
			for _, i := range g.origIndices {
				out = append(out, indexToExpr[i])
			}
			continue
		}
		merged := strings.Join(g.literals, " ")
		out = append(out, newTextMatchExpr(g.col, merged))
	}
	return out
}

func newTextMatchExpr(col *planpb.ColumnInfo, literal string) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_TextMatch,
				Value: &planpb.GenericValue{
					Val: &planpb.GenericValue_StringVal{
						StringVal: literal,
					},
				},
			},
		},
	}
}
