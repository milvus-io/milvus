package rewriter_test

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type orUnionInterval struct {
	lower, upper       int64
	lowerInc, upperInc bool
}

func orUnionRange(column *planpb.ColumnInfo, interval orUnionInterval) *planpb.Expr {
	expr := edgeTestBinaryRange(column, edgeTestIntValue(interval.lower), edgeTestIntValue(interval.upper))
	expr.GetBinaryRangeExpr().LowerInclusive = interval.lowerInc
	expr.GetBinaryRangeExpr().UpperInclusive = interval.upperInc
	return expr
}

func orUnionChain(parts ...*planpb.Expr) *planpb.Expr {
	root := parts[0]
	for _, part := range parts[1:] {
		root = edgeTestLogical(planpb.BinaryExpr_LogicalOr, root, part)
	}
	return root
}

func orUnionLeaves(expr *planpb.Expr) []*planpb.Expr {
	if binary := expr.GetBinaryExpr(); binary != nil {
		return append(orUnionLeaves(binary.GetLeft()), orUnionLeaves(binary.GetRight())...)
	}
	return []*planpb.Expr{expr}
}

func TestRewriteOrRangeUnionManyIntervals(t *testing.T) {
	column := edgeTestRangeColumn(schemapb.DataType_Int64)
	for _, tc := range []struct {
		name  string
		input []orUnionInterval
		want  []orUnionInterval
	}{
		{"unsorted_overlap", []orUnionInterval{{4, 9, true, true}, {0, 3, true, true}, {2, 6, true, true}}, []orUnionInterval{{0, 9, true, true}}},
		{"inclusive_adjacent", []orUnionInterval{{0, 1, true, false}, {1, 2, true, true}, {2, 3, false, true}}, []orUnionInterval{{0, 3, true, true}}},
		{"exclusive_adjacent", []orUnionInterval{{0, 1, true, false}, {1, 2, false, false}, {2, 3, false, true}}, []orUnionInterval{{0, 1, true, false}, {1, 2, false, false}, {2, 3, false, true}}},
		{"equal_lower", []orUnionInterval{{0, 4, true, false}, {0, 5, false, true}, {1, 3, false, true}}, []orUnionInterval{{0, 5, true, true}}},
		{"equal_upper", []orUnionInterval{{0, 5, true, false}, {1, 5, true, true}, {2, 4, true, false}}, []orUnionInterval{{0, 5, true, true}}},
		{"contained", []orUnionInterval{{0, 10, true, false}, {2, 4, true, true}, {5, 8, true, true}}, []orUnionInterval{{0, 10, true, false}}},
		{"disjoint_clusters", []orUnionInterval{{20, 21, true, true}, {1, 3, true, true}, {12, 15, true, true}, {0, 2, true, true}, {10, 13, true, true}}, []orUnionInterval{{0, 3, true, true}, {10, 15, true, true}, {20, 21, true, true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parts := make([]*planpb.Expr, len(tc.input))
			for i, interval := range tc.input {
				parts[i] = orUnionRange(column, interval)
			}
			output := rewriter.RewriteExprWithConfig(orUnionChain(parts...), true)
			var got []orUnionInterval
			for _, leaf := range orUnionLeaves(output) {
				predicate := leaf.GetBinaryRangeExpr()
				require.NotNil(t, predicate)
				got = append(got, orUnionInterval{predicate.GetLowerValue().GetInt64Val(), predicate.GetUpperValue().GetInt64Val(), predicate.GetLowerInclusive(), predicate.GetUpperInclusive()})
			}
			require.ElementsMatch(t, tc.want, got)
			for sample := int64(-1); sample <= 22; sample++ {
				matches := func(intervals []orUnionInterval) bool {
					for _, interval := range intervals {
						if (sample > interval.lower || sample == interval.lower && interval.lowerInc) &&
							(sample < interval.upper || sample == interval.upper && interval.upperInc) {
							return true
						}
					}
					return false
				}
				require.Equal(t, matches(tc.input), matches(got), "sample=%d", sample)
			}
		})
	}
}

// A missing/null numeric value is UNKNOWN for every source range and for the
// merged range. NOT must retain that UNKNOWN rather than turn it into true.
func TestRewriteOrRangeUnionNullableUnderNot(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_JSON} {
		t.Run(dataType.String(), func(t *testing.T) {
			column := edgeTestRangeColumn(dataType)
			column.Nullable = true
			input := []orUnionInterval{{4, 8, false, false}, {0, 3, true, true}, {2, 6, false, true}}
			parts := make([]*planpb.Expr, len(input))
			for i, interval := range input {
				parts[i] = orUnionRange(column, interval)
			}
			output := rewriter.RewriteExprWithConfig(&planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
				Op: planpb.UnaryExpr_Not, Child: orUnionChain(parts...),
			}}}, true)
			not := output.GetUnaryExpr()
			require.NotNil(t, not)
			require.Equal(t, planpb.UnaryExpr_Not, not.GetOp())
			merged := not.GetChild().GetBinaryRangeExpr()
			require.NotNil(t, merged)
			require.Same(t, column, merged.GetColumnInfo())
			require.True(t, merged.GetColumnInfo().GetNullable())
			require.Equal(t, column.GetNestedPath(), merged.GetColumnInfo().GetNestedPath())
			require.False(t, rewriter.IsAlwaysTrueExpr(output))
			require.False(t, rewriter.IsAlwaysFalseExpr(output))
			require.Equal(t, int64(0), merged.GetLowerValue().GetInt64Val())
			require.Equal(t, int64(8), merged.GetUpperValue().GetInt64Val())
		})
	}
}

func TestRewriteOrRangeUnionKeepsColumnsAndJSONKindsSeparate(t *testing.T) {
	first := edgeTestRangeColumn(schemapb.DataType_JSON)
	second := edgeTestRangeColumn(schemapb.DataType_JSON)
	second.NestedPath = []string{"other"}
	parts := []*planpb.Expr{}
	for _, column := range []*planpb.ColumnInfo{first, second} {
		for i := int64(0); i < 3; i++ {
			parts = append(parts, orUnionRange(column, orUnionInterval{i, i + 3, true, true}))
		}
	}
	for _, bounds := range [][2]string{{"a", "d"}, {"c", "f"}, {"e", "h"}} {
		parts = append(parts, edgeTestBinaryRange(first, edgeTestStringValue(bounds[0]), edgeTestStringValue(bounds[1])))
	}
	output := rewriter.RewriteExprWithConfig(orUnionChain(parts...), true)
	leaves := orUnionLeaves(output)
	require.Len(t, leaves, 3)
	seen := map[string]bool{}
	for _, leaf := range leaves {
		predicate := leaf.GetBinaryRangeExpr()
		require.NotNil(t, predicate)
		key := fmt.Sprint(predicate.GetColumnInfo().GetNestedPath())
		if _, ok := predicate.GetLowerValue().GetVal().(*planpb.GenericValue_StringVal); ok {
			key += "/string"
			require.Equal(t, "a", predicate.GetLowerValue().GetStringVal())
			require.Equal(t, "h", predicate.GetUpperValue().GetStringVal())
		} else {
			key += "/numeric"
			require.Equal(t, int64(0), predicate.GetLowerValue().GetInt64Val())
			require.Equal(t, int64(5), predicate.GetUpperValue().GetInt64Val())
		}
		seen[key] = true
	}
	require.Len(t, seen, 3)
}

func TestRewriteOrRangeUnionSkipsUnsafeGroups(t *testing.T) {
	for _, tc := range []struct {
		name string
		part func(*planpb.ColumnInfo) *planpb.Expr
	}{
		{"unbounded", func(column *planpb.ColumnInfo) *planpb.Expr {
			return edgeTestUnaryRange(column, planpb.OpType_GreaterThan, edgeTestFloatValue(1))
		}},
		{"infinite", func(column *planpb.ColumnInfo) *planpb.Expr {
			return edgeTestBinaryRange(column, edgeTestFloatValue(0), edgeTestFloatValue(math.Inf(1)))
		}},
		{"invalid", func(column *planpb.ColumnInfo) *planpb.Expr {
			return edgeTestBinaryRange(column, edgeTestFloatValue(4), edgeTestFloatValue(1))
		}},
		{"empty", func(column *planpb.ColumnInfo) *planpb.Expr {
			part := edgeTestBinaryRange(column, edgeTestFloatValue(1), edgeTestFloatValue(1))
			part.GetBinaryRangeExpr().UpperInclusive = false
			return part
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column := edgeTestRangeColumn(schemapb.DataType_Double)
			column.Nullable = true
			parts := []*planpb.Expr{
				edgeTestBinaryRange(column, edgeTestFloatValue(0), edgeTestFloatValue(2)),
				edgeTestBinaryRange(column, edgeTestFloatValue(1), edgeTestFloatValue(3)),
				tc.part(column),
			}
			output := rewriter.RewriteExprWithConfig(orUnionChain(parts...), true)
			require.Len(t, orUnionLeaves(output), len(parts))
		})
	}
}

func TestRewriteOrRangeUnionLargeParserChain(t *testing.T) {
	const count = 6000
	parts := make([]string, count)
	for i := range parts {
		parts[i] = fmt.Sprintf("(%d <= Int64Field <= %d)", count-i-1, 2*count-i-1)
	}
	expr, err := parser.ParseExpr(buildSchemaHelperForRewriteT(t), strings.Join(parts, " OR "), nil)
	require.NoError(t, err)
	merged := expr.GetBinaryRangeExpr()
	require.NotNil(t, merged)
	require.Equal(t, int64(0), merged.GetLowerValue().GetInt64Val())
	require.Equal(t, int64(2*count-1), merged.GetUpperValue().GetInt64Val())
	require.True(t, merged.GetLowerInclusive())
	require.True(t, merged.GetUpperInclusive())
}
