package rewriter

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func inNotEqualInt(value int64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: value}}
}

func TestGenericScalarKeyPreservesEquality(t *testing.T) {
	values := []*planpb.GenericValue{
		nil,
		{},
		{Val: &planpb.GenericValue_BoolVal{BoolVal: false}},
		{Val: &planpb.GenericValue_BoolVal{BoolVal: true}},
		inNotEqualInt(0), inNotEqualInt(1), inNotEqualInt(1 << 53), inNotEqualInt(1<<53 + 1),
		{Val: &planpb.GenericValue_FloatVal{FloatVal: 0}},
		{Val: &planpb.GenericValue_FloatVal{FloatVal: math.Copysign(0, -1)}},
		{Val: &planpb.GenericValue_FloatVal{FloatVal: 1}},
		{Val: &planpb.GenericValue_FloatVal{FloatVal: math.Inf(1)}},
		{Val: &planpb.GenericValue_FloatVal{FloatVal: math.NaN()}},
		{Val: &planpb.GenericValue_StringVal{StringVal: ""}},
		{Val: &planpb.GenericValue_StringVal{StringVal: "1"}},
		{Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{}}},
	}
	for i, left := range values {
		set := make(map[scalarValueKey]struct{})
		if key, ok := genericScalarKey(left); ok {
			set[key] = struct{}{}
		}
		for j, right := range values {
			key, supported := genericScalarKey(right)
			_, found := set[key]
			require.Equal(t, equalsGeneric(left, right), supported && found, "values[%d], values[%d]", i, j)
		}
	}
}

var (
	inNotEqualBenchmarkCount int
	inNotEqualBenchmarkPlan  []*planpb.Expr
)

// Isolate the old quadratic membership work from parser, sorting and plan
// construction. Both paths receive the same disjoint scalar value lists.
func BenchmarkInNotEqualMembership(b *testing.B) {
	values := make([]*planpb.GenericValue, 50000)
	for i := range values {
		values[i] = inNotEqualInt(int64(i))
	}
	for _, count := range []int{1, 10, 100} {
		exclusions := make([]*planpb.GenericValue, count)
		for i := range exclusions {
			exclusions[i] = inNotEqualInt(int64(len(values) + i))
		}
		b.Run(fmt.Sprintf("neq=%d/nested_scan", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				matches := 0
				for _, value := range values {
					for _, excluded := range exclusions {
						if equalsGeneric(value, excluded) {
							matches++
							break
						}
					}
				}
				inNotEqualBenchmarkCount = matches
			}
		})
		b.Run(fmt.Sprintf("neq=%d/set_lookup", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var set map[scalarValueKey]struct{}
				if len(exclusions) > 1 {
					set = make(map[scalarValueKey]struct{}, len(exclusions))
					for _, excluded := range exclusions {
						key, _ := genericScalarKey(excluded)
						set[key] = struct{}{}
					}
				}
				matches := 0
				for _, value := range values {
					found := false
					if len(exclusions) == 1 {
						found = equalsGeneric(value, exclusions[0])
					} else {
						key, _ := genericScalarKey(value)
						_, found = set[key]
					}
					if found {
						matches++
					}
				}
				inNotEqualBenchmarkCount = matches
			}
		})
	}
}

func BenchmarkInNotEqualLargeLists(b *testing.B) {
	col := &planpb.ColumnInfo{FieldId: 1, DataType: schemapb.DataType_Int64}
	values := make([]*planpb.GenericValue, 50000)
	for i := range values {
		values[i] = inNotEqualInt(int64(i))
	}
	for _, op := range []planpb.BinaryExpr_BinaryOp{planpb.BinaryExpr_LogicalAnd, planpb.BinaryExpr_LogicalOr} {
		for _, count := range []int{1, 10, 100} {
			parts := []*planpb.Expr{newTermExpr(col, values)}
			for i := 0; i < count; i++ {
				parts = append(parts, newUnaryRangeExpr(col, planpb.OpType_NotEqual, inNotEqualInt(int64(len(values)+i))))
			}
			b.Run(fmt.Sprintf("%s/neq=%d", op, count), func(b *testing.B) {
				result := combineInNotEqual(parts, op)
				if op == planpb.BinaryExpr_LogicalAnd {
					require.Len(b, result, 1)
					require.Len(b, result[0].GetTermExpr().GetValues(), len(values))
				} else {
					require.Len(b, result, count)
					require.Nil(b, result[0].GetTermExpr())
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					inNotEqualBenchmarkPlan = combineInNotEqual(parts, op)
				}
			})
		}
	}
}
