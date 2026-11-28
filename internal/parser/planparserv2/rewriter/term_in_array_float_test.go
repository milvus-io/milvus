package rewriter_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func arrayFloat32Schema(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
		Name: "array_float32_rewrite",
		Fields: []*schemapb.FieldSchema{{
			FieldID: 201, Name: "ArrayFloat32", DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_Float, Nullable: true,
		}},
	})
	require.NoError(t, err)
	return helper
}

func parseArrayFloat32Plan(t *testing.T, helper *typeutil.SchemaHelper, filter string, optimize bool) *planpb.Expr {
	t.Helper()
	old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(strconv.FormatBool(optimize))
	defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old)
	expr, err := parser.ParseExpr(helper, filter, nil)
	require.NoError(t, err)
	return expr
}

func TestRewriteArrayFloat32IndexPreservesMixedPrecisionExecutors(t *testing.T) {
	helper := arrayFloat32Schema(t)
	singleton := parseArrayFloat32Plan(t, helper, "ArrayFloat32[0] in [1.5]", true).GetTermExpr()
	require.NotNil(t, singleton)
	require.Len(t, singleton.GetValues(), 1)
	require.Equal(t, 1.5, singleton.GetValues()[0].GetFloatVal())

	for _, tc := range []struct {
		filter     string
		termValues []float64
		equalCount int
	}{
		{"ArrayFloat32[0] == 1.5 OR ArrayFloat32[0] == 2.5", nil, 2},
		{"ArrayFloat32[0] in [1.5, 2.5] OR ArrayFloat32[0] == 3.5", []float64{1.5, 2.5}, 1},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			expr := parseArrayFloat32Plan(t, helper, tc.filter, true)
			// Fixed ARRAY<FLOAT> index equality uses UnaryRange's float32
			// executor, while TermExpr uses double. Do not collapse the former
			// into the latter until the executor precision is made consistent.
			require.NotNil(t, expr.GetBinaryExpr())
			require.Equal(t, planpb.BinaryExpr_LogicalOr, expr.GetBinaryExpr().GetOp())
			equalCount := 0
			termCount := 0
			for _, child := range []*planpb.Expr{expr.GetBinaryExpr().GetLeft(), expr.GetBinaryExpr().GetRight()} {
				if eq := child.GetUnaryRangeExpr(); eq != nil {
					equalCount++
					require.Equal(t, planpb.OpType_Equal, eq.GetOp())
					require.Equal(t, []string{"0"}, eq.GetColumnInfo().GetNestedPath())
					continue
				}
				term := child.GetTermExpr()
				require.NotNil(t, term)
				termCount++
				require.Equal(t, []string{"0"}, term.GetColumnInfo().GetNestedPath())
				values := make([]float64, 0, len(term.GetValues()))
				for _, value := range term.GetValues() {
					values = append(values, value.GetFloatVal())
				}
				require.Equal(t, tc.termValues, values)
			}
			require.Equal(t, tc.equalCount, equalCount)
			require.Equal(t, len(tc.termValues) > 0, termCount == 1)
		})
	}
}

func TestRewriteArrayFloat32KeepsWholeArrayNormalization(t *testing.T) {
	helper := arrayFloat32Schema(t)
	for _, optimize := range []bool{false, true} {
		expr := parseArrayFloat32Plan(t, helper, "ArrayFloat32 in [[1.5], [2.5]]", optimize)
		binary := expr.GetBinaryExpr()
		require.NotNil(t, binary)
		require.Equal(t, planpb.BinaryExpr_LogicalOr, binary.GetOp())
		for _, child := range []*planpb.Expr{binary.GetLeft(), binary.GetRight()} {
			comparison := child.GetUnaryRangeExpr()
			require.NotNil(t, comparison)
			require.Equal(t, planpb.OpType_Equal, comparison.GetOp())
			require.NotNil(t, comparison.GetValue().GetArrayVal())
			require.Empty(t, comparison.GetColumnInfo().GetNestedPath())
		}
	}
}

func TestRewriteArrayFloat32ParsedBoundarySemantics(t *testing.T) {
	helper := arrayFloat32Schema(t)
	row := float64(16777216)
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{"ArrayFloat32[0] in [16777217.0]", 0},
		{"ArrayFloat32[0] == 16777217.0 OR ArrayFloat32[0] == 2.0", 2},
		{"ArrayFloat32[0] in [16777217.0, 2.0] OR ArrayFloat32[0] == 16777217.0", 2},
		{"ArrayFloat32[0] > 16777216.0 AND ArrayFloat32[0] >= 16777217.0", 0},
		{"ArrayFloat32[0] > 16777216.0 OR ArrayFloat32[0] >= 16777217.0", 2},
	} {
		for _, optimize := range []bool{false, true} {
			expr := parseArrayFloat32Plan(t, helper, tc.filter, optimize)
			require.Equal(t, tc.want, evalFloatRewritePlan(t, expr, &row), "optimize=%t %s; plan %s", optimize, tc.filter, expr)
			require.Equal(t, 1, evalFloatRewritePlan(t, expr, nil), "nullable array: %s", tc.filter)
		}
	}
}

func TestRewriteArrayFloat32ElementLevelStillUsesScalarOptimization(t *testing.T) {
	input := &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 201, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float, IsElementLevel: true},
		Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_FloatVal{FloatVal: 16777217}}},
	}}}
	result := rewriter.RewriteExprWithConfig(input, true)
	require.NotNil(t, result.GetUnaryRangeExpr())
	require.Equal(t, planpb.OpType_Equal, result.GetUnaryRangeExpr().GetOp())
	require.True(t, result.GetUnaryRangeExpr().GetColumnInfo().GetIsElementLevel())
}
